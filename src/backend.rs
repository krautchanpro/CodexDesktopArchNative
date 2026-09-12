use std::{
    collections::{HashMap, HashSet},
    env, fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{Receiver as UiReceiver, SyncSender, sync_channel},
    },
    time::Duration,
};

use anyhow::{Context, anyhow};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::UnixStream,
    process::Command,
    runtime::Runtime,
    sync::{Semaphore, mpsc},
    time::{Instant, MissedTickBehavior, interval, sleep, timeout},
};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    host::{self, HostAction},
    protocol::{PairingInfo, RequestId, RpcEnvelope, RpcResponse, initialize_request},
};

const COMMAND_CAPACITY: usize = 256;
const EVENT_CAPACITY: usize = 1024;
const INITIALIZE_ID: i64 = 0;
const MAX_MANAGED_FRAME_BYTES: u64 = 64 * 1024 * 1024;
const MANAGED_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MANAGED_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MANAGED_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const MANAGED_STALE_TIMEOUT: Duration = Duration::from_secs(30);
const PROFILE_WATCHDOG_INTERVAL: Duration = Duration::from_secs(30);
const PROFILE_WATCHDOG_SAMPLES: u8 = 2;
static DROPPED_UI_EVENTS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteAction {
    Enable,
    Disable,
    Pair,
    Refresh,
    Revoke {
        client_id: String,
        environment_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComputerAction {
    Doctor(PathBuf),
    Setup(PathBuf),
}

impl ComputerAction {
    fn binary(&self) -> &PathBuf {
        match self {
            Self::Doctor(binary) | Self::Setup(binary) => binary,
        }
    }

    fn argument(&self) -> &'static str {
        match self {
            Self::Doctor(_) => "doctor",
            Self::Setup(_) => "setup",
        }
    }
}

#[derive(Debug, Clone)]
pub enum BackendEvent {
    Connecting,
    Connected,
    Ready(Value),
    Disconnected(String),
    OversizedFrame(u64),
    Message(RpcEnvelope),
    Log(String),
    RemoteResult {
        action: RemoteAction,
        value: Value,
    },
    RemoteError {
        action: RemoteAction,
        message: String,
    },
    ComputerResult {
        action: ComputerAction,
        value: Value,
    },
    ComputerError {
        action: ComputerAction,
        message: String,
    },
    HostResult {
        action: HostAction,
        value: Value,
    },
    HostError {
        action: HostAction,
        message: String,
    },
    BuddyProgress {
        thread_id: String,
        turn_id: String,
        backend: String,
        progress: host::BuddyProgress,
    },
}

#[derive(Debug)]
enum HubCommand {
    Send(RpcEnvelope),
    Remote(RemoteAction),
    Computer(ComputerAction),
    Host(HostAction),
    Reconnect,
    Shutdown,
}

#[derive(Clone)]
pub struct AppServerHub {
    command_tx: mpsc::Sender<HubCommand>,
    events: Arc<std::sync::Mutex<UiReceiver<BackendEvent>>>,
    next_id: Arc<AtomicU64>,
    managed_transport: Arc<AtomicBool>,
    _runtime: Arc<Runtime>,
}

impl AppServerHub {
    pub fn spawn(runtime: Arc<Runtime>, codex_binary: Option<PathBuf>) -> Self {
        Self::spawn_with_profile(runtime, codex_binary, None)
    }

    pub fn spawn_with_profile(
        runtime: Arc<Runtime>,
        codex_binary: Option<PathBuf>,
        profile_home: Option<PathBuf>,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::channel(COMMAND_CAPACITY);
        let (event_tx, event_rx) = sync_channel(EVENT_CAPACITY);
        let binary = codex_binary
            .or_else(|| env::var_os("CODEX_CLI_PATH").map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from("codex"));
        let managed_transport = Arc::new(AtomicBool::new(false));
        runtime.spawn(supervise(
            binary,
            profile_home.clone(),
            command_rx,
            event_tx,
            managed_transport.clone(),
        ));

        Self {
            command_tx,
            events: Arc::new(std::sync::Mutex::new(event_rx)),
            next_id: Arc::new(AtomicU64::new(1)),
            managed_transport,
            _runtime: runtime,
        }
    }

    pub fn request(&self, method: &str, params: Value) -> RequestId {
        let id = RequestId::from(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.send(RpcEnvelope::request(id.clone(), method, params));
        id
    }

    pub fn respond(&self, id: RequestId, result: Value) {
        self.send(RpcEnvelope::success(id, result));
    }

    pub fn remote(&self, action: RemoteAction) {
        if let Err(error) = self.command_tx.try_send(HubCommand::Remote(action)) {
            warn!(%error, "remote command queue is full or closed");
        }
    }

    pub fn computer(&self, action: ComputerAction) {
        if let Err(error) = self.command_tx.try_send(HubCommand::Computer(action)) {
            warn!(%error, "computer-use command queue is full or closed");
        }
    }

    pub fn host(&self, action: HostAction) {
        if let Err(error) = self.command_tx.try_send(HubCommand::Host(action)) {
            warn!(%error, "native host command queue is full or closed");
        }
    }

    pub fn drain_events(&self, limit: usize, mut visit: impl FnMut(BackendEvent)) {
        let Ok(receiver) = self.events.lock() else {
            return;
        };
        for _ in 0..limit {
            let Ok(event) = receiver.try_recv() else {
                break;
            };
            visit(event);
        }
    }

    pub fn shutdown(&self) {
        let _ = self.command_tx.try_send(HubCommand::Shutdown);
    }

    pub fn reconnect(&self) {
        if let Err(error) = self.command_tx.try_send(HubCommand::Reconnect) {
            warn!(%error, "app-server reconnect queue is full or closed");
        }
    }

    pub fn uses_managed_transport(&self) -> bool {
        self.managed_transport.load(Ordering::Relaxed)
    }

    fn send(&self, message: RpcEnvelope) {
        if let Err(error) = self.command_tx.try_send(HubCommand::Send(message)) {
            warn!(%error, "app-server command queue is full or closed");
        }
    }
}

/// Keeps the desktop attached to exactly one account profile while allowing
/// each profile's Remote Control daemon to continue independently. Switching
/// closes only this GTK client's connection; it never stops a managed daemon.
#[derive(Clone)]
pub struct AccountHub {
    runtime: Arc<Runtime>,
    codex_binary: Option<PathBuf>,
    active: Arc<Mutex<AppServerHub>>,
    profile_home: Arc<Mutex<Option<PathBuf>>>,
    watched_profile_homes: Arc<Mutex<HashSet<Option<PathBuf>>>>,
}

impl AccountHub {
    pub fn spawn(runtime: Arc<Runtime>, codex_binary: Option<PathBuf>) -> Self {
        let active = AppServerHub::spawn(runtime.clone(), codex_binary.clone());
        Self {
            runtime,
            codex_binary,
            active: Arc::new(Mutex::new(active)),
            profile_home: Arc::new(Mutex::new(None)),
            watched_profile_homes: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Keep every configured Remote profile healthy even while its desktop
    /// view is inactive. Each check uses that profile's private app-server
    /// socket and restarts only the matching systemd service.
    pub fn watch_remote_profiles(&self, profile_homes: impl IntoIterator<Item = Option<PathBuf>>) {
        let mut watched = self
            .watched_profile_homes
            .lock()
            .expect("remote watchdog profile lock poisoned");
        for profile_home in profile_homes {
            if !watched.insert(profile_home.clone()) {
                continue;
            }
            self.runtime
                .spawn(supervise_profile_remote_watchdog(profile_home));
        }
    }

    pub fn switch_profile(&self, profile_home: Option<PathBuf>) {
        let next = AppServerHub::spawn_with_profile(
            self.runtime.clone(),
            self.codex_binary.clone(),
            profile_home.clone(),
        );
        let mut active = self
            .active
            .lock()
            .expect("active account hub lock poisoned");
        active.shutdown();
        *active = next;
        *self
            .profile_home
            .lock()
            .expect("active account profile lock poisoned") = profile_home;
    }

    pub fn request(&self, method: &str, params: Value) -> RequestId {
        self.active
            .lock()
            .expect("active account hub lock poisoned")
            .request(method, params)
    }

    pub fn respond(&self, id: RequestId, result: Value) {
        self.active
            .lock()
            .expect("active account hub lock poisoned")
            .respond(id, result);
    }

    pub fn remote(&self, action: RemoteAction) {
        self.active
            .lock()
            .expect("active account hub lock poisoned")
            .remote(action);
    }

    pub fn computer(&self, action: ComputerAction) {
        self.active
            .lock()
            .expect("active account hub lock poisoned")
            .computer(action);
    }

    pub fn host(&self, action: HostAction) {
        let profile_home = self
            .profile_home
            .lock()
            .expect("active account profile lock poisoned")
            .clone();
        let action = match action {
            HostAction::RemoteDoctor { configured_binary } => HostAction::RemoteDoctorProfile {
                configured_binary,
                profile_home,
            },
            HostAction::RemoteRestart { configured_binary } => HostAction::RemoteRestartProfile {
                configured_binary,
                profile_home,
            },
            action => action,
        };
        self.active
            .lock()
            .expect("active account hub lock poisoned")
            .host(action);
    }

    pub fn reconnect(&self) {
        self.active
            .lock()
            .expect("active account hub lock poisoned")
            .reconnect();
    }

    pub fn shutdown(&self) {
        self.active
            .lock()
            .expect("active account hub lock poisoned")
            .shutdown();
    }

    pub fn uses_managed_transport(&self) -> bool {
        self.active
            .lock()
            .expect("active account hub lock poisoned")
            .uses_managed_transport()
    }

    pub fn drain_events(&self, limit: usize, visit: impl FnMut(BackendEvent)) {
        self.active
            .lock()
            .expect("active account hub lock poisoned")
            .drain_events(limit, visit);
    }
}

async fn supervise(
    codex_binary: PathBuf,
    profile_home: Option<PathBuf>,
    mut commands: mpsc::Receiver<HubCommand>,
    events: SyncSender<BackendEvent>,
    managed_transport: Arc<AtomicBool>,
) {
    let mut failures = 0_u32;
    loop {
        emit(&events, BackendEvent::Connecting);
        let managed_socket = managed_primary_socket(profile_home.as_deref());
        managed_transport.store(managed_socket.is_some(), Ordering::Relaxed);
        let transport = if managed_socket.is_some() {
            "managed remote daemon"
        } else {
            "direct fallback"
        };
        let result = match managed_socket {
            Some(socket_path) => {
                run_managed_transport(
                    &socket_path,
                    &codex_binary,
                    profile_home.as_deref(),
                    &mut commands,
                    &events,
                )
                .await
            }
            None => {
                run_direct_transport(
                    &codex_binary,
                    profile_home.as_deref(),
                    &mut commands,
                    &events,
                )
                .await
            }
        };
        let reason = match result {
            Ok(ConnectionExit::Shutdown) => return,
            Ok(ConnectionExit::Reconnect) => {
                failures = 0;
                wait_for_managed_primary_socket(profile_home.as_deref()).await;
                continue;
            }
            Ok(ConnectionExit::Disconnected {
                reason,
                established,
            }) => {
                failures = if established {
                    0
                } else {
                    failures.saturating_add(1)
                };
                emit(&events, BackendEvent::Disconnected(reason.clone()));
                reason
            }
            Err(error) => {
                failures = failures.saturating_add(1);
                let reason = format!("{error:#}");
                emit(&events, BackendEvent::Disconnected(reason.clone()));
                reason
            }
        };

        let backoff = 1_u64 << failures.min(5);
        warn!(
            backoff,
            transport,
            %reason,
            "app-server transport disconnected; reconnecting"
        );
        sleep(Duration::from_secs(backoff)).await;
    }
}

async fn wait_for_managed_primary_socket(profile_home: Option<&Path>) {
    if env::var_os("CODEX_NATIVE_SMOKE_FIXTURES").is_some()
        || env::var_os("CODEX_NATIVE_FORCE_DIRECT_APP_SERVER").is_some()
    {
        return;
    }
    for _ in 0..40 {
        if managed_primary_socket(profile_home).is_some() {
            return;
        }
        sleep(Duration::from_millis(250)).await;
    }
}

enum ConnectionExit {
    Shutdown,
    Reconnect,
    Disconnected { reason: String, established: bool },
}

#[derive(Debug)]
enum ManagedRemotePending {
    RefreshStatus,
    RefreshClients { status: Value },
    Revoke(RemoteAction),
}

#[derive(Debug)]
struct ManagedRemoteRequest {
    request: ManagedRemotePending,
    sent_at: Instant,
}

fn managed_connection_is_stale(last_incoming: Instant, now: Instant) -> bool {
    now.duration_since(last_incoming) >= MANAGED_STALE_TIMEOUT
}

impl ManagedRemotePending {
    fn is_refresh(&self) -> bool {
        matches!(self, Self::RefreshStatus | Self::RefreshClients { .. })
    }

    fn action(&self) -> RemoteAction {
        match self {
            Self::RefreshStatus | Self::RefreshClients { .. } => RemoteAction::Refresh,
            Self::Revoke(action) => action.clone(),
        }
    }
}

async fn run_direct_transport(
    codex_binary: &PathBuf,
    profile_home: Option<&Path>,
    commands: &mut mpsc::Receiver<HubCommand>,
    events: &SyncSender<BackendEvent>,
) -> anyhow::Result<ConnectionExit> {
    ensure_profile_home(profile_home)?;
    let mut command = Command::new(codex_binary);
    configure_profile_command(&mut command, profile_home);
    let mut child = command
        .args(app_server_arguments())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("failed to start the shared direct Codex app-server")?;

    let mut stdin = child.stdin.take().context("app-server stdin unavailable")?;
    let stdout = child
        .stdout
        .take()
        .context("app-server stdout unavailable")?;
    let stderr = child
        .stderr
        .take()
        .context("app-server stderr unavailable")?;
    let mut stdout = BufReader::new(stdout).lines();
    let mut stderr = BufReader::new(stderr).lines();

    write_envelope(
        &mut stdin,
        &initialize_request(RequestId::number(INITIALIZE_ID)),
    )
    .await?;
    let mut initialized = false;
    let host_slots = Arc::new(Semaphore::new(4));

    loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(HubCommand::Send(message)) if initialized => {
                        write_envelope(&mut stdin, &message).await?;
                    }
                    Some(HubCommand::Send(_)) => {
                        warn!("dropping app-server message sent before initialization completed");
                    }
                    Some(HubCommand::Remote(action)) => {
                        tokio::spawn(run_remote_action(
                            codex_binary.clone(),
                            profile_home.map(Path::to_path_buf),
                            action,
                            events.clone(),
                        ));
                    }
                    Some(HubCommand::Computer(action)) => {
                        tokio::spawn(run_computer_action(action, events.clone()));
                    }
                    Some(HubCommand::Host(action)) => {
                        spawn_host_action(action, events.clone(), host_slots.clone());
                    }
                    Some(HubCommand::Reconnect) => {
                        let _ = child.kill().await;
                        return Ok(ConnectionExit::Reconnect);
                    }
                    Some(HubCommand::Shutdown) | None => {
                        let _ = child.kill().await;
                        return Ok(ConnectionExit::Shutdown);
                    }
                }
            }
            line = stdout.next_line() => {
                match line? {
                    Some(line) => {
                        let message = parse_stdout(&line, events);
                        match message {
                            Some(RpcEnvelope::Response(RpcResponse {
                                id: RequestId::Number(INITIALIZE_ID),
                                result: Some(value),
                                error: None,
                            })) => {
                                write_envelope(
                                    &mut stdin,
                                    &RpcEnvelope::notification("initialized", json!({})),
                                )
                                .await?;
                                initialized = true;
                                emit(events, BackendEvent::Connected);
                                emit(events, BackendEvent::Ready(value));
                                info!("connected to shared direct Codex app-server transport");
                            }
                            Some(RpcEnvelope::Response(RpcResponse {
                                id: RequestId::Number(INITIALIZE_ID),
                                error: Some(error),
                                ..
                            })) => {
                                return Err(anyhow!("app-server initialize failed: {}", error.message));
                            }
                            Some(message) if initialized => emit(events, BackendEvent::Message(message)),
                            Some(_) => warn!("received app-server message before initialize response"),
                            None => {}
                        }
                    }
                    None => return Ok(ConnectionExit::Disconnected {
                        reason: "direct app-server closed stdout".into(),
                        established: initialized,
                    }),
                }
            }
            line = stderr.next_line() => {
                if let Some(line) = line? {
                    debug!(target: "codex_app_server", "{line}");
                    emit(events, BackendEvent::Log(line));
                }
            }
        }
    }
}

async fn run_managed_transport(
    socket_path: &Path,
    codex_binary: &Path,
    profile_home: Option<&Path>,
    commands: &mut mpsc::Receiver<HubCommand>,
    events: &SyncSender<BackendEvent>,
) -> anyhow::Result<ConnectionExit> {
    let client = ManagedRemoteClient::connect(socket_path).await?;
    let initialize_result = client.initialize_result.clone();
    let (mut reader, mut writer) = client.stream.into_split();
    let (incoming_tx, mut incoming_rx) = mpsc::channel::<anyhow::Result<ManagedFrame>>(256);
    let reader_task = tokio::spawn(async move {
        loop {
            let frame = read_managed_frame(&mut reader).await;
            let terminal = frame
                .as_ref()
                .map_or(true, |frame| matches!(frame, ManagedFrame::Close(_)));
            if incoming_tx.send(frame).await.is_err() || terminal {
                return;
            }
        }
    });
    let _reader_guard = AbortTaskOnDrop(reader_task);
    let host_slots = Arc::new(Semaphore::new(4));
    let mut remote_pending = HashMap::new();
    let mut next_remote_id = 0_u64;
    let mut last_incoming = Instant::now();
    let mut heartbeat = interval(MANAGED_HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // The first interval tick fires immediately; consume it so a healthy
    // freshly initialized connection is not pinged twice during startup.
    heartbeat.tick().await;
    emit(events, BackendEvent::Connected);
    emit(events, BackendEvent::Ready(initialize_result));
    info!(socket = %socket_path.display(), "connected to managed Codex app-server transport shared with remote clients");

    loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(HubCommand::Send(message)) => {
                        let value = serde_json::to_value(message)?;
                        if let Err(error) = write_managed_json(&mut writer, &value).await {
                            return Ok(ConnectionExit::Disconnected {
                                reason: format!("managed app-server write failed: {error:#}"),
                                established: true,
                            });
                        }
                    }
                    Some(HubCommand::Remote(action)) => match action {
                        RemoteAction::Refresh => {
                            if !remote_pending
                                .values()
                                .any(|pending: &ManagedRemoteRequest| pending.request.is_refresh())
                                && let Err(error) = queue_managed_remote_request(
                                    &mut writer,
                                    &mut next_remote_id,
                                    &mut remote_pending,
                                    "remoteControl/status/read",
                                    json!({}),
                                    ManagedRemotePending::RefreshStatus,
                                )
                                .await
                            {
                                return Ok(managed_write_disconnected(error));
                            }
                        }
                        action @ RemoteAction::Revoke { .. } => {
                            let RemoteAction::Revoke {
                                client_id,
                                environment_id,
                            } = &action
                            else {
                                unreachable!();
                            };
                            if let Err(error) = queue_managed_remote_request(
                                &mut writer,
                                &mut next_remote_id,
                                &mut remote_pending,
                                "remoteControl/client/revoke",
                                json!({
                                    "clientId": client_id,
                                    "environmentId": environment_id,
                                }),
                                ManagedRemotePending::Revoke(action),
                            )
                            .await
                            {
                                return Ok(managed_write_disconnected(error));
                            }
                        }
                        action => {
                            tokio::spawn(run_remote_action(
                                codex_binary.to_path_buf(),
                                profile_home.map(Path::to_path_buf),
                                action,
                                events.clone(),
                            ));
                        }
                    },
                    Some(HubCommand::Computer(action)) => {
                        tokio::spawn(run_computer_action(action, events.clone()));
                    }
                    Some(HubCommand::Host(action)) => {
                        spawn_host_action(action, events.clone(), host_slots.clone());
                    }
                    Some(HubCommand::Reconnect) => {
                        let _ = write_managed_frame(&mut writer, 0x8, &[]).await;
                        return Ok(ConnectionExit::Reconnect);
                    }
                    Some(HubCommand::Shutdown) | None => {
                        let _ = write_managed_frame(&mut writer, 0x8, &[]).await;
                        return Ok(ConnectionExit::Shutdown);
                    }
                }
            }
            frame = incoming_rx.recv() => {
                match frame {
                    Some(Ok(ManagedFrame::Json(value))) => {
                        last_incoming = Instant::now();
                        match serde_json::from_value::<RpcEnvelope>(value.clone()) {
                            Ok(RpcEnvelope::Response(response))
                                if remote_pending.contains_key(&response.id) =>
                            {
                                if let Err(error) = handle_managed_remote_response(
                                    response,
                                    profile_home,
                                    &mut writer,
                                    &mut next_remote_id,
                                    &mut remote_pending,
                                    events,
                                )
                                .await
                                {
                                    return Ok(managed_write_disconnected(error));
                                }
                            }
                            Ok(RpcEnvelope::Notification(notification))
                                if notification.method == "remoteControl/status/changed" =>
                            {
                                if !remote_pending
                                    .values()
                                    .any(|pending: &ManagedRemoteRequest| pending.request.is_refresh())
                                    && let Err(error) = queue_managed_remote_request(
                                        &mut writer,
                                        &mut next_remote_id,
                                        &mut remote_pending,
                                        "remoteControl/status/read",
                                        json!({}),
                                        ManagedRemotePending::RefreshStatus,
                                    )
                                    .await
                                {
                                    return Ok(managed_write_disconnected(error));
                                }
                            }
                            Ok(message) => emit(events, BackendEvent::Message(message)),
                            Err(error) => {
                                error!(%error, message = %value, "invalid managed app-server JSON-RPC message");
                                emit(
                                    events,
                                    BackendEvent::Log(format!("Malformed managed app-server message: {error}")),
                                );
                            }
                        }
                    }
                    Some(Ok(ManagedFrame::Ping(payload))) => {
                        last_incoming = Instant::now();
                        if let Err(error) = write_managed_frame(&mut writer, 0xA, &payload).await {
                            return Ok(ConnectionExit::Disconnected {
                                reason: format!("managed app-server pong failed: {error:#}"),
                                established: true,
                            });
                        }
                    }
                    Some(Ok(ManagedFrame::Pong)) => {
                        last_incoming = Instant::now();
                    }
                    Some(Ok(ManagedFrame::Oversized(length))) => {
                        last_incoming = Instant::now();
                        warn!(length, "discarded oversized managed app-server frame without reconnecting");
                        emit(events, BackendEvent::OversizedFrame(length));
                    }
                    Some(Ok(ManagedFrame::Close(reason))) => {
                        return Ok(ConnectionExit::Disconnected {
                            reason,
                            established: true,
                        });
                    }
                    Some(Err(error)) => {
                        return Ok(ConnectionExit::Disconnected {
                            reason: format!("managed app-server read failed: {error:#}"),
                            established: true,
                        });
                    }
                    None => return Ok(ConnectionExit::Disconnected {
                        reason: "managed app-server reader stopped".into(),
                        established: true,
                    }),
                }
            }
            _ = heartbeat.tick() => {
                let now = Instant::now();
                if managed_connection_is_stale(last_incoming, now) {
                    return Ok(ConnectionExit::Disconnected {
                        reason: format!(
                            "managed app-server stopped answering heartbeats for {} seconds",
                            MANAGED_STALE_TIMEOUT.as_secs()
                        ),
                        established: true,
                    });
                }
                expire_managed_remote_requests(&mut remote_pending, now, events);
                if let Err(error) = write_managed_frame(&mut writer, 0x9, b"codex-native").await {
                    return Ok(managed_write_disconnected(error));
                }
            }
        }
    }
}

fn managed_write_disconnected(error: anyhow::Error) -> ConnectionExit {
    ConnectionExit::Disconnected {
        reason: format!("managed app-server write failed: {error:#}"),
        established: true,
    }
}

async fn queue_managed_remote_request<W>(
    writer: &mut W,
    next_remote_id: &mut u64,
    pending: &mut HashMap<RequestId, ManagedRemoteRequest>,
    method: &str,
    params: Value,
    request: ManagedRemotePending,
) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let id = RequestId::String(format!("codex-native-remote-{}", *next_remote_id));
    *next_remote_id = next_remote_id.saturating_add(1);
    let message = RpcEnvelope::request(id.clone(), method, params);
    write_managed_json(writer, &serde_json::to_value(message)?).await?;
    pending.insert(
        id,
        ManagedRemoteRequest {
            request,
            sent_at: Instant::now(),
        },
    );
    Ok(())
}

fn expire_managed_remote_requests(
    pending: &mut HashMap<RequestId, ManagedRemoteRequest>,
    now: Instant,
    events: &SyncSender<BackendEvent>,
) {
    let expired = pending
        .iter()
        .filter(|(_, pending)| now.duration_since(pending.sent_at) >= MANAGED_REQUEST_TIMEOUT)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    for id in expired {
        let Some(pending) = pending.remove(&id) else {
            continue;
        };
        emit(
            events,
            BackendEvent::RemoteError {
                action: pending.request.action(),
                message: format!(
                    "Managed remote status timed out after {} seconds; it will be retried.",
                    MANAGED_REQUEST_TIMEOUT.as_secs()
                ),
            },
        );
    }
}

async fn handle_managed_remote_response<W>(
    response: RpcResponse,
    profile_home: Option<&Path>,
    writer: &mut W,
    next_remote_id: &mut u64,
    pending: &mut HashMap<RequestId, ManagedRemoteRequest>,
    events: &SyncSender<BackendEvent>,
) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let Some(request) = pending.remove(&response.id) else {
        return Ok(());
    };
    let request = request.request;
    let action = request.action();
    if let Some(error) = response.error {
        if let ManagedRemotePending::RefreshClients { status } = request {
            warn!(message = %error.message, "managed remote client inventory is unavailable");
            emit_remote_status(events, profile_home, status, Vec::new());
        } else {
            emit(
                events,
                BackendEvent::RemoteError {
                    action,
                    message: error.message,
                },
            );
        }
        return Ok(());
    }
    let Some(result) = response.result else {
        emit(
            events,
            BackendEvent::RemoteError {
                action,
                message: "Managed remote response did not include a result".into(),
            },
        );
        return Ok(());
    };

    match request {
        ManagedRemotePending::RefreshStatus => {
            let environment_id = result
                .get("environmentId")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if result.get("status").and_then(Value::as_str) == Some("connected")
                && let Some(environment_id) = environment_id
            {
                queue_managed_remote_request(
                    writer,
                    next_remote_id,
                    pending,
                    "remoteControl/client/list",
                    json!({"environmentId": environment_id}),
                    ManagedRemotePending::RefreshClients { status: result },
                )
                .await?;
            } else {
                emit_remote_status(events, profile_home, result, Vec::new());
            }
        }
        ManagedRemotePending::RefreshClients { status } => {
            let clients = result
                .get("data")
                .or_else(|| result.get("clients"))
                .and_then(Value::as_array)
                .cloned()
                .or_else(|| result.as_array().cloned())
                .unwrap_or_default();
            emit_remote_status(events, profile_home, status, clients);
        }
        ManagedRemotePending::Revoke(action) => {
            emit(
                events,
                BackendEvent::RemoteResult {
                    action,
                    value: result,
                },
            );
        }
    }
    Ok(())
}

fn emit_remote_status(
    events: &SyncSender<BackendEvent>,
    profile_home: Option<&Path>,
    mut status: Value,
    clients: Vec<Value>,
) {
    let Some(status) = status.as_object_mut() else {
        emit(
            events,
            BackendEvent::RemoteError {
                action: RemoteAction::Refresh,
                message: "Managed remote status was not an object".into(),
            },
        );
        return;
    };
    status.insert("clients".into(), Value::Array(clients));
    if let Ok(codex_home) = codex_home(profile_home)
        && let Some(health) = managed_daemon_health(&codex_home)
    {
        status.insert("localDaemon".into(), health);
    }
    emit(
        events,
        BackendEvent::RemoteResult {
            action: RemoteAction::Refresh,
            value: Value::Object(status.clone()),
        },
    );
}

struct AbortTaskOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortTaskOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn spawn_host_action(action: HostAction, events: SyncSender<BackendEvent>, slots: Arc<Semaphore>) {
    tokio::spawn(async move {
        let Ok(_permit) = slots.acquire_owned().await else {
            return;
        };
        let task_action = action.clone();
        let progress_events = events.clone();
        let result = tokio::task::spawn_blocking(move || {
            if let HostAction::BuddyDelegate {
                thread_id,
                turn_id,
                backend,
                ..
            } = &task_action
            {
                return host::execute_buddy_with_progress(&task_action, |progress| {
                    emit(
                        &progress_events,
                        BackendEvent::BuddyProgress {
                            thread_id: thread_id.clone(),
                            turn_id: turn_id.clone(),
                            backend: backend.clone(),
                            progress,
                        },
                    );
                });
            }
            host::execute(&task_action)
        })
        .await;
        match result {
            Ok(Ok(value)) => emit(&events, BackendEvent::HostResult { action, value }),
            Ok(Err(error)) => emit(
                &events,
                BackendEvent::HostError {
                    action,
                    message: format!("{error:#}"),
                },
            ),
            Err(error) => emit(
                &events,
                BackendEvent::HostError {
                    action,
                    message: format!("native host task failed: {error}"),
                },
            ),
        }
    });
}

fn app_server_arguments() -> [&'static str; 2] {
    ["app-server", "--stdio"]
}

async fn run_computer_action(action: ComputerAction, events: SyncSender<BackendEvent>) {
    let result = timeout(
        Duration::from_secs(if matches!(&action, ComputerAction::Setup(_)) {
            120
        } else {
            30
        }),
        Command::new(action.binary())
            .arg(action.argument())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output(),
    )
    .await;

    match result {
        Ok(Ok(output)) if output.status.success() => {
            let text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            let value = if text.is_empty() {
                json!({"ok": true})
            } else {
                serde_json::from_str(&text).unwrap_or_else(|_| json!({"message": text}))
            };
            emit(&events, BackendEvent::ComputerResult { action, value });
        }
        Ok(Ok(output)) => {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            emit(
                &events,
                BackendEvent::ComputerError {
                    action,
                    message: if stderr.is_empty() { stdout } else { stderr },
                },
            );
        }
        Ok(Err(error)) => emit(
            &events,
            BackendEvent::ComputerError {
                action,
                message: error.to_string(),
            },
        ),
        Err(_) => emit(
            &events,
            BackendEvent::ComputerError {
                action,
                message: "Linux Computer Use command timed out".into(),
            },
        ),
    }
}

async fn write_envelope(
    stdin: &mut tokio::process::ChildStdin,
    message: &RpcEnvelope,
) -> anyhow::Result<()> {
    let mut bytes = serde_json::to_vec(message)?;
    bytes.push(b'\n');
    stdin.write_all(&bytes).await?;
    stdin.flush().await?;
    Ok(())
}

fn parse_stdout(line: &str, events: &SyncSender<BackendEvent>) -> Option<RpcEnvelope> {
    match serde_json::from_str::<RpcEnvelope>(line) {
        Ok(message) => Some(message),
        Err(error) => {
            error!(%error, line, "invalid app-server JSON-RPC message");
            emit(
                events,
                BackendEvent::Log(format!("Malformed app-server message: {error}")),
            );
            None
        }
    }
}

async fn run_remote_action(
    codex_binary: PathBuf,
    profile_home: Option<PathBuf>,
    action: RemoteAction,
    events: SyncSender<BackendEvent>,
) {
    let result = timeout(
        Duration::from_secs(30),
        execute_remote_action(&codex_binary, profile_home.as_deref(), &action),
    )
    .await;

    match result {
        Ok(Ok(value)) => {
            if matches!(&action, RemoteAction::Pair)
                && let Err(error) = serde_json::from_value::<PairingInfo>(value.clone())
            {
                warn!(%error, "remote pairing response used an unknown shape");
            }
            emit(&events, BackendEvent::RemoteResult { action, value });
        }
        Ok(Err(error)) => emit(
            &events,
            BackendEvent::RemoteError {
                action,
                message: format!("{error:#}"),
            },
        ),
        Err(_) => emit(
            &events,
            BackendEvent::RemoteError {
                action,
                message: "Remote command timed out".into(),
            },
        ),
    }
}

async fn execute_remote_action(
    codex_binary: &Path,
    profile_home: Option<&Path>,
    action: &RemoteAction,
) -> anyhow::Result<Value> {
    match action {
        RemoteAction::Enable => {
            if let Some(value) = run_remote_service_action("start", profile_home).await? {
                return Ok(value);
            }
            run_remote_cli(
                codex_binary,
                profile_home,
                &["remote-control", "start", "--json"],
            )
            .await
        }
        RemoteAction::Disable => {
            if let Some(value) = run_remote_service_action("stop", profile_home).await? {
                return Ok(value);
            }
            run_remote_cli(
                codex_binary,
                profile_home,
                &["remote-control", "stop", "--json"],
            )
            .await
        }
        RemoteAction::Pair => {
            run_remote_cli(
                codex_binary,
                profile_home,
                &["remote-control", "pair", "--json"],
            )
            .await
        }
        RemoteAction::Refresh => query_managed_remote(profile_home).await,
        RemoteAction::Revoke {
            client_id,
            environment_id,
        } => revoke_managed_remote_client(profile_home, client_id, environment_id).await,
    }
}

async fn run_remote_service_action(
    action: &str,
    profile_home: Option<&Path>,
) -> anyhow::Result<Option<Value>> {
    let unit_name = remote_service_unit(profile_home)?;
    let unit_status = Command::new("systemctl")
        .args(["--user", "cat", &unit_name])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
    if !unit_status.is_ok_and(|status| status.success()) {
        return Ok(None);
    }

    let output = Command::new("systemctl")
        .args(["--user", action, &unit_name])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .with_context(|| format!("failed to {action} {unit_name}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        return Err(anyhow!(if stderr.is_empty() { stdout } else { stderr }));
    }
    Ok(Some(json!({
        "ok": true,
        "service": unit_name,
        "action": action,
    })))
}

async fn remote_service_is_active(profile_home: Option<&Path>) -> bool {
    let Ok(unit_name) = remote_service_unit(profile_home) else {
        return false;
    };
    Command::new("systemctl")
        .args(["--user", "is-active", "--quiet", &unit_name])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .is_ok_and(|status| status.success())
}

fn profile_remote_watchdog_unhealthy(status: &Value) -> bool {
    status
        .pointer("/localDaemon/overloaded")
        .and_then(Value::as_bool)
        == Some(true)
        || status.get("status").and_then(Value::as_str) != Some("connected")
}

async fn supervise_profile_remote_watchdog(profile_home: Option<PathBuf>) {
    let mut unhealthy_samples = 0_u8;
    loop {
        sleep(PROFILE_WATCHDOG_INTERVAL).await;
        if !remote_service_is_active(profile_home.as_deref()).await {
            // A profile may have Remote disabled deliberately. systemd itself
            // owns crash recovery once an enabled service is active.
            unhealthy_samples = 0;
            continue;
        }
        let unhealthy = match query_managed_remote(profile_home.as_deref()).await {
            Ok(status) => profile_remote_watchdog_unhealthy(&status),
            Err(error) => {
                warn!(%error, profile = ?profile_home, "profile remote watchdog health probe failed");
                true
            }
        };
        if !unhealthy {
            unhealthy_samples = 0;
            continue;
        }
        unhealthy_samples = unhealthy_samples.saturating_add(1);
        if unhealthy_samples < PROFILE_WATCHDOG_SAMPLES {
            continue;
        }
        match run_remote_service_action("restart", profile_home.as_deref()).await {
            Ok(Some(_)) => {
                info!(profile = ?profile_home, "profile remote watchdog restarted unhealthy host")
            }
            Ok(None) => {
                warn!(profile = ?profile_home, "profile remote watchdog found no systemd host service")
            }
            Err(error) => {
                warn!(%error, profile = ?profile_home, "profile remote watchdog restart failed")
            }
        }
        unhealthy_samples = 0;
    }
}

async fn run_remote_cli(
    codex_binary: &Path,
    profile_home: Option<&Path>,
    args: &[&str],
) -> anyhow::Result<Value> {
    ensure_profile_home(profile_home)?;
    let mut command = Command::new(codex_binary);
    configure_profile_command(&mut command, profile_home);
    let output = command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .with_context(|| format!("failed to run {}", codex_binary.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        return Err(anyhow!(if stderr.is_empty() { stdout } else { stderr }));
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if text.is_empty() {
        Ok(json!({ "ok": true }))
    } else {
        serde_json::from_str(&text)
            .or_else(|_| Ok::<_, serde_json::Error>(json!({ "message": text })))
            .map_err(Into::into)
    }
}

fn configure_profile_command(command: &mut Command, profile_home: Option<&Path>) {
    if let Some(profile_home) = profile_home {
        command.env("CODEX_HOME", profile_home);
    }
}

fn ensure_profile_home(profile_home: Option<&Path>) -> anyhow::Result<()> {
    let Some(profile_home) = profile_home else {
        return Ok(());
    };
    fs::create_dir_all(profile_home)
        .with_context(|| format!("failed to create {}", profile_home.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(profile_home, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed to secure {}", profile_home.display()))?;
    }
    Ok(())
}

fn remote_service_unit(profile_home: Option<&Path>) -> anyhow::Result<String> {
    if profile_home.is_none() {
        return Ok("codex-native-remote.service".into());
    }
    let profile_id = profile_home
        .and_then(Path::parent)
        .and_then(Path::file_name)
        .and_then(|value| value.to_str())
        .filter(|value| {
            !value.is_empty()
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
        .context("account profile path has no safe service identifier")?;
    Ok(format!("codex-native-remote@{profile_id}.service"))
}

fn codex_home(profile_home: Option<&Path>) -> anyhow::Result<PathBuf> {
    if let Some(profile_home) = profile_home {
        return Ok(profile_home.to_path_buf());
    }
    if let Some(path) = env::var_os("CODEX_HOME") {
        return Ok(PathBuf::from(path));
    }
    dirs::home_dir()
        .map(|home| home.join(".codex"))
        .context("Codex home directory is unavailable")
}

fn daemon_pid_from_metadata(bytes: &[u8]) -> Option<u32> {
    serde_json::from_slice::<Value>(bytes)
        .ok()?
        .get("pid")?
        .as_u64()?
        .try_into()
        .ok()
}

fn proc_status_kib(status: &str, field: &str) -> Option<u64> {
    status.lines().find_map(|line| {
        let value = line.strip_prefix(field)?.trim();
        value.split_whitespace().next()?.parse().ok()
    })
}

fn is_legacy_codex_desktop_command(command_line: &[u8]) -> bool {
    command_line
        .split(|byte| *byte == 0)
        .any(|argument| argument.ends_with(b"/opt/codex-desktop/electron"))
        && command_line
            .split(|byte| *byte == 0)
            .any(|argument| argument == b"--app-id=codex-desktop")
}

fn legacy_codex_desktop_running() -> bool {
    let Ok(entries) = fs::read_dir("/proc") else {
        return false;
    };
    entries.filter_map(Result::ok).any(|entry| {
        entry
            .file_name()
            .to_string_lossy()
            .bytes()
            .all(|byte| byte.is_ascii_digit())
            && fs::read(entry.path().join("cmdline"))
                .is_ok_and(|command_line| is_legacy_codex_desktop_command(&command_line))
    })
}

fn managed_daemon_health(codex_home: &Path) -> Option<Value> {
    let metadata = fs::read(codex_home.join("app-server-daemon/app-server.pid")).ok()?;
    let pid = daemon_pid_from_metadata(&metadata)?;
    let status = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let rss_kib = proc_status_kib(&status, "VmRSS:").unwrap_or(0);
    let swap_kib = proc_status_kib(&status, "VmSwap:").unwrap_or(0);
    let footprint_mib = rss_kib.saturating_add(swap_kib).div_ceil(1024);
    let logs_database_bytes = fs::metadata(codex_home.join("logs_2.sqlite"))
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let logs_wal_bytes = fs::metadata(codex_home.join("logs_2.sqlite-wal"))
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let overloaded = footprint_mib >= 1536 || swap_kib.div_ceil(1024) >= 512;
    Some(json!({
        "pid": pid,
        "rssMiB": rss_kib.div_ceil(1024),
        "swapMiB": swap_kib.div_ceil(1024),
        "footprintMiB": footprint_mib,
        "logsDatabaseMiB": logs_database_bytes.div_ceil(1024 * 1024),
        "logsWalMiB": logs_wal_bytes.div_ceil(1024 * 1024),
        "legacyDesktopRunning": legacy_codex_desktop_running(),
        "overloaded": overloaded,
    }))
}

fn managed_remote_daemon_is_running(codex_home: &Path) -> bool {
    let Ok(metadata) = fs::read(codex_home.join("app-server-daemon/app-server.pid")) else {
        return false;
    };
    let Some(pid) = daemon_pid_from_metadata(&metadata) else {
        return false;
    };
    let Ok(command_line) = fs::read(format!("/proc/{pid}/cmdline")) else {
        return false;
    };
    let arguments = command_line
        .split(|byte| *byte == 0)
        .filter(|argument| !argument.is_empty())
        .collect::<Vec<_>>();
    arguments.contains(&b"app-server".as_slice())
        && arguments.contains(&b"--remote-control".as_slice())
}

fn managed_primary_socket(profile_home: Option<&Path>) -> Option<PathBuf> {
    if env::var_os("CODEX_NATIVE_SMOKE_FIXTURES").is_some()
        || env::var_os("CODEX_NATIVE_FORCE_DIRECT_APP_SERVER").is_some()
    {
        return None;
    }
    if let Some(socket_path) = env::var_os("CODEX_NATIVE_MANAGED_APP_SERVER_SOCKET") {
        return Some(PathBuf::from(socket_path));
    }
    let codex_home = codex_home(profile_home).ok()?;
    managed_remote_daemon_is_running(&codex_home)
        .then(|| codex_home.join("app-server-control/app-server-control.sock"))
}

async fn managed_remote_client(profile_home: Option<&Path>) -> anyhow::Result<ManagedRemoteClient> {
    let codex_home = codex_home(profile_home)?;
    if !managed_remote_daemon_is_running(&codex_home) {
        return Err(anyhow!("the managed remote-control daemon is not running"));
    }
    ManagedRemoteClient::connect(&codex_home.join("app-server-control/app-server-control.sock"))
        .await
}

async fn query_managed_remote(profile_home: Option<&Path>) -> anyhow::Result<Value> {
    let codex_home = codex_home(profile_home)?;
    if !managed_remote_daemon_is_running(&codex_home) {
        return Ok(json!({"status": "disabled", "clients": []}));
    }
    let mut client = ManagedRemoteClient::connect(
        &codex_home.join("app-server-control/app-server-control.sock"),
    )
    .await?;
    let result = async {
        let mut status = client.call("remoteControl/status/read", json!({})).await?;
        let clients = if status.get("status").and_then(Value::as_str) == Some("connected") {
            if let Some(environment_id) = status.get("environmentId").and_then(Value::as_str) {
                match client
                    .call(
                        "remoteControl/client/list",
                        json!({"environmentId": environment_id}),
                    )
                    .await
                {
                    Ok(value) => value
                        .get("data")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default(),
                    Err(error) => {
                        warn!(%error, "managed remote client inventory is unavailable");
                        Vec::new()
                    }
                }
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };
        status
            .as_object_mut()
            .context("managed remote status was not an object")?
            .insert("clients".into(), Value::Array(clients));
        Ok(status)
    };
    let result = result.await;
    let _ = client.close().await;
    result
}

async fn revoke_managed_remote_client(
    profile_home: Option<&Path>,
    client_id: &str,
    environment_id: &str,
) -> anyhow::Result<Value> {
    let mut client = managed_remote_client(profile_home).await?;
    let result = client
        .call(
            "remoteControl/client/revoke",
            json!({"clientId": client_id, "environmentId": environment_id}),
        )
        .await;
    let _ = client.close().await;
    result
}

struct ManagedRemoteClient {
    stream: UnixStream,
    next_id: u64,
    initialize_result: Value,
}

impl ManagedRemoteClient {
    async fn connect(socket_path: &Path) -> anyhow::Result<Self> {
        timeout(
            MANAGED_CONNECT_TIMEOUT,
            Self::connect_with_no_outer_timeout(socket_path),
        )
        .await
        .with_context(|| {
            format!(
                "managed app-server connection timed out after {} seconds",
                MANAGED_CONNECT_TIMEOUT.as_secs()
            )
        })?
    }

    async fn connect_with_no_outer_timeout(socket_path: &Path) -> anyhow::Result<Self> {
        let mut stream = UnixStream::connect(socket_path)
            .await
            .with_context(|| format!("could not connect to {}", socket_path.display()))?;
        let key = BASE64_STANDARD.encode(Uuid::new_v4().as_bytes());
        let request = format!(
            "GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).await?;
        stream.flush().await?;

        let mut response = Vec::with_capacity(512);
        let mut chunk = [0_u8; 512];
        while !response.windows(4).any(|window| window == b"\r\n\r\n") {
            let count = stream.read(&mut chunk).await?;
            if count == 0 {
                return Err(anyhow!(
                    "managed remote socket closed during WebSocket handshake"
                ));
            }
            response.extend_from_slice(&chunk[..count]);
            if response.len() > 16 * 1024 {
                return Err(anyhow!("managed remote WebSocket handshake was too large"));
            }
        }
        let headers = String::from_utf8_lossy(&response);
        if !headers.starts_with("HTTP/1.1 101 ") && !headers.starts_with("HTTP/1.0 101 ") {
            return Err(anyhow!(
                "managed remote WebSocket handshake failed: {}",
                headers.lines().next().unwrap_or("unknown response")
            ));
        }

        let mut client = Self {
            stream,
            next_id: 0,
            initialize_result: Value::Null,
        };
        let initialize_result = client
            .call(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "codex_native_arch_remote",
                        "title": "Codex Native for Arch Linux",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": {
                        "experimentalApi": true,
                        "mcpServerOpenaiFormElicitation": true
                    }
                }),
            )
            .await?;
        client
            .write_json(&json!({"method": "initialized", "params": {}}))
            .await?;
        client.initialize_result = initialize_result;
        Ok(client)
    }

    async fn call(&mut self, method: &str, params: Value) -> anyhow::Result<Value> {
        timeout(
            MANAGED_REQUEST_TIMEOUT,
            self.call_with_no_outer_timeout(method, params),
        )
        .await
        .with_context(|| {
            format!(
                "{method} timed out after {} seconds",
                MANAGED_REQUEST_TIMEOUT.as_secs()
            )
        })?
    }

    async fn call_with_no_outer_timeout(
        &mut self,
        method: &str,
        params: Value,
    ) -> anyhow::Result<Value> {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        self.write_json(&json!({"id": id, "method": method, "params": params}))
            .await?;
        loop {
            let message = self.read_json().await?;
            if message.get("id").and_then(Value::as_u64) != Some(id) {
                continue;
            }
            if let Some(error) = message.get("error") {
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("managed remote request failed");
                return Err(anyhow!("{method}: {message}"));
            }
            return message
                .get("result")
                .cloned()
                .context("managed remote response did not include a result");
        }
    }

    async fn write_json(&mut self, value: &Value) -> anyhow::Result<()> {
        write_managed_json(&mut self.stream, value).await
    }

    async fn write_frame(&mut self, opcode: u8, payload: &[u8]) -> anyhow::Result<()> {
        write_managed_frame(&mut self.stream, opcode, payload).await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.write_frame(0x8, &[]).await
    }

    async fn read_json(&mut self) -> anyhow::Result<Value> {
        loop {
            match read_managed_frame(&mut self.stream).await? {
                ManagedFrame::Json(value) => return Ok(value),
                ManagedFrame::Ping(payload) => self.write_frame(0xA, &payload).await?,
                ManagedFrame::Pong => {}
                ManagedFrame::Oversized(length) => {
                    return Err(anyhow!(
                        "managed remote handshake frame exceeded the bounded limit: {length} bytes"
                    ));
                }
                ManagedFrame::Close(reason) => return Err(anyhow!(reason)),
            }
        }
    }
}

#[derive(Debug)]
enum ManagedFrame {
    Json(Value),
    Ping(Vec<u8>),
    Pong,
    Oversized(u64),
    Close(String),
}

async fn write_managed_json<W>(writer: &mut W, value: &Value) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let payload = serde_json::to_vec(value)?;
    write_managed_frame(writer, 0x1, &payload).await
}

async fn write_managed_frame<W>(writer: &mut W, opcode: u8, payload: &[u8]) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut frame = Vec::with_capacity(payload.len() + 14);
    frame.push(0x80 | opcode);
    match payload.len() {
        length @ 0..=125 => frame.push(0x80 | length as u8),
        length @ 126..=65535 => {
            frame.push(0x80 | 126);
            frame.extend_from_slice(&(length as u16).to_be_bytes());
        }
        length => {
            frame.push(0x80 | 127);
            frame.extend_from_slice(&(length as u64).to_be_bytes());
        }
    }
    let uuid = Uuid::new_v4();
    let mask = &uuid.as_bytes()[..4];
    frame.extend_from_slice(mask);
    frame.extend(
        payload
            .iter()
            .enumerate()
            .map(|(index, byte)| byte ^ mask[index % 4]),
    );
    writer.write_all(&frame).await?;
    writer.flush().await?;
    Ok(())
}

async fn read_managed_frame<R>(reader: &mut R) -> anyhow::Result<ManagedFrame>
where
    R: AsyncRead + Unpin,
{
    read_managed_frame_with_limit(reader, MAX_MANAGED_FRAME_BYTES).await
}

async fn read_managed_frame_with_limit<R>(
    reader: &mut R,
    max_payload_bytes: u64,
) -> anyhow::Result<ManagedFrame>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0_u8; 2];
    reader.read_exact(&mut header).await?;
    if header[0] & 0x80 == 0 {
        return Err(anyhow!("fragmented managed remote frames are unsupported"));
    }
    let opcode = header[0] & 0x0f;
    let masked = header[1] & 0x80 != 0;
    let mut length = u64::from(header[1] & 0x7f);
    if length == 126 {
        let mut extended = [0_u8; 2];
        reader.read_exact(&mut extended).await?;
        length = u64::from(u16::from_be_bytes(extended));
    } else if length == 127 {
        let mut extended = [0_u8; 8];
        reader.read_exact(&mut extended).await?;
        length = u64::from_be_bytes(extended);
    }
    let mut mask = [0_u8; 4];
    if masked {
        reader.read_exact(&mut mask).await?;
    }
    if length > max_payload_bytes {
        let mut remaining = length;
        let mut discard = [0_u8; 64 * 1024];
        while remaining > 0 {
            let chunk = remaining.min(discard.len() as u64) as usize;
            reader.read_exact(&mut discard[..chunk]).await?;
            remaining -= chunk as u64;
        }
        return Ok(ManagedFrame::Oversized(length));
    }
    let mut payload = vec![0_u8; length as usize];
    reader.read_exact(&mut payload).await?;
    if masked {
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[index % 4];
        }
    }
    match opcode {
        0x1 => Ok(ManagedFrame::Json(serde_json::from_slice(&payload)?)),
        0x8 => {
            let reason = payload
                .get(2..)
                .filter(|reason| !reason.is_empty())
                .map(String::from_utf8_lossy)
                .map(|reason| reason.into_owned())
                .unwrap_or_else(|| "managed remote WebSocket closed".into());
            Ok(ManagedFrame::Close(reason))
        }
        0x9 => Ok(ManagedFrame::Ping(payload)),
        0xA => Ok(ManagedFrame::Pong),
        _ => Err(anyhow!(
            "unexpected managed remote WebSocket opcode {opcode}"
        )),
    }
}

fn emit(events: &SyncSender<BackendEvent>, event: BackendEvent) {
    if let Err(error) = events.try_send(event) {
        let dropped_events = increment_saturating(&DROPPED_UI_EVENTS);
        if should_report_drop(dropped_events) {
            warn!(
                %error,
                dropped_events,
                "dropping UI events because the bounded channel is full or closed"
            );
        }
    }
}

fn increment_saturating(counter: &AtomicU64) -> u64 {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_add(1))
        })
        .map_or_else(|current| current, |previous| previous.saturating_add(1))
}

fn should_report_drop(dropped_events: u64) -> bool {
    dropped_events.is_power_of_two()
}

// When Remote Control is active, Native and paired iOS clients share its one
// managed app-server and therefore its live subscriptions and active turns.
// Direct stdio remains the one-process fallback while remote hosting is off.

#[cfg(test)]
mod tests {
    use std::{sync::mpsc::sync_channel, time::Duration};

    use tokio::net::{UnixListener, UnixStream};

    use super::*;

    async fn read_test_ws_frame(stream: &mut UnixStream) -> Value {
        let mut header = [0_u8; 2];
        stream.read_exact(&mut header).await.unwrap();
        assert_eq!(header[0] & 0x0f, 0x1);
        let mut length = u64::from(header[1] & 0x7f);
        if length == 126 {
            let mut extended = [0_u8; 2];
            stream.read_exact(&mut extended).await.unwrap();
            length = u64::from(u16::from_be_bytes(extended));
        } else if length == 127 {
            let mut extended = [0_u8; 8];
            stream.read_exact(&mut extended).await.unwrap();
            length = u64::from_be_bytes(extended);
        }
        let masked = header[1] & 0x80 != 0;
        let mut mask = [0_u8; 4];
        if masked {
            stream.read_exact(&mut mask).await.unwrap();
        }
        let mut payload = vec![0_u8; length as usize];
        stream.read_exact(&mut payload).await.unwrap();
        if masked {
            for (index, byte) in payload.iter_mut().enumerate() {
                *byte ^= mask[index % 4];
            }
        }
        serde_json::from_slice(&payload).unwrap()
    }

    fn test_ws_frame(value: &Value) -> Vec<u8> {
        let payload = serde_json::to_vec(value).unwrap();
        let mut frame = vec![0x81];
        match payload.len() {
            length @ 0..=125 => frame.push(length as u8),
            length @ 126..=65535 => {
                frame.push(126);
                frame.extend_from_slice(&(length as u16).to_be_bytes());
            }
            length => {
                frame.push(127);
                frame.extend_from_slice(&(length as u64).to_be_bytes());
            }
        }
        frame.extend_from_slice(&payload);
        frame
    }

    async fn write_test_ws_frame(stream: &mut UnixStream, value: &Value) {
        let frame = test_ws_frame(value);
        stream.write_all(&frame).await.unwrap();
        stream.flush().await.unwrap();
    }

    #[test]
    fn parser_accepts_string_server_request_ids() {
        let (sender, _receiver) = sync_channel(2);
        let message = parse_stdout(
            r#"{"id":"approval-1","method":"item/fileChange/requestApproval","params":{}}"#,
            &sender,
        )
        .unwrap();
        assert!(matches!(message, RpcEnvelope::Request(_)));
    }

    #[test]
    fn direct_fallback_is_one_app_server() {
        assert_eq!(app_server_arguments(), ["app-server", "--stdio"]);
    }

    #[test]
    fn managed_daemon_pid_metadata_is_strict_json() {
        assert_eq!(
            daemon_pid_from_metadata(br#"{"pid":3518245}"#),
            Some(3_518_245)
        );
        assert_eq!(daemon_pid_from_metadata(br#"{"pid":"3518245"}"#), None);
        assert_eq!(daemon_pid_from_metadata(b"3518245"), None);
    }

    #[tokio::test]
    async fn managed_frames_allow_catalogs_and_skip_unbounded_payloads() {
        let accepted = json!({"data": "x".repeat(9 * 1024 * 1024)});
        let mut accepted_frame = std::io::Cursor::new(test_ws_frame(&accepted));
        assert!(matches!(
            read_managed_frame(&mut accepted_frame).await.unwrap(),
            ManagedFrame::Json(_)
        ));

        let oversized = json!({"data": "x".repeat(1025)});
        let following = json!({"id": 42, "result": {"status": "connected"}});
        let mut frames = test_ws_frame(&oversized);
        frames.extend(test_ws_frame(&following));
        let mut frames = std::io::Cursor::new(frames);
        assert!(matches!(
            read_managed_frame_with_limit(&mut frames, 1024)
                .await
                .unwrap(),
            ManagedFrame::Oversized(_)
        ));
        assert!(matches!(
            read_managed_frame_with_limit(&mut frames, 1024 * 1024)
                .await
                .unwrap(),
            ManagedFrame::Json(value) if value == following
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn managed_transport_keeps_partial_reads_while_routing_requests() {
        let temporary = tempfile::tempdir().unwrap();
        let socket_path = temporary.path().join("app-server.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let (frame_started_tx, frame_started_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut handshake = Vec::new();
            let mut byte = [0_u8; 1];
            while !handshake.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).await.unwrap();
                handshake.push(byte[0]);
            }
            stream
                .write_all(
                    b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
                )
                .await
                .unwrap();

            let initialize = read_test_ws_frame(&mut stream).await;
            assert_eq!(initialize.get("id"), Some(&json!(0)));
            assert_eq!(initialize.get("method"), Some(&json!("initialize")));
            write_test_ws_frame(
                &mut stream,
                &json!({"id": 0, "result": {"platform": {"family": "unix", "os": "linux"}}}),
            )
            .await;
            let initialized = read_test_ws_frame(&mut stream).await;
            assert_eq!(initialized.get("method"), Some(&json!("initialized")));

            let broadcast = test_ws_frame(&json!({
                "method": "thread/name/updated",
                "params": {"threadId": "shared-thread", "name": "Changed from iOS"}
            }));
            // Hold a text frame between its first and second header bytes while
            // the desktop sends a request. A select! that reads the socket
            // directly will cancel read_exact after consuming byte one and
            // corrupt the stream; the dedicated reader must finish the frame.
            stream.write_all(&broadcast[..1]).await.unwrap();
            frame_started_tx.send(()).unwrap();
            sleep(Duration::from_millis(100)).await;
            stream.write_all(&broadcast[1..]).await.unwrap();
            stream.flush().await.unwrap();
            let request = read_test_ws_frame(&mut stream).await;
            assert_eq!(request.get("id"), Some(&json!(7)));
            assert_eq!(request.get("method"), Some(&json!("thread/read")));
            write_test_ws_frame(
                &mut stream,
                &json!({"id": 7, "result": {"thread": {"id": "shared-thread"}}}),
            )
            .await;

            let status_request = read_test_ws_frame(&mut stream).await;
            assert_eq!(
                status_request.get("method"),
                Some(&json!("remoteControl/status/read"))
            );
            let status_id = status_request.get("id").cloned().unwrap();
            assert!(status_id.as_str().is_some());
            write_test_ws_frame(
                &mut stream,
                &json!({
                    "id": status_id,
                    "result": {
                        "status": "connected",
                        "environmentId": "environment-1"
                    }
                }),
            )
            .await;
            let clients_request = read_test_ws_frame(&mut stream).await;
            assert_eq!(
                clients_request.get("method"),
                Some(&json!("remoteControl/client/list"))
            );
            assert_eq!(
                clients_request
                    .get("params")
                    .and_then(|params| params.get("environmentId")),
                Some(&json!("environment-1"))
            );
            let clients_id = clients_request.get("id").cloned().unwrap();
            write_test_ws_frame(
                &mut stream,
                &json!({
                    "id": clients_id,
                    "result": {"data": [{"clientId": "iphone-1"}]}
                }),
            )
            .await;

            // A graceful close keeps the daemon from retaining a dead client
            // and broadcasting every later task event into a stale socket.
            let mut close = [0_u8; 6];
            stream.read_exact(&mut close).await.unwrap();
            assert_eq!(close[0] & 0x0f, 0x8);
            assert_eq!(close[1] & 0x7f, 0);
            assert_ne!(close[1] & 0x80, 0);
        });

        let (command_tx, mut command_rx) = mpsc::channel(8);
        let (event_tx, event_rx) = sync_channel(8);
        let transport_socket = socket_path.clone();
        let transport = tokio::spawn(async move {
            run_managed_transport(
                &transport_socket,
                &PathBuf::from("codex"),
                None,
                &mut command_rx,
                &event_tx,
            )
            .await
        });

        assert!(matches!(
            event_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            BackendEvent::Connected
        ));
        assert!(matches!(
            event_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            BackendEvent::Ready(_)
        ));
        frame_started_rx.await.unwrap();
        sleep(Duration::from_millis(30)).await;
        command_tx
            .send(HubCommand::Send(RpcEnvelope::request(
                RequestId::number(7),
                "thread/read",
                json!({"threadId": "shared-thread"}),
            )))
            .await
            .unwrap();
        let mut saw_broadcast = false;
        let mut saw_response = false;
        for _ in 0..2 {
            match event_rx.recv_timeout(Duration::from_secs(2)).unwrap() {
                BackendEvent::Message(RpcEnvelope::Notification(notification))
                    if notification.method == "thread/name/updated" =>
                {
                    saw_broadcast = true;
                }
                BackendEvent::Message(RpcEnvelope::Response(RpcResponse {
                    id: RequestId::Number(7),
                    error: None,
                    ..
                })) => saw_response = true,
                event => panic!("unexpected managed transport event: {event:?}"),
            }
        }
        assert!(saw_broadcast && saw_response);
        command_tx
            .send(HubCommand::Remote(RemoteAction::Refresh))
            .await
            .unwrap();
        command_tx
            .send(HubCommand::Remote(RemoteAction::Refresh))
            .await
            .unwrap();
        match event_rx.recv_timeout(Duration::from_secs(2)).unwrap() {
            BackendEvent::RemoteResult {
                action: RemoteAction::Refresh,
                value,
            } => {
                assert_eq!(value.get("status"), Some(&json!("connected")));
                assert_eq!(
                    value
                        .get("clients")
                        .and_then(Value::as_array)
                        .and_then(|clients| clients.first())
                        .and_then(|client| client.get("clientId")),
                    Some(&json!("iphone-1"))
                );
            }
            event => panic!("unexpected managed remote event: {event:?}"),
        }
        command_tx.send(HubCommand::Shutdown).await.unwrap();
        assert!(matches!(
            transport.await.unwrap().unwrap(),
            ConnectionExit::Shutdown
        ));
        server.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires the user's live managed remote-control daemon"]
    async fn live_managed_transport_survives_full_plugin_catalog() {
        assert_eq!(
            env::var("CODEX_NATIVE_LIVE_REMOTE_SMOKE").as_deref(),
            Ok("1"),
            "set CODEX_NATIVE_LIVE_REMOTE_SMOKE=1 to run the live probe"
        );
        let socket_path = managed_primary_socket(None).expect("managed remote socket");
        let (command_tx, mut command_rx) = mpsc::channel(8);
        let (event_tx, event_rx) = sync_channel(64);
        let transport = tokio::spawn(async move {
            run_managed_transport(
                &socket_path,
                &PathBuf::from("codex"),
                None,
                &mut command_rx,
                &event_tx,
            )
            .await
        });
        assert!(matches!(
            event_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            BackendEvent::Connected
        ));
        assert!(matches!(
            event_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            BackendEvent::Ready(_)
        ));

        let catalog_id = RequestId::number(9_001);
        command_tx
            .send(HubCommand::Send(RpcEnvelope::request(
                catalog_id.clone(),
                "plugin/list",
                json!({"cwds": [env::current_dir().unwrap()]}),
            )))
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(45);
        let catalog_bytes = loop {
            assert!(
                std::time::Instant::now() < deadline,
                "full plugin catalog timed out"
            );
            match event_rx.recv_timeout(Duration::from_secs(1)) {
                Ok(BackendEvent::Message(RpcEnvelope::Response(response)))
                    if response.id == catalog_id =>
                {
                    assert!(response.error.is_none(), "plugin/list failed: {response:?}");
                    break serde_json::to_vec(&response.result).unwrap().len();
                }
                Ok(BackendEvent::Disconnected(reason)) => {
                    panic!("managed transport disconnected during plugin/list: {reason}")
                }
                Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(error) => panic!("managed event channel failed: {error}"),
            }
        };
        assert!(
            catalog_bytes > 1024 * 1024,
            "live catalog did not exercise a large frame: {catalog_bytes} bytes"
        );
        eprintln!("full plugin catalog: {catalog_bytes} response bytes");

        let apps_id = RequestId::number(9_002);
        command_tx
            .send(HubCommand::Send(RpcEnvelope::request(
                apps_id.clone(),
                "app/list",
                json!({"limit": 100, "forceRefetch": true}),
            )))
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(45);
        let apps_bytes = loop {
            assert!(
                std::time::Instant::now() < deadline,
                "paged app catalog timed out"
            );
            match event_rx.recv_timeout(Duration::from_secs(1)) {
                Ok(BackendEvent::Message(RpcEnvelope::Response(response)))
                    if response.id == apps_id =>
                {
                    assert!(response.error.is_none(), "app/list failed: {response:?}");
                    break serde_json::to_vec(&response.result).unwrap().len();
                }
                Ok(BackendEvent::Disconnected(reason)) => {
                    panic!("managed transport disconnected during app/list: {reason}")
                }
                Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(error) => panic!("managed event channel failed: {error}"),
            }
        };
        eprintln!("paged app catalog: {apps_bytes} response bytes");

        let skills_id = RequestId::number(9_003);
        command_tx
            .send(HubCommand::Send(RpcEnvelope::request(
                skills_id.clone(),
                "skills/list",
                json!({"cwds": [env::current_dir().unwrap()]}),
            )))
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(45);
        let skills_bytes = loop {
            assert!(
                std::time::Instant::now() < deadline,
                "skills catalog timed out"
            );
            match event_rx.recv_timeout(Duration::from_secs(1)) {
                Ok(BackendEvent::Message(RpcEnvelope::Response(response)))
                    if response.id == skills_id =>
                {
                    assert!(response.error.is_none(), "skills/list failed: {response:?}");
                    break serde_json::to_vec(&response.result).unwrap().len();
                }
                Ok(BackendEvent::Disconnected(reason)) => {
                    panic!("managed transport disconnected during skills/list: {reason}")
                }
                Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(error) => panic!("managed event channel failed: {error}"),
            }
        };
        eprintln!("skills catalog: {skills_bytes} response bytes");

        let threads_id = RequestId::number(9_004);
        command_tx
            .send(HubCommand::Send(RpcEnvelope::request(
                threads_id.clone(),
                "thread/list",
                json!({
                    "limit": 100,
                    "archived": false,
                    "sortKey": "updated_at",
                    "sortDirection": "desc"
                }),
            )))
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(45);
        let threads_bytes = loop {
            assert!(
                std::time::Instant::now() < deadline,
                "thread list timed out"
            );
            match event_rx.recv_timeout(Duration::from_secs(1)) {
                Ok(BackendEvent::Message(RpcEnvelope::Response(response)))
                    if response.id == threads_id =>
                {
                    assert!(response.error.is_none(), "thread/list failed: {response:?}");
                    break serde_json::to_vec(&response.result).unwrap().len();
                }
                Ok(BackendEvent::Disconnected(reason)) => {
                    panic!("managed transport disconnected during thread/list: {reason}")
                }
                Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(error) => panic!("managed event channel failed: {error}"),
            }
        };
        eprintln!("thread list: {threads_bytes} response bytes");

        let agents_id = RequestId::number(9_005);
        command_tx
            .send(HubCommand::Send(RpcEnvelope::request(
                agents_id.clone(),
                "thread/list",
                json!({
                    "limit": 250,
                    "archived": false,
                    "sourceKinds": [
                        "subAgent",
                        "subAgentReview",
                        "subAgentCompact",
                        "subAgentThreadSpawn",
                        "subAgentOther"
                    ],
                    "sortKey": "updated_at",
                    "sortDirection": "desc"
                }),
            )))
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(45);
        let agents_bytes = loop {
            assert!(std::time::Instant::now() < deadline, "agent list timed out");
            match event_rx.recv_timeout(Duration::from_secs(1)) {
                Ok(BackendEvent::Message(RpcEnvelope::Response(response)))
                    if response.id == agents_id =>
                {
                    assert!(
                        response.error.is_none(),
                        "agent thread/list failed: {response:?}"
                    );
                    break serde_json::to_vec(&response.result).unwrap().len();
                }
                Ok(BackendEvent::Disconnected(reason)) => {
                    panic!("managed transport disconnected during agent list: {reason}")
                }
                Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(error) => panic!("managed event channel failed: {error}"),
            }
        };
        eprintln!("agent list: {agents_bytes} response bytes");

        if let Ok(thread_id) = env::var("CODEX_NATIVE_LIVE_THREAD_ID") {
            let resume_id = RequestId::number(9_006);
            command_tx
                .send(HubCommand::Send(RpcEnvelope::request(
                    resume_id.clone(),
                    "thread/resume",
                    json!({"threadId": thread_id, "excludeTurns": true}),
                )))
                .await
                .unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(45);
            let resume_bytes = loop {
                assert!(
                    std::time::Instant::now() < deadline,
                    "thread resume timed out"
                );
                match event_rx.recv_timeout(Duration::from_secs(1)) {
                    Ok(BackendEvent::Message(RpcEnvelope::Response(response)))
                        if response.id == resume_id =>
                    {
                        assert!(
                            response.error.is_none(),
                            "thread/resume failed: {response:?}"
                        );
                        break serde_json::to_vec(&response.result).unwrap().len();
                    }
                    Ok(BackendEvent::Disconnected(reason)) => {
                        panic!("managed transport disconnected during thread/resume: {reason}")
                    }
                    Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Err(error) => panic!("managed event channel failed: {error}"),
                }
            };
            eprintln!("thread resume: {resume_bytes} response bytes");

            let turns_id = RequestId::number(9_007);
            command_tx
                .send(HubCommand::Send(RpcEnvelope::request(
                    turns_id.clone(),
                    "thread/turns/list",
                    json!({
                        "threadId": thread_id,
                        "limit": 20,
                        "itemsView": "summary",
                        "sortDirection": "desc"
                    }),
                )))
                .await
                .unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(45);
            let (turns_bytes, turn_item_types) = loop {
                assert!(
                    std::time::Instant::now() < deadline,
                    "thread turns timed out"
                );
                match event_rx.recv_timeout(Duration::from_secs(1)) {
                    Ok(BackendEvent::Message(RpcEnvelope::Response(response)))
                        if response.id == turns_id =>
                    {
                        assert!(
                            response.error.is_none(),
                            "thread/turns/list failed: {response:?}"
                        );
                        let result = response.result.unwrap_or_else(|| json!({}));
                        let item_types = result
                            .get("data")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .map(|turn| {
                                turn.get("items")
                                    .and_then(Value::as_array)
                                    .into_iter()
                                    .flatten()
                                    .filter_map(|item| item.get("type").and_then(Value::as_str))
                                    .map(str::to_owned)
                                    .collect::<Vec<_>>()
                            })
                            .collect::<Vec<_>>();
                        break (serde_json::to_vec(&result).unwrap().len(), item_types);
                    }
                    Ok(BackendEvent::Disconnected(reason)) => {
                        panic!("managed transport disconnected during thread/turns/list: {reason}")
                    }
                    Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Err(error) => panic!("managed event channel failed: {error}"),
                }
            };
            eprintln!(
                "thread turns: {turns_bytes} response bytes; summary item types: {turn_item_types:?}"
            );

            let detail_id = RequestId::number(9_008);
            command_tx
                .send(HubCommand::Send(RpcEnvelope::request(
                    detail_id.clone(),
                    "thread/turns/list",
                    json!({
                        "threadId": thread_id,
                        "limit": 5,
                        "itemsView": "full",
                        "sortDirection": "desc"
                    }),
                )))
                .await
                .unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(45);
            loop {
                assert!(
                    std::time::Instant::now() < deadline,
                    "detailed thread page timed out"
                );
                match event_rx.recv_timeout(Duration::from_secs(1)) {
                    Ok(BackendEvent::Message(RpcEnvelope::Response(response)))
                        if response.id == detail_id =>
                    {
                        assert!(
                            response.error.is_none(),
                            "detailed thread/turns/list failed: {response:?}"
                        );
                        let bytes = serde_json::to_vec(&response.result).unwrap().len();
                        eprintln!("detailed thread page: {bytes} response bytes");
                        break;
                    }
                    Ok(BackendEvent::OversizedFrame(bytes)) => {
                        eprintln!("detailed thread page safely skipped: {bytes} response bytes");
                        break;
                    }
                    Ok(BackendEvent::Disconnected(reason)) => {
                        panic!("managed transport disconnected during detailed history: {reason}")
                    }
                    Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Err(error) => panic!("managed event channel failed: {error}"),
                }
            }

            let canonical_id = RequestId::number(9_009);
            command_tx
                .send(HubCommand::Send(RpcEnvelope::request(
                    canonical_id.clone(),
                    "thread/read",
                    json!({
                        "threadId": thread_id,
                        "includeTurns": true
                    }),
                )))
                .await
                .unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(45);
            loop {
                assert!(
                    std::time::Instant::now() < deadline,
                    "canonical thread/read timed out"
                );
                match event_rx.recv_timeout(Duration::from_secs(1)) {
                    Ok(BackendEvent::Message(RpcEnvelope::Response(response)))
                        if response.id == canonical_id =>
                    {
                        if let Some(error) = response.error {
                            if error.code == -32600 && error.message.contains("paginated threads") {
                                eprintln!(
                                    "canonical thread/read unavailable for paginated history: {}",
                                    error.message
                                );
                                break;
                            }
                            panic!("canonical thread/read failed: {error:?}");
                        }
                        let result = response.result.unwrap_or_else(|| json!({}));
                        let turns = result
                            .pointer("/thread/turns")
                            .and_then(Value::as_array)
                            .map_or(0, Vec::len);
                        let bytes = serde_json::to_vec(&result).unwrap().len();
                        eprintln!("canonical thread/read: {bytes} response bytes; {turns} turns");
                        break;
                    }
                    Ok(BackendEvent::OversizedFrame(bytes)) => {
                        panic!("canonical thread/read exceeded the frame limit: {bytes} bytes");
                    }
                    Ok(BackendEvent::Disconnected(reason)) => {
                        panic!("managed transport disconnected during canonical history: {reason}")
                    }
                    Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Err(error) => panic!("managed event channel failed: {error}"),
                }
            }
        }

        command_tx
            .send(HubCommand::Remote(RemoteAction::Refresh))
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            assert!(
                std::time::Instant::now() < deadline,
                "remote status timed out after plugin/list"
            );
            match event_rx.recv_timeout(Duration::from_secs(1)) {
                Ok(BackendEvent::RemoteResult {
                    action: RemoteAction::Refresh,
                    value,
                }) => {
                    assert_eq!(
                        value.get("status").and_then(Value::as_str),
                        Some("connected")
                    );
                    break;
                }
                Ok(BackendEvent::Disconnected(reason)) => {
                    panic!("managed transport disconnected after plugin/list: {reason}")
                }
                Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(error) => panic!("managed event channel failed: {error}"),
            }
        }
        command_tx.send(HubCommand::Shutdown).await.unwrap();
        assert!(matches!(
            transport.await.unwrap().unwrap(),
            ConnectionExit::Shutdown
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires the user's live managed remote-control daemon"]
    async fn live_managed_transport_survives_remote_health_cycles() {
        assert_eq!(
            env::var("CODEX_NATIVE_LIVE_REMOTE_SMOKE").as_deref(),
            Ok("1"),
            "set CODEX_NATIVE_LIVE_REMOTE_SMOKE=1 to run the live probe"
        );
        let socket_path = managed_primary_socket(None).expect("managed remote socket");
        let (command_tx, mut command_rx) = mpsc::channel(8);
        let (event_tx, event_rx) = sync_channel(64);
        let transport = tokio::spawn(async move {
            run_managed_transport(
                &socket_path,
                &PathBuf::from("codex"),
                None,
                &mut command_rx,
                &event_tx,
            )
            .await
        });
        assert!(matches!(
            event_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            BackendEvent::Connected
        ));
        assert!(matches!(
            event_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            BackendEvent::Ready(_)
        ));

        for cycle in 0..3 {
            command_tx
                .send(HubCommand::Remote(RemoteAction::Refresh))
                .await
                .unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            loop {
                assert!(
                    std::time::Instant::now() < deadline,
                    "remote status timed out"
                );
                match event_rx.recv_timeout(Duration::from_secs(1)) {
                    Ok(BackendEvent::RemoteResult {
                        action: RemoteAction::Refresh,
                        value,
                    }) => {
                        let status = value.get("status").and_then(Value::as_str);
                        eprintln!("remote health cycle {}: {status:?}", cycle + 1);
                        assert_eq!(status, Some("connected"));
                        break;
                    }
                    Ok(BackendEvent::Disconnected(reason)) => {
                        panic!("managed transport disconnected: {reason}")
                    }
                    Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Err(error) => panic!("managed event channel failed: {error}"),
                }
            }
            if cycle < 2 {
                sleep(Duration::from_secs(31)).await;
            }
        }
        command_tx.send(HubCommand::Shutdown).await.unwrap();
        assert!(matches!(
            transport.await.unwrap().unwrap(),
            ConnectionExit::Shutdown
        ));
    }

    #[test]
    fn proc_status_memory_fields_are_parsed_in_kibibytes() {
        let status = "Name:\tcodex\nVmRSS:\t 431812 kB\nVmSwap:\t1530020 kB\n";
        assert_eq!(proc_status_kib(status, "VmRSS:"), Some(431_812));
        assert_eq!(proc_status_kib(status, "VmSwap:"), Some(1_530_020));
        assert_eq!(proc_status_kib(status, "VmSize:"), None);
    }

    #[test]
    fn legacy_desktop_probe_matches_only_the_main_electron_process() {
        assert!(is_legacy_codex_desktop_command(
            b"/opt/codex-desktop/electron\0--app-id=codex-desktop\0"
        ));
        assert!(!is_legacy_codex_desktop_command(
            b"/opt/codex-desktop/electron\0--type=renderer\0"
        ));
        assert!(!is_legacy_codex_desktop_command(b"/usr/bin/codex-native\0"));
    }

    #[test]
    fn expired_remote_request_is_removed_and_reported() {
        let (events, receiver) = sync_channel(2);
        let id = RequestId::String("remote-health".into());
        let mut pending = HashMap::from([(
            id.clone(),
            ManagedRemoteRequest {
                request: ManagedRemotePending::RefreshStatus,
                sent_at: Instant::now() - MANAGED_REQUEST_TIMEOUT,
            },
        )]);
        expire_managed_remote_requests(&mut pending, Instant::now(), &events);
        assert!(!pending.contains_key(&id));
        assert!(matches!(
            receiver.try_recv().unwrap(),
            BackendEvent::RemoteError {
                action: RemoteAction::Refresh,
                ..
            }
        ));
    }

    #[test]
    fn managed_connection_staleness_uses_the_heartbeat_deadline() {
        let last_incoming = Instant::now();
        assert!(!managed_connection_is_stale(
            last_incoming,
            last_incoming + MANAGED_STALE_TIMEOUT - Duration::from_millis(1)
        ));
        assert!(managed_connection_is_stale(
            last_incoming,
            last_incoming + MANAGED_STALE_TIMEOUT
        ));
    }

    #[test]
    fn profile_watchdog_only_accepts_healthy_connected_hosts() {
        assert!(!profile_remote_watchdog_unhealthy(
            &json!({"status": "connected"})
        ));
        assert!(profile_remote_watchdog_unhealthy(
            &json!({"status": "connecting"})
        ));
        assert!(profile_remote_watchdog_unhealthy(
            &json!({"status": "disabled"})
        ));
        assert!(profile_remote_watchdog_unhealthy(&json!({
            "status": "connected",
            "localDaemon": {"overloaded": true}
        })));
    }

    #[test]
    fn ui_event_drain_obeys_its_per_tick_budget() {
        let (command_tx, _command_rx) = mpsc::channel(1);
        let (event_tx, event_rx) = sync_channel(8);
        for index in 0..5 {
            event_tx.send(BackendEvent::Log(index.to_string())).unwrap();
        }
        let hub = AppServerHub {
            command_tx,
            events: Arc::new(std::sync::Mutex::new(event_rx)),
            next_id: Arc::new(AtomicU64::new(1)),
            managed_transport: Arc::new(AtomicBool::new(false)),
            _runtime: Arc::new(Runtime::new().unwrap()),
        };
        let mut first = Vec::new();
        hub.drain_events(2, |event| first.push(event));
        assert_eq!(first.len(), 2);
        let mut second = Vec::new();
        hub.drain_events(8, |event| second.push(event));
        assert_eq!(second.len(), 3);
    }

    #[test]
    fn dropped_ui_event_reports_are_logarithmically_bounded() {
        let reports = (1..=7_523)
            .filter(|count| should_report_drop(*count))
            .count();

        assert_eq!(reports, 13);
        assert!(should_report_drop(1));
        assert!(should_report_drop(4_096));
        assert!(!should_report_drop(3));
        assert!(!should_report_drop(7_523));
    }

    #[test]
    fn dropped_ui_event_counter_saturates_without_wrapping() {
        let counter = AtomicU64::new(u64::MAX - 1);

        assert_eq!(increment_saturating(&counter), u64::MAX);
        assert_eq!(increment_saturating(&counter), u64::MAX);
        assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
    }
}
