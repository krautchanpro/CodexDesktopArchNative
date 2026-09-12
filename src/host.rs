use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    env,
    ffi::OsString,
    fs,
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    net::{TcpStream, ToSocketAddrs},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    time::{Duration, UNIX_EPOCH},
};

use anyhow::{Context, anyhow, bail};
use base64::Engine;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::context::ContextCheckpointRequest;

const MAX_COMMAND_OUTPUT: usize = 2 * 1024 * 1024;
const MAX_LOCAL_ROLLOUT_CHAT_BYTES: u64 = 128 * 1024 * 1024;
const MAX_LOCAL_ROLLOUT_RECORD_BYTES: usize = 4 * 1024 * 1024;
const MAX_LOCAL_CHAT_ITEM_BYTES: usize = 512 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostAction {
    AppShotCapture,
    RecordStart {
        title: String,
    },
    RecordFrame {
        session_dir: PathBuf,
        note: String,
    },
    RecordStop {
        session_dir: PathBuf,
    },
    RecordInstall {
        session_dir: PathBuf,
    },
    BrowserOpen {
        url: String,
        isolated_profile: bool,
        preferred_binary: Option<PathBuf>,
    },
    OpenEditor {
        path: PathBuf,
        preferred_binary: Option<PathBuf>,
    },
    Reveal(PathBuf),
    LoadRolloutChat {
        thread_id: String,
        path: PathBuf,
    },
    MemoriaJobs,
    Diagnostics,
    UpdateCheck,
    UpdateApply,
    UpdateRollback(PathBuf),
    RemoteDoctor {
        configured_binary: Option<PathBuf>,
    },
    RemoteDoctorProfile {
        configured_binary: Option<PathBuf>,
        profile_home: Option<PathBuf>,
    },
    RemoteRestart {
        configured_binary: Option<PathBuf>,
    },
    RemoteRestartProfile {
        configured_binary: Option<PathBuf>,
        profile_home: Option<PathBuf>,
    },
    RemoteAutostart {
        enabled: bool,
    },
    WorkspaceDoctor {
        cwd: PathBuf,
    },
    WorkspaceLaunch {
        cwd: PathBuf,
        additional_paths: Vec<PathBuf>,
        network: bool,
        memory_mib: u32,
    },
    WorkspaceStop {
        unit: String,
    },
    QwenTerminalLaunch {
        cwd: PathBuf,
    },
    BuddyDelegate {
        thread_id: String,
        turn_id: String,
        backend: String,
        prompt: String,
        context: String,
        effort: String,
        cwd: PathBuf,
        access: String,
    },
    QwenRefresh,
    ContextCheckpoint {
        request: Box<ContextCheckpointRequest>,
        compact_after: bool,
        automatic: bool,
    },
    ContextDelete {
        thread_id: String,
    },
    VoiceCapture {
        seconds: u8,
    },
    Speak(String),
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuddyProgress {
    pub phase: String,
    pub detail: String,
    #[serde(default)]
    pub model_calls: u64,
    #[serde(default)]
    pub prompt_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
    #[serde(default)]
    pub context_used_tokens: u64,
    #[serde(default)]
    pub context_window_tokens: u64,
    #[serde(default)]
    pub tokens_per_second: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticCheck {
    pub name: String,
    pub status: String,
    pub detail: String,
    pub remediation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessInfo {
    pub pid: u32,
    pub parent_pid: u32,
    pub name: String,
    pub rss_kib: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsReport {
    pub generated_at: DateTime<Utc>,
    pub application_version: String,
    pub client_pid: u32,
    pub client_rss_kib: u64,
    pub client_peak_rss_kib: u64,
    pub descendants_rss_kib: u64,
    pub descendants: Vec<ProcessInfo>,
    pub session_type: String,
    pub desktop: String,
    pub checks: Vec<DiagnosticCheck>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RecordingSession {
    id: String,
    title: String,
    started_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
    directory: PathBuf,
    frames: Vec<RecordedFrame>,
    draft_skill: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RecordedFrame {
    captured_at: DateTime<Utc>,
    screenshot: PathBuf,
    capture_tool: String,
    note: String,
    accessibility_available: bool,
}

pub fn execute(action: &HostAction) -> anyhow::Result<Value> {
    match action {
        HostAction::AppShotCapture => capture_appshot(),
        HostAction::RecordStart { title } => record_start(title),
        HostAction::RecordFrame { session_dir, note } => record_frame(session_dir, note),
        HostAction::RecordStop { session_dir } => record_stop(session_dir),
        HostAction::RecordInstall { session_dir } => record_install(session_dir),
        HostAction::BrowserOpen {
            url,
            isolated_profile,
            preferred_binary,
        } => open_browser(url, *isolated_profile, preferred_binary.as_deref()),
        HostAction::OpenEditor {
            path,
            preferred_binary,
        } => open_editor(path, preferred_binary.as_deref()),
        HostAction::Reveal(path) => reveal(path),
        HostAction::LoadRolloutChat { thread_id, path } => load_rollout_chat(thread_id, path),
        HostAction::MemoriaJobs => memoria_jobs(),
        HostAction::Diagnostics => serde_json::to_value(diagnostics()?).map_err(Into::into),
        HostAction::UpdateCheck => update_check(),
        HostAction::UpdateApply => update_apply(),
        HostAction::UpdateRollback(package) => update_rollback(package),
        HostAction::RemoteDoctor { configured_binary } => {
            remote_doctor(configured_binary.as_deref())
        }
        HostAction::RemoteDoctorProfile {
            configured_binary,
            profile_home,
        } => remote_doctor_for_profile(configured_binary.as_deref(), profile_home.as_deref()),
        HostAction::RemoteRestart { configured_binary } => {
            remote_restart(configured_binary.as_deref())
        }
        HostAction::RemoteRestartProfile {
            configured_binary,
            profile_home,
        } => remote_restart_for_profile(configured_binary.as_deref(), profile_home.as_deref()),
        HostAction::RemoteAutostart { enabled } => configure_remote_autostart(*enabled),
        HostAction::WorkspaceDoctor { cwd } => workspace_doctor(cwd),
        HostAction::WorkspaceLaunch {
            cwd,
            additional_paths,
            network,
            memory_mib,
        } => workspace_launch(cwd, additional_paths, *network, *memory_mib),
        HostAction::WorkspaceStop { unit } => workspace_stop(unit),
        HostAction::QwenTerminalLaunch { cwd } => qwen_terminal_launch(cwd),
        HostAction::BuddyDelegate {
            backend,
            prompt,
            context,
            effort,
            cwd,
            access,
            ..
        } => buddy_delegate(backend, prompt, context, effort, cwd, access, &mut |_| {}),
        HostAction::QwenRefresh => crate::qwen::inspect(),
        HostAction::ContextCheckpoint { request, .. } => {
            serde_json::to_value(crate::context::create_checkpoint(request)?).map_err(Into::into)
        }
        HostAction::ContextDelete { thread_id } => {
            crate::context::delete_thread_checkpoints(thread_id)?;
            Ok(json!({"deleted": true}))
        }
        HostAction::VoiceCapture { seconds } => capture_voice(*seconds),
        HostAction::Speak(text) => speak(text),
    }
}

pub fn execute_buddy_with_progress(
    action: &HostAction,
    mut on_progress: impl FnMut(BuddyProgress),
) -> anyhow::Result<Value> {
    let HostAction::BuddyDelegate {
        backend,
        prompt,
        context,
        effort,
        cwd,
        access,
        ..
    } = action
    else {
        bail!("progress execution requires a Buddy delegate action");
    };
    buddy_delegate(
        backend,
        prompt,
        context,
        effort,
        cwd,
        access,
        &mut on_progress,
    )
}

const MEMORIA_STATUS_MAX_BYTES: u64 = 1024 * 1024;
const MEMORIA_STATUS_TIMEOUT: Duration = Duration::from_millis(800);

fn memoria_jobs() -> anyhow::Result<Value> {
    let (host, port) = memoria_daemon_endpoint()?;
    let address = (host.as_str(), port)
        .to_socket_addrs()
        .context("could not resolve the Memoria daemon address")?
        .next()
        .ok_or_else(|| anyhow!("the Memoria daemon address did not resolve"))?;
    let mut stream = TcpStream::connect_timeout(&address, MEMORIA_STATUS_TIMEOUT)
        .context("could not connect to the Memoria daemon")?;
    stream.set_read_timeout(Some(MEMORIA_STATUS_TIMEOUT))?;
    stream.set_write_timeout(Some(MEMORIA_STATUS_TIMEOUT))?;
    let host_header = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    write!(
        stream,
        "GET /api/status HTTP/1.1\r\nHost: {host_header}\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
    )?;

    let mut response = Vec::new();
    stream
        .take(MEMORIA_STATUS_MAX_BYTES + 1)
        .read_to_end(&mut response)
        .context("could not read Memoria status")?;
    if response.len() as u64 > MEMORIA_STATUS_MAX_BYTES {
        bail!("Memoria status exceeded the 1 MiB safety limit");
    }
    let body_offset = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|offset| offset + 4)
        .ok_or_else(|| anyhow!("Memoria returned an invalid HTTP response"))?;
    let status_line = response
        .split(|byte| *byte == b'\n')
        .next()
        .and_then(|line| std::str::from_utf8(line).ok())
        .unwrap_or_default();
    if !status_line.contains(" 200 ") {
        bail!("Memoria status request failed: {}", status_line.trim());
    }
    let status: Value = serde_json::from_slice(&response[body_offset..])
        .context("Memoria returned invalid status JSON")?;
    Ok(memoria_job_summary(&status))
}

fn memoria_daemon_endpoint() -> anyhow::Result<(String, u16)> {
    if let Some(origin) = env::var_os("MEMORIA_DAEMON_URL") {
        let origin = origin.to_string_lossy();
        let authority = origin
            .trim()
            .strip_prefix("http://")
            .ok_or_else(|| anyhow!("MEMORIA_DAEMON_URL must use http for the local status badge"))?
            .trim_end_matches('/');
        if authority.contains('/') || authority.contains('@') || authority.is_empty() {
            bail!("MEMORIA_DAEMON_URL must point to a local daemon origin");
        }
        if let Some(bracketed) = authority.strip_prefix('[') {
            let end = bracketed
                .find(']')
                .ok_or_else(|| anyhow!("MEMORIA_DAEMON_URL contains an invalid IPv6 host"))?;
            let host = &bracketed[..end];
            let suffix = &bracketed[end + 1..];
            let port = suffix.strip_prefix(':').unwrap_or("3850").parse::<u16>()?;
            return Ok((host.into(), port));
        }
        let (host, port) = authority
            .rsplit_once(':')
            .map_or((authority, 3850), |(host, port)| {
                (host, port.parse::<u16>().unwrap_or(0))
            });
        if host.is_empty() || port == 0 {
            bail!("MEMORIA_DAEMON_URL contains an invalid host or port");
        }
        return Ok((host.into(), port));
    }

    let host = env::var("MEMORIA_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port = env::var("MEMORIA_PORT")
        .ok()
        .map(|port| port.parse::<u16>())
        .transpose()
        .context("MEMORIA_PORT must be an integer between 1 and 65535")?
        .unwrap_or(3850);
    Ok((host, port))
}

fn memoria_job_summary(status: &Value) -> Value {
    let count = |pointer: &str| status.pointer(pointer).and_then(Value::as_u64).unwrap_or(0);
    let memory_pending = count("/pipeline/queue/memory/pending");
    let memory_active = count("/pipeline/queue/memory/leased");
    let summary_pending = count("/pipeline/queue/summary/pending");
    let summary_active = count("/pipeline/queue/summary/leased");
    let transcript_pending = count("/transcripts/capture/pending");
    let transcript_active = count("/transcripts/capture/processing");
    let pending = memory_pending + summary_pending + transcript_pending;
    let active = memory_active + summary_active + transcript_active;
    json!({
        "count": pending + active,
        "pending": pending,
        "active": active,
        "memory": {"pending": memory_pending, "active": memory_active},
        "summary": {"pending": summary_pending, "active": summary_active},
        "transcripts": {"pending": transcript_pending, "active": transcript_active}
    })
}

fn safe_slug(value: &str) -> String {
    let slug = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    let slug = slug.trim_matches('-');
    if slug.is_empty() {
        "workspace".into()
    } else {
        slug.chars().take(80).collect()
    }
}

fn capture_appshot() -> anyhow::Result<Value> {
    let directory = dirs::cache_dir()
        .context("XDG cache directory is unavailable")?
        .join("codex-native/appshots");
    fs::create_dir_all(&directory)?;
    let path = directory.join(format!(
        "appshot-{}.png",
        Utc::now().format("%Y%m%dT%H%M%S%.3fZ")
    ));

    let tool = capture_screenshot(&path)?;
    let metadata = fs::metadata(&path)
        .with_context(|| format!("screenshot tool did not create {}", path.display()))?;
    if metadata.len() == 0 {
        bail!("screenshot utility created an empty file");
    }
    Ok(json!({
        "path": path,
        "tool": tool,
        "capturedAt": Utc::now(),
        "bytes": metadata.len(),
        "accessibility": {
            "available": env::var_os("AT_SPI_BUS_ADDRESS").is_some(),
            "source": "computer-use MCP",
            "note": "Element context is supplied by the Linux Computer Use accessibility backend when enabled."
        }
    }))
}

fn capture_screenshot(path: &Path) -> anyhow::Result<String> {
    let (tool, output) = if let Some(binary) = find_programs(&["spectacle"]) {
        let mut command = Command::new(binary);
        command.args(["-b", "-n", "-a", "-o"]).arg(path);
        (
            "spectacle",
            checked_output(&mut command, "capture active window")?,
        )
    } else if let Some(binary) = find_programs(&["gnome-screenshot"]) {
        let mut command = Command::new(binary);
        command.args(["-w", "-f"]).arg(path);
        (
            "gnome-screenshot",
            checked_output(&mut command, "capture active window")?,
        )
    } else if let Some(binary) = find_programs(&["scrot"]) {
        let mut command = Command::new(binary);
        command.arg("-u").arg(path);
        (
            "scrot",
            checked_output(&mut command, "capture active window")?,
        )
    } else if let Some(binary) = find_programs(&["grim"]) {
        let mut command = Command::new(binary);
        command.arg(path);
        ("grim", checked_output(&mut command, "capture desktop")?)
    } else {
        bail!(
            "no supported screenshot utility found; install spectacle, gnome-screenshot, scrot, or grim"
        );
    };
    drop(output);
    Ok(tool.into())
}

fn recordings_root() -> anyhow::Result<PathBuf> {
    Ok(dirs::state_dir()
        .or_else(dirs::data_local_dir)
        .context("XDG state directory is unavailable")?
        .join("codex-native/recordings"))
}

fn record_start(title: &str) -> anyhow::Result<Value> {
    let title = title.trim();
    if title.is_empty() {
        bail!("recording title cannot be empty");
    }
    let root = recordings_root()?;
    fs::create_dir_all(&root)?;
    let id = format!(
        "{}-{}",
        Utc::now().format("%Y%m%dT%H%M%SZ"),
        safe_slug(title)
    );
    let mut directory = root.join(&id);
    let mut suffix = 2;
    while directory.exists() {
        directory = root.join(format!("{id}-{suffix}"));
        suffix += 1;
    }
    fs::create_dir_all(directory.join("evidence"))?;
    let session = RecordingSession {
        id,
        title: title.into(),
        started_at: Utc::now(),
        completed_at: None,
        directory,
        frames: Vec::new(),
        draft_skill: None,
    };
    save_recording(&session)?;
    serde_json::to_value(session).map_err(Into::into)
}

fn record_frame(session_dir: &Path, note: &str) -> anyhow::Result<Value> {
    let mut session = load_recording(session_dir)?;
    if session.completed_at.is_some() {
        bail!("recording is already complete");
    }
    if session.frames.len() >= 200 {
        bail!("recording reached the 200-frame safety limit");
    }
    let used_bytes = session
        .frames
        .iter()
        .filter_map(|frame| fs::metadata(&frame.screenshot).ok())
        .map(|metadata| metadata.len())
        .sum::<u64>();
    if used_bytes >= 256 * 1024 * 1024 {
        bail!("recording reached the 256 MiB evidence limit");
    }
    let path = session.directory.join("evidence").join(format!(
        "frame-{:04}-{}.png",
        session.frames.len() + 1,
        Utc::now().format("%H%M%S%.3f")
    ));
    let tool = capture_screenshot(&path)?;
    let metadata = fs::metadata(&path)?;
    if metadata.len() == 0 {
        bail!("screenshot utility created an empty recording frame");
    }
    session.frames.push(RecordedFrame {
        captured_at: Utc::now(),
        screenshot: path,
        capture_tool: tool,
        note: note.trim().to_owned(),
        accessibility_available: env::var_os("AT_SPI_BUS_ADDRESS").is_some(),
    });
    save_recording(&session)?;
    serde_json::to_value(session).map_err(Into::into)
}

fn record_stop(session_dir: &Path) -> anyhow::Result<Value> {
    let mut session = load_recording(session_dir)?;
    session.completed_at = Some(Utc::now());
    let skill_path = session.directory.join("SKILL.md");
    let mut skill = format!(
        "---\nname: replay-{}\ndescription: Reproduce the recorded workflow: {}\n---\n\n# {}\n\nUse semantic UI state and the attached evidence. Do not replay absolute pointer coordinates. Re-check the current interface before each action and ask before consequential changes.\n\n## Recorded workflow\n\n",
        safe_slug(&session.title),
        session.title,
        session.title
    );
    if session.frames.is_empty() {
        skill.push_str("No evidence frames were captured. Add explicit workflow steps before using this skill.\n");
    } else {
        for (index, frame) in session.frames.iter().enumerate() {
            let relative = frame
                .screenshot
                .strip_prefix(&session.directory)
                .unwrap_or(&frame.screenshot);
            let note = if frame.note.is_empty() {
                "Inspect this state and infer the next semantic action."
            } else {
                &frame.note
            };
            skill.push_str(&format!(
                "{}. {} Evidence: `{}`.\n",
                index + 1,
                note,
                relative.display()
            ));
        }
    }
    fs::write(&skill_path, skill)
        .with_context(|| format!("failed to write {}", skill_path.display()))?;
    session.draft_skill = Some(skill_path);
    save_recording(&session)?;
    serde_json::to_value(session).map_err(Into::into)
}

fn record_install(session_dir: &Path) -> anyhow::Result<Value> {
    let session = load_recording(session_dir)?;
    let skill = session
        .draft_skill
        .as_ref()
        .filter(|path| path.is_file())
        .context("stop the recording and review its draft skill before installing")?;
    let skills_root = dirs::home_dir()
        .context("home directory is unavailable")?
        .join(".codex/skills");
    fs::create_dir_all(&skills_root)?;
    let slug = format!("replay-{}", safe_slug(&session.title));
    let mut target = skills_root.join(&slug);
    let mut suffix = 2;
    while target.exists() {
        target = skills_root.join(format!("{slug}-{suffix}"));
        suffix += 1;
    }
    fs::create_dir_all(target.join("evidence"))?;
    fs::copy(skill, target.join("SKILL.md"))?;
    for frame in &session.frames {
        if let Some(name) = frame.screenshot.file_name() {
            fs::copy(&frame.screenshot, target.join("evidence").join(name))?;
        }
    }
    Ok(json!({"installed": true, "path": target, "frames": session.frames.len()}))
}

fn load_recording(session_dir: &Path) -> anyhow::Result<RecordingSession> {
    let root = recordings_root()?;
    let root = root
        .canonicalize()
        .with_context(|| format!("recordings directory does not exist: {}", root.display()))?;
    let directory = session_dir
        .canonicalize()
        .with_context(|| format!("recording does not exist: {}", session_dir.display()))?;
    if !directory.starts_with(&root) {
        bail!("recording directory is outside Codex Native state");
    }
    let bytes = fs::read(directory.join("session.json"))?;
    let session = serde_json::from_slice::<RecordingSession>(&bytes)?;
    if session.directory.canonicalize().ok().as_ref() != Some(&directory) {
        bail!("recording manifest directory does not match its location");
    }
    Ok(session)
}

fn save_recording(session: &RecordingSession) -> anyhow::Result<()> {
    let path = session.directory.join("session.json");
    let temporary = session.directory.join("session.json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(session)?)?;
    fs::rename(&temporary, &path)?;
    Ok(())
}

fn open_browser(
    url: &str,
    isolated_profile: bool,
    preferred_binary: Option<&Path>,
) -> anyhow::Result<Value> {
    if !matches!(url.split(':').next(), Some("http" | "https" | "about")) {
        bail!("only http, https, and about URLs may be opened");
    }
    if isolated_profile {
        let binary = preferred_binary
            .filter(|path| path.is_file())
            .map(Path::to_path_buf)
            .or_else(|| {
                find_programs(&[
                    "google-chrome-stable",
                    "google-chrome",
                    "chromium",
                    "brave",
                    "brave-browser",
                    "thorium-browser",
                ])
            })
            .context("no supported external Chromium browser was found")?;
        let profile = dirs::config_dir()
            .context("XDG config directory is unavailable")?
            .join("codex-native/browser-profile");
        fs::create_dir_all(&profile)?;
        Command::new(&binary)
            .arg(format!("--user-data-dir={}", profile.display()))
            .args(["--no-first-run", "--new-window", url])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("failed to launch {}", binary.display()))?;
        Ok(json!({"opened": true, "binary": binary, "profile": profile, "url": url}))
    } else {
        let binary = find_programs(&["xdg-open"]).context("xdg-open is unavailable")?;
        Command::new(&binary)
            .arg(url)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("failed to open the system browser")?;
        Ok(json!({"opened": true, "binary": binary, "url": url}))
    }
}

fn open_editor(path: &Path, preferred_binary: Option<&Path>) -> anyhow::Result<Value> {
    if !path.exists() {
        bail!("target does not exist: {}", path.display());
    }
    let binary = preferred_binary
        .filter(|value| value.is_file())
        .map(Path::to_path_buf)
        .or_else(|| find_programs(&["code", "codium", "zed", "kate", "gnome-text-editor"]))
        .context("no supported editor was found; configure one in Settings")?;
    Command::new(&binary)
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("failed to launch {}", binary.display()))?;
    Ok(json!({"opened": true, "binary": binary, "path": path}))
}

fn reveal(path: &Path) -> anyhow::Result<Value> {
    if !path.exists() {
        bail!("target does not exist: {}", path.display());
    }
    let target = if path.is_file() {
        path.parent().unwrap_or(path)
    } else {
        path
    };
    let binary = find_programs(&["xdg-open"]).context("xdg-open is unavailable")?;
    Command::new(&binary)
        .arg(target)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to open the file manager")?;
    Ok(json!({"opened": true, "path": target}))
}

fn diagnostics() -> anyhow::Result<DiagnosticsReport> {
    let pid = std::process::id();
    let (rss, peak) = proc_memory(pid).unwrap_or_default();
    let descendants = descendant_processes(pid);
    let descendants_rss_kib = descendants.iter().map(|item| item.rss_kib).sum();
    let mut checks = Vec::new();
    for (name, candidates, remediation) in [
        (
            "Codex CLI",
            &["codex"][..],
            "Install the current Codex CLI with the system package manager.",
        ),
        (
            "Desktop portal",
            &["xdg-desktop-portal"][..],
            "Install xdg-desktop-portal and the backend for your desktop.",
        ),
        (
            "Screenshot backend",
            &["spectacle", "gnome-screenshot", "scrot", "grim"][..],
            "Install a supported screenshot utility for AppShots.",
        ),
        (
            "External browser",
            &[
                "google-chrome-stable",
                "chromium",
                "brave",
                "thorium-browser",
            ][..],
            "Install Chrome, Chromium, Brave, or Thorium for Browser tools.",
        ),
        (
            "Editor",
            &["code", "codium", "zed", "kate", "gnome-text-editor"][..],
            "Install or configure a supported editor.",
        ),
    ] {
        if let Some(path) = find_programs(candidates) {
            checks.push(DiagnosticCheck {
                name: name.into(),
                status: "ready".into(),
                detail: path.display().to_string(),
                remediation: None,
            });
        } else {
            checks.push(DiagnosticCheck {
                name: name.into(),
                status: "missing".into(),
                detail: "Not found in PATH".into(),
                remediation: Some(remediation.into()),
            });
        }
    }
    checks.push(DiagnosticCheck {
        name: "AT-SPI accessibility".into(),
        status: if env::var_os("AT_SPI_BUS_ADDRESS").is_some() {
            "ready"
        } else {
            "limited"
        }
        .into(),
        detail: env::var("AT_SPI_BUS_ADDRESS").unwrap_or_else(|_| "Bus not exported".into()),
        remediation: env::var_os("AT_SPI_BUS_ADDRESS").is_none().then(|| {
            "Enable desktop accessibility before using element-aware Computer Use.".into()
        }),
    });
    if let Some(history) = task_history_storage() {
        checks.push(DiagnosticCheck {
            name: "Task history storage".into(),
            status: if history.large_plain_files > 0 {
                "optimizing"
            } else {
                "ready"
            }
            .into(),
            detail: format!(
                "{} rollouts · {} plain · {} compressed · {} total · {} currently open",
                history.rollout_files,
                history.plain_files,
                history.compressed_files,
                human_bytes(history.total_bytes),
                history.open_files
            ),
            remediation: Some(format!(
                "{} large plain histories remain. New tasks use paginated storage; stock Codex keeps legacy histories readable while Native serves bounded pages. Archive preserves a task; Delete removes its canonical history.",
                history.large_plain_files
            )),
        });
    }
    Ok(DiagnosticsReport {
        generated_at: Utc::now(),
        application_version: env!("CARGO_PKG_VERSION").into(),
        client_pid: pid,
        client_rss_kib: rss,
        client_peak_rss_kib: peak,
        descendants_rss_kib,
        descendants,
        session_type: env::var("XDG_SESSION_TYPE").unwrap_or_else(|_| "unknown".into()),
        desktop: env::var("XDG_CURRENT_DESKTOP").unwrap_or_else(|_| "unknown".into()),
        checks,
    })
}

#[derive(Default)]
struct TaskHistoryStorage {
    rollout_files: u64,
    plain_files: u64,
    compressed_files: u64,
    large_plain_files: u64,
    total_bytes: u64,
    open_files: u64,
}

fn task_history_storage() -> Option<TaskHistoryStorage> {
    let codex_home = env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|home| home.join(".codex")))?;
    let open = crate::activity::open_app_server_rollout_paths();
    let mut report = TaskHistoryStorage::default();
    let mut stack = vec![
        codex_home.join("sessions"),
        codex_home.join("archived_sessions"),
    ];
    while let Some(directory) = stack.pop() {
        let Ok(entries) = fs::read_dir(directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let plain = name.starts_with("rollout-") && name.ends_with(".jsonl");
            let compressed = name.starts_with("rollout-") && name.ends_with(".jsonl.zst");
            if !plain && !compressed {
                continue;
            }
            let bytes = entry.metadata().map(|metadata| metadata.len()).unwrap_or(0);
            report.rollout_files = report.rollout_files.saturating_add(1);
            report.total_bytes = report.total_bytes.saturating_add(bytes);
            if plain {
                report.plain_files = report.plain_files.saturating_add(1);
                if bytes > 64 * 1024 * 1024 {
                    report.large_plain_files = report.large_plain_files.saturating_add(1);
                }
            } else {
                report.compressed_files = report.compressed_files.saturating_add(1);
            }
            if open.contains(&path) {
                report.open_files = report.open_files.saturating_add(1);
            }
        }
    }
    Some(report)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RolloutChatTurn {
    id: String,
    items: Vec<Value>,
    status: Value,
    error: Option<Value>,
    started_at: Option<i64>,
    completed_at: Option<i64>,
    items_view: String,
}

impl RolloutChatTurn {
    fn new(id: String) -> Self {
        Self {
            id,
            items: Vec::new(),
            status: json!("inProgress"),
            error: None,
            started_at: None,
            completed_at: None,
            items_view: "localRecovery".into(),
        }
    }
}

fn load_rollout_chat(thread_id: &str, path: &Path) -> anyhow::Result<Value> {
    if thread_id.len() != 36
        || !thread_id
            .chars()
            .all(|character| character.is_ascii_hexdigit() || character == '-')
    {
        bail!("invalid rollout thread ID");
    }
    let canonical = fs::canonicalize(path)
        .with_context(|| format!("failed to resolve rollout {}", path.display()))?;
    let file_name = canonical
        .file_name()
        .and_then(|name| name.to_str())
        .context("rollout filename is not UTF-8")?;
    if !file_name.starts_with("rollout-")
        || !file_name.ends_with(".jsonl")
        || !file_name.contains(thread_id)
    {
        bail!("rollout filename does not match the selected task");
    }
    let codex_home = env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|home| home.join(".codex")))
        .context("Codex home is unavailable")?;
    let allowed = ["sessions", "archived_sessions"].into_iter().any(|name| {
        fs::canonicalize(codex_home.join(name))
            .ok()
            .is_some_and(|root| canonical.starts_with(root))
    });
    if !allowed {
        bail!("rollout is outside the Codex session directories");
    }
    let metadata = fs::metadata(&canonical)
        .with_context(|| format!("failed to inspect rollout {}", canonical.display()))?;
    let file = fs::File::open(&canonical)
        .with_context(|| format!("failed to open rollout {}", canonical.display()))?;
    let (reader, scan_start_byte) =
        bounded_rollout_chat_reader(file, metadata.len(), MAX_LOCAL_ROLLOUT_CHAT_BYTES)?;
    let (turns, last_ordinal) = parse_rollout_chat(thread_id, reader)?;
    let projection_repair = if scan_start_byte == 0 {
        crate::history::repair_projection(thread_id, &canonical, &codex_home, metadata.len())
            .unwrap_or_else(|error| {
                tracing::warn!(
                    %thread_id,
                    path = %canonical.display(),
                    %error,
                    "shared Codex history projection repair was safely skipped"
                );
                crate::history::ProjectionRepair::skipped(error.to_string())
            })
    } else {
        crate::history::ProjectionRepair::skipped(format!(
            "oversized rollout recovered from a bounded {} MiB tail",
            MAX_LOCAL_ROLLOUT_CHAT_BYTES / (1024 * 1024)
        ))
    };
    let modified = metadata
        .modified()
        .ok()
        .and_then(|value| value.duration_since(UNIX_EPOCH).ok());
    Ok(json!({
        "threadId": thread_id,
        "path": canonical,
        "size": metadata.len(),
        "modifiedSeconds": modified.map(|value| value.as_secs()).unwrap_or_default(),
        "modifiedNanos": modified.map(|value| value.subsec_nanos()).unwrap_or_default(),
        "scanStartByte": scan_start_byte,
        "lastOrdinal": last_ordinal,
        "turns": turns,
        "projectionRepair": projection_repair
    }))
}

fn bounded_rollout_chat_reader(
    mut file: fs::File,
    size: u64,
    max_bytes: u64,
) -> anyhow::Result<(BufReader<fs::File>, u64)> {
    let requested_start = size.saturating_sub(max_bytes);
    let starts_on_record_boundary = if requested_start == 0 {
        true
    } else {
        file.seek(SeekFrom::Start(requested_start - 1))?;
        let mut previous = [0_u8; 1];
        file.read_exact(&mut previous)?;
        previous[0] == b'\n'
    };
    file.seek(SeekFrom::Start(requested_start))?;
    let mut reader = BufReader::new(file);
    if !starts_on_record_boundary {
        discard_partial_rollout_record(&mut reader)?;
    }
    let scan_start = reader.stream_position()?;
    Ok((reader, scan_start))
}

fn discard_partial_rollout_record(reader: &mut impl BufRead) -> std::io::Result<()> {
    loop {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            return Ok(());
        }
        if let Some(index) = memchr::memchr(b'\n', buffer) {
            reader.consume(index + 1);
            return Ok(());
        }
        let consumed = buffer.len();
        reader.consume(consumed);
    }
}

fn read_bounded_rollout_record(
    reader: &mut impl BufRead,
    record: &mut Vec<u8>,
    max_bytes: usize,
) -> std::io::Result<Option<bool>> {
    record.clear();
    let mut saw_bytes = false;
    let mut oversized = false;
    loop {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            return Ok(saw_bytes.then_some(!oversized));
        }
        saw_bytes = true;
        let newline = memchr::memchr(b'\n', buffer);
        let consumed = newline.map_or(buffer.len(), |index| index + 1);
        let content_end = newline.unwrap_or(buffer.len());
        if !oversized {
            if record.len().saturating_add(content_end) <= max_bytes {
                record.extend_from_slice(&buffer[..content_end]);
            } else {
                oversized = true;
                record.clear();
            }
        }
        reader.consume(consumed);
        if newline.is_some() {
            if record.last() == Some(&b'\r') {
                record.pop();
            }
            return Ok(Some(!oversized));
        }
    }
}

fn parse_rollout_chat(
    expected_thread_id: &str,
    mut reader: impl BufRead,
) -> anyhow::Result<(Vec<RolloutChatTurn>, u64)> {
    let mut turns = BTreeMap::<String, RolloutChatTurn>::new();
    let mut order = Vec::<String>::new();
    let mut last_ordinal = 0_u64;
    let mut record_bytes = Vec::new();
    while let Some(retained) = read_bounded_rollout_record(
        &mut reader,
        &mut record_bytes,
        MAX_LOCAL_ROLLOUT_RECORD_BYTES,
    )
    .context("failed to read rollout line")?
    {
        if !retained {
            continue;
        }
        let Ok(line) = std::str::from_utf8(&record_bytes) else {
            continue;
        };
        if !(line.contains("\"type\":\"task_started\"")
            || line.contains("\"type\":\"task_complete\"")
            || (line.contains("\"type\":\"item_completed\"")
                && (line.contains("\"type\":\"UserMessage\"")
                    || line.contains("\"type\":\"AgentMessage\"")))
            || (line.contains("\"type\":\"response_item\"")
                && line.contains("\"type\":\"message\"")
                && (line.contains("\"role\":\"user\"") || line.contains("\"role\":\"assistant\""))))
        {
            continue;
        }
        let record: Value = match serde_json::from_str(line) {
            Ok(record) => record,
            Err(_) => continue,
        };
        let ordinal = record
            .get("ordinal")
            .and_then(Value::as_u64)
            .unwrap_or(last_ordinal);
        last_ordinal = last_ordinal.max(ordinal);
        let payload = record.get("payload").unwrap_or(&Value::Null);
        let kind = payload.get("type").and_then(Value::as_str);
        let Some(turn_id) = payload.get("turn_id").and_then(Value::as_str).or_else(|| {
            payload
                .pointer("/internal_chat_message_metadata_passthrough/turn_id")
                .and_then(Value::as_str)
        }) else {
            continue;
        };
        if !turns.contains_key(turn_id) {
            order.push(turn_id.to_owned());
            turns.insert(turn_id.to_owned(), RolloutChatTurn::new(turn_id.to_owned()));
        }
        let Some(turn) = turns.get_mut(turn_id) else {
            continue;
        };
        match kind {
            Some("task_started") => {
                turn.started_at = payload.get("started_at").and_then(Value::as_i64);
            }
            Some("task_complete") => {
                turn.status = json!("completed");
                turn.started_at = payload
                    .get("started_at")
                    .and_then(Value::as_i64)
                    .or(turn.started_at);
                turn.completed_at = payload.get("completed_at").and_then(Value::as_i64);
            }
            Some("item_completed")
                if payload.get("thread_id").and_then(Value::as_str) == Some(expected_thread_id) =>
            {
                if let Some(item) = payload.get("item").and_then(rollout_chat_item) {
                    push_rollout_chat_item(turn, item);
                }
            }
            Some("message")
                if record.get("type").and_then(Value::as_str) == Some("response_item") =>
            {
                if let Some(item) = rollout_response_message(payload) {
                    push_rollout_chat_item(turn, item);
                }
            }
            _ => {}
        }
    }
    let mut recovered = order
        .into_iter()
        .filter_map(|turn_id| turns.remove(&turn_id))
        .collect::<Vec<_>>();
    let last = recovered.len().saturating_sub(1);
    for (index, turn) in recovered.iter_mut().enumerate() {
        if index < last && turn.status == json!("inProgress") {
            turn.status = json!("interrupted");
        }
    }
    Ok((recovered, last_ordinal))
}

fn push_rollout_chat_item(turn: &mut RolloutChatTurn, item: Value) {
    let item_id = item.get("id").and_then(Value::as_str).unwrap_or_default();
    let duplicate = turn.items.iter().any(|existing| {
        (!item_id.is_empty() && existing.get("id").and_then(Value::as_str) == Some(item_id))
            || same_rollout_chat_message(existing, &item)
    });
    if !duplicate {
        turn.items.push(item);
    }
}

fn same_rollout_chat_message(left: &Value, right: &Value) -> bool {
    match (
        left.get("type").and_then(Value::as_str),
        right.get("type").and_then(Value::as_str),
    ) {
        (Some("userMessage"), Some("userMessage")) => crate::model::same_user_message(left, right),
        (Some("agentMessage"), Some("agentMessage")) => left.get("text") == right.get("text"),
        _ => false,
    }
}

fn rollout_response_message(payload: &Value) -> Option<Value> {
    let id = payload.get("id").and_then(Value::as_str)?.to_owned();
    match payload.get("role").and_then(Value::as_str)? {
        "user" => {
            let content = sanitized_rollout_user_content(payload.get("content"))?;
            Some(json!({
                "id": id,
                "type": "userMessage",
                "content": content
            }))
        }
        "assistant" => {
            let mut text = payload
                .get("content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n");
            text.truncate(text.floor_char_boundary(MAX_LOCAL_CHAT_ITEM_BYTES));
            if text.trim().is_empty() {
                return None;
            }
            Some(json!({
                "id": id,
                "type": "agentMessage",
                "text": text
            }))
        }
        _ => None,
    }
}

fn rollout_chat_item(item: &Value) -> Option<Value> {
    let item_type = item.get("type").and_then(Value::as_str)?;
    let id = item.get("id").and_then(Value::as_str)?.to_owned();
    match item_type {
        "UserMessage" => {
            let content = sanitized_rollout_user_content(item.get("content"))?;
            Some(json!({
                "id": id,
                "type": "userMessage",
                "content": content
            }))
        }
        "AgentMessage" => {
            let mut text = item
                .get("content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n");
            text.truncate(text.floor_char_boundary(MAX_LOCAL_CHAT_ITEM_BYTES));
            if text.trim().is_empty() {
                return None;
            }
            Some(json!({
                "id": id,
                "type": "agentMessage",
                "text": text,
                "phase": item.get("phase").cloned().unwrap_or(Value::Null)
            }))
        }
        _ => None,
    }
}

fn sanitized_rollout_user_content(content: Option<&Value>) -> Option<Vec<Value>> {
    let mut content = content
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|part| {
            !part
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(is_injected_rollout_envelope)
        })
        .collect::<Vec<_>>();
    for part in &mut content {
        if part.get("type").and_then(Value::as_str) == Some("input_text") {
            part["type"] = Value::String("text".into());
        }
        if let Some(text) = part.get("text").and_then(Value::as_str).map(str::to_owned) {
            let mut text = text;
            text.truncate(text.floor_char_boundary(MAX_LOCAL_CHAT_ITEM_BYTES));
            part["text"] = Value::String(text);
        }
    }
    content
        .iter()
        .any(|part| {
            part.get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| !text.trim().is_empty())
                || !matches!(
                    part.get("type").and_then(Value::as_str),
                    None | Some("text") | Some("input_text")
                )
        })
        .then_some(content)
}

fn is_injected_rollout_envelope(text: &str) -> bool {
    let text = text.trim_start();
    [
        "# AGENTS.md instructions for ",
        "<environment_context>",
        "<recommended_plugins>",
        "<permissions instructions>",
        "<apps_instructions>",
        "<plugins_instructions>",
        "<skills_instructions>",
        "<multi_agent_mode>",
        "<collaboration_mode>",
    ]
    .iter()
    .any(|marker| text.starts_with(marker))
}

fn human_bytes(bytes: u64) -> String {
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} GiB", bytes as f64 / GIB)
    } else {
        format!("{:.1} MiB", bytes as f64 / MIB)
    }
}

fn update_check() -> anyhow::Result<Value> {
    let installed = if find_programs(&["pacman"]).is_some() {
        let mut command = Command::new("pacman");
        command.args(["-Q", "codex-native"]);
        command_output_lossy(&mut command)
            .unwrap_or_else(|_| format!("codex-native {} (local build)", env!("CARGO_PKG_VERSION")))
    } else {
        format!("codex-native {} (local build)", env!("CARGO_PKG_VERSION"))
    };
    let available = if let Some(checkupdates) = find_programs(&["checkupdates"]) {
        let mut command = Command::new(checkupdates);
        command.arg("codex-native");
        command_output_lossy(&mut command).unwrap_or_default()
    } else if find_programs(&["pacman"]).is_some() {
        let mut command = Command::new("pacman");
        command.args(["-Qu", "codex-native"]);
        command_output_lossy(&mut command).unwrap_or_default()
    } else {
        String::new()
    };
    let installed_version = installed.split_whitespace().nth(1).map(str::to_owned);
    let rollback_packages = fs::read_dir("/var/cache/pacman/pkg")
        .ok()
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|name| {
                    name.starts_with("codex-native-")
                        && (name.ends_with(".pkg.tar.zst") || name.ends_with(".pkg.tar.xz"))
                        && !installed_version.as_ref().is_some_and(|version| {
                            name.starts_with(&format!("codex-native-{version}-"))
                        })
                })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "installed": installed.trim(),
        "updateAvailable": !available.trim().is_empty(),
        "available": available.trim(),
        "channel": "Arch package",
        "installCommand": "sudo pacman -Syu codex-native",
        "rollbackCommand": "sudo pacman -U /var/cache/pacman/pkg/codex-native-<previous>.pkg.tar.zst",
        "rollbackPackages": rollback_packages
    }))
}

fn update_apply() -> anyhow::Result<Value> {
    let pkexec = find_programs(&["pkexec"]).context("pkexec is required for graphical updates")?;
    let pacman = find_programs(&["pacman"]).context("pacman is unavailable")?;
    let mut command = Command::new(pkexec);
    command
        .arg(pacman)
        .args(["-Syu", "--needed", "--noconfirm", "codex-native"]);
    checked_output(&mut command, "update Codex Native")?;
    Ok(json!({"updated": true, "restartRequired": true}))
}

fn update_rollback(package: &Path) -> anyhow::Result<Value> {
    let cache = Path::new("/var/cache/pacman/pkg").canonicalize()?;
    let package = package
        .canonicalize()
        .with_context(|| format!("rollback package does not exist: {}", package.display()))?;
    let valid_name = package
        .file_name()
        .and_then(|value| value.to_str())
        .is_some_and(|name| {
            name.starts_with("codex-native-")
                && (name.ends_with(".pkg.tar.zst") || name.ends_with(".pkg.tar.xz"))
        });
    if !package.starts_with(cache) || !valid_name {
        bail!("refusing to install a package outside the pacman cache");
    }
    let pkexec = find_programs(&["pkexec"]).context("pkexec is required for rollback")?;
    let pacman = find_programs(&["pacman"]).context("pacman is unavailable")?;
    let mut command = Command::new(pkexec);
    command
        .arg(pacman)
        .args(["-U", "--noconfirm"])
        .arg(&package);
    checked_output(&mut command, "roll back Codex Native")?;
    Ok(json!({"rolledBack": true, "package": package, "restartRequired": true}))
}

fn remote_doctor(configured_binary: Option<&Path>) -> anyhow::Result<Value> {
    let mut candidates = Vec::new();
    if let Some(path) = configured_binary {
        candidates.push(path.to_path_buf());
    }
    let system_binary = PathBuf::from("/usr/bin/codex");
    if system_binary.is_file() {
        candidates.push(system_binary);
    }
    if let Some(path) = find_programs(&["codex"]) {
        candidates.push(path);
    }
    candidates.dedup();
    let mut reports = Vec::new();
    for binary in candidates {
        if !binary.is_file() {
            continue;
        }
        let mut version_command = Command::new(&binary);
        version_command.arg("--version");
        let version = command_output_lossy(&mut version_command).unwrap_or_default();
        let mut help_command = Command::new(&binary);
        help_command.args(["remote-control", "--help"]);
        let remote_help = command_output_lossy(&mut help_command).unwrap_or_default();
        reports.push(json!({
            "path": binary,
            "version": version.trim(),
            "remoteControl": remote_help.contains("remote-control") || !remote_help.trim().is_empty(),
            "packageManaged": binary == Path::new("/usr/bin/codex")
        }));
    }
    let ready = reports
        .iter()
        .any(|value| value.get("remoteControl").and_then(Value::as_bool) == Some(true));
    Ok(json!({
        "ready": ready,
        "candidates": reports,
        "serviceActive": user_service_active("codex-native-remote.service"),
        "package": "openai-codex",
        "remediation": if ready {
            "Remote runtime is available."
        } else {
            "Install or update the Codex CLI with the system package manager, then restart the remote host."
        }
    }))
}

fn remote_profile_unit(profile_home: Option<&Path>) -> anyhow::Result<String> {
    if profile_home.is_none() {
        return Ok("codex-native-remote.service".into());
    }
    let profile_id = profile_home
        .and_then(Path::parent)
        .and_then(Path::file_name)
        .and_then(|value| value.to_str())
        .filter(|value| {
            value.starts_with("account-")
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
        .context("account profile path has no safe service identifier")?;
    Ok(format!("codex-native-remote@{profile_id}.service"))
}

fn remote_doctor_for_profile(
    configured_binary: Option<&Path>,
    profile_home: Option<&Path>,
) -> anyhow::Result<Value> {
    let mut report = remote_doctor(configured_binary)?;
    let unit = remote_profile_unit(profile_home)?;
    if let Some(object) = report.as_object_mut() {
        object.insert(
            "serviceActive".into(),
            Value::Bool(user_service_active(&unit)),
        );
        object.insert("service".into(), Value::String(unit));
    }
    Ok(report)
}

fn remote_restart(configured_binary: Option<&Path>) -> anyhow::Result<Value> {
    let unit = "codex-native-remote.service";
    let mut unit_check = Command::new("systemctl");
    unit_check.args(["--user", "cat", unit]);
    if unit_check.status().is_ok_and(|status| status.success()) {
        let mut restart = Command::new("systemctl");
        restart.args(["--user", "restart", unit]);
        checked_output(&mut restart, "restart the remote host service")?;
        return Ok(json!({
            "restarted": true,
            "mode": "systemd-user-service",
            "serviceActive": user_service_active(unit)
        }));
    }

    let binary = configured_binary
        .filter(|path| path.is_file())
        .map(Path::to_path_buf)
        .or_else(|| {
            Path::new("/usr/bin/codex")
                .is_file()
                .then(|| PathBuf::from("/usr/bin/codex"))
        })
        .or_else(|| find_programs(&["codex"]))
        .context("no Codex CLI was found for remote recovery")?;
    let mut stop = Command::new(&binary);
    stop.args(["remote-control", "stop", "--json"]);
    let _ = command_output_lossy(&mut stop);
    let mut start = Command::new(&binary);
    start.args(["remote-control", "start", "--json"]);
    let output = checked_output(&mut start, "restart the remote host")?;
    Ok(json!({
        "restarted": true,
        "mode": "direct-cli",
        "binary": binary,
        "result": serde_json::from_slice::<Value>(&output.stdout)
            .unwrap_or_else(|_| Value::String(truncate_output(&output.stdout)))
    }))
}

fn remote_restart_for_profile(
    configured_binary: Option<&Path>,
    profile_home: Option<&Path>,
) -> anyhow::Result<Value> {
    let Some(profile_home) = profile_home else {
        return remote_restart(configured_binary);
    };
    let unit = remote_profile_unit(Some(profile_home))?;
    let mut unit_check = Command::new("systemctl");
    unit_check.args(["--user", "cat", &unit]);
    if unit_check.status().is_ok_and(|status| status.success()) {
        let mut restart = Command::new("systemctl");
        restart.args(["--user", "restart", &unit]);
        checked_output(&mut restart, "restart the account remote host")?;
        return Ok(json!({
            "restarted": true,
            "mode": "systemd-user-service",
            "service": unit,
            "serviceActive": user_service_active(&unit)
        }));
    }
    bail!(
        "the account-specific Remote host service is not installed; update Codex Native before enabling this profile's Remote access"
    )
}

fn workspace_doctor(cwd: &Path) -> anyhow::Result<Value> {
    let cwd = cwd
        .canonicalize()
        .with_context(|| format!("workspace path is unavailable: {}", cwd.display()))?;
    if !cwd.is_dir() {
        bail!("workspace path is not a directory: {}", cwd.display());
    }
    let bubblewrap = find_programs(&["bwrap"]);
    let systemd_run = find_programs(&["systemd-run"]);
    let terminal = workspace_terminal().map(|(binary, _)| binary);
    let namespace_check = bubblewrap.as_ref().is_some_and(|binary| {
        let mut command = Command::new(binary);
        command.args([
            "--unshare-pid",
            "--ro-bind",
            "/usr",
            "/usr",
            "--symlink",
            "usr/bin",
            "/bin",
            "--symlink",
            "usr/lib",
            "/lib",
            "--symlink",
            "usr/lib",
            "/lib64",
            "--proc",
            "/proc",
            "--dev",
            "/dev",
            "/usr/bin/true",
        ]);
        command.status().is_ok_and(|status| status.success())
    });
    let user_systemd = systemd_run.is_some() && {
        let mut command = Command::new("systemctl");
        command.args(["--user", "show-environment"]);
        command.status().is_ok_and(|status| status.success())
    };
    let active_units = workspace_units();
    Ok(json!({
        "ready": bubblewrap.is_some() && systemd_run.is_some() && terminal.is_some()
            && namespace_check && user_systemd,
        "cwd": cwd,
        "bubblewrap": bubblewrap,
        "namespaceCheck": namespace_check,
        "systemdRun": systemd_run,
        "userSystemd": user_systemd,
        "terminal": terminal,
        "activeUnits": active_units,
        "security": {
            "filesystem": "read-only OS, private home, selected project writable",
            "networkDefault": "off",
            "secrets": "host home and Codex credentials are not mounted",
            "processes": "separate PID, IPC, UTS, user, and cgroup namespaces",
            "resources": "systemd memory and task limits"
        }
    }))
}

fn workspace_launch(
    cwd: &Path,
    additional_paths: &[PathBuf],
    network: bool,
    memory_mib: u32,
) -> anyhow::Result<Value> {
    let cwd = cwd
        .canonicalize()
        .with_context(|| format!("workspace path is unavailable: {}", cwd.display()))?;
    if !cwd.is_dir() {
        bail!("workspace path is not a directory: {}", cwd.display());
    }
    let bubblewrap = find_programs(&["bwrap"]).context("bubblewrap is not installed")?;
    let systemd_run = find_programs(&["systemd-run"]).context("systemd-run is unavailable")?;
    let (terminal, terminal_args) = workspace_terminal().context(
        "no supported terminal was found (WezTerm, GNOME Console, GNOME Terminal, Konsole, Foot, Alacritty, Kitty, or XTerm)",
    )?;
    let session_id = uuid::Uuid::new_v4().simple().to_string();
    let unit_base = format!("codex-native-workspace-{session_id}");
    let unit = format!("{unit_base}.service");
    let root = dirs::state_dir()
        .or_else(dirs::data_local_dir)
        .context("XDG state directory unavailable")?
        .join("codex-native/workspaces")
        .join(&session_id);
    let home = root.join("home");
    fs::create_dir_all(&home)
        .with_context(|| format!("failed to create isolated home {}", home.display()))?;

    let mut sandbox_args = vec![
        OsString::from("--die-with-parent"),
        OsString::from("--new-session"),
        OsString::from("--unshare-user"),
        OsString::from("--unshare-pid"),
        OsString::from("--unshare-ipc"),
        OsString::from("--unshare-uts"),
        OsString::from("--unshare-cgroup-try"),
        OsString::from("--cap-drop"),
        OsString::from("ALL"),
        OsString::from("--ro-bind"),
        OsString::from("/usr"),
        OsString::from("/usr"),
        OsString::from("--symlink"),
        OsString::from("usr/bin"),
        OsString::from("/bin"),
        OsString::from("--symlink"),
        OsString::from("usr/lib"),
        OsString::from("/lib"),
        OsString::from("--symlink"),
        OsString::from("usr/lib"),
        OsString::from("/lib64"),
        OsString::from("--ro-bind"),
        OsString::from("/etc"),
        OsString::from("/etc"),
        OsString::from("--proc"),
        OsString::from("/proc"),
        OsString::from("--dev"),
        OsString::from("/dev"),
        OsString::from("--tmpfs"),
        OsString::from("/tmp"),
        OsString::from("--dir"),
        OsString::from("/run"),
        OsString::from("--ro-bind-try"),
        OsString::from("/run/systemd/resolve"),
        OsString::from("/run/systemd/resolve"),
        OsString::from("--dir"),
        OsString::from("/home"),
        OsString::from("--bind"),
        home.as_os_str().to_owned(),
        OsString::from("/home/codex"),
        OsString::from("--bind"),
        cwd.as_os_str().to_owned(),
        OsString::from("/workspace"),
        OsString::from("--chdir"),
        OsString::from("/workspace"),
    ];
    if !network {
        sandbox_args.push(OsString::from("--unshare-net"));
    }
    let mut mounted_context = Vec::new();
    if !additional_paths.is_empty() {
        sandbox_args.extend([OsString::from("--dir"), OsString::from("/context")]);
    }
    for (index, path) in additional_paths.iter().take(16).enumerate() {
        let path = path
            .canonicalize()
            .with_context(|| format!("additional path is unavailable: {}", path.display()))?;
        let target = format!("/context/{index}");
        sandbox_args.extend([
            OsString::from("--ro-bind"),
            path.as_os_str().to_owned(),
            OsString::from(&target),
        ]);
        mounted_context.push(json!({"source": path, "target": target, "readOnly": true}));
    }
    sandbox_args.extend([
        OsString::from("--clearenv"),
        OsString::from("--setenv"),
        OsString::from("HOME"),
        OsString::from("/home/codex"),
        OsString::from("--setenv"),
        OsString::from("USER"),
        OsString::from("codex"),
        OsString::from("--setenv"),
        OsString::from("LOGNAME"),
        OsString::from("codex"),
        OsString::from("--setenv"),
        OsString::from("PATH"),
        OsString::from("/usr/local/bin:/usr/bin"),
        OsString::from("--setenv"),
        OsString::from("LANG"),
        OsString::from(env::var("LANG").unwrap_or_else(|_| "C.UTF-8".into())),
        OsString::from("--hostname"),
        OsString::from("codex-workspace"),
        OsString::from("/usr/bin/bash"),
        OsString::from("--noprofile"),
        OsString::from("--norc"),
    ]);

    let memory_mib = memory_mib.clamp(512, 65_536);
    let mut command = Command::new(systemd_run);
    command
        .args(["--user", "--collect", "--quiet"])
        .arg(format!("--unit={unit_base}"))
        .arg("--property=Description=Codex Native isolated agent workspace")
        .arg(format!("--property=MemoryMax={memory_mib}M"))
        .arg("--property=TasksMax=512")
        .arg("--property=NoNewPrivileges=yes")
        .arg(terminal)
        .args(terminal_args)
        .arg(bubblewrap)
        .args(sandbox_args);
    checked_output(&mut command, "launch the isolated agent workspace")?;
    Ok(json!({
        "id": session_id,
        "unit": unit,
        "cwd": cwd,
        "home": home,
        "network": network,
        "memoryMib": memory_mib,
        "additionalPaths": mounted_context,
        "startedAt": Utc::now(),
        "active": true
    }))
}

fn workspace_stop(unit: &str) -> anyhow::Result<Value> {
    if !valid_workspace_unit(unit) {
        bail!("refusing to stop an unmanaged systemd unit");
    }
    let mut command = Command::new("systemctl");
    command.args(["--user", "stop", unit]);
    checked_output(&mut command, "stop the isolated agent workspace")?;
    Ok(json!({"unit": unit, "active": false, "stopped": true}))
}

fn workspace_units() -> Vec<String> {
    let mut command = Command::new("systemctl");
    command.args([
        "--user",
        "list-units",
        "--state=running",
        "--plain",
        "--no-legend",
        "codex-native-workspace-*.service",
    ]);
    command_output_lossy(&mut command)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .filter(|unit| valid_workspace_unit(unit))
        .map(str::to_owned)
        .collect()
}

fn valid_workspace_unit(unit: &str) -> bool {
    unit.strip_prefix("codex-native-workspace-")
        .and_then(|value| value.strip_suffix(".service"))
        .is_some_and(|value| {
            value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

fn workspace_terminal() -> Option<(PathBuf, Vec<OsString>)> {
    let candidates: &[(&str, &[&str])] = &[
        ("wezterm", &["start", "--always-new-process", "--"]),
        ("kgx", &["--"]),
        ("gnome-terminal", &["--"]),
        ("konsole", &["-e"]),
        ("foot", &["-e"]),
        ("alacritty", &["-e"]),
        ("kitty", &["--"]),
        ("xterm", &["-e"]),
    ];
    for (name, args) in candidates {
        if let Some(binary) = find_programs(&[*name]) {
            return Some((binary, args.iter().map(OsString::from).collect()));
        }
    }
    None
}

fn qwen_terminal_launch(cwd: &Path) -> anyhow::Result<Value> {
    let cwd = cwd
        .canonicalize()
        .with_context(|| format!("workspace path is unavailable: {}", cwd.display()))?;
    if !cwd.is_dir() {
        bail!("workspace path is not a directory: {}", cwd.display());
    }
    let cli = qwen_buddy_cli().context("qwen-buddy-code is not installed")?;
    let systemd_run = find_programs(&["systemd-run"]).context("systemd-run is unavailable")?;
    let (terminal, terminal_args) = workspace_terminal().context(
        "no supported terminal was found (WezTerm, GNOME Console, GNOME Terminal, Konsole, Foot, Alacritty, Kitty, or XTerm)",
    )?;
    let session_id = uuid::Uuid::new_v4().simple().to_string();
    let unit_base = format!("codex-native-qwen-buddy-{session_id}");
    let mut command = Command::new(systemd_run);
    command
        .args(["--user", "--collect", "--quiet"])
        .arg(format!("--unit={unit_base}"))
        .arg("--property=Description=OpenCode local Qwen terminal")
        .arg("--property=MemoryMax=48G")
        .arg("--property=MemorySwapMax=16G")
        .arg("--property=TasksMax=512")
        .arg("--property=NoNewPrivileges=yes")
        .arg("--working-directory")
        .arg(&cwd)
        .arg(terminal)
        .args(terminal_args)
        .arg(cli)
        .args(["--mode", "code"])
        .args(["--cwd"])
        .arg(&cwd);
    checked_output(&mut command, "launch OpenCode")?;
    Ok(json!({
        "unit": format!("{unit_base}.service"),
        "cwd": cwd,
        "started": true
    }))
}

fn qwen_buddy_cli() -> Option<PathBuf> {
    env::var_os("QWEN_BUDDY_CODE_PATH")
        .map(PathBuf::from)
        .filter(|path| path.is_file())
        .or_else(|| {
            dirs::home_dir()
                .map(|home| home.join(".local/bin/qwen-buddy-code"))
                .filter(|path| path.is_file())
        })
        .or_else(|| find_programs(&["qwen-buddy-code"]))
}

fn buddy_delegate(
    backend: &str,
    prompt: &str,
    context: &str,
    effort: &str,
    cwd: &Path,
    access: &str,
    on_progress: &mut dyn FnMut(BuddyProgress),
) -> anyhow::Result<Value> {
    let cli = qwen_buddy_cli().context("qwen-buddy-code is not installed")?;
    buddy_delegate_with_cli(
        &cli,
        backend,
        prompt,
        context,
        effort,
        cwd,
        access,
        on_progress,
    )
}

#[allow(clippy::too_many_arguments)]
fn buddy_delegate_with_cli(
    cli: &Path,
    backend: &str,
    prompt: &str,
    context: &str,
    effort: &str,
    cwd: &Path,
    access: &str,
    on_progress: &mut dyn FnMut(BuddyProgress),
) -> anyhow::Result<Value> {
    if !matches!(backend, "qwen" | "gemini" | "openrouter" | "mistral") {
        bail!("unsupported Buddy model: {backend}");
    }
    let valid_effort = match backend {
        "qwen" => matches!(effort, "none" | "low" | "medium" | "high" | "xhigh"),
        "gemini" => matches!(effort, "minimal" | "low" | "medium" | "high"),
        "openrouter" => matches!(effort, "minimal" | "low" | "medium" | "high"),
        "mistral" => matches!(effort, "none" | "high"),
        _ => false,
    };
    if !valid_effort {
        bail!("unsupported {backend} effort: {effort}");
    }
    if !matches!(
        access,
        "read-only" | "workspace-write" | "danger-full-access"
    ) {
        bail!("unsupported Buddy access mode: {access}");
    }
    let cwd = cwd
        .canonicalize()
        .with_context(|| format!("workspace path is unavailable: {}", cwd.display()))?;
    if !cwd.is_dir() {
        bail!("workspace path is not a directory: {}", cwd.display());
    }
    if prompt.trim().is_empty() {
        bail!("Buddy prompt is empty");
    }
    let mut context_file = tempfile::NamedTempFile::new()
        .context("could not create the private Buddy context file")?;
    context_file
        .write_all(context.as_bytes())
        .context("could not write the private Buddy context file")?;
    context_file
        .flush()
        .context("could not flush the private Buddy context file")?;

    let mut command = Command::new(cli);
    command
        .args(["--cwd"])
        .arg(&cwd)
        .args(["--scope"])
        .arg(&cwd)
        .args(["--model", backend, "--effort", effort, "--thinking"])
        .arg(if matches!(effort, "none" | "minimal") {
            "off"
        } else {
            "on"
        })
        .args(["--access"])
        .arg(match (backend, access) {
            ("qwen", "workspace-write") => "workspace-write",
            ("qwen", "danger-full-access") => "full-write",
            _ => "read-only",
        })
        .args(["--context"])
        .arg(context_file.path())
        .args(["--json", "--progress-jsonl"]);
    if access == "danger-full-access" {
        command.args(["--scope", "/", "--allow-whole-pc", "--system"]);
    }
    if backend == "qwen" {
        command.args(["--agent", "--unbounded-output"]);
    } else {
        command.args(["--max-output-tokens", "4096"]);
    }
    command.arg("--").arg(prompt);
    let output = checked_output_with_buddy_progress(
        &mut command,
        &format!("run {backend} independently of Codex"),
        on_progress,
    )?;
    let (text, metrics) = buddy_output(&output.stdout, backend)?;
    Ok(json!({
        "backend": backend,
        "effort": effort,
        "text": text,
        "metrics": metrics,
        "quotaIndependent": true
    }))
}

fn checked_output_with_buddy_progress(
    command: &mut Command,
    description: &str,
    on_progress: &mut dyn FnMut(BuddyProgress),
) -> anyhow::Result<Output> {
    const PREFIX: &str = "QWEN_BUDDY_PROGRESS ";
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to {description}"))?;
    let mut stdout = child
        .stdout
        .take()
        .context("Buddy stdout was unavailable")?;
    let stdout_reader = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).map(|_| bytes)
    });
    let stderr = child
        .stderr
        .take()
        .context("Buddy stderr was unavailable")?;
    let mut stderr_output = Vec::new();
    for line in BufReader::new(stderr).lines() {
        let line = line.context("could not read Buddy progress")?;
        if let Some(payload) = line.strip_prefix(PREFIX)
            && let Ok(mut progress) = serde_json::from_str::<BuddyProgress>(payload)
        {
            progress.phase = bounded_progress_text(&progress.phase, 40);
            progress.detail = bounded_progress_text(&progress.detail, 320);
            on_progress(progress);
            continue;
        }
        if stderr_output.len() < MAX_COMMAND_OUTPUT {
            let remaining = MAX_COMMAND_OUTPUT - stderr_output.len();
            stderr_output.extend_from_slice(&line.as_bytes()[..line.len().min(remaining)]);
            if stderr_output.len() < MAX_COMMAND_OUTPUT {
                stderr_output.push(b'\n');
            }
        }
    }
    let status = child
        .wait()
        .with_context(|| format!("failed to wait for {description}"))?;
    let stdout = stdout_reader
        .join()
        .map_err(|_| anyhow!("Buddy stdout reader panicked"))?
        .context("could not read Buddy output")?;
    let output = Output {
        status,
        stdout,
        stderr: stderr_output,
    };
    if output.status.success() {
        Ok(output)
    } else {
        let stderr = truncate_output(&output.stderr);
        let stdout = truncate_output(&output.stdout);
        let detail = if stderr.trim().is_empty() {
            stdout
        } else {
            stderr
        };
        Err(anyhow!("could not {description}: {}", detail.trim()))
    }
}

fn bounded_progress_text(text: &str, max_chars: usize) -> String {
    text.chars()
        .filter(|character| !character.is_control())
        .take(max_chars)
        .collect()
}

fn buddy_output(output: &[u8], backend: &str) -> anyhow::Result<(String, Value)> {
    let raw = truncate_output(output);
    if let Ok(payload) = serde_json::from_str::<Value>(raw.trim()) {
        let provider = payload
            .get("provider")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if provider != backend {
            bail!("Buddy provider mismatch: expected {backend}, received {provider}");
        }
        let text = payload
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let text = buddy_output_text(text.as_bytes(), backend)?;
        let metrics = payload.get("metrics").cloned().unwrap_or_else(|| json!({}));
        return Ok((text, metrics));
    }
    Ok((buddy_output_text(raw.as_bytes(), backend)?, json!({})))
}

fn buddy_output_text(output: &[u8], backend: &str) -> anyhow::Result<String> {
    let text = truncate_output(output).trim().to_owned();
    if text.is_empty() {
        bail!("{backend} returned no response");
    }
    if text.starts_with("Qwen could not complete this request:")
        || text.starts_with("Gemini could not complete this request:")
        || text.starts_with("OpenRouter Free could not complete this request:")
        || text.starts_with("Mistral AI could not complete this request:")
    {
        bail!("{text}");
    }
    Ok(text)
}

fn capture_voice(seconds: u8) -> anyhow::Result<Value> {
    let seconds = seconds.clamp(1, 30);
    let sample_rate = 24_000_u32;
    let samples = sample_rate * u32::from(seconds);
    let binary = find_programs(&["pw-record"])
        .context("pw-record is unavailable; install pipewire-audio to use native dictation")?;
    let sample_rate_arg = sample_rate.to_string();
    let samples_arg = samples.to_string();
    let mut command = Command::new(binary);
    command
        .args(["--raw", "--rate"])
        .arg(sample_rate_arg)
        .args(["--channels", "1", "--format", "s16", "--sample-count"])
        .arg(samples_arg)
        .arg("-");
    let output = checked_output(&mut command, "record microphone audio")?;
    if output.stdout.is_empty() {
        bail!("microphone capture returned no audio");
    }
    Ok(json!({
        "data": base64::engine::general_purpose::STANDARD.encode(&output.stdout),
        "sampleRate": sample_rate,
        "numChannels": 1,
        "samplesPerChannel": output.stdout.len() / 2
    }))
}

fn speak(text: &str) -> anyhow::Result<Value> {
    let text = text.trim();
    if text.is_empty() {
        bail!("there is no assistant message to read");
    }
    let (binary, args): (PathBuf, &[&str]) = if let Some(binary) = find_programs(&["spd-say"]) {
        (binary, &["--wait"])
    } else if let Some(binary) = find_programs(&["espeak-ng", "espeak"]) {
        (binary, &[])
    } else {
        bail!("install speech-dispatcher or espeak-ng to use Read Aloud");
    };
    Command::new(&binary)
        .args(args)
        .arg(text)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("failed to launch {}", binary.display()))?;
    Ok(json!({"started": true, "binary": binary}))
}

fn user_service_active(unit: &str) -> bool {
    let mut command = Command::new("systemctl");
    command.args(["--user", "is-active", "--quiet", unit]);
    command.status().is_ok_and(|status| status.success())
}

fn configure_remote_autostart(enabled: bool) -> anyhow::Result<Value> {
    let unit = "codex-native-remote.service";
    let mut command = Command::new("systemctl");
    command.args(remote_autostart_args(enabled)).arg(unit);
    checked_output(&mut command, "update remote host autostart")?;
    Ok(json!({"enabled": enabled, "unit": unit}))
}

fn remote_autostart_args(enabled: bool) -> &'static [&'static str] {
    if enabled {
        &["--user", "--no-block", "enable", "--now"]
    } else {
        &["--user", "--no-block", "disable", "--now"]
    }
}

fn checked_output(command: &mut Command, description: &str) -> anyhow::Result<Output> {
    let output = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("failed to {description}"))?;
    if output.status.success() {
        Ok(output)
    } else {
        let stderr = truncate_output(&output.stderr);
        let stdout = truncate_output(&output.stdout);
        let detail = if stderr.trim().is_empty() {
            stdout
        } else {
            stderr
        };
        Err(anyhow!("could not {description}: {}", detail.trim()))
    }
}

fn command_output_lossy(command: &mut Command) -> anyhow::Result<String> {
    let output = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()?;
    if output.status.success() {
        Ok(truncate_output(&output.stdout))
    } else {
        Err(anyhow!(truncate_output(&output.stderr)))
    }
}

fn truncate_output(bytes: &[u8]) -> String {
    let slice = if bytes.len() > MAX_COMMAND_OUTPUT {
        &bytes[bytes.len() - MAX_COMMAND_OUTPUT..]
    } else {
        bytes
    };
    String::from_utf8_lossy(slice).into_owned()
}

fn find_programs(names: &[&str]) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    for directory in env::split_paths(&path) {
        for name in names {
            let candidate = directory.join(name);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

fn proc_memory(pid: u32) -> Option<(u64, u64)> {
    let status = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let mut rss = 0;
    let mut peak = 0;
    for line in status.lines() {
        if let Some(value) = line.strip_prefix("VmRSS:") {
            rss = value.split_whitespace().next()?.parse().ok()?;
        } else if let Some(value) = line.strip_prefix("VmHWM:") {
            peak = value.split_whitespace().next()?.parse().ok()?;
        }
    }
    Some((rss, peak))
}

fn descendant_processes(root_pid: u32) -> Vec<ProcessInfo> {
    let mut all = Vec::new();
    let Ok(entries) = fs::read_dir("/proc") else {
        return all;
    };
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|value| value.parse::<u32>().ok())
        else {
            continue;
        };
        let Ok(status) = fs::read_to_string(entry.path().join("status")) else {
            continue;
        };
        let mut name = String::new();
        let mut parent_pid = 0;
        let mut rss_kib = 0;
        for line in status.lines() {
            if let Some(value) = line.strip_prefix("Name:") {
                name = value.trim().to_owned();
            } else if let Some(value) = line.strip_prefix("PPid:") {
                parent_pid = value.trim().parse().unwrap_or(0);
            } else if let Some(value) = line.strip_prefix("VmRSS:") {
                rss_kib = value
                    .split_whitespace()
                    .next()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0);
            }
        }
        all.push(ProcessInfo {
            pid,
            parent_pid,
            name,
            rss_kib,
        });
    }
    let mut descendants = Vec::new();
    let mut parents = VecDeque::from([root_pid]);
    let mut seen = HashSet::from([root_pid]);
    while let Some(parent) = parents.pop_front() {
        for process in all.iter().filter(|process| process.parent_pid == parent) {
            if seen.insert(process.pid) {
                descendants.push(process.clone());
                parents.push_back(process.pid);
            }
        }
    }
    descendants.sort_by_key(|process| std::cmp::Reverse(process.rss_kib));
    descendants
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugs_never_escape_the_managed_directory() {
        assert_eq!(safe_slug("feature/../../danger"), "feature-------danger");
        assert_eq!(safe_slug(""), "workspace");
    }

    #[test]
    fn output_tail_is_bounded() {
        let bytes = vec![b'x'; MAX_COMMAND_OUTPUT + 20];
        assert_eq!(truncate_output(&bytes).len(), MAX_COMMAND_OUTPUT);
    }

    #[test]
    fn buddy_delegate_runs_without_a_codex_process() {
        let workspace = tempfile::tempdir().unwrap();
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tools/fake_qwen_buddy_code.py");
        let mut progress_events = Vec::new();
        let result = buddy_delegate_with_cli(
            &fixture,
            "gemini",
            "hello",
            "prior context",
            "minimal",
            workspace.path(),
            "read-only",
            &mut |progress| progress_events.push(progress),
        )
        .unwrap();
        assert_eq!(progress_events.len(), 7);
        assert_eq!(progress_events[0].phase, "planning");
        assert_eq!(progress_events[2].phase, "plan_step");
        assert_eq!(progress_events[3].phase, "read_started");
        assert_eq!(progress_events[4].phase, "read_completed");
        assert_eq!(progress_events[5].phase, "verification");
        let measured = progress_events.last().unwrap();
        assert_eq!(measured.phase, "working");
        assert_eq!(measured.total_tokens, 150);
        assert_eq!(measured.context_window_tokens, 65_536);
        assert_eq!(measured.tokens_per_second, Some(42.5));
        assert_eq!(result.get("quotaIndependent"), Some(&json!(true)));
        assert_eq!(result.pointer("/metrics/promptTokens"), Some(&json!(120)));
        assert_eq!(result.pointer("/metrics/outputTokens"), Some(&json!(30)));
        assert_eq!(
            result.get("text").and_then(Value::as_str),
            Some("quota-independent Buddy fixture")
        );
        assert!(
            buddy_delegate_with_cli(
                &fixture,
                "qwen",
                "hello",
                "",
                "minimal",
                workspace.path(),
                "read-only",
                &mut |_| {},
            )
            .is_err()
        );
        let qwen = buddy_delegate_with_cli(
            &fixture,
            "qwen",
            "hello",
            "prior context",
            "xhigh",
            workspace.path(),
            "read-only",
            &mut |_| {},
        )
        .unwrap();
        assert_eq!(qwen.get("backend"), Some(&json!("qwen")));
        let openrouter = buddy_delegate_with_cli(
            &fixture,
            "openrouter",
            "hello",
            "prior context",
            "medium",
            workspace.path(),
            "read-only",
            &mut |_| {},
        )
        .unwrap();
        assert_eq!(openrouter.get("backend"), Some(&json!("openrouter")));
        let mistral = buddy_delegate_with_cli(
            &fixture,
            "mistral",
            "hello",
            "prior context",
            "high",
            workspace.path(),
            "danger-full-access",
            &mut |_| {},
        )
        .unwrap();
        assert_eq!(mistral.get("backend"), Some(&json!("mistral")));
        assert!(
            buddy_output_text(
                b"Gemini could not complete this request: temporary HTTP 503",
                "gemini",
            )
            .is_err()
        );
        assert!(
            buddy_output_text(
                b"OpenRouter Free could not complete this request: rate limited",
                "openrouter",
            )
            .is_err()
        );
        assert!(
            buddy_output_text(
                b"Mistral AI could not complete this request: temporary failure",
                "mistral",
            )
            .is_err()
        );
    }

    #[test]
    fn local_rollout_chat_recovers_response_item_messages_without_duplicates() {
        let thread_id = "019f9f86-8517-7bd3-ae7a-43146df7e5f7";
        let fixture = [
            json!({"type":"event_msg","payload":{"type":"task_started","turn_id":"turn-1","started_at":10}}),
            json!({"type":"response_item","payload":{"type":"message","id":"response-user","role":"user","content":[{"type":"input_text","text":"prompt"}],"internal_chat_message_metadata_passthrough":{"turn_id":"turn-1"}}}),
            json!({"type":"event_msg","payload":{"type":"item_completed","thread_id":thread_id,"turn_id":"turn-1","item":{"type":"UserMessage","id":"event-user","content":[{"type":"text","text":"prompt","text_elements":[]}]}}}),
            json!({"type":"response_item","payload":{"type":"message","id":"response-agent","role":"assistant","content":[{"type":"output_text","text":"answer"}],"internal_chat_message_metadata_passthrough":{"turn_id":"turn-1"}}}),
            json!({"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-1","started_at":10,"completed_at":20}}),
        ]
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join("\n");

        let (turns, _) = parse_rollout_chat(thread_id, std::io::Cursor::new(fixture)).unwrap();

        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].items.len(), 2);
        assert_eq!(turns[0].items[0]["type"], "userMessage");
        assert_eq!(turns[0].items[0]["content"][0]["type"], "text");
        assert_eq!(turns[0].items[1]["text"], "answer");
    }

    #[test]
    fn local_rollout_chat_hides_injected_user_envelopes() {
        let thread_id = "019f9f86-8517-7bd3-ae7a-43146df7e5f7";
        let fixture = [
            json!({"type":"event_msg","payload":{"type":"task_started","turn_id":"turn-1","started_at":10}}),
            json!({"type":"response_item","payload":{"type":"message","id":"hidden-user","role":"user","content":[{"type":"input_text","text":"# AGENTS.md instructions for /workspace\n<INSTRUCTIONS>hidden</INSTRUCTIONS>"},{"type":"input_text","text":"<environment_context>hidden</environment_context>"}],"internal_chat_message_metadata_passthrough":{"turn_id":"turn-1"}}}),
            json!({"type":"response_item","payload":{"type":"message","id":"visible-user","role":"user","content":[{"type":"input_text","text":"<recommended_plugins>hidden</recommended_plugins>"},{"type":"input_text","text":"actual prompt"}],"internal_chat_message_metadata_passthrough":{"turn_id":"turn-1"}}}),
            json!({"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-1","started_at":10,"completed_at":20}}),
        ]
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join("\n");

        let (turns, _) = parse_rollout_chat(thread_id, std::io::Cursor::new(fixture)).unwrap();

        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].items.len(), 1);
        assert_eq!(turns[0].items[0]["id"], "visible-user");
        assert_eq!(turns[0].items[0]["content"].as_array().unwrap().len(), 1);
        assert_eq!(turns[0].items[0]["content"][0]["text"], "actual prompt");
    }

    #[test]
    fn bounded_rollout_reader_discards_only_the_partial_leading_record() {
        let mut fixture = tempfile::NamedTempFile::new().unwrap();
        writeln!(fixture, "{}", "x".repeat(80)).unwrap();
        writeln!(fixture, "{{\"latest\":true}}").unwrap();
        fixture.flush().unwrap();
        let size = fixture.as_file().metadata().unwrap().len();

        let (mut reader, scan_start) =
            bounded_rollout_chat_reader(fixture.reopen().unwrap(), size, 32).unwrap();
        let mut recovered = String::new();
        reader.read_to_string(&mut recovered).unwrap();

        assert!(scan_start > 0);
        assert_eq!(recovered, "{\"latest\":true}\n");
    }

    #[test]
    fn bounded_rollout_records_skip_oversized_lines_and_keep_following_chat() {
        let fixture = format!("{}\nlatest\n", "x".repeat(64));
        let mut reader = std::io::Cursor::new(fixture);
        let mut record = Vec::new();

        assert_eq!(
            read_bounded_rollout_record(&mut reader, &mut record, 16).unwrap(),
            Some(false)
        );
        assert!(record.is_empty());
        assert_eq!(
            read_bounded_rollout_record(&mut reader, &mut record, 16).unwrap(),
            Some(true)
        );
        assert_eq!(record, b"latest");
        assert_eq!(
            read_bounded_rollout_record(&mut reader, &mut record, 16).unwrap(),
            None
        );
    }

    #[test]
    fn local_rollout_chat_recovers_complete_and_interrupted_turns() {
        let thread_id = "019f9f86-8517-7bd3-ae7a-43146df7e5f7";
        let fixture = [
            json!({"ordinal":1,"type":"event_msg","payload":{"type":"task_started","turn_id":"turn-1","started_at":10}}),
            json!({"ordinal":2,"type":"event_msg","payload":{"type":"item_completed","thread_id":thread_id,"turn_id":"turn-1","item":{"type":"UserMessage","id":"user-1","content":[{"type":"text","text":"first prompt"}]}}}),
            json!({"ordinal":3,"type":"event_msg","payload":{"type":"item_completed","thread_id":thread_id,"turn_id":"turn-1","item":{"type":"AgentMessage","id":"agent-1","content":[{"type":"Text","text":"first answer"}],"phase":"final_answer"}}}),
            json!({"ordinal":4,"type":"event_msg","payload":{"type":"task_started","turn_id":"turn-2","started_at":20}}),
            json!({"ordinal":5,"type":"event_msg","payload":{"type":"item_completed","thread_id":thread_id,"turn_id":"turn-2","item":{"type":"UserMessage","id":"user-2","content":[{"type":"text","text":"second prompt"}]}}}),
            json!({"ordinal":6,"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-2","started_at":20,"completed_at":30}}),
        ]
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join("\n");
        let (turns, ordinal) =
            parse_rollout_chat(thread_id, std::io::Cursor::new(fixture)).unwrap();

        assert_eq!(ordinal, 6);
        assert_eq!(turns.len(), 2);
        assert_eq!(turns[0].status, json!("interrupted"));
        assert_eq!(turns[0].items[0]["type"], "userMessage");
        assert_eq!(turns[0].items[1]["text"], "first answer");
        assert_eq!(turns[1].status, json!("completed"));
        assert_eq!(turns[1].completed_at, Some(30));
    }

    #[test]
    fn local_rollout_chat_preserves_more_than_forty_turns() {
        let thread_id = "019f9f86-8517-7bd3-ae7a-43146df7e5f7";
        let mut ordinal = 0_u64;
        let mut records = Vec::new();
        for index in 0..60 {
            let turn_id = format!("turn-{index}");
            records.push(json!({
                "ordinal": ordinal,
                "type": "event_msg",
                "payload": {
                    "type": "task_started",
                    "turn_id": turn_id,
                    "started_at": index
                }
            }));
            ordinal += 1;
            records.push(json!({
                "ordinal": ordinal,
                "type": "event_msg",
                "payload": {
                    "type": "item_completed",
                    "thread_id": thread_id,
                    "turn_id": turn_id,
                    "item": {
                        "type": "UserMessage",
                        "id": format!("user-{index}"),
                        "content": [{"type": "text", "text": format!("prompt {index}")}]
                    }
                }
            }));
            ordinal += 1;
            records.push(json!({
                "ordinal": ordinal,
                "type": "event_msg",
                "payload": {
                    "type": "item_completed",
                    "thread_id": thread_id,
                    "turn_id": turn_id,
                    "item": {
                        "type": "AgentMessage",
                        "id": format!("agent-{index}"),
                        "content": [{"type": "Text", "text": format!("answer {index}")}],
                        "phase": "final_answer"
                    }
                }
            }));
            ordinal += 1;
            records.push(json!({
                "ordinal": ordinal,
                "type": "event_msg",
                "payload": {
                    "type": "task_complete",
                    "turn_id": turn_id,
                    "started_at": index,
                    "completed_at": index + 1
                }
            }));
            ordinal += 1;
        }
        let fixture = records
            .into_iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        let (turns, last_ordinal) =
            parse_rollout_chat(thread_id, std::io::Cursor::new(fixture)).unwrap();

        assert_eq!(turns.len(), 60);
        assert_eq!(turns.first().map(|turn| turn.id.as_str()), Some("turn-0"));
        assert_eq!(turns.last().map(|turn| turn.id.as_str()), Some("turn-59"));
        assert_eq!(last_ordinal, ordinal - 1);
    }

    #[test]
    fn workspace_stop_only_accepts_managed_units() {
        assert!(valid_workspace_unit(
            "codex-native-workspace-0123456789abcdef0123456789abcdef.service"
        ));
        assert!(!valid_workspace_unit("ssh.service"));
        assert!(!valid_workspace_unit(
            "codex-native-workspace-../../ssh.service"
        ));
    }

    #[test]
    fn remote_autostart_never_waits_for_the_service_job() {
        assert_eq!(
            remote_autostart_args(true),
            ["--user", "--no-block", "enable", "--now"]
        );
        assert_eq!(
            remote_autostart_args(false),
            ["--user", "--no-block", "disable", "--now"]
        );
    }

    #[test]
    fn memoria_job_summary_counts_pending_and_active_work() {
        let summary = memoria_job_summary(&json!({
            "pipeline": {"queue": {
                "memory": {"pending": 2, "leased": 1},
                "summary": {"pending": 3, "leased": 4}
            }},
            "transcripts": {"capture": {"pending": 5, "processing": 6}}
        }));

        assert_eq!(summary["count"], 21);
        assert_eq!(summary["pending"], 10);
        assert_eq!(summary["active"], 11);
        assert_eq!(summary["memory"], json!({"pending": 2, "active": 1}));
    }
}
