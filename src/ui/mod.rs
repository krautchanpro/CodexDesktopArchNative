#![allow(deprecated)] // ComboBoxText remains a compact native GTK control on GTK 4.18.

use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    env,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    rc::{Rc, Weak},
    sync::{Arc, mpsc::Receiver},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use adw::prelude::*;
use anyhow::Context;
use gtk::{gdk, gio, glib};
use serde_json::{Map, Value, json};
use sourceview::prelude::*;
use vte::prelude::*;

use crate::{
    activity,
    automation::{self, Automation, AutomationHistory, AutomationStore},
    backend::{AccountHub, AppServerHub, BackendEvent, ComputerAction, RemoteAction},
    chatgpt::ChatgptSurface,
    context::{self, CompactionDecision, CompactionGuards, ContextCheckpointReceipt},
    host::HostAction,
    markdown,
    model::{
        AppState, ApprovalRequest, ConnectionState, ContextCompactionMeasurement, PendingKind,
        RemoteState, TaskRuntimeSettings, ThreadSummary, Turn, TurnProgress, WorkspacePage,
    },
    persistence::{
        self, AccountProfile, LeanExperimentSample, MacroExecutorPreferences,
        MacroExperimentSample, OneTimeResetGuard, Project, StoredState,
    },
    plugin::{PluginEntry, app_enabled_key, app_toggle_state, plugin_enabled_key, plugin_mcp_key},
    protocol::{PairingInfo, RpcEnvelope, RpcResponse},
    routing::{
        self, MODE_GEMINI, MODE_MANUAL, MODE_MISTRAL, MODE_OPENROUTER, MODE_QWEN, MODE_QWEN_ASSIST,
        RouteDecision, TaskRoutingState, TokenSavingsReceipt,
    },
};

const REASONING_EFFORT_OPTIONS: &[(&str, &str)] = &[
    ("low", "Light"),
    ("medium", "Medium"),
    ("high", "High"),
    ("xhigh", "Extra High"),
    ("max", "Max"),
    ("ultra", "Ultra"),
];
const SERVICE_TIER_OPTIONS: &[(&str, &str)] = &[("standard", "Standard"), ("priority", "Fast")];
const GPT_SANDBOX_OPTIONS: &[(&str, &str)] = &[
    ("read-only", "Read-only"),
    ("workspace-write", "Workspace write"),
    ("danger-full-access", "Full access"),
    ("external-sandbox", "External sandbox"),
];
const BUDDY_SANDBOX_OPTIONS: &[(&str, &str)] = &[
    ("read-only", "Read-only"),
    ("workspace-write", "Workspace write"),
    ("danger-full-access", "Full access"),
];
const BACKEND_QWEN: &str = "codex-native-qwen";
const BACKEND_GEMINI: &str = "codex-native-gemini";
const BACKEND_OPENROUTER: &str = "codex-native-openrouter";
const BACKEND_MISTRAL: &str = "codex-native-mistral";
const DEFAULT_CODEX_MODEL: &str = "gpt-5.6-terra";
const QWEN_ORCHESTRATOR_MODEL: &str = "gpt-5.6-sol";
const COMPOSER_MODELS: &[(&str, &str)] = &[
    (BACKEND_QWEN, "OpenCode (Qwen Local)"),
    (BACKEND_GEMINI, "Gemini"),
    (BACKEND_OPENROUTER, "OpenRouter Free"),
    (BACKEND_MISTRAL, "Mistral AI"),
    ("gpt-5.5", "GPT-5.5"),
    ("gpt-5.6-luna", "GPT-5.6 Luna"),
    ("gpt-5.6-terra", "GPT-5.6 Terra"),
    ("gpt-5.6-sol", "GPT-5.6 Sol"),
    ("gpt-6-astra", "GPT-6 Astra"),
];
const EXTENSION_RENDER_LIMIT: usize = 120;
const APP_RENDER_PAGE: usize = 24;
const ACTIVITY_FRAMES: &[&str] = &["◴", "◷", "◶", "◵"];
const TRANSCRIPT_PAGE_TURNS: u64 = 20;
const TRANSCRIPT_DETAIL_TURNS: u64 = 1;
const MAX_AUTO_DETAIL_ROLLOUT_BYTES: u64 = 128 * 1024 * 1024;
const TASK_ROLLOVER_WARN_BYTES: u64 = 1024 * 1024 * 1024;
const BACKEND_EVENT_BATCH: usize = 96;
const TRANSCRIPT_RENDER_DEBOUNCE: Duration = Duration::from_millis(100);
const TRANSCRIPT_SCROLL_ANIMATION: Duration = Duration::from_millis(180);
const TRANSCRIPT_SCROLL_FRAME: Duration = Duration::from_millis(16);
const REMOTE_RECOVERY_SAMPLES: u8 = 2;
const REMOTE_RECOVERY_COOLDOWN: Duration = Duration::from_secs(5 * 60);
const MCP_RELOAD_METHOD: &str = "config/mcpServer/reload";

/// Runs the explicitly authorized one-shot reset guard without constructing a
/// GTK application. This lets an updated package protect a long-running older
/// GUI session without restarting that session or its active tasks.
pub fn run_one_time_reset_watch(threshold: u8, idempotency_key: String) -> anyhow::Result<()> {
    if !(1..=100).contains(&threshold) {
        anyhow::bail!("reset threshold must be between 1 and 100 percent");
    }
    uuid::Uuid::parse_str(&idempotency_key).context("invalid reset authorization key")?;
    let guard_path = persistence::config_dir()?.join("one-time-reset-guard.json");
    let mut guard = if guard_path.is_file() {
        persistence::read_json::<OneTimeResetGuard>(guard_path.clone())?
    } else {
        OneTimeResetGuard::default()
    };
    if let Some(existing_key) = guard.idempotency_key.as_deref()
        && existing_key != idempotency_key
    {
        anyhow::bail!(
            "a different one-time reset authorization already exists at {}",
            guard_path.display()
        );
    }
    if guard.idempotency_key.as_deref() == Some(idempotency_key.as_str()) && !guard.armed {
        println!(
            "One-time reset guard already completed: {}",
            guard.outcome.as_deref().unwrap_or("unknown")
        );
        return Ok(());
    }
    let now = chrono::Utc::now().timestamp();
    guard.armed = true;
    guard.threshold_percent = threshold;
    guard.idempotency_key = Some(idempotency_key.clone());
    guard.armed_at.get_or_insert(now);
    guard.completed_at = None;
    if guard.outcome.as_deref() != Some("pending") {
        guard.outcome = Some("armed".into());
    }
    persistence::write_json_atomic(&guard_path, &guard)?;

    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .thread_name("codex-reset-guard")
            .build()?,
    );
    let configured_binary = persistence::load_state().preferences.codex_binary;
    let hub = AppServerHub::spawn(runtime.clone(), configured_binary);
    let mut ready = false;
    let mut rate_request = None;
    let mut consume_request = None;
    let mut next_poll = Instant::now();

    loop {
        let mut events = Vec::new();
        hub.drain_events(BACKEND_EVENT_BATCH, |event| events.push(event));
        for event in events {
            match event {
                BackendEvent::Ready(_) => {
                    ready = true;
                    next_poll = Instant::now();
                }
                BackendEvent::Disconnected(message) => {
                    eprintln!("Reset guard reconnecting after transport loss: {message}");
                    ready = false;
                    rate_request = None;
                    consume_request = None;
                }
                BackendEvent::Message(RpcEnvelope::Request(request))
                    if request.method == "currentTime/read" =>
                {
                    hub.respond(
                        request.id,
                        json!({"currentTimeAt": chrono::Utc::now().timestamp()}),
                    );
                }
                BackendEvent::Message(RpcEnvelope::Notification(notification))
                    if notification.method == "account/rateLimits/updated" =>
                {
                    next_poll = Instant::now();
                }
                BackendEvent::Message(RpcEnvelope::Response(response))
                    if rate_request.as_ref() == Some(&response.id) =>
                {
                    rate_request = None;
                    if let Some(error) = response.error {
                        eprintln!(
                            "Reset guard rate-limit read failed ({}); retrying: {}",
                            error.code, error.message
                        );
                        next_poll = Instant::now() + Duration::from_secs(30);
                        continue;
                    }
                    let limits = response.result.unwrap_or_else(|| json!({}));
                    match one_time_reset_decision(&guard, &limits) {
                        OneTimeResetDecision::Wait => {
                            next_poll = Instant::now() + Duration::from_secs(30);
                        }
                        OneTimeResetDecision::Consume { credit_id } => {
                            guard.attempted_at = Some(chrono::Utc::now().timestamp());
                            guard.outcome = Some("pending".into());
                            persistence::write_json_atomic(&guard_path, &guard)?;
                            let mut params = Map::new();
                            params.insert(
                                "idempotencyKey".into(),
                                Value::String(idempotency_key.clone()),
                            );
                            if let Some(credit_id) = credit_id {
                                params.insert("creditId".into(), Value::String(credit_id));
                            }
                            consume_request = Some(hub.request(
                                "account/rateLimitResetCredit/consume",
                                Value::Object(params),
                            ));
                        }
                    }
                }
                BackendEvent::Message(RpcEnvelope::Response(response))
                    if consume_request.as_ref() == Some(&response.id) =>
                {
                    let outcome = if let Some(error) = response.error {
                        format!("failed:rpc-{}", error.code)
                    } else {
                        response
                            .result
                            .as_ref()
                            .and_then(|result| result.get("outcome"))
                            .and_then(Value::as_str)
                            .unwrap_or("unknown")
                            .to_owned()
                    };
                    guard.armed = false;
                    guard.completed_at = Some(chrono::Utc::now().timestamp());
                    guard.outcome = Some(outcome.clone());
                    persistence::write_json_atomic(&guard_path, &guard)?;
                    hub.shutdown();
                    println!("One-time reset guard completed: {outcome}");
                    return Ok(());
                }
                _ => {}
            }
        }
        if ready
            && rate_request.is_none()
            && consume_request.is_none()
            && Instant::now() >= next_poll
        {
            rate_request = Some(hub.request("account/rateLimits/read", json!({})));
            next_poll = Instant::now() + Duration::from_secs(30);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn schedule_heap_trim() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    glib::idle_add_local_once(|| {
        // SAFETY: malloc_trim has no pointer preconditions and is safe to call
        // after the large temporary protocol value has been dropped.
        unsafe {
            libc::malloc_trim(0);
        }
    });
}

fn should_inhibit_sleep(enabled: bool, remote: &RemoteState, active_task: bool) -> bool {
    enabled && (active_task || matches!(remote, RemoteState::Connecting | RemoteState::Connected))
}

fn remote_recovery_reason(status: &Value, expected_enabled: bool) -> Option<String> {
    if status
        .pointer("/localDaemon/overloaded")
        .and_then(Value::as_bool)
        == Some(true)
    {
        let footprint = status
            .pointer("/localDaemon/footprintMiB")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        return Some(format!(
            "the long-running remote daemon reached {footprint} MiB of RAM and swap"
        ));
    }
    match status.get("status").and_then(Value::as_str) {
        Some("connected") => None,
        Some("disabled") if expected_enabled => {
            Some("the previously enabled Remote daemon stopped unexpectedly".into())
        }
        Some("disabled") => None,
        Some("connecting") => Some("the iOS relay remained in connecting state".into()),
        Some("errored" | "error" | "disconnected") => Some(
            status
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("the iOS relay reported a connection error")
                .to_owned(),
        ),
        Some(other) => Some(format!("the iOS relay reported {other}")),
        None => Some("the remote host returned no connection status".into()),
    }
}

fn task_needs_resume(resumed_threads: &HashSet<String>, thread_id: &str) -> bool {
    !resumed_threads.contains(thread_id)
}

fn task_needs_local_rollout_resume(
    resumed_threads: &HashSet<String>,
    thread_id: &str,
    native_buddy_only: bool,
) -> bool {
    !native_buddy_only && task_needs_resume(resumed_threads, thread_id)
}

fn pending_targets_thread(pending: &PendingKind, thread_id: &str) -> bool {
    match pending {
        PendingKind::OpenThread(id)
        | PendingKind::UnsubscribeThread(id)
        | PendingKind::UpdateThreadSettings(id)
        | PendingKind::Goal(id)
        | PendingKind::GoalRead(id)
        | PendingKind::RollbackThread(id)
        | PendingKind::VoiceAppend(id) => id == thread_id,
        PendingKind::ResumeAndSend { thread_id: id, .. }
        | PendingKind::SendTurn { thread_id: id, .. }
        | PendingKind::SteerTurn { thread_id: id, .. }
        | PendingKind::GoalStatus { thread_id: id, .. }
        | PendingKind::CompactThread { thread_id: id, .. } => id == thread_id,
        _ => false,
    }
}

fn idle_resumed_thread_ids(state: &AppState) -> Vec<String> {
    state
        .resumed_threads
        .iter()
        .filter(|thread_id| {
            state.active_thread_id.as_deref() != Some(thread_id.as_str())
                && state.threads.get(*thread_id).is_none_or(|thread| {
                    !thread.native_buddy_only
                        && !thread_is_running_in_state(thread_id, thread, state)
                })
                && !state
                    .pending
                    .values()
                    .any(|pending| pending_targets_thread(pending, thread_id))
        })
        .cloned()
        .collect()
}

fn is_no_active_turn_to_steer(code: i64, message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    code == -32600 && message.contains("no active turn") && message.contains("steer")
}

pub struct MainWindow;

impl MainWindow {
    pub fn build(
        application: &adw::Application,
        hub: AccountHub,
        tray_available: bool,
        tray_restore_in_progress: Rc<Cell<bool>>,
    ) -> adw::ApplicationWindow {
        let widgets = Widgets::build(application);
        let window = widgets.window.clone();
        let chatgpt = ChatgptSurface::new(
            &widgets.chatgpt_web_host,
            &widgets.window,
            &widgets.toast_overlay,
            &widgets.chatgpt_web_status,
            &widgets.chatgpt_progress,
            &widgets.chatgpt_back,
            &widgets.chatgpt_forward,
            &widgets.chatgpt_reload,
        );
        let controller = Rc::new(Controller {
            weak_self: RefCell::new(Weak::new()),
            hub,
            rollout_activity_rx: activity::spawn_rollout_activity_monitor(),
            state: Rc::new(RefCell::new(AppState::default())),
            stored: Rc::new(RefCell::new(persistence::load_state())),
            automations: Rc::new(RefCell::new(AutomationStore::load())),
            attachments: Rc::new(RefCell::new(Vec::new())),
            terminal_started: Rc::new(Cell::new(false)),
            updating_computer_controls: Rc::new(Cell::new(false)),
            updating_qwen_controls: Rc::new(Cell::new(false)),
            updating_context_controls: Cell::new(false),
            updating_task_controls: Cell::new(false),
            backend_bootstrapped: Cell::new(false),
            login_after_profile_ready: Cell::new(false),
            extensions_bootstrapped: Cell::new(false),
            apps_render_limit: Cell::new(APP_RENDER_PAGE),
            qwen_bootstrapped: Cell::new(false),
            diagnostics_bootstrapped: Cell::new(false),
            memoria_jobs_refresh_pending: Cell::new(false),
            sleep_inhibit_cookie: Cell::new(None),
            activity_phase: Cell::new(0),
            activity_indicators: RefCell::new(Vec::new()),
            smoke_clipboard_texture: RefCell::new(None),
            remote_unhealthy_samples: Cell::new(0),
            remote_expected_enabled: Cell::new(false),
            remote_handoff_pending: Cell::new(false),
            remote_recovery_pending: Cell::new(false),
            remote_recovery_in_progress: Cell::new(false),
            remote_recovery_reason: RefCell::new(None),
            last_remote_recovery: Cell::new(None),
            thread_render_scheduled: Cell::new(false),
            transcript_render_scheduled: Cell::new(false),
            transcript_prepend_pending: Cell::new(false),
            transcript_rows: RefCell::new(Vec::new()),
            transcript_follow_bottom: Cell::new(true),
            transcript_scroll_restoring: Cell::new(false),
            transcript_scroll_animation_generation: Cell::new(0),
            last_transcript_revision: Cell::new(None),
            last_transcript_thread: RefCell::new(None),
            last_transcript_reasoning: Cell::new(false),
            selected_thread_ids: RefCell::new(BTreeSet::new()),
            deferred_shared_thread_open: RefCell::new(None),
            macro_turns: RefCell::new(HashMap::new()),
            macro_pending_samples: RefCell::new(HashMap::new()),
            token_savings_observations: RefCell::new(HashMap::new()),
            local_history_loading: RefCell::new(HashSet::new()),
            local_history_fingerprints: RefCell::new(HashMap::new()),
            chatgpt,
            widgets,
        });
        *controller.weak_self.borrow_mut() = Rc::downgrade(&controller);

        let active_profile_home = {
            let stored = controller.stored.borrow();
            stored
                .account_profiles
                .iter()
                .find(|profile| profile.id == stored.active_account_id)
                .and_then(|profile| profile.codex_home.clone())
        };
        if active_profile_home.is_some() {
            controller.hub.switch_profile(active_profile_home);
        }
        controller.hub.watch_remote_profiles({
            let stored = controller.stored.borrow();
            stored
                .account_profiles
                .iter()
                .map(|profile| profile.codex_home.clone())
                .collect::<Vec<_>>()
        });

        Controller::connect(&controller);
        install_smoke_clipboard_image(&controller);
        if smoke_fixtures_enabled() {
            install_smoke_fixtures(&mut controller.state.borrow_mut());
        }
        controller.state.borrow_mut().page =
            workspace_page_from_name(controller.widgets.stack.visible_child_name().as_deref());
        if controller.state.borrow().page == WorkspacePage::Chatgpt {
            controller.chatgpt.activate();
        }
        controller.apply_preferences();
        controller.populate_projects();
        controller.render_all();
        controller.refresh_memoria_jobs();

        if tray_available {
            let keep_alive = controller.clone();
            window.connect_close_request(move |window| {
                keep_alive.persist();
                keep_alive.chatgpt.unload();
                window.set_visible(false);
                glib::Propagation::Stop
            });

            let keep_alive = controller.clone();
            let weak_window = window.downgrade();
            let tray_restore_in_progress = tray_restore_in_progress.clone();
            window.connect_realize(move |window| {
                let Some(surface) = window.surface() else {
                    return;
                };
                let Ok(toplevel) = surface.downcast::<gdk::Toplevel>() else {
                    return;
                };
                let weak_window = weak_window.clone();
                let keep_alive = keep_alive.clone();
                let tray_restore_in_progress = tray_restore_in_progress.clone();
                toplevel.connect_state_notify(move |toplevel| {
                    if tray_restore_in_progress.get()
                        || !toplevel.state().contains(gdk::ToplevelState::MINIMIZED)
                    {
                        return;
                    }
                    let weak_window = weak_window.clone();
                    let keep_alive = keep_alive.clone();
                    let tray_restore_in_progress = tray_restore_in_progress.clone();
                    if let Some(window) = weak_window.upgrade() {
                        window.unminimize();
                    }
                    glib::timeout_add_local_once(Duration::from_millis(180), move || {
                        let Some(window) = weak_window.upgrade() else {
                            return;
                        };
                        let still_minimized = window
                            .surface()
                            .and_then(|surface| surface.downcast::<gdk::Toplevel>().ok())
                            .is_some_and(|toplevel| {
                                toplevel.state().contains(gdk::ToplevelState::MINIMIZED)
                            });
                        if tray_restore_in_progress.get() {
                            return;
                        }
                        if still_minimized {
                            tracing::warn!(
                                "compositor did not clear minimized state; leaving the window in the taskbar"
                            );
                            return;
                        }
                        keep_alive.persist();
                        keep_alive.chatgpt.unload();
                        window.set_visible(false);
                    });
                });
            });
        } else {
            let keep_alive = controller.clone();
            window.connect_close_request(move |_| {
                keep_alive.persist();
                keep_alive.chatgpt.unload();
                glib::Propagation::Proceed
            });
        }

        let weak = Rc::downgrade(&controller);
        glib::timeout_add_local(Duration::from_millis(40), move || {
            let Some(controller) = weak.upgrade() else {
                return glib::ControlFlow::Break;
            };
            controller.poll_backend();
            glib::ControlFlow::Continue
        });

        let weak = Rc::downgrade(&controller);
        glib::timeout_add_local(Duration::from_millis(250), move || {
            let Some(controller) = weak.upgrade() else {
                return glib::ControlFlow::Break;
            };
            controller.tick_activity_indicators();
            glib::ControlFlow::Continue
        });

        let weak = Rc::downgrade(&controller);
        glib::timeout_add_local(Duration::from_secs(2), move || {
            let Some(controller) = weak.upgrade() else {
                return glib::ControlFlow::Break;
            };
            if controller.widgets.window.is_visible() {
                // The canonical rollout can keep growing while Settings,
                // Diagnostics, or another workspace page is displayed (for
                // example from a remote client). Keep its projection healthy
                // so returning to Chat never exposes a stale partial history.
                controller.refresh_active_local_chat_history();
            }
            glib::ControlFlow::Continue
        });

        let weak = Rc::downgrade(&controller);
        gio::NetworkMonitor::default().connect_network_changed(move |_, available| {
            if !available {
                return;
            }
            let weak = weak.clone();
            glib::timeout_add_local_once(Duration::from_secs(1), move || {
                with_controller(&weak, |controller| {
                    controller.refresh_remote();
                    if controller.state.borrow().connection == ConnectionState::Ready
                        && controller.widgets.thread_search.text().trim().is_empty()
                    {
                        controller.refresh_threads();
                    }
                });
            });
        });

        let weak = Rc::downgrade(&controller);
        glib::timeout_add_local(Duration::from_secs(3), move || {
            let Some(controller) = weak.upgrade() else {
                return glib::ControlFlow::Break;
            };
            controller.refresh_memoria_jobs();
            glib::ControlFlow::Continue
        });

        let weak = Rc::downgrade(&controller);
        glib::timeout_add_local(Duration::from_secs(30), move || {
            let Some(controller) = weak.upgrade() else {
                return glib::ControlFlow::Break;
            };
            let should_refresh = controller.remote_expected_enabled.get()
                || controller.state.borrow().page == WorkspacePage::Remote
                || matches!(controller.state.borrow().remote, RemoteState::Connected);
            if should_refresh {
                controller.refresh_remote();
            }
            let qwen_should_refresh = controller.state.borrow().page == WorkspacePage::QwenBuddy
                || controller
                    .stored
                    .borrow()
                    .preferences
                    .qwen_buddy
                    .routing_enabled;
            if qwen_should_refresh {
                controller.refresh_qwen();
            }
            let reset_guard_armed = controller.stored.borrow().one_time_reset_guard.armed;
            if reset_guard_armed && controller.state.borrow().connection == ConnectionState::Ready {
                controller.refresh_account_rate_limits();
            }
            glib::ControlFlow::Continue
        });

        let weak = Rc::downgrade(&controller);
        glib::timeout_add_local(Duration::from_secs(120), move || {
            let Some(controller) = weak.upgrade() else {
                return glib::ControlFlow::Break;
            };
            if controller.state.borrow().connection == ConnectionState::Ready
                && controller.widgets.thread_search.text().trim().is_empty()
            {
                controller.refresh_threads();
            }
            glib::ControlFlow::Continue
        });

        let weak = Rc::downgrade(&controller);
        glib::timeout_add_local(Duration::from_secs(300), move || {
            let Some(controller) = weak.upgrade() else {
                return glib::ControlFlow::Break;
            };
            if controller.state.borrow().connection == ConnectionState::Ready {
                controller.refresh_account_usage();
            }
            glib::ControlFlow::Continue
        });

        window
    }
}

struct Widgets {
    window: adw::ApplicationWindow,
    toast_overlay: adw::ToastOverlay,
    stack: gtk::Stack,
    status_icon: gtk::Image,
    status_label: gtk::Label,
    memoria_jobs_label: gtk::Label,
    usage_label: gtk::Label,
    weekly_progress: gtk::ProgressBar,
    five_hour_progress: gtk::ProgressBar,
    reset_credit_button: gtk::Button,
    project_combo: gtk::ComboBoxText,
    projects_status: gtk::Label,
    projects_open_editor: gtk::Button,
    projects_reveal: gtk::Button,
    projects_list: gtk::Box,
    sites_status: gtk::Label,
    sites_new_task: gtk::Button,
    sites_open_plugins: gtk::Button,
    thread_search: gtk::SearchEntry,
    thread_archived_toggle: gtk::CheckButton,
    thread_select_toggle: gtk::ToggleButton,
    thread_bulk_bar: gtk::Box,
    thread_bulk_count: gtk::Label,
    thread_select_all: gtk::Button,
    thread_bulk_archive: gtk::Button,
    thread_bulk_delete: gtk::Button,
    thread_list: gtk::ListBox,
    new_task: gtk::Button,
    chatgpt_status: gtk::Label,
    chatgpt_web_status: gtk::Label,
    chatgpt_web_host: gtk::Box,
    chatgpt_progress: gtk::ProgressBar,
    chatgpt_back: gtk::Button,
    chatgpt_forward: gtk::Button,
    chatgpt_home: gtk::Button,
    chatgpt_reload: gtk::Button,
    chatgpt_unload: gtk::Button,
    task_title: gtk::Label,
    transcript: gtk::Box,
    transcript_scroller: gtk::ScrolledWindow,
    task_progress: gtk::Box,
    approval_revealer: gtk::Revealer,
    approval_title: gtk::Label,
    approval_detail: gtk::Label,
    approval_once: gtk::Button,
    approval_session: gtk::Button,
    approval_deny: gtk::Button,
    composer: gtk::TextView,
    attachments_label: gtk::Label,
    attach_button: gtk::Button,
    paste_button: gtk::Button,
    dictate_button: gtk::Button,
    read_aloud_button: gtk::Button,
    subagents_toggle: gtk::ToggleButton,
    send_button: gtk::Button,
    stop_button: gtk::Button,
    model_combo: gtk::ComboBoxText,
    effort_combo: gtk::ComboBoxText,
    speed_combo: gtk::ComboBoxText,
    sandbox_combo: gtk::ComboBoxText,
    approval_combo: gtk::ComboBoxText,
    rename_button: gtk::Button,
    pin_button: gtk::Button,
    goal_button: gtk::MenuButton,
    goal_summary: gtk::Label,
    goal_detail: gtk::Label,
    goal_reason: gtk::Label,
    goal_edit: gtk::Button,
    goal_stop: gtk::Button,
    goal_resume: gtk::Button,
    goal_complete: gtk::Button,
    goal_clear: gtk::Button,
    compact_button: gtk::Button,
    rollback_button: gtk::Button,
    fork_button: gtk::Button,
    archive_button: gtk::Button,
    unarchive_button: gtk::Button,
    delete_button: gtk::Button,
    load_older_button: gtk::Button,
    load_activity_button: gtk::Button,
    terminal: vte::Terminal,
    terminal_status: gtk::Label,
    terminal_start: gtk::Button,
    workspace_status: gtk::Label,
    workspace_network: gtk::CheckButton,
    workspace_memory: gtk::SpinButton,
    workspace_doctor: gtk::Button,
    workspace_launch: gtk::Button,
    workspace_stop: gtk::Button,
    lean_context_status: gtk::Label,
    lean_context_enabled: gtk::Switch,
    lean_context_mode: gtk::ComboBoxText,
    lean_context_threshold: gtk::SpinButton,
    lean_context_evidence_budget: gtk::SpinButton,
    lean_context_command_budget: gtk::SpinButton,
    lean_context_condensation_target: gtk::SpinButton,
    lean_context_qwen: gtk::Switch,
    lean_context_checkpoint: gtk::Button,
    lean_context_latest: gtk::Label,
    appshot_status: gtk::Label,
    appshot_capture: gtk::Button,
    appshot_attach: gtk::Button,
    record_title: gtk::Entry,
    record_note: gtk::Entry,
    record_status: gtk::Label,
    record_start: gtk::Button,
    record_frame: gtk::Button,
    record_stop: gtk::Button,
    record_install: gtk::Button,
    record_reveal: gtk::Button,
    browser_url: gtk::Entry,
    browser_isolated: gtk::CheckButton,
    browser_open: gtk::Button,
    context_open_editor: gtk::Button,
    context_reveal: gtk::Button,
    extensions_search: gtk::SearchEntry,
    extensions_installed_only: gtk::CheckButton,
    extensions_refresh: gtk::Button,
    marketplace_add: gtk::Button,
    marketplace_upgrade: gtk::Button,
    extensions_list: gtk::Box,
    qwen_status: gtk::Label,
    qwen_detail: gtk::Label,
    qwen_gpu: gtk::Label,
    qwen_usage: gtk::Label,
    qwen_routing_switch: gtk::Switch,
    qwen_luna_check: gtk::CheckButton,
    qwen_condenser_check: gtk::CheckButton,
    qwen_sol_check: gtk::CheckButton,
    qwen_gpu_guard_switch: gtk::Switch,
    qwen_refresh: gtk::Button,
    qwen_open_extensions: gtk::Button,
    qwen_open_terminal: gtk::Button,
    computer_status: gtk::Label,
    computer_detail: gtk::Label,
    computer_capabilities: gtk::Box,
    computer_plugin_switch: gtk::Switch,
    computer_server_switch: gtk::Switch,
    computer_approval_combo: gtk::ComboBoxText,
    computer_doctor: gtk::Button,
    computer_setup: gtk::Button,
    computer_new_task: gtk::Button,
    automations_list: gtk::Box,
    automation_add: gtk::Button,
    remote_status: gtk::Label,
    remote_detail: gtk::Label,
    remote_pairing: gtk::Label,
    remote_clients: gtk::Box,
    remote_enable: gtk::Button,
    remote_disable: gtk::Button,
    remote_pair: gtk::Button,
    remote_restart: gtk::Button,
    remote_refresh: gtk::Button,
    diagnostics_summary: gtk::Label,
    diagnostics_list: gtk::Box,
    diagnostics_refresh: gtk::Button,
    update_check: gtk::Button,
    update_apply: gtk::Button,
    update_rollback: gtk::Button,
    update_label: gtk::Label,
    remote_doctor_button: gtk::Button,
    remote_doctor_label: gtk::Label,
    theme_combo: gtk::ComboBoxText,
    binary_entry: gtk::Entry,
    browser_entry: gtk::Entry,
    editor_entry: gtk::Entry,
    notification_switch: gtk::Switch,
    reasoning_switch: gtk::Switch,
    remote_autostart_switch: gtk::Switch,
    keep_awake_switch: gtk::Switch,
    resource_monitor_switch: gtk::Switch,
    account_menu: gtk::MenuButton,
    account_menu_summary: gtk::Label,
    account_profiles: gtk::Box,
    account_add: gtk::Button,
    account_settings: gtk::Button,
    account_label: gtk::Label,
    account_usage_label: gtk::Label,
    account_refresh: gtk::Button,
    login_button: gtk::Button,
    logout_button: gtk::Button,
    settings_save: gtk::Button,
}

impl Widgets {
    fn build(application: &adw::Application) -> Self {
        let window = adw::ApplicationWindow::builder()
            .application(application)
            .title("Codex Native")
            .default_width(1280)
            .default_height(820)
            .width_request(900)
            .height_request(620)
            .build();

        let toast_overlay = adw::ToastOverlay::new();
        let outer = gtk::Box::new(gtk::Orientation::Vertical, 0);
        toast_overlay.set_child(Some(&outer));
        window.set_content(Some(&toast_overlay));

        let header = adw::HeaderBar::new();
        let brand = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        let chatgpt_icon = branding_image("chatgpt-symbol.ico", "ChatGPT-powered Codex Native");
        brand.append(&chatgpt_icon);
        let brand_title = gtk::Label::new(Some("Codex Native"));
        brand_title.add_css_class("sidebar-title");
        brand.append(&brand_title);
        let arch_icon = branding_image("arch-linux-symbol.svg", "Codex Native for Arch Linux");
        brand.append(&arch_icon);
        header.set_title_widget(Some(&brand));

        let status_box = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        let status_icon = gtk::Image::from_icon_name("network-transmit-receive-symbolic");
        status_icon.add_css_class("connection-busy");
        let status_label = gtk::Label::new(Some("Connecting"));
        status_label.add_css_class("caption");
        let memoria_jobs_label = gtk::Label::new(Some("Memoria jobs: —"));
        memoria_jobs_label.add_css_class("caption");
        memoria_jobs_label.set_tooltip_text(Some("Checking Memoria's live work queues"));
        let usage_label = gtk::Label::new(None);
        usage_label.add_css_class("caption");
        let weekly_progress = gtk::ProgressBar::new();
        weekly_progress.set_width_request(150);
        weekly_progress.set_show_text(true);
        weekly_progress.set_text(Some("Weekly —"));
        weekly_progress.set_tooltip_text(Some("Weekly Codex allowance remaining"));
        let five_hour_progress = gtk::ProgressBar::new();
        five_hour_progress.set_width_request(150);
        five_hour_progress.set_show_text(true);
        five_hour_progress.set_text(Some("5-hour —"));
        five_hour_progress.set_tooltip_text(Some("Five-hour Codex allowance remaining"));
        let reset_credit_button = gtk::Button::with_label("Resets —");
        reset_credit_button.add_css_class("flat");
        reset_credit_button.set_sensitive(false);
        reset_credit_button.set_tooltip_text(Some("Weekly-limit reset credits"));
        let account_menu = gtk::MenuButton::builder()
            .label("Account")
            .tooltip_text("Account menu")
            .build();
        account_menu.add_css_class("flat");
        account_menu.update_property(&[
            gtk::accessible::Property::Label("Account menu"),
            gtk::accessible::Property::HasPopup(true),
        ]);
        let account_popover = gtk::Popover::new();
        let account_menu_box = gtk::Box::new(gtk::Orientation::Vertical, 6);
        account_menu_box.set_width_request(260);
        account_menu_box.set_margin_top(10);
        account_menu_box.set_margin_bottom(10);
        account_menu_box.set_margin_start(10);
        account_menu_box.set_margin_end(10);
        let account_menu_summary = gtk::Label::new(Some("Checking account…"));
        account_menu_summary.set_xalign(0.0);
        account_menu_summary.set_wrap(true);
        account_menu_summary.add_css_class("heading");
        let account_menu_detail = gtk::Label::new(Some(
            "Profiles remain signed in separately. Remote hosts keep running when you switch.",
        ));
        account_menu_detail.set_xalign(0.0);
        account_menu_detail.set_wrap(true);
        account_menu_detail.add_css_class("caption");
        let account_profiles = gtk::Box::new(gtk::Orientation::Vertical, 4);
        let account_add = gtk::Button::with_label("Add ChatGPT account…");
        account_add.set_halign(gtk::Align::Fill);
        let account_settings = gtk::Button::with_label("Account settings");
        account_settings.set_halign(gtk::Align::Fill);
        account_menu_box.append(&account_menu_summary);
        account_menu_box.append(&account_menu_detail);
        account_menu_box.append(&gtk::Separator::new(gtk::Orientation::Horizontal));
        account_menu_box.append(&account_profiles);
        account_menu_box.append(&account_add);
        account_menu_box.append(&account_settings);
        account_popover.set_child(Some(&account_menu_box));
        account_menu.set_popover(Some(&account_popover));
        status_box.append(&status_icon);
        status_box.append(&status_label);
        status_box.append(&memoria_jobs_label);
        status_box.append(&usage_label);
        status_box.append(&gtk::Separator::new(gtk::Orientation::Vertical));
        status_box.append(&weekly_progress);
        status_box.append(&five_hour_progress);
        status_box.append(&reset_credit_button);
        status_box.append(&account_menu);
        header.pack_end(&status_box);
        outer.append(&header);

        let paned = gtk::Paned::new(gtk::Orientation::Horizontal);
        paned.set_wide_handle(false);
        paned.set_position(292);
        paned.set_vexpand(true);
        outer.append(&paned);

        let sidebar = gtk::Box::new(gtk::Orientation::Vertical, 8);
        sidebar.set_width_request(240);
        sidebar.add_css_class("sidebar");
        sidebar.set_margin_top(10);
        sidebar.set_margin_bottom(10);
        sidebar.set_margin_start(10);
        sidebar.set_margin_end(10);
        paned.set_start_child(Some(&sidebar));
        paned.set_shrink_start_child(false);

        let project_row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        let project_combo = gtk::ComboBoxText::new();
        project_combo.set_hexpand(true);
        project_combo.set_tooltip_text(Some("Working folder"));
        project_row.append(&project_combo);
        let add_project = icon_button("folder-new-symbolic", "Add project folder");
        project_row.append(&add_project);
        sidebar.append(&project_row);

        let task_create_row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        let new_task = labeled_icon_button("New task", "list-add-symbolic");
        new_task.add_css_class("suggested-action");
        new_task.set_hexpand(true);
        task_create_row.append(&new_task);
        let thread_select_toggle = gtk::ToggleButton::with_label("Select tasks");
        thread_select_toggle.set_tooltip_text(Some("Select multiple tasks"));
        thread_select_toggle.update_property(&[gtk::accessible::Property::Label("Select tasks")]);
        task_create_row.append(&thread_select_toggle);
        sidebar.append(&task_create_row);

        let thread_search = gtk::SearchEntry::builder()
            .placeholder_text("Search tasks")
            .build();
        let thread_filter_row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        thread_search.set_hexpand(true);
        thread_filter_row.append(&thread_search);
        let thread_archived_toggle = gtk::CheckButton::builder()
            .label("Archived")
            .tooltip_text("Show archived tasks")
            .build();
        thread_filter_row.append(&thread_archived_toggle);
        sidebar.append(&thread_filter_row);

        let thread_bulk_bar = gtk::Box::new(gtk::Orientation::Horizontal, 4);
        thread_bulk_bar.add_css_class("task-bulk-bar");
        thread_bulk_bar.set_visible(false);
        let thread_bulk_count = gtk::Label::new(Some("0 selected"));
        thread_bulk_count.set_xalign(0.0);
        thread_bulk_count.set_hexpand(true);
        thread_bulk_count.add_css_class("caption");
        thread_bulk_bar.append(&thread_bulk_count);
        let thread_select_all = gtk::Button::with_label("All");
        thread_select_all.set_tooltip_text(Some("Select all visible tasks"));
        thread_bulk_bar.append(&thread_select_all);
        let thread_bulk_archive = icon_button("folder-symbolic", "Archive selected tasks");
        thread_bulk_bar.append(&thread_bulk_archive);
        let thread_bulk_delete = icon_button("user-trash-symbolic", "Delete selected tasks");
        thread_bulk_delete.add_css_class("destructive-action");
        thread_bulk_bar.append(&thread_bulk_delete);
        sidebar.append(&thread_bulk_bar);

        let thread_list = gtk::ListBox::new();
        thread_list.set_selection_mode(gtk::SelectionMode::None);
        thread_list.add_css_class("navigation-sidebar");
        let thread_scroller = gtk::ScrolledWindow::builder()
            .vexpand(true)
            .hscrollbar_policy(gtk::PolicyType::Never)
            .child(&thread_list)
            .build();

        let sidebar_split = gtk::Paned::new(gtk::Orientation::Vertical);
        sidebar_split.set_vexpand(true);
        sidebar_split.set_wide_handle(false);
        sidebar_split.set_start_child(Some(&thread_scroller));
        sidebar_split.set_resize_start_child(true);
        sidebar_split.set_shrink_start_child(true);

        let stack = gtk::Stack::builder()
            .hexpand(true)
            .vexpand(true)
            .transition_type(gtk::StackTransitionType::Crossfade)
            .build();
        paned.set_end_child(Some(&stack));
        paned.set_resize_end_child(true);
        paned.set_shrink_end_child(false);

        let nav = gtk::Box::new(gtk::Orientation::Vertical, 2);
        let mut nav_buttons = Vec::new();
        for (label, icon, page) in [
            ("Chat", "mail-message-new-symbolic", "chat"),
            ("ChatGPT", "mail-message-new-symbolic", "chatgpt"),
            ("Projects", "folder-symbolic", "projects"),
            ("Sites", "web-browser-symbolic", "sites"),
            ("Scheduled", "alarm-symbolic", "automations"),
            ("Terminal", "utilities-terminal-symbolic", "terminal"),
            ("Agent workspace", "system-run-symbolic", "agent-workspace"),
            ("Context", "camera-photo-symbolic", "context"),
            ("Extensions", "applications-system-symbolic", "extensions"),
            ("OpenCode", "system-run-symbolic", "qwen-buddy"),
            ("Computer use", "input-mouse-symbolic", "computer-use"),
            ("Remote access", "network-server-symbolic", "remote"),
            ("Diagnostics", "dialog-information-symbolic", "diagnostics"),
            ("Settings", "preferences-system-symbolic", "settings"),
        ] {
            let button = labeled_icon_button(label, icon);
            button.set_halign(gtk::Align::Fill);
            button.add_css_class("flat");
            button.add_css_class("nav-item");
            let stack_clone = stack.clone();
            button.connect_clicked(move |_| stack_clone.set_visible_child_name(page));
            nav.append(&button);
            nav_buttons.push((page.to_owned(), button));
        }
        let nav_buttons = Rc::new(nav_buttons);
        let nav_buttons_for_stack = nav_buttons.clone();
        stack.connect_visible_child_name_notify(move |stack| {
            let visible = stack.visible_child_name();
            for (page, button) in nav_buttons_for_stack.iter() {
                let selected = visible.as_deref() == Some(page.as_str());
                button.update_state(&[gtk::accessible::State::Selected(Some(selected))]);
                if selected {
                    button.add_css_class("nav-item-active");
                } else {
                    button.remove_css_class("nav-item-active");
                }
            }
        });
        let nav_scroller = gtk::ScrolledWindow::builder()
            .vscrollbar_policy(gtk::PolicyType::Automatic)
            .hscrollbar_policy(gtk::PolicyType::Never)
            .min_content_height(112)
            .max_content_height(320)
            .propagate_natural_height(true)
            .overlay_scrolling(false)
            .child(&nav)
            .build();
        nav_scroller.update_property(&[
            gtk::accessible::Property::Label("Workspace navigation"),
            gtk::accessible::Property::Description(
                "Scrollable navigation from Chat through Settings",
            ),
        ]);

        let nav_revealer = gtk::Revealer::builder()
            .transition_type(gtk::RevealerTransitionType::SlideDown)
            .reveal_child(true)
            .child(&nav_scroller)
            .build();
        let nav_panel = gtk::Box::new(gtk::Orientation::Vertical, 4);
        nav_panel.add_css_class("sidebar-navigation");
        let nav_header = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        let nav_title = gtk::Label::new(Some("Menu"));
        nav_title.set_xalign(0.0);
        nav_title.set_hexpand(true);
        nav_title.add_css_class("sidebar-title");
        nav_header.append(&nav_title);
        let nav_toggle = icon_button("pan-down-symbolic", "Collapse workspace navigation");
        nav_toggle.set_tooltip_text(Some("Collapse workspace navigation"));
        nav_toggle.update_state(&[gtk::accessible::State::Expanded(Some(true))]);
        nav_header.append(&nav_toggle);
        nav_panel.append(&gtk::Separator::new(gtk::Orientation::Horizontal));
        nav_panel.append(&nav_header);
        nav_panel.append(&nav_revealer);

        let nav_revealer_for_toggle = nav_revealer.clone();
        nav_toggle.connect_clicked(move |button| {
            let collapsing = nav_revealer_for_toggle.reveals_child();
            nav_revealer_for_toggle.set_reveal_child(!collapsing);
            let (icon, label) = if collapsing {
                ("pan-up-symbolic", "Expand workspace navigation")
            } else {
                ("pan-down-symbolic", "Collapse workspace navigation")
            };
            button.set_icon_name(icon);
            button.set_tooltip_text(Some(label));
            button.update_property(&[gtk::accessible::Property::Label(label)]);
            button.update_state(&[gtk::accessible::State::Expanded(Some(!collapsing))]);
        });

        sidebar_split.set_end_child(Some(&nav_panel));
        sidebar_split.set_resize_end_child(false);
        sidebar_split.set_shrink_end_child(true);
        sidebar.append(&sidebar_split);

        let chat = gtk::Box::new(gtk::Orientation::Vertical, 0);
        let task_bar = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        task_bar.add_css_class("workspace-toolbar");
        let task_title = gtk::Label::new(Some("New task"));
        task_title.set_xalign(0.0);
        task_title.set_ellipsize(gtk::pango::EllipsizeMode::End);
        task_title.set_hexpand(true);
        task_title.add_css_class("heading");
        task_bar.append(&task_title);
        let pin_button = icon_button("non-starred-symbolic", "Pin or unpin task");
        let goal_button = gtk::MenuButton::builder()
            .icon_name("emblem-important-symbolic")
            .tooltip_text("Goal selector")
            .build();
        goal_button.update_property(&[
            gtk::accessible::Property::Label("Goal selector"),
            gtk::accessible::Property::Description(
                "Create, inspect, stop, resume, complete, or clear this task's goal",
            ),
            gtk::accessible::Property::HasPopup(true),
        ]);
        let goal_popover = gtk::Popover::new();
        let goal_menu = gtk::Box::new(gtk::Orientation::Vertical, 6);
        goal_menu.set_width_request(320);
        goal_menu.set_margin_top(10);
        goal_menu.set_margin_bottom(10);
        goal_menu.set_margin_start(10);
        goal_menu.set_margin_end(10);
        let goal_summary = gtk::Label::new(Some("No goal for this task"));
        goal_summary.set_xalign(0.0);
        goal_summary.set_wrap(true);
        goal_summary.add_css_class("heading");
        goal_menu.append(&goal_summary);
        let goal_detail = gtk::Label::new(None);
        goal_detail.set_xalign(0.0);
        goal_detail.set_wrap(true);
        goal_detail.add_css_class("caption");
        goal_menu.append(&goal_detail);
        let goal_reason = gtk::Label::new(None);
        goal_reason.set_xalign(0.0);
        goal_reason.set_wrap(true);
        goal_reason.add_css_class("warning");
        goal_menu.append(&goal_reason);
        goal_menu.append(&gtk::Separator::new(gtk::Orientation::Horizontal));
        let goal_edit = gtk::Button::with_label("Create goal…");
        let goal_stop = gtk::Button::with_label("Stop goal");
        let goal_resume = gtk::Button::with_label("Resume goal");
        goal_resume.add_css_class("suggested-action");
        let goal_complete = gtk::Button::with_label("Mark goal complete");
        let goal_clear = gtk::Button::with_label("Clear goal…");
        goal_clear.add_css_class("destructive-action");
        for action in [
            &goal_edit,
            &goal_stop,
            &goal_resume,
            &goal_complete,
            &goal_clear,
        ] {
            action.set_halign(gtk::Align::Fill);
            goal_menu.append(action);
        }
        goal_stop.set_visible(false);
        goal_resume.set_visible(false);
        goal_complete.set_visible(false);
        goal_clear.set_visible(false);
        goal_popover.set_child(Some(&goal_menu));
        goal_button.set_popover(Some(&goal_popover));
        let compact_button = icon_button("view-restore-symbolic", "Compact task context");
        let rollback_button = icon_button("edit-undo-symbolic", "Roll back last conversation turn");
        let rename_button = icon_button("document-edit-symbolic", "Rename task");
        let fork_button = icon_button("edit-copy-symbolic", "Fork task");
        let archive_button = icon_button("folder-symbolic", "Archive task");
        let delete_button = icon_button("user-trash-symbolic", "Delete task");
        let unarchive_button = icon_button("folder-new-symbolic", "Restore archived task");
        unarchive_button.set_visible(false);
        task_bar.append(&pin_button);
        task_bar.append(&goal_button);
        task_bar.append(&compact_button);
        task_bar.append(&rollback_button);
        task_bar.append(&rename_button);
        task_bar.append(&fork_button);
        task_bar.append(&archive_button);
        task_bar.append(&unarchive_button);
        task_bar.append(&delete_button);
        chat.append(&task_bar);

        let approval_revealer = gtk::Revealer::builder()
            .transition_type(gtk::RevealerTransitionType::SlideDown)
            .build();
        let approval_box = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        approval_box.add_css_class("approval-bar");
        let approval_text = gtk::Box::new(gtk::Orientation::Vertical, 2);
        approval_text.set_hexpand(true);
        let approval_title = gtk::Label::new(Some("Approval requested"));
        approval_title.set_xalign(0.0);
        approval_title.add_css_class("heading");
        let approval_detail = gtk::Label::new(None);
        approval_detail.set_xalign(0.0);
        approval_detail.set_ellipsize(gtk::pango::EllipsizeMode::End);
        approval_detail.add_css_class("caption");
        approval_text.append(&approval_title);
        approval_text.append(&approval_detail);
        approval_box.append(&approval_text);
        let approval_deny = gtk::Button::with_label("Deny");
        let approval_session = gtk::Button::with_label("Allow for task");
        let approval_once = gtk::Button::with_label("Allow once");
        approval_once.add_css_class("suggested-action");
        approval_box.append(&approval_deny);
        approval_box.append(&approval_session);
        approval_box.append(&approval_once);
        approval_revealer.set_child(Some(&approval_box));
        chat.append(&approval_revealer);

        let load_older_button = gtk::Button::with_label("Load older turns");
        load_older_button.set_halign(gtk::Align::Center);
        load_older_button.set_visible(false);
        let load_activity_button = gtk::Button::with_label("Load previous activity");
        load_activity_button.set_halign(gtk::Align::Center);
        load_activity_button.set_visible(false);
        let history_controls = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        history_controls.set_halign(gtk::Align::Center);
        history_controls.set_margin_top(8);
        history_controls.append(&load_older_button);
        history_controls.append(&load_activity_button);
        chat.append(&history_controls);

        let transcript = gtk::Box::new(gtk::Orientation::Vertical, 8);
        transcript.add_css_class("transcript");
        transcript.set_margin_top(18);
        transcript.set_margin_bottom(18);
        let transcript_clamp = adw::Clamp::builder()
            .maximum_size(1080)
            .tightening_threshold(760)
            .child(&transcript)
            .build();
        transcript_clamp.add_css_class("transcript-clamp");
        let transcript_scroller = gtk::ScrolledWindow::builder()
            .vexpand(true)
            .hscrollbar_policy(gtk::PolicyType::Never)
            .child(&transcript_clamp)
            .build();
        transcript_scroller.update_property(&[gtk::accessible::Property::Label("Task transcript")]);
        chat.append(&transcript_scroller);

        // Keep live task status outside the scrolling transcript. It remains
        // centered directly above the composer even as messages are appended.
        let task_progress = gtk::Box::new(gtk::Orientation::Vertical, 0);
        task_progress.set_halign(gtk::Align::Center);
        task_progress.set_visible(false);
        chat.append(&task_progress);

        let composer_shell = gtk::Box::new(gtk::Orientation::Vertical, 6);
        composer_shell.add_css_class("composer-shell");
        let attachments_label = gtk::Label::new(None);
        attachments_label.set_xalign(0.0);
        attachments_label.set_visible(false);
        attachments_label.add_css_class("caption");
        composer_shell.append(&attachments_label);
        let composer = gtk::TextView::new();
        composer.set_wrap_mode(gtk::WrapMode::WordChar);
        composer.set_top_margin(7);
        composer.set_bottom_margin(7);
        composer.set_left_margin(7);
        composer.set_right_margin(7);
        composer.set_accepts_tab(false);
        composer.set_accessible_role(gtk::AccessibleRole::TextBox);
        composer.update_property(&[gtk::accessible::Property::Label("Task message")]);
        composer.set_tooltip_text(Some(
            "Enter sends · Shift+Enter adds a new line · paste images with Ctrl+V",
        ));
        let composer_scroller = gtk::ScrolledWindow::builder()
            .min_content_height(72)
            .max_content_height(168)
            .propagate_natural_height(true)
            .hscrollbar_policy(gtk::PolicyType::Never)
            .child(&composer)
            .build();
        let composer_overlay = gtk::Overlay::new();
        composer_overlay.set_child(Some(&composer_scroller));
        let composer_placeholder = gtk::Label::new(Some("Ask Codex to build, change, or explain…"));
        composer_placeholder.set_halign(gtk::Align::Start);
        composer_placeholder.set_valign(gtk::Align::Start);
        composer_placeholder.set_margin_top(10);
        composer_placeholder.set_margin_start(12);
        composer_placeholder.set_can_target(false);
        composer_placeholder.add_css_class("composer-placeholder");
        composer_overlay.add_overlay(&composer_placeholder);
        let placeholder = composer_placeholder.clone();
        composer.buffer().connect_changed(move |buffer| {
            placeholder.set_visible(buffer.char_count() == 0);
        });
        composer_shell.append(&composer_overlay);

        let composer_controls = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        let attach_button = icon_button("mail-attachment-symbolic", "Attach files or images");
        composer_controls.append(&attach_button);
        let paste_button =
            icon_button("edit-paste-symbolic", "Paste image or files from clipboard");
        composer_controls.append(&paste_button);
        let dictate_button = icon_button(
            "audio-input-microphone-symbolic",
            "Record twelve seconds of native dictation",
        );
        let read_aloud_button = icon_button(
            "audio-speakers-symbolic",
            "Read the latest Codex message aloud",
        );
        composer_controls.append(&dictate_button);
        composer_controls.append(&read_aloud_button);
        let model_combo = compact_combo(&[("", "Default model")]);
        model_combo.set_tooltip_text(Some("Model"));
        composer_controls.append(&model_combo);
        let effort_combo = compact_combo(REASONING_EFFORT_OPTIONS);
        effort_combo.set_tooltip_text(Some("Reasoning effort"));
        composer_controls.append(&effort_combo);
        let speed_combo = compact_combo(SERVICE_TIER_OPTIONS);
        speed_combo.set_tooltip_text(Some(
            "Response speed. Fast is 1.5× speed and uses more allowance.",
        ));
        composer_controls.append(&speed_combo);
        let sandbox_combo = compact_combo(GPT_SANDBOX_OPTIONS);
        sandbox_combo.set_tooltip_text(Some("Sandbox"));
        composer_controls.append(&sandbox_combo);
        let approval_combo = compact_combo(&[
            ("untrusted", "Ask often"),
            ("on-request", "On request"),
            ("never", "Never ask"),
            ("granular", "Custom approvals"),
        ]);
        approval_combo.set_tooltip_text(Some("Approval policy"));
        composer_controls.append(&approval_combo);
        let subagents_toggle = gtk::ToggleButton::with_label("Subagents: On");
        subagents_toggle.set_active(true);
        subagents_toggle.add_css_class("composer-toggle");
        subagents_toggle.set_tooltip_text(Some(
            "Subagents are allowed for this task. Click to disable local and cloud delegation.",
        ));
        subagents_toggle.update_property(&[gtk::accessible::Property::Label(
            "Allow subagents for this task",
        )]);
        composer_controls.append(&subagents_toggle);
        let spacer = gtk::Box::new(gtk::Orientation::Horizontal, 0);
        spacer.set_hexpand(true);
        composer_controls.append(&spacer);
        let stop_button = gtk::Button::builder()
            .icon_name("media-playback-stop-symbolic")
            .tooltip_text("Stop current turn")
            .sensitive(false)
            .build();
        composer_controls.append(&stop_button);
        let send_button = labeled_icon_button("Send", "mail-send-symbolic");
        send_button.add_css_class("suggested-action");
        composer_controls.append(&send_button);
        composer_shell.append(&composer_controls);
        let composer_clamp = adw::Clamp::builder()
            .maximum_size(1120)
            .tightening_threshold(780)
            .child(&composer_shell)
            .build();
        composer_clamp.add_css_class("composer-clamp");
        chat.append(&composer_clamp);
        stack.add_named(&chat, Some("chat"));

        let chatgpt_page = page_shell(
            "ChatGPT",
            "Use ChatGPT Pro chat and ChatGPT Voice inside this window. Only this page uses a lazy, isolated WebKit process; Codex tasks and Markdown remain native GTK.",
        );
        let chatgpt_status = gtk::Label::new(Some("Checking your ChatGPT plan…"));
        chatgpt_status.set_xalign(0.0);
        chatgpt_status.set_wrap(true);
        chatgpt_status.add_css_class("caption");
        chatgpt_status.set_margin_start(18);
        chatgpt_status.set_margin_end(18);
        chatgpt_page.append(&chatgpt_status);

        let chatgpt_toolbar = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        chatgpt_toolbar.set_margin_top(10);
        chatgpt_toolbar.set_margin_start(18);
        chatgpt_toolbar.set_margin_end(18);
        let chatgpt_back = icon_button("go-previous-symbolic", "ChatGPT back");
        let chatgpt_forward = icon_button("go-next-symbolic", "ChatGPT forward");
        let chatgpt_home = labeled_icon_button("ChatGPT home", "go-home-symbolic");
        let chatgpt_reload = labeled_icon_button("Reload ChatGPT", "view-refresh-symbolic");
        let chatgpt_unload = labeled_icon_button("Unload ChatGPT", "media-playback-stop-symbolic");
        chatgpt_toolbar.append(&chatgpt_back);
        chatgpt_toolbar.append(&chatgpt_forward);
        chatgpt_toolbar.append(&chatgpt_home);
        chatgpt_toolbar.append(&chatgpt_reload);
        let chatgpt_toolbar_spacer = gtk::Box::new(gtk::Orientation::Horizontal, 0);
        chatgpt_toolbar_spacer.set_hexpand(true);
        chatgpt_toolbar.append(&chatgpt_toolbar_spacer);
        chatgpt_toolbar.append(&chatgpt_unload);
        chatgpt_page.append(&chatgpt_toolbar);

        let chatgpt_web_status = gtk::Label::new(Some("ChatGPT is unloaded."));
        chatgpt_web_status.set_xalign(0.0);
        chatgpt_web_status.set_wrap(true);
        chatgpt_web_status.add_css_class("caption");
        chatgpt_web_status.set_margin_top(6);
        chatgpt_web_status.set_margin_start(18);
        chatgpt_web_status.set_margin_end(18);
        chatgpt_page.append(&chatgpt_web_status);
        let chatgpt_progress = gtk::ProgressBar::new();
        chatgpt_progress.set_margin_top(6);
        chatgpt_progress.set_margin_start(18);
        chatgpt_progress.set_margin_end(18);
        chatgpt_progress.set_visible(false);
        chatgpt_page.append(&chatgpt_progress);
        let chatgpt_web_host = gtk::Box::new(gtk::Orientation::Vertical, 0);
        chatgpt_web_host.set_vexpand(true);
        chatgpt_web_host.set_hexpand(true);
        chatgpt_web_host.set_margin_top(8);
        chatgpt_web_host.add_css_class("webview-frame");
        chatgpt_page.append(&chatgpt_web_host);
        stack.add_named(&chatgpt_page, Some("chatgpt"));

        let projects_page = page_shell(
            "Projects",
            "Choose and manage the local folders used by new Codex tasks.",
        );
        let projects_toolbar = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        projects_toolbar.set_margin_start(18);
        projects_toolbar.set_margin_end(18);
        let projects_status = gtk::Label::new(Some("No project selected"));
        projects_status.set_xalign(0.0);
        projects_status.set_hexpand(true);
        projects_status.add_css_class("caption");
        let projects_add = gtk::Button::with_label("Add project");
        projects_add.add_css_class("suggested-action");
        let projects_open_editor = gtk::Button::with_label("Open in editor");
        let projects_reveal = gtk::Button::with_label("Reveal folder");
        projects_toolbar.append(&projects_status);
        projects_toolbar.append(&projects_add);
        projects_toolbar.append(&projects_open_editor);
        projects_toolbar.append(&projects_reveal);
        projects_page.append(&projects_toolbar);
        let projects_list = gtk::Box::new(gtk::Orientation::Vertical, 8);
        let projects_scroller = gtk::ScrolledWindow::builder()
            .vexpand(true)
            .margin_top(12)
            .margin_start(18)
            .margin_end(18)
            .margin_bottom(18)
            .child(&projects_list)
            .build();
        projects_page.append(&projects_scroller);
        stack.add_named(&projects_page, Some("projects"));

        let sites_page = page_shell(
            "Sites",
            "Start a website task in the selected project and manage the Sites plugin from one native surface.",
        );
        let sites_status = gtk::Label::new(Some("Checking the Sites plugin…"));
        sites_status.set_xalign(0.0);
        sites_status.set_wrap(true);
        sites_status.set_margin_start(18);
        sites_status.set_margin_end(18);
        sites_status.add_css_class("heading");
        sites_page.append(&sites_status);
        let sites_detail = gtk::Label::new(Some(
            "Starting a Sites task prepares a new Codex chat and fills the composer; nothing is sent until you review it.",
        ));
        sites_detail.set_xalign(0.0);
        sites_detail.set_wrap(true);
        sites_detail.set_margin_top(8);
        sites_detail.set_margin_start(18);
        sites_detail.set_margin_end(18);
        sites_detail.add_css_class("caption");
        sites_page.append(&sites_detail);
        let sites_controls = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        sites_controls.set_margin_top(14);
        sites_controls.set_margin_start(18);
        sites_controls.set_margin_end(18);
        let sites_new_task = gtk::Button::with_label("Start a Sites task");
        sites_new_task.add_css_class("suggested-action");
        let sites_open_plugins = gtk::Button::with_label("Manage Sites plugin");
        sites_controls.append(&sites_new_task);
        sites_controls.append(&sites_open_plugins);
        sites_page.append(&sites_controls);
        stack.add_named(&sites_page, Some("sites"));

        let terminal_page = page_shell(
            "Terminal",
            "A real VTE terminal in the selected project folder. It is independent of Codex command output.",
        );
        let terminal_toolbar = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        terminal_toolbar.set_margin_start(18);
        terminal_toolbar.set_margin_end(18);
        let terminal_status = gtk::Label::new(Some("Shell not started"));
        terminal_status.set_xalign(0.0);
        terminal_status.set_hexpand(true);
        terminal_status.add_css_class("caption");
        let terminal_start = gtk::Button::with_label("Start shell");
        terminal_toolbar.append(&terminal_status);
        terminal_toolbar.append(&terminal_start);
        terminal_page.append(&terminal_toolbar);
        let terminal = vte::Terminal::new();
        terminal.set_scrollback_lines(20_000);
        terminal.set_font(Some(&gtk::pango::FontDescription::from_string(
            "monospace 10",
        )));
        terminal.set_hexpand(true);
        terminal.set_vexpand(true);
        terminal.add_css_class("terminal");
        let terminal_frame = gtk::Frame::builder()
            .margin_top(12)
            .margin_start(18)
            .margin_end(18)
            .margin_bottom(18)
            .hexpand(true)
            .vexpand(true)
            .child(&terminal)
            .build();
        terminal_page.append(&terminal_frame);
        stack.add_named(&terminal_page, Some("terminal"));

        let workspace_page = page_shell(
            "Agent workspace",
            "Launch a project shell in a native bubblewrap sandbox with a private home, resource limits, and no network by default.",
        );
        let workspace_status = gtk::Label::new(Some(
            "Run the readiness check before launching an isolated workspace.",
        ));
        workspace_status.set_xalign(0.0);
        workspace_status.set_wrap(true);
        workspace_status.add_css_class("heading");
        workspace_page.append(&workspace_status);
        let workspace_warning = gtk::Label::new(Some(
            "The selected project is writable at /workspace. Extra project paths are mounted read-only under /context. Your host home, Codex credentials, and desktop session are not mounted. Codex sandbox and approval rules still apply independently.",
        ));
        workspace_warning.set_xalign(0.0);
        workspace_warning.set_wrap(true);
        workspace_warning.add_css_class("caption");
        workspace_page.append(&workspace_warning);
        let workspace_settings = gtk::Grid::builder()
            .column_spacing(16)
            .row_spacing(12)
            .margin_top(16)
            .build();
        let workspace_network = gtk::CheckButton::with_label("Allow network access");
        workspace_network.set_tooltip_text(Some(
            "Off creates a separate network namespace with no host or internet access.",
        ));
        let workspace_memory = gtk::SpinButton::with_range(512.0, 65_536.0, 512.0);
        workspace_memory.set_value(4096.0);
        workspace_memory.set_tooltip_text(Some("Hard systemd memory limit in MiB"));
        attach_setting(
            &workspace_settings,
            0,
            "Workspace networking",
            &workspace_network,
        );
        attach_setting(
            &workspace_settings,
            1,
            "Memory limit (MiB)",
            &workspace_memory,
        );
        workspace_page.append(&workspace_settings);
        let workspace_controls = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        workspace_controls.set_margin_top(16);
        let workspace_doctor = gtk::Button::with_label("Check isolation");
        let workspace_launch = gtk::Button::with_label("Launch isolated shell");
        workspace_launch.add_css_class("suggested-action");
        workspace_launch.set_sensitive(false);
        let workspace_stop = gtk::Button::with_label("Stop workspace");
        workspace_stop.set_sensitive(false);
        workspace_controls.append(&workspace_doctor);
        workspace_controls.append(&workspace_launch);
        workspace_controls.append(&workspace_stop);
        workspace_page.append(&workspace_controls);
        stack.add_named(&workspace_page, Some("agent-workspace"));

        let context_page = page_shell(
            "Desktop context",
            "Keep long tasks lean, capture AppShots, and hand browser work to a normal external browser—never an embedded WebView.",
        );
        let lean_context_card = gtk::Box::new(gtk::Orientation::Vertical, 10);
        lean_context_card.add_css_class("settings-card");
        lean_context_card.set_margin_start(18);
        lean_context_card.set_margin_end(18);
        lean_context_card.append(&section_heading("Lean Context Engine"));
        let lean_context_status = gtk::Label::new(Some(
            "Select a task to view its context budget and checkpoint history.",
        ));
        lean_context_status.set_xalign(0.0);
        lean_context_status.set_wrap(true);
        lean_context_status.add_css_class("heading");
        lean_context_card.append(&lean_context_status);
        let lean_context_description = gtk::Label::new(Some(
            "Creates one private structured checkpoint per context generation. Codex remains the sole automatic compactor, preserving its model-aware rollover behavior and the same desktop/iOS thread.",
        ));
        lean_context_description.set_xalign(0.0);
        lean_context_description.set_wrap(true);
        lean_context_description.add_css_class("caption");
        lean_context_card.append(&lean_context_description);
        let lean_context_grid = gtk::Grid::builder()
            .column_spacing(16)
            .row_spacing(10)
            .margin_top(6)
            .build();
        let lean_context_enabled = gtk::Switch::new();
        let lean_context_mode = compact_combo(&[
            ("observe", "Observe only"),
            ("prompt", "Ask before checkpointing"),
            ("auto", "Automatic"),
        ]);
        let lean_context_threshold = gtk::SpinButton::with_range(70.0, 92.0, 1.0);
        lean_context_threshold.set_digits(0);
        lean_context_threshold.set_tooltip_text(Some(
            "Create one checkpoint after this percentage of the current model context window is used. Codex independently decides when to compact.",
        ));
        let lean_context_evidence_budget = gtk::SpinButton::with_range(500.0, 8_000.0, 250.0);
        lean_context_evidence_budget.set_digits(0);
        let lean_context_command_budget = gtk::SpinButton::with_range(250.0, 4_000.0, 250.0);
        lean_context_command_budget.set_digits(0);
        let lean_context_condensation_target = gtk::SpinButton::with_range(200.0, 2_000.0, 100.0);
        lean_context_condensation_target.set_digits(0);
        let lean_context_qwen = gtk::Switch::new();
        lean_context_qwen.set_tooltip_text(Some(
            "For large safe text, source, and log files, use the OpenCode local-Qwen worker in read-only mode for a cited summary. Codex still writes and verifies every change.",
        ));
        attach_setting(&lean_context_grid, 0, "Enabled", &lean_context_enabled);
        attach_setting(&lean_context_grid, 1, "Checkpoint mode", &lean_context_mode);
        attach_setting(
            &lean_context_grid,
            2,
            "Checkpoint at context usage (%)",
            &lean_context_threshold,
        );
        attach_setting(
            &lean_context_grid,
            3,
            "Ordinary evidence budget (tokens)",
            &lean_context_evidence_budget,
        );
        attach_setting(
            &lean_context_grid,
            4,
            "Command output budget (tokens)",
            &lean_context_command_budget,
        );
        attach_setting(
            &lean_context_grid,
            5,
            "Large-file condensation target (tokens)",
            &lean_context_condensation_target,
        );
        attach_setting(
            &lean_context_grid,
            6,
            "Use OpenCode for large-file analysis",
            &lean_context_qwen,
        );
        lean_context_card.append(&lean_context_grid);
        let lean_context_latest =
            gtk::Label::new(Some("No checkpoint has been created for this task."));
        lean_context_latest.set_xalign(0.0);
        lean_context_latest.set_wrap(true);
        lean_context_latest.add_css_class("caption");
        lean_context_card.append(&lean_context_latest);
        let lean_context_checkpoint = gtk::Button::with_label("Checkpoint & compact now");
        lean_context_checkpoint.set_halign(gtk::Align::Start);
        lean_context_checkpoint.add_css_class("suggested-action");
        lean_context_checkpoint.set_sensitive(false);
        lean_context_card.append(&lean_context_checkpoint);
        context_page.append(&lean_context_card);

        let appshot_card = gtk::Box::new(gtk::Orientation::Vertical, 10);
        appshot_card.add_css_class("settings-card");
        appshot_card.set_margin_start(18);
        appshot_card.set_margin_end(18);
        let appshot_title = section_heading("AppShot");
        appshot_card.append(&appshot_title);
        let appshot_status = gtk::Label::new(Some(
            "Capture the focused application window, then attach it to the next Codex turn.",
        ));
        appshot_status.set_wrap(true);
        appshot_status.set_xalign(0.0);
        appshot_status.add_css_class("caption");
        appshot_card.append(&appshot_status);
        let appshot_controls = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        let appshot_capture = gtk::Button::with_label("Capture focused window");
        appshot_capture.add_css_class("suggested-action");
        let appshot_attach = gtk::Button::with_label("Attach latest");
        appshot_attach.set_sensitive(false);
        appshot_controls.append(&appshot_capture);
        appshot_controls.append(&appshot_attach);
        appshot_card.append(&appshot_controls);
        context_page.append(&appshot_card);

        let record_card = gtk::Box::new(gtk::Orientation::Vertical, 10);
        record_card.add_css_class("settings-card");
        record_card.set_margin_top(12);
        record_card.set_margin_start(18);
        record_card.set_margin_end(18);
        record_card.append(&section_heading("Record and Replay"));
        let record_title = gtk::Entry::builder()
            .placeholder_text("Workflow title")
            .build();
        let record_note = gtk::Entry::builder()
            .placeholder_text("What semantic action or state does this frame show?")
            .build();
        record_note.set_sensitive(false);
        let record_status = gtk::Label::new(Some(
            "Record semantic evidence into a reviewable draft skill; pointer coordinates are never replayed.",
        ));
        record_status.set_wrap(true);
        record_status.set_xalign(0.0);
        record_status.add_css_class("caption");
        let record_controls = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        let record_start = gtk::Button::with_label("Start recording");
        let record_frame = gtk::Button::with_label("Capture evidence");
        let record_stop = gtk::Button::with_label("Stop and draft skill");
        let record_install = gtk::Button::with_label("Install reviewed skill");
        let record_reveal = icon_button("folder-open-symbolic", "Reveal recording files");
        record_frame.set_sensitive(false);
        record_stop.set_sensitive(false);
        record_install.set_sensitive(false);
        record_reveal.set_sensitive(false);
        record_controls.append(&record_start);
        record_controls.append(&record_frame);
        record_controls.append(&record_stop);
        record_controls.append(&record_install);
        record_controls.append(&record_reveal);
        record_card.append(&record_title);
        record_card.append(&record_note);
        record_card.append(&record_status);
        record_card.append(&record_controls);
        context_page.append(&record_card);

        let browser_card = gtk::Box::new(gtk::Orientation::Vertical, 10);
        browser_card.add_css_class("settings-card");
        browser_card.set_margin_top(12);
        browser_card.set_margin_start(18);
        browser_card.set_margin_end(18);
        let browser_title = section_heading("External Browser companion");
        browser_card.append(&browser_title);
        let browser_url = gtk::Entry::builder()
            .text("about:blank")
            .placeholder_text("https://…")
            .build();
        browser_card.append(&browser_url);
        let browser_controls = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        let browser_isolated = gtk::CheckButton::with_label("Use isolated Codex profile");
        browser_isolated.set_active(true);
        browser_isolated.set_hexpand(true);
        let browser_open = gtk::Button::with_label("Open browser");
        browser_open.add_css_class("suggested-action");
        browser_controls.append(&browser_isolated);
        browser_controls.append(&browser_open);
        browser_card.append(&browser_controls);
        context_page.append(&browser_card);

        let target_controls = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        target_controls.set_margin_top(12);
        target_controls.set_margin_start(18);
        target_controls.set_margin_end(18);
        let context_open_editor = gtk::Button::with_label("Open project in editor");
        let context_reveal = gtk::Button::with_label("Reveal in file manager");
        target_controls.append(&context_open_editor);
        target_controls.append(&context_reveal);
        context_page.append(&target_controls);
        let context_scroller = gtk::ScrolledWindow::builder()
            .hscrollbar_policy(gtk::PolicyType::Never)
            .vexpand(true)
            .child(&context_page)
            .build();
        stack.add_named(&context_scroller, Some("context"));

        let extensions_page = page_shell(
            "Extensions",
            "Install and control plugins, skills, authentication, and MCP servers on the shared Codex backend.",
        );
        let extensions_toolbar = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        extensions_toolbar.set_margin_start(18);
        extensions_toolbar.set_margin_end(18);
        let extensions_search = gtk::SearchEntry::builder()
            .placeholder_text("Search plugins and apps")
            .hexpand(true)
            .build();
        let extensions_installed_only = gtk::CheckButton::with_label("Installed only");
        extensions_installed_only.set_active(true);
        let extensions_refresh = icon_button("view-refresh-symbolic", "Refresh extensions");
        let marketplace_add = gtk::Button::with_label("Add marketplace");
        let marketplace_upgrade = gtk::Button::with_label("Upgrade catalogs");
        extensions_toolbar.append(&extensions_search);
        extensions_toolbar.append(&extensions_installed_only);
        extensions_toolbar.append(&marketplace_add);
        extensions_toolbar.append(&marketplace_upgrade);
        extensions_toolbar.append(&extensions_refresh);
        extensions_page.append(&extensions_toolbar);
        let extensions_list = gtk::Box::new(gtk::Orientation::Vertical, 8);
        let extensions_scroller = gtk::ScrolledWindow::builder()
            .vexpand(true)
            .margin_top(12)
            .margin_start(18)
            .margin_end(18)
            .margin_bottom(18)
            .child(&extensions_list)
            .build();
        extensions_page.append(&extensions_scroller);
        stack.add_named(&extensions_page, Some("extensions"));

        let qwen_page = page_shell(
            "OpenCode",
            "ChatGPT/Sol plans every task, local Qwen implements it through pinned OpenCode with xhigh thinking, and ChatGPT/Sol reviews, approves destructive requests, and verifies the result.",
        );

        let qwen_controls = gtk::Box::new(gtk::Orientation::Vertical, 10);
        let qwen_status_card = gtk::Box::new(gtk::Orientation::Vertical, 6);
        qwen_status_card.add_css_class("settings-card");
        let qwen_status = gtk::Label::new(Some("Checking OpenCode and local Qwen…"));
        qwen_status.set_xalign(0.0);
        qwen_status.add_css_class("heading");
        let qwen_detail = gtk::Label::new(Some(
            "Inspecting the plugin and its on-demand local runtime.",
        ));
        qwen_detail.set_xalign(0.0);
        qwen_detail.set_wrap(true);
        qwen_detail.add_css_class("caption");
        let qwen_gpu = gtk::Label::new(Some("GPU pressure: checking…"));
        qwen_gpu.set_xalign(0.0);
        qwen_gpu.set_wrap(true);
        qwen_gpu.add_css_class("caption");
        let qwen_status_actions = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        let qwen_refresh = gtk::Button::with_label("Refresh health and usage");
        qwen_refresh.add_css_class("suggested-action");
        let qwen_open_extensions = gtk::Button::with_label("Open plugin manager");
        let qwen_open_terminal = gtk::Button::with_label("Open OpenCode terminal");
        qwen_status_actions.append(&qwen_refresh);
        qwen_status_actions.append(&qwen_open_extensions);
        qwen_status_actions.append(&qwen_open_terminal);
        qwen_status_card.append(&qwen_status);
        qwen_status_card.append(&qwen_detail);
        qwen_status_card.append(&qwen_gpu);
        qwen_status_card.append(&qwen_status_actions);
        qwen_controls.append(&qwen_status_card);

        let qwen_routing_card = gtk::Box::new(gtk::Orientation::Vertical, 8);
        qwen_routing_card.add_css_class("settings-card");
        qwen_routing_card.append(&section_heading("Local delegation policy"));
        let qwen_routing_grid = gtk::Grid::builder()
            .column_spacing(16)
            .row_spacing(8)
            .build();
        let qwen_routing_switch = gtk::Switch::new();
        let qwen_luna_check = gtk::CheckButton::with_label("OpenCode: supplied text");
        let qwen_condenser_check = gtk::CheckButton::with_label("OpenCode: large-file analysis");
        let qwen_sol_check = gtk::CheckButton::with_label("OpenCode: bounded coding agent");
        let qwen_gpu_guard_switch = gtk::Switch::new();
        attach_setting(
            &qwen_routing_grid,
            0,
            "Enable automatic local subagents",
            &qwen_routing_switch,
        );
        attach_setting(&qwen_routing_grid, 1, "Local routes", &qwen_luna_check);
        qwen_routing_grid.attach(&qwen_condenser_check, 1, 2, 1, 1);
        qwen_routing_grid.attach(&qwen_sol_check, 1, 3, 1, 1);
        attach_setting(
            &qwen_routing_grid,
            4,
            "Pause under GPU pressure",
            &qwen_gpu_guard_switch,
        );
        qwen_routing_card.append(&qwen_routing_grid);
        let qwen_authority = gtk::Label::new(Some(
            "Universal workflow: ChatGPT/Sol plans and defines acceptance criteria, then local Qwen implements only through pinned OpenCode with controlled search, read, create, and edit access inside explicit scopes. Shell, deletion, rename, credentials, extensions, subagents, MCP servers, and web tools are unavailable. ChatGPT/Sol keeps secrets, consequential external actions, destructive authorization, integration, and final verification.",
        ));
        qwen_authority.set_xalign(0.0);
        qwen_authority.set_wrap(true);
        qwen_authority.add_css_class("caption");
        qwen_routing_card.append(&qwen_authority);
        qwen_controls.append(&qwen_routing_card);

        let qwen_usage_card = gtk::Box::new(gtk::Orientation::Vertical, 6);
        qwen_usage_card.add_css_class("settings-card");
        qwen_usage_card.append(&section_heading("Measured local usage"));
        let qwen_usage = gtk::Label::new(Some("No Qwen Buddy usage snapshot yet."));
        qwen_usage.set_xalign(0.0);
        qwen_usage.set_wrap(true);
        qwen_usage.set_selectable(true);
        qwen_usage.add_css_class("caption");
        qwen_usage_card.append(&qwen_usage);
        qwen_controls.append(&qwen_usage_card);

        let qwen_controls_scroller = gtk::ScrolledWindow::builder()
            .vexpand(true)
            .hscrollbar_policy(gtk::PolicyType::Never)
            .child(&qwen_controls)
            .build();
        qwen_page.append(&qwen_controls_scroller);
        stack.add_named(&qwen_page, Some("qwen-buddy"));

        let computer_page = page_shell(
            "Computer use",
            "Control Linux desktop apps through the Computer Use plugin, XDG portals, accessibility APIs, and your compositor.",
        );
        let computer_status = gtk::Label::new(Some("Checking Computer Use…"));
        computer_status.set_xalign(0.0);
        computer_status.add_css_class("heading");
        let computer_detail = gtk::Label::new(Some(
            "Mutating desktop actions remain subject to Codex and MCP approval policy.",
        ));
        computer_detail.set_xalign(0.0);
        computer_detail.set_wrap(true);
        computer_detail.add_css_class("caption");
        computer_page.append(&computer_status);
        computer_page.append(&computer_detail);

        let computer_settings = gtk::Grid::builder()
            .column_spacing(12)
            .row_spacing(10)
            .margin_top(12)
            .build();
        let computer_plugin_switch = gtk::Switch::new();
        let computer_server_switch = gtk::Switch::new();
        let computer_approval_combo = compact_combo(&[
            ("prompt", "Ask for every tool"),
            ("writes", "Ask for desktop changes"),
            ("auto", "Use tool risk hints"),
            ("approve", "Always allow"),
        ]);
        attach_setting(
            &computer_settings,
            0,
            "Computer Use plugin",
            &computer_plugin_switch,
        );
        attach_setting(
            &computer_settings,
            1,
            "Linux MCP server",
            &computer_server_switch,
        );
        attach_setting(
            &computer_settings,
            2,
            "Desktop action approvals",
            &computer_approval_combo,
        );
        computer_page.append(&computer_settings);

        let computer_controls = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        computer_controls.set_margin_top(12);
        let computer_doctor = gtk::Button::with_label("Check readiness");
        computer_doctor.add_css_class("suggested-action");
        let computer_setup = gtk::Button::with_label("Run Linux setup");
        let computer_new_task = gtk::Button::with_label("Start a computer task");
        computer_controls.append(&computer_doctor);
        computer_controls.append(&computer_setup);
        computer_controls.append(&computer_new_task);
        computer_page.append(&computer_controls);

        let computer_capabilities = gtk::Box::new(gtk::Orientation::Vertical, 8);
        let computer_scroller = gtk::ScrolledWindow::builder()
            .vexpand(true)
            .margin_top(12)
            .margin_bottom(18)
            .child(&computer_capabilities)
            .build();
        computer_page.append(&computer_scroller);
        stack.add_named(&computer_page, Some("computer-use"));

        let automations_page = page_shell(
            "Scheduled",
            "Create and manage scheduled Codex tasks with transparent user-level systemd timers.",
        );
        let automation_toolbar = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        automation_toolbar.set_margin_start(18);
        automation_toolbar.set_margin_end(18);
        let automation_add = gtk::Button::with_label("New scheduled task");
        automation_add.add_css_class("suggested-action");
        automation_toolbar.append(&automation_add);
        automations_page.append(&automation_toolbar);
        let automations_list = gtk::Box::new(gtk::Orientation::Vertical, 8);
        let automations_scroller = gtk::ScrolledWindow::builder()
            .vexpand(true)
            .margin_top(12)
            .margin_start(18)
            .margin_end(18)
            .margin_bottom(18)
            .child(&automations_list)
            .build();
        automations_page.append(&automations_scroller);
        stack.add_named(&automations_page, Some("automations"));

        let remote_page = page_shell(
            "Remote access",
            "Pair this Arch workstation as an experimental Codex remote host, then continue its tasks from the ChatGPT iOS app.",
        );
        let warning = gtk::Label::new(Some(
            "Experimental: remote hosting on Linux depends on your Codex CLI version and account rollout. Keep this machine online and protect its login session.",
        ));
        warning.set_wrap(true);
        warning.set_xalign(0.0);
        warning.add_css_class("remote-warning");
        remote_page.append(&warning);
        let remote_status = gtk::Label::new(Some("Status: checking…"));
        remote_status.set_xalign(0.0);
        remote_status.add_css_class("heading");
        remote_page.append(&remote_status);
        let remote_detail = gtk::Label::new(None);
        remote_detail.set_wrap(true);
        remote_detail.set_xalign(0.0);
        remote_detail.add_css_class("caption");
        remote_page.append(&remote_detail);
        let remote_controls = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        let remote_enable = gtk::Button::with_label("Enable host");
        remote_enable.add_css_class("suggested-action");
        let remote_pair = gtk::Button::with_label("Pair iPhone or iPad");
        let remote_disable = gtk::Button::with_label("Disable host");
        let remote_restart = gtk::Button::with_label("Restart host");
        let remote_refresh = icon_button("view-refresh-symbolic", "Refresh remote status");
        remote_controls.append(&remote_enable);
        remote_controls.append(&remote_pair);
        remote_controls.append(&remote_disable);
        remote_controls.append(&remote_restart);
        remote_controls.append(&remote_refresh);
        remote_page.append(&remote_controls);
        let remote_pairing = gtk::Label::new(Some(
            "Enable the host, select Codex in the ChatGPT iOS app, choose Connect to computer, then enter the pairing code shown here.",
        ));
        remote_pairing.set_wrap(true);
        remote_pairing.set_xalign(0.0);
        remote_pairing.add_css_class("remote-code");
        remote_page.append(&remote_pairing);
        let clients_title = gtk::Label::new(Some("Paired devices"));
        clients_title.set_xalign(0.0);
        clients_title.add_css_class("heading");
        remote_page.append(&clients_title);
        let remote_clients = gtk::Box::new(gtk::Orientation::Vertical, 6);
        let remote_scroller = gtk::ScrolledWindow::builder()
            .vexpand(true)
            .child(&remote_clients)
            .build();
        remote_page.append(&remote_scroller);
        stack.add_named(&remote_page, Some("remote"));

        let diagnostics_page = page_shell(
            "Diagnostics",
            "Run focused checks for Codex Native, the local host, and Arch package updates.",
        );
        let diagnostics_content = gtk::Box::new(gtk::Orientation::Vertical, 10);
        let diagnostics_content_scroller = gtk::ScrolledWindow::builder()
            .vexpand(true)
            .hscrollbar_policy(gtk::PolicyType::Never)
            .child(&diagnostics_content)
            .build();
        diagnostics_page.append(&diagnostics_content_scroller);
        let diagnostics_toolbar = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        diagnostics_toolbar.set_margin_start(18);
        diagnostics_toolbar.set_margin_end(18);
        let diagnostics_summary = gtk::Label::new(Some("No diagnostics collected yet."));
        diagnostics_summary.set_xalign(0.0);
        diagnostics_summary.set_hexpand(true);
        diagnostics_summary.add_css_class("heading");
        let diagnostics_refresh = gtk::Button::with_label("Run diagnostics");
        diagnostics_refresh.add_css_class("suggested-action");
        diagnostics_toolbar.append(&diagnostics_summary);
        diagnostics_toolbar.append(&diagnostics_refresh);
        diagnostics_content.append(&diagnostics_toolbar);
        let diagnostics_list = gtk::Box::new(gtk::Orientation::Vertical, 6);
        diagnostics_list.set_margin_top(12);
        diagnostics_list.set_margin_start(18);
        diagnostics_list.set_margin_end(18);
        diagnostics_content.append(&diagnostics_list);

        let update_card = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        update_card.add_css_class("settings-card");
        update_card.set_margin_top(12);
        update_card.set_margin_start(18);
        update_card.set_margin_end(18);
        let update_label =
            gtk::Label::new(Some("Arch package update status has not been checked."));
        update_label.set_xalign(0.0);
        update_label.set_wrap(true);
        update_label.set_hexpand(true);
        let update_check = gtk::Button::with_label("Check package update");
        let update_apply = gtk::Button::with_label("Install update");
        let update_rollback = gtk::Button::with_label("Roll back");
        update_apply.set_sensitive(false);
        update_rollback.set_sensitive(false);
        update_card.append(&update_label);
        update_card.append(&update_check);
        update_card.append(&update_apply);
        update_card.append(&update_rollback);
        diagnostics_content.append(&update_card);

        let remote_doctor_card = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        remote_doctor_card.add_css_class("settings-card");
        remote_doctor_card.set_margin_top(12);
        remote_doctor_card.set_margin_start(18);
        remote_doctor_card.set_margin_end(18);
        remote_doctor_card.set_margin_bottom(18);
        let remote_doctor_label =
            gtk::Label::new(Some("Remote runtime health has not been inspected."));
        remote_doctor_label.set_xalign(0.0);
        remote_doctor_label.set_wrap(true);
        remote_doctor_label.set_hexpand(true);
        let remote_doctor_button = gtk::Button::with_label("Check remote runtime");
        remote_doctor_card.append(&remote_doctor_label);
        remote_doctor_card.append(&remote_doctor_button);
        diagnostics_content.append(&remote_doctor_card);
        stack.add_named(&diagnostics_page, Some("diagnostics"));
        let settings_page = page_shell(
            "Settings",
            "Local preferences for the native client. Backend settings remain owned by Codex.",
        );
        let settings_grid = gtk::Grid::builder()
            .column_spacing(16)
            .row_spacing(12)
            .margin_top(10)
            .margin_start(18)
            .margin_end(18)
            .halign(gtk::Align::Fill)
            .build();
        let theme_combo =
            compact_combo(&[("system", "System"), ("light", "Light"), ("dark", "Dark")]);
        let binary_entry = gtk::Entry::builder()
            .placeholder_text("codex (from PATH)")
            .hexpand(true)
            .build();
        let browser_entry = gtk::Entry::builder()
            .placeholder_text("Auto-detect Chrome/Chromium")
            .hexpand(true)
            .build();
        let editor_entry = gtk::Entry::builder()
            .placeholder_text("Auto-detect editor")
            .hexpand(true)
            .build();
        let notification_switch = gtk::Switch::new();
        let reasoning_switch = gtk::Switch::new();
        let remote_autostart_switch = gtk::Switch::new();
        let keep_awake_switch = gtk::Switch::new();
        let resource_monitor_switch = gtk::Switch::new();
        attach_setting(&settings_grid, 0, "Theme", &theme_combo);
        attach_setting(&settings_grid, 1, "Codex CLI", &binary_entry);
        attach_setting(&settings_grid, 2, "Browser", &browser_entry);
        attach_setting(&settings_grid, 3, "Editor", &editor_entry);
        attach_setting(&settings_grid, 4, "Notifications", &notification_switch);
        attach_setting(&settings_grid, 5, "Show reasoning", &reasoning_switch);
        attach_setting(
            &settings_grid,
            6,
            "Start Remote access on login",
            &remote_autostart_switch,
        );
        attach_setting(
            &settings_grid,
            7,
            "Keep system awake while busy",
            &keep_awake_switch,
        );
        attach_setting(
            &settings_grid,
            8,
            "Include resource checks in diagnostics",
            &resource_monitor_switch,
        );
        settings_page.append(&settings_grid);
        let account_box = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        account_box.set_margin_start(18);
        account_box.set_margin_end(18);
        account_box.set_margin_top(18);
        let account_label = gtk::Label::new(Some("Account: checking…"));
        account_label.set_xalign(0.0);
        account_label.set_hexpand(true);
        let account_usage_label = gtk::Label::new(Some("Usage: checking…"));
        account_usage_label.set_xalign(0.0);
        account_usage_label.add_css_class("caption");
        let account_text = gtk::Box::new(gtk::Orientation::Vertical, 2);
        account_text.set_hexpand(true);
        account_text.append(&account_label);
        account_text.append(&account_usage_label);
        let account_refresh = icon_button("view-refresh-symbolic", "Refresh account usage");
        let login_button = gtk::Button::with_label("Sign in with ChatGPT");
        let logout_button = gtk::Button::with_label("Sign out");
        account_box.append(&account_text);
        account_box.append(&account_refresh);
        account_box.append(&login_button);
        account_box.append(&logout_button);
        settings_page.append(&account_box);
        let settings_save = gtk::Button::with_label("Save settings");
        settings_save.set_halign(gtk::Align::Start);
        settings_save.set_margin_start(18);
        settings_save.set_margin_top(14);
        settings_save.add_css_class("suggested-action");
        settings_page.append(&settings_save);
        stack.add_named(&settings_page, Some("settings"));
        let initial_page = env::var("CODEX_NATIVE_START_PAGE").unwrap_or_else(|_| "chat".into());
        stack.set_visible_child_name(match initial_page.as_str() {
            "chat" | "chatgpt" | "projects" | "sites" | "terminal" | "agent-workspace"
            | "context" | "extensions" | "qwen-buddy" | "computer-use" | "automations"
            | "remote" | "diagnostics" | "settings" => &initial_page,
            _ => "chat",
        });

        for button in [&add_project, &projects_add] {
            let add_window = window.clone();
            let add_project_combo = project_combo.clone();
            button.connect_clicked(move |_| {
                let dialog = gtk::FileDialog::builder()
                    .title("Add project folder")
                    .modal(true)
                    .build();
                let combo = add_project_combo.clone();
                dialog.select_folder(
                    Some(&add_window),
                    None::<&gio::Cancellable>,
                    move |result| {
                        if let Ok(file) = result
                            && let Some(path) = file.path()
                        {
                            let id = path.to_string_lossy().into_owned();
                            let name = path.file_name().and_then(|v| v.to_str()).unwrap_or(&id);
                            combo.append(Some(&id), name);
                            combo.set_active_id(Some(&id));
                        }
                    },
                );
            });
        }

        Self {
            window,
            toast_overlay,
            stack,
            status_icon,
            status_label,
            memoria_jobs_label,
            usage_label,
            weekly_progress,
            five_hour_progress,
            reset_credit_button,
            project_combo,
            projects_status,
            projects_open_editor,
            projects_reveal,
            projects_list,
            sites_status,
            sites_new_task,
            sites_open_plugins,
            thread_search,
            thread_archived_toggle,
            thread_select_toggle,
            thread_bulk_bar,
            thread_bulk_count,
            thread_select_all,
            thread_bulk_archive,
            thread_bulk_delete,
            thread_list,
            new_task,
            chatgpt_status,
            chatgpt_web_status,
            chatgpt_web_host,
            chatgpt_progress,
            chatgpt_back,
            chatgpt_forward,
            chatgpt_home,
            chatgpt_reload,
            chatgpt_unload,
            task_title,
            transcript,
            transcript_scroller,
            task_progress,
            approval_revealer,
            approval_title,
            approval_detail,
            approval_once,
            approval_session,
            approval_deny,
            composer,
            attachments_label,
            attach_button,
            paste_button,
            dictate_button,
            read_aloud_button,
            subagents_toggle,
            send_button,
            stop_button,
            model_combo,
            effort_combo,
            speed_combo,
            sandbox_combo,
            approval_combo,
            rename_button,
            pin_button,
            goal_button,
            goal_summary,
            goal_detail,
            goal_reason,
            goal_edit,
            goal_stop,
            goal_resume,
            goal_complete,
            goal_clear,
            compact_button,
            rollback_button,
            fork_button,
            archive_button,
            unarchive_button,
            delete_button,
            load_older_button,
            load_activity_button,
            terminal,
            terminal_status,
            terminal_start,
            workspace_status,
            workspace_network,
            workspace_memory,
            workspace_doctor,
            workspace_launch,
            workspace_stop,
            lean_context_status,
            lean_context_enabled,
            lean_context_mode,
            lean_context_threshold,
            lean_context_evidence_budget,
            lean_context_command_budget,
            lean_context_condensation_target,
            lean_context_qwen,
            lean_context_checkpoint,
            lean_context_latest,
            appshot_status,
            appshot_capture,
            appshot_attach,
            record_title,
            record_note,
            record_status,
            record_start,
            record_frame,
            record_stop,
            record_install,
            record_reveal,
            browser_url,
            browser_isolated,
            browser_open,
            context_open_editor,
            context_reveal,
            extensions_search,
            extensions_installed_only,
            extensions_refresh,
            marketplace_add,
            marketplace_upgrade,
            extensions_list,
            qwen_status,
            qwen_detail,
            qwen_gpu,
            qwen_usage,
            qwen_routing_switch,
            qwen_luna_check,
            qwen_condenser_check,
            qwen_sol_check,
            qwen_gpu_guard_switch,
            qwen_refresh,
            qwen_open_extensions,
            qwen_open_terminal,
            computer_status,
            computer_detail,
            computer_capabilities,
            computer_plugin_switch,
            computer_server_switch,
            computer_approval_combo,
            computer_doctor,
            computer_setup,
            computer_new_task,
            automations_list,
            automation_add,
            remote_status,
            remote_detail,
            remote_pairing,
            remote_clients,
            remote_enable,
            remote_disable,
            remote_pair,
            remote_restart,
            remote_refresh,
            diagnostics_summary,
            diagnostics_list,
            diagnostics_refresh,
            update_check,
            update_apply,
            update_rollback,
            update_label,
            remote_doctor_button,
            remote_doctor_label,
            theme_combo,
            binary_entry,
            browser_entry,
            editor_entry,
            notification_switch,
            reasoning_switch,
            remote_autostart_switch,
            keep_awake_switch,
            resource_monitor_switch,
            account_menu,
            account_menu_summary,
            account_profiles,
            account_add,
            account_settings,
            account_label,
            account_usage_label,
            account_refresh,
            login_button,
            logout_button,
            settings_save,
        }
    }
}

struct Controller {
    weak_self: RefCell<Weak<Controller>>,
    hub: AccountHub,
    rollout_activity_rx: Receiver<HashSet<String>>,
    state: Rc<RefCell<AppState>>,
    stored: Rc<RefCell<StoredState>>,
    automations: Rc<RefCell<AutomationStore>>,
    attachments: Rc<RefCell<Vec<PathBuf>>>,
    terminal_started: Rc<Cell<bool>>,
    updating_computer_controls: Rc<Cell<bool>>,
    updating_qwen_controls: Rc<Cell<bool>>,
    updating_context_controls: Cell<bool>,
    updating_task_controls: Cell<bool>,
    backend_bootstrapped: Cell<bool>,
    login_after_profile_ready: Cell<bool>,
    extensions_bootstrapped: Cell<bool>,
    apps_render_limit: Cell<usize>,
    qwen_bootstrapped: Cell<bool>,
    diagnostics_bootstrapped: Cell<bool>,
    memoria_jobs_refresh_pending: Cell<bool>,
    sleep_inhibit_cookie: Cell<Option<u32>>,
    activity_phase: Cell<usize>,
    activity_indicators: RefCell<Vec<glib::WeakRef<gtk::Label>>>,
    smoke_clipboard_texture: RefCell<Option<gdk::Texture>>,
    remote_unhealthy_samples: Cell<u8>,
    remote_expected_enabled: Cell<bool>,
    remote_handoff_pending: Cell<bool>,
    remote_recovery_pending: Cell<bool>,
    remote_recovery_in_progress: Cell<bool>,
    remote_recovery_reason: RefCell<Option<String>>,
    last_remote_recovery: Cell<Option<Instant>>,
    thread_render_scheduled: Cell<bool>,
    transcript_render_scheduled: Cell<bool>,
    transcript_prepend_pending: Cell<bool>,
    transcript_rows: RefCell<Vec<RenderedTranscriptRow>>,
    transcript_follow_bottom: Cell<bool>,
    transcript_scroll_restoring: Cell<bool>,
    transcript_scroll_animation_generation: Cell<u64>,
    last_transcript_revision: Cell<Option<u64>>,
    last_transcript_thread: RefCell<Option<String>>,
    last_transcript_reasoning: Cell<bool>,
    selected_thread_ids: RefCell<BTreeSet<String>>,
    /// A task selected from the shared timeline while its owning account was
    /// inactive. It is opened after that profile's app-server is ready.
    deferred_shared_thread_open: RefCell<Option<String>>,
    macro_turns: RefCell<HashMap<String, MacroTurnObservation>>,
    macro_pending_samples: RefCell<HashMap<String, MacroExperimentSample>>,
    token_savings_observations: RefCell<HashMap<String, TokenSavingsObservation>>,
    local_history_loading: RefCell<HashSet<String>>,
    local_history_fingerprints: RefCell<HashMap<String, (u64, u64, u32)>>,
    chatgpt: Rc<ChatgptSurface>,
    widgets: Widgets,
}

struct MacroTurnObservation {
    started: Instant,
    item_ids: HashSet<String>,
    tool_calls: u32,
    macro_used: bool,
    peak_memory_bytes: Option<u64>,
    workload_class: Option<String>,
    context_bytes_avoided: u64,
    cache_hits: u32,
    command_calls: u32,
    file_change_calls: u32,
    external_calls: u32,
}

#[derive(Debug, Clone, Copy, Default)]
struct TurnTokenBreakdown {
    total: Option<u64>,
    input: Option<u64>,
    cached_input: Option<u64>,
    output: Option<u64>,
    reasoning_output: Option<u64>,
}

struct TokenSavingsObservation {
    turn_id: String,
    before_cumulative_tokens: Option<u64>,
    latest_cumulative_tokens: Option<u64>,
    usage_seen: bool,
    completed: bool,
    qwen_event_ids: HashSet<String>,
    qwen: routing::QwenSavingsEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContextCompactionPhase {
    Started,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ContextCompactionEvent {
    phase: ContextCompactionPhase,
    thread_id: String,
    turn_id: String,
    item_id: String,
}

struct RenderedTranscriptRow {
    key: String,
    fingerprint: u64,
    widget: gtk::Widget,
    streaming_body: Option<gtk::Label>,
}

struct BuiltTranscriptRow {
    widget: gtk::Widget,
    streaming_body: Option<gtk::Label>,
}

struct TranscriptRowSpec {
    key: String,
    fingerprint: u64,
    content: TranscriptRowContent,
}

enum TranscriptRowContent {
    Route(RouteDecision),
    TokenSavings(TokenSavingsReceipt),
    Item {
        item: Value,
        streamed_text: String,
        show_reasoning: bool,
        cwd: String,
    },
    Images {
        images: Vec<String>,
        verb: &'static str,
    },
    TurnError(Value),
}

impl Controller {
    fn connect(this: &Rc<Self>) {
        let weak = Rc::downgrade(this);
        this.widgets
            .new_task
            .connect_clicked(move |_| with_controller(&weak, |c| c.new_task()));

        let weak = Rc::downgrade(this);
        this.widgets
            .send_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.send_composer()));

        let weak = Rc::downgrade(this);
        this.widgets
            .stop_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.interrupt_turn()));

        let key = gtk::EventControllerKey::new();
        let weak = Rc::downgrade(this);
        key.connect_key_pressed(move |_, key, _, modifiers| {
            let paste = modifiers.contains(gdk::ModifierType::CONTROL_MASK)
                && key
                    .to_unicode()
                    .is_some_and(|character| character.eq_ignore_ascii_case(&'v'));
            if paste
                && weak
                    .upgrade()
                    .is_some_and(|controller| controller.paste_composer_clipboard())
            {
                glib::Propagation::Stop
            } else if composer_return_sends(key, modifiers) {
                with_controller(&weak, |c| c.send_composer());
                glib::Propagation::Stop
            } else {
                glib::Propagation::Proceed
            }
        });
        this.widgets.composer.add_controller(key);

        let adjustment = this.widgets.transcript_scroller.vadjustment();
        let weak = Rc::downgrade(this);
        adjustment.connect_value_changed(move |adjustment| {
            with_controller(&weak, |controller| {
                if !controller.transcript_scroll_restoring.get() {
                    controller.cancel_transcript_scroll_animation();
                    controller
                        .transcript_follow_bottom
                        .set(adjustment_is_near_bottom(adjustment));
                }
            });
        });
        let weak = Rc::downgrade(this);
        adjustment.connect_upper_notify(move |adjustment| {
            with_controller(&weak, |controller| {
                if controller.transcript_follow_bottom.get() {
                    controller.animate_transcript_scroll_to(
                        adjustment,
                        (adjustment.upper() - adjustment.page_size()).max(0.0),
                    );
                }
            });
        });

        let weak = Rc::downgrade(this);
        this.widgets
            .attach_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.choose_attachments()));
        let weak = Rc::downgrade(this);
        this.widgets.paste_button.connect_clicked(move |_| {
            with_controller(&weak, |controller| {
                if !controller.paste_composer_clipboard() {
                    controller.toast("The clipboard does not contain an image or local files");
                }
            })
        });
        let weak = Rc::downgrade(this);
        this.widgets
            .dictate_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.capture_dictation()));
        let weak = Rc::downgrade(this);
        this.widgets
            .read_aloud_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.read_latest_aloud()));

        let weak = Rc::downgrade(this);
        this.widgets
            .subagents_toggle
            .connect_toggled(move |toggle| {
                let enabled = toggle.is_active();
                set_subagents_toggle_appearance(toggle, enabled);
                with_controller(&weak, |controller| {
                    controller.set_active_task_subagents_enabled(enabled);
                });
            });

        let weak = Rc::downgrade(this);
        this.widgets
            .chatgpt_back
            .connect_clicked(move |_| with_controller(&weak, |c| c.chatgpt.go_back()));
        let weak = Rc::downgrade(this);
        this.widgets
            .chatgpt_forward
            .connect_clicked(move |_| with_controller(&weak, |c| c.chatgpt.go_forward()));
        let weak = Rc::downgrade(this);
        this.widgets
            .chatgpt_home
            .connect_clicked(move |_| with_controller(&weak, |c| c.chatgpt.go_home()));
        let weak = Rc::downgrade(this);
        this.widgets
            .chatgpt_reload
            .connect_clicked(move |_| with_controller(&weak, |c| c.chatgpt.reload()));
        let weak = Rc::downgrade(this);
        this.widgets
            .chatgpt_unload
            .connect_clicked(move |_| with_controller(&weak, |c| c.chatgpt.unload()));

        let weak = Rc::downgrade(this);
        this.widgets
            .thread_search
            .connect_search_changed(move |entry| {
                with_controller(&weak, |controller| {
                    if entry.text().trim().is_empty() {
                        controller.state.borrow_mut().clear_thread_search();
                    }
                    controller.render_threads();
                })
            });
        let weak = Rc::downgrade(this);
        this.widgets
            .thread_search
            .connect_activate(move |_| with_controller(&weak, |c| c.search_threads()));
        let weak = Rc::downgrade(this);
        this.widgets
            .thread_archived_toggle
            .connect_toggled(move |toggle| {
                with_controller(&weak, |c| {
                    c.close_task_selection();
                    c.stored.borrow_mut().show_archived_threads = toggle.is_active();
                    c.persist();
                    c.refresh_threads();
                })
            });
        let weak = Rc::downgrade(this);
        this.widgets
            .thread_select_toggle
            .connect_toggled(move |_| with_controller(&weak, Controller::sync_task_selection_mode));
        let weak = Rc::downgrade(this);
        this.widgets
            .thread_select_all
            .connect_clicked(move |_| with_controller(&weak, Controller::toggle_all_visible_tasks));
        let weak = Rc::downgrade(this);
        this.widgets
            .thread_bulk_archive
            .connect_clicked(move |_| with_controller(&weak, Controller::archive_selected_tasks));
        let weak = Rc::downgrade(this);
        this.widgets.thread_bulk_delete.connect_clicked(move |_| {
            with_controller(&weak, Controller::delete_selected_tasks_dialog)
        });

        let weak = Rc::downgrade(this);
        this.widgets
            .project_combo
            .connect_changed(move |_| with_controller(&weak, |c| c.project_changed()));
        let weak = Rc::downgrade(this);
        this.widgets
            .projects_open_editor
            .connect_clicked(move |_| with_controller(&weak, Controller::open_project_editor));
        let weak = Rc::downgrade(this);
        this.widgets
            .projects_reveal
            .connect_clicked(move |_| with_controller(&weak, Controller::reveal_project));
        let weak = Rc::downgrade(this);
        this.widgets
            .sites_new_task
            .connect_clicked(move |_| with_controller(&weak, Controller::start_sites_task));
        let weak = Rc::downgrade(this);
        this.widgets
            .sites_open_plugins
            .connect_clicked(move |_| with_controller(&weak, Controller::open_sites_plugin));
        let weak = Rc::downgrade(this);
        this.widgets.model_combo.connect_changed(move |_| {
            with_controller(&weak, |controller| {
                if controller.updating_task_controls.get() {
                    return;
                }
                controller.updating_task_controls.set(true);
                controller.populate_efforts();
                controller.populate_service_tiers();
                controller.populate_sandboxes(true);
                controller.updating_task_controls.set(false);
                controller.update_task_settings_from_controls();
            })
        });
        for combo in [
            &this.widgets.effort_combo,
            &this.widgets.speed_combo,
            &this.widgets.sandbox_combo,
            &this.widgets.approval_combo,
        ] {
            let weak = Rc::downgrade(this);
            combo.connect_changed(move |_| {
                with_controller(&weak, |controller| {
                    controller.update_task_settings_from_controls()
                })
            });
        }

        let weak = Rc::downgrade(this);
        this.widgets
            .rename_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.rename_thread_dialog()));
        let weak = Rc::downgrade(this);
        this.widgets
            .pin_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.toggle_pin()));
        let weak = Rc::downgrade(this);
        this.widgets
            .goal_edit
            .connect_clicked(move |_| with_controller(&weak, |c| c.goal_dialog()));
        let weak = Rc::downgrade(this);
        this.widgets
            .goal_stop
            .connect_clicked(move |_| with_controller(&weak, |c| c.set_goal_status("paused")));
        let weak = Rc::downgrade(this);
        this.widgets
            .goal_resume
            .connect_clicked(move |_| with_controller(&weak, |c| c.set_goal_status("active")));
        let weak = Rc::downgrade(this);
        this.widgets
            .goal_complete
            .connect_clicked(move |_| with_controller(&weak, |c| c.set_goal_status("complete")));
        let weak = Rc::downgrade(this);
        this.widgets
            .goal_clear
            .connect_clicked(move |_| with_controller(&weak, |c| c.confirm_clear_goal()));
        let weak = Rc::downgrade(this);
        this.widgets
            .compact_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.compact_thread()));
        let weak = Rc::downgrade(this);
        this.widgets
            .rollback_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.rollback_thread_dialog()));
        let weak = Rc::downgrade(this);
        this.widgets
            .fork_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.fork_thread()));
        let weak = Rc::downgrade(this);
        this.widgets
            .archive_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.archive_thread()));
        let weak = Rc::downgrade(this);
        this.widgets
            .unarchive_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.unarchive_thread()));
        let weak = Rc::downgrade(this);
        this.widgets
            .delete_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.delete_thread_dialog()));
        let weak = Rc::downgrade(this);
        this.widgets
            .load_older_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.load_older_turns()));
        let weak = Rc::downgrade(this);
        this.widgets
            .load_activity_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.load_previous_turn_activity()));

        let weak = Rc::downgrade(this);
        this.widgets
            .approval_once
            .connect_clicked(move |_| with_controller(&weak, |c| c.resolve_approval("accept")));
        let weak = Rc::downgrade(this);
        this.widgets.approval_session.connect_clicked(move |_| {
            with_controller(&weak, |c| c.resolve_approval("acceptForSession"))
        });
        let weak = Rc::downgrade(this);
        this.widgets
            .approval_deny
            .connect_clicked(move |_| with_controller(&weak, |c| c.resolve_approval("decline")));

        let weak = Rc::downgrade(this);
        this.widgets
            .terminal_start
            .connect_clicked(move |_| with_controller(&weak, |c| c.start_terminal()));
        let weak = Rc::downgrade(this);
        this.widgets
            .terminal
            .connect_child_exited(move |_, status| {
                with_controller(&weak, |c| {
                    c.terminal_started.set(false);
                    c.widgets
                        .terminal_status
                        .set_label(&format!("Shell exited ({status})"));
                    c.widgets.terminal_start.set_label("Restart shell");
                    c.widgets.terminal_start.set_sensitive(true);
                });
            });

        let weak = Rc::downgrade(this);
        this.widgets
            .workspace_doctor
            .connect_clicked(move |_| with_controller(&weak, |c| c.check_agent_workspace()));
        let weak = Rc::downgrade(this);
        this.widgets
            .workspace_launch
            .connect_clicked(move |_| with_controller(&weak, |c| c.launch_agent_workspace()));
        let weak = Rc::downgrade(this);
        this.widgets
            .workspace_stop
            .connect_clicked(move |_| with_controller(&weak, |c| c.stop_agent_workspace()));

        let weak = Rc::downgrade(this);
        this.widgets
            .appshot_capture
            .connect_clicked(move |_| with_controller(&weak, |c| c.capture_appshot()));
        let weak = Rc::downgrade(this);
        this.widgets
            .appshot_attach
            .connect_clicked(move |_| with_controller(&weak, |c| c.attach_latest_appshot()));
        let weak = Rc::downgrade(this);
        this.widgets
            .record_start
            .connect_clicked(move |_| with_controller(&weak, |c| c.start_recording()));
        let weak = Rc::downgrade(this);
        this.widgets
            .record_frame
            .connect_clicked(move |_| with_controller(&weak, |c| c.capture_recording_frame()));
        let weak = Rc::downgrade(this);
        this.widgets
            .record_stop
            .connect_clicked(move |_| with_controller(&weak, |c| c.stop_recording()));
        let weak = Rc::downgrade(this);
        this.widgets
            .record_install
            .connect_clicked(move |_| with_controller(&weak, |c| c.install_recording_skill()));
        let weak = Rc::downgrade(this);
        this.widgets
            .record_reveal
            .connect_clicked(move |_| with_controller(&weak, |c| c.reveal_recording()));
        let weak = Rc::downgrade(this);
        this.widgets
            .browser_open
            .connect_clicked(move |_| with_controller(&weak, |c| c.open_browser_companion()));
        let weak = Rc::downgrade(this);
        this.widgets
            .context_open_editor
            .connect_clicked(move |_| with_controller(&weak, |c| c.open_project_editor()));
        let weak = Rc::downgrade(this);
        this.widgets
            .context_reveal
            .connect_clicked(move |_| with_controller(&weak, |c| c.reveal_project()));
        let weak = Rc::downgrade(this);
        this.widgets
            .lean_context_enabled
            .connect_active_notify(move |_| {
                with_controller(&weak, Controller::save_lean_context_preferences)
            });
        let weak = Rc::downgrade(this);
        this.widgets.lean_context_mode.connect_changed(move |_| {
            with_controller(&weak, Controller::save_lean_context_preferences)
        });
        let weak = Rc::downgrade(this);
        this.widgets
            .lean_context_threshold
            .connect_value_changed(move |_| {
                with_controller(&weak, Controller::save_lean_context_preferences)
            });
        let weak = Rc::downgrade(this);
        this.widgets
            .lean_context_evidence_budget
            .connect_value_changed(move |_| {
                with_controller(&weak, Controller::save_lean_context_preferences)
            });
        let weak = Rc::downgrade(this);
        this.widgets
            .lean_context_command_budget
            .connect_value_changed(move |_| {
                with_controller(&weak, Controller::save_lean_context_preferences)
            });
        let weak = Rc::downgrade(this);
        this.widgets
            .lean_context_condensation_target
            .connect_value_changed(move |_| {
                with_controller(&weak, Controller::save_lean_context_preferences)
            });
        let weak = Rc::downgrade(this);
        this.widgets
            .lean_context_qwen
            .connect_active_notify(move |_| {
                with_controller(&weak, Controller::save_lean_context_preferences)
            });
        let weak = Rc::downgrade(this);
        this.widgets
            .lean_context_checkpoint
            .connect_clicked(move |_| with_controller(&weak, Controller::compact_thread));
        let weak = Rc::downgrade(this);
        this.widgets
            .automation_add
            .connect_clicked(move |_| with_controller(&weak, |c| c.new_automation_dialog()));

        let weak = Rc::downgrade(this);
        this.widgets
            .extensions_search
            .connect_search_changed(move |_| {
                with_controller(&weak, |controller| {
                    controller.apps_render_limit.set(APP_RENDER_PAGE);
                    controller.render_extensions();
                })
            });
        let weak = Rc::downgrade(this);
        this.widgets
            .extensions_installed_only
            .connect_toggled(move |toggle| {
                with_controller(&weak, |controller| {
                    if toggle.is_active() {
                        // Replace the potentially large remote catalog with the compact
                        // local/workspace inventory as soon as the user returns to the
                        // installed-only view. This lets the remote response and its
                        // rendered metadata be reclaimed instead of keeping it resident.
                        controller.load_local_plugin_catalog();
                    } else {
                        controller.load_full_plugin_catalog(false);
                    }
                    controller.render_extensions();
                })
            });
        let weak = Rc::downgrade(this);
        this.widgets.extensions_refresh.connect_clicked(move |_| {
            with_controller(&weak, Controller::refresh_extension_runtime)
        });
        let weak = Rc::downgrade(this);
        this.widgets
            .marketplace_add
            .connect_clicked(move |_| with_controller(&weak, |c| c.add_marketplace_dialog()));
        let weak = Rc::downgrade(this);
        this.widgets.marketplace_upgrade.connect_clicked(move |_| {
            with_controller(&weak, |c| {
                c.request("marketplace/upgrade", json!({}), PendingKind::Marketplace)
            })
        });

        let weak = Rc::downgrade(this);
        this.widgets
            .qwen_refresh
            .connect_clicked(move |_| with_controller(&weak, Controller::refresh_qwen));
        let weak = Rc::downgrade(this);
        this.widgets.qwen_open_extensions.connect_clicked(move |_| {
            with_controller(&weak, |c| {
                c.widgets.extensions_search.set_text("Qwen Buddy");
                c.widgets.stack.set_visible_child_name("extensions");
            })
        });
        let weak = Rc::downgrade(this);
        this.widgets
            .qwen_open_terminal
            .connect_clicked(move |button| {
                button.set_sensitive(false);
                with_controller(&weak, |c| {
                    c.hub.host(HostAction::QwenTerminalLaunch {
                        cwd: PathBuf::from(c.current_cwd_string()),
                    })
                })
            });
        let weak = Rc::downgrade(this);
        this.widgets
            .qwen_routing_switch
            .connect_active_notify(move |_| {
                with_controller(&weak, Controller::save_qwen_preferences)
            });
        let weak = Rc::downgrade(this);
        this.widgets
            .qwen_luna_check
            .connect_toggled(move |_| with_controller(&weak, Controller::save_qwen_preferences));
        let weak = Rc::downgrade(this);
        this.widgets
            .qwen_condenser_check
            .connect_toggled(move |_| with_controller(&weak, Controller::save_qwen_preferences));
        let weak = Rc::downgrade(this);
        this.widgets
            .qwen_sol_check
            .connect_toggled(move |_| with_controller(&weak, Controller::save_qwen_preferences));
        let weak = Rc::downgrade(this);
        this.widgets
            .qwen_gpu_guard_switch
            .connect_active_notify(move |_| {
                with_controller(&weak, Controller::save_qwen_preferences)
            });
        let weak = Rc::downgrade(this);
        this.widgets
            .computer_plugin_switch
            .connect_active_notify(move |switch| {
                with_controller(&weak, |c| c.set_computer_plugin_enabled(switch.is_active()))
            });
        let weak = Rc::downgrade(this);
        this.widgets
            .computer_server_switch
            .connect_active_notify(move |switch| {
                with_controller(&weak, |c| c.set_computer_server_enabled(switch.is_active()))
            });
        let weak = Rc::downgrade(this);
        this.widgets
            .computer_approval_combo
            .connect_changed(move |combo| {
                let mode = combo
                    .active_id()
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "prompt".into());
                with_controller(&weak, |c| c.set_computer_approval_mode(&mode));
            });
        let weak = Rc::downgrade(this);
        this.widgets
            .computer_doctor
            .connect_clicked(move |_| with_controller(&weak, |c| c.run_computer_doctor()));
        let weak = Rc::downgrade(this);
        this.widgets
            .computer_setup
            .connect_clicked(move |_| with_controller(&weak, |c| c.confirm_computer_setup()));
        let weak = Rc::downgrade(this);
        this.widgets
            .computer_new_task
            .connect_clicked(move |_| with_controller(&weak, |c| c.new_computer_task()));

        let weak = Rc::downgrade(this);
        this.widgets
            .remote_enable
            .connect_clicked(move |_| with_controller(&weak, Controller::enable_remote_host));
        let weak = Rc::downgrade(this);
        this.widgets.remote_disable.connect_clicked(move |_| {
            with_controller(&weak, |c| c.hub.remote(RemoteAction::Disable))
        });
        let weak = Rc::downgrade(this);
        this.widgets
            .remote_pair
            .connect_clicked(move |_| with_controller(&weak, |c| c.start_remote_pairing()));
        let weak = Rc::downgrade(this);
        this.widgets
            .remote_refresh
            .connect_clicked(move |_| with_controller(&weak, |c| c.refresh_remote()));
        let weak = Rc::downgrade(this);
        this.widgets
            .remote_restart
            .connect_clicked(move |_| with_controller(&weak, |c| c.restart_remote_host()));

        let weak = Rc::downgrade(this);
        this.widgets
            .diagnostics_refresh
            .connect_clicked(move |_| with_controller(&weak, |c| c.run_diagnostics()));
        let weak = Rc::downgrade(this);
        this.widgets
            .update_check
            .connect_clicked(move |_| with_controller(&weak, |c| c.check_updates()));
        let weak = Rc::downgrade(this);
        this.widgets
            .update_apply
            .connect_clicked(move |_| with_controller(&weak, |c| c.confirm_update()));
        let weak = Rc::downgrade(this);
        this.widgets
            .update_rollback
            .connect_clicked(move |_| with_controller(&weak, |c| c.confirm_rollback()));
        let weak = Rc::downgrade(this);
        this.widgets
            .remote_doctor_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.run_remote_doctor()));

        let weak = Rc::downgrade(this);
        this.widgets
            .settings_save
            .connect_clicked(move |_| with_controller(&weak, |c| c.save_preferences()));
        let weak = Rc::downgrade(this);
        this.widgets
            .login_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.login()));
        let weak = Rc::downgrade(this);
        this.widgets
            .account_add
            .connect_clicked(move |_| with_controller(&weak, |c| c.add_account_profile()));
        let weak = Rc::downgrade(this);
        this.widgets.account_settings.connect_clicked(move |_| {
            with_controller(&weak, |c| {
                c.widgets.stack.set_visible_child_name("settings");
                c.widgets.account_menu.popdown();
            })
        });
        let weak = Rc::downgrade(this);
        this.widgets
            .logout_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.logout()));
        let weak = Rc::downgrade(this);
        this.widgets
            .account_refresh
            .connect_clicked(move |_| with_controller(&weak, |c| c.refresh_account_usage()));
        let weak = Rc::downgrade(this);
        this.widgets
            .reset_credit_button
            .connect_clicked(move |_| with_controller(&weak, |c| c.confirm_use_reset_credit()));

        let weak = Rc::downgrade(this);
        this.widgets
            .stack
            .connect_visible_child_name_notify(move |stack| {
                with_controller(&weak, |c| {
                    let page = workspace_page_from_name(stack.visible_child_name().as_deref());
                    c.state.borrow_mut().page = page;
                    if page == WorkspacePage::Chatgpt {
                        c.chatgpt.activate();
                    } else {
                        c.chatgpt.deactivate();
                    }
                    c.render_current_page();
                });
            });
    }

    fn poll_backend(&self) {
        let activity_changed = self.poll_rollout_activity();
        let mut events = Vec::with_capacity(BACKEND_EVENT_BATCH);
        self.hub
            .drain_events(BACKEND_EVENT_BATCH, |event| events.push(event));
        if events.is_empty() {
            if activity_changed {
                self.schedule_thread_render();
                self.update_sleep_inhibition();
            }
            return;
        }
        let mut needs_render = false;
        for event in events {
            needs_render |= !matches!(&event, BackendEvent::Log(_));
            self.handle_backend_event(event);
        }
        if needs_render {
            if self.state.borrow().page == WorkspacePage::Chat {
                // Streaming notifications can arrive faster than GTK can
                // rebuild a long heterogeneous transcript. Keep connection
                // and approval controls immediate, but coalesce transcript
                // work into one bounded refresh.
                self.render_connection();
                self.render_approval();
                self.schedule_transcript_render();
            } else {
                self.render_current_page();
            }
        }
        if activity_changed {
            self.schedule_thread_render();
        }
        if needs_render || activity_changed {
            self.update_sleep_inhibition();
        }
    }

    fn schedule_thread_render(&self) {
        if self.thread_render_scheduled.replace(true) {
            return;
        }
        let weak = self.weak_self.borrow().clone();
        glib::timeout_add_local_once(Duration::from_millis(100), move || {
            with_controller(&weak, |controller| {
                controller.thread_render_scheduled.set(false);
                controller.render_threads();
            });
        });
    }

    fn schedule_transcript_render(&self) {
        if self.transcript_render_scheduled.replace(true) {
            return;
        }
        let weak = self.weak_self.borrow().clone();
        glib::timeout_add_local_once(TRANSCRIPT_RENDER_DEBOUNCE, move || {
            with_controller(&weak, |controller| {
                controller.transcript_render_scheduled.set(false);
                if controller.state.borrow().page == WorkspacePage::Chat {
                    controller.render_transcript();
                }
            });
        });
    }

    fn activity_indicator(&self, tooltip: Option<&str>) -> gtk::Label {
        let indicator = gtk::Label::new(Some(ACTIVITY_FRAMES[self.activity_phase.get()]));
        indicator.set_size_request(14, 14);
        indicator.set_halign(gtk::Align::End);
        indicator.set_valign(gtk::Align::Center);
        indicator.set_tooltip_text(tooltip);
        indicator.add_css_class("task-running-spinner");
        indicator.update_property(&[gtk::accessible::Property::Label("Running")]);
        self.activity_indicators
            .borrow_mut()
            .push(indicator.downgrade());
        indicator
    }

    fn tick_activity_indicators(&self) {
        if !self.widgets.window.is_visible() {
            return;
        }
        let phase = (self.activity_phase.get() + 1) % ACTIVITY_FRAMES.len();
        self.activity_phase.set(phase);
        let frame = ACTIVITY_FRAMES[phase];
        self.activity_indicators.borrow_mut().retain(|weak| {
            let Some(indicator) = weak.upgrade() else {
                return false;
            };
            indicator.set_label(frame);
            true
        });
    }

    fn invalidate_transcript(&self) {
        self.last_transcript_revision.set(None);
    }

    fn poll_rollout_activity(&self) -> bool {
        let mut latest = None;
        while let Ok(snapshot) = self.rollout_activity_rx.try_recv() {
            latest = Some(snapshot);
        }
        let Some(latest) = latest else {
            return false;
        };
        let mut state = self.state.borrow_mut();
        if state.external_running_threads == latest {
            false
        } else {
            state.external_running_threads = latest;
            drop(state);
            self.unsubscribe_idle_threads();
            true
        }
    }

    fn handle_backend_event(&self, event: BackendEvent) {
        match event {
            BackendEvent::Connecting => {
                self.state.borrow_mut().connection = ConnectionState::Connecting
            }
            BackendEvent::Connected => {
                self.state.borrow_mut().connection = ConnectionState::Connecting
            }
            BackendEvent::Ready(value) => {
                if self.hub.uses_managed_transport() {
                    self.remote_handoff_pending.set(false);
                }
                let first_ready = !self.backend_bootstrapped.replace(true);
                let mut state = self.state.borrow_mut();
                state.connection = ConnectionState::Ready;
                let active_thread_id = state.active_thread_id.clone();
                state.platform_family = value
                    .pointer("/platform/family")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                state.platform_os = value
                    .pointer("/platform/os")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                drop(state);
                if let Some(thread_id) = active_thread_id
                    && !(smoke_fixtures_enabled() && thread_id == SMOKE_FIXTURE_THREAD_ID)
                {
                    self.open_thread_live(&thread_id);
                }
                if first_ready {
                    self.bootstrap();
                    if self.login_after_profile_ready.replace(false) {
                        self.login();
                    }
                } else {
                    // A transport reconnect only needs live task/account state.
                    // Reloading models, plugins, apps, skills, hooks, and policy
                    // catalogs on every socket blip creates a large request and
                    // rendering storm before the open transcript can return.
                    self.refresh_threads();
                    self.refresh_account_usage();
                    self.refresh_remote();
                }
            }
            BackendEvent::Disconnected(message) => {
                let mut state = self.state.borrow_mut();
                let interrupted_goals = state
                    .thread_goals
                    .iter()
                    .filter(|(thread_id, goal)| {
                        goal.get("status").and_then(Value::as_str) == Some("active")
                            && state.threads.get(*thread_id).is_some_and(thread_is_running)
                    })
                    .map(|(thread_id, _)| thread_id.clone())
                    .collect::<Vec<_>>();
                state.connection = ConnectionState::Disconnected;
                state.last_error = Some(message.clone());
                // Responses from the dead transport cannot arrive. Retaining
                // these entries permanently blocks sends and makes every Ready
                // cycle accumulate another full bootstrap in memory.
                state.pending.clear();
                state.resumed_threads.clear();
                state.reset_credit_in_flight = false;
                drop(state);
                if !interrupted_goals.is_empty() {
                    let mut stored = self.stored.borrow_mut();
                    for thread_id in interrupted_goals {
                        stored.goal_stop_reasons.insert(
                            thread_id,
                            format!("Stopped because the Codex connection was lost: {message}"),
                        );
                    }
                    drop(stored);
                    self.persist();
                    self.render_goal_selector();
                }
            }
            BackendEvent::OversizedFrame(bytes) => {
                let message = format!(
                    "Skipped an unusually large history page ({} MiB) while keeping Remote connected. Load activity in smaller pages.",
                    bytes.div_ceil(1024 * 1024)
                );
                let mut state = self.state.borrow_mut();
                state.pending.retain(|_, pending| {
                    !matches!(
                        pending,
                        PendingKind::ThreadTurns { .. } | PendingKind::ThreadTurnDetails { .. }
                    )
                });
                state.transcript_detail_loading.clear();
                if let Some(thread_id) = state.active_thread_id.clone() {
                    state.transcript_detail_cursors.remove(&thread_id);
                    state.transcript_detail_available.remove(&thread_id);
                }
                state.last_error = Some(message.clone());
                drop(state);
                self.toast(&message);
            }
            BackendEvent::Message(message) => {
                let message = match message {
                    RpcEnvelope::Response(response) => {
                        self.handle_response(response);
                        return;
                    }
                    message => message,
                };
                let thread_list_changed = matches!(
                    &message,
                    RpcEnvelope::Notification(notification)
                        if matches!(
                            notification.method.as_str(),
                            "thread/started"
                                | "thread/status/changed"
                                | "turn/started"
                                | "turn/completed"
                                | "thread/archived"
                                | "thread/deleted"
                                | "thread/unarchived"
                                | "thread/name/updated"
                        )
                );
                let rate_limits_changed = matches!(
                    &message,
                    RpcEnvelope::Notification(notification)
                        if notification.method == "account/rateLimits/updated"
                );
                if let RpcEnvelope::Request(request) = &message
                    && request.method == "currentTime/read"
                {
                    self.hub.respond(
                        request.id.clone(),
                        json!({"currentTimeAt": chrono::Utc::now().timestamp()}),
                    );
                    return;
                }
                if let RpcEnvelope::Notification(notification) = &message
                    && notification.method == "mcpServer/oauthLogin/completed"
                {
                    let success = notification
                        .params
                        .get("success")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    if success {
                        self.toast("Extension sign-in completed");
                    } else if let Some(error) =
                        notification.params.get("error").and_then(Value::as_str)
                    {
                        self.toast(&format!("Extension sign-in failed: {error}"));
                    }
                    self.request(
                        "mcpServerStatus/list",
                        json!({}),
                        PendingKind::Bootstrap("mcp"),
                    );
                }
                let completed = matches!(
                    &message,
                    RpcEnvelope::Notification(notification) if notification.method == "turn/completed"
                );
                let completed_thread_id = match &message {
                    RpcEnvelope::Notification(notification)
                        if notification.method == "turn/completed" =>
                    {
                        notification
                            .params
                            .get("threadId")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    }
                    _ => None,
                };
                let context_observation = match &message {
                    RpcEnvelope::Notification(notification)
                        if matches!(
                            notification.method.as_str(),
                            "turn/completed" | "thread/tokenUsage/updated"
                        ) =>
                    {
                        notification
                            .params
                            .get("threadId")
                            .and_then(Value::as_str)
                            .map(|thread_id| (notification.method.clone(), thread_id.to_owned()))
                    }
                    _ => None,
                };
                let context_compaction = match &message {
                    RpcEnvelope::Notification(notification) => {
                        context_compaction_event(&notification.method, &notification.params)
                    }
                    _ => None,
                };
                let deleted_thread = match &message {
                    RpcEnvelope::Notification(notification)
                        if notification.method == "thread/deleted" =>
                    {
                        notification
                            .params
                            .get("threadId")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    }
                    _ => None,
                };
                self.state.borrow_mut().apply_server_message(&message);
                if let Some(event) = context_compaction {
                    self.observe_context_compaction(event);
                }
                if let RpcEnvelope::Notification(notification) = &message {
                    if notification.method == "thread/settings/updated"
                        && let Some(thread_id) =
                            self.store_task_runtime_settings(&notification.params)
                        && self.state.borrow().active_thread_id.as_deref()
                            == Some(thread_id.as_str())
                    {
                        self.apply_active_task_settings();
                    }
                    self.observe_turn_routing(&notification.method, &notification.params);
                    self.observe_token_savings(&notification.method, &notification.params);
                    self.observe_macro_experiment(&notification.method, &notification.params);
                    self.observe_goal_lifecycle(&notification.method, &notification.params);
                }
                if let Some(thread_id) = deleted_thread {
                    self.purge_deleted_thread_context(&thread_id);
                }
                if let Some((method, thread_id)) = context_observation {
                    if method == "thread/tokenUsage/updated" {
                        self.observe_context_usage(&thread_id);
                    } else {
                        self.maybe_manage_context(&thread_id);
                    }
                }
                if thread_list_changed {
                    self.schedule_thread_render();
                }
                if rate_limits_changed {
                    self.refresh_account_usage();
                }
                if completed {
                    self.notify_turn_complete();
                    self.maybe_recover_remote_host();
                    if self.remote_handoff_pending.get() {
                        self.maybe_handoff_to_managed_transport();
                    }
                    if let Some(thread_id) = completed_thread_id.as_deref()
                        && self.state.borrow().active_thread_id.as_deref() == Some(thread_id)
                    {
                        // Remote/iOS turns can arrive while Native is idle or
                        // between transport reconnects. Re-read the bounded
                        // canonical page at completion so missed progressive
                        // notifications cannot leave the open task stale.
                        self.request_thread_summary(thread_id);
                    }
                    if completed_thread_id.as_deref().is_some_and(|thread_id| {
                        self.state.borrow().active_thread_id.as_deref() != Some(thread_id)
                    }) {
                        self.unsubscribe_idle_threads();
                    }
                }
            }
            BackendEvent::Log(line) => self.state.borrow_mut().push_log(line),
            BackendEvent::RemoteResult { action, value } => {
                let health = matches!(&action, RemoteAction::Refresh)
                    .then(|| remote_recovery_reason(&value, self.remote_expected_enabled.get()));
                self.handle_remote_result(action, value);
                if let Some(health) = health {
                    self.observe_remote_health(health);
                }
            }
            BackendEvent::RemoteError { action, message } => {
                self.state.borrow_mut().remote = RemoteState::Error(message.clone());
                if matches!(&action, RemoteAction::Refresh) {
                    self.observe_remote_health(Some(message.clone()));
                }
                if !matches!(&action, RemoteAction::Refresh) {
                    self.toast(&format!("Remote {action:?} failed: {message}"));
                }
            }
            BackendEvent::ComputerResult { action, value } => {
                let was_setup = matches!(&action, ComputerAction::Setup(_));
                let report = match action {
                    ComputerAction::Setup(_) => value.get("after").cloned().unwrap_or(value),
                    ComputerAction::Doctor(_) => value,
                };
                let mut state = self.state.borrow_mut();
                state.computer_busy = false;
                state.computer_report = Some(report);
                drop(state);
                if was_setup {
                    self.toast("Linux Computer Use setup finished");
                }
            }
            BackendEvent::ComputerError { action, message } => {
                self.state.borrow_mut().computer_busy = false;
                self.toast(&format!("Computer Use {action:?} failed: {message}"));
            }
            BackendEvent::BuddyProgress {
                thread_id,
                turn_id,
                backend,
                progress,
            } => self.update_buddy_progress(&thread_id, &turn_id, &backend, &progress),
            BackendEvent::HostResult { action, value } => match action {
                HostAction::AppShotCapture => {
                    self.state.borrow_mut().latest_appshot = Some(value);
                    self.toast("AppShot captured");
                }
                HostAction::RecordStart { .. } => {
                    self.state.borrow_mut().recording_session = Some(value);
                    self.toast("Recording started");
                }
                HostAction::RecordFrame { .. } => {
                    self.state.borrow_mut().recording_session = Some(value);
                    self.widgets.record_note.set_text("");
                    self.toast("Evidence frame captured");
                }
                HostAction::RecordStop { .. } => {
                    self.state.borrow_mut().recording_session = Some(value);
                    self.toast("Draft replay skill created for review");
                }
                HostAction::RecordInstall { .. } => {
                    self.toast("Recorded skill installed for new tasks");
                    self.refresh_extensions();
                }
                HostAction::MemoriaJobs => {
                    self.memoria_jobs_refresh_pending.set(false);
                    self.render_memoria_jobs(&value);
                }
                HostAction::Diagnostics => {
                    self.state.borrow_mut().diagnostics = Some(value);
                }
                HostAction::UpdateCheck => {
                    self.state.borrow_mut().update_report = Some(value);
                }
                HostAction::UpdateApply => {
                    self.toast("Update installed. Restart Codex Native when ready.");
                    self.hub.host(HostAction::UpdateCheck);
                }
                HostAction::UpdateRollback(_) => {
                    self.toast("Rollback installed. Restart Codex Native when ready.");
                    self.hub.host(HostAction::UpdateCheck);
                }
                HostAction::RemoteDoctor { .. } | HostAction::RemoteDoctorProfile { .. } => {
                    self.state.borrow_mut().remote_doctor = Some(value);
                }
                HostAction::RemoteRestart { .. } | HostAction::RemoteRestartProfile { .. } => {
                    self.remote_recovery_in_progress.set(false);
                    self.remote_recovery_pending.set(false);
                    self.remote_unhealthy_samples.set(0);
                    self.remote_recovery_reason.borrow_mut().take();
                    self.last_remote_recovery.set(Some(Instant::now()));
                    self.widgets.remote_restart.set_sensitive(true);
                    self.toast("Remote host restarted");
                    self.hub.reconnect();
                    self.refresh_remote();
                    self.run_remote_doctor();
                }
                HostAction::RemoteAutostart { enabled } => {
                    self.widgets.settings_save.set_sensitive(true);
                    self.widgets.settings_save.set_label("Save settings");
                    self.toast(if enabled {
                        "Settings saved. Remote host will start at login."
                    } else {
                        "Settings saved. Remote host autostart disabled."
                    });
                    self.refresh_remote();
                }
                HostAction::WorkspaceDoctor { .. } => {
                    let active_units = value
                        .get("activeUnits")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    let mut state = self.state.borrow_mut();
                    let known_unit = state
                        .workspace_session
                        .as_ref()
                        .and_then(|session| session.get("unit"))
                        .and_then(Value::as_str);
                    if known_unit.is_some_and(|unit| {
                        !active_units
                            .iter()
                            .any(|value| value.as_str() == Some(unit))
                    }) {
                        state.workspace_session = None;
                    }
                    if state.workspace_session.is_none()
                        && let Some(unit) = active_units.first().and_then(Value::as_str)
                    {
                        state.workspace_session = Some(json!({
                            "unit": unit,
                            "recovered": true,
                            "active": true
                        }));
                    }
                    state.workspace_report = Some(value);
                }
                HostAction::WorkspaceLaunch { .. } => {
                    self.state.borrow_mut().workspace_session = Some(value);
                    self.toast("Isolated agent workspace launched");
                }
                HostAction::WorkspaceStop { .. } => {
                    self.state.borrow_mut().workspace_session = None;
                    self.toast("Isolated agent workspace stopped");
                    self.check_agent_workspace();
                }
                HostAction::QwenTerminalLaunch { .. } => {
                    self.widgets.qwen_open_terminal.set_sensitive(true);
                    self.toast("OpenCode opened with local Qwen in a terminal");
                }
                HostAction::BuddyDelegate {
                    thread_id,
                    turn_id,
                    backend,
                    prompt,
                    context,
                    effort,
                    ..
                } => {
                    let author = buddy_author(&backend);
                    let text = value
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or("Buddy returned no displayable response")
                        .to_owned();
                    self.record_completed_buddy_usage(
                        &thread_id,
                        &turn_id,
                        &backend,
                        value.get("metrics"),
                    );
                    self.finish_buddy_turn(&thread_id, &turn_id, author, Ok(&text));
                    self.record_external_buddy_savings(
                        &thread_id,
                        &turn_id,
                        &backend,
                        &effort,
                        &prompt,
                        &context,
                        &text,
                        value.get("metrics"),
                    );
                    self.toast(&format!("{author} completed without Codex usage"));
                }
                HostAction::QwenRefresh => {
                    let mut state = self.state.borrow_mut();
                    state.qwen_busy = false;
                    state.qwen_report = Some(value);
                }
                HostAction::ContextCheckpoint {
                    request,
                    compact_after,
                    automatic,
                } => {
                    let thread_id = request.thread_id.clone();
                    self.state
                        .borrow_mut()
                        .context_checkpoint_inflight
                        .remove(&thread_id);
                    match serde_json::from_value::<ContextCheckpointReceipt>(value) {
                        Ok(receipt) => {
                            let generation = receipt.context_generation;
                            let mut stored = self.stored.borrow_mut();
                            stored
                                .context_checkpoints
                                .insert(thread_id.clone(), receipt.clone());
                            let metrics =
                                stored.context_metrics.entry(thread_id.clone()).or_default();
                            metrics.checkpoint_count = metrics.checkpoint_count.saturating_add(1);
                            metrics.last_checkpoint_generation = Some(generation);
                            metrics.last_error = None;
                            drop(stored);
                            if let Some(measurement) = self
                                .state
                                .borrow_mut()
                                .context_compaction_measurements
                                .get_mut(&thread_id)
                                .filter(|measurement| measurement.generation_before == generation)
                            {
                                measurement.checkpointed = true;
                                measurement.checkpoint_elapsed_ms = receipt.elapsed_ms;
                                measurement.checkpoint_hashed_bytes = receipt.hashed_bytes;
                                measurement.checkpoint_reused_evidence = receipt.reused_evidence;
                            }
                            self.persist();
                            self.render_context();
                            if compact_after {
                                self.begin_context_compaction(&receipt, automatic);
                            } else if !automatic {
                                self.toast("Context checkpoint saved");
                            }
                        }
                        Err(error) => {
                            self.toast(&format!("Could not read the context checkpoint: {error}"));
                        }
                    }
                }
                HostAction::ContextDelete { .. } => {}
                HostAction::BrowserOpen { .. } => self.toast("Opened external browser"),
                HostAction::OpenEditor { path, .. } => self.toast(if path.is_file() {
                    "Opened file in editor"
                } else {
                    "Opened project in editor"
                }),
                HostAction::Reveal(_) => self.toast("Opened project folder"),
                HostAction::LoadRolloutChat { thread_id, .. } => {
                    self.local_history_loading.borrow_mut().remove(&thread_id);
                    let projection_repaired = value
                        .pointer("/projectionRepair/status")
                        .and_then(Value::as_str)
                        == Some("repaired");
                    if projection_repaired {
                        tracing::info!(
                            %thread_id,
                            projected_turns = value
                                .pointer("/projectionRepair/projectedTurns")
                                .and_then(|value| value.as_u64())
                                .unwrap_or_default(),
                            projected_items = value
                                .pointer("/projectionRepair/projectedItems")
                                .and_then(|value| value.as_u64())
                                .unwrap_or_default(),
                            skipped_items = value
                                .pointer("/projectionRepair/skippedItems")
                                .and_then(|value| value.as_u64())
                                .unwrap_or_default(),
                            next_ordinal = value
                                .pointer("/projectionRepair/nextOrdinal")
                                .and_then(|value| value.as_u64())
                                .unwrap_or_default(),
                            "repaired the shared Codex history projection from its canonical rollout"
                        );
                    }
                    let fingerprint = (
                        value.get("size").and_then(Value::as_u64),
                        value.get("modifiedSeconds").and_then(Value::as_u64),
                        value
                            .get("modifiedNanos")
                            .and_then(Value::as_u64)
                            .and_then(|value| u32::try_from(value).ok()),
                    );
                    if let (Some(size), Some(seconds), Some(nanos)) = fingerprint {
                        self.local_history_fingerprints
                            .borrow_mut()
                            .insert(thread_id.clone(), (size, seconds, nanos));
                    }
                    if self.state.borrow().active_thread_id.as_deref() != Some(thread_id.as_str()) {
                        return;
                    }
                    let recovered = value
                        .get("turns")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(|turn| serde_json::from_value::<Turn>(turn.clone()).ok())
                        .collect::<Vec<_>>();
                    let recovered_count = recovered.len();
                    let (added_turns, added_items) = self
                        .state
                        .borrow_mut()
                        .merge_local_chat_turns(&thread_id, recovered);
                    if added_turns > 0 || added_items > 0 {
                        tracing::info!(
                            %thread_id,
                            recovered_count,
                            added_turns,
                            added_items,
                            "restored canonical rollout chat missing from the server projection"
                        );
                    }
                    if projection_repaired {
                        self.request_thread_summary(&thread_id);
                    }
                }
                HostAction::VoiceCapture { .. } => {
                    self.widgets.dictate_button.set_sensitive(true);
                    let Some(thread_id) = self.state.borrow().active_thread_id.clone() else {
                        self.toast("The active task closed before dictation finished");
                        return;
                    };
                    self.request(
                        "thread/realtime/start",
                        json!({
                            "threadId": thread_id,
                            "outputModality": "text",
                            "transport": {"type": "websocket"},
                            "version": "v2",
                            "includeStartupContext": true,
                            "flushTranscriptTailOnSessionEnd": true
                        }),
                        PendingKind::VoiceStart {
                            thread_id,
                            audio: value,
                        },
                    );
                }
                HostAction::Speak(_) => self.toast("Read Aloud started"),
            },
            BackendEvent::HostError { action, message } => {
                if matches!(&action, HostAction::MemoriaJobs) {
                    self.memoria_jobs_refresh_pending.set(false);
                    self.widgets.memoria_jobs_label.set_label("Memoria jobs: —");
                    self.widgets
                        .memoria_jobs_label
                        .remove_css_class("memoria-jobs-active");
                    self.widgets
                        .memoria_jobs_label
                        .set_tooltip_text(Some(&format!("Memoria status unavailable: {message}")));
                    return;
                }
                if let HostAction::LoadRolloutChat { thread_id, path } = &action {
                    self.local_history_loading.borrow_mut().remove(thread_id);
                    if let Some(fingerprint) = local_rollout_fingerprint(path) {
                        self.local_history_fingerprints
                            .borrow_mut()
                            .insert(thread_id.clone(), fingerprint);
                    }
                    tracing::warn!(
                        %thread_id,
                        path = %path.display(),
                        %message,
                        "canonical rollout chat recovery failed"
                    );
                    return;
                }
                if matches!(&action, HostAction::ContextDelete { .. }) {
                    tracing::warn!(%message, "failed to remove deleted task checkpoints");
                    return;
                }
                if let HostAction::ContextCheckpoint { request, .. } = &action {
                    let thread_id = request.thread_id.clone();
                    self.state
                        .borrow_mut()
                        .context_checkpoint_inflight
                        .remove(&thread_id);
                    self.stored
                        .borrow_mut()
                        .context_metrics
                        .entry(thread_id)
                        .or_default()
                        .last_error = Some(message.clone());
                    self.persist();
                    self.render_context();
                    self.toast(&format!("Context checkpoint failed: {message}"));
                    return;
                }
                if let HostAction::BuddyDelegate {
                    thread_id,
                    turn_id,
                    backend,
                    ..
                } = &action
                {
                    let author = buddy_author(backend);
                    self.finish_buddy_turn(thread_id, turn_id, author, Err(&message));
                    self.toast(&format!("{author} failed: {message}"));
                    return;
                }
                if matches!(&action, HostAction::VoiceCapture { .. }) {
                    self.widgets.dictate_button.set_sensitive(true);
                }
                if matches!(
                    &action,
                    HostAction::RemoteRestart { .. } | HostAction::RemoteRestartProfile { .. }
                ) {
                    self.remote_recovery_in_progress.set(false);
                    self.remote_recovery_pending.set(false);
                    self.last_remote_recovery.set(Some(Instant::now()));
                    self.widgets.remote_restart.set_sensitive(true);
                }
                if matches!(&action, HostAction::RemoteAutostart { .. }) {
                    self.widgets.settings_save.set_sensitive(true);
                    self.widgets.settings_save.set_label("Save settings");
                }
                if matches!(&action, HostAction::QwenRefresh) {
                    self.state.borrow_mut().qwen_busy = false;
                    self.widgets.qwen_refresh.set_sensitive(true);
                }
                if matches!(&action, HostAction::QwenTerminalLaunch { .. }) {
                    self.widgets.qwen_open_terminal.set_sensitive(true);
                }
                if matches!(
                    &action,
                    HostAction::WorkspaceDoctor { .. }
                        | HostAction::WorkspaceLaunch { .. }
                        | HostAction::WorkspaceStop { .. }
                ) {
                    self.widgets.workspace_doctor.set_sensitive(true);
                    self.widgets.workspace_launch.set_sensitive(true);
                    self.widgets
                        .workspace_stop
                        .set_sensitive(self.state.borrow().workspace_session.is_some());
                }
                if matches!(
                    &action,
                    HostAction::RecordStart { .. }
                        | HostAction::RecordFrame { .. }
                        | HostAction::RecordStop { .. }
                ) {
                    self.widgets.record_start.set_sensitive(true);
                    self.widgets.record_frame.set_sensitive(true);
                    self.widgets.record_stop.set_sensitive(true);
                }
                self.toast(&format!("{action:?} failed: {message}"));
            }
        }
    }

    fn bootstrap(&self) {
        let (archived, sort_key, cwd) = {
            let stored = self.stored.borrow();
            (
                stored.show_archived_threads,
                stored.thread_sort.clone(),
                self.current_cwd_string(),
            )
        };
        for (name, method, params) in initial_bootstrap_requests(archived, &sort_key, &cwd) {
            self.request(method, params, PendingKind::Bootstrap(name));
        }
        self.refresh_account_usage();
        self.refresh_remote();
        let qwen_needed = {
            let stored = self.stored.borrow();
            stored.preferences.qwen_buddy.routing_enabled
                || routing::normalize_mode(&stored.preferences.routing_mode) == MODE_QWEN_ASSIST
                || routing::is_delegate_mode(&stored.preferences.routing_mode)
                || stored.task_routing.values().any(|state| {
                    routing::normalize_mode(&state.mode) == MODE_QWEN_ASSIST
                        || routing::is_delegate_mode(&state.mode)
                })
        };
        if qwen_needed {
            self.ensure_qwen_loaded();
        }
    }

    fn request(&self, method: &str, params: Value, kind: PendingKind) {
        let id = self.hub.request(method, params);
        self.state.borrow_mut().pending.insert(id, kind);
    }

    fn request_thread_summary(&self, thread_id: &str) {
        if self
            .state
            .borrow()
            .threads
            .get(thread_id)
            .is_some_and(|thread| thread.native_buddy_only)
        {
            return;
        }
        let already_loading = self.state.borrow().pending.values().any(|pending| {
            matches!(
                pending,
                PendingKind::ThreadTurns {
                    thread_id: pending_thread_id,
                    prepend: false,
                } if pending_thread_id == thread_id
            )
        });
        if already_loading {
            return;
        }
        self.request(
            "thread/turns/list",
            json!({
                "threadId": thread_id,
                "limit": TRANSCRIPT_PAGE_TURNS,
                "itemsView": "summary",
                "sortDirection": "desc"
            }),
            PendingKind::ThreadTurns {
                thread_id: thread_id.to_owned(),
                prepend: false,
            },
        );
    }

    fn refresh_active_local_chat_history(&self) {
        let (thread_id, path, has_turns, history_pending) = {
            let state = self.state.borrow();
            let Some(thread_id) = state.active_thread_id.clone() else {
                return;
            };
            let Some(thread) = state.threads.get(&thread_id) else {
                return;
            };
            let Some(path) = thread.path.clone() else {
                return;
            };
            let history_pending = state.pending.values().any(|pending| match pending {
                PendingKind::OpenThread(pending_thread_id)
                | PendingKind::UnsubscribeThread(pending_thread_id) => {
                    pending_thread_id == &thread_id
                }
                PendingKind::ResumeAndSend {
                    thread_id: pending_thread_id,
                    ..
                }
                | PendingKind::ThreadTurns {
                    thread_id: pending_thread_id,
                    ..
                }
                | PendingKind::ThreadTurnDetails {
                    thread_id: pending_thread_id,
                } => pending_thread_id == &thread_id,
                _ => false,
            });
            (thread_id, path, !thread.turns.is_empty(), history_pending)
        };
        if history_pending
            || path.extension().and_then(|extension| extension.to_str()) != Some("jsonl")
        {
            return;
        }
        let Some(fingerprint) = local_rollout_fingerprint(&path) else {
            return;
        };
        if self.local_history_loading.borrow().contains(&thread_id) {
            return;
        }
        if has_turns
            && self
                .local_history_fingerprints
                .borrow()
                .get(&thread_id)
                .is_some_and(|loaded| loaded == &fingerprint)
        {
            return;
        }
        self.local_history_loading
            .borrow_mut()
            .insert(thread_id.clone());
        self.hub
            .host(HostAction::LoadRolloutChat { thread_id, path });
    }

    fn open_timeline_thread(&self, thread_id: &str) {
        let (owner_account_id, active_account_id) = {
            let stored = self.stored.borrow();
            (
                stored.task_owner(thread_id).map(str::to_owned),
                stored.active_account_id.clone(),
            )
        };
        if let Some(owner_account_id) = owner_account_id
            && owner_account_id != active_account_id
        {
            *self.deferred_shared_thread_open.borrow_mut() = Some(thread_id.to_owned());
            self.activate_account_profile(&owner_account_id, false);
            if self.stored.borrow().active_account_id != owner_account_id {
                self.deferred_shared_thread_open.borrow_mut().take();
                return;
            }
            self.toast("Switching to this task's account…");
            return;
        }

        let local_smoke_fixture = smoke_fixtures_enabled() && thread_id == SMOKE_FIXTURE_THREAD_ID;
        let exists = self.state.borrow().threads.contains_key(thread_id);
        if !exists {
            self.toast("This task is no longer available in its owning account");
            return;
        }
        {
            let mut state = self.state.borrow_mut();
            state.activate_thread(thread_id.to_owned());
            if local_smoke_fixture {
                install_smoke_fixtures(&mut state);
            }
        }
        self.restore_buddy_history(thread_id);
        if !local_smoke_fixture {
            self.unsubscribe_idle_threads();
            self.open_thread_live(thread_id);
        }
        self.apply_active_task_settings();
        if let Some(cwd) = self
            .state
            .borrow()
            .threads
            .get(thread_id)
            .map(|thread| thread.cwd.clone())
        {
            self.stored
                .borrow_mut()
                .last_thread_by_project
                .insert(cwd, thread_id.to_owned());
            self.persist();
        }
        self.widgets.stack.set_visible_child_name("chat");
        self.render_threads();
        self.render_current_page();
    }

    fn record_active_profile_task(&self, thread: &ThreadSummary) {
        if thread.parent_thread_id.is_some() || thread.native_buddy_only {
            return;
        }
        let changed = {
            let mut stored = self.stored.borrow_mut();
            let active_account_id = stored.active_account_id.clone();
            stored.record_shared_tasks(&active_account_id, [thread.clone()])
        };
        if changed {
            self.persist();
        }
    }

    fn open_deferred_shared_thread(&self) {
        let Some(thread_id) = self.deferred_shared_thread_open.borrow_mut().take() else {
            return;
        };
        let owned_by_active_profile = {
            let stored = self.stored.borrow();
            stored.task_owner(&thread_id) == Some(stored.active_account_id.as_str())
        };
        if owned_by_active_profile {
            self.open_timeline_thread(&thread_id);
        }
    }

    fn open_thread_live(&self, thread_id: &str) {
        let state = self.state.borrow();
        if state
            .threads
            .get(thread_id)
            .is_some_and(|thread| thread.native_buddy_only)
        {
            return;
        }
        let resume_pending = state.pending.values().any(|pending| {
            matches!(pending, PendingKind::OpenThread(pending_id) if pending_id == thread_id)
        });
        let needs_resume = task_needs_resume(&state.resumed_threads, thread_id);
        drop(state);
        if needs_resume && !resume_pending {
            self.request(
                "thread/resume",
                thread_resume_params(thread_id),
                PendingKind::OpenThread(thread_id.to_owned()),
            );
        } else if !needs_resume {
            self.request_thread_summary(thread_id);
        }
    }

    fn unsubscribe_idle_threads(&self) {
        if self.state.borrow().connection != ConnectionState::Ready {
            return;
        }
        let thread_ids = idle_resumed_thread_ids(&self.state.borrow());
        for thread_id in thread_ids {
            self.request(
                "thread/unsubscribe",
                json!({"threadId": thread_id}),
                PendingKind::UnsubscribeThread(thread_id),
            );
        }
    }

    fn apply_initial_turns_page(&self, result: &Value, thread_id: &str) -> bool {
        let Some(page) = result.get("initialTurnsPage") else {
            return false;
        };
        let mut turns = value_array(page)
            .iter()
            .filter_map(|value| serde_json::from_value(value.clone()).ok())
            .collect::<Vec<_>>();
        turns.reverse();
        let next_cursor = page
            .get("nextCursor")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let has_turns = !turns.is_empty();
        let mut state = self.state.borrow_mut();
        state.set_thread_turns(thread_id, turns, next_cursor, false);
        if has_turns {
            state
                .transcript_detail_available
                .insert(thread_id.to_owned());
        }
        true
    }

    fn should_auto_hydrate_thread_activity(&self, thread_id: &str) -> bool {
        let state = self.state.borrow();
        state.transcript_detail_available.contains(thread_id)
            && state
                .threads
                .get(thread_id)
                .is_none_or(auto_hydrate_turn_activity)
    }

    fn handle_response(&self, response: RpcResponse) {
        let pending = self.state.borrow_mut().pending.remove(&response.id);
        let reset_credit_request =
            matches!(&pending, Some(PendingKind::ConsumeRateLimitResetCredit));
        if let Some(error) = response.error {
            let message = format!("{} ({})", error.message, error.code);
            let bulk_task_mutation = pending.as_ref().is_some_and(is_bulk_task_mutation);
            let safe_continuation = error.message.contains("memory-safe continuation")
                || error.message.contains("safe-resume window");
            if let Some(PendingKind::SteerTurn {
                thread_id,
                pending_item_id,
                ..
            }) = pending.as_ref()
            {
                self.state
                    .borrow_mut()
                    .remove_pending_steer(thread_id, pending_item_id);
                self.schedule_transcript_render();
            }
            if let Some(PendingKind::SteerTurn {
                thread_id,
                prompt,
                attachments,
                ..
            }) = pending.as_ref()
                && is_no_active_turn_to_steer(error.code, &error.message)
            {
                let thread_id = thread_id.clone();
                let prompt = prompt.clone();
                let attachments = attachments.clone();
                self.state.borrow_mut().clear_stale_turn(&thread_id);
                self.render_threads();
                self.start_turn(&thread_id, prompt, attachments);
                self.render_connection();
                return;
            }
            match pending.as_ref() {
                Some(PendingKind::UnsubscribeThread(thread_id)) => {
                    let thread_id = thread_id.clone();
                    let mut state = self.state.borrow_mut();
                    state.resumed_threads.remove(&thread_id);
                    let remains_active =
                        state.active_thread_id.as_deref() == Some(thread_id.as_str());
                    drop(state);
                    if remains_active {
                        self.open_thread_live(&thread_id);
                    }
                }
                Some(PendingKind::ResumeAndSend {
                    prompt,
                    attachments,
                    ..
                })
                | Some(PendingKind::StartThread {
                    prompt,
                    attachments,
                    ..
                })
                | Some(PendingKind::SendTurn {
                    prompt,
                    attachments,
                    ..
                })
                | Some(PendingKind::SteerTurn {
                    prompt,
                    attachments,
                    ..
                }) => self.restore_composer_draft(prompt, attachments),
                Some(PendingKind::Bootstrap("plugins" | "plugins-local")) => {
                    self.state.borrow_mut().plugins_full_catalog_loading = false;
                }
                Some(PendingKind::ThreadTurnDetails { thread_id }) => {
                    self.state
                        .borrow_mut()
                        .transcript_detail_loading
                        .remove(thread_id);
                }
                _ => {}
            }
            if let Some(PendingKind::CompactThread { thread_id, .. }) = pending.as_ref() {
                self.stored
                    .borrow_mut()
                    .context_metrics
                    .entry(thread_id.clone())
                    .or_default()
                    .last_error = Some(message.clone());
                self.persist();
                self.render_context();
            }
            let mut state = self.state.borrow_mut();
            state.last_error = Some(message.clone());
            if reset_credit_request {
                state.reset_credit_in_flight = false;
            }
            drop(state);
            if reset_credit_request {
                self.finish_one_time_reset_guard(&format!("failed:rpc-{}", error.code));
            }
            if safe_continuation {
                match pending.as_ref() {
                    Some(PendingKind::OpenThread(thread_id)) => {
                        self.offer_memory_safe_continuation(thread_id, None, &[]);
                    }
                    Some(PendingKind::ResumeAndSend {
                        thread_id,
                        prompt,
                        attachments,
                    }) => {
                        self.offer_memory_safe_continuation(
                            thread_id,
                            Some(prompt.as_str()),
                            attachments,
                        );
                    }
                    _ => {}
                }
            }
            if bulk_task_mutation {
                self.finish_bulk_task_mutation();
            }
            self.toast(&message);
            return;
        }
        let result = response.result.unwrap_or_else(|| json!({}));
        if matches!(
            &pending,
            Some(
                PendingKind::OpenThread(_)
                    | PendingKind::ResumeAndSend { .. }
                    | PendingKind::StartThread { .. }
                    | PendingKind::ForkThread
            )
        ) {
            self.store_task_runtime_settings(&result);
        }
        match pending {
            Some(PendingKind::Bootstrap("threads")) => {
                let threads = value_array(&result)
                    .iter()
                    .filter_map(|value| serde_json::from_value::<ThreadSummary>(value.clone()).ok())
                    .collect::<Vec<_>>();
                let (other_account_threads, history_changed) = {
                    let mut stored = self.stored.borrow_mut();
                    let active_account_id = stored.active_account_id.clone();
                    let history_changed = stored.record_shared_tasks(
                        &active_account_id,
                        threads
                            .iter()
                            .filter(|thread| thread.parent_thread_id.is_none())
                            .cloned(),
                    );
                    let other_account_threads = stored.shared_tasks_except(&active_account_id);
                    (other_account_threads, history_changed)
                };
                let mut state = self.state.borrow_mut();
                state.set_threads(threads);
                // The selected account remains authoritative for its own
                // threads. Other accounts contribute summary-only rows that
                // route back to their owner when opened.
                state.merge_threads(other_account_threads);
                if smoke_fixtures_enabled() {
                    install_smoke_fixtures(&mut state);
                }
                drop(state);
                if history_changed {
                    self.persist();
                }
                self.restore_all_buddy_history();
                self.render_threads();
                self.open_deferred_shared_thread();
            }
            Some(PendingKind::Bootstrap("agents")) => {
                let threads = value_array(&result)
                    .iter()
                    .filter_map(|value| serde_json::from_value::<ThreadSummary>(value.clone()).ok())
                    .collect();
                self.state.borrow_mut().merge_threads(threads);
                self.render_threads();
            }
            Some(PendingKind::Bootstrap("models")) => {
                self.state.borrow_mut().models = value_array(&result).to_vec();
                self.populate_models();
            }
            Some(PendingKind::Bootstrap("skills")) => {
                self.state.borrow_mut().skills = value_array(&result).to_vec();
            }
            Some(PendingKind::Bootstrap(kind @ ("plugins" | "plugins-local"))) => {
                match crate::plugin::PluginCatalog::parse(result) {
                    Ok(catalog) => {
                        let mut state = self.state.borrow_mut();
                        state.plugins = catalog;
                        state.plugins_full_catalog_loading = false;
                        state.plugins_full_catalog_loaded = kind == "plugins";
                        drop(state);
                        if kind == "plugins-local"
                            && !self.widgets.extensions_installed_only.is_active()
                        {
                            self.load_full_plugin_catalog(false);
                        }
                    }
                    Err(error) => {
                        self.state.borrow_mut().plugins_full_catalog_loading = false;
                        self.toast(&format!("Could not read plugin catalog: {error}"));
                    }
                }
                schedule_heap_trim();
            }
            Some(PendingKind::Bootstrap("mcp")) => {
                self.state.borrow_mut().mcp_servers = value_array(&result).to_vec();
            }
            Some(PendingKind::Bootstrap("apps")) => {
                self.state.borrow_mut().apps = value_array(&result).to_vec();
            }
            Some(PendingKind::Bootstrap("account")) => {
                self.state.borrow_mut().account = Some(result);
            }
            Some(PendingKind::Bootstrap("config")) => {
                self.state.borrow_mut().config = Some(result);
            }
            Some(PendingKind::Bootstrap("requirements")) => {
                self.state.borrow_mut().config_requirements = Some(result);
            }
            Some(PendingKind::Bootstrap("permission-profiles")) => {
                self.state.borrow_mut().permission_profiles = value_array(&result).to_vec();
            }
            Some(PendingKind::Bootstrap("features")) => {
                self.state.borrow_mut().experimental_features = value_array(&result).to_vec();
            }
            Some(PendingKind::Bootstrap("collaboration")) => {
                self.state.borrow_mut().collaboration_modes = value_array(&result).to_vec();
            }
            Some(PendingKind::Bootstrap("provider-capabilities")) => {
                self.state.borrow_mut().model_provider_capabilities = Some(result);
            }
            Some(PendingKind::Bootstrap("hooks")) => {
                self.state.borrow_mut().hooks = value_array(&result).to_vec();
            }
            Some(PendingKind::OpenThread(id)) => {
                let initial_turns_loaded = self.apply_initial_turns_page(&result, &id);
                let thread = result.get("thread").cloned().unwrap_or(result);
                match serde_json::from_value::<ThreadSummary>(thread) {
                    Ok(thread) => {
                        let id = thread.id.clone();
                        self.record_active_profile_task(&thread);
                        let mut state = self.state.borrow_mut();
                        state.upsert_thread(thread);
                        state.resumed_threads.insert(id.clone());
                        let remains_active = state.active_thread_id.as_deref() == Some(id.as_str());
                        drop(state);
                        self.restore_buddy_history(&id);
                        if !remains_active {
                            self.unsubscribe_idle_threads();
                            return;
                        }
                        if !initial_turns_loaded {
                            // Older or temporarily degraded app-server
                            // versions can omit initialTurnsPage. Never treat
                            // that omission as an empty canonical transcript.
                            self.request_thread_summary(&id);
                        } else if self.should_auto_hydrate_thread_activity(&id) {
                            // initialTurnsPage is deliberately conversational
                            // summary data. Restore the newest turn's tool,
                            // reasoning, image, and progress activity just as
                            // the explicit summary fallback does.
                            self.request_turn_activity(&id, None);
                        }
                        self.apply_active_task_settings();
                        self.request(
                            "thread/goal/get",
                            json!({"threadId": id}),
                            PendingKind::GoalRead(id.clone()),
                        );
                        self.request(
                            "thread/list",
                            task_subagent_params(&id),
                            PendingKind::Bootstrap("agents"),
                        );
                        self.refresh_active_local_chat_history();
                    }
                    Err(error) => self.toast(&format!("Could not open task {id}: {error}")),
                }
            }
            Some(PendingKind::ResumeAndSend {
                thread_id,
                prompt,
                attachments,
            }) => {
                let initial_turns_loaded = self.apply_initial_turns_page(&result, &thread_id);
                let thread = result.get("thread").cloned().unwrap_or(result);
                match serde_json::from_value::<ThreadSummary>(thread) {
                    Ok(thread) if thread.id == thread_id => {
                        self.record_active_profile_task(&thread);
                        let mut state = self.state.borrow_mut();
                        state.upsert_thread(thread);
                        state.resumed_threads.insert(thread_id.clone());
                        let remains_active =
                            state.active_thread_id.as_deref() == Some(thread_id.as_str());
                        drop(state);
                        self.restore_buddy_history(&thread_id);
                        if remains_active {
                            if !initial_turns_loaded {
                                self.request_thread_summary(&thread_id);
                            } else if self.should_auto_hydrate_thread_activity(&thread_id) {
                                self.request_turn_activity(&thread_id, None);
                            }
                            self.apply_active_task_settings();
                        }
                        self.start_turn(&thread_id, prompt, attachments);
                        self.refresh_active_local_chat_history();
                    }
                    Ok(thread) => {
                        self.restore_composer_draft(&prompt, &attachments);
                        self.toast(&format!(
                            "Could not resume task {thread_id}: server returned {}",
                            thread.id
                        ));
                    }
                    Err(error) => {
                        self.restore_composer_draft(&prompt, &attachments);
                        self.toast(&format!("Could not resume task {thread_id}: {error}"));
                    }
                }
            }
            Some(PendingKind::ThreadTurns { thread_id, prepend }) => {
                if self.state.borrow().active_thread_id.as_deref() != Some(thread_id.as_str()) {
                    return;
                }
                let mut turns = value_array(&result)
                    .iter()
                    .filter_map(|value| serde_json::from_value(value.clone()).ok())
                    .collect::<Vec<_>>();
                turns.reverse();
                let next_cursor = result
                    .get("nextCursor")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                tracing::debug!(
                    %thread_id,
                    turn_count = turns.len(),
                    prepend,
                    "loaded bounded transcript page"
                );
                if prepend {
                    self.transcript_prepend_pending.set(true);
                }
                let has_turns = !turns.is_empty();
                let auto_hydrate = {
                    let mut state = self.state.borrow_mut();
                    state.set_thread_turns(&thread_id, turns, next_cursor, prepend);
                    if !prepend && has_turns {
                        state.transcript_detail_available.insert(thread_id.clone());
                    }
                    !prepend
                        && has_turns
                        && state
                            .threads
                            .get(&thread_id)
                            .is_none_or(auto_hydrate_turn_activity)
                };
                self.restore_buddy_history(&thread_id);
                if auto_hydrate {
                    self.request_turn_activity(&thread_id, None);
                }
                self.maybe_manage_context(&thread_id);
                if !prepend {
                    // The stock server's paginated projection can lag its
                    // canonical rollout after an iOS turn or interrupted tool
                    // call. Re-merge the read-only source after every newest
                    // page so a stale response cannot erase recovered chat.
                    self.local_history_fingerprints
                        .borrow_mut()
                        .remove(&thread_id);
                    self.refresh_active_local_chat_history();
                }
            }
            Some(PendingKind::ThreadTurnDetails { thread_id }) => {
                if self.state.borrow().active_thread_id.as_deref() != Some(thread_id.as_str()) {
                    self.state
                        .borrow_mut()
                        .transcript_detail_loading
                        .remove(&thread_id);
                    return;
                }
                let mut turns = value_array(&result)
                    .iter()
                    .filter_map(|value| serde_json::from_value(value.clone()).ok())
                    .collect::<Vec<_>>();
                let next_cursor = result
                    .get("nextCursor")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                if let Some(turn) = turns.pop() {
                    self.state.borrow_mut().merge_thread_turn_details(
                        &thread_id,
                        turn,
                        next_cursor,
                    );
                } else {
                    let mut state = self.state.borrow_mut();
                    state.transcript_detail_loading.remove(&thread_id);
                    state.transcript_detail_cursors.remove(&thread_id);
                    state.transcript_detail_available.remove(&thread_id);
                }
                schedule_heap_trim();
            }
            Some(PendingKind::SearchThreads { query }) => {
                if self.widgets.thread_search.text().trim() != query {
                    return;
                }
                let mut results = value_array(&result)
                    .iter()
                    .filter_map(|value| {
                        let thread = value
                            .get("thread")
                            .cloned()
                            .unwrap_or_else(|| value.clone());
                        let thread = serde_json::from_value::<ThreadSummary>(thread).ok()?;
                        let snippet = value
                            .get("snippet")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned();
                        Some((thread, snippet))
                    })
                    .collect::<Vec<(ThreadSummary, String)>>();
                let (other_account_matches, history_changed) = {
                    let mut stored = self.stored.borrow_mut();
                    let active_account_id = stored.active_account_id.clone();
                    let history_changed = stored.record_shared_tasks(
                        &active_account_id,
                        results.iter().map(|(thread, _)| thread.clone()),
                    );
                    let query_lower = query.to_lowercase();
                    let other_account_matches = stored
                        .shared_tasks_except(&active_account_id)
                        .into_iter()
                        .filter(|thread| {
                            thread.title().to_lowercase().contains(&query_lower)
                                || thread.cwd.to_lowercase().contains(&query_lower)
                        })
                        .map(|thread| (thread, "Shared local task".to_owned()))
                        .collect::<Vec<_>>();
                    (other_account_matches, history_changed)
                };
                results.extend(other_account_matches);
                if history_changed {
                    self.persist();
                }
                self.state
                    .borrow_mut()
                    .set_thread_search_results(query, results);
                self.render_threads();
            }
            Some(PendingKind::StartThread {
                prompt,
                attachments,
                mut settings,
                routing,
            }) => {
                let thread = result.get("thread").cloned().unwrap_or(result);
                match serde_json::from_value::<ThreadSummary>(thread) {
                    Ok(thread) => {
                        let id = thread.id.clone();
                        self.record_active_profile_task(&thread);
                        let mut state = self.state.borrow_mut();
                        state.upsert_thread(thread);
                        state.activate_thread(id.clone());
                        state.resumed_threads.insert(id.clone());
                        if settings.model.is_empty()
                            && let Some(model) = state
                                .task_runtime_settings
                                .get(&id)
                                .map(|actual| actual.model.clone())
                        {
                            settings.model = model;
                        }
                        state.task_runtime_settings.insert(id.clone(), settings);
                        drop(state);
                        self.stored
                            .borrow_mut()
                            .task_routing
                            .insert(id.clone(), *routing);
                        self.persist();
                        self.unsubscribe_idle_threads();
                        self.apply_active_task_settings();
                        self.start_turn(&id, prompt, attachments);
                    }
                    Err(error) => self.toast(&format!("Could not start task: {error}")),
                }
            }
            Some(PendingKind::SendTurn {
                thread_id, route, ..
            }) => {
                if let Some(turn_id) = result
                    .get("turn")
                    .and_then(|turn| turn.get("id"))
                    .and_then(Value::as_str)
                {
                    let mut state = self.state.borrow_mut();
                    if state.active_thread_id.as_deref() == Some(thread_id.as_str()) {
                        state.active_turn_id = Some(turn_id.to_owned());
                    }
                    if let Some(thread) = state.threads.get_mut(&thread_id) {
                        thread.status = json!({"type": "active", "activeFlags": []});
                    }
                    drop(state);
                    self.record_turn_route(&thread_id, turn_id, route);
                    self.render_threads();
                }
            }
            Some(PendingKind::SteerTurn { thread_id, .. }) => {
                if let Some(turn_id) = result
                    .get("turn")
                    .and_then(|turn| turn.get("id"))
                    .and_then(Value::as_str)
                {
                    let mut state = self.state.borrow_mut();
                    if state.active_thread_id.as_deref() == Some(thread_id.as_str()) {
                        state.active_turn_id = Some(turn_id.to_owned());
                    }
                    if let Some(thread) = state.threads.get_mut(&thread_id) {
                        thread.status = json!({"type": "active", "activeFlags": []});
                    }
                    drop(state);
                    self.render_threads();
                }
            }
            Some(PendingKind::Login) => {
                if let Some(url) = result
                    .get("authUrl")
                    .or_else(|| result.get("verificationUrl"))
                    .and_then(Value::as_str)
                {
                    if let Err(error) =
                        gio::AppInfo::launch_default_for_uri(url, None::<&gio::AppLaunchContext>)
                    {
                        self.toast(&format!("Could not open your browser: {error}"));
                    } else {
                        self.toast("Continue sign-in in your web browser");
                    }
                }
            }
            Some(PendingKind::Logout) => {
                self.state.borrow_mut().account = None;
                self.request(
                    "account/read",
                    json!({"refreshToken": false}),
                    PendingKind::Bootstrap("account"),
                );
            }
            Some(PendingKind::ForkThread) => {
                let thread = result.get("thread").cloned().unwrap_or(result);
                if let Ok(thread) = serde_json::from_value::<ThreadSummary>(thread) {
                    let id = thread.id.clone();
                    self.record_active_profile_task(&thread);
                    let mut state = self.state.borrow_mut();
                    state.upsert_thread(thread);
                    state.activate_thread(id.clone());
                    state.resumed_threads.insert(id);
                    drop(state);
                    self.apply_active_task_settings();
                }
            }
            Some(PendingKind::Goal(thread_id)) => {
                if let Some(goal) = result.get("goal").filter(|value| !value.is_null()) {
                    self.state
                        .borrow_mut()
                        .thread_goals
                        .insert(thread_id, goal.clone());
                    self.toast("Task goal updated");
                } else {
                    self.state.borrow_mut().thread_goals.remove(&thread_id);
                    self.stored
                        .borrow_mut()
                        .goal_stop_reasons
                        .remove(&thread_id);
                    self.persist();
                    self.toast("Task goal cleared");
                }
                self.render_goal_selector();
            }
            Some(PendingKind::GoalStatus { thread_id, status }) => {
                if let Some(goal) = result.get("goal").filter(|value| !value.is_null()) {
                    self.state
                        .borrow_mut()
                        .thread_goals
                        .insert(thread_id.clone(), goal.clone());
                }
                let reason = match status.as_str() {
                    "paused" => Some("Stopped manually."),
                    "active" | "complete" => None,
                    _ => goal_status_reason(&status),
                };
                let mut stored = self.stored.borrow_mut();
                match reason {
                    Some(reason) => {
                        stored
                            .goal_stop_reasons
                            .insert(thread_id.clone(), reason.to_owned());
                    }
                    None => {
                        stored.goal_stop_reasons.remove(&thread_id);
                    }
                }
                drop(stored);
                self.persist();
                self.toast(match status.as_str() {
                    "paused" => "Goal stopped",
                    "active" => "Goal resumed",
                    "complete" => "Goal marked complete",
                    _ => "Goal status updated",
                });
                self.render_goal_selector();
            }
            Some(PendingKind::GoalRead(thread_id)) => {
                if let Some(goal) = result.get("goal").filter(|value| !value.is_null()) {
                    self.state
                        .borrow_mut()
                        .thread_goals
                        .insert(thread_id, goal.clone());
                } else {
                    self.state.borrow_mut().thread_goals.remove(&thread_id);
                }
                self.render_goal_selector();
            }
            Some(PendingKind::CompactThread {
                thread_id: _,
                automatic,
                checkpoint_id,
            }) => {
                self.render_context();
                self.toast(if automatic {
                    "Codex accepted the compaction request; waiting for canonical completion"
                } else if checkpoint_id.is_empty() {
                    "Compaction request accepted; waiting for Codex completion"
                } else {
                    "Checkpoint saved; waiting for Codex compaction completion"
                });
            }
            Some(PendingKind::RollbackThread(thread_id)) => {
                self.toast("Last conversation turn rolled back");
                self.open_thread_live(&thread_id);
            }
            Some(
                PendingKind::ArchiveThread
                | PendingKind::UnarchiveThread
                | PendingKind::DeleteThread,
            ) => {
                self.refresh_threads();
            }
            Some(
                PendingKind::BulkArchiveThread
                | PendingKind::BulkUnarchiveThread
                | PendingKind::BulkDeleteThread,
            ) => {
                self.finish_bulk_task_mutation();
            }
            Some(PendingKind::AccountRateLimits) => {
                self.state.borrow_mut().account_rate_limits = Some(result);
                self.maybe_consume_one_time_reset();
            }
            Some(PendingKind::AccountUsage) => {
                self.state.borrow_mut().account_usage = Some(result);
            }
            Some(PendingKind::ConsumeRateLimitResetCredit) => {
                self.state.borrow_mut().reset_credit_in_flight = false;
                let outcome = result
                    .get("outcome")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                self.finish_one_time_reset_guard(outcome);
                let message = match outcome {
                    "reset" => "A reset credit was used and eligible Codex limits were reset.",
                    "nothingToReset" => {
                        "No current rate-limit window was eligible; the credit was not used."
                    }
                    "noCredit" => "No reset credit is currently available.",
                    "alreadyRedeemed" => {
                        "This reset request already completed; no second credit was used."
                    }
                    _ => "The reset request completed with an unknown backend outcome.",
                };
                self.toast(message);
                self.refresh_account_usage();
            }
            Some(PendingKind::VoiceStart { thread_id, audio }) => {
                self.request(
                    "thread/realtime/appendAudio",
                    json!({"threadId": thread_id, "audio": audio}),
                    PendingKind::VoiceAppend(thread_id),
                );
            }
            Some(PendingKind::VoiceAppend(thread_id)) => {
                self.request(
                    "thread/realtime/stop",
                    json!({"threadId": thread_id}),
                    PendingKind::Generic,
                );
                self.toast("Dictation sent to Codex");
            }
            Some(PendingKind::Hooks) => {
                self.state.borrow_mut().hooks = value_array(&result).to_vec();
            }
            Some(PendingKind::PluginInstall(plugin_id)) => {
                let apps = result
                    .get("appsNeedingAuth")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let mut opened_auth = false;
                for app in apps {
                    if let Some(url) = app.get("installUrl").and_then(Value::as_str) {
                        opened_auth |= gio::AppInfo::launch_default_for_uri(
                            url,
                            None::<&gio::AppLaunchContext>,
                        )
                        .is_ok();
                    }
                }
                self.toast(if opened_auth {
                    "Plugin installed. Finish connecting its apps in your browser."
                } else {
                    "Plugin installed"
                });
                tracing::info!(%plugin_id, "plugin installed");
                self.refresh_extension_runtime();
            }
            Some(PendingKind::PluginUninstall(plugin_id)) => {
                self.toast("Plugin uninstalled");
                tracing::info!(%plugin_id, "plugin uninstalled");
                self.refresh_extension_runtime();
            }
            Some(PendingKind::PluginEnable { plugin_id, enabled }) => {
                self.toast(if enabled {
                    "Plugin enabled for new tasks"
                } else {
                    "Plugin disabled for new tasks"
                });
                tracing::info!(%plugin_id, enabled, "plugin availability changed");
                self.refresh_extension_runtime();
            }
            Some(PendingKind::AppEnable { app_id, enabled }) => {
                if let Some(app) = self
                    .state
                    .borrow_mut()
                    .apps
                    .iter_mut()
                    .find(|app| app.get("id").and_then(Value::as_str) == Some(app_id.as_str()))
                    && let Some(app) = app.as_object_mut()
                {
                    app.insert("isEnabled".into(), Value::Bool(enabled));
                }
                self.toast(if enabled {
                    "App enabled for new tasks"
                } else {
                    "App disabled for new tasks"
                });
                tracing::info!(%app_id, enabled, "app availability changed");
            }
            Some(PendingKind::PluginRead(plugin_id)) => {
                self.show_plugin_details(&plugin_id, &result);
            }
            Some(PendingKind::SkillEnable {
                skill_name,
                enabled,
            }) => {
                self.toast(if enabled {
                    "Skill enabled"
                } else {
                    "Skill disabled"
                });
                tracing::info!(%skill_name, enabled, "skill availability changed");
                self.refresh_extension_runtime();
            }
            Some(PendingKind::McpOauth(server)) => {
                if let Some(url) = result.get("authorizationUrl").and_then(Value::as_str) {
                    match gio::AppInfo::launch_default_for_uri(url, None::<&gio::AppLaunchContext>)
                    {
                        Ok(()) => self.toast("Finish extension sign-in in your browser"),
                        Err(error) => self.toast(&format!("Could not open sign-in: {error}")),
                    }
                }
                tracing::info!(%server, "started MCP OAuth login");
            }
            Some(PendingKind::McpRefresh) => self.refresh_extensions(),
            Some(PendingKind::MacroConfig { enabled }) => {
                self.toast(if enabled {
                    "Native macro execution enabled for Desktop and iOS tasks"
                } else {
                    "Native macro execution disabled"
                });
                self.request(MCP_RELOAD_METHOD, json!({}), PendingKind::McpRefresh);
            }
            Some(PendingKind::ComputerPolicy) => {
                self.toast("Computer Use policy saved");
                self.refresh_extension_runtime();
            }
            Some(PendingKind::Marketplace) => {
                self.toast("Marketplace configuration updated");
                self.refresh_extensions();
            }
            Some(PendingKind::UpdateThreadSettings(thread_id)) => {
                tracing::debug!(%thread_id, "task runtime settings updated");
            }
            Some(PendingKind::UnsubscribeThread(thread_id)) => {
                let mut state = self.state.borrow_mut();
                state.resumed_threads.remove(&thread_id);
                let remains_active = state.active_thread_id.as_deref() == Some(thread_id.as_str());
                drop(state);
                schedule_heap_trim();
                if remains_active {
                    self.open_thread_live(&thread_id);
                }
            }
            Some(PendingKind::RenameThread | PendingKind::Generic) | None => {}
            Some(PendingKind::Bootstrap(_)) => {}
        }
    }

    fn handle_remote_result(&self, action: RemoteAction, value: Value) {
        match action {
            RemoteAction::Enable => {
                self.remote_expected_enabled.set(true);
                let status = value
                    .get("status")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                if status.is_some() {
                    self.state.borrow_mut().apply_remote_status(&value);
                } else {
                    let mut state = self.state.borrow_mut();
                    state.remote = RemoteState::Connecting;
                    state.remote_details = Some(value);
                }
                self.toast(match status.as_deref() {
                    Some("connected") => "Remote host connected",
                    Some("connecting") => "Remote host enabled and connecting",
                    _ => "Remote host started; checking its connection",
                });
                self.maybe_handoff_to_managed_transport();
            }
            RemoteAction::Disable => {
                self.remote_expected_enabled.set(false);
                self.remote_handoff_pending.set(false);
                let mut state = self.state.borrow_mut();
                state.remote = RemoteState::Disabled;
                state.remote_pairing = None;
                state.remote_clients.clear();
            }
            RemoteAction::Pair => {
                if let Ok(info) = serde_json::from_value::<PairingInfo>(value.clone()) {
                    self.state.borrow_mut().remote_pairing = Some(info);
                } else {
                    self.state.borrow_mut().remote_details = Some(value);
                }
            }
            RemoteAction::Refresh => {
                let remote_active = matches!(
                    value.get("status").and_then(Value::as_str),
                    Some("connected" | "connecting")
                );
                if remote_active {
                    self.remote_expected_enabled.set(true);
                }
                let clients = value
                    .get("clients")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let mut state = self.state.borrow_mut();
                state.apply_remote_status(&value);
                state.remote_clients = clients;
                drop(state);
                if remote_active && !self.hub.uses_managed_transport() && !smoke_fixtures_enabled()
                {
                    self.maybe_handoff_to_managed_transport();
                }
            }
            RemoteAction::Revoke { .. } => {
                self.toast("Paired device revoked");
                self.refresh_remote();
            }
        }
    }

    fn refresh_threads(&self) {
        self.state.borrow_mut().clear_thread_search();
        let stored = self.stored.borrow();
        let archived = stored.show_archived_threads;
        let sort_key = stored.thread_sort.clone();
        drop(stored);
        self.request(
            "thread/list",
            thread_refresh_params(archived, &sort_key),
            PendingKind::Bootstrap("threads"),
        );
        self.request(
            "thread/list",
            subagent_thread_refresh_params(),
            PendingKind::Bootstrap("agents"),
        );
    }

    fn search_threads(&self) {
        let query = self.widgets.thread_search.text().trim().to_owned();
        if query.is_empty() {
            self.state.borrow_mut().clear_thread_search();
            self.refresh_threads();
            return;
        }
        let (archived, sort_key) = {
            let stored = self.stored.borrow();
            (stored.show_archived_threads, stored.thread_sort.clone())
        };
        self.request(
            "thread/search",
            thread_search_params(&query, archived, &sort_key),
            PendingKind::SearchThreads { query },
        );
    }

    fn refresh_account_usage(&self) {
        self.refresh_account_rate_limits();
        let usage_pending = self
            .state
            .borrow()
            .pending
            .values()
            .any(|pending| matches!(pending, PendingKind::AccountUsage));
        if !usage_pending {
            self.request("account/usage/read", json!({}), PendingKind::AccountUsage);
        }
    }

    fn refresh_account_rate_limits(&self) {
        let limits_pending = self
            .state
            .borrow()
            .pending
            .values()
            .any(|pending| matches!(pending, PendingKind::AccountRateLimits));
        if !limits_pending {
            self.request(
                "account/rateLimits/read",
                json!({}),
                PendingKind::AccountRateLimits,
            );
        }
    }

    fn maybe_consume_one_time_reset(&self) {
        if self.state.borrow().reset_credit_in_flight {
            return;
        }
        let decision = {
            let state = self.state.borrow();
            let Some(limits) = state.account_rate_limits.as_ref() else {
                return;
            };
            let stored = self.stored.borrow();
            one_time_reset_decision(&stored.one_time_reset_guard, limits)
        };
        match decision {
            OneTimeResetDecision::Wait => {}
            OneTimeResetDecision::Consume { credit_id } => {
                let now = chrono::Utc::now().timestamp();
                let idempotency_key = {
                    let mut stored = self.stored.borrow_mut();
                    let guard = &mut stored.one_time_reset_guard;
                    let key = guard
                        .idempotency_key
                        .get_or_insert_with(|| uuid::Uuid::new_v4().to_string())
                        .clone();
                    guard.attempted_at = Some(now);
                    guard.outcome = Some("pending".into());
                    key
                };
                // Persist the stable key before the request. If the transport
                // disappears before a response, reconnect retries the same
                // operation instead of risking a second credit.
                self.persist();
                let mut params = Map::new();
                params.insert("idempotencyKey".into(), Value::String(idempotency_key));
                if let Some(credit_id) = credit_id {
                    params.insert("creditId".into(), Value::String(credit_id));
                }
                let id = self.hub.request(
                    "account/rateLimitResetCredit/consume",
                    Value::Object(params),
                );
                let mut state = self.state.borrow_mut();
                state.reset_credit_in_flight = true;
                state
                    .pending
                    .insert(id, PendingKind::ConsumeRateLimitResetCredit);
                drop(state);
                self.toast("Weekly allowance reached 2%; using the authorized one-time reset");
            }
        }
    }

    fn finish_one_time_reset_guard(&self, outcome: &str) {
        let changed = {
            let mut stored = self.stored.borrow_mut();
            let guard = &mut stored.one_time_reset_guard;
            if !guard.armed || guard.outcome.as_deref() != Some("pending") {
                false
            } else {
                guard.armed = false;
                guard.completed_at = Some(chrono::Utc::now().timestamp());
                guard.outcome = Some(outcome.to_owned());
                true
            }
        };
        if changed {
            self.persist();
        }
    }

    fn confirm_use_reset_credit(&self) {
        let (available, credit_id, credit_title, credit_description, expires_at, weekly) = {
            let state = self.state.borrow();
            let Some(limits) = state.account_rate_limits.as_ref() else {
                self.toast("Reset-credit availability has not loaded yet");
                return;
            };
            let summary = limits.get("rateLimitResetCredits");
            let available = summary
                .and_then(|value| value.get("availableCount"))
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let credit = summary
                .and_then(|value| value.get("credits"))
                .and_then(Value::as_array)
                .and_then(|credits| {
                    credits.iter().find(|credit| {
                        credit.get("status").and_then(Value::as_str) == Some("available")
                    })
                });
            (
                available,
                credit
                    .and_then(|value| value.get("id"))
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                credit
                    .and_then(|value| value.get("title"))
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                credit
                    .and_then(|value| value.get("description"))
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                credit
                    .and_then(|value| value.get("expiresAt"))
                    .and_then(Value::as_i64),
                weekly_limit_details(limits),
            )
        };
        if available <= 0 {
            self.toast("No reset credits are currently available");
            return;
        }
        let mut details = vec![format!(
            "You have {available} reset credit{} available.",
            if available == 1 { "" } else { "s" }
        )];
        if let Some((remaining, resets_at, _)) = weekly {
            let reset_text = resets_at
                .and_then(format_timestamp)
                .map(|value| format!("; scheduled reset {value}"))
                .unwrap_or_default();
            details.push(format!("Weekly allowance: {remaining}% left{reset_text}."));
        }
        if let Some(title) = credit_title {
            details.push(title);
        }
        if let Some(description) = credit_description {
            details.push(description);
        }
        if let Some(expires_at) = expires_at.and_then(format_timestamp) {
            details.push(format!("This credit expires {expires_at}."));
        }
        details.push(
            "Using one is irreversible and resets every currently eligible Codex rate-limit window. If nothing is eligible, the backend will not consume it."
                .into(),
        );
        let dialog = adw::AlertDialog::new(
            Some("Use one Codex reset credit?"),
            Some(&details.join("\n\n")),
        );
        dialog.add_responses(&[("cancel", "Cancel"), ("use", "Use one reset")]);
        dialog.set_default_response(Some("cancel"));
        dialog.set_close_response("cancel");
        dialog.set_response_appearance("use", adw::ResponseAppearance::Destructive);
        let hub = self.hub.clone();
        let state = self.state.clone();
        let button = self.widgets.reset_credit_button.clone();
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            move |response| {
                if response.as_str() != "use" {
                    return;
                }
                button.set_sensitive(false);
                button.set_label("Resetting…");
                let mut params = Map::new();
                params.insert(
                    "idempotencyKey".into(),
                    Value::String(uuid::Uuid::new_v4().to_string()),
                );
                if let Some(credit_id) = &credit_id {
                    params.insert("creditId".into(), Value::String(credit_id.clone()));
                }
                let id = hub.request(
                    "account/rateLimitResetCredit/consume",
                    Value::Object(params),
                );
                let mut state = state.borrow_mut();
                state.reset_credit_in_flight = true;
                state
                    .pending
                    .insert(id, PendingKind::ConsumeRateLimitResetCredit);
            },
        );
    }

    fn refresh_extensions(&self) {
        if self.state.borrow().connection != ConnectionState::Ready {
            return;
        }
        self.extensions_bootstrapped.set(true);
        let cwd = self.current_cwd_string();
        if self.widgets.extensions_installed_only.is_active() {
            self.load_local_plugin_catalog();
        } else {
            self.load_full_plugin_catalog(true);
        }
        self.request(
            "skills/list",
            json!({"cwds": [cwd], "forceReload": true}),
            PendingKind::Bootstrap("skills"),
        );
        self.request(
            "mcpServerStatus/list",
            json!({}),
            PendingKind::Bootstrap("mcp"),
        );
        self.request(
            "app/list",
            json!({"limit": 100, "forceRefetch": true}),
            PendingKind::Bootstrap("apps"),
        );
        self.request(
            "config/read",
            json!({"includeLayers": false}),
            PendingKind::Bootstrap("config"),
        );
        self.request(
            "hooks/list",
            json!({"cwds": [self.current_cwd_string()]}),
            PendingKind::Hooks,
        );
    }

    fn ensure_extensions_loaded(&self) {
        if self.state.borrow().connection != ConnectionState::Ready
            || self.extensions_bootstrapped.replace(true)
        {
            return;
        }
        let cwd = self.current_cwd_string();
        self.load_local_plugin_catalog();
        for (name, method, params) in [
            ("skills", "skills/list", json!({"cwds": [cwd]})),
            ("mcp", "mcpServerStatus/list", json!({})),
            ("apps", "app/list", json!({"limit": 100})),
            (
                "hooks",
                "hooks/list",
                json!({"cwds": [self.current_cwd_string()]}),
            ),
        ] {
            self.request(method, params, PendingKind::Bootstrap(name));
        }
    }

    fn ensure_qwen_loaded(&self) {
        if self.state.borrow().connection != ConnectionState::Ready
            || self.qwen_bootstrapped.replace(true)
        {
            return;
        }
        self.refresh_qwen();
    }

    fn load_local_plugin_catalog(&self) {
        self.request(
            "plugin/list",
            local_plugin_list_params(&self.current_cwd_string()),
            PendingKind::Bootstrap("plugins-local"),
        );
    }

    fn ensure_diagnostics_loaded(&self) {
        if !self.stored.borrow().preferences.resource_monitor
            || self.diagnostics_bootstrapped.replace(true)
        {
            return;
        }
        self.hub.host(HostAction::Diagnostics);
    }

    fn load_full_plugin_catalog(&self, force: bool) {
        let mut state = self.state.borrow_mut();
        if state.connection != ConnectionState::Ready
            || state.plugins_full_catalog_loading
            || (!force && state.plugins_full_catalog_loaded)
        {
            return;
        }
        state.plugins_full_catalog_loading = true;
        drop(state);
        self.request(
            "plugin/list",
            json!({"cwds": [self.current_cwd_string()]}),
            PendingKind::Bootstrap("plugins"),
        );
    }

    fn refresh_extension_runtime(&self) {
        self.request(MCP_RELOAD_METHOD, json!({}), PendingKind::McpRefresh);
    }

    fn refresh_memoria_jobs(&self) {
        if self.memoria_jobs_refresh_pending.replace(true) {
            return;
        }
        self.hub.host(HostAction::MemoriaJobs);
    }

    fn render_memoria_jobs(&self, value: &Value) {
        let count = value.get("count").and_then(Value::as_u64).unwrap_or(0);
        let pending = value.get("pending").and_then(Value::as_u64).unwrap_or(0);
        let active = value.get("active").and_then(Value::as_u64).unwrap_or(0);
        self.widgets
            .memoria_jobs_label
            .set_label(&format!("Memoria jobs: {count}"));
        if count > 0 {
            self.widgets
                .memoria_jobs_label
                .add_css_class("memoria-jobs-active");
        } else {
            self.widgets
                .memoria_jobs_label
                .remove_css_class("memoria-jobs-active");
        }
        self.widgets
            .memoria_jobs_label
            .set_tooltip_text(Some(&format!(
                "Live Memoria work queues · {pending} pending · {active} active"
            )));
    }

    fn refresh_qwen(&self) {
        let mut state = self.state.borrow_mut();
        if state.qwen_busy {
            return;
        }
        state.qwen_busy = true;
        drop(state);
        self.widgets.qwen_refresh.set_sensitive(false);
        self.widgets.qwen_status.set_label("Checking Qwen Buddy…");
        self.request(
            "plugin/list",
            local_plugin_list_params(&self.current_cwd_string()),
            PendingKind::Bootstrap("plugins-local"),
        );
        self.request(
            "mcpServerStatus/list",
            json!({}),
            PendingKind::Bootstrap("mcp"),
        );
        self.hub.host(HostAction::QwenRefresh);
    }

    fn qwen_plugin_ready(&self) -> bool {
        self.state.borrow().mcp_servers.iter().any(|server| {
            server.get("name").and_then(Value::as_str) == Some("local-qwen-delegate")
                && server
                    .get("tools")
                    .and_then(Value::as_object)
                    .is_some_and(|tools| {
                        tools.contains_key("local_qwen_agent")
                            && tools.contains_key("local_qwen_agent_status")
                            && tools.contains_key("gemini_buddy_delegate")
                    })
        })
    }

    fn qwen_plugin_ready_or_loading(&self) -> bool {
        self.qwen_plugin_ready()
            || self
                .state
                .borrow()
                .pending
                .values()
                .any(|pending| matches!(pending, PendingKind::Bootstrap("mcp")))
    }

    fn qwen_gpu_blocked(&self) -> bool {
        if smoke_fixtures_enabled() && env::var_os("CODEX_NATIVE_FAKE_QWEN_READY").is_some() {
            return false;
        }
        self.state
            .borrow()
            .qwen_report
            .as_ref()
            .is_some_and(|report| {
                report
                    .pointer("/gpu/routingBlocked")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                    || (report
                        .pointer("/runtime/modelSleeping")
                        .and_then(Value::as_bool)
                        == Some(true)
                        && report
                            .pointer("/runtime/wakePlacementReady")
                            .and_then(Value::as_bool)
                            == Some(false))
            })
    }

    fn qwen_route_for_turn(
        &self,
        mode: &str,
        prompt: &str,
        attachment_count: usize,
    ) -> Option<crate::qwen::AutomaticQwenRoute> {
        crate::qwen::automatic_route(
            mode,
            prompt,
            attachment_count,
            &self.stored.borrow().preferences.qwen_buddy,
            self.qwen_plugin_ready_or_loading(),
            self.qwen_gpu_blocked(),
        )
    }

    fn qwen_routing_context(&self, route: crate::qwen::AutomaticQwenRoute) -> Option<String> {
        crate::qwen::routing_context(&self.stored.borrow().preferences.qwen_buddy, route)
    }

    fn decorate_route_with_qwen(
        &self,
        mut route: RouteDecision,
        mode: &str,
        prompt: &str,
        attachment_count: usize,
    ) -> RouteDecision {
        route.local_route = self
            .qwen_route_for_turn(mode, prompt, attachment_count)
            .map(|route| route.label().to_owned());
        route
    }

    fn add_marketplace_dialog(&self) {
        let source = gtk::Entry::builder()
            .placeholder_text("Git URL or local marketplace path")
            .hexpand(true)
            .build();
        let reference = gtk::Entry::builder()
            .placeholder_text("Optional branch, tag, or commit")
            .hexpand(true)
            .build();
        let sparse = gtk::Entry::builder()
            .placeholder_text("Optional comma-separated sparse paths")
            .hexpand(true)
            .build();
        let form = gtk::Grid::builder()
            .column_spacing(12)
            .row_spacing(9)
            .build();
        attach_setting(&form, 0, "Source", &source);
        attach_setting(&form, 1, "Reference", &reference);
        attach_setting(&form, 2, "Sparse paths", &sparse);
        let dialog = adw::AlertDialog::new(
            Some("Add plugin marketplace"),
            Some(
                "Codex validates and checks out the catalog. Review its plugins before installing.",
            ),
        );
        dialog.set_extra_child(Some(&form));
        dialog.add_responses(&[("cancel", "Cancel"), ("add", "Add")]);
        dialog.set_default_response(Some("add"));
        dialog.set_close_response("cancel");
        let hub = self.hub.clone();
        let state = self.state.clone();
        let overlay = self.widgets.toast_overlay.clone();
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            move |response| {
                if response.as_str() != "add" {
                    return;
                }
                let source = source.text().trim().to_owned();
                if source.is_empty() {
                    overlay.add_toast(adw::Toast::new("Enter a marketplace source"));
                    return;
                }
                let reference = reference.text().trim().to_owned();
                let sparse_paths = sparse
                    .text()
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                let id = hub.request(
                    "marketplace/add",
                    json!({
                        "source": source,
                        "refName": (!reference.is_empty()).then_some(reference),
                        "sparsePaths": (!sparse_paths.is_empty()).then_some(sparse_paths)
                    }),
                );
                state
                    .borrow_mut()
                    .pending
                    .insert(id, PendingKind::Marketplace);
            },
        );
    }

    fn write_config(&self, key_path: String, value: Value, kind: PendingKind) {
        self.request(
            "config/batchWrite",
            json!({
                "edits": [{
                    "keyPath": key_path,
                    "value": value,
                    "mergeStrategy": "upsert"
                }],
                "reloadUserConfig": true
            }),
            kind,
        );
    }

    fn configure_macro_executor(&self, preferences: &MacroExecutorPreferences) {
        let executable = match macro_helper_binary() {
            Ok(path) => path,
            Err(error) => {
                self.toast(&format!(
                    "Macro executor was not registered because its helper could not be resolved: {error}"
                ));
                return;
            }
        };
        let mut args = vec![
            "--max-parallel".to_owned(),
            preferences.max_parallel.to_string(),
            "--memory-mib".to_owned(),
            preferences.memory_mib.to_string(),
            "--timeout-seconds".to_owned(),
            preferences.timeout_seconds.to_string(),
            "--tasks-max".to_owned(),
            "96".to_owned(),
        ];
        let project_roots = self
            .stored
            .borrow()
            .projects
            .iter()
            .map(|project| project.path.clone())
            .filter(|path| path.is_absolute() && path.is_dir())
            .take(32)
            .collect::<Vec<_>>();
        for root in &project_roots {
            args.push("--allowed-root".into());
            args.push(root.to_string_lossy().into_owned());
        }
        if preferences.enabled && project_roots.is_empty() {
            self.toast("Macro execution is enabled, but no saved project root is available");
        }
        self.request(
            "config/batchWrite",
            json!({
                "edits": [{
                    "keyPath": standalone_mcp_key("codex-native-macro"),
                    "value": {
                        "command": executable.to_string_lossy(),
                        "args": args,
                        "enabled": preferences.enabled,
                        "startup_timeout_sec": 10,
                        "tool_timeout_sec": preferences.timeout_seconds.saturating_add(30)
                    },
                    "mergeStrategy": "upsert"
                }],
                "reloadUserConfig": true
            }),
            PendingKind::MacroConfig {
                enabled: preferences.enabled,
            },
        );
    }

    fn computer_plugin(&self) -> Option<PluginEntry> {
        self.state
            .borrow()
            .plugins
            .find_entry(PluginEntry::is_computer_use)
    }

    fn computer_binary(&self) -> Option<PathBuf> {
        self.computer_plugin()
            .and_then(|entry| entry.local_root())
            .map(|root| root.join("bin/codex-computer-use-linux"))
            .filter(|path| path.is_file())
    }

    fn set_computer_plugin_enabled(&self, enabled: bool) {
        if self.updating_computer_controls.get() {
            return;
        }
        let Some(plugin) = self.computer_plugin() else {
            self.toast("Computer Use is not available in a configured marketplace");
            return;
        };
        if !plugin.summary.installed {
            if enabled {
                self.request(
                    "plugin/install",
                    plugin.locator_params(),
                    PendingKind::PluginInstall(plugin.summary.id),
                );
            }
            return;
        }
        if plugin.summary.enabled == enabled {
            return;
        }
        self.write_config(
            plugin_enabled_key(&plugin.summary.id),
            Value::Bool(enabled),
            PendingKind::PluginEnable {
                plugin_id: plugin.summary.id,
                enabled,
            },
        );
    }

    fn set_computer_server_enabled(&self, enabled: bool) {
        if self.updating_computer_controls.get() {
            return;
        }
        let Some(plugin) = self
            .computer_plugin()
            .filter(|entry| entry.summary.installed)
        else {
            self.toast("Install Computer Use before configuring its MCP server");
            return;
        };
        self.write_config(
            plugin_mcp_key(&plugin.summary.id, "computer-use", "enabled"),
            Value::Bool(enabled),
            PendingKind::ComputerPolicy,
        );
    }

    fn set_computer_approval_mode(&self, mode: &str) {
        if self.updating_computer_controls.get() {
            return;
        }
        let Some(plugin) = self
            .computer_plugin()
            .filter(|entry| entry.summary.installed)
        else {
            return;
        };
        if mode == "approve" {
            self.confirm_unrestricted_computer_policy(plugin, mode);
            return;
        }
        self.write_computer_approval_mode(&plugin, mode);
    }

    fn write_computer_approval_mode(&self, plugin: &PluginEntry, mode: &str) {
        self.write_config(
            plugin_mcp_key(
                &plugin.summary.id,
                "computer-use",
                "default_tools_approval_mode",
            ),
            Value::String(mode.to_owned()),
            PendingKind::ComputerPolicy,
        );
    }

    fn confirm_unrestricted_computer_policy(&self, plugin: PluginEntry, mode: &str) {
        let dialog = adw::AlertDialog::new(
            Some("Always allow desktop actions?"),
            Some(
                "Computer Use could click, type, drag, and activate windows without another prompt. Codex safety rules still apply, but this removes the local MCP confirmation gate.",
            ),
        );
        dialog.add_responses(&[("cancel", "Keep asking"), ("allow", "Always allow")]);
        dialog.set_response_appearance("allow", adw::ResponseAppearance::Destructive);
        dialog.set_close_response("cancel");
        let hub = self.hub.clone();
        let state = self.state.clone();
        let key = plugin_mcp_key(
            &plugin.summary.id,
            "computer-use",
            "default_tools_approval_mode",
        );
        let plugin_id = plugin.summary.id;
        let mode = mode.to_owned();
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            move |response| {
                if response.as_str() != "allow" {
                    return;
                }
                let id = hub.request(
                    "config/batchWrite",
                    json!({
                        "edits": [{"keyPath": key, "value": mode, "mergeStrategy": "upsert"}],
                        "reloadUserConfig": true
                    }),
                );
                tracing::info!(%plugin_id, "setting unrestricted Computer Use approval mode");
                state
                    .borrow_mut()
                    .pending
                    .insert(id, PendingKind::ComputerPolicy);
            },
        );
    }

    fn run_computer_doctor(&self) {
        let Some(binary) = self.computer_binary() else {
            self.toast("Install and enable the Linux Computer Use plugin first");
            return;
        };
        self.state.borrow_mut().computer_busy = true;
        self.hub.computer(ComputerAction::Doctor(binary));
    }

    fn confirm_computer_setup(&self) {
        let Some(binary) = self.computer_binary() else {
            self.toast("Install and enable the Linux Computer Use plugin first");
            return;
        };
        let dialog = adw::AlertDialog::new(
            Some("Configure Linux desktop access?"),
            Some(
                "This runs the plugin's native setup. It may enable accessibility settings and show desktop portal permission dialogs. You can revoke portal access later in System Settings.",
            ),
        );
        dialog.add_responses(&[("cancel", "Cancel"), ("setup", "Run setup")]);
        dialog.set_default_response(Some("setup"));
        dialog.set_close_response("cancel");
        let hub = self.hub.clone();
        let state = self.state.clone();
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            move |response| {
                if response.as_str() == "setup" {
                    state.borrow_mut().computer_busy = true;
                    hub.computer(ComputerAction::Setup(binary));
                }
            },
        );
    }

    fn new_computer_task(&self) {
        self.new_task();
        self.widgets
            .approval_combo
            .set_active_id(Some("on-request"));
        self.widgets.composer.buffer().set_text(
            "Use the Computer Use plugin to inspect and control the relevant desktop app. Begin with get_app_state, avoid terminals and Codex itself, and ask before any consequential action.",
        );
        self.widgets.composer.grab_focus();
    }

    fn refresh_remote(&self) {
        self.hub.remote(RemoteAction::Refresh);
    }

    fn observe_remote_health(&self, reason: Option<String>) {
        let Some(reason) = reason else {
            self.remote_unhealthy_samples.set(0);
            self.remote_recovery_pending.set(false);
            self.remote_recovery_reason.borrow_mut().take();
            return;
        };
        let samples = self.remote_unhealthy_samples.get().saturating_add(1);
        self.remote_unhealthy_samples.set(samples);
        *self.remote_recovery_reason.borrow_mut() = Some(reason);
        if samples >= REMOTE_RECOVERY_SAMPLES {
            self.remote_recovery_pending.set(true);
            self.maybe_recover_remote_host();
        }
    }

    fn maybe_recover_remote_host(&self) {
        if !self.remote_recovery_pending.get() || self.remote_recovery_in_progress.get() {
            return;
        }
        if self
            .last_remote_recovery
            .get()
            .is_some_and(|last| last.elapsed() < REMOTE_RECOVERY_COOLDOWN)
        {
            return;
        }
        if remote_transition_is_busy(&self.state.borrow()) {
            return;
        }
        self.remote_recovery_in_progress.set(true);
        self.remote_recovery_pending.set(false);
        self.widgets.remote_restart.set_sensitive(false);
        let reason = self
            .remote_recovery_reason
            .borrow()
            .clone()
            .unwrap_or_else(|| "the remote host became unhealthy".into());
        self.toast(&format!("Recovering Remote automatically because {reason}"));
        self.hub.host(HostAction::RemoteRestart {
            configured_binary: self.stored.borrow().preferences.codex_binary.clone(),
        });
    }

    fn maybe_handoff_to_managed_transport(&self) {
        if self.hub.uses_managed_transport() {
            self.remote_handoff_pending.set(false);
            return;
        }
        if remote_transition_is_busy(&self.state.borrow()) {
            self.remote_handoff_pending.set(true);
            return;
        }
        self.remote_handoff_pending.set(false);
        // Ready handling resumes the open task and reloads its bounded
        // transcript on the one daemon shared with paired iOS clients.
        self.hub.reconnect();
    }

    fn enable_remote_host(&self) {
        self.remote_expected_enabled.set(true);
        self.state.borrow_mut().remote = RemoteState::Connecting;
        self.render_remote();
        self.hub.remote(RemoteAction::Enable);
    }

    fn restart_remote_host(&self) {
        self.remote_recovery_in_progress.set(true);
        self.widgets.remote_restart.set_sensitive(false);
        self.widgets
            .remote_detail
            .set_label("Restarting the managed remote host…");
        self.hub.host(HostAction::RemoteRestart {
            configured_binary: self.stored.borrow().preferences.codex_binary.clone(),
        });
    }

    fn start_remote_pairing(&self) {
        if !matches!(self.state.borrow().remote, RemoteState::Connected) {
            self.toast("Enable the remote host and wait for it to connect before pairing");
            return;
        }
        self.hub.remote(RemoteAction::Pair);
    }

    fn login(&self) {
        self.request(
            "account/login/start",
            json!({
                "type": "chatgpt",
                "appBrand": "chatgpt",
                "useHostedLoginSuccessPage": true,
                "codexStreamlinedLogin": true
            }),
            PendingKind::Login,
        );
    }

    fn logout(&self) {
        self.request("account/logout", json!({}), PendingKind::Logout);
    }

    fn add_account_profile(&self) {
        let profile = {
            let mut stored = self.stored.borrow_mut();
            if stored.account_profiles.len() >= 2 {
                self.toast("Codex Native currently supports two active ChatGPT profiles");
                return;
            }
            let profile = AccountProfile::additional(stored.account_profiles.len() + 1);
            stored.account_profiles.push(profile.clone());
            profile
        };
        self.hub.watch_remote_profiles([profile.codex_home.clone()]);
        self.activate_account_profile(&profile.id, true);
    }

    fn activate_account_profile(&self, profile_id: &str, sign_in_when_ready: bool) {
        let profile = {
            let stored = self.stored.borrow();
            stored
                .account_profiles
                .iter()
                .find(|profile| profile.id == profile_id)
                .cloned()
        };
        let Some(profile) = profile else {
            self.toast("That account profile is no longer available");
            return;
        };
        if self.stored.borrow().active_account_id == profile.id {
            return;
        }
        let has_direct_running_work = {
            let stored = self.stored.borrow();
            let state = self.state.borrow();
            !self.hub.uses_managed_transport()
                && has_current_profile_direct_running_work(&state, &stored)
        };
        if has_direct_running_work {
            self.toast("Finish this local task or enable Remote access before switching accounts");
            return;
        }

        let page = self.state.borrow().page;
        self.hub.switch_profile(profile.codex_home.clone());
        {
            let mut stored = self.stored.borrow_mut();
            stored.active_account_id = profile.id.clone();
        }
        let state = AppState {
            page,
            ..Default::default()
        };
        *self.state.borrow_mut() = state;
        self.backend_bootstrapped.set(false);
        self.extensions_bootstrapped.set(false);
        self.qwen_bootstrapped.set(false);
        self.diagnostics_bootstrapped.set(false);
        self.remote_expected_enabled.set(false);
        self.remote_handoff_pending.set(false);
        self.login_after_profile_ready.set(sign_in_when_ready);
        self.selected_thread_ids.borrow_mut().clear();
        self.invalidate_transcript();
        self.persist();
        self.render_all();
        self.widgets.account_menu.popdown();
    }
}

fn value_array(value: &Value) -> &[Value] {
    value
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| value.as_array())
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn initial_bootstrap_requests(
    archived: bool,
    sort_key: &str,
    cwd: &str,
) -> Vec<(&'static str, &'static str, Value)> {
    vec![
        (
            "threads",
            "thread/list",
            thread_list_params(archived, sort_key),
        ),
        ("agents", "thread/list", subagent_thread_list_params()),
        ("models", "model/list", model_list_params()),
        ("account", "account/read", json!({"refreshToken": false})),
        ("config", "config/read", json!({"includeLayers": false})),
        ("requirements", "configRequirements/read", json!({})),
        (
            "permission-profiles",
            "permissionProfile/list",
            json!({"cwd": cwd, "limit": 100}),
        ),
        (
            "features",
            "experimentalFeature/list",
            json!({"limit": 100}),
        ),
        ("collaboration", "collaborationMode/list", json!({})),
        (
            "provider-capabilities",
            "modelProvider/capabilities/read",
            json!({}),
        ),
    ]
}

fn thread_list_params(archived: bool, sort_key: &str) -> Value {
    json!({
        "limit": 100,
        "archived": archived,
        "sortKey": sort_key,
        "sortDirection": "desc"
    })
}

fn thread_refresh_params(archived: bool, sort_key: &str) -> Value {
    let mut params = thread_list_params(archived, sort_key);
    params["useStateDbOnly"] = Value::Bool(true);
    params
}

fn thread_resume_params(thread_id: &str) -> Value {
    json!({
        "threadId": thread_id,
        // Bootstrap the newest bounded page in the same request. This avoids
        // separately replaying a multi-gigabyte legacy rollout before resume.
        "excludeTurns": true,
        "initialTurnsPage": {
            "limit": TRANSCRIPT_PAGE_TURNS,
            "itemsView": "summary",
            "sortDirection": "desc"
        }
    })
}

fn auto_hydrate_turn_activity(thread: &ThreadSummary) -> bool {
    should_auto_hydrate_rollout_size(
        thread
            .path
            .as_deref()
            .and_then(|path| std::fs::metadata(path).ok())
            .map(|metadata| metadata.len()),
    )
}

fn should_auto_hydrate_rollout_size(size: Option<u64>) -> bool {
    size.is_none_or(|bytes| bytes <= MAX_AUTO_DETAIL_ROLLOUT_BYTES)
}

fn local_rollout_fingerprint(path: &Path) -> Option<(u64, u64, u32)> {
    let metadata = std::fs::metadata(path).ok()?;
    if !metadata.is_file() {
        return None;
    }
    let modified = metadata.modified().ok()?.duration_since(UNIX_EPOCH).ok()?;
    Some((metadata.len(), modified.as_secs(), modified.subsec_nanos()))
}

fn transcript_scroll_target(
    follow_bottom: bool,
    prepended: bool,
    previous_value: f64,
    previous_upper: f64,
    new_upper: f64,
    new_page_size: f64,
) -> f64 {
    let maximum = (new_upper - new_page_size).max(0.0);
    let target = if follow_bottom {
        maximum
    } else if prepended {
        previous_value + (new_upper - previous_upper).max(0.0)
    } else {
        previous_value
    };
    target.clamp(0.0, maximum)
}

fn eased_transcript_scroll_progress(elapsed: Duration) -> f64 {
    let progress =
        (elapsed.as_secs_f64() / TRANSCRIPT_SCROLL_ANIMATION.as_secs_f64()).clamp(0.0, 1.0);
    1.0 - (1.0 - progress).powi(3)
}

fn adjustment_is_near_bottom(adjustment: &gtk::Adjustment) -> bool {
    let maximum = (adjustment.upper() - adjustment.page_size()).max(0.0);
    maximum - adjustment.value() < 96.0
}

fn composer_return_sends(key: gdk::Key, modifiers: gdk::ModifierType) -> bool {
    matches!(key, gdk::Key::Return | gdk::Key::KP_Enter)
        && !modifiers.contains(gdk::ModifierType::SHIFT_MASK)
}

fn transcript_row_specs(
    thread: &ThreadSummary,
    state: &AppState,
    show_reasoning: bool,
    routing_state: Option<&TaskRoutingState>,
) -> Vec<TranscriptRowSpec> {
    let mut rows = Vec::new();
    for (turn_index, turn) in thread.turns.iter().enumerate() {
        let turn_key = if turn.id.is_empty() {
            format!("turn-index-{turn_index}")
        } else {
            format!("turn-{}", turn.id)
        };
        if !turn.id.is_empty()
            && let Some(route) = routing_state.and_then(|state| state.route_for_turn(&turn.id))
        {
            rows.push(TranscriptRowSpec {
                key: format!("{turn_key}:route"),
                fingerprint: fingerprint_values(
                    [
                        &route.model,
                        &route.effort,
                        &route.service_tier,
                        &route.reason,
                    ],
                    &[],
                ),
                content: TranscriptRowContent::Route(route.clone()),
            });
        }
        let mut index = 0;
        while index < turn.items.len() {
            let item = &turn.items[index];
            let kind = item.get("type").and_then(Value::as_str).unwrap_or("item");
            if kind == "reasoning" && !show_reasoning {
                index += 1;
                continue;
            }
            if kind == "imageView" {
                let start = index;
                let mut images = Vec::new();
                while index < turn.items.len()
                    && turn.items[index].get("type").and_then(Value::as_str) == Some("imageView")
                {
                    images.extend(item_image_sources(&turn.items[index], &thread.cwd));
                    index += 1;
                }
                if images.is_empty() {
                    continue;
                }
                let identity = item
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("index-{start}"));
                rows.push(TranscriptRowSpec {
                    key: format!("{turn_key}:images:{identity}"),
                    fingerprint: fingerprint_values(["Viewed"], &images),
                    content: TranscriptRowContent::Images {
                        images,
                        verb: "Viewed",
                    },
                });
                continue;
            }

            let item_id = item
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| format!("index-{index}"));
            let streamed_text = item
                .get("id")
                .and_then(Value::as_str)
                .and_then(|id| state.streaming_text.get(id))
                .cloned()
                .unwrap_or_default();
            rows.push(TranscriptRowSpec {
                key: format!("{turn_key}:item:{item_id}"),
                fingerprint: transcript_item_fingerprint(
                    item,
                    &streamed_text,
                    show_reasoning,
                    &thread.cwd,
                ),
                content: TranscriptRowContent::Item {
                    item: item.clone(),
                    streamed_text,
                    show_reasoning,
                    cwd: thread.cwd.clone(),
                },
            });
            index += 1;
        }
        if let Some(error) = &turn.error {
            rows.push(TranscriptRowSpec {
                key: format!("{turn_key}:error"),
                fingerprint: fingerprint_values(["error"], &[error.to_string()]),
                content: TranscriptRowContent::TurnError(error.clone()),
            });
        }
        if !turn.id.is_empty()
            && let Some(receipt) =
                routing_state.and_then(|state| state.token_savings_for_turn(&turn.id))
        {
            rows.push(TranscriptRowSpec {
                key: format!("{turn_key}:token-savings"),
                fingerprint: fingerprint_values(
                    [
                        &receipt.potential_tokens_saved.to_string(),
                        &receipt.automatic_routing_tokens_saved.to_string(),
                        &receipt.qwen_tokens_saved.to_string(),
                        &receipt.qwen_successful_uses.to_string(),
                        &receipt.observed_turn_tokens.unwrap_or(0).to_string(),
                        &receipt.basis,
                    ],
                    &receipt.qwen_routes,
                ),
                content: TranscriptRowContent::TokenSavings(receipt.clone()),
            });
        }
    }
    rows
}

fn transcript_item_fingerprint(
    item: &Value,
    streamed_text: &str,
    show_reasoning: bool,
    cwd: &str,
) -> u64 {
    let kind = item.get("type").and_then(Value::as_str).unwrap_or("item");
    match kind {
        "agentMessage" => {
            let text = if streamed_text.is_empty() {
                item.get("text").and_then(Value::as_str).unwrap_or_default()
            } else {
                streamed_text
            };
            fingerprint_values([kind, text], &[])
        }
        "userMessage" => {
            let images = item_image_sources(item, cwd);
            fingerprint_values([kind, &user_message_text(item)], &images)
        }
        "reasoning" => fingerprint_values(
            [kind, &show_reasoning.to_string(), &extract_reasoning(item)],
            &[],
        ),
        _ => fingerprint_values([kind, cwd, &item.to_string()], &[]),
    }
}

fn streamed_agent_message_text(content: &TranscriptRowContent) -> Option<&str> {
    let TranscriptRowContent::Item {
        item,
        streamed_text,
        ..
    } = content
    else {
        return None;
    };
    (item.get("type").and_then(Value::as_str) == Some("agentMessage") && !streamed_text.is_empty())
        .then_some(streamed_text.as_str())
}

fn fingerprint_values<const N: usize>(parts: [&str; N], values: &[String]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    parts.hash(&mut hasher);
    values.hash(&mut hasher);
    hasher.finish()
}

fn clipboard_formats_contain_image(formats: &gdk::ContentFormats) -> bool {
    formats.contains_type(gdk::Texture::static_type())
        || formats
            .mime_types()
            .iter()
            .any(|mime| mime.as_str().starts_with("image/"))
}

fn clipboard_image_path() -> std::io::Result<PathBuf> {
    let directory = env::temp_dir().join("codex-native-clipboard");
    std::fs::create_dir_all(&directory)?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    Ok(directory.join(format!(
        "clipboard-image-{}-{nonce}.png",
        std::process::id()
    )))
}

const SMOKE_FIXTURE_THREAD_ID: &str = "codex-native-smoke-task";

fn smoke_fixtures_enabled() -> bool {
    env::var_os("CODEX_NATIVE_SMOKE_FIXTURES").is_some()
}

fn install_smoke_clipboard_image(controller: &Controller) {
    if env::var_os("CODEX_NATIVE_SMOKE_CLIPBOARD_IMAGE").is_none() {
        return;
    }
    let installed_icon =
        Path::new("/usr/share/icons/hicolor/scalable/apps/io.codexnative.Arch.svg");
    let path = if installed_icon.is_file() {
        installed_icon.to_path_buf()
    } else {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("data/io.codexnative.Arch.svg")
    };
    match gdk::Texture::from_file(&gio::File::for_path(path)) {
        Ok(texture) => {
            controller
                .widgets
                .composer
                .clipboard()
                .set_texture(&texture);
            controller
                .smoke_clipboard_texture
                .replace(Some(texture.clone()));
            tracing::debug!(
                formats = ?controller.widgets.composer.clipboard().formats(),
                "installed smoke clipboard image"
            );
        }
        Err(error) => tracing::warn!(%error, "could not install smoke clipboard image"),
    }
}

fn install_smoke_fixtures(state: &mut AppState) {
    let activate_fixture = state
        .active_thread_id
        .as_deref()
        .is_none_or(|id| id == SMOKE_FIXTURE_THREAD_ID || id == "codex-native-smoke-agent");
    let installed_icon =
        Path::new("/usr/share/icons/hicolor/scalable/apps/io.codexnative.Arch.svg");
    let image_path = if installed_icon.is_file() {
        installed_icon.to_path_buf()
    } else {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("data/io.codexnative.Arch.svg")
    };
    let image_path = image_path.to_string_lossy().into_owned();
    let parent = ThreadSummary {
        id: SMOKE_FIXTURE_THREAD_ID.into(),
        preview: "Synthetic local UI fixture; no server or account mutation.".into(),
        name: Some("Codex Native rich transcript smoke".into()),
        cwd: env!("CARGO_MANIFEST_DIR").into(),
        status: json!({"type": "active", "activeFlags": []}),
        turns: vec![Turn {
            id: "codex-native-smoke-turn".into(),
            status: json!("inProgress"),
            items: vec![
                json!({
                    "id": "smoke-message",
                    "type": "agentMessage",
                    "text": "This local-only fixture verifies rich transcript controls without starting an inference."
                }),
                json!({
                    "id": "smoke-image",
                    "type": "imageView",
                    "path": image_path.clone()
                }),
                json!({
                    "id": "smoke-image-two",
                    "type": "imageView",
                    "path": image_path
                }),
                json!({
                    "id": "smoke-file-change",
                    "type": "fileChange",
                    "status": "completed",
                    "changes": [{
                        "path": "Cargo.toml",
                        "type": "update",
                        "diff": "@@ -1 +1 @@\n-name = old\n+name = codex-native"
                    }]
                }),
                json!({
                    "id": "smoke-agent",
                    "type": "collabAgentToolCall",
                    "tool": "spawnAgent",
                    "senderThreadId": SMOKE_FIXTURE_THREAD_ID,
                    "receiverThreadIds": ["codex-native-smoke-agent"],
                    "agentsStates": {
                        "codex-native-smoke-agent": {"status": "running"}
                    },
                    "status": "completed"
                }),
                json!({
                    "id": "smoke-hook",
                    "type": "hookPrompt",
                    "fragments": [{"hookRunId": "smoke", "text": "Local hook context smoke fixture."}]
                }),
                json!({
                    "id": "smoke-subagent-activity",
                    "type": "subAgentActivity",
                    "agentPath": "Smoke Builder",
                    "agentThreadId": "codex-native-smoke-agent",
                    "kind": "started"
                }),
                json!({
                    "id": "smoke-wait",
                    "type": "sleep",
                    "durationMs": 1250
                }),
                json!({
                    "id": "smoke-review-enter",
                    "type": "enteredReviewMode",
                    "review": "Native activity rendering smoke"
                }),
                json!({
                    "id": "smoke-review-exit",
                    "type": "exitedReviewMode",
                    "review": "Native activity rendering smoke"
                }),
                json!({
                    "id": "smoke-compaction",
                    "type": "contextCompaction"
                }),
            ],
            ..Turn::default()
        }],
        ..ThreadSummary::default()
    };
    let child = ThreadSummary {
        id: "codex-native-smoke-agent".into(),
        parent_thread_id: Some(SMOKE_FIXTURE_THREAD_ID.into()),
        agent_nickname: Some("Smoke Builder".into()),
        cwd: env!("CARGO_MANIFEST_DIR").into(),
        status: json!({"type": "active", "activeFlags": []}),
        ..ThreadSummary::default()
    };
    state.upsert_thread(parent);
    state.upsert_thread(child);
    state
        .thread_order
        .retain(|id| id != SMOKE_FIXTURE_THREAD_ID);
    state.thread_order.insert(0, SMOKE_FIXTURE_THREAD_ID.into());
    // Periodic thread/list refreshes reinstall deterministic fixture data.
    // They must never steal focus from a real task the smoke operator opened.
    if activate_fixture {
        state.active_thread_id = Some(SMOKE_FIXTURE_THREAD_ID.into());
        state.active_turn_id = Some("codex-native-smoke-turn".into());
    }
    state.turn_progress.insert(
        SMOKE_FIXTURE_THREAD_ID.into(),
        TurnProgress {
            turn_id: "codex-native-smoke-turn".into(),
            plan: vec![
                json!({"step": "Inspect protocol", "status": "completed"}),
                json!({"step": "Render native controls", "status": "inProgress"}),
                json!({"step": "Verify interactions", "status": "pending"}),
            ],
            diff: "diff --git a/src/model.rs b/src/model.rs\n--- a/src/model.rs\n+++ b/src/model.rs\n-old\n+new\ndiff --git a/src/ui/mod.rs b/src/ui/mod.rs\n--- a/src/ui/mod.rs\n+++ b/src/ui/mod.rs\n+viewer\n".into(),
        },
    );
}

fn subagent_thread_list_params() -> Value {
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
    })
}

fn subagent_thread_refresh_params() -> Value {
    let mut params = subagent_thread_list_params();
    params["useStateDbOnly"] = Value::Bool(true);
    params
}

fn task_subagent_params(thread_id: &str) -> Value {
    json!({
        "limit": 1000,
        "archived": false,
        "ancestorThreadId": thread_id,
        "sortKey": "updated_at",
        "sortDirection": "desc",
        "useStateDbOnly": true
    })
}

fn thread_search_params(query: &str, archived: bool, sort_key: &str) -> Value {
    let mut params = thread_list_params(archived, sort_key);
    params["searchTerm"] = Value::String(query.to_owned());
    params
}

fn model_list_params() -> Value {
    json!({"limit": 100, "includeHidden": false})
}

fn local_plugin_list_params(cwd: &str) -> Value {
    json!({
        "cwds": [cwd],
        "marketplaceKinds": ["local", "workspace-directory"]
    })
}

fn qwen_install_state(
    catalog_state: Option<(bool, bool)>,
    server_present: bool,
    server_ready: bool,
) -> (bool, bool) {
    let catalog_installed = catalog_state.is_some_and(|(installed, _)| installed);
    let catalog_enabled = catalog_state.is_some_and(|(installed, enabled)| installed && enabled);
    (
        catalog_installed || server_present,
        catalog_enabled || server_ready,
    )
}

fn default_model_option_label(models: &[Value]) -> String {
    let _ = models;
    "Default · GPT-5.6 Terra".to_owned()
}

fn selected_model_metadata<'a>(models: &'a [Value], selected_model: &str) -> Option<&'a Value> {
    if selected_model.is_empty() {
        return models
            .iter()
            .find(|model| {
                model
                    .get("id")
                    .or_else(|| model.get("model"))
                    .and_then(Value::as_str)
                    == Some(DEFAULT_CODEX_MODEL)
            })
            .or_else(|| models.first());
    }
    models.iter().find(|model| {
        model
            .get("id")
            .or_else(|| model.get("model"))
            .and_then(Value::as_str)
            == Some(selected_model)
    })
}

fn resolved_model_id(models: &[Value], selected_model: &str) -> String {
    if selected_model == BACKEND_QWEN {
        return QWEN_ORCHESTRATOR_MODEL.to_owned();
    }
    if matches!(
        selected_model,
        BACKEND_GEMINI | BACKEND_OPENROUTER | BACKEND_MISTRAL
    ) || selected_model.is_empty()
    {
        return DEFAULT_CODEX_MODEL.to_owned();
    }
    if !selected_model.is_empty() {
        return selected_model.to_owned();
    }
    selected_model_metadata(models, selected_model)
        .and_then(|model| model.get("id").or_else(|| model.get("model")))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn routing_mode_for_model(selected_model: &str) -> &'static str {
    match selected_model {
        BACKEND_QWEN => MODE_QWEN,
        BACKEND_GEMINI => MODE_GEMINI,
        BACKEND_OPENROUTER => MODE_OPENROUTER,
        BACKEND_MISTRAL => MODE_MISTRAL,
        _ => MODE_MANUAL,
    }
}

fn is_buddy_model(selected_model: &str) -> bool {
    matches!(
        selected_model,
        BACKEND_QWEN | BACKEND_GEMINI | BACKEND_OPENROUTER | BACKEND_MISTRAL
    )
}

fn sandbox_options_for_model(selected_model: &str) -> &'static [(&'static str, &'static str)] {
    if is_buddy_model(selected_model) {
        BUDDY_SANDBOX_OPTIONS
    } else {
        GPT_SANDBOX_OPTIONS
    }
}

fn selected_model_for_task(
    runtime_model: &str,
    routing_state: Option<&TaskRoutingState>,
) -> String {
    match routing_state.map(|state| routing::normalize_mode(&state.mode)) {
        Some(MODE_QWEN) => BACKEND_QWEN.to_owned(),
        Some(MODE_GEMINI) => BACKEND_GEMINI.to_owned(),
        Some(MODE_OPENROUTER) => BACKEND_OPENROUTER.to_owned(),
        Some(MODE_MISTRAL) => BACKEND_MISTRAL.to_owned(),
        _ => runtime_model.to_owned(),
    }
}

fn model_display_name(model_id: &str) -> Option<&'static str> {
    if model_id.is_empty() {
        return Some("Default · GPT-5.6 Terra");
    }
    COMPOSER_MODELS
        .iter()
        .find_map(|(id, label)| (*id == model_id).then_some(*label))
}

fn task_model_badge(routing: Option<&TaskRoutingState>) -> Option<&'static str> {
    match routing.map(|routing| routing::normalize_mode(&routing.mode)) {
        Some(MODE_QWEN) | Some(MODE_QWEN_ASSIST) => Some("Qwen"),
        Some(MODE_GEMINI) => Some("Gemini"),
        Some(MODE_OPENROUTER) => Some("OpenRouter Free"),
        Some(MODE_MISTRAL) => Some("Mistral AI"),
        _ => None,
    }
}

fn buddy_author(backend: &str) -> &'static str {
    match backend {
        "gemini" => "Gemini",
        "openrouter" => "OpenRouter Free",
        "mistral" => "Mistral AI",
        _ => "Qwen",
    }
}

fn buddy_activity_item(turn_id: &str, author: &str, effort: &str) -> Value {
    json!({
        "id": format!("{turn_id}-reasoning"),
        "type": "reasoning",
        "author": author,
        "reasoningKind": if matches!(effort, "none" | "minimal") {
            "activity"
        } else {
            "thinking"
        },
        "summary": "Preparing request",
        "turnTotalTokens": 0,
        "promptTokens": 0,
        "outputTokens": 0,
        "contextUsedTokens": 0,
        "contextWindowTokens": 0,
        "modelCalls": 0,
    })
}

fn update_buddy_activity(
    turn: &mut Turn,
    author: &str,
    progress: &crate::host::BuddyProgress,
) -> u64 {
    let item_id = format!("{}-reasoning", turn.id);
    let index = turn
        .items
        .iter()
        .position(|item| item.get("id").and_then(Value::as_str) == Some(item_id.as_str()))
        .unwrap_or_else(|| {
            turn.items
                .push(buddy_activity_item(&turn.id, author, "none"));
            turn.items.len() - 1
        });
    let item = &mut turn.items[index];
    let previous_turn_tokens = item
        .get("turnTotalTokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let line = buddy_activity_line(progress);
    let mut lines = item
        .get("summary")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if !line.is_empty() && lines.last().is_none_or(|last| last != &line) {
        lines.push(line);
    }
    while lines.len() > 80 || lines.iter().map(String::len).sum::<usize>() > 12_000 {
        lines.remove(0);
    }
    if let Some(object) = item.as_object_mut() {
        object.insert("author".into(), author.into());
        object.insert("summary".into(), lines.join("\n").into());
        object.insert("modelCalls".into(), progress.model_calls.into());
        object.insert("promptTokens".into(), progress.prompt_tokens.into());
        object.insert("outputTokens".into(), progress.output_tokens.into());
        object.insert("turnTotalTokens".into(), progress.total_tokens.into());
        object.insert(
            "contextUsedTokens".into(),
            progress.context_used_tokens.into(),
        );
        object.insert(
            "contextWindowTokens".into(),
            progress.context_window_tokens.into(),
        );
        object.insert("tokensPerSecond".into(), json!(progress.tokens_per_second));
    }
    previous_turn_tokens
}

fn buddy_activity_line(progress: &crate::host::BuddyProgress) -> String {
    let (base_phase, lifecycle) = progress
        .phase
        .strip_suffix("_started")
        .map(|phase| (phase, Some("started")))
        .or_else(|| {
            progress
                .phase
                .strip_suffix("_completed")
                .map(|phase| (phase, Some("completed")))
        })
        .unwrap_or((progress.phase.as_str(), None));
    let phase = match base_phase {
        "starting" => "Starting",
        "planning" | "plan_ready" => "Planning",
        "plan_step" => "Plan step",
        "plan_risk" => "Plan risk",
        "acceptance_check" => "Acceptance",
        "read" => "Read",
        "search" => "Search",
        "inspect_diff" => "Diff",
        "inspect_system" => "System",
        "run_test" => "Test",
        "propose_patch" => "Patch",
        "action_rejected" => "Retry",
        "reviewing" | "reviewed" => "Review",
        "review_issue" => "Review issue",
        "verification" => "Verify",
        "needs_revision" => "Revision",
        "finish" => "Finish",
        "complete" => "Complete",
        "error" => "Error",
        _ => "Working",
    };
    let phase = lifecycle.map_or_else(|| phase.to_owned(), |state| format!("{phase} {state}"));
    let mut line = if progress.detail.trim().is_empty() {
        phase
    } else {
        format!("{phase} — {}", compact_ui_text(&progress.detail, 240))
    };
    if progress.context_used_tokens > 0 {
        line.push_str(&format!(
            " · context {}{}",
            format_integer(i64::try_from(progress.context_used_tokens).unwrap_or(i64::MAX)),
            if progress.context_window_tokens > 0 {
                format!(
                    " / {} tokens",
                    format_integer(
                        i64::try_from(progress.context_window_tokens).unwrap_or(i64::MAX)
                    )
                )
            } else {
                " tokens".to_owned()
            }
        ));
    }
    if let Some(tokens_per_second) = progress.tokens_per_second.filter(|value| value.is_finite()) {
        line.push_str(&format!(" · {tokens_per_second:.1} tok/s"));
    }
    line
}

fn buddy_usage_from_progress(
    existing: Option<&Value>,
    previous_turn_tokens: u64,
    provider: &str,
    progress: &crate::host::BuddyProgress,
) -> Option<Value> {
    let turn_total = progress.total_tokens.max(
        progress
            .prompt_tokens
            .saturating_add(progress.output_tokens),
    );
    let current_context = progress.context_used_tokens;
    if turn_total == 0 && current_context == 0 {
        return None;
    }
    let previous_total = existing
        .and_then(|value| value.pointer("/total/totalTokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let base_total = previous_total.saturating_sub(previous_turn_tokens);
    let mut provider_totals = existing
        .and_then(|value| value.get("providerTotals"))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let previous_provider_total = provider_totals
        .get(provider)
        .and_then(Value::as_u64)
        .or_else(|| {
            existing
                .filter(|value| value.get("provider").and_then(Value::as_str) == Some(provider))
                .and_then(|_| {
                    existing
                        .and_then(|value| value.pointer("/total/totalTokens"))
                        .and_then(Value::as_u64)
                })
        })
        .unwrap_or(0);
    let provider_total = previous_provider_total
        .saturating_sub(previous_turn_tokens)
        .saturating_add(turn_total);
    provider_totals.insert(provider.to_owned(), provider_total.into());
    let context_window = progress.context_window_tokens.max(
        existing
            .and_then(|value| value.get("modelContextWindow"))
            .and_then(Value::as_u64)
            .unwrap_or_else(|| if provider == "Qwen" { 65_536 } else { 0 }),
    );
    let tokens_per_second = progress.tokens_per_second.or_else(|| {
        existing
            .and_then(|value| value.get("tokensPerSecond"))
            .and_then(Value::as_f64)
    });
    Some(json!({
        "provider": provider,
        "providerTotalTokens": provider_total,
        "providerTotals": provider_totals,
        "tokensPerSecond": tokens_per_second,
        "modelContextWindow": context_window,
        "last": {
            "totalTokens": current_context,
            "inputTokens": progress.prompt_tokens,
            "outputTokens": progress.output_tokens,
        },
        "total": {
            "totalTokens": base_total.saturating_add(turn_total),
        },
    }))
}

fn buddy_completion_progress(
    metrics: Option<&Value>,
    backend: &str,
) -> Option<crate::host::BuddyProgress> {
    let metrics = metrics?;
    let prompt_tokens = metrics
        .get("promptTokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output_tokens = metrics
        .get("outputTokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let context_used_tokens = metrics
        .get("contextUsedTokens")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| prompt_tokens.saturating_add(output_tokens));
    if prompt_tokens == 0 && output_tokens == 0 && context_used_tokens == 0 {
        return None;
    }
    Some(crate::host::BuddyProgress {
        phase: "complete".into(),
        detail: "Response ready".into(),
        model_calls: metrics
            .get("modelCalls")
            .and_then(Value::as_u64)
            .unwrap_or(1),
        prompt_tokens,
        output_tokens,
        total_tokens: prompt_tokens.saturating_add(output_tokens),
        context_used_tokens,
        context_window_tokens: metrics
            .get("contextWindowTokens")
            .and_then(Value::as_u64)
            .unwrap_or_else(|| if backend == "qwen" { 65_536 } else { 0 }),
        tokens_per_second: metrics.get("tokensPerSecond").and_then(Value::as_f64),
    })
}

fn buddy_progress_plan(backend: &str, phase: &str, detail: &str) -> Vec<Value> {
    let provider = buddy_author(backend);
    let current = match phase {
        "starting" => 0,
        "planning" => 1,
        "reviewing" | "reviewed" => 3,
        "complete" | "needs_revision" => 4,
        _ => 2,
    };
    let mut labels = [
        format!("{provider}: Preparing request"),
        format!("{provider}: Planning"),
        format!("{provider}: Working"),
        format!("{provider}: Reviewing"),
        format!("{provider}: Finalizing"),
    ];
    if !detail.trim().is_empty() {
        labels[current] = format!("{provider}: {}", compact_ui_text(detail, 180));
    }
    let finished = phase == "complete";
    labels
        .into_iter()
        .enumerate()
        .map(|(index, step)| {
            json!({
                "step": step,
                "status": if finished || index < current {
                    "completed"
                } else if index == current {
                    "inProgress"
                } else {
                    "pending"
                }
            })
        })
        .collect()
}

fn approximate_tokens(text: &str) -> u64 {
    (text.chars().count() as u64).div_ceil(4)
}

fn thread_start_runtime_params(settings: &TaskRuntimeSettings) -> Map<String, Value> {
    let mut params = Map::new();
    if !settings.model.is_empty() {
        params.insert("model".into(), Value::String(settings.model.clone()));
    }
    if let Some(effort) = settings
        .reasoning_effort
        .as_deref()
        .filter(|effort| !effort.is_empty())
    {
        params.insert("config".into(), json!({"model_reasoning_effort": effort}));
    }
    params.insert(
        "serviceTier".into(),
        settings
            .service_tier
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    params
}

fn reasoning_effort_options_for_model(
    models: &[Value],
    selected_model: &str,
) -> Vec<(&'static str, &'static str)> {
    if selected_model == BACKEND_QWEN {
        return vec![
            ("xhigh", "Extra High"),
            ("high", "High"),
            ("medium", "Medium"),
            ("low", "Low"),
            ("none", "Non-thinking"),
        ];
    }
    if selected_model == BACKEND_GEMINI {
        return vec![
            ("minimal", "Minimal"),
            ("low", "Low"),
            ("medium", "Medium"),
            ("high", "High"),
        ];
    }
    if selected_model == BACKEND_OPENROUTER {
        return vec![
            ("minimal", "Minimal"),
            ("low", "Low"),
            ("medium", "Medium"),
            ("high", "High"),
        ];
    }
    if selected_model == BACKEND_MISTRAL {
        return vec![("high", "High"), ("none", "Non-thinking")];
    }
    let Some(supported) = selected_model_metadata(models, selected_model)
        .and_then(|model| model.get("supportedReasoningEfforts"))
        .and_then(Value::as_array)
        .filter(|supported| !supported.is_empty())
    else {
        return REASONING_EFFORT_OPTIONS.to_vec();
    };
    let options = REASONING_EFFORT_OPTIONS
        .iter()
        .copied()
        .filter(|(id, _)| {
            supported.iter().any(|value| {
                value
                    .get("reasoningEffort")
                    .or_else(|| value.get("effort"))
                    .and_then(Value::as_str)
                    .or_else(|| value.as_str())
                    == Some(*id)
            })
        })
        .collect::<Vec<_>>();
    if options.is_empty() {
        REASONING_EFFORT_OPTIONS.to_vec()
    } else {
        options
    }
}

fn codex_effort_for_backend(selected_model: &str, selected_effort: &str) -> String {
    if matches!(
        selected_model,
        BACKEND_QWEN | BACKEND_GEMINI | BACKEND_OPENROUTER | BACKEND_MISTRAL
    ) {
        match selected_effort {
            "none" | "minimal" | "low" => "low",
            "high" | "xhigh" => "high",
            _ => "medium",
        }
        .to_owned()
    } else {
        selected_effort.to_owned()
    }
}

fn normalized_effort_for_model(selected_model: &str, effort: &str) -> String {
    match selected_model {
        BACKEND_QWEN if matches!(effort, "none" | "low" | "medium" | "high" | "xhigh") => effort,
        BACKEND_QWEN => "xhigh",
        BACKEND_GEMINI | BACKEND_OPENROUTER
            if matches!(effort, "minimal" | "low" | "medium" | "high") =>
        {
            effort
        }
        BACKEND_GEMINI | BACKEND_OPENROUTER => "medium",
        BACKEND_MISTRAL if matches!(effort, "none" | "high") => effort,
        BACKEND_MISTRAL => "high",
        _ => effort,
    }
    .to_owned()
}

fn service_tier_options_for_model(
    models: &[Value],
    selected_model: &str,
) -> Vec<(&'static str, &'static str)> {
    if matches!(
        selected_model,
        BACKEND_QWEN | BACKEND_GEMINI | BACKEND_OPENROUTER | BACKEND_MISTRAL
    ) {
        return vec![("standard", "Standard")];
    }
    let Some(model) = selected_model_metadata(models, selected_model) else {
        return SERVICE_TIER_OPTIONS.to_vec();
    };
    let supports_fast = model
        .get("serviceTiers")
        .and_then(Value::as_array)
        .is_some_and(|tiers| {
            tiers
                .iter()
                .any(|tier| tier.get("id").and_then(Value::as_str) == Some("priority"))
        });
    if supports_fast {
        SERVICE_TIER_OPTIONS.to_vec()
    } else {
        vec![("standard", "Standard")]
    }
}

#[cfg(test)]
fn service_tier_value(selection: &str) -> Value {
    if selection == "priority" {
        Value::String("priority".into())
    } else {
        Value::Null
    }
}

fn task_runtime_settings_from_server(value: &Value) -> Option<(String, TaskRuntimeSettings)> {
    let settings = value.get("threadSettings").unwrap_or(value);
    let thread_id = value
        .get("threadId")
        .or_else(|| value.pointer("/thread/id"))
        .and_then(Value::as_str)?;
    let model = settings.get("model").and_then(Value::as_str)?;
    let sandbox_policy = settings
        .get("sandboxPolicy")
        .or_else(|| settings.get("sandbox"))
        .cloned()
        .unwrap_or_else(|| json!({"type": "workspaceWrite"}));
    let approval_policy = settings
        .get("approvalPolicy")
        .cloned()
        .unwrap_or_else(|| json!("on-request"));
    Some((
        thread_id.to_owned(),
        TaskRuntimeSettings {
            model: model.to_owned(),
            reasoning_effort: settings
                .get("effort")
                .or_else(|| settings.get("reasoningEffort"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            // App-server serializes the normal tier as `"default"`, while
            // turn/start and settings/update accept null for that same mode.
            // Keep the UI vocabulary deliberately limited to Standard/Fast.
            service_tier: settings
                .get("serviceTier")
                .and_then(Value::as_str)
                .filter(|tier| *tier == "priority")
                .map(str::to_owned),
            sandbox_policy,
            approval_policy,
        },
    ))
}

fn sandbox_control_id(policy: &Value) -> &'static str {
    match policy
        .get("type")
        .and_then(Value::as_str)
        .or_else(|| policy.as_str())
    {
        Some("readOnly" | "read-only") => "read-only",
        Some("dangerFullAccess" | "danger-full-access") => "danger-full-access",
        Some("externalSandbox" | "external-sandbox") => "external-sandbox",
        _ => "workspace-write",
    }
}

fn sandbox_policy_for_selection(selection: &str, existing: &Value) -> Value {
    if sandbox_control_id(existing) == selection {
        return existing.clone();
    }
    match selection {
        "read-only" => json!({"type": "readOnly"}),
        "danger-full-access" => json!({"type": "dangerFullAccess"}),
        "external-sandbox" => json!({"type": "externalSandbox"}),
        _ => json!({"type": "workspaceWrite"}),
    }
}

fn approval_control_id(policy: &Value) -> &str {
    policy.as_str().unwrap_or("granular")
}

fn approval_policy_for_selection(selection: &str, existing: &Value) -> Value {
    if selection == "granular" && !existing.is_string() {
        existing.clone()
    } else {
        Value::String(selection.to_owned())
    }
}

fn task_settings_update_params(thread_id: &str, settings: &TaskRuntimeSettings) -> Value {
    json!({
        "threadId": thread_id,
        "model": settings.model,
        "effort": settings.reasoning_effort,
        "serviceTier": settings.service_tier,
        "sandboxPolicy": settings.sandbox_policy,
        "approvalPolicy": settings.approval_policy,
    })
}

fn route_from_runtime_settings(settings: &TaskRuntimeSettings) -> RouteDecision {
    routing::manual_route(
        &settings.model,
        settings.reasoning_effort.as_deref().unwrap_or("medium"),
        settings.service_tier.as_deref(),
    )
}

fn subagents_enabled_metadata_value(enabled: bool) -> Value {
    Value::String(enabled.to_string())
}

#[cfg(test)]
fn turn_completion_failed(params: &Value) -> bool {
    params.pointer("/turn/status").and_then(Value::as_str) == Some("failed")
        || params
            .pointer("/turn/error")
            .is_some_and(|error| !error.is_null())
}

impl Controller {
    fn new_task(&self) {
        let mut state = self.state.borrow_mut();
        state.active_thread_id = None;
        state.active_turn_id = None;
        state.latest_diff.clear();
        state.mark_transcript_changed();
        drop(state);
        self.unsubscribe_idle_threads();
        self.invalidate_transcript();
        self.attachments.borrow_mut().clear();
        self.widgets.composer.buffer().set_text("");
        self.widgets.stack.set_visible_child_name("chat");
        self.apply_new_task_defaults();
        self.render_threads();
        self.render_current_page();
        self.widgets.composer.grab_focus();
    }

    fn send_composer(&self) {
        if self.state.borrow().connection != ConnectionState::Ready {
            self.toast("Codex is still connecting");
            return;
        }
        if self.state.borrow().pending.values().any(|kind| {
            matches!(
                kind,
                PendingKind::StartThread { .. }
                    | PendingKind::ResumeAndSend { .. }
                    | PendingKind::SendTurn { .. }
                    | PendingKind::SteerTurn { .. }
                    | PendingKind::UnsubscribeThread(_)
            )
        }) {
            self.toast("The previous message is still starting");
            return;
        }
        let buffer = self.widgets.composer.buffer();
        let prompt = buffer
            .text(&buffer.start_iter(), &buffer.end_iter(), false)
            .trim()
            .to_owned();
        let attachments = self
            .attachments
            .borrow()
            .iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        if prompt.is_empty() && attachments.is_empty() {
            return;
        }
        buffer.set_text("");
        self.attachments.borrow_mut().clear();
        self.render_attachments();

        let (thread_id, turn_id, follow_up) = {
            let state = self.state.borrow();
            (
                state.active_thread_id.clone(),
                state.active_turn_id.clone(),
                self.stored.borrow().preferences.follow_up_behavior.clone(),
            )
        };

        let delegate_selected = thread_id.as_ref().is_some_and(|thread_id| {
            self.stored
                .borrow()
                .task_routing
                .get(thread_id)
                .is_some_and(|routing| routing::is_direct_delegate_mode(&routing.mode))
        });
        if let (Some(thread_id), Some(turn_id)) = (&thread_id, turn_id)
            && follow_up == "steer"
            && !delegate_selected
        {
            let input = make_inputs(&prompt, &attachments);
            let pending_item_id = format!("native-steer-{}", uuid::Uuid::new_v4());
            if !self.state.borrow_mut().push_pending_steer(
                thread_id,
                &turn_id,
                &pending_item_id,
                input.clone(),
                chrono::Utc::now().timestamp(),
            ) {
                self.restore_composer_draft(&prompt, &attachments);
                self.toast("The selected task is no longer available");
                return;
            }
            self.schedule_transcript_render();
            let params = json!({
                "threadId": thread_id,
                "expectedTurnId": turn_id,
                "input": input,
            });
            self.request(
                "turn/steer",
                params,
                PendingKind::SteerTurn {
                    thread_id: thread_id.clone(),
                    prompt,
                    attachments,
                    pending_item_id,
                },
            );
            return;
        }

        if let Some(thread_id) = thread_id {
            // Resume once per transport connection. A disconnect clears this
            // set, retaining stale-session safety without reinitializing every
            // task and its MCP/plugin servers before every message. Native
            // Qwen tasks have no Codex rollout to resume and continue directly.
            let needs_resume = {
                let state = self.state.borrow();
                let native_buddy_only = state
                    .threads
                    .get(&thread_id)
                    .is_some_and(|thread| thread.native_buddy_only);
                task_needs_local_rollout_resume(
                    &state.resumed_threads,
                    &thread_id,
                    native_buddy_only,
                )
            };
            if !needs_resume {
                self.start_turn(&thread_id, prompt, attachments);
            } else {
                self.request(
                    "thread/resume",
                    thread_resume_params(&thread_id),
                    PendingKind::ResumeAndSend {
                        thread_id,
                        prompt,
                        attachments,
                    },
                );
            }
            self.render_connection();
        } else {
            let preferences = self.stored.borrow().preferences.clone();
            let mut params = Map::new();
            params.insert("cwd".into(), Value::String(self.current_cwd_string()));
            let approval_policy = Value::String(selected_id(
                &self.widgets.approval_combo,
                &preferences.approval_policy,
            ));
            params.insert("approvalPolicy".into(), approval_policy.clone());
            let sandbox = selected_id(&self.widgets.sandbox_combo, &preferences.sandbox);
            params.insert("sandbox".into(), Value::String(sandbox.clone()));
            let selected_model = selected_id(&self.widgets.model_combo, &preferences.model);
            let selected_effort =
                selected_id(&self.widgets.effort_combo, &preferences.reasoning_effort);
            let mut routing_state = TaskRoutingState::new(routing_mode_for_model(&selected_model));
            routing_state.allow_subagents = self.widgets.subagents_toggle.is_active();
            if routing::is_delegate_mode(&routing_state.mode) {
                routing_state.delegate_effort = Some(selected_effort.clone());
            }
            let settings = TaskRuntimeSettings {
                model: resolved_model_id(&self.state.borrow().models, &selected_model),
                reasoning_effort: Some(codex_effort_for_backend(&selected_model, &selected_effort)),
                service_tier: match selected_id(
                    &self.widgets.speed_combo,
                    &preferences.service_tier,
                )
                .as_str()
                {
                    "priority" => Some("priority".into()),
                    _ => None,
                },
                sandbox_policy: sandbox_policy_for_selection(&sandbox, &Value::Null),
                approval_policy,
            };
            params.extend(thread_start_runtime_params(&settings));
            // Every new native task uses projection-backed history. Legacy
            // rollouts remain readable, but they are never created again here.
            params.insert("historyMode".into(), Value::String("paginated".into()));
            self.request(
                "thread/start",
                Value::Object(params),
                PendingKind::StartThread {
                    prompt,
                    attachments,
                    settings,
                    routing: Box::new(routing_state),
                },
            );
            self.render_connection();
        }
    }

    fn restore_composer_draft(&self, prompt: &str, attachments: &[String]) {
        let buffer = self.widgets.composer.buffer();
        let current = buffer
            .text(&buffer.start_iter(), &buffer.end_iter(), false)
            .to_string();
        if current.trim().is_empty() {
            buffer.set_text(prompt);
        } else if current.trim() != prompt.trim() {
            buffer.set_text(&format!("{prompt}\n\n{current}"));
        }
        let mut selected = self.attachments.borrow_mut();
        for attachment in attachments {
            let path = PathBuf::from(attachment);
            if !selected.iter().any(|existing| existing == &path) {
                selected.push(path);
            }
        }
        drop(selected);
        self.render_attachments();
    }

    fn offer_memory_safe_continuation(
        &self,
        thread_id: &str,
        original_prompt: Option<&str>,
        attachments: &[String],
    ) {
        let (title, cwd) = self
            .state
            .borrow()
            .threads
            .get(thread_id)
            .map(|thread| (thread.title().to_owned(), thread.cwd.clone()))
            .unwrap_or_else(|| ("Legacy task".into(), self.current_cwd_string()));
        let original_prompt = original_prompt.map(str::to_owned);
        let attachments = attachments.to_vec();
        let dialog = adw::AlertDialog::new(
            Some("Legacy task needs bounded recovery"),
            Some(
                "Codex could not prove a complete resumable checkpoint and turn-context pair inside its bounded safety window. The original task and its raw history remain unchanged. You can keep it selected or prepare a new paginated continuation that reconstructs current state from the workspace; nothing is sent until you review and send it.",
            ),
        );
        dialog.add_responses(&[
            ("cancel", "Keep original task"),
            ("continue", "Prepare safe continuation"),
        ]);
        dialog.set_default_response(Some("continue"));
        dialog.set_close_response("cancel");
        let weak = self.weak_self.borrow().clone();
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            move |response| {
                if response.as_str() != "continue" {
                    return;
                }
                with_controller(&weak, |controller| {
                    controller.new_task();
                    let mut prompt = format!(
                        "Continue the task \"{title}\" in {cwd}. Its legacy transcript could not be reconstructed within the bounded resume safety window. First inspect the current workspace changes through the GitHub plugin or the user's preferred tool, then inspect running services and relevant files to reconstruct authoritative current state. Do not redo completed work or delete evidence solely because it is old."
                    );
                    if let Some(original_prompt) = original_prompt.as_deref()
                        && !original_prompt.trim().is_empty()
                    {
                        prompt.push_str("\n\nCurrent request:\n");
                        prompt.push_str(original_prompt);
                    }
                    controller.widgets.composer.buffer().set_text(&prompt);
                    let mut selected = controller.attachments.borrow_mut();
                    for attachment in &attachments {
                        let path = PathBuf::from(attachment);
                        if !selected.iter().any(|existing| existing == &path) {
                            selected.push(path);
                        }
                    }
                    drop(selected);
                    controller.render_attachments();
                    controller.widgets.composer.grab_focus();
                });
            },
        );
    }

    fn start_turn(&self, thread_id: &str, prompt: String, attachments: Vec<String>) {
        let (preferences, lean_context_payload, lean_context_marker) = {
            let stored = self.stored.borrow();
            (
                stored.preferences.clone(),
                stored.lean_context_payload(thread_id).map(str::to_owned),
                stored
                    .context_checkpoints
                    .get(thread_id)
                    .map(|receipt| receipt.id.clone())
                    .unwrap_or_else(|| "enabled".into()),
            )
        };
        let fallback_settings = TaskRuntimeSettings {
            model: resolved_model_id(
                &self.state.borrow().models,
                &selected_id(&self.widgets.model_combo, &preferences.model),
            ),
            reasoning_effort: Some(codex_effort_for_backend(
                &selected_id(&self.widgets.model_combo, &preferences.model),
                &selected_id(&self.widgets.effort_combo, &preferences.reasoning_effort),
            )),
            service_tier: match selected_id(&self.widgets.speed_combo, &preferences.service_tier)
                .as_str()
            {
                "priority" => Some("priority".into()),
                _ => None,
            },
            sandbox_policy: sandbox_policy_for_selection(
                &selected_id(&self.widgets.sandbox_combo, &preferences.sandbox),
                &Value::Null,
            ),
            approval_policy: Value::String(selected_id(
                &self.widgets.approval_combo,
                &preferences.approval_policy,
            )),
        };
        let task_settings = {
            let mut state = self.state.borrow_mut();
            state
                .task_runtime_settings
                .entry(thread_id.to_owned())
                .or_insert(fallback_settings)
                .clone()
        };
        let routing_state = self
            .stored
            .borrow()
            .task_routing
            .get(thread_id)
            .cloned()
            .unwrap_or_default();
        if routing_state.allow_subagents && routing::is_direct_delegate_mode(&routing_state.mode) {
            self.start_buddy_turn(thread_id, prompt, attachments, &routing_state);
            return;
        }
        let was_native_buddy = self
            .stored
            .borrow_mut()
            .native_buddy_threads
            .remove(thread_id);
        if was_native_buddy {
            self.persist();
        }
        if let Some(thread) = self.state.borrow_mut().threads.get_mut(thread_id) {
            // A GPT turn materializes this thread in the app server.
            thread.native_buddy_only = false;
        }
        let route = if !routing_state.allow_subagents {
            route_from_runtime_settings(&task_settings)
        } else if routing::normalize_mode(&routing_state.mode) == MODE_QWEN {
            let delegate_effort = routing_state.delegate_effort.as_deref().unwrap_or("xhigh");
            let mut route = routing::qwen_assist_route(BACKEND_QWEN, delegate_effort, None);
            route.local_route = Some(crate::qwen::explicit_qwen_route(&prompt).label().to_owned());
            route
        } else if routing::normalize_mode(&routing_state.mode) == MODE_GEMINI {
            let delegate_effort = routing_state.delegate_effort.as_deref().unwrap_or("medium");
            let mut route = routing::qwen_assist_route(BACKEND_GEMINI, delegate_effort, None);
            route.local_route = Some(crate::qwen::AutomaticQwenRoute::Gemini.label().to_owned());
            route
        } else if routing::normalize_mode(&routing_state.mode) == MODE_OPENROUTER {
            let delegate_effort = routing_state.delegate_effort.as_deref().unwrap_or("medium");
            routing::qwen_assist_route(BACKEND_OPENROUTER, delegate_effort, None)
        } else if routing::normalize_mode(&routing_state.mode) == MODE_MISTRAL {
            let delegate_effort = routing_state.delegate_effort.as_deref().unwrap_or("high");
            routing::qwen_assist_route(BACKEND_MISTRAL, delegate_effort, None)
        } else if routing::is_auto_mode(&routing_state.mode) {
            let route = routing::qwen_assist_route(
                &task_settings.model,
                task_settings
                    .reasoning_effort
                    .as_deref()
                    .unwrap_or("medium"),
                task_settings.service_tier.as_deref(),
            );
            self.decorate_route_with_qwen(route, &routing_state.mode, &prompt, attachments.len())
        } else {
            route_from_runtime_settings(&task_settings)
        };
        let route = routing::enforce_subagent_policy(route, routing_state.allow_subagents);
        let mut params = Map::new();
        params.insert("threadId".into(), Value::String(thread_id.to_owned()));
        params.insert(
            "input".into(),
            Value::Array(make_inputs(&prompt, &attachments)),
        );
        params.insert("cwd".into(), Value::String(self.current_cwd_string()));
        params.insert(
            "effort".into(),
            Value::String(task_settings.reasoning_effort.clone().unwrap_or_else(|| {
                selected_id(&self.widgets.effort_combo, &preferences.reasoning_effort)
            })),
        );
        params.insert(
            "serviceTier".into(),
            task_settings
                .service_tier
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
        params.insert(
            "approvalPolicy".into(),
            task_settings.approval_policy.clone(),
        );
        params.insert("sandboxPolicy".into(), task_settings.sandbox_policy.clone());
        let model = Some(task_settings.model.clone())
            .filter(|model| !model.is_empty())
            .unwrap_or_else(|| selected_id(&self.widgets.model_combo, &preferences.model));
        if !model.is_empty() {
            params.insert("model".into(), Value::String(model));
        }
        let mut additional_context = Map::new();
        let mut client_metadata = Map::new();
        if let Some(lean_context_payload) = lean_context_payload {
            additional_context.insert(
                "codex-native.lean-context".into(),
                json!({
                    "kind": "application",
                    "value": lean_context_payload
                }),
            );
            client_metadata.insert("lean_context".into(), Value::String(lean_context_marker));
        }
        if let Some(qwen_route) = route
            .local_route
            .as_deref()
            .and_then(crate::qwen::AutomaticQwenRoute::from_label)
            && let Some(mut qwen_context) = self.qwen_routing_context(qwen_route)
        {
            if routing::normalize_mode(&routing_state.mode) == MODE_QWEN {
                let effort = routing_state.delegate_effort.as_deref().unwrap_or("high");
                qwen_context.push_str(&format!(
                    " The user explicitly selected OpenCode with local Qwen and thinking_effort={effort}. Use only local_qwen_agent with thinking_effort={effort}; poll local_qwen_agent_status until completion."
                ));
            } else if routing::normalize_mode(&routing_state.mode) == MODE_GEMINI {
                let effort = routing_state.delegate_effort.as_deref().unwrap_or("medium");
                qwen_context.push_str(&format!(
                    " The user explicitly selected Gemini with effort={effort}; pass that effort to gemini_buddy_delegate."
                ));
            } else if routing::normalize_mode(&routing_state.mode) == MODE_OPENROUTER {
                let effort = routing_state.delegate_effort.as_deref().unwrap_or("medium");
                qwen_context.push_str(&format!(
                    " The user explicitly selected OpenRouter Free with effort={effort}."
                ));
            } else if routing::normalize_mode(&routing_state.mode) == MODE_MISTRAL {
                let effort = routing_state.delegate_effort.as_deref().unwrap_or("xhigh");
                qwen_context.push_str(&format!(
                    " The user explicitly selected Mistral AI with effort={effort}."
                ));
            }
            additional_context.insert(
                "codex-native.qwen-buddy-routing".into(),
                json!({
                    "kind": "application",
                    "value": qwen_context
                }),
            );
            client_metadata.insert(
                "qwen_buddy_routing".into(),
                Value::String(qwen_route.label().into()),
            );
        } else if routing::normalize_mode(&routing_state.mode) == MODE_QWEN_ASSIST
            && preferences.qwen_buddy.routing_enabled
        {
            if preferences.qwen_buddy.gpu_guard && self.qwen_gpu_blocked() {
                self.toast("Qwen routing paused by the GPU guard; Codex is handling this turn");
            } else if !self.qwen_plugin_ready_or_loading() {
                self.toast("Qwen routing is enabled but its MCP server is unavailable; Codex is handling this turn");
            }
        }
        if !additional_context.is_empty() {
            params.insert(
                "additionalContext".into(),
                Value::Object(additional_context),
            );
        }
        client_metadata.insert(
            "codex_native_route_mode".into(),
            if routing_state.allow_subagents {
                routing_state.mode.clone()
            } else {
                MODE_MANUAL.into()
            }
            .into(),
        );
        client_metadata.insert(
            "codex_native_subagents_enabled".into(),
            subagents_enabled_metadata_value(routing_state.allow_subagents),
        );
        client_metadata.insert(
            "codex_native_route_model".into(),
            route.model.clone().into(),
        );
        client_metadata.insert(
            "codex_native_route_effort".into(),
            route.effort.clone().into(),
        );
        client_metadata.insert(
            "codex_native_route_speed".into(),
            route.service_tier.clone().into(),
        );
        params.insert(
            "responsesapiClientMetadata".into(),
            Value::Object(client_metadata),
        );
        self.request(
            "turn/start",
            Value::Object(params),
            PendingKind::SendTurn {
                thread_id: thread_id.to_owned(),
                prompt,
                attachments,
                route,
            },
        );
    }

    fn start_buddy_turn(
        &self,
        thread_id: &str,
        prompt: String,
        attachments: Vec<String>,
        routing_state: &TaskRoutingState,
    ) {
        let mode = routing::normalize_mode(&routing_state.mode);
        let (backend, author, default_effort) = match mode {
            MODE_GEMINI => ("gemini", "Gemini", "medium"),
            MODE_OPENROUTER => ("openrouter", "OpenRouter Free", "medium"),
            MODE_MISTRAL => ("mistral", "Mistral AI", "high"),
            _ => ("qwen", "Qwen", "none"),
        };
        let effort = routing_state
            .delegate_effort
            .as_deref()
            .unwrap_or(default_effort)
            .to_owned();
        let mut buddy_prompt = prompt.clone();
        if !attachments.is_empty() {
            buddy_prompt
                .push_str("\n\nAttached local paths available inside the authorized workspace:\n");
            buddy_prompt.push_str(
                &attachments
                    .iter()
                    .map(|path| format!("- {path}"))
                    .collect::<Vec<_>>()
                    .join("\n"),
            );
        }
        let turn_id = format!("buddy-{}", uuid::Uuid::new_v4());
        let item_id = format!("{turn_id}-user");
        let now = chrono::Utc::now().timestamp();
        let buddy_cwd = self.current_cwd_string();
        let context = self
            .state
            .borrow()
            .threads
            .get(thread_id)
            .map(buddy_thread_context)
            .unwrap_or_default();
        {
            let mut state = self.state.borrow_mut();
            let Some(thread) = state.threads.get_mut(thread_id) else {
                self.restore_composer_draft(&prompt, &attachments);
                self.toast("The selected task is no longer available");
                return;
            };
            if thread.preview.trim().is_empty() {
                thread.preview = compact_ui_text(&prompt, 120);
            }
            if thread.path.is_none()
                && thread
                    .turns
                    .iter()
                    .all(|turn| turn.id.starts_with("buddy-"))
            {
                thread.native_buddy_only = true;
            }
            thread.status = json!({"type": "active", "activeFlags": []});
            let buddy_turn = Turn {
                id: turn_id.clone(),
                items: vec![
                    json!({
                        "id": item_id,
                        "type": "userMessage",
                        "content": make_inputs(&prompt, &attachments),
                        "cwd": buddy_cwd.clone(),
                    }),
                    buddy_activity_item(&turn_id, author, &effort),
                ],
                status: json!({"type": "inProgress"}),
                started_at: Some(now),
                items_view: "full".into(),
                ..Turn::default()
            };
            thread.turns.push(buddy_turn.clone());
            state.external_running_threads.insert(thread_id.to_owned());
            state.turn_progress.insert(
                thread_id.to_owned(),
                TurnProgress {
                    turn_id: turn_id.clone(),
                    plan: buddy_progress_plan(backend, "starting", "Preparing the request"),
                    diff: String::new(),
                },
            );
            state.mark_transcript_changed();
            drop(state);
            let mut stored = self.stored.borrow_mut();
            upsert_buddy_turn(&mut stored, thread_id, buddy_turn);
            stored.native_buddy_threads.insert(thread_id.to_owned());
        }
        self.persist();
        self.render_threads();
        self.render_transcript();
        self.render_connection();
        self.hub.host(HostAction::BuddyDelegate {
            thread_id: thread_id.to_owned(),
            turn_id,
            backend: backend.into(),
            prompt: buddy_prompt,
            context,
            effort,
            cwd: PathBuf::from(buddy_cwd),
            access: selected_id(&self.widgets.sandbox_combo, "read-only"),
        });
        self.toast(&format!("{author} is working without Codex usage"));
    }

    fn finish_buddy_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
        author: &str,
        result: Result<&str, &str>,
    ) {
        let now = chrono::Utc::now().timestamp();
        let mut state = self.state.borrow_mut();
        state.external_running_threads.remove(thread_id);
        state.turn_progress.remove(thread_id);
        let mut durable_turn = None;
        if let Some(thread) = state.threads.get_mut(thread_id) {
            thread.status = json!({"type": "idle"});
            if let Some(turn) = thread.turns.iter_mut().find(|turn| turn.id == turn_id) {
                turn.completed_at = Some(now);
                match result {
                    Ok(text) => {
                        turn.items.push(json!({
                            "id": format!("{turn_id}-assistant"),
                            "type": "agentMessage",
                            "author": author,
                            "text": text,
                        }));
                        turn.status = json!({"type": "completed"});
                    }
                    Err(message) => {
                        turn.status = json!({"type": "failed"});
                        turn.error = Some(json!({"message": message}));
                    }
                }
                durable_turn = Some(turn.clone());
            }
        }
        state.mark_transcript_changed();
        drop(state);
        if let Some(turn) = durable_turn {
            let mut stored = self.stored.borrow_mut();
            upsert_buddy_turn(&mut stored, thread_id, turn);
            drop(stored);
            self.persist();
        }
        self.render_threads();
        self.render_transcript();
        self.render_connection();
    }

    fn update_buddy_progress(
        &self,
        thread_id: &str,
        turn_id: &str,
        backend: &str,
        progress: &crate::host::BuddyProgress,
    ) {
        let mut state = self.state.borrow_mut();
        if !state.external_running_threads.contains(thread_id)
            || !state
                .turn_progress
                .get(thread_id)
                .is_some_and(|progress| progress.turn_id == turn_id)
        {
            return;
        }
        state.turn_progress.insert(
            thread_id.to_owned(),
            TurnProgress {
                turn_id: turn_id.to_owned(),
                plan: buddy_progress_plan(backend, &progress.phase, &progress.detail),
                diff: String::new(),
            },
        );
        let existing_usage = state.thread_token_usage.get(thread_id).cloned();
        let activity = state
            .threads
            .get_mut(thread_id)
            .and_then(|thread| thread.turns.iter_mut().find(|turn| turn.id == turn_id))
            .map(|turn| {
                let previous_turn_tokens =
                    update_buddy_activity(turn, buddy_author(backend), progress);
                (turn.clone(), previous_turn_tokens)
            });
        let usage = activity.as_ref().and_then(|(_, previous_turn_tokens)| {
            buddy_usage_from_progress(
                existing_usage.as_ref(),
                *previous_turn_tokens,
                buddy_author(backend),
                progress,
            )
        });
        if let Some(usage) = usage.as_ref() {
            state
                .thread_token_usage
                .insert(thread_id.to_owned(), usage.clone());
        }
        state.mark_transcript_changed();
        drop(state);
        if let Some((turn, _)) = activity {
            let mut stored = self.stored.borrow_mut();
            upsert_buddy_turn(&mut stored, thread_id, turn);
            if let Some(usage) = usage {
                stored.buddy_token_usage.insert(thread_id.to_owned(), usage);
            }
            drop(stored);
            self.persist();
        }
        self.schedule_transcript_render();
        self.render_connection();
    }

    fn record_completed_buddy_usage(
        &self,
        thread_id: &str,
        turn_id: &str,
        backend: &str,
        metrics: Option<&Value>,
    ) {
        let Some(progress) = buddy_completion_progress(metrics, backend) else {
            return;
        };
        let mut state = self.state.borrow_mut();
        let existing_usage = state.thread_token_usage.get(thread_id).cloned();
        let activity = state
            .threads
            .get_mut(thread_id)
            .and_then(|thread| thread.turns.iter_mut().find(|turn| turn.id == turn_id))
            .map(|turn| {
                let previous_turn_tokens =
                    update_buddy_activity(turn, buddy_author(backend), &progress);
                (turn.clone(), previous_turn_tokens)
            });
        let usage = activity.as_ref().and_then(|(_, previous_turn_tokens)| {
            buddy_usage_from_progress(
                existing_usage.as_ref(),
                *previous_turn_tokens,
                buddy_author(backend),
                &progress,
            )
        });
        if let Some(usage) = usage.as_ref() {
            state
                .thread_token_usage
                .insert(thread_id.to_owned(), usage.clone());
        }
        drop(state);
        if let Some((turn, _)) = activity {
            let mut stored = self.stored.borrow_mut();
            upsert_buddy_turn(&mut stored, thread_id, turn);
            if let Some(usage) = usage {
                stored.buddy_token_usage.insert(thread_id.to_owned(), usage);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn record_external_buddy_savings(
        &self,
        thread_id: &str,
        turn_id: &str,
        backend: &str,
        effort: &str,
        prompt: &str,
        context: &str,
        output: &str,
        metrics: Option<&Value>,
    ) {
        let measured_prompt = metrics
            .and_then(|value| value.get("promptTokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let measured_output = metrics
            .and_then(|value| value.get("outputTokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let measured = measured_prompt > 0 || measured_output > 0;
        let prompt_tokens = if measured {
            measured_prompt
        } else {
            approximate_tokens(prompt).saturating_add(approximate_tokens(context))
        };
        let output_tokens = if measured {
            measured_output
        } else {
            approximate_tokens(output)
        };
        let model_calls = metrics
            .and_then(|value| value.get("modelCalls"))
            .and_then(Value::as_u64)
            .unwrap_or(1)
            .min(u64::from(u32::MAX)) as u32;
        let provider = buddy_author(backend);
        let mut evidence = routing::QwenSavingsEvidence::default();
        evidence.record(
            provider,
            true,
            model_calls,
            prompt_tokens,
            output_tokens,
            prompt_tokens.saturating_add(output_tokens),
        );
        let mut route = routing::manual_route(backend, effort, None);
        route.reason = format!("{provider} handled this turn outside GPT quota");
        let receipt = routing::estimate_token_savings_with_qwen(
            &route,
            None,
            if measured {
                "provider-reported non-GPT token usage"
            } else {
                "bounded text-length estimate because provider usage was unavailable"
            },
            &evidence,
        );
        let mut stored = self.stored.borrow_mut();
        let routing_state = stored.task_routing.entry(thread_id.to_owned()).or_default();
        routing_state.record_turn(turn_id, route);
        if !routing_state.record_token_savings(turn_id, receipt) {
            return;
        }
        drop(stored);
        self.persist();
        self.state.borrow_mut().mark_transcript_changed();
        self.schedule_transcript_render();
    }

    fn restore_all_buddy_history(&self) {
        let (thread_ids, token_usage) = {
            let stored = self.stored.borrow();
            (
                stored.buddy_turns.keys().cloned().collect::<Vec<_>>(),
                stored.buddy_token_usage.clone(),
            )
        };
        for thread_id in thread_ids {
            self.restore_buddy_history(&thread_id);
        }
        let mut state = self.state.borrow_mut();
        for (thread_id, usage) in token_usage {
            state.thread_token_usage.entry(thread_id).or_insert(usage);
        }
    }

    fn restore_buddy_history(&self, thread_id: &str) {
        let (saved, native_buddy_only) = {
            let stored = self.stored.borrow();
            (
                stored
                    .buddy_turns
                    .get(thread_id)
                    .cloned()
                    .unwrap_or_default(),
                stored.native_buddy_threads.contains(thread_id),
            )
        };
        if saved.is_empty() {
            return;
        }
        let cwd = self.current_cwd_string();
        let mut state = self.state.borrow_mut();
        if !state.threads.contains_key(thread_id) {
            state.upsert_thread(restored_buddy_thread(thread_id, saved, cwd));
            state.mark_transcript_changed();
            return;
        }
        let Some(thread) = state.threads.get_mut(thread_id) else {
            return;
        };
        thread.native_buddy_only = native_buddy_only;
        let mut changed = false;
        for turn in saved {
            if !thread.turns.iter().any(|existing| existing.id == turn.id) {
                thread.turns.push(turn);
                changed = true;
            }
        }
        if changed {
            thread.turns.sort_by(|left, right| {
                left.started_at
                    .cmp(&right.started_at)
                    .then_with(|| left.id.cmp(&right.id))
            });
            state.mark_transcript_changed();
        }
    }

    fn interrupt_turn(&self) {
        let Some((thread_id, turn_id)) = cloned_active_turn_target(self.state.as_ref()) else {
            return;
        };
        self.request(
            "turn/interrupt",
            json!({"threadId": thread_id, "turnId": turn_id}),
            PendingKind::Generic,
        );
    }

    fn paste_composer_clipboard(&self) -> bool {
        if let Some(texture) = self.smoke_clipboard_texture.borrow().clone() {
            self.attach_clipboard_texture(&texture);
            return true;
        }
        let clipboard = self.widgets.composer.clipboard();
        let formats = clipboard.formats();
        tracing::debug!(?formats, "inspecting composer clipboard");
        if formats.contains_type(gdk::FileList::static_type()) {
            let weak = self.weak_self.borrow().clone();
            clipboard.read_value_async(
                gdk::FileList::static_type(),
                glib::Priority::DEFAULT,
                None::<&gio::Cancellable>,
                move |result| {
                    with_controller(&weak, |controller| match result {
                        Ok(value) => match value.get::<gdk::FileList>() {
                            Ok(files) => {
                                let paths = files
                                    .files()
                                    .into_iter()
                                    .filter_map(|file| file.path())
                                    .collect::<Vec<_>>();
                                let added = controller.attach_paths(paths);
                                if added > 0 {
                                    controller.toast(&format!(
                                        "Pasted {added} attachment{}",
                                        if added == 1 { "" } else { "s" }
                                    ));
                                } else {
                                    controller.toast("The copied files are not available locally");
                                }
                            }
                            Err(error) => {
                                controller.toast(&format!("Could not paste files: {error}"));
                            }
                        },
                        Err(error) => controller.toast(&format!("Could not paste files: {error}")),
                    });
                },
            );
            return true;
        }
        if !clipboard_formats_contain_image(&formats) {
            return false;
        }

        let weak = self.weak_self.borrow().clone();
        if formats.contains_type(gdk::Texture::static_type()) {
            clipboard.read_value_async(
                gdk::Texture::static_type(),
                glib::Priority::DEFAULT,
                None::<&gio::Cancellable>,
                move |result| {
                    with_controller(&weak, |controller| match result {
                        Ok(value) => match value.get::<gdk::Texture>() {
                            Ok(texture) => controller.attach_clipboard_texture(&texture),
                            Err(error) => {
                                controller.toast(&format!("Could not paste image: {error}"));
                            }
                        },
                        Err(error) => {
                            tracing::warn!(%error, "could not read native clipboard texture");
                            controller.toast(&format!("Could not paste image: {error}"));
                        }
                    });
                },
            );
            return true;
        }

        let weak = self.weak_self.borrow().clone();
        clipboard.read_texture_async(None::<&gio::Cancellable>, move |result| {
            with_controller(&weak, |controller| match result {
                Ok(Some(texture)) => controller.attach_clipboard_texture(&texture),
                Ok(None) => controller.toast("The clipboard did not contain a readable image"),
                Err(error) => {
                    tracing::warn!(%error, "could not read clipboard image");
                    controller.toast(&format!("Could not paste image: {error}"));
                }
            });
        });
        true
    }

    fn attach_clipboard_texture(&self, texture: &gdk::Texture) {
        match clipboard_image_path() {
            Ok(path) => match texture.save_to_png(&path) {
                Ok(()) => {
                    if self.attach_paths([path]) > 0 {
                        self.toast("Image pasted into the next message");
                    }
                }
                Err(error) => self.toast(&format!("Could not save pasted image: {error}")),
            },
            Err(error) => self.toast(&format!("Could not prepare pasted image: {error}")),
        }
    }

    fn attach_paths(&self, paths: impl IntoIterator<Item = PathBuf>) -> usize {
        let mut attachments = self.attachments.borrow_mut();
        let before = attachments.len();
        for path in paths {
            if path.is_file() && !attachments.iter().any(|candidate| candidate == &path) {
                attachments.push(path);
            }
        }
        let added = attachments.len() - before;
        drop(attachments);
        self.render_attachments();
        self.widgets.composer.grab_focus();
        added
    }

    fn choose_attachments(&self) {
        let dialog = gtk::FileDialog::builder()
            .title("Attach files or images")
            .modal(true)
            .build();
        let window = self.widgets.window.clone();
        let attachments = self.attachments.clone();
        let label = self.widgets.attachments_label.clone();
        dialog.open_multiple(Some(&window), None::<&gio::Cancellable>, move |result| {
            let Ok(files) = result else { return };
            let mut selected = attachments.borrow_mut();
            for index in 0..files.n_items() {
                if let Some(file) = files.item(index).and_downcast::<gio::File>()
                    && let Some(path) = file.path()
                    && !selected.contains(&path)
                {
                    selected.push(path);
                }
            }
            let names = selected
                .iter()
                .filter_map(|path| path.file_name().and_then(|name| name.to_str()))
                .collect::<Vec<_>>()
                .join(", ");
            label.set_label(&format!("Attached: {names}"));
            label.set_visible(!selected.is_empty());
        });
    }

    fn capture_dictation(&self) {
        if self.state.borrow().active_thread_id.is_none() {
            self.toast("Start or open a task before using dictation");
            return;
        }
        self.widgets.dictate_button.set_sensitive(false);
        self.toast("Recording microphone for twelve seconds…");
        self.hub.host(HostAction::VoiceCapture { seconds: 12 });
    }

    fn read_latest_aloud(&self) {
        let state = self.state.borrow();
        let text = state.active_thread().and_then(|thread| {
            thread
                .turns
                .iter()
                .rev()
                .flat_map(|turn| turn.items.iter().rev())
                .find(|item| item.get("type").and_then(Value::as_str) == Some("agentMessage"))
                .and_then(|item| item.get("text").and_then(Value::as_str))
                .map(str::to_owned)
        });
        drop(state);
        let Some(mut text) = text else {
            self.toast("There is no Codex message to read");
            return;
        };
        text.truncate(text.floor_char_boundary(20_000));
        self.hub.host(HostAction::Speak(text));
    }

    fn render_attachments(&self) {
        let attachments = self.attachments.borrow();
        let names = attachments
            .iter()
            .filter_map(|path| path.file_name().and_then(|name| name.to_str()))
            .collect::<Vec<_>>()
            .join(", ");
        self.widgets
            .attachments_label
            .set_label(&format!("Attached: {names}"));
        self.widgets
            .attachments_label
            .set_visible(!attachments.is_empty());
    }

    fn project_changed(&self) {
        let Some(path) = self.widgets.project_combo.active_id().map(PathBuf::from) else {
            return;
        };
        let mut stored = self.stored.borrow_mut();
        let mut added = false;
        if !stored.projects.iter().any(|project| project.path == path) {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("Project")
                .to_owned();
            stored.projects.push(Project {
                name,
                path: path.clone(),
                additional_paths: Vec::new(),
                remote_host: None,
            });
            added = true;
        }
        stored.last_project = Some(path);
        let macro_executor = stored.preferences.macro_executor.clone();
        if let Err(error) = persistence::save_state(&stored) {
            self.toast(&format!("Could not save project: {error}"));
        }
        drop(stored);
        if added && macro_executor.enabled {
            self.configure_macro_executor(&macro_executor);
        }
        if self.state.borrow().connection == ConnectionState::Ready {
            self.refresh_threads();
            self.refresh_extension_runtime();
        }
        let page = self.state.borrow().page;
        match page {
            WorkspacePage::Projects => self.render_projects(),
            WorkspacePage::Sites => self.render_sites(),
            _ => {}
        }
    }

    fn start_sites_task(&self) {
        self.new_task();
        self.widgets.composer.buffer().set_text(
            "Help me build and deploy a website with Sites in this project. Ask me what the site should include before changing files.",
        );
        self.widgets.composer.grab_focus();
        self.toast("Sites task prepared; review the prompt before sending");
    }

    fn open_sites_plugin(&self) {
        self.widgets.extensions_search.set_text("Sites");
        let installed = self.state.borrow().plugins.find_entry(|entry| {
            (entry.summary.name.eq_ignore_ascii_case("sites")
                || entry.display_name().eq_ignore_ascii_case("sites"))
                && entry.summary.installed
        });
        if installed.is_none() {
            self.widgets.extensions_installed_only.set_active(false);
        }
        self.widgets.stack.set_visible_child_name("extensions");
    }

    fn current_cwd_string(&self) -> String {
        self.widgets
            .project_combo
            .active_id()
            .map(|value| value.to_string())
            .or_else(|| {
                self.stored
                    .borrow()
                    .last_project
                    .as_ref()
                    .map(|path| path.to_string_lossy().into_owned())
            })
            .or_else(|| {
                env::current_dir()
                    .ok()
                    .map(|path| path.to_string_lossy().into_owned())
            })
            .unwrap_or_else(|| "/".into())
    }

    fn store_task_runtime_settings(&self, value: &Value) -> Option<String> {
        let (thread_id, settings) = task_runtime_settings_from_server(value)?;
        self.state
            .borrow_mut()
            .task_runtime_settings
            .insert(thread_id.clone(), settings);
        Some(thread_id)
    }

    fn apply_active_task_settings(&self) {
        self.apply_subagents_toggle();
        let (settings, selected_model) = {
            let state = self.state.borrow();
            let settings = state
                .active_thread_id
                .as_ref()
                .and_then(|thread_id| state.task_runtime_settings.get(thread_id).cloned());
            let selected = state.active_thread_id.as_ref().and_then(|thread_id| {
                settings.as_ref().map(|settings| {
                    selected_model_for_task(
                        &settings.model,
                        self.stored.borrow().task_routing.get(thread_id),
                    )
                })
            });
            (settings, selected)
        };
        let Some(settings) = settings else {
            self.apply_routing_controls();
            return;
        };

        let previous_guard = self.updating_task_controls.replace(true);
        if !self
            .widgets
            .model_combo
            .set_active_id(selected_model.as_deref())
        {
            let selected_model = selected_model.unwrap_or_else(|| settings.model.clone());
            self.widgets.model_combo.append(
                Some(&selected_model),
                &format!("Current · {selected_model}"),
            );
            self.widgets
                .model_combo
                .set_active_id(Some(&selected_model));
        }
        self.populate_efforts();
        let displayed_effort = self
            .state
            .borrow()
            .active_thread_id
            .as_ref()
            .and_then(|thread_id| {
                self.stored
                    .borrow()
                    .task_routing
                    .get(thread_id)
                    .and_then(|routing| routing.delegate_effort.clone())
            })
            .or_else(|| settings.reasoning_effort.clone());
        if let Some(effort) = displayed_effort.as_deref()
            && !self.widgets.effort_combo.set_active_id(Some(effort))
        {
            let label = REASONING_EFFORT_OPTIONS
                .iter()
                .find_map(|(id, label)| (*id == effort).then_some(*label))
                .unwrap_or(effort);
            self.widgets.effort_combo.append(Some(effort), label);
            self.widgets.effort_combo.set_active_id(Some(effort));
        }
        self.populate_service_tiers();
        let speed = settings.service_tier.as_deref().unwrap_or("standard");
        if !self.widgets.speed_combo.set_active_id(Some(speed)) {
            // Unsupported or legacy server values are the normal Standard
            // tier; never add a third, raw protocol value to this picker.
            self.widgets.speed_combo.set_active_id(Some("standard"));
        }
        set_combo(
            &self.widgets.sandbox_combo,
            sandbox_control_id(&settings.sandbox_policy),
        );
        set_combo(
            &self.widgets.approval_combo,
            approval_control_id(&settings.approval_policy),
        );
        self.update_task_control_accessibility();
        self.updating_task_controls.set(previous_guard);
        self.apply_routing_controls();
    }

    fn apply_new_task_defaults(&self) {
        let preferences = self.stored.borrow().preferences.clone();
        self.apply_subagents_toggle();
        let previous_guard = self.updating_task_controls.replace(true);
        set_combo(&self.widgets.model_combo, &preferences.model);
        self.populate_efforts();
        set_combo(&self.widgets.effort_combo, &preferences.reasoning_effort);
        self.populate_service_tiers();
        set_combo(&self.widgets.speed_combo, &preferences.service_tier);
        set_combo(&self.widgets.sandbox_combo, &preferences.sandbox);
        set_combo(&self.widgets.approval_combo, &preferences.approval_policy);
        self.update_task_control_accessibility();
        self.updating_task_controls.set(previous_guard);
        self.apply_routing_controls();
    }

    fn apply_subagents_toggle(&self) {
        let enabled = {
            let thread_id = self.state.borrow().active_thread_id.clone();
            thread_id
                .and_then(|thread_id| {
                    self.stored
                        .borrow()
                        .task_routing
                        .get(&thread_id)
                        .map(|routing| routing.allow_subagents)
                })
                .unwrap_or(true)
        };
        let previous_guard = self.updating_task_controls.replace(true);
        self.widgets.subagents_toggle.set_active(enabled);
        set_subagents_toggle_appearance(&self.widgets.subagents_toggle, enabled);
        self.updating_task_controls.set(previous_guard);
    }

    fn set_active_task_subagents_enabled(&self, enabled: bool) {
        if self.updating_task_controls.get() {
            return;
        }
        let Some(thread_id) = self.state.borrow().active_thread_id.clone() else {
            return;
        };
        let changed = {
            let mut stored = self.stored.borrow_mut();
            let routing = stored.task_routing.entry(thread_id).or_default();
            if routing.allow_subagents == enabled {
                false
            } else {
                routing.allow_subagents = enabled;
                true
            }
        };
        if changed {
            self.persist();
        }
    }

    fn apply_routing_controls(&self) {
        self.widgets.model_combo.set_sensitive(true);
        self.widgets.effort_combo.set_sensitive(true);
        self.widgets.speed_combo.set_sensitive(true);
    }

    fn record_turn_route(&self, thread_id: &str, turn_id: &str, route: RouteDecision) {
        self.stored
            .borrow_mut()
            .task_routing
            .entry(thread_id.to_owned())
            .or_default()
            .record_turn(turn_id, route);
        self.persist();
        self.state.borrow_mut().mark_transcript_changed();
        self.apply_routing_controls();
        self.schedule_transcript_render();
    }

    fn observe_turn_routing(&self, method: &str, params: &Value) {
        let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
            return;
        };
        if method == "thread/deleted" {
            if self
                .stored
                .borrow_mut()
                .task_routing
                .remove(thread_id)
                .is_some()
            {
                self.persist();
            }
            return;
        }
        if method == "turn/started" {
            let Some(turn_id) = params.pointer("/turn/id").and_then(Value::as_str) else {
                return;
            };
            let already_recorded = self
                .stored
                .borrow()
                .task_routing
                .get(thread_id)
                .and_then(|state| state.route_for_turn(turn_id))
                .is_some();
            if already_recorded {
                return;
            }
            let settings = self
                .state
                .borrow()
                .task_runtime_settings
                .get(thread_id)
                .cloned();
            if let Some(settings) = settings {
                // Remote/iOS turns arrive after their settings have already
                // been chosen. Snapshot the shared runtime truth without
                // pretending Native injected a Qwen subagent requirement.
                self.record_turn_route(thread_id, turn_id, route_from_runtime_settings(&settings));
            }
        }
    }

    fn observe_token_savings(&self, method: &str, params: &Value) {
        let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
            return;
        };
        match method {
            "thread/deleted" => {
                self.token_savings_observations
                    .borrow_mut()
                    .remove(thread_id);
            }
            "turn/started" => {
                let Some(turn_id) = params.pointer("/turn/id").and_then(Value::as_str) else {
                    return;
                };
                let stale = self
                    .token_savings_observations
                    .borrow_mut()
                    .remove(thread_id)
                    .filter(|observation| observation.completed);
                if let Some(stale) = stale {
                    self.store_token_savings_receipt(
                        thread_id,
                        &stale.turn_id,
                        None,
                        "app-server turn telemetry was unavailable",
                    );
                }
                let before_cumulative_tokens = self
                    .state
                    .borrow()
                    .thread_token_usage
                    .get(thread_id)
                    .and_then(cumulative_thread_tokens);
                self.token_savings_observations.borrow_mut().insert(
                    thread_id.to_owned(),
                    TokenSavingsObservation {
                        turn_id: turn_id.to_owned(),
                        before_cumulative_tokens,
                        latest_cumulative_tokens: before_cumulative_tokens,
                        usage_seen: false,
                        completed: false,
                        qwen_event_ids: HashSet::new(),
                        qwen: routing::QwenSavingsEvidence::default(),
                    },
                );
            }
            "item/completed" => {
                let Some(item) = params.get("item") else {
                    return;
                };
                if let Some(observation) = self
                    .token_savings_observations
                    .borrow_mut()
                    .get_mut(thread_id)
                    && crate::qwen::observe_usage_item(
                        item,
                        &mut observation.qwen_event_ids,
                        &mut observation.qwen,
                    )
                {
                    tracing::debug!(
                        thread_id,
                        turn_id = observation.turn_id,
                        qwen_uses = observation.qwen.observed_uses,
                        "recorded hidden Qwen Buddy usage evidence"
                    );
                }
            }
            "thread/tokenUsage/updated" => {
                let latest = params.get("tokenUsage").and_then(cumulative_thread_tokens);
                let completed = {
                    let mut observations = self.token_savings_observations.borrow_mut();
                    let Some(observation) = observations.get_mut(thread_id) else {
                        return;
                    };
                    observation.latest_cumulative_tokens = latest;
                    observation.usage_seen = true;
                    observation.completed.then(|| {
                        (
                            observation.turn_id.clone(),
                            cumulative_turn_delta(observation),
                        )
                    })
                };
                if let Some((turn_id, tokens)) = completed {
                    self.store_token_savings_receipt(
                        thread_id,
                        &turn_id,
                        tokens,
                        "cumulative app-server turn-token delta",
                    );
                }
            }
            "turn/completed" => {
                let event_turn_id = params
                    .pointer("/turn/id")
                    .and_then(Value::as_str)
                    .filter(|turn_id| !turn_id.is_empty())
                    .map(str::to_owned);
                let direct_tokens = turn_last_tokens(params);
                let ready = {
                    let mut observations = self.token_savings_observations.borrow_mut();
                    let observation =
                        observations.entry(thread_id.to_owned()).or_insert_with(|| {
                            TokenSavingsObservation {
                                turn_id: event_turn_id.clone().unwrap_or_default(),
                                before_cumulative_tokens: None,
                                latest_cumulative_tokens: None,
                                usage_seen: false,
                                completed: false,
                                qwen_event_ids: HashSet::new(),
                                qwen: routing::QwenSavingsEvidence::default(),
                            }
                        });
                    for item in params
                        .pointer("/turn/items")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                    {
                        crate::qwen::observe_usage_item(
                            item,
                            &mut observation.qwen_event_ids,
                            &mut observation.qwen,
                        );
                    }
                    if let Some(turn_id) = event_turn_id
                        && !turn_id.is_empty()
                    {
                        observation.turn_id = turn_id;
                    }
                    observation.completed = true;
                    if let Some(tokens) = direct_tokens {
                        Some((
                            observation.turn_id.clone(),
                            Some(tokens),
                            "per-turn app-server telemetry",
                        ))
                    } else if observation.usage_seen {
                        Some((
                            observation.turn_id.clone(),
                            cumulative_turn_delta(observation),
                            "cumulative app-server turn-token delta",
                        ))
                    } else {
                        None
                    }
                };
                if let Some((turn_id, tokens, measurement)) = ready {
                    self.store_token_savings_receipt(thread_id, &turn_id, tokens, measurement);
                    return;
                }
                let turn_id = self
                    .token_savings_observations
                    .borrow()
                    .get(thread_id)
                    .map(|observation| observation.turn_id.clone())
                    .unwrap_or_default();
                let thread_id = thread_id.to_owned();
                let weak = self.weak_self.borrow().clone();
                glib::timeout_add_local_once(Duration::from_millis(750), move || {
                    with_controller(&weak, |controller| {
                        let pending = controller
                            .token_savings_observations
                            .borrow()
                            .get(&thread_id)
                            .is_some_and(|observation| {
                                observation.completed && observation.turn_id == turn_id
                            });
                        if pending {
                            controller.store_token_savings_receipt(
                                &thread_id,
                                &turn_id,
                                None,
                                "app-server turn telemetry was unavailable",
                            );
                        }
                    });
                });
            }
            _ => {}
        }
    }

    fn store_token_savings_receipt(
        &self,
        thread_id: &str,
        turn_id: &str,
        observed_turn_tokens: Option<u64>,
        measurement: &str,
    ) {
        if turn_id.is_empty() {
            self.token_savings_observations
                .borrow_mut()
                .remove(thread_id);
            return;
        }
        let qwen = self
            .token_savings_observations
            .borrow()
            .get(thread_id)
            .filter(|observation| observation.turn_id == turn_id)
            .map(|observation| observation.qwen.clone())
            .unwrap_or_default();
        let matches_observation = self
            .token_savings_observations
            .borrow()
            .get(thread_id)
            .is_some_and(|observation| observation.turn_id == turn_id);
        if matches_observation {
            self.token_savings_observations
                .borrow_mut()
                .remove(thread_id);
        }

        let runtime_route = self
            .state
            .borrow()
            .task_runtime_settings
            .get(thread_id)
            .map(route_from_runtime_settings);
        let mut stored = self.stored.borrow_mut();
        let routing_state = stored.task_routing.entry(thread_id.to_owned()).or_default();
        let mut route = routing_state
            .route_for_turn(turn_id)
            .cloned()
            .or(runtime_route)
            .unwrap_or_else(|| routing::manual_route("", "", None));
        if routing::is_auto_mode(&routing_state.mode) && !route.automatic {
            route.automatic = true;
            route.reason =
                "Qwen Assist is enabled, but this externally started turn kept its originating client's Codex settings and received no Native pre-turn interception.".into();
        }
        route = routing::enforce_subagent_policy(route, routing_state.allow_subagents);
        if routing_state.route_for_turn(turn_id) != Some(&route) {
            routing_state.record_turn(turn_id, route.clone());
        }
        let receipt = routing::estimate_token_savings_with_qwen(
            &route,
            observed_turn_tokens,
            measurement,
            &qwen,
        );
        if !routing_state.record_token_savings(turn_id, receipt) {
            return;
        }
        drop(stored);
        self.persist();
        self.state.borrow_mut().mark_transcript_changed();
        self.schedule_transcript_render();
    }

    fn observe_macro_experiment(&self, method: &str, params: &Value) {
        if !self.stored.borrow().preferences.macro_executor.enabled {
            self.macro_turns.borrow_mut().clear();
            self.macro_pending_samples.borrow_mut().clear();
            return;
        }
        let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
            return;
        };
        match method {
            "turn/started" => {
                if let Some(sample) = self.macro_pending_samples.borrow_mut().remove(thread_id) {
                    self.record_macro_sample(sample);
                }
                self.macro_turns.borrow_mut().insert(
                    thread_id.to_owned(),
                    MacroTurnObservation {
                        started: Instant::now(),
                        item_ids: HashSet::new(),
                        tool_calls: 0,
                        macro_used: false,
                        peak_memory_bytes: None,
                        workload_class: None,
                        context_bytes_avoided: 0,
                        cache_hits: 0,
                        command_calls: 0,
                        file_change_calls: 0,
                        external_calls: 0,
                    },
                );
            }
            "item/completed" => {
                if let Some(item) = params.get("item")
                    && let Some(observation) = self.macro_turns.borrow_mut().get_mut(thread_id)
                {
                    observe_macro_item(observation, item);
                }
            }
            "turn/completed" => {
                let mut observation = self
                    .macro_turns
                    .borrow_mut()
                    .remove(thread_id)
                    .unwrap_or_else(|| MacroTurnObservation {
                        started: Instant::now(),
                        item_ids: HashSet::new(),
                        tool_calls: 0,
                        macro_used: false,
                        peak_memory_bytes: None,
                        workload_class: None,
                        context_bytes_avoided: 0,
                        cache_hits: 0,
                        command_calls: 0,
                        file_change_calls: 0,
                        external_calls: 0,
                    });
                for item in params
                    .pointer("/turn/items")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    observe_macro_item(&mut observation, item);
                }
                let token_usage = turn_token_breakdown(params);
                let sample = MacroExperimentSample {
                    recorded_at: chrono::Utc::now().timestamp(),
                    group: if observation.macro_used {
                        "macro".into()
                    } else {
                        "baseline".into()
                    },
                    turn_succeeded: turn_completed_successfully(params),
                    elapsed_ms: observation
                        .started
                        .elapsed()
                        .as_millis()
                        .try_into()
                        .unwrap_or(u64::MAX),
                    total_tokens: token_usage.total,
                    input_tokens: token_usage.input,
                    cached_input_tokens: token_usage.cached_input,
                    output_tokens: token_usage.output,
                    reasoning_output_tokens: token_usage.reasoning_output,
                    tool_calls: observation.tool_calls,
                    peak_memory_bytes: observation.peak_memory_bytes,
                    workload_class: observation_workload_class(&observation),
                    context_bytes_avoided: observation.context_bytes_avoided,
                    cache_hits: observation.cache_hits,
                };
                if sample.total_tokens.is_some() {
                    self.record_macro_sample(sample);
                } else {
                    self.macro_pending_samples
                        .borrow_mut()
                        .insert(thread_id.to_owned(), sample);
                }
            }
            "thread/tokenUsage/updated" => {
                let usage = turn_token_breakdown(params);
                let Some(tokens) = usage.total else {
                    return;
                };
                if let Some(mut sample) = self.macro_pending_samples.borrow_mut().remove(thread_id)
                {
                    sample.total_tokens = Some(tokens);
                    sample.input_tokens = usage.input;
                    sample.cached_input_tokens = usage.cached_input;
                    sample.output_tokens = usage.output;
                    sample.reasoning_output_tokens = usage.reasoning_output;
                    self.record_macro_sample(sample);
                }
            }
            "thread/deleted" => {
                self.macro_turns.borrow_mut().remove(thread_id);
                self.macro_pending_samples.borrow_mut().remove(thread_id);
            }
            _ => {}
        }
    }

    fn record_macro_sample(&self, sample: MacroExperimentSample) {
        self.stored.borrow_mut().macro_experiment.record(sample);
        self.persist();
    }

    fn update_task_control_accessibility(&self) {
        let model_id = selected_id(&self.widgets.model_combo, "");
        let models = self.state.borrow().models.clone();
        let model = model_display_name(&model_id)
            .or_else(|| {
                selected_model_metadata(&models, &model_id).and_then(|model| {
                    model
                        .get("displayName")
                        .or_else(|| model.get("name"))
                        .or_else(|| model.get("id"))
                        .and_then(Value::as_str)
                })
            })
            .unwrap_or(if model_id.is_empty() {
                "Default model"
            } else {
                &model_id
            });
        let effort_id = selected_id(&self.widgets.effort_combo, "high");
        let effort_options = reasoning_effort_options_for_model(&models, &model_id);
        let effort = effort_options
            .iter()
            .find_map(|(id, label)| (*id == effort_id).then_some(*label))
            .unwrap_or(&effort_id);
        let speed_id = selected_id(&self.widgets.speed_combo, "standard");
        let speed = if speed_id == "priority" {
            "Fast"
        } else {
            "Standard"
        };
        let sandbox = match selected_id(&self.widgets.sandbox_combo, "workspace-write").as_str() {
            "read-only" => "Read-only",
            "danger-full-access" => "Full access",
            "external-sandbox" => "External sandbox",
            _ => "Workspace write",
        };
        let approval = match selected_id(&self.widgets.approval_combo, "on-request").as_str() {
            "untrusted" => "Ask often",
            "never" => "Never ask",
            "granular" => "Custom approvals",
            _ => "On request",
        };
        self.widgets
            .model_combo
            .update_property(&[gtk::accessible::Property::Label(&format!("Model: {model}"))]);
        self.widgets
            .effort_combo
            .update_property(&[gtk::accessible::Property::Label(&format!(
                "Reasoning effort: {effort}"
            ))]);
        self.widgets
            .speed_combo
            .update_property(&[gtk::accessible::Property::Label(&format!(
                "Response speed: {speed}"
            ))]);
        self.widgets
            .sandbox_combo
            .update_property(&[gtk::accessible::Property::Label(&format!(
                "Sandbox: {sandbox}"
            ))]);
        self.widgets
            .approval_combo
            .update_property(&[gtk::accessible::Property::Label(&format!(
                "Approval policy: {approval}"
            ))]);
    }

    fn update_task_settings_from_controls(&self) {
        if self.updating_task_controls.get() {
            return;
        }
        let active_thread_id = self.state.borrow().active_thread_id.clone();
        let selected_model = selected_id(&self.widgets.model_combo, "");
        let selected_mode = routing_mode_for_model(&selected_model);
        let selected_effort = normalized_effort_for_model(
            &selected_model,
            &selected_id(&self.widgets.effort_combo, "medium"),
        );
        if routing::is_delegate_mode(selected_mode) {
            self.ensure_qwen_loaded();
        }
        let Some(thread_id) = active_thread_id else {
            let mut stored = self.stored.borrow_mut();
            stored.preferences.model = selected_model;
            stored.preferences.routing_mode = selected_mode.into();
            stored.preferences.reasoning_effort = selected_effort;
            stored.preferences.service_tier = selected_id(&self.widgets.speed_combo, "standard");
            stored.preferences.sandbox =
                selected_id(&self.widgets.sandbox_combo, "workspace-write");
            stored.preferences.approval_policy =
                selected_id(&self.widgets.approval_combo, "on-request");
            drop(stored);
            self.persist();
            return;
        };
        let existing = self
            .state
            .borrow()
            .task_runtime_settings
            .get(&thread_id)
            .cloned();
        let Some(existing) = existing else {
            return;
        };
        let settings = TaskRuntimeSettings {
            model: resolved_model_id(&self.state.borrow().models, &selected_model),
            reasoning_effort: Some(codex_effort_for_backend(&selected_model, &selected_effort)),
            service_tier: match selected_id(
                &self.widgets.speed_combo,
                existing.service_tier.as_deref().unwrap_or("standard"),
            )
            .as_str()
            {
                "priority" => Some("priority".into()),
                _ => None,
            },
            sandbox_policy: sandbox_policy_for_selection(
                &selected_id(&self.widgets.sandbox_combo, "workspace-write"),
                &existing.sandbox_policy,
            ),
            approval_policy: approval_policy_for_selection(
                &selected_id(&self.widgets.approval_combo, "on-request"),
                &existing.approval_policy,
            ),
        };
        self.state
            .borrow_mut()
            .task_runtime_settings
            .insert(thread_id.clone(), settings.clone());
        {
            let mut stored = self.stored.borrow_mut();
            let routing = stored.task_routing.entry(thread_id.clone()).or_default();
            if routing::normalize_mode(&routing.mode) != selected_mode {
                routing.last_route = None;
            }
            routing.set_mode(selected_mode);
            routing.delegate_effort =
                routing::is_delegate_mode(selected_mode).then(|| selected_effort.clone());
        }
        self.persist();
        self.update_task_control_accessibility();
        self.request(
            "thread/settings/update",
            task_settings_update_params(&thread_id, &settings),
            PendingKind::UpdateThreadSettings(thread_id),
        );
    }

    fn populate_projects(&self) {
        self.widgets.project_combo.remove_all();
        let (projects, last_project) = {
            let stored = self.stored.borrow();
            (stored.projects.clone(), stored.last_project.clone())
        };
        for project in &projects {
            let id = project.path.to_string_lossy();
            self.widgets.project_combo.append(Some(&id), &project.name);
        }
        if projects.is_empty() {
            if let Ok(path) = env::current_dir() {
                let id = path.to_string_lossy().into_owned();
                let name = path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or(&id);
                self.widgets.project_combo.append(Some(&id), name);
                self.widgets.project_combo.set_active_id(Some(&id));
            }
        } else if let Some(path) = &last_project {
            self.widgets
                .project_combo
                .set_active_id(Some(&path.to_string_lossy()));
        } else {
            self.widgets.project_combo.set_active(Some(0));
        }
    }

    fn populate_models(&self) {
        let previous_guard = self.updating_task_controls.replace(true);
        let selected = {
            let state = self.state.borrow();
            state.active_thread_id.as_ref().and_then(|thread_id| {
                state.task_runtime_settings.get(thread_id).map(|settings| {
                    let stored = self.stored.borrow();
                    selected_model_for_task(&settings.model, stored.task_routing.get(thread_id))
                })
            })
        }
        .or_else(|| {
            self.widgets
                .model_combo
                .active_id()
                .map(|value| value.to_string())
        })
        .unwrap_or_else(|| self.stored.borrow().preferences.model.clone());
        let models = self.state.borrow().models.clone();
        self.widgets.model_combo.remove_all();
        self.widgets
            .model_combo
            .append(Some(""), &default_model_option_label(&models));
        for (id, label) in COMPOSER_MODELS {
            self.widgets.model_combo.append(Some(id), label);
        }
        if !self.widgets.model_combo.set_active_id(Some(&selected)) {
            if selected.is_empty() {
                self.widgets.model_combo.set_active(Some(0));
            } else {
                self.widgets
                    .model_combo
                    .append(Some(&selected), &format!("Current · {selected}"));
                self.widgets.model_combo.set_active_id(Some(&selected));
            }
        }
        self.populate_efforts();
        self.populate_service_tiers();
        self.populate_sandboxes(false);
        self.update_task_control_accessibility();
        self.updating_task_controls.set(previous_guard);
    }

    fn populate_efforts(&self) {
        let selected = self
            .state
            .borrow()
            .active_thread_id
            .as_ref()
            .and_then(|thread_id| {
                self.stored
                    .borrow()
                    .task_routing
                    .get(thread_id)
                    .and_then(|routing| routing.delegate_effort.clone())
            })
            .or_else(|| {
                self.widgets
                    .effort_combo
                    .active_id()
                    .map(|value| value.to_string())
            })
            .unwrap_or_else(|| self.stored.borrow().preferences.reasoning_effort.clone());
        let selected_model = selected_id(
            &self.widgets.model_combo,
            &self.stored.borrow().preferences.model,
        );
        let models = self.state.borrow().models.clone();
        let options = reasoning_effort_options_for_model(&models, &selected_model);
        let model_default = selected_model_metadata(&models, &selected_model)
            .and_then(|model| model.get("defaultReasoningEffort"))
            .and_then(Value::as_str)
            .map(str::to_owned);

        self.widgets.effort_combo.remove_all();
        for (id, label) in options {
            self.widgets.effort_combo.append(Some(id), label);
        }
        if self.widgets.effort_combo.set_active_id(Some(&selected)) {
            return;
        }
        if model_default
            .as_deref()
            .is_some_and(|effort| self.widgets.effort_combo.set_active_id(Some(effort)))
        {
            return;
        }
        self.widgets.effort_combo.set_active(Some(0));
    }

    fn populate_service_tiers(&self) {
        let selected = self
            .widgets
            .speed_combo
            .active_id()
            .map(|value| value.to_string())
            .unwrap_or_else(|| self.stored.borrow().preferences.service_tier.clone());
        let selected_model = selected_id(
            &self.widgets.model_combo,
            &self.stored.borrow().preferences.model,
        );
        let models = self.state.borrow().models.clone();
        let options = service_tier_options_for_model(&models, &selected_model);
        self.widgets.speed_combo.remove_all();
        for (id, label) in options {
            self.widgets.speed_combo.append(Some(id), label);
        }
        if !self.widgets.speed_combo.set_active_id(Some(&selected)) {
            self.widgets.speed_combo.set_active_id(Some("standard"));
        }
    }

    fn populate_sandboxes(&self, reset_buddy_default: bool) {
        let selected_model = selected_id(
            &self.widgets.model_combo,
            &self.stored.borrow().preferences.model,
        );
        let selected = self
            .widgets
            .sandbox_combo
            .active_id()
            .map(|value| value.to_string())
            .unwrap_or_else(|| self.stored.borrow().preferences.sandbox.clone());
        self.widgets.sandbox_combo.remove_all();
        for (id, label) in sandbox_options_for_model(&selected_model) {
            self.widgets.sandbox_combo.append(Some(id), label);
        }
        let kept_selected = !(is_buddy_model(&selected_model) && reset_buddy_default)
            && self.widgets.sandbox_combo.set_active_id(Some(&selected));
        if !kept_selected {
            self.widgets.sandbox_combo.set_active_id(Some("read-only"));
        }
    }

    fn apply_preferences(&self) {
        let preferences = self.stored.borrow().preferences.clone();
        set_combo(&self.widgets.theme_combo, &preferences.theme);
        let previous_task_guard = self.updating_task_controls.replace(true);
        set_combo(&self.widgets.effort_combo, &preferences.reasoning_effort);
        set_combo(&self.widgets.speed_combo, &preferences.service_tier);
        set_combo(&self.widgets.sandbox_combo, &preferences.sandbox);
        set_combo(&self.widgets.approval_combo, &preferences.approval_policy);
        set_combo(&self.widgets.model_combo, &preferences.model);
        self.update_task_control_accessibility();
        self.updating_task_controls.set(previous_task_guard);
        self.apply_routing_controls();
        self.widgets.binary_entry.set_text(
            preferences
                .codex_binary
                .as_ref()
                .map(|path| path.to_string_lossy())
                .as_deref()
                .unwrap_or(""),
        );
        self.widgets.browser_entry.set_text(
            preferences
                .browser_command
                .as_ref()
                .map(|path| path.to_string_lossy())
                .as_deref()
                .unwrap_or(""),
        );
        self.widgets.editor_entry.set_text(
            preferences
                .editor_command
                .as_ref()
                .map(|path| path.to_string_lossy())
                .as_deref()
                .unwrap_or(""),
        );
        self.widgets
            .notification_switch
            .set_active(preferences.desktop_notifications);
        self.widgets
            .reasoning_switch
            .set_active(preferences.show_reasoning);
        self.widgets
            .remote_autostart_switch
            .set_active(preferences.remote_autostart);
        self.widgets
            .keep_awake_switch
            .set_active(preferences.prevent_sleep_while_running);
        self.widgets
            .resource_monitor_switch
            .set_active(preferences.resource_monitor);
        self.updating_qwen_controls.set(true);
        self.widgets
            .qwen_routing_switch
            .set_active(preferences.qwen_buddy.routing_enabled);
        self.widgets
            .qwen_luna_check
            .set_active(preferences.qwen_buddy.luna_enabled);
        self.widgets
            .qwen_condenser_check
            .set_active(preferences.qwen_buddy.condenser_enabled);
        self.widgets
            .qwen_sol_check
            .set_active(preferences.qwen_buddy.sol_enabled);
        self.widgets
            .qwen_gpu_guard_switch
            .set_active(preferences.qwen_buddy.gpu_guard);
        self.updating_qwen_controls.set(false);
        let mut lean_context = preferences.lean_context.clone();
        lean_context.normalize();
        self.updating_context_controls.set(true);
        self.widgets
            .lean_context_enabled
            .set_active(lean_context.enabled);
        set_combo(&self.widgets.lean_context_mode, &lean_context.mode);
        self.widgets
            .lean_context_threshold
            .set_value(f64::from(lean_context.compact_threshold_percent));
        self.widgets
            .lean_context_evidence_budget
            .set_value(f64::from(lean_context.evidence_token_budget));
        self.widgets
            .lean_context_command_budget
            .set_value(f64::from(lean_context.command_output_token_budget));
        self.widgets
            .lean_context_condensation_target
            .set_value(f64::from(lean_context.condensation_target_tokens));
        self.widgets
            .lean_context_qwen
            .set_active(lean_context.qwen_condense_large_files);
        self.updating_context_controls.set(false);
        self.widgets
            .thread_archived_toggle
            .set_active(self.stored.borrow().show_archived_threads);
        apply_theme(&preferences.theme);
        self.update_sleep_inhibition();
    }

    fn save_preferences(&self) {
        let mut stored = self.stored.borrow_mut();
        let preferences = &mut stored.preferences;
        preferences.theme = selected_id(&self.widgets.theme_combo, "system");
        preferences.model = selected_id(&self.widgets.model_combo, "");
        preferences.reasoning_effort = selected_id(&self.widgets.effort_combo, "high");
        preferences.service_tier = selected_id(&self.widgets.speed_combo, "standard");
        preferences.sandbox = selected_id(&self.widgets.sandbox_combo, "workspace-write");
        preferences.approval_policy = selected_id(&self.widgets.approval_combo, "on-request");
        preferences.desktop_notifications = self.widgets.notification_switch.is_active();
        preferences.show_reasoning = self.widgets.reasoning_switch.is_active();
        preferences.remote_autostart = self.widgets.remote_autostart_switch.is_active();
        preferences.remote_ios_auto_routing = false;
        preferences.prevent_sleep_while_running = self.widgets.keep_awake_switch.is_active();
        preferences.resource_monitor = self.widgets.resource_monitor_switch.is_active();
        preferences.qwen_buddy.routing_enabled = self.widgets.qwen_routing_switch.is_active();
        preferences.qwen_buddy.luna_enabled = self.widgets.qwen_luna_check.is_active();
        preferences.qwen_buddy.condenser_enabled = self.widgets.qwen_condenser_check.is_active();
        preferences.qwen_buddy.sol_enabled = self.widgets.qwen_sol_check.is_active();
        preferences.qwen_buddy.gpu_guard = self.widgets.qwen_gpu_guard_switch.is_active();
        preferences.lean_context.enabled = self.widgets.lean_context_enabled.is_active();
        preferences.lean_context.mode = selected_id(&self.widgets.lean_context_mode, "auto");
        preferences.lean_context.compact_threshold_percent =
            self.widgets.lean_context_threshold.value_as_int() as u8;
        preferences.lean_context.evidence_token_budget =
            self.widgets.lean_context_evidence_budget.value_as_int() as u32;
        preferences.lean_context.command_output_token_budget =
            self.widgets.lean_context_command_budget.value_as_int() as u32;
        preferences.lean_context.condensation_target_tokens =
            self.widgets.lean_context_condensation_target.value_as_int() as u32;
        preferences.lean_context.qwen_condense_large_files =
            self.widgets.lean_context_qwen.is_active();
        preferences.lean_context.normalize();
        let binary = self.widgets.binary_entry.text();
        preferences.codex_binary =
            (!binary.trim().is_empty()).then(|| PathBuf::from(binary.trim()));
        let browser = self.widgets.browser_entry.text();
        preferences.browser_command =
            (!browser.trim().is_empty()).then(|| PathBuf::from(browser.trim()));
        let editor = self.widgets.editor_entry.text();
        preferences.editor_command =
            (!editor.trim().is_empty()).then(|| PathBuf::from(editor.trim()));
        apply_theme(&preferences.theme);
        let remote_autostart = preferences.remote_autostart;
        stored.prepare_for_save();
        match persistence::save_state(&stored) {
            Ok(()) => {
                drop(stored);
                self.update_sleep_inhibition();
                self.widgets.settings_save.set_sensitive(false);
                self.widgets.settings_save.set_label("Saving…");
                if smoke_fixtures_enabled() {
                    // UI smoke instances use isolated XDG state but share the
                    // user's systemd session. Never let a test save stop or
                    // start the real Remote Control host.
                    self.widgets.settings_save.set_sensitive(true);
                    self.widgets.settings_save.set_label("Save settings");
                } else {
                    self.hub.host(HostAction::RemoteAutostart {
                        enabled: remote_autostart,
                    });
                }
            }
            Err(error) => self.toast(&format!("Could not save settings: {error}")),
        }
    }

    fn persist(&self) {
        let mut stored = self.stored.borrow_mut();
        stored.prepare_for_save();
        if let Err(error) = persistence::save_state(&stored) {
            tracing::warn!(%error, "failed to persist UI state");
        }
    }

    fn update_sleep_inhibition(&self) {
        let enabled = self.stored.borrow().preferences.prevent_sleep_while_running;
        let state = self.state.borrow();
        let active_task = !state.external_running_threads.is_empty()
            || state.threads.values().any(thread_is_running);
        let should_inhibit = should_inhibit_sleep(enabled, &state.remote, active_task);
        drop(state);
        let Some(application) = self.widgets.window.application() else {
            return;
        };
        if should_inhibit && self.sleep_inhibit_cookie.get().is_none() {
            let cookie = application.inhibit(
                Some(&self.widgets.window),
                gtk::ApplicationInhibitFlags::SUSPEND,
                Some("Keep Codex tasks and paired Remote access available"),
            );
            if cookie != 0 {
                self.sleep_inhibit_cookie.set(Some(cookie));
            }
        } else if !should_inhibit && let Some(cookie) = self.sleep_inhibit_cookie.take() {
            application.uninhibit(cookie);
        }
    }

    fn observe_goal_lifecycle(&self, method: &str, params: &Value) {
        let mut changed = false;
        match method {
            "thread/goal/updated" => {
                let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
                    return;
                };
                let status = params
                    .pointer("/goal/status")
                    .and_then(Value::as_str)
                    .unwrap_or("active");
                let mut stored = self.stored.borrow_mut();
                if let Some(reason) = goal_status_reason(status) {
                    if !stored.goal_stop_reasons.contains_key(thread_id) {
                        stored
                            .goal_stop_reasons
                            .insert(thread_id.to_owned(), reason.to_owned());
                        changed = true;
                    }
                } else if matches!(status, "active" | "complete") {
                    changed = stored.goal_stop_reasons.remove(thread_id).is_some();
                }
            }
            "thread/goal/cleared" => {
                if let Some(thread_id) = params.get("threadId").and_then(Value::as_str) {
                    changed = self
                        .stored
                        .borrow_mut()
                        .goal_stop_reasons
                        .remove(thread_id)
                        .is_some();
                }
            }
            "turn/started" => {
                if let Some(thread_id) = params.get("threadId").and_then(Value::as_str)
                    && self
                        .state
                        .borrow()
                        .thread_goals
                        .get(thread_id)
                        .and_then(|goal| goal.get("status"))
                        .and_then(Value::as_str)
                        == Some("active")
                {
                    changed = self
                        .stored
                        .borrow_mut()
                        .goal_stop_reasons
                        .remove(thread_id)
                        .is_some();
                }
            }
            "turn/completed" => {
                let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
                    return;
                };
                let Some((status, reason)) = turn_goal_stop_reason(params) else {
                    return;
                };
                let mut state = self.state.borrow_mut();
                let Some(goal) = state.thread_goals.get_mut(thread_id) else {
                    return;
                };
                if goal.get("status").and_then(Value::as_str) != Some("active") {
                    return;
                }
                if let Some(goal) = goal.as_object_mut() {
                    goal.insert("status".to_owned(), Value::String(status.to_owned()));
                }
                drop(state);
                self.stored
                    .borrow_mut()
                    .goal_stop_reasons
                    .insert(thread_id.to_owned(), reason);
                changed = true;
            }
            _ => {}
        }
        if changed {
            self.persist();
            self.render_goal_selector();
        }
    }

    fn save_qwen_preferences(&self) {
        if self.updating_qwen_controls.get() {
            return;
        }
        {
            let mut stored = self.stored.borrow_mut();
            let qwen = &mut stored.preferences.qwen_buddy;
            qwen.routing_enabled = self.widgets.qwen_routing_switch.is_active();
            qwen.luna_enabled = self.widgets.qwen_luna_check.is_active();
            qwen.condenser_enabled = self.widgets.qwen_condenser_check.is_active();
            qwen.sol_enabled = self.widgets.qwen_sol_check.is_active();
            qwen.gpu_guard = self.widgets.qwen_gpu_guard_switch.is_active();
        }
        self.persist();
        if self.stored.borrow().preferences.qwen_buddy.routing_enabled {
            self.refresh_qwen();
        }
        self.render_qwen();
    }

    fn save_lean_context_preferences(&self) {
        if self.updating_context_controls.get() {
            return;
        }
        {
            let mut stored = self.stored.borrow_mut();
            let preferences = &mut stored.preferences.lean_context;
            preferences.enabled = self.widgets.lean_context_enabled.is_active();
            preferences.mode = selected_id(&self.widgets.lean_context_mode, "auto");
            preferences.compact_threshold_percent =
                self.widgets.lean_context_threshold.value_as_int() as u8;
            preferences.evidence_token_budget =
                self.widgets.lean_context_evidence_budget.value_as_int() as u32;
            preferences.command_output_token_budget =
                self.widgets.lean_context_command_budget.value_as_int() as u32;
            preferences.condensation_target_tokens =
                self.widgets.lean_context_condensation_target.value_as_int() as u32;
            preferences.qwen_condense_large_files = self.widgets.lean_context_qwen.is_active();
            preferences.normalize();
        }
        self.persist();
        self.render_context();
    }

    fn rename_thread_dialog(&self) {
        let state = self.state.borrow();
        let Some(thread) = state.active_thread() else {
            return;
        };
        let thread_id = thread.id.clone();
        let entry = gtk::Entry::new();
        entry.set_text(thread.title());
        entry.set_activates_default(true);
        drop(state);

        let dialog = adw::AlertDialog::new(Some("Rename task"), None);
        dialog.set_extra_child(Some(&entry));
        dialog.add_responses(&[("cancel", "Cancel"), ("rename", "Rename")]);
        dialog.set_default_response(Some("rename"));
        dialog.set_close_response("cancel");
        let hub = self.hub.clone();
        let state = self.state.clone();
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            move |response| {
                let name = entry.text().trim().to_owned();
                if response.as_str() == "rename" && !name.is_empty() {
                    let id = hub.request(
                        "thread/name/set",
                        json!({"threadId": thread_id, "name": name}),
                    );
                    state
                        .borrow_mut()
                        .pending
                        .insert(id, PendingKind::RenameThread);
                }
            },
        );
    }

    fn fork_thread(&self) {
        let Some(thread_id) = self.state.borrow().active_thread_id.clone() else {
            return;
        };
        self.request(
            "thread/fork",
            json!({"threadId": thread_id}),
            PendingKind::ForkThread,
        );
    }

    fn toggle_pin(&self) {
        let Some(thread_id) = self.state.borrow().active_thread_id.clone() else {
            return;
        };
        self.toggle_thread_pin(&thread_id);
    }

    fn toggle_thread_pin(&self, thread_id: &str) {
        let mut stored = self.stored.borrow_mut();
        if stored.pinned_threads.iter().any(|id| id == thread_id) {
            stored.pinned_threads.retain(|id| id != thread_id);
            self.toast("Task unpinned");
        } else {
            stored.pinned_threads.push(thread_id.to_owned());
            self.toast("Task pinned");
        }
        drop(stored);
        self.persist();
        self.render_threads();
        self.invalidate_transcript();
        self.render_transcript();
    }

    fn goal_dialog(&self) {
        self.widgets.goal_button.popdown();
        let state = self.state.borrow();
        let Some(thread_id) = state.active_thread_id.clone() else {
            return;
        };
        let existing = state.thread_goals.get(&thread_id).cloned();
        drop(state);
        let objective = gtk::TextView::new();
        objective.set_wrap_mode(gtk::WrapMode::WordChar);
        objective.set_size_request(380, 100);
        if let Some(text) = existing
            .as_ref()
            .and_then(|value| value.get("objective"))
            .and_then(Value::as_str)
        {
            objective.buffer().set_text(text);
        }
        let objective_frame = gtk::Frame::builder().child(&objective).build();
        let budget = gtk::Entry::builder()
            .placeholder_text("Optional token budget")
            .build();
        if let Some(value) = existing
            .as_ref()
            .and_then(|value| value.get("tokenBudget"))
            .and_then(Value::as_i64)
        {
            budget.set_text(&value.to_string());
        }
        let status = compact_combo(&[
            ("active", "Active"),
            ("paused", "Paused"),
            ("blocked", "Blocked"),
            ("usageLimited", "Usage limited"),
            ("budgetLimited", "Budget limited"),
            ("complete", "Complete"),
        ]);
        status.set_active_id(Some(
            existing
                .as_ref()
                .and_then(|value| value.get("status"))
                .and_then(Value::as_str)
                .unwrap_or("active"),
        ));
        let form = gtk::Grid::builder()
            .column_spacing(12)
            .row_spacing(9)
            .build();
        attach_setting(&form, 0, "Objective", &objective_frame);
        attach_setting(&form, 1, "Status", &status);
        attach_setting(&form, 2, "Budget", &budget);
        let dialog = adw::AlertDialog::new(
            Some("Task goal"),
            Some("Goals persist across remote handoff and report token/time progress."),
        );
        dialog.set_extra_child(Some(&form));
        dialog.add_responses(&[("cancel", "Cancel"), ("clear", "Clear"), ("save", "Save")]);
        dialog.set_response_appearance("clear", adw::ResponseAppearance::Destructive);
        dialog.set_default_response(Some("save"));
        dialog.set_close_response("cancel");
        let hub = self.hub.clone();
        let pending = self.state.clone();
        let overlay = self.widgets.toast_overlay.clone();
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            move |response| {
                let response = response.as_str();
                if response == "clear" {
                    let id = hub.request("thread/goal/clear", json!({"threadId": thread_id}));
                    pending
                        .borrow_mut()
                        .pending
                        .insert(id, PendingKind::Goal(thread_id.clone()));
                    return;
                }
                if response != "save" {
                    return;
                }
                let buffer = objective.buffer();
                let objective = buffer
                    .text(&buffer.start_iter(), &buffer.end_iter(), false)
                    .trim()
                    .to_owned();
                if objective.is_empty() {
                    overlay.add_toast(adw::Toast::new("Enter a goal objective"));
                    return;
                }
                let budget_text = budget.text().trim().to_owned();
                let token_budget = if budget_text.is_empty() {
                    None
                } else {
                    match budget_text.parse::<i64>() {
                        Ok(value) if value > 0 => Some(value),
                        _ => {
                            overlay.add_toast(adw::Toast::new(
                                "Token budget must be a positive number",
                            ));
                            return;
                        }
                    }
                };
                let id = hub.request(
                    "thread/goal/set",
                    json!({
                        "threadId": thread_id,
                        "objective": objective,
                        "status": status.active_id().as_deref().unwrap_or("active"),
                        "tokenBudget": token_budget
                    }),
                );
                pending
                    .borrow_mut()
                    .pending
                    .insert(id, PendingKind::Goal(thread_id.clone()));
            },
        );
    }

    fn set_goal_status(&self, status: &str) {
        self.widgets.goal_button.popdown();
        let state = self.state.borrow();
        let Some(thread_id) = state.active_thread_id.clone() else {
            return;
        };
        if !state.thread_goals.contains_key(&thread_id) {
            drop(state);
            self.toast("Create a goal for this task first");
            return;
        }
        drop(state);
        self.request(
            "thread/goal/set",
            json!({"threadId": thread_id, "status": status}),
            PendingKind::GoalStatus {
                thread_id,
                status: status.to_owned(),
            },
        );
    }

    fn confirm_clear_goal(&self) {
        self.widgets.goal_button.popdown();
        let Some(thread_id) = self.state.borrow().active_thread_id.clone() else {
            return;
        };
        let dialog = adw::AlertDialog::new(
            Some("Clear this task’s goal?"),
            Some("The goal objective, budget, and tracked progress will be removed."),
        );
        dialog.add_responses(&[("cancel", "Cancel"), ("clear", "Clear goal")]);
        dialog.set_response_appearance("clear", adw::ResponseAppearance::Destructive);
        dialog.set_default_response(Some("cancel"));
        dialog.set_close_response("cancel");
        let hub = self.hub.clone();
        let state = self.state.clone();
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            move |response| {
                if response.as_str() != "clear" {
                    return;
                }
                let id = hub.request("thread/goal/clear", json!({"threadId": thread_id}));
                state
                    .borrow_mut()
                    .pending
                    .insert(id, PendingKind::Goal(thread_id));
            },
        );
    }

    fn compact_thread(&self) {
        let Some(thread_id) = self.state.borrow().active_thread_id.clone() else {
            return;
        };
        self.queue_context_checkpoint(&thread_id, true, false, "manual");
    }

    fn queue_context_checkpoint(
        &self,
        thread_id: &str,
        compact_after: bool,
        automatic: bool,
        trigger: &str,
    ) {
        let request = {
            let state = self.state.borrow();
            let Some(thread) = state.threads.get(thread_id) else {
                if !automatic {
                    self.toast("Select a loaded task before creating a context checkpoint");
                }
                return;
            };
            if state.context_checkpoint_inflight.contains(thread_id) {
                if !automatic {
                    self.toast("A context checkpoint is already being created");
                }
                return;
            }
            if compact_after && state.connection != ConnectionState::Ready {
                if !automatic {
                    self.toast("Reconnect to Codex before compacting this task");
                }
                return;
            }
            if compact_after
                && (thread_is_running(thread)
                    || (state.active_thread_id.as_deref() == Some(thread_id)
                        && state.active_turn_id.is_some()))
            {
                if !automatic {
                    self.toast("Wait for the active turn to finish before compacting");
                }
                return;
            }
            if compact_after && !state.approvals.is_empty() {
                if !automatic {
                    self.toast("Resolve the pending approval before compacting");
                }
                return;
            }
            let stored = self.stored.borrow();
            let context_generation = stored
                .context_metrics
                .get(thread_id)
                .map(|metrics| metrics.context_generation)
                .unwrap_or(0);
            context::checkpoint_request(
                thread,
                state.thread_goals.get(thread_id),
                state.task_runtime_settings.get(thread_id),
                state.thread_token_usage.get(thread_id),
                trigger,
                stored.context_checkpoints.get(thread_id),
                context_generation,
            )
        };
        self.state
            .borrow_mut()
            .context_checkpoint_inflight
            .insert(thread_id.to_owned());
        self.hub.host(HostAction::ContextCheckpoint {
            request: Box::new(request),
            compact_after,
            automatic,
        });
        self.render_context();
    }

    fn begin_context_compaction(&self, receipt: &ContextCheckpointReceipt, automatic: bool) {
        {
            let state = self.state.borrow();
            let Some(thread) = state.threads.get(&receipt.thread_id) else {
                return;
            };
            let blocked = state.connection != ConnectionState::Ready
                || thread_is_running(thread)
                || (state.active_thread_id.as_deref() == Some(receipt.thread_id.as_str())
                    && state.active_turn_id.is_some())
                || !state.approvals.is_empty()
                || state.pending.values().any(|pending| {
                    matches!(
                        pending,
                        PendingKind::CompactThread { thread_id, .. }
                            if thread_id == &receipt.thread_id
                    )
                });
            if blocked {
                if !automatic {
                    self.toast(
                        "Checkpoint saved. Compaction is waiting for an idle, connected task.",
                    );
                }
                return;
            }
        }
        self.request(
            "thread/compact/start",
            json!({"threadId": receipt.thread_id}),
            PendingKind::CompactThread {
                thread_id: receipt.thread_id.clone(),
                automatic,
                checkpoint_id: receipt.id.clone(),
            },
        );
    }

    fn maybe_manage_context(&self, thread_id: &str) {
        let active = self.state.borrow().active_thread_id.as_deref() == Some(thread_id);
        let (preferences, usage, guards) = {
            let stored = self.stored.borrow();
            let preferences = stored.preferences.lean_context.clone();
            let checkpoint_current = stored
                .context_metrics
                .get(thread_id)
                .is_some_and(|metrics| {
                    metrics.last_checkpoint_generation == Some(metrics.context_generation)
                });
            let state = self.state.borrow();
            let thread_running = state.threads.get(thread_id).is_some_and(thread_is_running)
                || (state.active_thread_id.as_deref() == Some(thread_id)
                    && state.active_turn_id.is_some());
            let compact_pending = state.pending.values().any(|pending| {
                matches!(
                    pending,
                    PendingKind::CompactThread {
                        thread_id: pending_thread_id,
                        ..
                    } if pending_thread_id == thread_id
                )
            });
            let usage = context::usage_snapshot(state.thread_token_usage.get(thread_id));
            (
                preferences,
                usage,
                CompactionGuards {
                    connection_ready: state.connection == ConnectionState::Ready,
                    thread_running,
                    pending_approval: !state.approvals.is_empty(),
                    request_inflight: state.context_checkpoint_inflight.contains(thread_id)
                        || compact_pending
                        || state
                            .context_compaction_measurements
                            .contains_key(thread_id)
                        || checkpoint_current,
                },
            )
        };
        match context::compaction_decision(&preferences, usage, guards) {
            CompactionDecision::Checkpoint(percent) => self.queue_context_checkpoint(
                thread_id,
                false,
                true,
                &format!("automatic-checkpoint-{percent}-percent"),
            ),
            CompactionDecision::Prompt(percent) => {
                if !active {
                    self.render_context();
                    return;
                }
                let band = percent / 5 * 5;
                let should_prompt = {
                    let mut state = self.state.borrow_mut();
                    if state
                        .context_prompted_percent
                        .get(thread_id)
                        .is_some_and(|previous| *previous >= band)
                    {
                        false
                    } else {
                        state
                            .context_prompted_percent
                            .insert(thread_id.to_owned(), band);
                        true
                    }
                };
                if should_prompt {
                    self.show_context_compaction_prompt(thread_id, percent);
                }
            }
            _ => {}
        }
        self.render_context();
    }

    fn observe_context_compaction(&self, event: ContextCompactionEvent) {
        match event.phase {
            ContextCompactionPhase::Started => {
                if self
                    .stored
                    .borrow()
                    .context_metrics
                    .get(&event.thread_id)
                    .and_then(|metrics| metrics.last_compaction_id.as_deref())
                    == Some(event.item_id.as_str())
                {
                    return;
                }
                let usage = {
                    let state = self.state.borrow();
                    context::usage_snapshot(state.thread_token_usage.get(&event.thread_id))
                };
                let (generation, checkpointed, receipt, should_checkpoint) = {
                    let stored = self.stored.borrow();
                    let metrics = stored.context_metrics.get(&event.thread_id);
                    let generation = metrics
                        .map(|metrics| metrics.context_generation)
                        .unwrap_or_default();
                    let checkpointed = metrics.is_some_and(|metrics| {
                        metrics.last_checkpoint_generation == Some(generation)
                    });
                    let receipt = stored
                        .context_checkpoints
                        .get(&event.thread_id)
                        .filter(|receipt| receipt.context_generation == generation)
                        .cloned();
                    let should_checkpoint = stored.preferences.lean_context.enabled
                        && stored.preferences.lean_context.mode == "auto"
                        && !checkpointed
                        && !self
                            .state
                            .borrow()
                            .context_checkpoint_inflight
                            .contains(&event.thread_id);
                    (generation, checkpointed, receipt, should_checkpoint)
                };
                let measurement = ContextCompactionMeasurement {
                    item_id: event.item_id,
                    turn_id: event.turn_id,
                    generation_before: generation,
                    before_tokens: usage.total_tokens,
                    context_window: usage.context_window,
                    completed: false,
                    checkpointed,
                    checkpoint_elapsed_ms: receipt
                        .as_ref()
                        .map(|receipt| receipt.elapsed_ms)
                        .unwrap_or_default(),
                    checkpoint_hashed_bytes: receipt
                        .as_ref()
                        .map(|receipt| receipt.hashed_bytes)
                        .unwrap_or_default(),
                    checkpoint_reused_evidence: receipt
                        .as_ref()
                        .map(|receipt| receipt.reused_evidence)
                        .unwrap_or_default(),
                };
                let mut state = self.state.borrow_mut();
                let replace = state
                    .context_compaction_measurements
                    .get(&event.thread_id)
                    .is_none_or(|existing| existing.item_id != measurement.item_id);
                if replace {
                    state
                        .context_compaction_measurements
                        .insert(event.thread_id.clone(), measurement);
                }
                drop(state);
                if should_checkpoint {
                    self.queue_context_checkpoint(
                        &event.thread_id,
                        false,
                        true,
                        "codex-compaction-started",
                    );
                }
            }
            ContextCompactionPhase::Completed => {
                let usage = {
                    let state = self.state.borrow();
                    context::usage_snapshot(state.thread_token_usage.get(&event.thread_id))
                };
                let mut stored = self.stored.borrow_mut();
                let metrics = stored
                    .context_metrics
                    .entry(event.thread_id.clone())
                    .or_default();
                let generation = metrics.context_generation;
                let duplicate =
                    metrics.last_compaction_id.as_deref() == Some(event.item_id.as_str());
                if duplicate {
                    return;
                }
                metrics.compaction_count = metrics.compaction_count.saturating_add(1);
                metrics.last_before_tokens = usage.total_tokens;
                metrics.last_compacted_at = Some(chrono::Utc::now().timestamp());
                metrics.last_compaction_id = Some(event.item_id.clone());
                metrics.context_generation = metrics.context_generation.saturating_add(1);
                metrics.last_error = None;
                let checkpointed = metrics.last_checkpoint_generation == Some(generation);
                let receipt = stored
                    .context_checkpoints
                    .get(&event.thread_id)
                    .filter(|receipt| receipt.context_generation == generation)
                    .cloned();
                drop(stored);

                let mut state = self.state.borrow_mut();
                let measurement = state
                    .context_compaction_measurements
                    .entry(event.thread_id.clone())
                    .or_insert_with(|| ContextCompactionMeasurement {
                        item_id: event.item_id.clone(),
                        turn_id: event.turn_id.clone(),
                        generation_before: generation,
                        before_tokens: 0,
                        context_window: usage.context_window,
                        completed: false,
                        checkpointed,
                        checkpoint_elapsed_ms: 0,
                        checkpoint_hashed_bytes: 0,
                        checkpoint_reused_evidence: 0,
                    });
                measurement.completed = true;
                measurement.checkpointed |= checkpointed;
                if let Some(receipt) = receipt {
                    measurement.checkpoint_elapsed_ms = receipt.elapsed_ms;
                    measurement.checkpoint_hashed_bytes = receipt.hashed_bytes;
                    measurement.checkpoint_reused_evidence = receipt.reused_evidence;
                }
                state.context_prompted_percent.remove(&event.thread_id);
                drop(state);
                self.persist();
                self.render_context();
            }
        }
    }

    fn show_context_compaction_prompt(&self, thread_id: &str, percent: u8) {
        let dialog = adw::AlertDialog::new(
            Some("Create a checkpoint before Codex rollover?"),
            Some(&format!(
                "This task is using {percent}% of its model context window. Codex Native will create one private structured checkpoint; Codex remains responsible for model-aware automatic compaction on desktop and iOS."
            )),
        );
        dialog.add_responses(&[("later", "Later"), ("checkpoint", "Create checkpoint")]);
        dialog.set_default_response(Some("checkpoint"));
        dialog.set_close_response("later");
        dialog.set_response_appearance("checkpoint", adw::ResponseAppearance::Suggested);
        let weak = self.weak_self.borrow().clone();
        let thread_id = thread_id.to_owned();
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            move |response| {
                if response.as_str() == "checkpoint"
                    && let Some(controller) = weak.upgrade()
                {
                    controller.queue_context_checkpoint(
                        &thread_id,
                        false,
                        false,
                        "prompt-approved-checkpoint",
                    );
                }
            },
        );
    }

    fn observe_context_usage(&self, thread_id: &str) {
        let usage = {
            let state = self.state.borrow();
            context::usage_snapshot(state.thread_token_usage.get(thread_id))
        };
        let measurement = if usage.current_window {
            let mut state = self.state.borrow_mut();
            state
                .context_compaction_measurements
                .get(thread_id)
                .is_some_and(|measurement| measurement.completed)
                .then(|| state.context_compaction_measurements.remove(thread_id))
                .flatten()
        } else {
            None
        };
        if let Some(measurement) = measurement {
            if measurement.before_tokens == 0 || usage.total_tokens == 0 {
                self.render_context();
                return;
            }
            let reduced = usage.total_tokens < measurement.before_tokens;
            let removed = measurement.before_tokens.saturating_sub(usage.total_tokens);
            let mut stored = self.stored.borrow_mut();
            let metrics = stored
                .context_metrics
                .entry(thread_id.to_owned())
                .or_default();
            metrics.last_before_tokens = measurement.before_tokens;
            metrics.last_after_tokens = usage.total_tokens;
            if reduced {
                metrics.observed_tokens_removed =
                    metrics.observed_tokens_removed.saturating_add(removed);
                metrics.successful_measurements = metrics.successful_measurements.saturating_add(1);
                metrics.last_error = None;
            } else {
                metrics.failed_measurements = metrics.failed_measurements.saturating_add(1);
                metrics.last_error = Some(context::NO_IMMEDIATE_COMPACTION_REDUCTION.into());
            }
            stored.lean_experiment.record(LeanExperimentSample {
                recorded_at: chrono::Utc::now().timestamp(),
                group: if measurement.checkpointed {
                    "checkpointed".into()
                } else {
                    "uncheckpointed".into()
                },
                reduced_context: reduced,
                before_tokens: measurement.before_tokens,
                after_tokens: usage.total_tokens,
                context_window: measurement.context_window.max(usage.context_window),
                tokens_removed: removed,
                checkpoint_elapsed_ms: measurement.checkpoint_elapsed_ms,
                checkpoint_hashed_bytes: measurement.checkpoint_hashed_bytes,
                checkpoint_reused_evidence: measurement.checkpoint_reused_evidence,
            });
            drop(stored);
            self.persist();
            self.render_context();
            return;
        }
        self.maybe_manage_context(thread_id);
    }

    fn purge_deleted_thread_context(&self, thread_id: &str) {
        {
            let mut stored = self.stored.borrow_mut();
            stored.context_checkpoints.remove(thread_id);
            stored.context_metrics.remove(thread_id);
            stored.lean_context_thread_payloads.remove(thread_id);
            stored.buddy_turns.remove(thread_id);
            stored.buddy_token_usage.remove(thread_id);
        }
        self.persist();
        self.hub.host(HostAction::ContextDelete {
            thread_id: thread_id.to_owned(),
        });
    }

    fn rollback_thread_dialog(&self) {
        let Some(thread_id) = self.state.borrow().active_thread_id.clone() else {
            return;
        };
        let dialog = adw::AlertDialog::new(
            Some("Roll back the last turn?"),
            Some(
                "This removes the last conversation turn. It does not revert files changed by that turn; use the GitHub plugin or your preferred Git tool for file restoration.",
            ),
        );
        dialog.add_responses(&[("cancel", "Cancel"), ("rollback", "Roll back")]);
        dialog.set_response_appearance("rollback", adw::ResponseAppearance::Destructive);
        dialog.set_close_response("cancel");
        let hub = self.hub.clone();
        let state = self.state.clone();
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            move |response| {
                if response.as_str() == "rollback" {
                    let id = hub.request(
                        "thread/rollback",
                        json!({"threadId": thread_id, "numTurns": 1}),
                    );
                    state
                        .borrow_mut()
                        .pending
                        .insert(id, PendingKind::RollbackThread(thread_id.clone()));
                }
            },
        );
    }

    fn archive_thread(&self) {
        let Some(thread_id) = self.state.borrow().active_thread_id.clone() else {
            return;
        };
        self.request(
            "thread/archive",
            json!({"threadId": thread_id}),
            PendingKind::ArchiveThread,
        );
    }

    fn sync_task_selection_mode(&self) {
        let enabled = self.widgets.thread_select_toggle.is_active();
        if !enabled {
            self.selected_thread_ids.borrow_mut().clear();
        }
        self.widgets
            .thread_select_toggle
            .set_label(if enabled { "Done" } else { "Select tasks" });
        self.widgets.thread_bulk_bar.set_visible(enabled);
        self.render_threads();
    }

    fn close_task_selection(&self) {
        let was_active = self.widgets.thread_select_toggle.is_active();
        let had_selection = !self.selected_thread_ids.borrow().is_empty();
        if was_active {
            self.widgets.thread_select_toggle.set_active(false);
        }
        if !was_active && !had_selection {
            return;
        }
        self.selected_thread_ids.borrow_mut().clear();
        self.widgets.thread_select_toggle.set_label("Select tasks");
        self.widgets.thread_bulk_bar.set_visible(false);
        self.render_threads();
    }

    fn toggle_task_selection(&self, thread_id: &str) {
        if !self.widgets.thread_select_toggle.is_active() {
            return;
        }
        let belongs_to_active_account = {
            let stored = self.stored.borrow();
            stored
                .task_owner(thread_id)
                .is_none_or(|owner| owner == stored.active_account_id)
        };
        if !belongs_to_active_account {
            self.toast("Switch to this task's account before changing it");
            return;
        }
        let mut selected = self.selected_thread_ids.borrow_mut();
        if !selected.remove(thread_id) {
            selected.insert(thread_id.to_owned());
        }
        drop(selected);
        self.render_threads();
    }

    fn toggle_all_visible_tasks(&self) {
        let search = self.widgets.thread_search.text().trim().to_lowercase();
        let (pinned, active_account_id) = {
            let stored = self.stored.borrow();
            (
                stored.pinned_threads.clone(),
                stored.active_account_id.clone(),
            )
        };
        let visible = visible_thread_ids(&self.state.borrow(), &pinned, &search)
            .into_iter()
            .filter(|thread_id| {
                self.stored
                    .borrow()
                    .task_owner(thread_id)
                    .is_none_or(|owner| owner == active_account_id)
            })
            .collect::<Vec<_>>();
        if visible.is_empty() {
            return;
        }
        let mut selected = self.selected_thread_ids.borrow_mut();
        if visible.iter().all(|id| selected.contains(id)) {
            for id in visible {
                selected.remove(&id);
            }
        } else {
            selected.extend(visible);
        }
        drop(selected);
        self.render_threads();
    }

    fn selected_existing_task_ids(&self) -> Vec<String> {
        let state = self.state.borrow();
        let active_account_id = self.stored.borrow().active_account_id.clone();
        self.selected_thread_ids
            .borrow()
            .iter()
            .filter(|id| {
                state.threads.contains_key(*id)
                    && self
                        .stored
                        .borrow()
                        .task_owner(id)
                        .is_none_or(|owner| owner == active_account_id)
            })
            .cloned()
            .collect()
    }

    fn update_bulk_task_controls(&self) {
        let search = self.widgets.thread_search.text().trim().to_lowercase();
        let (pinned, archived, active_account_id) = {
            let stored = self.stored.borrow();
            (
                stored.pinned_threads.clone(),
                stored.show_archived_threads,
                stored.active_account_id.clone(),
            )
        };
        let (ready, bulk_pending, visible) = {
            let state = self.state.borrow();
            (
                state.connection == ConnectionState::Ready,
                state.pending.values().any(is_bulk_task_mutation),
                visible_thread_ids(&state, &pinned, &search)
                    .into_iter()
                    .filter(|thread_id| {
                        self.stored
                            .borrow()
                            .task_owner(thread_id)
                            .is_none_or(|owner| owner == active_account_id)
                    })
                    .collect::<Vec<_>>(),
            )
        };
        let selected = self.selected_thread_ids.borrow();
        let selected_count = selected.len();
        let all_visible_selected =
            !visible.is_empty() && visible.iter().all(|id| selected.contains(id));
        drop(selected);

        self.widgets
            .thread_bulk_count
            .set_label(&format!("{selected_count} selected"));
        self.widgets
            .thread_select_all
            .set_label(if all_visible_selected { "None" } else { "All" });
        self.widgets
            .thread_select_all
            .set_sensitive(ready && !bulk_pending && !visible.is_empty());
        self.widgets
            .thread_select_toggle
            .set_sensitive(ready && !bulk_pending);
        self.widgets
            .thread_bulk_archive
            .set_sensitive(ready && !bulk_pending && selected_count > 0);
        self.widgets
            .thread_bulk_delete
            .set_sensitive(ready && !bulk_pending && selected_count > 0);

        let archive_label = if archived {
            "Restore selected tasks"
        } else {
            "Archive selected tasks"
        };
        self.widgets.thread_bulk_archive.set_icon_name(if archived {
            "folder-new-symbolic"
        } else {
            "folder-symbolic"
        });
        self.widgets
            .thread_bulk_archive
            .set_tooltip_text(Some(archive_label));
        self.widgets
            .thread_bulk_archive
            .update_property(&[gtk::accessible::Property::Label(archive_label)]);
    }

    fn start_bulk_task_mutation(
        &self,
        thread_ids: Vec<String>,
        method: &str,
        pending: PendingKind,
        progress_verb: &str,
    ) {
        if thread_ids.is_empty() {
            return;
        }
        if self.state.borrow().connection != ConnectionState::Ready {
            self.toast("Reconnect to Codex before changing selected tasks");
            return;
        }
        let count = thread_ids.len();
        self.close_task_selection();
        for thread_id in thread_ids {
            self.request(method, json!({"threadId": thread_id}), pending.clone());
        }
        self.update_bulk_task_controls();
        self.toast(&format!(
            "{progress_verb} {count} selected task{}…",
            if count == 1 { "" } else { "s" }
        ));
    }

    fn archive_selected_tasks(&self) {
        let thread_ids = self.selected_existing_task_ids();
        if self.stored.borrow().show_archived_threads {
            self.start_bulk_task_mutation(
                thread_ids,
                "thread/unarchive",
                PendingKind::BulkUnarchiveThread,
                "Restoring",
            );
        } else {
            self.start_bulk_task_mutation(
                thread_ids,
                "thread/archive",
                PendingKind::BulkArchiveThread,
                "Archiving",
            );
        }
    }

    fn delete_selected_tasks_dialog(&self) {
        let thread_ids = self.selected_existing_task_ids();
        if thread_ids.is_empty() {
            return;
        }
        let count = thread_ids.len();
        let dialog = adw::AlertDialog::new(
            Some(&bulk_delete_dialog_title(count)),
            Some(
                "This removes every selected task history from Codex. This action cannot be undone.",
            ),
        );
        dialog.add_responses(&[("cancel", "Cancel"), ("delete", "Delete tasks")]);
        dialog.set_response_appearance("delete", adw::ResponseAppearance::Destructive);
        dialog.set_close_response("cancel");
        let weak = self.weak_self.borrow().clone();
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            move |response| {
                if response.as_str() == "delete" {
                    with_controller(&weak, |controller| {
                        controller.start_bulk_task_mutation(
                            thread_ids,
                            "thread/delete",
                            PendingKind::BulkDeleteThread,
                            "Deleting",
                        )
                    });
                }
            },
        );
    }

    fn finish_bulk_task_mutation(&self) {
        let remaining = self
            .state
            .borrow()
            .pending
            .values()
            .any(is_bulk_task_mutation);
        if !remaining {
            self.refresh_threads();
        }
        self.update_bulk_task_controls();
    }

    fn unarchive_thread(&self) {
        let Some(thread_id) = self.state.borrow().active_thread_id.clone() else {
            return;
        };
        self.request(
            "thread/unarchive",
            json!({"threadId": thread_id}),
            PendingKind::UnarchiveThread,
        );
    }

    fn load_older_turns(&self) {
        let state = self.state.borrow();
        let Some(thread_id) = state.active_thread_id.clone() else {
            return;
        };
        let Some(cursor) = state.transcript_cursors.get(&thread_id).cloned() else {
            return;
        };
        drop(state);
        self.widgets.load_older_button.set_sensitive(false);
        self.request(
            "thread/turns/list",
            json!({
                "threadId": thread_id,
                "cursor": cursor,
                "limit": TRANSCRIPT_PAGE_TURNS,
                "itemsView": "summary",
                "sortDirection": "desc"
            }),
            PendingKind::ThreadTurns {
                thread_id,
                prepend: true,
            },
        );
    }

    fn request_turn_activity(&self, thread_id: &str, cursor: Option<String>) {
        let mut state = self.state.borrow_mut();
        if state.connection != ConnectionState::Ready
            || !state.transcript_detail_loading.insert(thread_id.to_owned())
        {
            return;
        }
        if cursor.is_none() {
            state.transcript_detail_cursors.remove(thread_id);
        }
        drop(state);

        self.widgets.load_activity_button.set_visible(true);
        self.widgets.load_activity_button.set_sensitive(false);
        self.widgets
            .load_activity_button
            .set_label(if cursor.is_some() {
                "Loading previous activity…"
            } else {
                "Loading recent activity…"
            });
        let mut params = json!({
            "threadId": thread_id,
            "limit": TRANSCRIPT_DETAIL_TURNS,
            "itemsView": "full",
            "sortDirection": "desc"
        });
        if let Some(cursor) = cursor {
            params["cursor"] = Value::String(cursor);
        }
        self.request(
            "thread/turns/list",
            params,
            PendingKind::ThreadTurnDetails {
                thread_id: thread_id.to_owned(),
            },
        );
    }

    fn load_previous_turn_activity(&self) {
        let state = self.state.borrow();
        let Some(thread_id) = state.active_thread_id.clone() else {
            return;
        };
        let cursor = state.transcript_detail_cursors.get(&thread_id).cloned();
        if cursor.is_none() && !state.transcript_detail_available.contains(&thread_id) {
            return;
        }
        drop(state);
        self.request_turn_activity(&thread_id, cursor);
    }

    fn delete_thread_dialog(&self) {
        let Some(thread_id) = self.state.borrow().active_thread_id.clone() else {
            return;
        };
        let dialog = adw::AlertDialog::new(
            Some("Delete this task?"),
            Some("This removes the task history from Codex. This action cannot be undone."),
        );
        dialog.add_responses(&[("cancel", "Cancel"), ("delete", "Delete")]);
        dialog.set_response_appearance("delete", adw::ResponseAppearance::Destructive);
        dialog.set_close_response("cancel");
        let hub = self.hub.clone();
        let state = self.state.clone();
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            move |response| {
                if response.as_str() == "delete" {
                    let id = hub.request("thread/delete", json!({"threadId": thread_id}));
                    state
                        .borrow_mut()
                        .pending
                        .insert(id, PendingKind::DeleteThread);
                }
            },
        );
    }

    fn resolve_approval(&self, decision: &str) {
        let Some(request) = self.state.borrow().approvals.front().cloned() else {
            return;
        };
        if request.method == "item/tool/requestUserInput" && decision.starts_with("accept") {
            self.show_user_input_dialog(request);
            return;
        }
        if request.method == "mcpServer/elicitation/request" && decision.starts_with("accept") {
            self.show_mcp_elicitation_dialog(request);
            return;
        }

        let result = match request.method.as_str() {
            "item/permissions/requestApproval" => {
                let permissions = if decision.starts_with("accept") {
                    request
                        .params
                        .get("permissions")
                        .cloned()
                        .unwrap_or_else(|| json!({}))
                } else {
                    json!({})
                };
                json!({
                    "permissions": permissions,
                    "scope": if decision == "acceptForSession" { "session" } else { "turn" }
                })
            }
            "mcpServer/elicitation/request" => json!({
                "action": if decision.starts_with("accept") { "accept" } else { "decline" },
                "content": if decision.starts_with("accept") { Some(json!({})) } else { None }
            }),
            "item/tool/requestUserInput" => json!({"answers": {}}),
            "execCommandApproval" | "applyPatchApproval" => json!({
                "decision": if decision.starts_with("accept") { "approved" } else { "denied" }
            }),
            _ => json!({"decision": decision}),
        };
        self.hub.respond(request.id.clone(), result);
        self.state.borrow_mut().approvals.pop_front();
    }

    fn show_user_input_dialog(&self, request: ApprovalRequest) {
        let questions = request
            .params
            .get("questions")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let content = gtk::Box::new(gtk::Orientation::Vertical, 10);
        let mut controls: Vec<(String, gtk::Entry, Option<gtk::ComboBoxText>)> = Vec::new();
        for question in questions {
            let id = question
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("answer")
                .to_owned();
            let text = question
                .get("question")
                .or_else(|| question.get("header"))
                .and_then(Value::as_str)
                .unwrap_or("Codex needs input");
            let label = gtk::Label::new(Some(text));
            label.set_wrap(true);
            label.set_xalign(0.0);
            content.append(&label);
            let entry = gtk::Entry::new();
            let combo = question
                .get("options")
                .and_then(Value::as_array)
                .map(|options| {
                    let combo = gtk::ComboBoxText::new();
                    for (index, option) in options.iter().enumerate() {
                        let value = option
                            .get("label")
                            .and_then(Value::as_str)
                            .unwrap_or("Option");
                        combo.append(Some(&index.to_string()), value);
                    }
                    combo.set_active(Some(0));
                    content.append(&combo);
                    combo
                });
            if combo.is_none() {
                if question
                    .get("isSecret")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    entry.set_visibility(false);
                }
                content.append(&entry);
            }
            controls.push((id, entry, combo));
        }
        let controls = Rc::new(controls);
        let dialog = adw::AlertDialog::new(Some("Answer Codex"), None);
        dialog.set_extra_child(Some(&content));
        dialog.add_responses(&[("cancel", "Cancel"), ("submit", "Submit")]);
        dialog.set_default_response(Some("submit"));
        dialog.set_close_response("cancel");
        let hub = self.hub.clone();
        let state = self.state.clone();
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            move |response| {
                if response.as_str() != "submit" {
                    hub.respond(request.id.clone(), json!({"answers": {}}));
                    state
                        .borrow_mut()
                        .approvals
                        .retain(|item| item.id != request.id);
                    return;
                }
                let mut answers = Map::new();
                for (id, entry, combo) in controls.iter() {
                    let answer = combo
                        .as_ref()
                        .and_then(|combo| combo.active_text())
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| entry.text().to_string());
                    answers.insert(id.clone(), json!({"answers": [answer]}));
                }
                hub.respond(request.id.clone(), json!({"answers": answers}));
                state
                    .borrow_mut()
                    .approvals
                    .retain(|item| item.id != request.id);
            },
        );
    }

    fn show_mcp_elicitation_dialog(&self, request: ApprovalRequest) {
        if request.params.get("mode").and_then(Value::as_str) == Some("url") {
            let message = request
                .params
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("This extension needs you to continue in a web browser.");
            let url = request
                .params
                .get("url")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let link = gtk::LinkButton::with_label(&url, "Open in browser");
            let dialog = adw::AlertDialog::new(Some("Extension authorization"), Some(message));
            dialog.set_extra_child(Some(&link));
            dialog.add_responses(&[("decline", "Decline"), ("accept", "Continue")]);
            dialog.set_default_response(Some("accept"));
            dialog.set_close_response("decline");
            let hub = self.hub.clone();
            let state = self.state.clone();
            dialog.choose(
                Some(&self.widgets.window),
                None::<&gio::Cancellable>,
                move |response| {
                    let accepted = response.as_str() == "accept";
                    hub.respond(
                        request.id.clone(),
                        json!({
                            "action": if accepted { "accept" } else { "decline" },
                            "content": null
                        }),
                    );
                    state
                        .borrow_mut()
                        .approvals
                        .retain(|item| item.id != request.id);
                },
            );
            return;
        }

        enum FormControl {
            Text(gtk::Entry, String),
            Boolean(gtk::Switch),
            Choice(gtk::ComboBoxText, Vec<String>),
            MultiChoice(Vec<(gtk::CheckButton, String)>),
        }

        let message = request
            .params
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("An extension needs information from you.");
        let schema = request
            .params
            .get("requestedSchema")
            .cloned()
            .unwrap_or_else(|| json!({"type":"object","properties":{}}));
        let required = schema
            .get("required")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let form = gtk::Grid::builder()
            .column_spacing(12)
            .row_spacing(10)
            .margin_top(4)
            .build();
        let mut controls = Vec::<(String, FormControl)>::new();
        if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
            for (row, (key, property)) in properties.iter().enumerate() {
                let title = property.get("title").and_then(Value::as_str).unwrap_or(key);
                let is_required = required.iter().any(|value| value.as_str() == Some(key));
                let label = if is_required {
                    format!("{title} *")
                } else {
                    title.to_owned()
                };
                let kind = property
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("string");
                let control = if kind == "boolean" {
                    let switch = gtk::Switch::new();
                    switch.set_active(
                        property
                            .get("default")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                    );
                    form.attach(&switch, 1, row as i32, 1, 1);
                    FormControl::Boolean(switch)
                } else if kind == "array"
                    && let Some(options) = elicitation_choices(property)
                {
                    let choices = gtk::Box::new(gtk::Orientation::Vertical, 4);
                    let controls = options
                        .into_iter()
                        .map(|option| {
                            let check = gtk::CheckButton::with_label(&option);
                            choices.append(&check);
                            (check, option)
                        })
                        .collect::<Vec<_>>();
                    form.attach(&choices, 1, row as i32, 1, 1);
                    FormControl::MultiChoice(controls)
                } else if let Some(options) = elicitation_choices(property) {
                    let combo = gtk::ComboBoxText::new();
                    for (index, option) in options.iter().enumerate() {
                        combo.append(Some(&index.to_string()), option);
                    }
                    combo.set_active(Some(0));
                    form.attach(&combo, 1, row as i32, 1, 1);
                    FormControl::Choice(combo, options)
                } else {
                    let entry = gtk::Entry::new();
                    if let Some(default) = property.get("default") {
                        entry.set_text(&value_to_text(default));
                    }
                    if kind == "array" {
                        entry.set_placeholder_text(Some("Comma-separated values"));
                    }
                    entry.set_hexpand(true);
                    form.attach(&entry, 1, row as i32, 1, 1);
                    FormControl::Text(entry, kind.to_owned())
                };
                let label = gtk::Label::new(Some(&label));
                label.set_xalign(0.0);
                form.attach(&label, 0, row as i32, 1, 1);
                controls.push((key.clone(), control));
            }
        }

        let dialog = adw::AlertDialog::new(Some("Extension input"), Some(message));
        dialog.set_extra_child(Some(&form));
        dialog.add_responses(&[("decline", "Decline"), ("submit", "Submit")]);
        dialog.set_default_response(Some("submit"));
        dialog.set_close_response("decline");
        let hub = self.hub.clone();
        let state = self.state.clone();
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            move |response| {
                let accepted = response.as_str() == "submit";
                let content = accepted.then(|| {
                    let mut values = Map::new();
                    for (key, control) in &controls {
                        let value = match control {
                            FormControl::Boolean(switch) => Value::Bool(switch.is_active()),
                            FormControl::Choice(combo, options) => combo
                                .active()
                                .and_then(|index| options.get(index as usize))
                                .cloned()
                                .map(Value::String)
                                .unwrap_or(Value::Null),
                            FormControl::MultiChoice(options) => Value::Array(
                                options
                                    .iter()
                                    .filter(|(check, _)| check.is_active())
                                    .map(|(_, value)| Value::String(value.clone()))
                                    .collect(),
                            ),
                            FormControl::Text(entry, kind) => {
                                let text = entry.text().to_string();
                                match kind.as_str() {
                                    "integer" => text
                                        .parse::<i64>()
                                        .map(Value::from)
                                        .unwrap_or(Value::String(text)),
                                    "number" => text
                                        .parse::<f64>()
                                        .map(Value::from)
                                        .unwrap_or(Value::String(text)),
                                    "array" => Value::Array(
                                        text.split(',')
                                            .map(str::trim)
                                            .filter(|value| !value.is_empty())
                                            .map(|value| Value::String(value.to_owned()))
                                            .collect(),
                                    ),
                                    _ => Value::String(text),
                                }
                            }
                        };
                        values.insert(key.clone(), value);
                    }
                    Value::Object(values)
                });
                hub.respond(
                    request.id.clone(),
                    json!({
                        "action": if accepted { "accept" } else { "decline" },
                        "content": content
                    }),
                );
                state
                    .borrow_mut()
                    .approvals
                    .retain(|item| item.id != request.id);
            },
        );
    }

    fn render_all(&self) {
        self.render_connection();
        self.render_threads();
        self.render_transcript();
        self.render_chatgpt();
        self.render_projects();
        self.render_sites();
        self.render_approval();
        self.render_agent_workspace();
        self.render_context();
        self.render_extensions();
        self.render_qwen();
        self.render_computer_use();
        self.render_automations();
        self.render_remote();
        self.render_diagnostics();
        self.render_account();
    }

    fn render_current_page(&self) {
        self.render_connection();
        let page = self.state.borrow().page;
        match page {
            WorkspacePage::Chat => {
                self.render_transcript();
                self.render_approval();
            }
            WorkspacePage::Chatgpt => self.render_chatgpt(),
            WorkspacePage::Projects => self.render_projects(),
            WorkspacePage::Sites => self.render_sites(),
            WorkspacePage::Terminal => {}
            WorkspacePage::AgentWorkspace => self.render_agent_workspace(),
            WorkspacePage::Context => self.render_context(),
            WorkspacePage::Extensions => {
                self.ensure_extensions_loaded();
                self.render_extensions();
            }
            WorkspacePage::QwenBuddy => {
                self.ensure_qwen_loaded();
                self.render_qwen();
            }
            WorkspacePage::ComputerUse => {
                self.ensure_extensions_loaded();
                self.render_computer_use();
            }
            WorkspacePage::Automations => self.render_automations(),
            WorkspacePage::Remote => self.render_remote(),
            WorkspacePage::Diagnostics => {
                self.ensure_diagnostics_loaded();
                self.render_diagnostics();
            }
            WorkspacePage::Settings => self.render_account(),
        }
    }

    fn render_connection(&self) {
        let send_pending = self.state.borrow().pending.values().any(|kind| {
            matches!(
                kind,
                PendingKind::StartThread { .. }
                    | PendingKind::ResumeAndSend { .. }
                    | PendingKind::SendTurn { .. }
                    | PendingKind::SteerTurn { .. }
                    | PendingKind::UnsubscribeThread(_)
            )
        });
        self.widgets
            .status_icon
            .remove_css_class("connection-ready");
        self.widgets.status_icon.remove_css_class("connection-busy");
        self.widgets
            .status_icon
            .remove_css_class("connection-error");
        match self.state.borrow().connection {
            ConnectionState::Connecting => {
                self.widgets
                    .status_icon
                    .set_icon_name(Some("network-transmit-receive-symbolic"));
                self.widgets.status_icon.add_css_class("connection-busy");
                self.widgets.status_label.set_label("Connecting");
                self.widgets.send_button.set_sensitive(false);
            }
            ConnectionState::Ready => {
                self.widgets
                    .status_icon
                    .set_icon_name(Some("emblem-ok-symbolic"));
                self.widgets.status_icon.add_css_class("connection-ready");
                self.widgets.status_label.set_label("Codex ready");
                self.widgets.send_button.set_sensitive(!send_pending);
            }
            ConnectionState::Disconnected => {
                self.widgets
                    .status_icon
                    .set_icon_name(Some("dialog-error-symbolic"));
                self.widgets.status_icon.add_css_class("connection-error");
                self.widgets.status_label.set_label("Reconnecting");
                self.widgets.send_button.set_sensitive(false);
            }
        }
        let running = self.state.borrow().turn_is_running();
        self.widgets.stop_button.set_sensitive(running);
        let state = self.state.borrow();
        let active_id = state.active_thread_id.as_deref();
        let usage = active_id.and_then(|id| state.thread_token_usage.get(id));
        let goal = active_id.and_then(|id| state.thread_goals.get(id));
        let history_warning = active_id
            .and_then(|id| state.threads.get(id))
            .and_then(|thread| thread.path.as_deref())
            .and_then(|path| std::fs::metadata(path).ok())
            .and_then(|metadata| task_history_rollover_warning(Some(metadata.len())));
        let mut usage_text = if let Some(goal) = goal {
            let status = goal
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("active");
            let used = goal.get("tokensUsed").and_then(Value::as_i64).unwrap_or(0);
            if let Some(budget) = goal.get("tokenBudget").and_then(Value::as_i64) {
                format!(
                    "Goal {} · {} / {} tokens",
                    goal_status_label(status),
                    format_integer(used),
                    format_integer(budget)
                )
            } else {
                format!(
                    "Goal {} · {} tokens",
                    goal_status_label(status),
                    format_integer(used)
                )
            }
        } else if let Some(usage) = usage {
            context_usage_label(usage)
        } else {
            String::new()
        };
        if let Some(history_warning) = history_warning {
            if !usage_text.is_empty() {
                usage_text.push_str(" · ");
            }
            usage_text.push_str(&history_warning);
        }
        self.widgets.usage_label.set_label(&usage_text);
        let reset_guard = self.stored.borrow().one_time_reset_guard.clone();

        if let Some(limits) = state.account_rate_limits.as_ref() {
            if let Some((remaining, resets_at, duration_mins)) = weekly_limit_details(limits) {
                self.widgets
                    .weekly_progress
                    .set_fraction(remaining as f64 / 100.0);
                let guard_label = if reset_guard.armed {
                    format!(" · reset armed ≤{}%", reset_guard.threshold_percent)
                } else {
                    String::new()
                };
                self.widgets
                    .weekly_progress
                    .set_text(Some(&format!("Weekly {remaining}% left{guard_label}")));
                let duration = duration_mins
                    .map(|minutes| format!("{minutes} minute rolling window"))
                    .unwrap_or_else(|| "long rolling window".into());
                let reset = resets_at
                    .and_then(format_timestamp_local)
                    .map(|value| format!(" · resets {value}"))
                    .unwrap_or_default();
                let guard_tooltip = if reset_guard.armed {
                    format!(
                        " · one-time automatic reset armed at {}% remaining",
                        reset_guard.threshold_percent
                    )
                } else {
                    String::new()
                };
                self.widgets
                    .weekly_progress
                    .set_tooltip_text(Some(&format!("{duration}{reset}{guard_tooltip}")));
            } else {
                self.widgets.weekly_progress.set_fraction(0.0);
                self.widgets.weekly_progress.set_text(Some("Weekly —"));
                self.widgets
                    .weekly_progress
                    .set_tooltip_text(Some("Weekly limit data is unavailable for this account"));
            }
            if let Some((remaining, resets_at)) = five_hour_limit_details(limits) {
                self.widgets
                    .five_hour_progress
                    .set_fraction(remaining as f64 / 100.0);
                self.widgets
                    .five_hour_progress
                    .set_text(Some(&format!("5-hour {remaining}% left")));
                let reset = resets_at
                    .and_then(format_timestamp_local)
                    .map(|value| format!(" · resets {value}"))
                    .unwrap_or_default();
                self.widgets
                    .five_hour_progress
                    .set_tooltip_text(Some(&format!("Five-hour rolling window{reset}")));
            } else {
                self.widgets.five_hour_progress.set_fraction(0.0);
                self.widgets.five_hour_progress.set_text(Some("5-hour —"));
                self.widgets
                    .five_hour_progress
                    .set_tooltip_text(Some("Five-hour limit data is unavailable for this account"));
            }
            let reset_count = reset_credit_count(limits);
            if state.reset_credit_in_flight {
                self.widgets.reset_credit_button.set_label("Resetting…");
                self.widgets.reset_credit_button.set_sensitive(false);
            } else {
                self.widgets
                    .reset_credit_button
                    .set_label(&format!("Resets {reset_count}"));
                self.widgets
                    .reset_credit_button
                    .set_sensitive(reset_count > 0);
            }
            self.widgets
                .reset_credit_button
                .set_tooltip_text(Some(&format!(
                    "{reset_count} Codex weekly-limit reset credit{} available{}",
                    if reset_count == 1 { "" } else { "s" },
                    if reset_guard.armed {
                        format!(
                            "; one will be used once at {}% weekly remaining",
                            reset_guard.threshold_percent
                        )
                    } else {
                        String::new()
                    }
                )));
        } else {
            self.widgets.weekly_progress.set_fraction(0.0);
            self.widgets
                .weekly_progress
                .set_text(Some(if reset_guard.armed {
                    "Weekly — · reset armed"
                } else {
                    "Weekly —"
                }));
            self.widgets.five_hour_progress.set_fraction(0.0);
            self.widgets.five_hour_progress.set_text(Some("5-hour —"));
            self.widgets
                .five_hour_progress
                .set_tooltip_text(Some("Five-hour limit data is unavailable for this account"));
            self.widgets.reset_credit_button.set_label("Resets —");
            self.widgets.reset_credit_button.set_sensitive(false);
        }
        drop(state);
        self.update_bulk_task_controls();
    }

    fn render_threads(&self) {
        clear_list_box(&self.widgets.thread_list);
        let search = self.widgets.thread_search.text().trim().to_lowercase();
        let pinned = self.stored.borrow().pinned_threads.clone();
        let owner_labels = {
            let stored = self.stored.borrow();
            stored
                .shared_task_history
                .iter()
                .filter_map(|(thread_id, record)| {
                    stored
                        .account_profiles
                        .iter()
                        .find(|profile| profile.id == record.owner_account_id)
                        .map(|profile| (thread_id.clone(), profile.label.clone()))
                })
                .collect::<HashMap<_, _>>()
        };
        let model_labels = self
            .stored
            .borrow()
            .task_routing
            .iter()
            .filter_map(|(thread_id, routing)| {
                task_model_badge(Some(routing)).map(|label| (thread_id.clone(), label))
            })
            .collect::<HashMap<_, _>>();
        let selection_mode = self.widgets.thread_select_toggle.is_active();
        let selected_thread_ids = self.selected_thread_ids.borrow().clone();
        let state = self.state.borrow();
        let active = state.active_thread_id.as_deref();
        let server_search_active = state
            .thread_search_query
            .as_deref()
            .is_some_and(|query| query.trim().to_lowercase() == search);
        let ordered_ids = visible_thread_ids(&state, &pinned, &search);
        for id in &ordered_ids {
            let Some(thread) = state.threads.get(id) else {
                continue;
            };
            let button = gtk::Button::new();
            button.set_has_frame(false);
            button.add_css_class("thread-row");
            let is_active = active == Some(id.as_str());
            let is_bulk_selected = selected_thread_ids.contains(id);
            let agent_summary = thread_agent_summary(thread, &state);
            let is_running =
                thread_is_running_in_state(id, thread, &state) || agent_summary.running > 0;
            button.update_state(&[gtk::accessible::State::Selected(Some(if selection_mode {
                is_bulk_selected
            } else {
                is_active
            }))]);
            if is_active {
                button.add_css_class("thread-row-active");
            }
            if is_bulk_selected {
                button.add_css_class("thread-row-bulk-selected");
            }
            let row = gtk::Box::new(gtk::Orientation::Vertical, 2);
            let compact_title = compact_ui_text(thread.title(), 180);
            let display_title = if pinned.iter().any(|pinned_id| pinned_id == id) {
                format!("★ {compact_title}")
            } else {
                compact_title
            };
            let title_row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
            title_row.set_hexpand(true);
            if selection_mode {
                let selection_mark =
                    gtk::Label::new(Some(if is_bulk_selected { "☑" } else { "☐" }));
                selection_mark.add_css_class("task-selection-mark");
                selection_mark.set_tooltip_text(Some(if is_bulk_selected {
                    "Selected for bulk action"
                } else {
                    "Not selected"
                }));
                title_row.append(&selection_mark);
            }
            if is_active {
                let current = gtk::Image::from_icon_name("go-next-symbolic");
                current.set_tooltip_text(Some("Currently open task"));
                title_row.append(&current);
            }
            let title = gtk::Label::new(Some(&display_title));
            title.set_xalign(0.0);
            title.set_ellipsize(gtk::pango::EllipsizeMode::End);
            title.set_lines(1);
            title.set_hexpand(true);
            title_row.append(&title);
            if let Some(model_label) = model_labels.get(id) {
                let model = gtk::Label::new(Some(model_label));
                model.add_css_class("caption");
                model.add_css_class("task-model-badge");
                model.set_tooltip_text(Some(&format!("Started with {model_label}")));
                title_row.append(&model);
            }
            if let Some(owner_label) = owner_labels.get(id) {
                let owner = gtk::Label::new(Some(owner_label));
                owner.add_css_class("caption");
                owner.add_css_class("muted");
                owner.set_ellipsize(gtk::pango::EllipsizeMode::End);
                owner.set_max_width_chars(20);
                owner.set_tooltip_text(Some(&format!("Owned by {owner_label}")));
                title_row.append(&owner);
            }
            if agent_summary.total > 0 {
                let badge = gtk::Box::new(gtk::Orientation::Horizontal, 3);
                badge.add_css_class("task-agent-badge");
                let icon = gtk::Image::from_icon_name("system-users-symbolic");
                icon.set_pixel_size(13);
                badge.append(&icon);
                let count = gtk::Label::new(Some(&agent_summary.total.to_string()));
                count.add_css_class("caption");
                badge.append(&count);
                badge.set_tooltip_text(Some(&agent_summary.tooltip));
                title_row.append(&badge);
            }
            if is_running {
                let spinner = self.activity_indicator(Some(thread_running_description(thread)));
                // Keep the activity ring last so it stays at the far-right edge
                // of the task row, matching the Codex task-list convention.
                title_row.append(&spinner);
            }
            row.append(&title_row);
            let subtitle_text = thread_subtitle(thread);
            let subtitle = gtk::Label::new(Some(&subtitle_text));
            subtitle.set_xalign(0.0);
            subtitle.set_ellipsize(gtk::pango::EllipsizeMode::Middle);
            subtitle.add_css_class("caption");
            if agent_summary.total > 0 {
                subtitle.set_tooltip_text(Some(&agent_summary.tooltip));
            }
            row.append(&subtitle);
            if server_search_active && let Some(snippet) = state.thread_search_snippets.get(id) {
                let snippet = gtk::Label::new(Some(&compact_ui_text(snippet, 260)));
                snippet.set_xalign(0.0);
                snippet.set_wrap(true);
                snippet.set_wrap_mode(gtk::pango::WrapMode::WordChar);
                snippet.set_lines(2);
                snippet.set_ellipsize(gtk::pango::EllipsizeMode::End);
                snippet.add_css_class("caption");
                snippet.add_css_class("muted");
                snippet.set_tooltip_text(Some(snippet.text().as_str()));
                row.append(&snippet);
            }
            button.set_child(Some(&row));

            let is_pinned = pinned.iter().any(|pinned_id| pinned_id == id);
            let weak_controller = self.weak_self.borrow().clone();
            let context_thread_id = id.clone();
            let context_gesture = gtk::GestureClick::new();
            context_gesture.set_button(thread_context_mouse_button());
            let weak_button = button.downgrade();
            context_gesture.connect_pressed(move |_, _, x, y| {
                let Some(button) = weak_button.upgrade() else {
                    return;
                };
                let context_menu = gtk::Popover::new();
                context_menu.set_autohide(true);
                context_menu.set_parent(&button);
                let context_content = gtk::Box::new(gtk::Orientation::Vertical, 2);
                context_content.set_margin_top(6);
                context_content.set_margin_bottom(6);
                context_content.set_margin_start(6);
                context_content.set_margin_end(6);
                let pin_action = gtk::Button::with_label(pin_context_action_label(is_pinned));
                pin_action.add_css_class("flat");
                context_content.append(&pin_action);
                context_menu.set_child(Some(&context_content));

                let weak_controller = weak_controller.clone();
                let context_thread_id = context_thread_id.clone();
                let context_menu_for_action = context_menu.clone();
                pin_action.connect_clicked(move |_| {
                    context_menu_for_action.popdown();
                    if context_menu_for_action.parent().is_some() {
                        context_menu_for_action.unparent();
                    }
                    with_controller(&weak_controller, |controller| {
                        controller.toggle_thread_pin(&context_thread_id)
                    });
                });
                context_menu.connect_closed(|context_menu| {
                    if context_menu.parent().is_some() {
                        context_menu.unparent();
                    }
                });
                let pointing_to = gdk::Rectangle::new(x as i32, y as i32, 1, 1);
                context_menu.set_pointing_to(Some(&pointing_to));
                context_menu.popup();
            });
            button.add_controller(context_gesture);

            let open_controller = self.weak_self.borrow().clone();
            let id = id.clone();
            button.connect_clicked(move |_| {
                if selection_mode {
                    with_controller(&open_controller, |controller| {
                        controller.toggle_task_selection(&id)
                    });
                    return;
                }
                with_controller(&open_controller, |controller| {
                    controller.open_timeline_thread(&id);
                });
            });
            self.widgets.thread_list.append(&button);
        }
        drop(state);
        self.update_bulk_task_controls();
    }

    fn render_transcript(&self) {
        self.render_goal_selector();
        let show_reasoning = self.stored.borrow().preferences.show_reasoning;
        let (active_thread_id, transcript_revision) = {
            let state = self.state.borrow();
            (state.active_thread_id.clone(), state.transcript_revision)
        };
        let unchanged = self.last_transcript_revision.get() == Some(transcript_revision)
            && self.last_transcript_reasoning.get() == show_reasoning
            && self.last_transcript_thread.borrow().as_ref() == active_thread_id.as_ref();
        if unchanged {
            return;
        }

        let adjustment = self.widgets.transcript_scroller.vadjustment();
        let previous_value = adjustment.value();
        let previous_upper = adjustment.upper();
        let was_at_bottom =
            self.transcript_follow_bottom.get() || adjustment_is_near_bottom(&adjustment);
        let thread_changed =
            self.last_transcript_thread.borrow().as_ref() != active_thread_id.as_ref();
        let prepended = self.transcript_prepend_pending.replace(false);
        if thread_changed {
            self.transcript_follow_bottom.set(true);
        }

        let routing_state = {
            let stored = self.stored.borrow();
            active_thread_id
                .as_ref()
                .and_then(|thread_id| stored.task_routing.get(thread_id).cloned())
        };

        self.last_transcript_revision.set(Some(transcript_revision));
        self.last_transcript_reasoning.set(show_reasoning);
        *self.last_transcript_thread.borrow_mut() = active_thread_id;
        let state = self.state.borrow();
        let Some(thread) = state.active_thread().cloned() else {
            self.widgets.task_title.set_label("New task");
            self.render_task_progress(None);
            self.widgets.load_older_button.set_visible(false);
            self.widgets.load_activity_button.set_visible(false);
            self.reset_transcript_rows();
            let welcome = gtk::Box::new(gtk::Orientation::Vertical, 8);
            welcome.set_halign(gtk::Align::Center);
            welcome.set_valign(gtk::Align::Center);
            welcome.set_vexpand(true);
            let icon = gtk::Image::from_icon_name("applications-development-symbolic");
            icon.set_pixel_size(52);
            welcome.append(&icon);
            let title = gtk::Label::new(Some("What should we build?"));
            title.add_css_class("panel-title");
            welcome.append(&title);
            let text = gtk::Label::new(Some(
                "Choose a project folder, describe the task, and Codex will work through the shared native backend.",
            ));
            text.set_wrap(true);
            text.set_justify(gtk::Justification::Center);
            text.add_css_class("muted");
            welcome.append(&text);
            self.widgets.transcript.append(&welcome);
            self.set_thread_action_sensitivity(false);
            drop(state);
            self.restore_transcript_scroll(
                adjustment,
                true,
                false,
                false,
                previous_value,
                previous_upper,
            );
            return;
        };
        self.widgets
            .task_title
            .set_label(&compact_ui_text(thread.title(), 240));
        self.set_thread_action_sensitivity(true);
        let is_pinned = self
            .stored
            .borrow()
            .pinned_threads
            .iter()
            .any(|id| id == &thread.id);
        self.widgets.pin_button.set_icon_name(if is_pinned {
            "starred-symbolic"
        } else {
            "non-starred-symbolic"
        });
        let archived = self.stored.borrow().show_archived_threads;
        self.widgets.archive_button.set_visible(!archived);
        self.widgets.unarchive_button.set_visible(archived);
        self.widgets
            .load_older_button
            .set_visible(state.transcript_cursors.contains_key(&thread.id));
        self.widgets.load_older_button.set_sensitive(true);
        let detail_loading = state.transcript_detail_loading.contains(&thread.id);
        let has_detail_cursor = state.transcript_detail_cursors.contains_key(&thread.id);
        let has_recent_activity = state.transcript_detail_available.contains(&thread.id);
        self.widgets
            .load_activity_button
            .set_visible(detail_loading || has_detail_cursor || has_recent_activity);
        self.widgets
            .load_activity_button
            .set_sensitive((has_detail_cursor || has_recent_activity) && !detail_loading);
        self.widgets
            .load_activity_button
            .set_label(if detail_loading {
                "Loading activity…"
            } else if has_detail_cursor {
                "Load previous activity"
            } else {
                "Load recent activity"
            });
        self.render_task_progress(state.turn_progress.get(&thread.id));
        let rows = transcript_row_specs(&thread, &state, show_reasoning, routing_state.as_ref());
        tracing::debug!(
            thread_id = %thread.id,
            turn_count = thread.turns.len(),
            row_count = rows.len(),
            "reconciling active transcript"
        );
        drop(state);
        self.reconcile_transcript_rows(rows, thread_changed);
        self.restore_transcript_scroll(
            adjustment,
            thread_changed || was_at_bottom,
            prepended,
            !thread_changed && was_at_bottom && !prepended,
            previous_value,
            previous_upper,
        );
    }

    fn reset_transcript_rows(&self) {
        clear_box(&self.widgets.transcript);
        self.transcript_rows.borrow_mut().clear();
    }

    fn render_task_progress(&self, progress: Option<&TurnProgress>) {
        clear_box(&self.widgets.task_progress);
        if let Some(progress) = progress {
            let spinner = self.activity_indicator(Some("Task is running"));
            self.widgets
                .task_progress
                .append(&task_progress_pill(progress, &spinner));
            self.widgets.task_progress.set_visible(true);
        } else {
            self.widgets.task_progress.set_visible(false);
        }
    }

    fn reconcile_transcript_rows(&self, specs: Vec<TranscriptRowSpec>, reset: bool) {
        let mut existing = std::mem::take(&mut *self.transcript_rows.borrow_mut());
        if reset {
            clear_box(&self.widgets.transcript);
            existing.clear();
        }
        let mut rendered = Vec::with_capacity(specs.len());
        let mut previous: Option<gtk::Widget> = None;

        for spec in specs {
            let existing_index = existing.iter().position(|row| row.key == spec.key);
            let old = existing_index.map(|index| existing.remove(index));
            let built = if let Some(mut old) = old {
                if old.fingerprint == spec.fingerprint {
                    let expected_previous = old.widget.prev_sibling();
                    if expected_previous.as_ref() != previous.as_ref() {
                        self.widgets
                            .transcript
                            .reorder_child_after(&old.widget, previous.as_ref());
                    }
                    Some(BuiltTranscriptRow {
                        widget: old.widget,
                        streaming_body: old.streaming_body,
                    })
                } else if let (Some(body), Some(text)) = (
                    old.streaming_body.as_ref(),
                    streamed_agent_message_text(&spec.content),
                ) {
                    body.set_label(text);
                    old.fingerprint = spec.fingerprint;
                    Some(BuiltTranscriptRow {
                        widget: old.widget,
                        streaming_body: old.streaming_body,
                    })
                } else {
                    self.widgets.transcript.remove(&old.widget);
                    self.build_transcript_row(&spec.content).inspect(|built| {
                        self.widgets
                            .transcript
                            .insert_child_after(&built.widget, previous.as_ref());
                    })
                }
            } else {
                self.build_transcript_row(&spec.content).inspect(|built| {
                    self.widgets
                        .transcript
                        .insert_child_after(&built.widget, previous.as_ref());
                })
            };
            let Some(built) = built else { continue };
            previous = Some(built.widget.clone());
            rendered.push(RenderedTranscriptRow {
                key: spec.key,
                fingerprint: spec.fingerprint,
                widget: built.widget,
                streaming_body: built.streaming_body,
            });
        }
        for stale in existing {
            self.widgets.transcript.remove(&stale.widget);
        }
        *self.transcript_rows.borrow_mut() = rendered;
    }

    fn build_transcript_row(&self, content: &TranscriptRowContent) -> Option<BuiltTranscriptRow> {
        let row = gtk::Box::new(gtk::Orientation::Vertical, 6);
        row.add_css_class("transcript-row");
        let mut streaming_body = None;
        match content {
            TranscriptRowContent::Route(route) => {
                let receipt = gtk::Label::new(Some(&route.display_label()));
                receipt.set_xalign(0.0);
                receipt.set_tooltip_text(Some(&route.reason));
                receipt.add_css_class("route-receipt");
                receipt.update_property(&[gtk::accessible::Property::Label(&format!(
                    "Turn settings: {}. {}",
                    route.display_label(),
                    route.reason
                ))]);
                row.append(&receipt);
            }
            TranscriptRowContent::TokenSavings(receipt) => {
                let label = gtk::Label::new(Some(&receipt.display_label()));
                label.set_xalign(0.0);
                label.set_tooltip_text(Some(&receipt.tooltip()));
                label.add_css_class("route-receipt");
                label.add_css_class("token-savings-receipt");
                label.update_property(&[
                    gtk::accessible::Property::Label(&receipt.display_label()),
                    gtk::accessible::Property::Description(&receipt.tooltip()),
                ]);
                row.append(&label);
            }
            TranscriptRowContent::Item {
                item,
                streamed_text,
                show_reasoning,
                cwd,
            } if streamed_agent_message_text(content).is_some() => {
                let author = item
                    .get("author")
                    .and_then(Value::as_str)
                    .unwrap_or("Codex");
                let (card, body) = streaming_message_card(author, streamed_text);
                row.append(&card);
                streaming_body = Some(body);
            }
            TranscriptRowContent::Item {
                item,
                streamed_text,
                show_reasoning,
                cwd,
            } => self.append_item_to(&row, item, streamed_text, *show_reasoning, cwd),
            TranscriptRowContent::Images { images, verb } => {
                self.append_image_activity_to(&row, images, verb)
            }
            TranscriptRowContent::TurnError(error) => row.append(&message_card(
                "Turn error",
                &pretty_value(error),
                "message-error",
            )),
        }
        row.first_child()?;
        Some(BuiltTranscriptRow {
            widget: row.upcast(),
            streaming_body,
        })
    }

    fn set_transcript_scroll_value(&self, adjustment: &gtk::Adjustment, value: f64) {
        self.transcript_scroll_restoring.set(true);
        adjustment.set_value(value);
        self.transcript_scroll_restoring.set(false);
    }

    fn cancel_transcript_scroll_animation(&self) {
        self.transcript_scroll_animation_generation.set(
            self.transcript_scroll_animation_generation
                .get()
                .wrapping_add(1),
        );
    }

    fn animate_transcript_scroll_to(&self, adjustment: &gtk::Adjustment, target: f64) {
        let maximum = (adjustment.upper() - adjustment.page_size()).max(0.0);
        let target = target.clamp(0.0, maximum);
        let start = adjustment.value();
        self.cancel_transcript_scroll_animation();
        if (target - start).abs() < 0.5 {
            self.set_transcript_scroll_value(adjustment, target);
            return;
        }
        let generation = self.transcript_scroll_animation_generation.get();
        let adjustment = adjustment.clone();
        let started = Instant::now();
        let weak = self.weak_self.borrow().clone();
        glib::timeout_add_local(TRANSCRIPT_SCROLL_FRAME, move || {
            let Some(controller) = weak.upgrade() else {
                return glib::ControlFlow::Break;
            };
            if controller.transcript_scroll_animation_generation.get() != generation {
                return glib::ControlFlow::Break;
            }
            let progress = eased_transcript_scroll_progress(started.elapsed());
            controller
                .set_transcript_scroll_value(&adjustment, start + (target - start) * progress);
            if progress >= 1.0 {
                glib::ControlFlow::Break
            } else {
                glib::ControlFlow::Continue
            }
        });
    }

    fn restore_transcript_scroll(
        &self,
        adjustment: gtk::Adjustment,
        follow_bottom: bool,
        prepended: bool,
        animate_follow_bottom: bool,
        previous_value: f64,
        previous_upper: f64,
    ) {
        if !follow_bottom && !prepended {
            return;
        }
        let weak = self.weak_self.borrow().clone();
        glib::idle_add_local_once(move || {
            with_controller(&weak, |controller| {
                let target = transcript_scroll_target(
                    follow_bottom,
                    prepended,
                    previous_value,
                    previous_upper,
                    adjustment.upper(),
                    adjustment.page_size(),
                );
                if follow_bottom {
                    controller.transcript_follow_bottom.set(true);
                }
                if animate_follow_bottom {
                    controller.animate_transcript_scroll_to(&adjustment, target);
                } else {
                    controller.cancel_transcript_scroll_animation();
                    controller.set_transcript_scroll_value(&adjustment, target);
                }
            });
        });
    }

    fn render_goal_selector(&self) {
        let state = self.state.borrow();
        let Some(thread_id) = state.active_thread_id.as_deref() else {
            self.widgets
                .goal_button
                .set_icon_name("emblem-important-symbolic");
            self.widgets
                .goal_button
                .set_tooltip_text(Some("Goal selector"));
            self.widgets.goal_summary.set_label("No task selected");
            self.widgets
                .goal_detail
                .set_label("Open a task before creating a goal.");
            self.widgets.goal_reason.set_visible(false);
            self.widgets.goal_edit.set_label("Create goal…");
            self.widgets.goal_edit.set_sensitive(false);
            for button in [
                &self.widgets.goal_stop,
                &self.widgets.goal_resume,
                &self.widgets.goal_complete,
                &self.widgets.goal_clear,
            ] {
                button.set_visible(false);
            }
            return;
        };
        let Some(goal) = state.thread_goals.get(thread_id) else {
            self.widgets
                .goal_button
                .set_icon_name("emblem-important-symbolic");
            self.widgets
                .goal_button
                .set_tooltip_text(Some("Create a goal for this task"));
            self.widgets.goal_summary.set_label("No goal for this task");
            self.widgets.goal_detail.set_label(
                "Create a goal to preserve an objective, optional token budget, and progress across sessions.",
            );
            self.widgets.goal_reason.set_visible(false);
            self.widgets.goal_edit.set_label("Create goal…");
            self.widgets.goal_edit.set_sensitive(true);
            for button in [
                &self.widgets.goal_stop,
                &self.widgets.goal_resume,
                &self.widgets.goal_complete,
                &self.widgets.goal_clear,
            ] {
                button.set_visible(false);
            }
            return;
        };

        let objective = goal
            .get("objective")
            .and_then(Value::as_str)
            .unwrap_or("Untitled goal");
        let status = goal
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("active");
        let local_reason = self
            .stored
            .borrow()
            .goal_stop_reasons
            .get(thread_id)
            .cloned();
        let reason = local_reason
            .as_deref()
            .or_else(|| goal_status_reason(status));
        let interrupted = reason.is_some() && status == "active";
        let stopped = interrupted
            || matches!(
                status,
                "paused" | "blocked" | "usageLimited" | "budgetLimited"
            );
        let status_label = if interrupted {
            "Interrupted"
        } else {
            goal_status_label(status)
        };
        self.widgets
            .goal_summary
            .set_label(&format!("{status_label} goal"));
        let used = goal.get("tokensUsed").and_then(Value::as_i64).unwrap_or(0);
        let seconds = goal
            .get("timeUsedSeconds")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let budget = goal.get("tokenBudget").and_then(Value::as_i64);
        let token_progress = budget
            .map(|budget| {
                format!(
                    "{} / {} tokens",
                    format_integer(used),
                    format_integer(budget)
                )
            })
            .unwrap_or_else(|| format!("{} tokens", format_integer(used)));
        self.widgets.goal_detail.set_label(&format!(
            "{objective}\n{token_progress} · {}",
            format_goal_time(seconds)
        ));
        self.widgets.goal_reason.set_visible(reason.is_some());
        self.widgets
            .goal_reason
            .set_label(reason.unwrap_or_default());
        let icon = match status {
            "active" if !interrupted => "media-playback-start-symbolic",
            "complete" => "emblem-ok-symbolic",
            "paused" => "media-playback-pause-symbolic",
            _ => "dialog-warning-symbolic",
        };
        self.widgets.goal_button.set_icon_name(icon);
        self.widgets
            .goal_button
            .set_tooltip_text(Some(&format!("Goal: {status_label} — {objective}")));
        self.widgets.goal_edit.set_label("Edit goal…");
        self.widgets.goal_edit.set_sensitive(true);
        self.widgets
            .goal_stop
            .set_visible(status == "active" && !interrupted);
        self.widgets.goal_resume.set_visible(stopped);
        self.widgets.goal_complete.set_visible(status != "complete");
        self.widgets.goal_clear.set_visible(true);
    }

    fn append_item_to(
        &self,
        target: &gtk::Box,
        item: &Value,
        streamed: &str,
        show_reasoning: bool,
        thread_cwd: &str,
    ) {
        let kind = item.get("type").and_then(Value::as_str).unwrap_or("item");
        match kind {
            "userMessage" => {
                let text = user_message_text(item);
                if !text.trim().is_empty() {
                    target.append(&message_card("You", &text, "message-user"));
                }
                let images = item_image_sources(item, thread_cwd);
                self.append_image_activity_to(target, &images, "Attached");
            }
            "agentMessage" => {
                let text = if streamed.is_empty() {
                    item.get("text").and_then(Value::as_str).unwrap_or_default()
                } else {
                    streamed
                };
                if !text.trim().is_empty() {
                    let author = item
                        .get("author")
                        .and_then(Value::as_str)
                        .unwrap_or("Codex");
                    target.append(&message_card(author, text, "message-assistant"));
                }
            }
            "reasoning" if show_reasoning => {
                let text = extract_reasoning(item);
                if !text.is_empty() {
                    let author = item.get("author").and_then(Value::as_str);
                    let label = match (author, item.get("reasoningKind").and_then(Value::as_str)) {
                        (Some("Qwen"), Some("activity")) => "Qwen verbose activity".to_owned(),
                        (Some("Qwen"), _) => "Qwen verbose thinking and activity".to_owned(),
                        (Some(author), Some("activity")) => format!("{author} activity"),
                        (Some(author), _) => format!("{author} thinking and activity"),
                        _ => "Reasoning summary".to_owned(),
                    };
                    let expander = gtk::Expander::builder()
                        .label(&label)
                        .expanded(author == Some("Qwen"))
                        .build();
                    expander.set_child(Some(&markdown::render_rich(&text)));
                    expander.add_css_class("reasoning-card");
                    target.append(&expander);
                }
            }
            "commandExecution" => {
                let command = item
                    .get("command")
                    .map(value_to_text)
                    .unwrap_or_else(|| "Command".into());
                let output = item
                    .get("aggregatedOutput")
                    .or_else(|| item.get("output"))
                    .map(value_to_text)
                    .unwrap_or_default();
                target.append(&tool_card(
                    &format!("{} · {}", command_activity_label(item), status_text(item)),
                    &command,
                    &output,
                    "sh",
                ));
            }
            "fileChange" => {
                let (summary, changes) = file_change_display(item);
                let card = tool_card(
                    &format!("{} · {}", file_activity_label(item), status_text(item)),
                    &summary,
                    &changes,
                    "diff",
                );
                self.append_file_actions(&card, item, thread_cwd);
                target.append(&card);
            }
            "imageView" => {
                let images = item_image_sources(item, thread_cwd);
                self.append_image_activity_to(target, &images, "Viewed");
            }
            "imageGeneration" => {
                let images = item_image_sources(item, thread_cwd);
                if images.is_empty() {
                    let detail = item.get("result").map(pretty_value).unwrap_or_default();
                    target.append(&tool_card(
                        &format!("Generated image · {}", status_text(item)),
                        "Image generation",
                        &detail,
                        "json",
                    ));
                } else {
                    self.append_image_activity_to(target, &images, "Generated");
                }
            }
            "mcpToolCall" | "dynamicToolCall" | "webSearch" => {
                let title = item
                    .get("tool")
                    .or_else(|| item.get("name"))
                    .or_else(|| item.get("query"))
                    .map(value_to_text)
                    .unwrap_or_else(|| kind.to_owned());
                let detail = item
                    .get("result")
                    .or_else(|| item.get("output"))
                    .or_else(|| item.get("arguments"))
                    .map(pretty_value)
                    .unwrap_or_default();
                target.append(&tool_card(
                    &format!("Used tool · {}", status_text(item)),
                    &title,
                    &detail,
                    "json",
                ));
            }
            "hookPrompt"
            | "collabAgentToolCall"
            | "subAgentActivity"
            | "sleep"
            | "enteredReviewMode"
            | "exitedReviewMode"
            | "contextCompaction" => {
                if let Some(activity) = canonical_activity_presentation(item) {
                    target.append(&tool_card(
                        &activity.title,
                        &activity.summary,
                        &activity.detail,
                        activity.language,
                    ));
                }
            }
            "plan" => {
                let text = item
                    .get("text")
                    .or_else(|| item.get("plan"))
                    .map(value_to_text)
                    .unwrap_or_default();
                if !text.is_empty() {
                    target.append(&message_card("Plan", &text, "message-tool"));
                }
            }
            _ => {
                let display = item.get("text").map(value_to_text).unwrap_or_default();
                if !display.is_empty() {
                    target.append(&message_card(kind, &display, "message-tool"));
                }
            }
        }
    }

    fn append_image_activity_to(&self, target: &gtk::Box, images: &[String], verb: &str) {
        if images.is_empty() {
            return;
        }
        let group = gtk::Box::new(gtk::Orientation::Vertical, 4);
        group.add_css_class("image-activity-group");
        let row = image_activity_row(images, verb);
        let window = self.widgets.window.clone();
        let viewer_images = images.to_vec();
        row.connect_clicked(move |_| present_image_viewer(&window, viewer_images.clone(), 0));
        group.append(&row);

        let thumbnails = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        thumbnails.add_css_class("image-thumbnail-strip");
        for (index, source) in images.iter().take(8).enumerate() {
            let thumbnail = image_thumbnail_button(source, index);
            let window = self.widgets.window.clone();
            let viewer_images = images.to_vec();
            thumbnail.connect_clicked(move |_| {
                present_image_viewer(&window, viewer_images.clone(), index)
            });
            thumbnails.append(&thumbnail);
        }
        if images.len() > 8 {
            let more = gtk::Label::new(Some(&format!("+{}", images.len() - 8)));
            more.add_css_class("muted");
            more.set_tooltip_text(Some("Open the image viewer to browse every image"));
            thumbnails.append(&more);
        }
        let thumbnail_scroller = gtk::ScrolledWindow::builder()
            .hscrollbar_policy(gtk::PolicyType::Automatic)
            .vscrollbar_policy(gtk::PolicyType::Never)
            .propagate_natural_height(true)
            .child(&thumbnails)
            .build();
        thumbnail_scroller.add_css_class("image-thumbnail-scroller");
        group.append(&thumbnail_scroller);
        target.append(&group);
    }

    fn append_file_actions(&self, card: &gtk::Box, item: &Value, thread_cwd: &str) {
        let paths = file_change_paths(item, thread_cwd);
        if paths.is_empty() {
            return;
        }
        let actions = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        actions.add_css_class("file-action-row");
        for path in paths.into_iter().take(12) {
            let display = path
                .strip_prefix(thread_cwd)
                .unwrap_or(&path)
                .to_string_lossy();
            let button = gtk::Button::with_label(&compact_ui_text(&display, 110));
            button.add_css_class("file-action");
            button.set_tooltip_text(Some(&format!(
                "Open {} in your configured editor",
                path.display()
            )));
            button.set_sensitive(path.exists());
            let hub = self.hub.clone();
            let preferred_binary = self.stored.borrow().preferences.editor_command.clone();
            button.connect_clicked(move |_| {
                hub.host(HostAction::OpenEditor {
                    path: path.clone(),
                    preferred_binary: preferred_binary.clone(),
                });
            });
            actions.append(&button);
        }
        let scroller = gtk::ScrolledWindow::builder()
            .hscrollbar_policy(gtk::PolicyType::Automatic)
            .vscrollbar_policy(gtk::PolicyType::Never)
            .propagate_natural_height(true)
            .child(&actions)
            .build();
        card.append(&scroller);
    }

    fn set_thread_action_sensitivity(&self, enabled: bool) {
        self.widgets.pin_button.set_sensitive(enabled);
        self.widgets.goal_button.set_sensitive(enabled);
        self.widgets.compact_button.set_sensitive(enabled);
        self.widgets.rollback_button.set_sensitive(enabled);
        self.widgets.rename_button.set_sensitive(enabled);
        self.widgets.fork_button.set_sensitive(enabled);
        self.widgets.archive_button.set_sensitive(enabled);
        self.widgets.unarchive_button.set_sensitive(enabled);
        self.widgets.delete_button.set_sensitive(enabled);
    }

    fn render_approval(&self) {
        let state = self.state.borrow();
        let Some(request) = state.approvals.front() else {
            self.widgets.approval_revealer.set_reveal_child(false);
            return;
        };
        self.widgets.approval_revealer.set_reveal_child(true);
        let title = match request.method.as_str() {
            "item/commandExecution/requestApproval" | "execCommandApproval" => "Command approval",
            "item/fileChange/requestApproval" | "applyPatchApproval" => "File change approval",
            "item/permissions/requestApproval" => "Additional permissions",
            "item/tool/requestUserInput" => "Codex has a question",
            "mcpServer/elicitation/request" => "Extension needs input",
            _ => "Codex request",
        };
        self.widgets.approval_title.set_label(title);
        self.widgets
            .approval_detail
            .set_label(&approval_description(request));
        let needs_answer = request.method == "item/tool/requestUserInput";
        self.widgets.approval_once.set_label(if needs_answer {
            "Answer…"
        } else {
            "Allow once"
        });
        self.widgets.approval_session.set_visible(!needs_answer);
    }

    fn render_projects(&self) {
        clear_box(&self.widgets.projects_list);
        let selected = PathBuf::from(self.current_cwd_string());
        let projects = self.stored.borrow().projects.clone();
        self.widgets.projects_status.set_label(&format!(
            "{} saved project{} · Selected: {}",
            projects.len(),
            if projects.len() == 1 { "" } else { "s" },
            selected.display()
        ));
        if projects.is_empty() {
            self.widgets.projects_list.append(&empty_label(
                "Add a project folder to make it available to new tasks.",
            ));
            return;
        }

        let preferred_editor = self.stored.borrow().preferences.editor_command.clone();
        for project in projects {
            let row = gtk::Box::new(gtk::Orientation::Horizontal, 10);
            row.add_css_class("settings-card");
            let text = gtk::Box::new(gtk::Orientation::Vertical, 3);
            text.set_hexpand(true);
            let title = gtk::Label::new(Some(&project.name));
            title.set_xalign(0.0);
            title.add_css_class("heading");
            text.append(&title);
            let path_label = gtk::Label::new(Some(&project.path.to_string_lossy()));
            path_label.set_xalign(0.0);
            path_label.set_selectable(true);
            path_label.set_ellipsize(gtk::pango::EllipsizeMode::Middle);
            path_label.add_css_class("caption");
            text.append(&path_label);
            row.append(&text);

            let is_selected = project.path == selected;
            let select = gtk::Button::with_label(if is_selected {
                "Selected"
            } else {
                "Use project"
            });
            select.set_sensitive(!is_selected);
            let combo = self.widgets.project_combo.clone();
            let project_id = project.path.to_string_lossy().into_owned();
            select.connect_clicked(move |_| {
                combo.set_active_id(Some(&project_id));
            });
            row.append(&select);

            let open = gtk::Button::with_label("Open in editor");
            let hub = self.hub.clone();
            let path = project.path.clone();
            let preferred_binary = preferred_editor.clone();
            open.connect_clicked(move |_| {
                hub.host(HostAction::OpenEditor {
                    path: path.clone(),
                    preferred_binary: preferred_binary.clone(),
                });
            });
            row.append(&open);

            let reveal = gtk::Button::with_label("Reveal folder");
            let hub = self.hub.clone();
            let path = project.path.clone();
            reveal.connect_clicked(move |_| hub.host(HostAction::Reveal(path.clone())));
            row.append(&reveal);
            self.widgets.projects_list.append(&row);
        }
    }

    fn render_sites(&self) {
        let sites = self.state.borrow().plugins.find_entry(|entry| {
            entry.summary.name.eq_ignore_ascii_case("sites")
                || entry.display_name().eq_ignore_ascii_case("sites")
        });
        let (status, ready) = match sites {
            Some(entry) if entry.summary.installed && entry.summary.enabled => (
                format!(
                    "Sites is installed and enabled for {}.",
                    self.widgets
                        .project_combo
                        .active_text()
                        .as_deref()
                        .unwrap_or("the selected project")
                ),
                true,
            ),
            Some(entry) if entry.summary.installed => (
                "Sites is installed but disabled. Enable it in Plugins before starting a task."
                    .to_owned(),
                false,
            ),
            _ => (
                "Sites is not installed. Open Plugins to install or enable it.".to_owned(),
                false,
            ),
        };
        self.widgets.sites_status.set_label(&status);
        self.widgets.sites_new_task.set_sensitive(ready);
    }

    fn render_context(&self) {
        let mut preferences = self.stored.borrow().preferences.lean_context.clone();
        preferences.normalize();
        let (thread_id, usage, guards, measuring) = {
            let state = self.state.borrow();
            let thread_id = state.active_thread_id.clone();
            let usage = context::usage_snapshot(
                thread_id
                    .as_ref()
                    .and_then(|thread_id| state.thread_token_usage.get(thread_id)),
            );
            let thread_running = thread_id.as_ref().is_some_and(|thread_id| {
                state.threads.get(thread_id).is_some_and(thread_is_running)
                    || (state.active_thread_id.as_deref() == Some(thread_id.as_str())
                        && state.active_turn_id.is_some())
            });
            let compact_pending = thread_id.as_ref().is_some_and(|thread_id| {
                state.pending.values().any(|pending| {
                    matches!(
                        pending,
                        PendingKind::CompactThread {
                            thread_id: pending_thread_id,
                            ..
                        } if pending_thread_id == thread_id
                    )
                })
            });
            let checkpoint_pending = thread_id
                .as_ref()
                .is_some_and(|thread_id| state.context_checkpoint_inflight.contains(thread_id));
            let measuring = thread_id.as_ref().is_some_and(|thread_id| {
                state
                    .context_compaction_measurements
                    .contains_key(thread_id)
            });
            (
                thread_id,
                usage,
                CompactionGuards {
                    connection_ready: state.connection == ConnectionState::Ready,
                    thread_running,
                    pending_approval: !state.approvals.is_empty(),
                    request_inflight: checkpoint_pending || compact_pending || measuring,
                },
                measuring,
            )
        };
        let checkpoint_current = {
            let stored = self.stored.borrow();
            thread_id.as_ref().is_some_and(|thread_id| {
                stored
                    .context_metrics
                    .get(thread_id)
                    .is_some_and(|metrics| {
                        metrics.last_checkpoint_generation == Some(metrics.context_generation)
                    })
            })
        };
        let mut guards = guards;
        guards.request_inflight |= checkpoint_current;
        let decision = context::compaction_decision(&preferences, usage, guards);
        let status = if thread_id.is_none() {
            "Select a task to view its context budget and checkpoint history.".to_owned()
        } else if checkpoint_current {
            format!(
                "Context usage: {}% · this context generation is checkpointed; Codex owns automatic rollover.",
                usage.percent
            )
        } else {
            match decision {
                CompactionDecision::Disabled => {
                    "Lean Context is disabled. Raw Codex history is unchanged.".into()
                }
                CompactionDecision::WaitingForUsage => {
                    "Waiting for model context-usage telemetry from Codex.".into()
                }
                CompactionDecision::BelowThreshold(percent) => format!(
                    "Context usage: {percent}% · checkpoint threshold: {}% · same-thread sync protected.",
                    preferences.compact_threshold_percent
                ),
                CompactionDecision::Blocked(reason) => format!(
                    "Context usage: {}% · checkpoint safely paused because {reason}.",
                    usage.percent
                ),
                CompactionDecision::Observe(percent) => format!(
                    "Context usage: {percent}% · observe-only mode reports pressure without checkpointing."
                ),
                CompactionDecision::Prompt(percent) => format!(
                    "Context usage: {percent}% · confirmation is required before checkpointing."
                ),
                CompactionDecision::Checkpoint(percent) => format!(
                    "Context usage: {percent}% · an automatic checkpoint is ready; Codex owns compaction."
                ),
            }
        };
        self.widgets.lean_context_status.set_label(&status);
        for control in [
            self.widgets
                .lean_context_mode
                .clone()
                .upcast::<gtk::Widget>(),
            self.widgets
                .lean_context_threshold
                .clone()
                .upcast::<gtk::Widget>(),
            self.widgets
                .lean_context_evidence_budget
                .clone()
                .upcast::<gtk::Widget>(),
            self.widgets
                .lean_context_command_budget
                .clone()
                .upcast::<gtk::Widget>(),
            self.widgets
                .lean_context_condensation_target
                .clone()
                .upcast::<gtk::Widget>(),
            self.widgets
                .lean_context_qwen
                .clone()
                .upcast::<gtk::Widget>(),
        ] {
            control.set_sensitive(preferences.enabled);
        }
        self.widgets.lean_context_checkpoint.set_sensitive(
            preferences.enabled
                && thread_id.is_some()
                && !guards.request_inflight
                && guards.connection_ready
                && !guards.thread_running
                && !guards.pending_approval,
        );
        self.widgets
            .lean_context_checkpoint
            .set_label(if measuring {
                "Observing Codex compaction…"
            } else if guards.request_inflight {
                "Checkpoint in progress…"
            } else {
                "Checkpoint & compact now"
            });
        let (receipt, metrics, lean_sample_count) = {
            let stored = self.stored.borrow();
            let receipt = thread_id
                .as_ref()
                .and_then(|thread_id| stored.context_checkpoints.get(thread_id))
                .cloned();
            let metrics = thread_id
                .as_ref()
                .and_then(|thread_id| stored.context_metrics.get(thread_id))
                .cloned()
                .unwrap_or_default();
            (receipt, metrics, stored.lean_experiment.samples.len())
        };
        if let Some(receipt) = receipt {
            let unchanged = receipt
                .evidence
                .iter()
                .filter(|evidence| evidence.unchanged)
                .count();
            let error = metrics
                .last_error
                .as_deref()
                .map(|error| format!("\nLast issue: {error}"))
                .unwrap_or_default();
            self.widgets.lean_context_latest.set_label(&format!(
                "Latest checkpoint {} · {} loaded turns · {} evidence files ({unchanged} unchanged) · {} ms\n{} checkpoints · {} canonical compactions · {} observed tokens removed · {} successful/{} inconclusive measurements · {} anonymous A/B samples{error}",
                receipt.id,
                receipt.source_turn_count,
                receipt.evidence.len(),
                receipt.elapsed_ms,
                metrics.checkpoint_count,
                metrics.compaction_count,
                metrics.observed_tokens_removed,
                metrics.successful_measurements,
                metrics.failed_measurements,
                lean_sample_count,
            ));
            self.widgets
                .lean_context_latest
                .set_tooltip_text(Some(&format!("Private checkpoint: {}", receipt.path)));
        } else {
            let detail = metrics
                .last_error
                .as_deref()
                .map(|error| format!("No checkpoint is available. Last issue: {error}"))
                .unwrap_or_else(|| "No checkpoint has been created for this task.".into());
            self.widgets.lean_context_latest.set_label(&detail);
            self.widgets.lean_context_latest.set_tooltip_text(None);
        }

        self.widgets.appshot_capture.set_sensitive(true);
        let appshot = self.state.borrow().latest_appshot.clone();
        if let Some(appshot) = appshot {
            let path = appshot
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or("unknown file");
            let tool = appshot
                .get("tool")
                .and_then(Value::as_str)
                .unwrap_or("screenshot backend");
            let bytes = appshot.get("bytes").and_then(Value::as_u64).unwrap_or(0);
            self.widgets.appshot_status.set_label(&format!(
                "Latest: {path}\nCaptured with {tool} · {bytes} bytes"
            ));
            self.widgets.appshot_attach.set_sensitive(true);
        } else {
            self.widgets.appshot_attach.set_sensitive(false);
        }
        let recording = self.state.borrow().recording_session.clone();
        if let Some(recording) = recording {
            let title = recording
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or("Recording");
            let frames = recording
                .get("frames")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or(0);
            let completed = recording
                .get("completedAt")
                .is_some_and(|value| !value.is_null());
            let skill = recording
                .get("draftSkill")
                .and_then(Value::as_str)
                .unwrap_or_default();
            self.widgets.record_title.set_text(title);
            self.widgets.record_title.set_sensitive(completed);
            self.widgets.record_note.set_sensitive(!completed);
            self.widgets.record_start.set_sensitive(completed);
            self.widgets.record_start.set_label(if completed {
                "New recording"
            } else {
                "Recording…"
            });
            self.widgets.record_frame.set_sensitive(!completed);
            self.widgets.record_stop.set_sensitive(!completed);
            self.widgets
                .record_install
                .set_sensitive(completed && !skill.is_empty());
            self.widgets.record_reveal.set_sensitive(true);
            let recording_status = if completed {
                format!("Recording complete · {frames} evidence frames\nDraft: {skill}")
            } else {
                format!("Recording in progress · {frames} evidence frames")
            };
            self.widgets.record_status.set_label(&recording_status);
        } else {
            self.widgets.record_title.set_sensitive(true);
            self.widgets.record_note.set_sensitive(false);
            self.widgets.record_start.set_sensitive(true);
            self.widgets.record_start.set_label("Start recording");
            self.widgets.record_frame.set_sensitive(false);
            self.widgets.record_stop.set_sensitive(false);
            self.widgets.record_install.set_sensitive(false);
            self.widgets.record_reveal.set_sensitive(false);
        }
    }

    fn render_diagnostics(&self) {
        clear_box(&self.widgets.diagnostics_list);
        let state = self.state.borrow();
        if let Some(report) = &state.diagnostics {
            let rss = report
                .get("clientRssKib")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let peak = report
                .get("clientPeakRssKib")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let descendants = report
                .get("descendantsRssKib")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            self.widgets.diagnostics_summary.set_label(&format!(
                "Client {:.1} MiB · peak {:.1} MiB · child processes {:.1} MiB",
                rss as f64 / 1024.0,
                peak as f64 / 1024.0,
                descendants as f64 / 1024.0
            ));
            for check in report
                .get("checks")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let name = check.get("name").and_then(Value::as_str).unwrap_or("Check");
                let status = check
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                let detail = check
                    .get("detail")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let remediation = check
                    .get("remediation")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let subtitle = if remediation.is_empty() {
                    format!("{status} · {detail}")
                } else {
                    format!("{status} · {detail}\n{remediation}")
                };
                self.widgets
                    .diagnostics_list
                    .append(&info_row(name, &subtitle));
            }
            if let Some(processes) = report.get("descendants").and_then(Value::as_array)
                && !processes.is_empty()
            {
                self.widgets
                    .diagnostics_list
                    .append(&section_heading("Managed child processes"));
                for process in processes.iter().take(20) {
                    let name = process
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("process");
                    let pid = process.get("pid").and_then(Value::as_u64).unwrap_or(0);
                    let rss = process.get("rssKib").and_then(Value::as_u64).unwrap_or(0);
                    self.widgets.diagnostics_list.append(&info_row(
                        name,
                        &format!("PID {pid} · {:.1} MiB", rss as f64 / 1024.0),
                    ));
                }
            }
        } else {
            self.widgets
                .diagnostics_list
                .append(&empty_label("Run diagnostics to inspect this session."));
        }

        if let Some(update) = &state.update_report {
            let installed = update
                .get("installed")
                .and_then(Value::as_str)
                .unwrap_or("unknown version");
            let available = update
                .get("available")
                .and_then(Value::as_str)
                .unwrap_or_default();
            self.widgets.update_apply.set_sensitive(
                update.get("updateAvailable").and_then(Value::as_bool) == Some(true),
            );
            self.widgets.update_rollback.set_sensitive(
                update
                    .get("rollbackPackages")
                    .and_then(Value::as_array)
                    .is_some_and(|packages| !packages.is_empty()),
            );
            let update_text = if available.is_empty() {
                format!("{installed}\nNo repository update reported.")
            } else {
                format!("{installed}\nUpdate: {available}")
            };
            self.widgets.update_label.set_label(&update_text);
        }
        if let Some(remote) = &state.remote_doctor {
            let ready = remote
                .get("ready")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let remediation = remote
                .get("remediation")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let candidates = remote
                .get("candidates")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or(0);
            self.widgets.remote_doctor_label.set_label(&format!(
                "{} · {candidates} Codex installation(s) inspected\n{remediation}",
                if ready {
                    "Remote runtime ready"
                } else {
                    "Remote runtime needs attention"
                }
            ));
        }

        self.widgets
            .diagnostics_list
            .append(&section_heading("Codex policy and capabilities"));
        let policy_values = [
            (
                "Admin requirements",
                state.config_requirements.clone().unwrap_or(Value::Null),
            ),
            (
                "Permission profiles",
                Value::Array(state.permission_profiles.clone()),
            ),
            (
                "Experimental features",
                Value::Array(state.experimental_features.clone()),
            ),
            (
                "Collaboration modes",
                Value::Array(state.collaboration_modes.clone()),
            ),
            (
                "Model-provider capabilities",
                state
                    .model_provider_capabilities
                    .clone()
                    .unwrap_or(Value::Null),
            ),
        ];
        for (title, value) in policy_values {
            let count = match &value {
                Value::Array(values) => values.len(),
                Value::Object(values) => values.len(),
                Value::Null => 0,
                _ => 1,
            };
            let row = gtk::Box::new(gtk::Orientation::Horizontal, 10);
            row.add_css_class("settings-card");
            let label = gtk::Label::new(Some(&format!(
                "{title}\n{}",
                if count == 0 {
                    "No values reported".to_owned()
                } else {
                    format!("{count} value(s) reported")
                }
            )));
            label.set_xalign(0.0);
            label.set_wrap(true);
            label.set_hexpand(true);
            row.append(&label);
            let details = gtk::Button::with_label("Details");
            let window = self.widgets.window.clone();
            let value_for_dialog = value.clone();
            details.connect_clicked(move |_| show_value_dialog(&window, title, &value_for_dialog));
            details.set_sensitive(!value.is_null());
            row.append(&details);
            self.widgets.diagnostics_list.append(&row);
        }
    }

    fn render_extensions(&self) {
        clear_box(&self.widgets.extensions_list);
        let query = self
            .widgets
            .extensions_search
            .text()
            .trim()
            .to_ascii_lowercase();
        let installed_only = self.widgets.extensions_installed_only.is_active();
        let state = self.state.borrow();
        let (plugins, plugin_count) =
            state
                .plugins
                .filtered_entries(installed_only, &query, EXTENSION_RENDER_LIMIT);
        let marketplaces = state
            .plugins
            .marketplaces
            .iter()
            .map(|marketplace| {
                (
                    marketplace.name.clone(),
                    marketplace.path.clone(),
                    marketplace.plugins.len(),
                )
            })
            .collect::<Vec<_>>();
        let marketplace_errors = state.plugins.marketplace_load_errors.clone();
        let full_catalog_loading = state.plugins_full_catalog_loading;
        let skills = flatten_skills(&state.skills);
        let mcp_servers = state.mcp_servers.clone();
        let apps = state.apps.clone();
        let hooks = state.hooks.clone();
        drop(state);

        self.widgets
            .extensions_list
            .append(&section_heading(&format!(
                "Marketplaces ({})",
                marketplaces.len()
            )));
        for (marketplace_name, marketplace_path, marketplace_plugin_count) in marketplaces {
            let path = marketplace_path
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "Remote catalog".into());
            let row = info_row(
                &marketplace_name,
                &format!("{marketplace_plugin_count} plugins · {path}"),
            );
            let removable = !marketplace_name.starts_with("openai-")
                && !matches!(marketplace_name.as_str(), "personal" | "workspace" | "repo");
            if removable {
                let remove = icon_button("user-trash-symbolic", "Remove marketplace");
                remove.add_css_class("destructive-action");
                let name = marketplace_name.clone();
                let window = self.widgets.window.clone();
                let hub = self.hub.clone();
                let state = self.state.clone();
                remove.connect_clicked(move |_| {
                    let dialog = adw::AlertDialog::new(
                        Some(&format!("Remove {name}?")),
                        Some("Installed plugins remain installed, but this catalog stops receiving updates."),
                    );
                    dialog.add_responses(&[("cancel", "Cancel"), ("remove", "Remove")]);
                    dialog.set_response_appearance(
                        "remove",
                        adw::ResponseAppearance::Destructive,
                    );
                    dialog.set_close_response("cancel");
                    let hub = hub.clone();
                    let state = state.clone();
                    let name = name.clone();
                    dialog.choose(
                        Some(&window),
                        None::<&gio::Cancellable>,
                        move |response| {
                            if response.as_str() == "remove" {
                                let id = hub.request(
                                    "marketplace/remove",
                                    json!({"marketplaceName": name}),
                                );
                                state
                                    .borrow_mut()
                                    .pending
                                    .insert(id, PendingKind::Marketplace);
                            }
                        },
                    );
                });
                row.append(&remove);
            }
            self.widgets.extensions_list.append(&row);
        }

        self.widgets
            .extensions_list
            .append(&section_heading(&plugin_section_title(
                installed_only,
                plugin_count,
            )));
        let rendered_plugin_count = plugins.len();
        for entry in plugins {
            self.widgets
                .extensions_list
                .append(&self.plugin_card(entry));
        }
        if rendered_plugin_count == 0 {
            self.widgets
                .extensions_list
                .append(&empty_label("No plugins match this filter."));
        } else if plugin_count > rendered_plugin_count {
            self.widgets.extensions_list.append(&info_row(
                "Refine the plugin search",
                &format!(
                    "Showing the first {rendered_plugin_count} of {plugin_count} matches to keep the native UI responsive."
                ),
            ));
        }
        if full_catalog_loading {
            self.widgets.extensions_list.append(&info_row(
                "Loading remote plugins…",
                "Installed plugins remain usable while the remote catalog loads.",
            ));
        }
        for error in marketplace_errors {
            self.widgets.extensions_list.append(&info_row(
                "Marketplace could not be loaded",
                &format!("{} — {}", error.marketplace_path, error.message),
            ));
        }

        let apps = apps
            .into_iter()
            .filter(|app| {
                query.is_empty()
                    || app
                        .get("name")
                        .and_then(Value::as_str)
                        .is_some_and(|name| name.to_ascii_lowercase().contains(&query))
                    || app
                        .get("description")
                        .and_then(Value::as_str)
                        .is_some_and(|detail| detail.to_ascii_lowercase().contains(&query))
            })
            .collect::<Vec<_>>();
        let app_count = apps.len();
        let app_render_limit = self.apps_render_limit.get();
        self.widgets
            .extensions_list
            .append(&section_heading(&format!("Apps ({app_count})")));
        for app in apps.into_iter().take(app_render_limit) {
            let toggle_state = app_toggle_state(&app);
            let name = app
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("App")
                .to_owned();
            let detail = app.get("description").and_then(Value::as_str).unwrap_or(
                "Structured app tools are rendered natively; custom web UI opens externally.",
            );
            let row = info_row(&name, detail);
            let details = gtk::Button::with_label("Details");
            let window = self.widgets.window.clone();
            let app_for_dialog = app.clone();
            details
                .connect_clicked(move |_| show_value_dialog(&window, "Codex app", &app_for_dialog));
            row.append(&details);
            if !toggle_state.connected
                && let Some(url) = app.get("installUrl").and_then(Value::as_str)
            {
                let connect = gtk::LinkButton::with_label(url, "Connect in browser");
                row.append(&connect);
            }
            let app_id = app
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            if !app_id.is_empty() {
                let enabled = gtk::Switch::builder()
                    .active(toggle_state.active)
                    .sensitive(toggle_state.connected)
                    .tooltip_text(if toggle_state.connected {
                        "Enable this app for new tasks"
                    } else {
                        "Connect this app in your browser before enabling it"
                    })
                    .build();
                let hub = self.hub.clone();
                let state = self.state.clone();
                enabled.connect_active_notify(move |switch| {
                    let enabled = switch.is_active();
                    let id = hub.request(
                        "config/batchWrite",
                        json!({
                            "edits": [{
                                "keyPath": app_enabled_key(&app_id),
                                "value": enabled,
                                "mergeStrategy": "upsert"
                            }],
                            "reloadUserConfig": true
                        }),
                    );
                    state.borrow_mut().pending.insert(
                        id,
                        PendingKind::AppEnable {
                            app_id: app_id.clone(),
                            enabled,
                        },
                    );
                });
                row.append(&enabled);
            }
            self.widgets.extensions_list.append(&row);
        }
        if app_count > app_render_limit {
            let show_more = gtk::Button::with_label(&format!(
                "Show {} more apps",
                (app_count - app_render_limit).min(APP_RENDER_PAGE)
            ));
            show_more.set_halign(gtk::Align::Center);
            let weak = self.weak_self.borrow().clone();
            show_more.connect_clicked(move |_| {
                with_controller(&weak, |controller| {
                    controller
                        .apps_render_limit
                        .set(controller.apps_render_limit.get() + APP_RENDER_PAGE);
                    controller.render_extensions();
                })
            });
            self.widgets.extensions_list.append(&show_more);
        }

        self.widgets
            .extensions_list
            .append(&section_heading(&format!("Skills ({})", skills.len())));
        for skill in skills {
            self.widgets.extensions_list.append(&self.skill_card(skill));
        }

        self.widgets
            .extensions_list
            .append(&section_heading(&format!(
                "MCP servers ({})",
                mcp_servers.len()
            )));
        for server in mcp_servers {
            self.widgets
                .extensions_list
                .append(&self.mcp_server_card(server));
        }

        self.widgets
            .extensions_list
            .append(&section_heading(&format!("Hooks ({})", hooks.len())));
        for hook in hooks {
            let (name, detail) = extension_summary(&hook);
            let row = info_row(&name, &detail);
            let details = gtk::Button::with_label("Details");
            let window = self.widgets.window.clone();
            details.connect_clicked(move |_| show_value_dialog(&window, "Codex hook", &hook));
            row.append(&details);
            self.widgets.extensions_list.append(&row);
        }

        if self.state.borrow().plugins.marketplaces.is_empty()
            && self.state.borrow().skills.is_empty()
            && self.state.borrow().mcp_servers.is_empty()
        {
            self.widgets
                .extensions_list
                .append(&empty_label("No extension data is available yet."));
        }
    }

    fn plugin_card(&self, entry: PluginEntry) -> gtk::Box {
        let row = gtk::Box::new(gtk::Orientation::Horizontal, 10);
        row.add_css_class("settings-card");
        let text = gtk::Box::new(gtk::Orientation::Vertical, 3);
        text.set_hexpand(true);
        let title = gtk::Label::new(Some(entry.display_name()));
        title.set_xalign(0.0);
        title.add_css_class("heading");
        text.append(&title);
        let mut details = vec![entry.marketplace_name.clone()];
        if !entry.version().is_empty() {
            details.push(format!("v{}", entry.version()));
        }
        details.push(if entry.summary.installed {
            if entry.summary.enabled {
                "installed · enabled".into()
            } else {
                "installed · disabled".into()
            }
        } else {
            "available".into()
        });
        if !entry.summary.auth_policy.is_empty() {
            details.push(format!(
                "auth {}",
                entry
                    .summary
                    .auth_policy
                    .to_ascii_lowercase()
                    .replace('_', " ")
            ));
        }
        let subtitle = [entry.description(), &details.join(" · ")]
            .into_iter()
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        let subtitle = gtk::Label::new(Some(&subtitle));
        subtitle.set_xalign(0.0);
        subtitle.set_wrap(true);
        subtitle.add_css_class("caption");
        text.append(&subtitle);
        row.append(&text);

        let details_button = gtk::Button::with_label("Details");
        let hub = self.hub.clone();
        let state = self.state.clone();
        let params = entry.locator_params();
        let plugin_id = entry.summary.id.clone();
        details_button.connect_clicked(move |_| {
            let id = hub.request("plugin/read", params.clone());
            state
                .borrow_mut()
                .pending
                .insert(id, PendingKind::PluginRead(plugin_id.clone()));
        });
        row.append(&details_button);

        if entry.summary.installed {
            let enabled = gtk::Switch::builder()
                .active(entry.summary.enabled)
                .tooltip_text("Enable this plugin for new tasks")
                .build();
            let hub = self.hub.clone();
            let state = self.state.clone();
            let plugin_id = entry.summary.id.clone();
            enabled.connect_active_notify(move |switch| {
                let enabled = switch.is_active();
                let id = hub.request(
                    "config/batchWrite",
                    json!({
                        "edits": [{
                            "keyPath": plugin_enabled_key(&plugin_id),
                            "value": enabled,
                            "mergeStrategy": "upsert"
                        }],
                        "reloadUserConfig": true
                    }),
                );
                state.borrow_mut().pending.insert(
                    id,
                    PendingKind::PluginEnable {
                        plugin_id: plugin_id.clone(),
                        enabled,
                    },
                );
            });
            row.append(&enabled);

            let remove = icon_button("user-trash-symbolic", "Uninstall plugin");
            remove.add_css_class("destructive-action");
            let window = self.widgets.window.clone();
            let hub = self.hub.clone();
            let state = self.state.clone();
            let plugin_id = entry.summary.id.clone();
            let display_name = entry.display_name().to_owned();
            remove.connect_clicked(move |_| {
                let dialog = adw::AlertDialog::new(
                    Some(&format!("Uninstall {display_name}?")),
                    Some(
                        "Its bundled skills, hooks, apps, and MCP tools will stop loading in new tasks.",
                    ),
                );
                dialog.add_responses(&[("cancel", "Cancel"), ("remove", "Uninstall")]);
                dialog.set_response_appearance("remove", adw::ResponseAppearance::Destructive);
                dialog.set_close_response("cancel");
                let hub = hub.clone();
                let state = state.clone();
                let plugin_id = plugin_id.clone();
                dialog.choose(
                    Some(&window),
                    None::<&gio::Cancellable>,
                    move |response| {
                        if response.as_str() == "remove" {
                            let id = hub.request(
                                "plugin/uninstall",
                                json!({"pluginId": plugin_id}),
                            );
                            state.borrow_mut().pending.insert(
                                id,
                                PendingKind::PluginUninstall(plugin_id.clone()),
                            );
                        }
                    },
                );
            });
            row.append(&remove);
        } else {
            let install = gtk::Button::with_label("Install");
            install.add_css_class("suggested-action");
            install.set_sensitive(
                entry.summary.install_policy != "NOT_AVAILABLE"
                    && entry.summary.availability != "DISABLED_BY_ADMIN",
            );
            let hub = self.hub.clone();
            let state = self.state.clone();
            let params = entry.locator_params();
            let plugin_id = entry.summary.id.clone();
            install.connect_clicked(move |button| {
                button.set_sensitive(false);
                let id = hub.request("plugin/install", params.clone());
                state
                    .borrow_mut()
                    .pending
                    .insert(id, PendingKind::PluginInstall(plugin_id.clone()));
            });
            row.append(&install);
        }
        row
    }

    fn skill_card(&self, skill: Value) -> gtk::Box {
        let (name, detail) = extension_summary(&skill);
        let row = info_row(&name, &detail);
        let enabled_value = skill
            .get("enabled")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let enabled = gtk::Switch::builder()
            .active(enabled_value)
            .tooltip_text("Enable this skill")
            .build();
        let hub = self.hub.clone();
        let state = self.state.clone();
        let skill_name = name.clone();
        let path = skill.get("path").and_then(Value::as_str).map(str::to_owned);
        enabled.connect_active_notify(move |switch| {
            let enabled = switch.is_active();
            let params = if let Some(path) = &path {
                json!({"path": path, "enabled": enabled})
            } else {
                json!({"name": skill_name, "enabled": enabled})
            };
            let id = hub.request("skills/config/write", params);
            state.borrow_mut().pending.insert(
                id,
                PendingKind::SkillEnable {
                    skill_name: skill_name.clone(),
                    enabled,
                },
            );
        });
        row.append(&enabled);
        row
    }

    fn mcp_server_card(&self, server: Value) -> gtk::Box {
        let name = server
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("MCP server")
            .to_owned();
        let auth = server
            .get("authStatus")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let tools = server
            .get("tools")
            .and_then(Value::as_object)
            .map(Map::len)
            .unwrap_or(0);
        let version = server
            .pointer("/serverInfo/version")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let detail = [
            (!version.is_empty()).then(|| format!("v{version}")),
            Some(format!("{tools} tools")),
            Some(format!("auth {}", auth_canonical_label(auth))),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" · ");
        let row = info_row(&name, &detail);

        let details = gtk::Button::with_label("Tools");
        let window = self.widgets.window.clone();
        let server_for_dialog = server.clone();
        let title = name.clone();
        details.connect_clicked(move |_| {
            show_value_dialog(&window, &format!("{title} MCP server"), &server_for_dialog)
        });
        row.append(&details);

        if auth == "notLoggedIn" {
            let login = gtk::Button::with_label("Sign in");
            login.add_css_class("suggested-action");
            let hub = self.hub.clone();
            let state = self.state.clone();
            let server_name = name.clone();
            login.connect_clicked(move |_| {
                let id = hub.request("mcpServer/oauth/login", json!({"name": server_name}));
                state
                    .borrow_mut()
                    .pending
                    .insert(id, PendingKind::McpOauth(server_name.clone()));
            });
            row.append(&login);
        }
        row
    }

    fn show_plugin_details(&self, plugin_id: &str, result: &Value) {
        let plugin = result.get("plugin").unwrap_or(result);
        let summary = plugin.get("summary").unwrap_or(plugin);
        let interface = summary.get("interface").unwrap_or(&Value::Null);
        let title = interface
            .get("displayName")
            .or_else(|| summary.get("name"))
            .and_then(Value::as_str)
            .unwrap_or(plugin_id);
        let description = plugin
            .get("description")
            .or_else(|| interface.get("longDescription"))
            .or_else(|| interface.get("shortDescription"))
            .and_then(Value::as_str)
            .unwrap_or("No description supplied.");

        let content = gtk::Box::new(gtk::Orientation::Vertical, 9);
        content.set_size_request(520, -1);
        let description_label = gtk::Label::new(Some(description));
        description_label.set_xalign(0.0);
        description_label.set_wrap(true);
        content.append(&description_label);

        let marketplace = plugin
            .get("marketplaceName")
            .and_then(Value::as_str)
            .unwrap_or("unknown marketplace");
        let version = summary
            .get("localVersion")
            .or_else(|| summary.get("version"))
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let auth = summary
            .get("authPolicy")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        content.append(&info_row(
            "Package",
            &format!("{plugin_id} · {marketplace} · v{version} · auth {auth}"),
        ));

        for (label, key) in [
            ("Skills", "skills"),
            ("MCP servers", "mcpServers"),
            ("Apps", "apps"),
            ("App templates", "appTemplates"),
            ("Hooks", "hooks"),
        ] {
            let values = plugin
                .get(key)
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if values.is_empty() {
                continue;
            }
            let names = values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .or_else(|| {
                            ["name", "key", "templateId", "id"]
                                .into_iter()
                                .find_map(|key| value.get(key).and_then(Value::as_str))
                                .map(str::to_owned)
                        })
                        .unwrap_or_else(|| value_to_text(value))
                })
                .collect::<Vec<_>>()
                .join(" · ");
            content.append(&info_row(label, &names));
        }

        if let Some(prompts) = interface.get("defaultPrompt").and_then(Value::as_array) {
            let prompts = prompts
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join("\n");
            if !prompts.is_empty() {
                content.append(&info_row("Starter prompts", &prompts));
            }
        }

        let links = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        for (label, key) in [
            ("Website", "websiteUrl"),
            ("Privacy", "privacyPolicyUrl"),
            ("Terms", "termsOfServiceUrl"),
        ] {
            if let Some(url) = interface.get(key).and_then(Value::as_str) {
                links.append(&gtk::LinkButton::with_label(url, label));
            }
        }
        if links.first_child().is_some() {
            content.append(&links);
        }

        let scroller = gtk::ScrolledWindow::builder()
            .min_content_height(260)
            .max_content_height(520)
            .propagate_natural_height(true)
            .hscrollbar_policy(gtk::PolicyType::Never)
            .child(&content)
            .build();
        let dialog = adw::AlertDialog::new(Some(title), None);
        dialog.set_extra_child(Some(&scroller));
        dialog.add_response("close", "Close");
        dialog.set_close_response("close");
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            |_| {},
        );
    }

    fn render_qwen(&self) {
        let state = self.state.borrow();
        let plugin = state.plugins.find_entry(PluginEntry::is_qwen_buddy);
        let server = state
            .mcp_servers
            .iter()
            .find(|value| value.get("name").and_then(Value::as_str) == Some("local-qwen-delegate"))
            .cloned();
        let report = state.qwen_report.clone();
        let busy = state.qwen_busy;
        drop(state);

        let server_ready = server.as_ref().is_some_and(|value| {
            value
                .get("tools")
                .and_then(Value::as_object)
                .is_some_and(|tools| {
                    tools.contains_key("local_qwen_agent")
                        && tools.contains_key("local_qwen_agent_status")
                        && tools.contains_key("gemini_buddy_delegate")
                })
        });
        let catalog_state = plugin
            .as_ref()
            .map(|entry| (entry.summary.installed, entry.summary.enabled));
        let (installed, enabled) =
            qwen_install_state(catalog_state, server.is_some(), server_ready);
        let host_ready = report
            .as_ref()
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str)
            == Some("ready");
        let qwen_preferences = self.stored.borrow().preferences.qwen_buddy.clone();
        let gpu_blocked = self.qwen_gpu_blocked();
        let route_state = if qwen_preferences.gpu_guard && gpu_blocked {
            "local routing paused by GPU guard"
        } else if server_ready && host_ready {
            "local routing ready"
        } else if !qwen_preferences.routing_enabled {
            "automatic local subagents are disabled"
        } else {
            "local routing unavailable"
        };
        let headline = if busy && report.is_none() {
            "Checking OpenCode and local Qwen…".into()
        } else if !installed {
            "Qwen Buddy plugin is not installed".into()
        } else if !enabled {
            "Qwen Buddy plugin is disabled".into()
        } else if !server_ready {
            "Qwen Buddy MCP server is unavailable".into()
        } else if !host_ready {
            "Qwen Buddy local runtime needs attention".into()
        } else {
            format!("OpenCode ready with local Qwen · {route_state}")
        };
        self.widgets.qwen_status.set_label(&headline);

        let version = plugin
            .as_ref()
            .map(PluginEntry::version)
            .filter(|value| !value.is_empty())
            .unwrap_or("unknown");
        let tool_count = server
            .as_ref()
            .and_then(|value| value.get("tools"))
            .and_then(Value::as_object)
            .map(Map::len)
            .unwrap_or(0);
        let model_installed = report
            .as_ref()
            .and_then(|value| value.pointer("/runtime/modelInstalled"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let model_running = report
            .as_ref()
            .and_then(|value| value.pointer("/runtime/modelRunning"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let model_unloaded = report
            .as_ref()
            .and_then(|value| value.pointer("/runtime/modelUnloaded"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let model_state = if model_running {
            "Qwen model loaded"
        } else if model_unloaded {
            "Qwen model installed · unloaded until needed"
        } else if model_installed {
            "Qwen model installed · idle"
        } else {
            "Qwen model missing"
        };
        self.widgets.qwen_detail.set_label(&format!(
            "Plugin v{version} · {tool_count} MCP tools · {model_state} · Qwen is the controlled implementation worker; ChatGPT/Sol plans, approves destructive requests, and verifies."
        ));

        let pressure = report
            .as_ref()
            .and_then(|value| value.pointer("/gpu/pressure"))
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let total = report
            .as_ref()
            .and_then(|value| value.pointer("/gpu/memoryTotalMiB"))
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let used = report
            .as_ref()
            .and_then(|value| value.pointer("/gpu/memoryUsedMiB"))
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let utilization = report
            .as_ref()
            .and_then(|value| value.pointer("/gpu/utilizationPercent"))
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let workloads = report
            .as_ref()
            .and_then(|value| value.pointer("/gpu/heavyWorkloads"))
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "none".into());
        let memory = if total > 0 {
            format!("{used} / {total} MiB")
        } else {
            "unavailable".into()
        };
        self.widgets.qwen_gpu.set_label(&format!(
            "GPU pressure: {pressure} · memory {memory} · utilization {utilization}% · game/3D workloads: {workloads} · {route_state}."
        ));

        if let Some(usage) = report.as_ref().and_then(|value| value.get("usage")) {
            let lifetime = usage.get("lifetime").unwrap_or(&Value::Null);
            let calls = lifetime
                .get("toolCalls")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let luna = lifetime
                .get("directDelegateCalls")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let condenser = lifetime
                .get("fileCondenseCalls")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let sol = lifetime
                .get("agentSessionCalls")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let prompt_tokens = lifetime
                .get("localPromptTokens")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let output_tokens = lifetime
                .get("localOutputTokens")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let avoided = lifetime
                .get("avoidedTokensApprox")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let potential_saved = lifetime
                .get("potentialCodexTokensSavedApprox")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let provider_tokens = [
                ("Qwen", "qwenTokens"),
                ("Gemini", "geminiTokens"),
                ("OpenRouter", "openrouterTokens"),
                ("Mistral", "mistralTokens"),
            ]
            .into_iter()
            .filter_map(|(label, key)| {
                let tokens = lifetime.get(key).and_then(Value::as_i64).unwrap_or(0);
                (tokens > 0).then(|| format!("{label} {}", format_integer(tokens)))
            })
            .collect::<Vec<_>>()
            .join(" · ");
            let provider_tokens = if provider_tokens.is_empty() {
                "No successful provider tokens recorded".to_owned()
            } else {
                provider_tokens
            };
            let direct_unquantified = lifetime
                .get("directSavingsUnquantifiedCalls")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let agent_unquantified = lifetime
                .get("agentSavingsUnquantifiedCalls")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let task_uses = usage
                .pointer("/codexTaskTotals/toolUses")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            self.widgets.qwen_usage.set_label(&format!(
                "MCP lifetime: {} calls (Direct {luna} · Condenser {condenser} · Coding agent {sol}) · {} non-GPT tokens ({} prompt + {} output).\nProviders: {provider_tokens}. Potential GPT tokens avoided: ~{}. File context kept out: ~{} tokens. Legacy unquantified calls: {}.\nCodex task-hook events: {task_uses}. All successful Qwen, Gemini, OpenRouter, and Mistral prompt/output tokens count; failed calls count zero, and receipts never enter model context.",
                format_integer(calls),
                format_integer(prompt_tokens.saturating_add(output_tokens)),
                format_integer(prompt_tokens),
                format_integer(output_tokens),
                format_integer(potential_saved),
                format_integer(avoided),
                format_integer(direct_unquantified.saturating_add(agent_unquantified)),
            ));
        } else {
            self.widgets
                .qwen_usage
                .set_label("No Qwen Buddy usage ledger has been detected yet.");
        }

        self.widgets.qwen_refresh.set_sensitive(!busy);
        self.widgets
            .qwen_routing_switch
            .set_sensitive(installed && enabled && server_ready);
        let lanes_sensitive = self.widgets.qwen_routing_switch.is_active();
        self.widgets.qwen_luna_check.set_sensitive(lanes_sensitive);
        self.widgets
            .qwen_condenser_check
            .set_sensitive(lanes_sensitive);
        self.widgets.qwen_sol_check.set_sensitive(lanes_sensitive);
        self.widgets
            .qwen_gpu_guard_switch
            .set_sensitive(lanes_sensitive);
    }

    fn render_computer_use(&self) {
        clear_box(&self.widgets.computer_capabilities);
        let state = self.state.borrow();
        let plugin = state.plugins.find_entry(PluginEntry::is_computer_use);
        let server = state
            .mcp_servers
            .iter()
            .find(|value| value.get("name").and_then(Value::as_str) == Some("computer-use"))
            .cloned();
        let report = state.computer_report.clone();
        let busy = state.computer_busy;
        let config = state.config.clone();
        drop(state);

        let installed = plugin.as_ref().is_some_and(|entry| entry.summary.installed);
        let enabled = plugin
            .as_ref()
            .is_some_and(|entry| entry.summary.installed && entry.summary.enabled);
        let plugin_id = plugin
            .as_ref()
            .map(|entry| entry.summary.id.as_str())
            .unwrap_or("computer-use@openai-bundled");
        let server_enabled = config_plugin_value(
            config.as_ref(),
            plugin_id,
            &["mcp_servers", "computer-use", "enabled"],
        )
        .and_then(Value::as_bool)
        .unwrap_or(enabled);
        let approval_mode = config_plugin_value(
            config.as_ref(),
            plugin_id,
            &["mcp_servers", "computer-use", "default_tools_approval_mode"],
        )
        .and_then(Value::as_str)
        .unwrap_or("prompt");

        self.updating_computer_controls.set(true);
        self.widgets.computer_plugin_switch.set_active(enabled);
        self.widgets
            .computer_plugin_switch
            .set_sensitive(plugin.is_some());
        self.widgets
            .computer_server_switch
            .set_active(server_enabled);
        self.widgets
            .computer_server_switch
            .set_sensitive(installed && enabled);
        set_combo(&self.widgets.computer_approval_combo, approval_mode);
        self.widgets
            .computer_approval_combo
            .set_sensitive(installed && enabled);
        self.updating_computer_controls.set(false);

        let binary_ready = self.computer_binary().is_some();
        self.widgets
            .computer_doctor
            .set_sensitive(binary_ready && !busy);
        self.widgets
            .computer_setup
            .set_sensitive(binary_ready && !busy);
        self.widgets
            .computer_new_task
            .set_sensitive(enabled && server.is_some() && !busy);
        self.widgets.computer_doctor.set_label(if busy {
            "Checking…"
        } else {
            "Check readiness"
        });

        let blockers = report
            .as_ref()
            .and_then(|value| value.pointer("/readiness/blockers"))
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
        let (status, detail) = match (&plugin, enabled, server.as_ref(), report.as_ref()) {
            (None, _, _, _) => (
                "Computer Use is unavailable",
                "Add or refresh a marketplace that contains the Computer Use plugin.",
            ),
            (Some(entry), false, _, _) if !entry.summary.installed => (
                "Computer Use is ready to install",
                "Turn on the plugin switch to install the Linux backend.",
            ),
            (Some(_), false, _, _) => (
                "Computer Use is disabled",
                "Enable the plugin. Changes apply to new tasks.",
            ),
            (Some(_), true, None, _) if !server_enabled => (
                "Computer Use MCP server is disabled",
                "Enable the Linux MCP server, then start a new task.",
            ),
            (Some(_), true, None, _) => (
                "Computer Use backend did not start",
                "Refresh Extensions and inspect the Codex app-server log for an MCP startup error.",
            ),
            (_, _, Some(_), Some(_)) if blockers == 0 => (
                "Computer Use is ready",
                "Linux desktop inspection and input backends passed the readiness check.",
            ),
            (_, _, Some(_), Some(_)) => (
                "Computer Use is partially ready",
                "Core tools loaded, but the readiness report lists setup work below.",
            ),
            (_, _, Some(_), None) => (
                "Computer Use tools are loaded",
                "Run the readiness check before the first desktop-control task.",
            ),
        };
        self.widgets.computer_status.set_label(status);
        self.widgets.computer_detail.set_label(detail);

        if let Some(report) = report {
            let readiness = report.get("readiness").unwrap_or(&Value::Null);
            self.widgets
                .computer_capabilities
                .append(&section_heading("Readiness"));
            for (label, key) in [
                ("MCP tools", "can_register_mcp_tools"),
                ("Window discovery", "can_query_windows"),
                ("App focus", "can_focus_apps"),
                ("Window focus", "can_focus_windows"),
                ("Desktop input", "can_send_development_input"),
                ("Accessibility tree", "can_build_accessibility_tree"),
            ] {
                let ok = readiness.get(key).and_then(Value::as_bool).unwrap_or(false);
                self.widgets
                    .computer_capabilities
                    .append(&info_row(label, if ok { "Ready" } else { "Needs setup" }));
            }
            if let Some(next) = readiness
                .get("recommended_next_step")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
            {
                self.widgets
                    .computer_capabilities
                    .append(&info_row("Recommended next step", next));
            }
            if let Some(values) = readiness.get("blockers").and_then(Value::as_array) {
                for value in values.iter().filter_map(Value::as_str) {
                    self.widgets
                        .computer_capabilities
                        .append(&info_row("Setup blocker", value));
                }
            }

            let desktop = report
                .pointer("/platform/xdg_current_desktop")
                .and_then(Value::as_str)
                .unwrap_or("Linux");
            let session = report
                .pointer("/platform/xdg_session_type")
                .and_then(Value::as_str)
                .unwrap_or("unknown session");
            let input = report
                .pointer("/capabilities/preferred/input")
                .and_then(Value::as_str)
                .unwrap_or("unavailable");
            let screenshot = report
                .pointer("/capabilities/preferred/screenshot")
                .and_then(Value::as_str)
                .unwrap_or("unavailable");
            let windows = report
                .pointer("/capabilities/preferred/window_control")
                .and_then(Value::as_str)
                .unwrap_or("unavailable");
            self.widgets
                .computer_capabilities
                .append(&section_heading("Native backends"));
            self.widgets.computer_capabilities.append(&info_row(
                &format!("{desktop} · {session}"),
                &format!("Input: {input} · Screenshots: {screenshot} · Windows: {windows}"),
            ));
        }

        if let Some(server) = server {
            let mut tool_names = server
                .get("tools")
                .and_then(Value::as_object)
                .map(|tools| tools.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            tool_names.sort();
            self.widgets
                .computer_capabilities
                .append(&section_heading(&format!(
                    "Available tools ({})",
                    tool_names.len()
                )));
            self.widgets
                .computer_capabilities
                .append(&info_row("Linux Computer Use MCP", &tool_names.join(" · ")));
        }
    }

    fn render_account(&self) {
        let state = self.state.borrow();
        let (profiles, active_profile_id) = {
            let stored = self.stored.borrow();
            (
                stored.account_profiles.clone(),
                stored.active_account_id.clone(),
            )
        };
        clear_box(&self.widgets.account_profiles);
        for profile in &profiles {
            let active = profile.id == active_profile_id;
            let button = gtk::Button::with_label(&if active {
                format!("● {}", profile.label)
            } else {
                profile.label.clone()
            });
            button.set_halign(gtk::Align::Fill);
            if active {
                button.add_css_class("suggested-action");
            }
            let profile_id = profile.id.clone();
            let weak = self.weak_self.borrow().clone();
            button.connect_clicked(move |_| {
                with_controller(&weak, |controller| {
                    controller.activate_account_profile(&profile_id, false)
                })
            });
            self.widgets.account_profiles.append(&button);
        }
        let account = state
            .account
            .as_ref()
            .and_then(|value| value.get("account"))
            .filter(|value| !value.is_null());
        if let Some(account) = account {
            let kind = account
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("account");
            let email = account
                .get("email")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let plan = account
                .get("planType")
                .and_then(Value::as_str)
                .unwrap_or_default();
            self.widgets
                .account_label
                .set_label(&format!("Account: {email} · {plan} ({kind})"));
            self.widgets.account_menu.set_label(email);
            self.widgets
                .account_menu_summary
                .set_label(&format!("{email} · {plan}"));
            if !email.is_empty() {
                let mut stored = self.stored.borrow_mut();
                if let Some(profile) = stored
                    .account_profiles
                    .iter_mut()
                    .find(|profile| profile.id == active_profile_id)
                    && profile.label != email
                {
                    profile.label = email.to_owned();
                }
            }
            self.widgets.login_button.set_visible(false);
            self.widgets.logout_button.set_visible(true);
        } else {
            self.widgets
                .account_label
                .set_label("Account: not signed in");
            let label = profiles
                .iter()
                .find(|profile| profile.id == active_profile_id)
                .map(|profile| profile.label.as_str())
                .unwrap_or("Account");
            self.widgets.account_menu.set_label(label);
            self.widgets
                .account_menu_summary
                .set_label(&format!("{label}: not signed in"));
            self.widgets.login_button.set_visible(true);
            self.widgets.logout_button.set_visible(false);
        }
        let lifetime = state
            .account_usage
            .as_ref()
            .and_then(|value| value.pointer("/summary/lifetimeTokens"))
            .and_then(Value::as_i64);
        let mut parts = Vec::new();
        if let Some(limits) = state.account_rate_limits.as_ref() {
            if let Some((remaining, resets_at, _)) = weekly_limit_details(limits) {
                let reset = resets_at
                    .and_then(format_timestamp)
                    .map(|value| format!(" (resets {value})"))
                    .unwrap_or_default();
                parts.push(format!("weekly {remaining}% left{reset}"));
            }
            if let Some(snapshot) = rate_limit_snapshot(limits)
                && let Some(primary) = snapshot.get("primary")
                && let Some(used) = primary.get("usedPercent").and_then(Value::as_i64)
            {
                parts.push(format!("short window {}% left", (100 - used).clamp(0, 100)));
            }
            let reset_count = reset_credit_count(limits);
            parts.push(format!(
                "{reset_count} reset credit{}",
                if reset_count == 1 { "" } else { "s" }
            ));
        }
        if let Some(lifetime) = lifetime {
            parts.push(format!("{} lifetime tokens", format_integer(lifetime)));
        }
        let usage = if parts.is_empty() {
            "Usage details unavailable for this account".into()
        } else {
            parts.join(" · ")
        };
        self.widgets.account_usage_label.set_label(&usage);
    }

    fn render_chatgpt(&self) {
        let state = self.state.borrow();
        let account = state
            .account
            .as_ref()
            .and_then(|value| value.get("account"))
            .filter(|value| !value.is_null());
        let status = match account {
            Some(account) => {
                let email = account
                    .get("email")
                    .and_then(Value::as_str)
                    .unwrap_or("signed-in account");
                let plan = account
                    .get("planType")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown plan");
                if plan.to_ascii_lowercase().starts_with("pro") {
                    format!(
                        "ChatGPT Pro detected for {email}. The in-app ChatGPT profile may ask you to sign in once."
                    )
                } else {
                    format!(
                        "Codex is signed in as {email} ({plan}). The in-app ChatGPT session will confirm Pro and Voice eligibility."
                    )
                }
            }
            None => {
                "Codex is not signed in. Sign in directly on the in-app ChatGPT page.".to_owned()
            }
        };
        self.widgets.chatgpt_status.set_label(&status);
    }

    fn render_remote(&self) {
        let state = self.state.borrow();
        let (label, mut detail) = match &state.remote {
            RemoteState::Unknown => (
                "Status: unavailable",
                "Waiting for remote-control status.".to_owned(),
            ),
            RemoteState::Disabled => (
                "Status: disabled",
                "Enable this workstation before pairing an iOS device.".to_owned(),
            ),
            RemoteState::Connecting => (
                "Status: connecting",
                "Connecting this workstation to the Codex remote service…".to_owned(),
            ),
            RemoteState::Connected => (
                "Status: connected",
                "This workstation can accept paired ChatGPT iOS remote sessions.".to_owned(),
            ),
            RemoteState::Error(error) => ("Status: error", error.clone()),
        };
        if let Some(daemon) = state
            .remote_details
            .as_ref()
            .and_then(|value| value.get("localDaemon"))
        {
            let pid = daemon
                .get("pid")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let rss = daemon
                .get("rssMiB")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let swap = daemon
                .get("swapMiB")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let logs = daemon
                .get("logsDatabaseMiB")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            detail.push_str(&format!(
                "\nHost process {pid}: {rss} MiB RAM · {swap} MiB swap · {logs} MiB event database"
            ));
            if daemon.get("legacyDesktopRunning").and_then(Value::as_bool) == Some(true) {
                detail.push_str(
                    "\nThe older Electron Codex app is also running. Close it after moving to Native to avoid duplicate app-servers and split live state.",
                );
            }
        }
        if self.remote_handoff_pending.get() {
            detail.push_str(
                "\nShared iOS transport handoff is queued until all active turns are idle.",
            );
        }
        if self.remote_recovery_pending.get() {
            detail.push_str(
                "\nAutomatic recovery is queued and will run as soon as all tasks are idle.",
            );
        } else if self.remote_recovery_in_progress.get() {
            detail.push_str("\nAutomatic recovery is restarting the host now…");
        }
        detail.push_str(
            "\nStock managed Codex host: iPhone and iPad model/reasoning selections pass through unchanged. Qwen Buddy remains available to Codex for bounded delegation.",
        );
        self.widgets.remote_status.set_label(label);
        if let Some(environment) = state
            .remote_details
            .as_ref()
            .and_then(|value| value.get("environmentId"))
            .and_then(Value::as_str)
        {
            self.widgets
                .remote_detail
                .set_label(&format!("{detail}\nEnvironment: {environment}"));
        } else {
            self.widgets.remote_detail.set_label(&detail);
        }
        self.widgets
            .remote_enable
            .set_sensitive(!matches!(state.remote, RemoteState::Connected));
        self.widgets
            .remote_disable
            .set_sensitive(!matches!(state.remote, RemoteState::Disabled));
        self.widgets
            .remote_pair
            .set_sensitive(matches!(state.remote, RemoteState::Connected));
        if let Some(pairing) = &state.remote_pairing {
            let code = pairing
                .manual_pairing_code
                .as_deref()
                .or(pairing.pairing_code.as_deref())
                .unwrap_or("Pairing requested");
            self.widgets.remote_pairing.set_label(&format!(
                "Pairing code: {code}\nOpen ChatGPT on iOS → Codex → Connect to computer."
            ));
        }

        clear_box(&self.widgets.remote_clients);
        if state.remote_clients.is_empty() {
            self.widgets.remote_clients.append(&empty_label(
                "No paired devices reported by the managed host.",
            ));
        }
        let environment_id = state
            .remote_details
            .as_ref()
            .and_then(|value| value.get("environmentId"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        for client in &state.remote_clients {
            let client_id = client
                .get("clientId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let name = client
                .get("displayName")
                .or_else(|| client.get("deviceModel"))
                .and_then(Value::as_str)
                .unwrap_or("iOS device");
            let subtitle = [
                client.get("platform").and_then(Value::as_str),
                client.get("osVersion").and_then(Value::as_str),
                client.get("appVersion").and_then(Value::as_str),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" · ");
            let row = info_row(name, &subtitle);
            let revoke = gtk::Button::with_label("Revoke");
            revoke.add_css_class("destructive-action");
            row.append(&revoke);
            if let Some(environment_id) = environment_id.clone() {
                let hub = self.hub.clone();
                revoke.connect_clicked(move |_| {
                    hub.remote(RemoteAction::Revoke {
                        client_id: client_id.clone(),
                        environment_id: environment_id.clone(),
                    });
                });
            } else {
                revoke.set_sensitive(false);
            }
            self.widgets.remote_clients.append(&row);
        }
    }

    fn toast(&self, message: &str) {
        self.widgets
            .toast_overlay
            .add_toast(adw::Toast::new(message));
    }

    fn notify_turn_complete(&self) {
        if !self.stored.borrow().preferences.desktop_notifications
            || self.widgets.window.is_active()
        {
            return;
        }
        let notification = gio::Notification::new("Codex task completed");
        let body = self
            .state
            .borrow()
            .active_thread()
            .map(|thread| compact_ui_text(thread.title(), 240))
            .unwrap_or_else(|| "Your task is ready".into());
        notification.set_body(Some(&body));
        if let Some(application) = self.widgets.window.application() {
            application.send_notification(None, &notification);
        }
    }

    fn start_terminal(&self) {
        if self.terminal_started.replace(true) {
            self.widgets.terminal.grab_focus();
            return;
        }
        self.widgets.terminal_start.set_sensitive(false);
        self.widgets.terminal_status.set_label("Starting shell…");
        let shell = env::var("SHELL").unwrap_or_else(|_| "/bin/bash".into());
        let cwd = self.current_cwd_string();
        let env_values = env::vars()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>();
        let env_refs = env_values.iter().map(String::as_str).collect::<Vec<_>>();
        let terminal_status = self.widgets.terminal_status.clone();
        let terminal_start = self.widgets.terminal_start.clone();
        let started = self.terminal_started.clone();
        self.widgets.terminal.spawn_async(
            vte::PtyFlags::DEFAULT,
            Some(&cwd),
            &[shell.as_str()],
            &env_refs,
            glib::SpawnFlags::DEFAULT,
            || {},
            -1,
            None::<&gio::Cancellable>,
            move |result| match result {
                Ok(_) => {
                    terminal_status.set_label("Shell running");
                    terminal_start.set_label("Focus shell");
                    terminal_start.set_sensitive(true);
                }
                Err(error) => {
                    started.set(false);
                    terminal_status.set_label(&format!("Could not start shell: {error}"));
                    terminal_start.set_label("Retry");
                    terminal_start.set_sensitive(true);
                }
            },
        );
    }

    fn open_project_editor(&self) {
        self.hub.host(HostAction::OpenEditor {
            path: PathBuf::from(self.current_cwd_string()),
            preferred_binary: self.stored.borrow().preferences.editor_command.clone(),
        });
    }

    fn reveal_project(&self) {
        self.hub
            .host(HostAction::Reveal(PathBuf::from(self.current_cwd_string())));
    }

    fn current_additional_paths(&self) -> Vec<PathBuf> {
        let cwd = PathBuf::from(self.current_cwd_string());
        self.stored
            .borrow()
            .projects
            .iter()
            .find(|project| project.path == cwd)
            .map(|project| project.additional_paths.clone())
            .unwrap_or_default()
    }

    fn check_agent_workspace(&self) {
        self.widgets.workspace_doctor.set_sensitive(false);
        self.widgets.workspace_launch.set_sensitive(false);
        self.widgets
            .workspace_status
            .set_label("Checking bubblewrap, user systemd, terminal, and namespace support…");
        self.hub.host(HostAction::WorkspaceDoctor {
            cwd: PathBuf::from(self.current_cwd_string()),
        });
    }

    fn launch_agent_workspace(&self) {
        self.widgets.workspace_launch.set_sensitive(false);
        self.widgets.workspace_doctor.set_sensitive(false);
        self.widgets
            .workspace_status
            .set_label("Launching a resource-limited isolated shell…");
        self.hub.host(HostAction::WorkspaceLaunch {
            cwd: PathBuf::from(self.current_cwd_string()),
            additional_paths: self.current_additional_paths(),
            network: self.widgets.workspace_network.is_active(),
            memory_mib: self.widgets.workspace_memory.value_as_int().max(512) as u32,
        });
    }

    fn stop_agent_workspace(&self) {
        let unit = self
            .state
            .borrow()
            .workspace_session
            .as_ref()
            .and_then(|session| session.get("unit"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let Some(unit) = unit else {
            self.toast("No managed workspace is active");
            return;
        };
        self.widgets.workspace_stop.set_sensitive(false);
        self.hub.host(HostAction::WorkspaceStop { unit });
    }

    fn render_agent_workspace(&self) {
        let state = self.state.borrow();
        let ready = state
            .workspace_report
            .as_ref()
            .and_then(|report| report.get("ready"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if let Some(session) = &state.workspace_session {
            let unit = session
                .get("unit")
                .and_then(Value::as_str)
                .unwrap_or("managed workspace");
            let network = session
                .get("network")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let memory = session
                .get("memoryMib")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let recovered = session
                .get("recovered")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let detail = if recovered {
                "Recovered a running managed unit; launch details are unavailable after an app restart."
                    .to_owned()
            } else {
                format!(
                    "Network {} · {memory} MiB limit · project mounted at /workspace",
                    if network { "enabled" } else { "isolated" }
                )
            };
            self.widgets
                .workspace_status
                .set_label(&format!("Workspace active · {unit}\n{detail}"));
            self.widgets.workspace_launch.set_sensitive(false);
            self.widgets.workspace_stop.set_sensitive(true);
            self.widgets.workspace_network.set_sensitive(false);
            self.widgets.workspace_memory.set_sensitive(false);
        } else if let Some(report) = &state.workspace_report {
            let bubblewrap = report
                .get("bubblewrap")
                .and_then(Value::as_str)
                .unwrap_or("missing");
            let terminal = report
                .get("terminal")
                .and_then(Value::as_str)
                .unwrap_or("missing");
            let namespace = report
                .get("namespaceCheck")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            self.widgets.workspace_status.set_label(&format!(
                "{}\nBubblewrap: {bubblewrap} · terminal: {terminal} · namespaces: {}",
                if ready {
                    "Isolation is ready"
                } else {
                    "Isolation needs attention"
                },
                if namespace { "ready" } else { "blocked" }
            ));
            self.widgets.workspace_launch.set_sensitive(ready);
            self.widgets.workspace_stop.set_sensitive(false);
            self.widgets.workspace_network.set_sensitive(true);
            self.widgets.workspace_memory.set_sensitive(true);
        } else {
            self.widgets.workspace_launch.set_sensitive(false);
            self.widgets.workspace_stop.set_sensitive(false);
            self.widgets.workspace_network.set_sensitive(true);
            self.widgets.workspace_memory.set_sensitive(true);
        }
        self.widgets.workspace_doctor.set_sensitive(true);
    }

    fn capture_appshot(&self) {
        self.widgets
            .appshot_status
            .set_label("Capturing focused window…");
        self.widgets.appshot_capture.set_sensitive(false);
        self.hub.host(HostAction::AppShotCapture);
    }

    fn attach_latest_appshot(&self) {
        let path = self
            .state
            .borrow()
            .latest_appshot
            .as_ref()
            .and_then(|value| value.get("path"))
            .and_then(Value::as_str)
            .map(PathBuf::from);
        let Some(path) = path else {
            self.toast("Capture an AppShot first");
            return;
        };
        if !self.attachments.borrow().iter().any(|item| item == &path) {
            self.attachments.borrow_mut().push(path);
        }
        self.render_attachments();
        self.widgets.stack.set_visible_child_name("chat");
        self.widgets.composer.grab_focus();
        self.toast("AppShot attached to the next turn");
    }

    fn start_recording(&self) {
        let title = self.widgets.record_title.text().trim().to_owned();
        if title.is_empty() {
            self.toast("Enter a workflow title");
            return;
        }
        self.widgets.record_start.set_sensitive(false);
        self.widgets.record_status.set_label("Starting recording…");
        self.hub.host(HostAction::RecordStart { title });
    }

    fn recording_directory(&self) -> Option<PathBuf> {
        self.state
            .borrow()
            .recording_session
            .as_ref()
            .and_then(|value| value.get("directory"))
            .and_then(Value::as_str)
            .map(PathBuf::from)
    }

    fn capture_recording_frame(&self) {
        let Some(session_dir) = self.recording_directory() else {
            self.toast("Start a recording first");
            return;
        };
        self.widgets.record_frame.set_sensitive(false);
        self.widgets
            .record_status
            .set_label("Capturing semantic evidence…");
        self.hub.host(HostAction::RecordFrame {
            session_dir,
            note: self.widgets.record_note.text().trim().to_owned(),
        });
    }

    fn stop_recording(&self) {
        let Some(session_dir) = self.recording_directory() else {
            return;
        };
        self.widgets.record_stop.set_sensitive(false);
        self.widgets
            .record_status
            .set_label("Writing reviewable draft skill…");
        self.hub.host(HostAction::RecordStop { session_dir });
    }

    fn install_recording_skill(&self) {
        let Some(session_dir) = self.recording_directory() else {
            return;
        };
        let dialog = adw::AlertDialog::new(
            Some("Install the recorded skill?"),
            Some(
                "Install only after reviewing SKILL.md and every evidence frame. The skill will be available to new Codex tasks.",
            ),
        );
        dialog.add_responses(&[("cancel", "Cancel"), ("install", "Install")]);
        dialog.set_default_response(Some("install"));
        dialog.set_close_response("cancel");
        let hub = self.hub.clone();
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            move |response| {
                if response.as_str() == "install" {
                    hub.host(HostAction::RecordInstall { session_dir });
                }
            },
        );
    }

    fn reveal_recording(&self) {
        if let Some(directory) = self.recording_directory() {
            self.hub.host(HostAction::Reveal(directory));
        }
    }

    fn open_browser_companion(&self) {
        let url = self.widgets.browser_url.text().trim().to_owned();
        self.hub.host(HostAction::BrowserOpen {
            url,
            isolated_profile: self.widgets.browser_isolated.is_active(),
            preferred_binary: self.stored.borrow().preferences.browser_command.clone(),
        });
    }

    fn run_diagnostics(&self) {
        self.widgets
            .diagnostics_summary
            .set_label("Collecting resource and host diagnostics…");
        self.hub.host(HostAction::Diagnostics);
    }

    fn check_updates(&self) {
        self.widgets
            .update_label
            .set_label("Checking Arch package state…");
        self.hub.host(HostAction::UpdateCheck);
    }

    fn confirm_update(&self) {
        let dialog = adw::AlertDialog::new(
            Some("Install the Arch package update?"),
            Some(
                "Arch updates must remain coherent, so this runs a full pacman -Syu transaction through polkit. Review the package prompt and restart Codex Native afterward.",
            ),
        );
        dialog.add_responses(&[("cancel", "Cancel"), ("update", "Authenticate and update")]);
        dialog.set_default_response(Some("update"));
        dialog.set_close_response("cancel");
        let hub = self.hub.clone();
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            move |response| {
                if response.as_str() == "update" {
                    hub.host(HostAction::UpdateApply);
                }
            },
        );
    }

    fn confirm_rollback(&self) {
        let packages = self
            .state
            .borrow()
            .update_report
            .as_ref()
            .and_then(|value| value.get("rollbackPackages"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if packages.is_empty() {
            self.toast("No cached Codex Native package is available for rollback");
            return;
        }
        let combo = gtk::ComboBoxText::new();
        for (index, package) in packages.iter().enumerate() {
            if let Some(path) = package.as_str() {
                let label = Path::new(path)
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or(path);
                combo.append(Some(&index.to_string()), label);
            }
        }
        combo.set_active(Some(0));
        let dialog = adw::AlertDialog::new(
            Some("Roll back Codex Native?"),
            Some("Select a cached package. Pacman will authenticate through polkit."),
        );
        dialog.set_extra_child(Some(&combo));
        dialog.add_responses(&[("cancel", "Cancel"), ("rollback", "Roll back")]);
        dialog.set_response_appearance("rollback", adw::ResponseAppearance::Destructive);
        dialog.set_close_response("cancel");
        let hub = self.hub.clone();
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            move |response| {
                if response.as_str() != "rollback" {
                    return;
                }
                let Some(index) = combo.active().map(|value| value as usize) else {
                    return;
                };
                let Some(path) = packages.get(index).and_then(Value::as_str) else {
                    return;
                };
                hub.host(HostAction::UpdateRollback(PathBuf::from(path)));
            },
        );
    }

    fn run_remote_doctor(&self) {
        self.widgets
            .remote_doctor_label
            .set_label("Inspecting package-managed Codex and remote service…");
        self.hub.host(HostAction::RemoteDoctor {
            configured_binary: self.stored.borrow().preferences.codex_binary.clone(),
        });
    }

    fn render_automations(&self) {
        clear_box(&self.widgets.automations_list);
        let items = self.automations.borrow().items.clone();
        let history = AutomationHistory::load();
        if items.is_empty() {
            self.widgets.automations_list.append(&empty_label(
                "No scheduled tasks yet. Scheduled prompts run through user-level systemd timers.",
            ));
            return;
        }
        for automation in items {
            let card = gtk::Box::new(gtk::Orientation::Horizontal, 10);
            card.add_css_class("settings-card");
            let text = gtk::Box::new(gtk::Orientation::Vertical, 3);
            text.set_hexpand(true);
            let title = gtk::Label::new(Some(&automation.title));
            title.set_xalign(0.0);
            title.add_css_class("heading");
            text.append(&title);
            let subtitle = gtk::Label::new(Some(&format!(
                "{} · {}",
                automation.schedule,
                automation.cwd.display()
            )));
            subtitle.set_xalign(0.0);
            subtitle.set_ellipsize(gtk::pango::EllipsizeMode::Middle);
            subtitle.add_css_class("caption");
            text.append(&subtitle);
            let prompt = gtk::Label::new(Some(&automation.prompt));
            prompt.set_xalign(0.0);
            prompt.set_ellipsize(gtk::pango::EllipsizeMode::End);
            prompt.add_css_class("muted");
            text.append(&prompt);
            if let Some(run) = history.for_automation(automation.id).first() {
                let completed = run
                    .completed_at
                    .map(|value| value.format("%Y-%m-%d %H:%M").to_string())
                    .unwrap_or_else(|| "running now".into());
                let run_label = gtk::Label::new(Some(&format!(
                    "Last run: {} · {} · {}",
                    run.status, run.trigger, completed
                )));
                run_label.set_xalign(0.0);
                run_label.add_css_class("caption");
                text.append(&run_label);
            }
            card.append(&text);

            let run_now = gtk::Button::with_label("Run now");
            let run_id = automation.id;
            let overlay = self.widgets.toast_overlay.clone();
            run_now.connect_clicked(move |_| match automation::start_automation_now(run_id) {
                Ok(()) => overlay.add_toast(adw::Toast::new("Scheduled task started")),
                Err(error) => overlay.add_toast(adw::Toast::new(&format!(
                    "Could not start scheduled task: {error}"
                ))),
            });
            card.append(&run_now);

            if let Some(run) = history.for_automation(automation.id).first() {
                let log = icon_button("text-x-generic-symbolic", "Reveal latest run log");
                let hub = self.hub.clone();
                let path = run.log_path.clone();
                log.connect_clicked(move |_| hub.host(HostAction::Reveal(path.clone())));
                card.append(&log);
            }

            let edit = icon_button("document-edit-symbolic", "Edit scheduled task");
            let parent = self.widgets.window.clone();
            let store = self.automations.clone();
            let overlay = self.widgets.toast_overlay.clone();
            let list = self.widgets.automations_list.clone();
            let automation_for_edit = automation.clone();
            edit.connect_clicked(move |_| {
                show_edit_automation_dialog(
                    &parent,
                    store.clone(),
                    overlay.clone(),
                    list.clone(),
                    automation_for_edit.clone(),
                );
            });
            card.append(&edit);

            let toggle = gtk::Switch::builder()
                .active(automation.enabled)
                .valign(gtk::Align::Center)
                .tooltip_text("Enable schedule")
                .build();
            let id = automation.id;
            let store = self.automations.clone();
            let overlay = self.widgets.toast_overlay.clone();
            toggle.connect_active_notify(move |toggle| {
                let mut store = store.borrow_mut();
                let Some(item) = store.items.iter_mut().find(|item| item.id == id) else {
                    return;
                };
                item.enabled = toggle.is_active();
                let item = item.clone();
                let result = store.save().and_then(|_| {
                    if item.enabled {
                        let executable = env::current_exe()?;
                        automation::install_systemd_units(&item, &executable)
                    } else {
                        automation::remove_systemd_units(item.id)
                    }
                });
                if let Err(error) = result {
                    overlay.add_toast(adw::Toast::new(&format!(
                        "Scheduled task update failed: {error}"
                    )));
                }
            });
            card.append(&toggle);

            let delete = icon_button("user-trash-symbolic", "Delete scheduled task");
            let id = automation.id;
            let store = self.automations.clone();
            let list = self.widgets.automations_list.clone();
            let card_clone = card.clone();
            let overlay = self.widgets.toast_overlay.clone();
            delete.connect_clicked(move |_| {
                let mut store = store.borrow_mut();
                store.remove(id);
                let result = store
                    .save()
                    .and_then(|_| automation::remove_systemd_units(id));
                if let Err(error) = result {
                    overlay.add_toast(adw::Toast::new(&format!(
                        "Could not remove scheduled task: {error}"
                    )));
                } else {
                    list.remove(&card_clone);
                }
            });
            card.append(&delete);
            self.widgets.automations_list.append(&card);
        }
    }

    fn new_automation_dialog(&self) {
        let form = gtk::Grid::builder()
            .column_spacing(12)
            .row_spacing(9)
            .margin_top(4)
            .build();
        let title = gtk::Entry::builder()
            .placeholder_text("Daily project check")
            .hexpand(true)
            .build();
        let prompt = gtk::TextView::new();
        prompt.set_wrap_mode(gtk::WrapMode::WordChar);
        prompt.set_size_request(360, 100);
        let prompt_frame = gtk::Frame::builder().child(&prompt).build();
        let cwd = gtk::Entry::new();
        cwd.set_text(&self.current_cwd_string());
        let schedule = gtk::Entry::builder()
            .text("Mon..Fri *-*-* 09:00:00")
            .tooltip_text("systemd OnCalendar expression")
            .build();
        let enabled = gtk::Switch::builder().active(true).build();
        attach_setting(&form, 0, "Title", &title);
        attach_setting(&form, 1, "Prompt", &prompt_frame);
        attach_setting(&form, 2, "Folder", &cwd);
        attach_setting(&form, 3, "Schedule", &schedule);
        attach_setting(&form, 4, "Enable now", &enabled);

        let dialog = adw::AlertDialog::new(
            Some("New scheduled task"),
            Some(
                "The prompt is stored locally and passed to Codex over stdin when the timer runs.",
            ),
        );
        dialog.set_extra_child(Some(&form));
        dialog.add_responses(&[("cancel", "Cancel"), ("create", "Create")]);
        dialog.set_default_response(Some("create"));
        dialog.set_close_response("cancel");
        let store = self.automations.clone();
        let list = self.widgets.automations_list.clone();
        let overlay = self.widgets.toast_overlay.clone();
        dialog.choose(
            Some(&self.widgets.window),
            None::<&gio::Cancellable>,
            move |response| {
                if response.as_str() != "create" {
                    return;
                }
                let buffer = prompt.buffer();
                let prompt_text = buffer
                    .text(&buffer.start_iter(), &buffer.end_iter(), false)
                    .trim()
                    .to_owned();
                let title_text = title.text().trim().to_owned();
                if title_text.is_empty() || prompt_text.is_empty() {
                    overlay.add_toast(adw::Toast::new(
                        "A scheduled task needs both a title and prompt",
                    ));
                    return;
                }
                let mut item = Automation::new(
                    title_text,
                    prompt_text,
                    PathBuf::from(cwd.text().as_str()),
                    schedule.text().to_string(),
                );
                item.enabled = enabled.is_active();
                let executable = env::current_exe();
                let result = executable.and_then(|executable| {
                    let mut store = store.borrow_mut();
                    store.upsert(item.clone());
                    store.save().map_err(std::io::Error::other)?;
                    automation::install_systemd_units(&item, &executable)
                        .map_err(std::io::Error::other)
                });
                match result {
                    Ok(()) => {
                        clear_box(&list);
                        list.append(&empty_label(
                            "Scheduled task created. Reopen this page to refresh the list.",
                        ));
                        overlay.add_toast(adw::Toast::new("Scheduled task created"));
                    }
                    Err(error) => overlay.add_toast(adw::Toast::new(&format!(
                        "Could not create scheduled task: {error}"
                    ))),
                }
            },
        );
    }
}

fn show_edit_automation_dialog(
    parent: &adw::ApplicationWindow,
    store: Rc<RefCell<AutomationStore>>,
    overlay: adw::ToastOverlay,
    list: gtk::Box,
    automation: Automation,
) {
    let form = gtk::Grid::builder()
        .column_spacing(12)
        .row_spacing(9)
        .margin_top(4)
        .build();
    let title = gtk::Entry::builder()
        .text(&automation.title)
        .hexpand(true)
        .build();
    let prompt = gtk::TextView::new();
    prompt.set_wrap_mode(gtk::WrapMode::WordChar);
    prompt.set_size_request(360, 100);
    prompt.buffer().set_text(&automation.prompt);
    let prompt_frame = gtk::Frame::builder().child(&prompt).build();
    let cwd = gtk::Entry::new();
    cwd.set_text(&automation.cwd.to_string_lossy());
    let schedule = gtk::Entry::builder().text(&automation.schedule).build();
    let enabled = gtk::Switch::builder().active(automation.enabled).build();
    attach_setting(&form, 0, "Title", &title);
    attach_setting(&form, 1, "Prompt", &prompt_frame);
    attach_setting(&form, 2, "Folder", &cwd);
    attach_setting(&form, 3, "Schedule", &schedule);
    attach_setting(&form, 4, "Enabled", &enabled);
    let dialog = adw::AlertDialog::new(
        Some("Edit scheduled task"),
        Some("Saving replaces its user-level systemd timer atomically."),
    );
    dialog.set_extra_child(Some(&form));
    dialog.add_responses(&[("cancel", "Cancel"), ("save", "Save")]);
    dialog.set_default_response(Some("save"));
    dialog.set_close_response("cancel");
    dialog.choose(Some(parent), None::<&gio::Cancellable>, move |response| {
        if response.as_str() != "save" {
            return;
        }
        let buffer = prompt.buffer();
        let prompt_text = buffer
            .text(&buffer.start_iter(), &buffer.end_iter(), false)
            .trim()
            .to_owned();
        let title_text = title.text().trim().to_owned();
        if title_text.is_empty() || prompt_text.is_empty() {
            overlay.add_toast(adw::Toast::new(
                "A scheduled task needs both a title and prompt",
            ));
            return;
        }
        let mut updated = automation.clone();
        updated.title = title_text;
        updated.prompt = prompt_text;
        updated.cwd = PathBuf::from(cwd.text().as_str());
        updated.schedule = schedule.text().to_string();
        updated.enabled = enabled.is_active();
        let result = env::current_exe()
            .map_err(anyhow::Error::from)
            .and_then(|executable| {
                let mut store = store.borrow_mut();
                store.upsert(updated.clone());
                store.save()?;
                let _ = automation::remove_systemd_units(updated.id);
                automation::install_systemd_units(&updated, &executable)
            });
        match result {
            Ok(()) => {
                clear_box(&list);
                list.append(&empty_label(
                    "Scheduled task saved. Reopen this page to refresh.",
                ));
                overlay.add_toast(adw::Toast::new("Scheduled task saved"));
            }
            Err(error) => overlay.add_toast(adw::Toast::new(&format!(
                "Could not update scheduled task: {error}"
            ))),
        }
    });
}

fn make_inputs(prompt: &str, attachments: &[String]) -> Vec<Value> {
    let mut inputs = Vec::new();
    if !prompt.trim().is_empty() {
        inputs.push(json!({
            "type": "text",
            "text": prompt,
            "text_elements": []
        }));
    }
    for path in attachments {
        let extension = Path::new(path)
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase();
        if ["png", "jpg", "jpeg", "gif", "webp"].contains(&extension.as_str()) {
            inputs.push(json!({"type": "localImage", "path": path}));
        } else {
            let name = Path::new(path)
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or(path);
            inputs.push(json!({"type": "mention", "name": name, "path": path}));
        }
    }
    inputs
}

fn selected_id(combo: &gtk::ComboBoxText, fallback: &str) -> String {
    combo
        .active_id()
        .map(|value| value.to_string())
        .unwrap_or_else(|| fallback.to_owned())
}

fn workspace_page_from_name(name: Option<&str>) -> WorkspacePage {
    match name {
        Some("chatgpt") => WorkspacePage::Chatgpt,
        Some("projects") => WorkspacePage::Projects,
        Some("sites") => WorkspacePage::Sites,
        Some("terminal") => WorkspacePage::Terminal,
        Some("agent-workspace") => WorkspacePage::AgentWorkspace,
        Some("context") => WorkspacePage::Context,
        Some("extensions") => WorkspacePage::Extensions,
        Some("qwen-buddy") => WorkspacePage::QwenBuddy,
        Some("computer-use") => WorkspacePage::ComputerUse,
        Some("automations") => WorkspacePage::Automations,
        Some("remote") => WorkspacePage::Remote,
        Some("diagnostics") => WorkspacePage::Diagnostics,
        Some("settings") => WorkspacePage::Settings,
        _ => WorkspacePage::Chat,
    }
}

fn standalone_mcp_key(server: &str) -> String {
    format!(
        "mcp_servers.\"{}\"",
        server.replace('\\', "\\\\").replace('"', "\\\"")
    )
}

fn macro_helper_binary() -> anyhow::Result<PathBuf> {
    if let Some(configured) = env::var_os("CODEX_NATIVE_MACRO_BINARY") {
        let configured = PathBuf::from(configured);
        if configured.is_absolute() && configured.is_file() {
            return Ok(configured);
        }
        anyhow::bail!(
            "CODEX_NATIVE_MACRO_BINARY is not an absolute executable file: {}",
            configured.display()
        );
    }
    let installed = PathBuf::from("/usr/lib/codex-native/codex-native-macro");
    if installed.is_file() {
        return Ok(installed);
    }
    let current = env::current_exe().context("current executable is unavailable")?;
    let adjacent = current
        .parent()
        .context("current executable has no parent")?
        .join("codex-native-macro");
    if adjacent.is_file() {
        return Ok(adjacent);
    }
    anyhow::bail!("expected {} or {}", installed.display(), adjacent.display())
}

fn set_combo(combo: &gtk::ComboBoxText, id: &str) {
    if !combo.set_active_id(Some(id)) {
        combo.set_active(Some(0));
    }
}

fn apply_theme(theme: &str) {
    let manager = adw::StyleManager::default();
    manager.set_color_scheme(match theme {
        "light" => adw::ColorScheme::ForceLight,
        "dark" => adw::ColorScheme::ForceDark,
        _ => adw::ColorScheme::Default,
    });
}

fn clear_box(container: &gtk::Box) {
    while let Some(child) = container.first_child() {
        container.remove(&child);
    }
}

fn clear_list_box(container: &gtk::ListBox) {
    while let Some(child) = container.first_child() {
        container.remove(&child);
    }
}

fn visible_thread_ids(state: &AppState, pinned: &[String], search: &str) -> Vec<String> {
    let server_search_active = state
        .thread_search_query
        .as_deref()
        .is_some_and(|query| query.trim().to_lowercase() == search);
    let mut ids = if server_search_active {
        state.thread_search_order.clone()
    } else {
        state.thread_order.clone()
    };
    ids.retain(|id| {
        let Some(thread) = state.threads.get(id) else {
            return false;
        };
        server_search_active
            || search.is_empty()
            || thread.title().to_lowercase().contains(search)
            || thread.cwd.to_lowercase().contains(search)
    });
    ids.sort_by_key(|id| !pinned.iter().any(|pinned_id| pinned_id == id));
    ids
}

fn is_bulk_task_mutation(pending: &PendingKind) -> bool {
    matches!(
        pending,
        PendingKind::BulkArchiveThread
            | PendingKind::BulkUnarchiveThread
            | PendingKind::BulkDeleteThread
    )
}

fn bulk_delete_dialog_title(count: usize) -> String {
    format!(
        "Delete {count} selected task{}?",
        if count == 1 { "" } else { "s" }
    )
}

fn thread_subtitle(thread: &ThreadSummary) -> String {
    let folder = Path::new(&thread.cwd)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(&thread.cwd);
    let status = match thread.status.as_str() {
        Some(value) => value.to_owned(),
        None if thread.status.is_object() => thread
            .status
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        None => String::new(),
    };
    [folder, &status]
        .into_iter()
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>()
        .join(" · ")
}

fn compact_ui_text(value: &str, max_chars: usize) -> String {
    let max_chars = max_chars.max(1);
    let mut compact = String::with_capacity(value.len().min(max_chars.saturating_mul(4)));
    let mut character_count = 0usize;
    let mut pending_space = false;

    for character in value.chars() {
        if character.is_whitespace() {
            pending_space = !compact.is_empty();
            continue;
        }
        if pending_space {
            if character_count >= max_chars {
                compact.push('…');
                return compact;
            }
            compact.push(' ');
            character_count += 1;
            pending_space = false;
        }
        if character_count >= max_chars {
            compact.push('…');
            return compact;
        }
        compact.push(character);
        character_count += 1;
    }

    if compact.is_empty() {
        "New task".to_owned()
    } else {
        compact
    }
}

fn thread_is_running(thread: &ThreadSummary) -> bool {
    thread.status.as_str() == Some("active")
        || thread.status.get("type").and_then(Value::as_str) == Some("active")
}

fn observe_macro_item(observation: &mut MacroTurnObservation, item: &Value) {
    let item_id = item
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            item.to_string().hash(&mut hasher);
            format!("anonymous-{:016x}", hasher.finish())
        });
    if !observation.item_ids.insert(item_id) {
        return;
    }
    if matches!(
        item.get("type").and_then(Value::as_str),
        Some("commandExecution" | "fileChange" | "mcpToolCall" | "dynamicToolCall")
    ) {
        observation.tool_calls = observation.tool_calls.saturating_add(1);
    }
    match item.get("type").and_then(Value::as_str) {
        Some("commandExecution") => {
            observation.command_calls = observation.command_calls.saturating_add(1)
        }
        Some("fileChange") => {
            observation.file_change_calls = observation.file_change_calls.saturating_add(1)
        }
        Some("mcpToolCall" | "dynamicToolCall") => {
            observation.external_calls = observation.external_calls.saturating_add(1)
        }
        _ => {}
    }
    observation.macro_used |= [
        item.get("tool").and_then(Value::as_str),
        item.get("name").and_then(Value::as_str),
        item.pointer("/result/tool").and_then(Value::as_str),
        item.pointer("/result/name").and_then(Value::as_str),
    ]
    .into_iter()
    .flatten()
    .any(|name| name.ends_with("codex_native_macro_run"))
        || item
            .get("server")
            .and_then(Value::as_str)
            .is_some_and(|server| matches!(server, "codex-native-macro" | "codex_native_macro"));
    observation.peak_memory_bytes = [
        "/result/structuredContent/peakMemoryBytes",
        "/result/structured_content/peak_memory_bytes",
        "/structuredContent/peakMemoryBytes",
        "/structured_content/peak_memory_bytes",
    ]
    .into_iter()
    .filter_map(|pointer| item.pointer(pointer).and_then(Value::as_u64))
    .chain(observation.peak_memory_bytes)
    .max();
    observation.context_bytes_avoided = [
        "/result/structuredContent/contextBytesAvoided",
        "/result/structured_content/context_bytes_avoided",
        "/structuredContent/contextBytesAvoided",
        "/structured_content/context_bytes_avoided",
    ]
    .into_iter()
    .filter_map(|pointer| item.pointer(pointer).and_then(Value::as_u64))
    .max()
    .unwrap_or(observation.context_bytes_avoided);
    observation.cache_hits = [
        "/result/structuredContent/cacheHits",
        "/result/structured_content/cache_hits",
        "/structuredContent/cacheHits",
        "/structured_content/cache_hits",
    ]
    .into_iter()
    .filter_map(|pointer| item.pointer(pointer).and_then(Value::as_u64))
    .max()
    .and_then(|value| u32::try_from(value).ok())
    .unwrap_or(observation.cache_hits);
    observation.workload_class = [
        "/result/structuredContent/workloadClass",
        "/result/structured_content/workload_class",
        "/structuredContent/workloadClass",
        "/structured_content/workload_class",
    ]
    .into_iter()
    .find_map(|pointer| item.pointer(pointer).and_then(Value::as_str))
    .map(str::to_owned)
    .or_else(|| observation.workload_class.clone());
}

fn turn_completed_successfully(params: &Value) -> bool {
    matches!(
        params.pointer("/turn/status").and_then(Value::as_str),
        Some("completed" | "succeeded")
    ) || matches!(
        params.pointer("/turn/status/type").and_then(Value::as_str),
        Some("completed" | "succeeded")
    )
}

fn turn_last_tokens(params: &Value) -> Option<u64> {
    turn_token_breakdown(params).total
}

fn turn_token_breakdown(params: &Value) -> TurnTokenBreakdown {
    let usage = ["/turn/tokenUsage/last", "/turn/usage", "/tokenUsage/last"]
        .into_iter()
        .find_map(|pointer| params.pointer(pointer))
        .unwrap_or(&Value::Null);
    TurnTokenBreakdown {
        total: usage.get("totalTokens").and_then(Value::as_u64),
        input: usage.get("inputTokens").and_then(Value::as_u64),
        cached_input: usage.get("cachedInputTokens").and_then(Value::as_u64),
        output: usage.get("outputTokens").and_then(Value::as_u64),
        reasoning_output: usage.get("reasoningOutputTokens").and_then(Value::as_u64),
    }
}

fn observation_workload_class(observation: &MacroTurnObservation) -> String {
    if let Some(workload) = &observation.workload_class {
        return workload.clone();
    }
    match (
        observation.command_calls > 0,
        observation.file_change_calls > 0,
        observation.external_calls > 0,
    ) {
        (true, false, false) => "read",
        (false, true, false) => "mutate",
        (false, false, true) => "external",
        (false, false, false) => "none",
        _ => "mixed",
    }
    .into()
}

fn cumulative_thread_tokens(value: &Value) -> Option<u64> {
    value.pointer("/total/totalTokens").and_then(Value::as_u64)
}

fn cumulative_turn_delta(observation: &TokenSavingsObservation) -> Option<u64> {
    observation
        .latest_cumulative_tokens
        .zip(observation.before_cumulative_tokens)
        .and_then(|(after, before)| after.checked_sub(before))
}

#[cfg(test)]
fn macro_sample_summary(samples: &[MacroExperimentSample], group: &str) -> String {
    let samples = samples
        .iter()
        .filter(|sample| sample.group == group)
        .collect::<Vec<_>>();
    if samples.is_empty() {
        return "no samples yet".into();
    }
    let completed = samples
        .iter()
        .filter(|sample| sample.turn_succeeded)
        .count();
    let elapsed_samples = samples
        .iter()
        .map(|sample| sample.elapsed_ms)
        .collect::<Vec<_>>();
    let elapsed = elapsed_samples
        .iter()
        .map(|sample| u128::from(*sample))
        .sum::<u128>()
        / samples.len() as u128;
    let tool_calls = samples
        .iter()
        .map(|sample| u128::from(sample.tool_calls))
        .sum::<u128>()
        / samples.len() as u128;
    let token_samples = samples
        .iter()
        .filter_map(|sample| sample.total_tokens)
        .collect::<Vec<_>>();
    let tokens = if token_samples.is_empty() {
        "tokens pending".into()
    } else {
        let average = token_samples
            .iter()
            .map(|value| u128::from(*value))
            .sum::<u128>()
            / token_samples.len() as u128;
        format!("{average} avg tokens")
    };
    let token_breakdown = [
        (
            "input",
            samples
                .iter()
                .filter_map(|sample| sample.input_tokens)
                .collect::<Vec<_>>(),
        ),
        (
            "cached",
            samples
                .iter()
                .filter_map(|sample| sample.cached_input_tokens)
                .collect::<Vec<_>>(),
        ),
        (
            "output",
            samples
                .iter()
                .filter_map(|sample| sample.output_tokens)
                .collect::<Vec<_>>(),
        ),
    ]
    .into_iter()
    .filter_map(|(label, values)| {
        (!values.is_empty()).then(|| {
            let average =
                values.iter().map(|value| u128::from(*value)).sum::<u128>() / values.len() as u128;
            format!("{label} {average}")
        })
    })
    .collect::<Vec<_>>()
    .join("/");
    let token_breakdown = if token_breakdown.is_empty() {
        String::new()
    } else {
        format!(" ({token_breakdown})")
    };
    let peak_memory = samples
        .iter()
        .filter_map(|sample| sample.peak_memory_bytes)
        .max()
        .map(|bytes| format!(" · {:.0} MiB peak", bytes as f64 / 1024.0 / 1024.0))
        .unwrap_or_default();
    let context_bytes_avoided = samples
        .iter()
        .map(|sample| sample.context_bytes_avoided)
        .sum::<u64>();
    let cache_hits = samples
        .iter()
        .map(|sample| u64::from(sample.cache_hits))
        .sum::<u64>();
    format!(
        "{} turns · {:.0}% completed · {elapsed} ms avg/p50 {}/p95 {} · {tokens}{token_breakdown} · {tool_calls} tool calls avg · ~{} context tokens avoided · {cache_hits} cache hits{peak_memory}",
        samples.len(),
        completed as f64 * 100.0 / samples.len() as f64,
        numeric_percentile(&elapsed_samples, 50),
        numeric_percentile(&elapsed_samples, 95),
        context_bytes_avoided / 4,
    )
}

#[cfg(test)]
fn macro_matched_summary(samples: &[MacroExperimentSample], group: &str) -> String {
    let other_group = if group == "macro" {
        "baseline"
    } else {
        "macro"
    };
    let matching_classes = samples
        .iter()
        .filter(|sample| sample.group == group && !sample.workload_class.is_empty())
        .filter(|sample| {
            samples.iter().any(|candidate| {
                candidate.group == other_group && candidate.workload_class == sample.workload_class
            })
        })
        .map(|sample| sample.workload_class.as_str())
        .collect::<HashSet<_>>();
    if matching_classes.is_empty() {
        return "no matched workload samples".into();
    }
    let matched = samples
        .iter()
        .filter(|sample| {
            sample.group == group && matching_classes.contains(sample.workload_class.as_str())
        })
        .cloned()
        .collect::<Vec<_>>();
    macro_sample_summary(&matched, group)
}

#[cfg(test)]
fn numeric_percentile(values: &[u64], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut values = values.to_vec();
    values.sort_unstable();
    let index = (values.len() - 1)
        .saturating_mul(percentile.min(100))
        .div_ceil(100);
    values[index.min(values.len() - 1)]
}

fn thread_is_running_in_state(thread_id: &str, thread: &ThreadSummary, state: &AppState) -> bool {
    thread_is_running(thread)
        || state.external_running_threads.contains(thread_id)
        || state.turn_progress.contains_key(thread_id)
        || (state.active_thread_id.as_deref() == Some(thread_id) && state.active_turn_id.is_some())
        || thread.turns.iter().any(|turn| {
            matches!(
                turn.status
                    .as_str()
                    .or_else(|| turn.status.get("type").and_then(Value::as_str)),
                Some("active" | "inProgress" | "running")
            )
        })
}

/// A task list includes summary rows from the other account so the user can
/// jump back to them. Those rows do not belong to this direct app-server, so
/// their status must never prevent an account switch.
fn has_current_profile_direct_running_work(state: &AppState, stored: &StoredState) -> bool {
    state.threads.iter().any(|(thread_id, thread)| {
        stored
            .task_owner(thread_id)
            .is_none_or(|owner| owner == stored.active_account_id)
            && thread_is_running_in_state(thread_id, thread, state)
    })
}

fn remote_transition_is_busy(state: &AppState) -> bool {
    state.active_turn_id.is_some()
        || !state.turn_progress.is_empty()
        || !state.external_running_threads.is_empty()
        || state
            .threads
            .iter()
            .any(|(thread_id, thread)| thread_is_running_in_state(thread_id, thread, state))
        || state.pending.values().any(|pending| {
            matches!(
                pending,
                PendingKind::OpenThread(_)
                    | PendingKind::ResumeAndSend { .. }
                    | PendingKind::StartThread { .. }
                    | PendingKind::SendTurn { .. }
                    | PendingKind::SteerTurn { .. }
            )
        })
}

fn cloned_active_turn_target(state: &RefCell<AppState>) -> Option<(String, String)> {
    let state = state.borrow();
    Some((
        state.active_thread_id.clone()?,
        state.active_turn_id.clone()?,
    ))
}

#[derive(Debug, Default)]
struct AgentSummary {
    total: usize,
    running: usize,
    tooltip: String,
}

#[derive(Debug, Default)]
struct AgentRecord {
    label: String,
    status: String,
}

fn thread_agent_summary(thread: &ThreadSummary, state: &AppState) -> AgentSummary {
    let mut agents = BTreeMap::<String, AgentRecord>::new();
    for item in thread.turns.iter().flat_map(|turn| &turn.items) {
        match item.get("type").and_then(Value::as_str) {
            Some("collabAgentToolCall") => {
                let states = item.get("agentsStates").and_then(Value::as_object);
                for id in item
                    .get("receiverThreadIds")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                {
                    let status = states
                        .and_then(|states| states.get(id))
                        .and_then(|state| state.get("status"))
                        .and_then(Value::as_str)
                        .unwrap_or("spawned");
                    let record = agents.entry(id.to_owned()).or_default();
                    if record.label.is_empty() {
                        record.label = compact_agent_id(id);
                    }
                    record.status = status.to_owned();
                }
            }
            Some("subAgentActivity") => {
                if let Some(id) = item.get("agentThreadId").and_then(Value::as_str) {
                    let status = match item.get("kind").and_then(Value::as_str) {
                        Some("interrupted") => "interrupted",
                        Some("started" | "interacted") => "running",
                        _ => "spawned",
                    };
                    let record = agents.entry(id.to_owned()).or_default();
                    if record.label.is_empty() {
                        record.label = item
                            .get("agentPath")
                            .and_then(Value::as_str)
                            .map(|path| compact_ui_text(path, 80))
                            .unwrap_or_else(|| compact_agent_id(id));
                    }
                    record.status = status.to_owned();
                }
            }
            _ => {}
        }
    }

    for candidate in state.threads.values().filter(|candidate| {
        candidate.id != thread.id && thread_descends_from(&candidate.id, &thread.id, state)
    }) {
        let record = agents.entry(candidate.id.clone()).or_default();
        record.label = candidate
            .agent_nickname
            .as_deref()
            .or(candidate.agent_role.as_deref())
            .map(|name| compact_ui_text(name, 80))
            .unwrap_or_else(|| compact_ui_text(candidate.title(), 80));
        record.status = if thread_is_running(candidate) {
            "running".to_owned()
        } else {
            candidate
                .status
                .as_str()
                .or_else(|| candidate.status.get("type").and_then(Value::as_str))
                .unwrap_or("idle")
                .to_owned()
        };
    }

    let running = agents
        .values()
        .filter(|agent| agent_status_is_running(&agent.status))
        .count();
    let total = agents.len();
    let mut lines = vec![format!(
        "{total} spawned agent{}{}",
        if total == 1 { "" } else { "s" },
        if running > 0 {
            format!(" · {running} running")
        } else {
            String::new()
        }
    )];
    for agent in agents.values().take(16) {
        lines.push(format!(
            "{} — {}",
            agent.label,
            agent_status_label(&agent.status)
        ));
    }
    if total > 16 {
        lines.push(format!("…and {} more", total - 16));
    }
    AgentSummary {
        total,
        running,
        tooltip: lines.join("\n"),
    }
}

fn thread_descends_from(candidate_id: &str, ancestor_id: &str, state: &AppState) -> bool {
    let mut current = state
        .threads
        .get(candidate_id)
        .and_then(|thread| thread.parent_thread_id.as_deref());
    for _ in 0..32 {
        let Some(parent_id) = current else {
            return false;
        };
        if parent_id == ancestor_id {
            return true;
        }
        current = state
            .threads
            .get(parent_id)
            .and_then(|thread| thread.parent_thread_id.as_deref());
    }
    false
}

fn compact_agent_id(id: &str) -> String {
    let short = id.chars().take(8).collect::<String>();
    format!("Agent {short}")
}

fn agent_status_is_running(status: &str) -> bool {
    matches!(status, "active" | "running" | "pendingInit" | "inProgress")
}

fn agent_status_label(status: &str) -> &str {
    match status {
        "pendingInit" => "Starting",
        "active" | "running" | "inProgress" => "Running",
        "completed" | "idle" => "Done",
        "interrupted" => "Interrupted",
        "errored" => "Errored",
        "shutdown" => "Closed",
        "notFound" => "Unavailable",
        _ => "Spawned",
    }
}

fn thread_running_description(thread: &ThreadSummary) -> &'static str {
    let flags = thread.status.get("activeFlags").and_then(Value::as_array);
    if flags.is_some_and(|flags| {
        flags
            .iter()
            .any(|flag| flag.as_str() == Some("waitingOnApproval"))
    }) {
        "Task running — waiting for approval"
    } else if flags.is_some_and(|flags| {
        flags
            .iter()
            .any(|flag| flag.as_str() == Some("waitingOnUserInput"))
    }) {
        "Task running — waiting for your input"
    } else {
        "Task running"
    }
}

fn goal_status_label(status: &str) -> &'static str {
    match status {
        "active" => "Active",
        "paused" => "Stopped",
        "blocked" => "Blocked",
        "usageLimited" => "Usage limited",
        "budgetLimited" => "Budget limited",
        "complete" => "Complete",
        _ => "Unknown",
    }
}

fn goal_status_reason(status: &str) -> Option<&'static str> {
    match status {
        "paused" => Some("Stopped manually."),
        "blocked" => Some("The goal was blocked by an error or required input."),
        "usageLimited" => Some("Stopped because the available Codex usage was exhausted."),
        "budgetLimited" => Some("Stopped after reaching the goal’s token budget."),
        _ => None,
    }
}

fn format_goal_time(seconds: i64) -> String {
    let seconds = seconds.max(0);
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    if hours > 0 {
        format!("{hours}h {minutes}m")
    } else if minutes > 0 {
        format!("{minutes}m")
    } else {
        format!("{seconds}s")
    }
}

fn turn_goal_stop_reason(params: &Value) -> Option<(&'static str, String)> {
    let turn = params.get("turn")?;
    if turn.get("status").and_then(Value::as_str) != Some("failed") {
        return None;
    }
    let error = turn.get("error")?;
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Codex stopped the goal after a turn failed.");
    let info = error.get("codexErrorInfo");
    let code = info.and_then(Value::as_str).or_else(|| {
        info.and_then(Value::as_object)
            .and_then(|value| value.keys().next().map(String::as_str))
    });
    Some(match code {
        Some("usageLimitExceeded") => (
            "usageLimited",
            "Stopped because the available Codex usage was exhausted.".to_owned(),
        ),
        Some("sessionBudgetExceeded") => (
            "budgetLimited",
            "Stopped after reaching the active session or goal budget.".to_owned(),
        ),
        Some(
            "httpConnectionFailed"
            | "responseStreamConnectionFailed"
            | "responseStreamDisconnected"
            | "responseTooManyFailedAttempts",
        ) => (
            "blocked",
            format!("Stopped after the Codex connection failed: {message}"),
        ),
        Some("unauthorized") => (
            "blocked",
            format!("Stopped because ChatGPT authentication failed: {message}"),
        ),
        Some("serverOverloaded" | "internalServerError") => (
            "blocked",
            format!("Stopped because the Codex service failed: {message}"),
        ),
        _ => ("blocked", format!("Stopped after a failed turn: {message}")),
    })
}

fn pin_context_action_label(is_pinned: bool) -> &'static str {
    if is_pinned { "Unpin task" } else { "Pin task" }
}

fn thread_context_mouse_button() -> u32 {
    gdk::BUTTON_SECONDARY
}

fn message_card(role: &str, content: &str, class: &str) -> gtk::Box {
    let (card, role_label) = message_card_shell(role, class);
    // GtkTextView exposes rendered Markdown visually, but some GTK/AT-SPI
    // combinations do not publish its buffer text. Attach a bounded message
    // summary to the always-visible role label so screen readers and release
    // smoke tests can still discover the content without retaining an
    // unbounded duplicate in accessibility state.
    let accessible_message = format!("{role}: {}", compact_ui_text(content, 4_096));
    role_label.update_property(&[gtk::accessible::Property::Label(&accessible_message)]);
    let body = markdown::render_rich(content);
    card.append(&body);
    card
}

fn streaming_message_card(role: &str, content: &str) -> (gtk::Box, gtk::Label) {
    let (card, role_label) = message_card_shell(role, "message-assistant");
    role_label.update_property(&[gtk::accessible::Property::Label(&format!(
        "{role}: {}",
        compact_ui_text(content, 4_096)
    ))]);
    let body = gtk::Label::new(Some(content));
    body.set_xalign(0.0);
    body.set_wrap(true);
    body.set_wrap_mode(gtk::pango::WrapMode::WordChar);
    body.set_selectable(true);
    body.add_css_class("streaming-message-body");
    card.append(&body);
    (card, body)
}

fn message_card_shell(role: &str, class: &str) -> (gtk::Box, gtk::Label) {
    let card = gtk::Box::new(gtk::Orientation::Vertical, 8);
    card.set_hexpand(true);
    card.add_css_class("message-card");
    card.add_css_class(class);
    let role_row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
    role_row.add_css_class("message-role-row");
    let icon_name = if role == "You" {
        "avatar-default-symbolic"
    } else if role == "Codex" {
        "applications-development-symbolic"
    } else {
        "dialog-information-symbolic"
    };
    let icon = gtk::Image::from_icon_name(icon_name);
    icon.set_pixel_size(14);
    role_row.append(&icon);
    let role_label = gtk::Label::new(Some(role));
    role_label.set_xalign(0.0);
    role_label.add_css_class("message-role");
    role_row.append(&role_label);
    card.append(&role_row);
    (card, role_label)
}

#[derive(Debug, Default, PartialEq, Eq)]
struct DiffStats {
    files: usize,
    additions: usize,
    deletions: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlanStepVisualState {
    Completed,
    Current,
    Pending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlanTooltipStep {
    label: String,
    state: PlanStepVisualState,
}

fn task_progress_pill(progress: &TurnProgress, spinner: &gtk::Label) -> gtk::Box {
    let pill = gtk::Box::new(gtk::Orientation::Horizontal, 7);
    pill.set_halign(gtk::Align::Center);
    pill.add_css_class("task-progress-pill");
    pill.append(spinner);

    let (current, total, current_step) = plan_position(&progress.plan);
    let step_label = if total > 0 {
        format!("Step {current} / {total}")
    } else {
        "Working".to_owned()
    };
    let step = gtk::Label::new(Some(&step_label));
    step.add_css_class("task-progress-step");
    pill.append(&step);

    if let Some(current_step) = current_step.as_deref() {
        append_progress_separator(&pill);
        let activity = gtk::Label::new(Some(&compact_ui_text(current_step, 120)));
        activity.add_css_class("muted");
        activity.set_ellipsize(gtk::pango::EllipsizeMode::End);
        activity.set_max_width_chars(42);
        pill.append(&activity);
    }

    let stats = diff_stats(&progress.diff);
    if stats.files > 0 {
        append_progress_separator(&pill);
        let files = gtk::Label::new(Some(&format!(
            "{} file{} changed",
            stats.files,
            if stats.files == 1 { "" } else { "s" }
        )));
        files.add_css_class("muted");
        pill.append(&files);
    }
    if stats.additions > 0 {
        append_progress_separator(&pill);
        let additions = gtk::Label::new(Some(&format!("+{}", stats.additions)));
        additions.add_css_class("progress-additions");
        pill.append(&additions);
    }
    if stats.deletions > 0 {
        let deletions = gtk::Label::new(Some(&format!("-{}", stats.deletions)));
        deletions.add_css_class("progress-deletions");
        pill.append(&deletions);
    }
    let tooltip_steps = plan_tooltip_steps(&progress.plan);
    if tooltip_steps.is_empty() {
        if let Some(current_step) = current_step {
            pill.set_tooltip_text(Some(&format!("Current step: {current_step}")));
        } else {
            pill.set_tooltip_text(Some("Codex is working on this task"));
        }
    } else {
        let description = plan_tooltip_accessible_text(&tooltip_steps);
        step.update_property(&[gtk::accessible::Property::Description(&description)]);
        pill.update_property(&[gtk::accessible::Property::Description(&description)]);
        pill.set_has_tooltip(true);
        pill.connect_query_tooltip(move |_, _, _, _, tooltip| {
            let content = plan_tooltip_widget(&tooltip_steps);
            tooltip.set_custom(Some(&content));
            true
        });
    }
    pill
}

fn append_progress_separator(pill: &gtk::Box) {
    let separator = gtk::Label::new(Some("·"));
    separator.add_css_class("muted");
    pill.append(&separator);
}

fn plan_position(plan: &[Value]) -> (usize, usize, Option<String>) {
    let total = plan.len();
    if total == 0 {
        return (0, 0, None);
    }
    let in_progress = plan.iter().position(|step| {
        matches!(
            step.get("status").and_then(Value::as_str),
            Some("inProgress" | "in_progress" | "active" | "running")
        )
    });
    let completed = plan
        .iter()
        .filter(|step| step.get("status").and_then(Value::as_str) == Some("completed"))
        .count();
    let index = in_progress.unwrap_or_else(|| {
        if completed >= total {
            total - 1
        } else {
            completed.min(total - 1)
        }
    });
    let label = plan[index]
        .get("step")
        .and_then(Value::as_str)
        .map(|step| compact_ui_text(step, 180));
    (index + 1, total, label)
}

fn plan_tooltip_steps(plan: &[Value]) -> Vec<PlanTooltipStep> {
    let completed = plan
        .iter()
        .filter(|step| step.get("status").and_then(Value::as_str) == Some("completed"))
        .count();
    let explicit_current = plan.iter().position(|step| {
        matches!(
            step.get("status").and_then(Value::as_str),
            Some("inProgress" | "in_progress" | "active" | "running")
        )
    });
    let current = explicit_current.or_else(|| {
        if completed < plan.len() {
            Some(completed.min(plan.len().saturating_sub(1)))
        } else {
            None
        }
    });
    plan.iter()
        .enumerate()
        .map(|(index, step)| {
            let label = step
                .get("step")
                .and_then(Value::as_str)
                .map(|label| compact_ui_text(label, 320))
                .unwrap_or_else(|| format!("Step {}", index + 1));
            let state = if step.get("status").and_then(Value::as_str) == Some("completed") {
                PlanStepVisualState::Completed
            } else if current == Some(index) {
                PlanStepVisualState::Current
            } else {
                PlanStepVisualState::Pending
            };
            PlanTooltipStep { label, state }
        })
        .collect()
}

fn plan_tooltip_accessible_text(steps: &[PlanTooltipStep]) -> String {
    steps
        .iter()
        .map(|step| {
            let status = match step.state {
                PlanStepVisualState::Completed => "Completed",
                PlanStepVisualState::Current => "Current",
                PlanStepVisualState::Pending => "Pending",
            };
            format!("{status}: {}", step.label)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn plan_tooltip_widget(steps: &[PlanTooltipStep]) -> gtk::Box {
    let content = gtk::Box::new(gtk::Orientation::Vertical, 8);
    content.add_css_class("plan-tooltip");
    for step in steps {
        let row = gtk::Box::new(gtk::Orientation::Horizontal, 8);
        row.add_css_class("plan-tooltip-row");
        match step.state {
            PlanStepVisualState::Completed => {
                let marker = gtk::Label::new(Some("✓"));
                marker.set_size_request(16, 16);
                marker.add_css_class("plan-tooltip-completed");
                row.append(&marker);
            }
            PlanStepVisualState::Current => {
                let marker = gtk::Spinner::new();
                marker.set_size_request(14, 14);
                marker.set_spinning(true);
                marker.add_css_class("plan-tooltip-current");
                row.append(&marker);
            }
            PlanStepVisualState::Pending => {
                let marker = gtk::Label::new(Some("○"));
                marker.set_size_request(16, 16);
                marker.add_css_class("plan-tooltip-pending");
                row.append(&marker);
            }
        }
        let label = gtk::Label::new(Some(&step.label));
        label.set_xalign(0.0);
        label.set_yalign(0.0);
        label.set_hexpand(true);
        label.set_wrap(true);
        label.set_wrap_mode(gtk::pango::WrapMode::WordChar);
        label.set_max_width_chars(46);
        if step.state == PlanStepVisualState::Current {
            label.add_css_class("plan-tooltip-current-label");
        }
        row.append(&label);
        content.append(&row);
    }
    content
}

fn diff_stats(diff: &str) -> DiffStats {
    let mut paths = BTreeSet::new();
    let mut additions = 0;
    let mut deletions = 0;
    for line in diff.lines() {
        if let Some(rest) = line.strip_prefix("diff --git ") {
            if let Some(path) = rest.split_whitespace().last() {
                let path = path.strip_prefix("b/").unwrap_or(path);
                if path != "/dev/null" {
                    paths.insert(path.to_owned());
                }
            }
        } else if let Some(path) = line.strip_prefix("+++ ") {
            let path = path.split('\t').next().unwrap_or(path);
            let path = path.strip_prefix("b/").unwrap_or(path);
            if path != "/dev/null" {
                paths.insert(path.to_owned());
            }
        } else if let Some(path) = line.strip_prefix("*** Update File: ") {
            paths.insert(path.to_owned());
        }

        if line.starts_with('+') && !line.starts_with("+++") {
            additions += 1;
        } else if line.starts_with('-') && !line.starts_with("---") {
            deletions += 1;
        }
    }
    DiffStats {
        files: paths.len(),
        additions,
        deletions,
    }
}

fn item_image_sources(item: &Value, cwd: &str) -> Vec<String> {
    let mut sources = Vec::new();
    match item.get("type").and_then(Value::as_str) {
        Some("imageView") => {
            if let Some(path) = item.get("path").and_then(Value::as_str) {
                sources.push(resolve_image_source(path, cwd));
            }
        }
        Some("imageGeneration") => {
            if let Some(path) = item.get("savedPath").and_then(Value::as_str) {
                sources.push(resolve_image_source(path, cwd));
            }
        }
        Some("userMessage") => {
            for content in item
                .get("content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let source = match content.get("type").and_then(Value::as_str) {
                    Some("localImage") => content.get("path").and_then(Value::as_str),
                    Some("image") => content.get("url").and_then(Value::as_str),
                    _ => None,
                };
                if let Some(source) = source {
                    sources.push(resolve_image_source(source, cwd));
                }
            }
        }
        _ => {}
    }
    sources
}

fn resolve_image_source(source: &str, cwd: &str) -> String {
    if source.contains("://") || source.starts_with("data:") {
        return source.to_owned();
    }
    let path = Path::new(source);
    if path.is_absolute() || cwd.is_empty() {
        path.to_string_lossy().into_owned()
    } else {
        Path::new(cwd).join(path).to_string_lossy().into_owned()
    }
}

fn image_thumbnail_button(source: &str, index: usize) -> gtk::Button {
    let button = gtk::Button::new();
    button.set_has_frame(false);
    button.add_css_class("image-thumbnail");
    let display = image_source_display(source);
    let accessible_label = format!("Open image {}: {display}", index + 1);
    button.update_property(&[gtk::accessible::Property::Label(&accessible_label)]);
    button.set_tooltip_text(Some(&accessible_label));

    if source.starts_with("data:") || (source.contains("://") && !source.starts_with("file://")) {
        let icon = gtk::Image::from_icon_name("image-x-generic-symbolic");
        icon.set_pixel_size(42);
        button.set_child(Some(&icon));
    } else {
        let file = if source.starts_with("file://") {
            gio::File::for_uri(source)
        } else {
            gio::File::for_path(source)
        };
        let picture = gtk::Picture::for_file(&file);
        picture.set_alternative_text(Some(&display));
        picture.set_content_fit(gtk::ContentFit::Cover);
        picture.set_can_shrink(true);
        picture.set_size_request(92, 72);
        button.set_child(Some(&picture));
    }
    button
}

fn image_activity_row(images: &[String], verb: &str) -> gtk::Button {
    let row = gtk::Button::new();
    row.set_has_frame(false);
    row.set_hexpand(true);
    row.add_css_class("image-activity-row");
    let content = gtk::Box::new(gtk::Orientation::Horizontal, 8);
    let icon = gtk::Image::from_icon_name("image-x-generic-symbolic");
    icon.set_pixel_size(15);
    icon.add_css_class("tool-card-icon");
    content.append(&icon);
    let label_text = if images.len() == 1 {
        format!("{verb} an image")
    } else {
        format!("{verb} {} images", images.len())
    };
    let label = gtk::Label::new(Some(&label_text));
    label.set_xalign(0.0);
    label.set_hexpand(true);
    label.add_css_class("message-role");
    content.append(&label);
    let open = gtk::Image::from_icon_name("go-next-symbolic");
    open.set_pixel_size(13);
    open.add_css_class("tool-card-icon");
    content.append(&open);
    row.set_tooltip_text(Some("Open image viewer"));
    row.set_child(Some(&content));
    row
}

fn present_image_viewer(
    parent: &adw::ApplicationWindow,
    images: Vec<String>,
    initial_index: usize,
) {
    if images.is_empty() {
        return;
    }
    let viewer = adw::Window::builder()
        .title("Image viewer")
        .transient_for(parent)
        .modal(true)
        .default_width(1100)
        .default_height(760)
        .build();
    let content = gtk::Box::new(gtk::Orientation::Vertical, 0);
    let header = adw::HeaderBar::new();
    let title = gtk::Label::new(None);
    title.set_ellipsize(gtk::pango::EllipsizeMode::Middle);
    title.set_max_width_chars(80);
    header.set_title_widget(Some(&title));
    let previous = gtk::Button::from_icon_name("go-previous-symbolic");
    previous.set_tooltip_text(Some("Previous image"));
    header.pack_start(&previous);
    let next = gtk::Button::from_icon_name("go-next-symbolic");
    next.set_tooltip_text(Some("Next image"));
    header.pack_start(&next);
    content.append(&header);

    let overlay = gtk::Overlay::new();
    overlay.set_hexpand(true);
    overlay.set_vexpand(true);
    let picture = gtk::Picture::new();
    picture.set_content_fit(gtk::ContentFit::Contain);
    picture.set_can_shrink(true);
    picture.set_hexpand(true);
    picture.set_vexpand(true);
    picture.add_css_class("image-viewer-picture");
    overlay.set_child(Some(&picture));
    let error = gtk::Label::new(None);
    error.set_wrap(true);
    error.set_justify(gtk::Justification::Center);
    error.set_halign(gtk::Align::Center);
    error.set_valign(gtk::Align::Center);
    error.add_css_class("image-viewer-error");
    overlay.add_overlay(&error);
    content.append(&overlay);
    viewer.set_content(Some(&content));

    let images = Rc::new(images);
    let initial_index = initial_index.min(images.len() - 1);
    let index = Rc::new(Cell::new(initial_index));
    update_image_viewer(
        &picture,
        &title,
        &error,
        &previous,
        &next,
        &images,
        initial_index,
    );
    {
        let picture = picture.clone();
        let title = title.clone();
        let error = error.clone();
        let previous_button = previous.clone();
        let next_button = next.clone();
        let images = images.clone();
        let index = index.clone();
        previous.connect_clicked(move |_| {
            let next_index = index.get().saturating_sub(1);
            index.set(next_index);
            update_image_viewer(
                &picture,
                &title,
                &error,
                &previous_button,
                &next_button,
                &images,
                next_index,
            );
        });
    }
    {
        let picture = picture.clone();
        let title = title.clone();
        let error = error.clone();
        let previous_button = previous.clone();
        let next_button = next.clone();
        let images = images.clone();
        let index = index.clone();
        next.connect_clicked(move |_| {
            let next_index = (index.get() + 1).min(images.len() - 1);
            index.set(next_index);
            update_image_viewer(
                &picture,
                &title,
                &error,
                &previous_button,
                &next_button,
                &images,
                next_index,
            );
        });
    }
    viewer.present();
}

fn update_image_viewer(
    picture: &gtk::Picture,
    title: &gtk::Label,
    error: &gtk::Label,
    previous: &gtk::Button,
    next: &gtk::Button,
    images: &[String],
    index: usize,
) {
    let source = &images[index];
    let display = image_source_display(source);
    title.set_label(&format!("{display} · {} / {}", index + 1, images.len()));
    previous.set_sensitive(index > 0);
    next.set_sensitive(index + 1 < images.len());
    let file = if source.contains("://") {
        gio::File::for_uri(source)
    } else {
        gio::File::for_path(source)
    };
    match gdk::Texture::from_file(&file) {
        Ok(texture) => {
            picture.set_paintable(Some(&texture));
            error.set_label("");
            error.set_visible(false);
        }
        Err(load_error) => {
            picture.set_paintable(None::<&gdk::Texture>);
            error.set_label(&format!("Could not load {display}\n{load_error}"));
            error.set_visible(true);
        }
    }
}

fn image_source_display(source: &str) -> String {
    if source.starts_with("data:") {
        return "Embedded image".to_owned();
    }
    source
        .trim_end_matches('/')
        .rsplit(['/', '\\'])
        .next()
        .filter(|name| !name.is_empty())
        .map(|name| compact_ui_text(name, 100))
        .unwrap_or_else(|| "Image".to_owned())
}

fn tool_card(title: &str, summary: &str, output: &str, language: &str) -> gtk::Box {
    let card = gtk::Box::new(gtk::Orientation::Vertical, 0);
    card.set_hexpand(true);
    card.add_css_class("tool-card");
    let header = gtk::Box::new(gtk::Orientation::Horizontal, 8);
    header.add_css_class("tool-card-header");
    let icon_name = if title.starts_with("Ran command") || title.starts_with("Running command") {
        "utilities-terminal-symbolic"
    } else if title.starts_with("Edited") || title.starts_with("Editing") {
        "document-edit-symbolic"
    } else {
        "system-run-symbolic"
    };
    let icon = gtk::Image::from_icon_name(icon_name);
    icon.set_pixel_size(14);
    icon.add_css_class("tool-card-icon");
    header.append(&icon);
    let title_label = gtk::Label::new(Some(title));
    title_label.set_xalign(0.0);
    title_label.add_css_class("message-role");
    header.append(&title_label);
    if !summary.is_empty() {
        let summary_label = gtk::Label::new(Some(summary));
        summary_label.set_xalign(0.0);
        summary_label.set_hexpand(true);
        summary_label.set_ellipsize(gtk::pango::EllipsizeMode::End);
        summary_label.set_single_line_mode(true);
        summary_label.set_tooltip_text(Some(summary));
        summary_label.add_css_class("tool-card-summary");
        header.append(&summary_label);
    }
    if !output.is_empty() {
        let expander = gtk::Expander::new(None);
        expander.set_label_widget(Some(&header));
        expander.set_expanded(false);
        let output_view = code_block(output, language);
        output_view.add_css_class("tool-output");
        expander.set_child(Some(&output_view));
        card.append(&expander);
    } else {
        card.append(&header);
    }
    card
}

fn code_block(content: &str, language_id: &str) -> gtk::ScrolledWindow {
    let buffer = sourceview::Buffer::new(None::<&gtk::TextTagTable>);
    buffer.set_text(content);
    markdown::configure_source_buffer(&buffer, language_id);
    let view = sourceview::View::with_buffer(&buffer);
    view.set_editable(false);
    view.set_cursor_visible(false);
    view.set_monospace(true);
    view.set_show_line_numbers(true);
    view.set_highlight_current_line(false);
    view.set_wrap_mode(gtk::WrapMode::None);
    view.set_top_margin(8);
    view.set_bottom_margin(8);
    view.set_left_margin(8);
    view.set_right_margin(8);
    view.add_css_class("source-code-view");
    let height = (content.lines().count().clamp(2, 18) as i32) * 21;
    let scroller = gtk::ScrolledWindow::builder()
        .hscrollbar_policy(gtk::PolicyType::Automatic)
        .vscrollbar_policy(gtk::PolicyType::Automatic)
        .min_content_height(height)
        .max_content_height(height)
        .propagate_natural_height(true)
        .child(&view)
        .build();
    scroller.add_css_class("code-scroller");
    scroller
}

fn command_activity_label(item: &Value) -> &'static str {
    match status_text(item) {
        "completed" | "success" => "Ran command",
        "failed" | "error" => "Command failed",
        _ => "Running command",
    }
}

fn file_activity_label(item: &Value) -> &'static str {
    match status_text(item) {
        "completed" | "success" => "Edited files",
        "failed" | "error" => "File edit failed",
        _ => "Editing files",
    }
}

fn file_change_display(item: &Value) -> (String, String) {
    let Some(changes) = item.get("changes").and_then(Value::as_array) else {
        return (
            "Proposed patch".to_owned(),
            item.get("changes").map(pretty_value).unwrap_or_default(),
        );
    };

    let paths = changes
        .iter()
        .filter_map(|change| change.get("path").and_then(Value::as_str))
        .map(|path| compact_ui_text(path, 120))
        .collect::<Vec<_>>();
    let summary = match paths.as_slice() {
        [] => format!(
            "{} file change{}",
            changes.len(),
            if changes.len() == 1 { "" } else { "s" }
        ),
        [path] => format!("Edited {path}"),
        [first, ..] => format!("Edited {first} and {} more", paths.len() - 1),
    };

    let mut rendered = Vec::new();
    for change in changes {
        let path = change
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or("changed file");
        if let Some(diff) = change.get("diff").and_then(Value::as_str) {
            rendered.push(format!("# {path}\n{diff}"));
        } else {
            rendered.push(format!("# {path}\n{}", pretty_value(change)));
        }
    }
    (summary, rendered.join("\n\n"))
}

fn file_change_paths(item: &Value, cwd: &str) -> Vec<PathBuf> {
    let mut paths = BTreeSet::new();
    for change in item
        .get("changes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        for key in ["path", "movePath", "move_path"] {
            let Some(value) = change.get(key).and_then(Value::as_str) else {
                continue;
            };
            let path = PathBuf::from(value);
            let resolved = if path.is_absolute() || cwd.is_empty() {
                path
            } else {
                Path::new(cwd).join(path)
            };
            paths.insert(resolved);
        }
    }
    paths.into_iter().collect()
}

fn user_message_text(item: &Value) -> String {
    if let Some(text) = item
        .get("text")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
    {
        return text.to_owned();
    }
    item.get("content")
        .and_then(Value::as_array)
        .map(|parts| {
            parts
                .iter()
                .filter_map(|part| {
                    part.get("text")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .or_else(|| {
                            part.get("path")
                                .and_then(Value::as_str)
                                .map(|path| format!("Attached: {path}"))
                        })
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

fn buddy_thread_context(thread: &ThreadSummary) -> String {
    const MAX_CONTEXT_BYTES: usize = 24_000;
    let mut messages = Vec::new();
    for turn in &thread.turns {
        for item in &turn.items {
            match item.get("type").and_then(Value::as_str) {
                Some("userMessage") => {
                    let text = user_message_text(item);
                    if !text.trim().is_empty() {
                        messages.push(format!("User: {text}"));
                    }
                }
                Some("agentMessage") => {
                    let text = item.get("text").and_then(Value::as_str).unwrap_or_default();
                    if !text.trim().is_empty() {
                        let author = item
                            .get("author")
                            .and_then(Value::as_str)
                            .unwrap_or("Codex");
                        messages.push(format!("{author}: {text}"));
                    }
                }
                _ => {}
            }
        }
    }
    let context = messages.join("\n\n");
    if context.len() <= MAX_CONTEXT_BYTES {
        return context;
    }
    let mut start = context.len() - MAX_CONTEXT_BYTES;
    while !context.is_char_boundary(start) {
        start += 1;
    }
    format!("[Earlier conversation omitted]\n\n{}", &context[start..])
}

fn restored_buddy_thread(thread_id: &str, turns: Vec<Turn>, cwd: String) -> ThreadSummary {
    let preview = turns
        .iter()
        .flat_map(|turn| turn.items.iter())
        .find(|item| item.get("type").and_then(Value::as_str) == Some("userMessage"))
        .map(user_message_text)
        .map(|text| compact_ui_text(&text, 120))
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| "Buddy conversation".into());
    let cwd = turns
        .iter()
        .flat_map(|turn| turn.items.iter())
        .find_map(|item| item.get("cwd").and_then(Value::as_str))
        .filter(|path| !path.is_empty())
        .map(str::to_owned)
        .unwrap_or(cwd);
    ThreadSummary {
        id: thread_id.to_owned(),
        preview: preview.clone(),
        name: Some(preview),
        cwd,
        status: json!({"type": "idle"}),
        turns,
        native_buddy_only: true,
        ..ThreadSummary::default()
    }
}

fn upsert_buddy_turn(stored: &mut StoredState, thread_id: &str, turn: Turn) {
    let turns = stored.buddy_turns.entry(thread_id.to_owned()).or_default();
    if let Some(existing) = turns.iter_mut().find(|existing| existing.id == turn.id) {
        *existing = turn;
    } else {
        turns.push(turn);
    }
    if turns.len() > 80 {
        turns.drain(..turns.len() - 80);
    }
}

fn extract_reasoning(item: &Value) -> String {
    item.get("summary")
        .or_else(|| item.get("content"))
        .map(value_to_text)
        .unwrap_or_default()
}

fn value_to_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .map(value_to_text)
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Object(values) => values
            .get("text")
            .or_else(|| values.get("content"))
            .or_else(|| values.get("value"))
            .map(value_to_text)
            .unwrap_or_else(|| pretty_value(value)),
        Value::Bool(_) | Value::Number(_) => value.to_string(),
    }
}

fn pretty_value(value: &Value) -> String {
    let mut text = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
    const LIMIT: usize = 24_000;
    if text.len() > LIMIT {
        text.truncate(LIMIT);
        text.push_str("\n… output truncated in the UI …");
    }
    text
}

fn rate_limit_snapshot(value: &Value) -> Option<&Value> {
    value
        .pointer("/rateLimitsByLimitId/codex")
        .or_else(|| {
            value
                .get("rateLimitsByLimitId")
                .and_then(Value::as_object)
                .and_then(|limits| {
                    limits.values().find(|snapshot| {
                        snapshot.get("limitId").and_then(Value::as_str) == Some("codex")
                    })
                })
        })
        .or_else(|| value.get("rateLimits"))
}

fn weekly_limit_details(value: &Value) -> Option<(i64, Option<i64>, Option<i64>)> {
    let snapshot = rate_limit_snapshot(value)?;
    let primary = snapshot.get("primary").filter(|value| value.is_object());
    let secondary = snapshot.get("secondary").filter(|value| value.is_object());
    let window = [(0_i64, primary), (1_i64, secondary)]
        .into_iter()
        .filter_map(|(fallback, window)| window.map(|window| (fallback, window)))
        .max_by_key(|(fallback, window)| {
            window
                .get("windowDurationMins")
                .and_then(Value::as_i64)
                .unwrap_or(*fallback)
        })?
        .1;
    let used = window.get("usedPercent")?.as_i64()?.clamp(0, 100);
    Some((
        100 - used,
        window.get("resetsAt").and_then(Value::as_i64),
        window.get("windowDurationMins").and_then(Value::as_i64),
    ))
}

fn five_hour_limit_details(value: &Value) -> Option<(i64, Option<i64>)> {
    let snapshot = rate_limit_snapshot(value)?;
    let primary = snapshot.get("primary").filter(|value| value.is_object())?;
    let duration = primary
        .get("windowDurationMins")
        .and_then(Value::as_i64)
        .unwrap_or(300);
    if duration != 300 {
        return None;
    }
    let used = primary.get("usedPercent")?.as_i64()?.clamp(0, 100);
    Some((100 - used, primary.get("resetsAt").and_then(Value::as_i64)))
}

#[derive(Debug, PartialEq, Eq)]
enum OneTimeResetDecision {
    Wait,
    Consume { credit_id: Option<String> },
}

fn one_time_reset_decision(guard: &OneTimeResetGuard, limits: &Value) -> OneTimeResetDecision {
    if !guard.armed {
        return OneTimeResetDecision::Wait;
    }
    let retry_pending = guard.outcome.as_deref() == Some("pending")
        && guard
            .idempotency_key
            .as_deref()
            .is_some_and(|key| !key.is_empty());
    if !retry_pending {
        let Some((remaining, _, _)) = weekly_limit_details(limits) else {
            return OneTimeResetDecision::Wait;
        };
        if remaining > i64::from(guard.threshold_percent) {
            return OneTimeResetDecision::Wait;
        }
    }
    let credit_id = limits
        .pointer("/rateLimitResetCredits/credits")
        .and_then(Value::as_array)
        .and_then(|credits| {
            credits
                .iter()
                .find(|credit| credit.get("status").and_then(Value::as_str) == Some("available"))
        })
        .and_then(|credit| credit.get("id"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    OneTimeResetDecision::Consume { credit_id }
}

fn reset_credit_count(value: &Value) -> i64 {
    value
        .pointer("/rateLimitResetCredits/availableCount")
        .and_then(Value::as_i64)
        .unwrap_or(0)
        .max(0)
}

fn format_timestamp(value: i64) -> Option<String> {
    chrono::DateTime::<chrono::Utc>::from_timestamp(value, 0)
        .map(|timestamp| timestamp.format("%a %b %e, %H:%M UTC").to_string())
}

fn format_timestamp_local(value: i64) -> Option<String> {
    format_timestamp_in_timezone(value, &chrono::Local)
}

fn format_timestamp_in_timezone<Tz>(value: i64, timezone: &Tz) -> Option<String>
where
    Tz: chrono::TimeZone,
    Tz::Offset: std::fmt::Display,
{
    chrono::DateTime::<chrono::Utc>::from_timestamp(value, 0).map(|timestamp| {
        timestamp
            .with_timezone(timezone)
            .format("%a %b %e, %H:%M %:z")
            .to_string()
    })
}

fn format_integer(value: i64) -> String {
    let negative = value.is_negative();
    let digits = value.unsigned_abs().to_string();
    let mut grouped =
        String::with_capacity(digits.len() + digits.len() / 3 + usize::from(negative));
    if negative {
        grouped.push('-');
    }
    for (index, character) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(character);
    }
    grouped
}

fn context_usage_label(value: &Value) -> String {
    let current = context::usage_snapshot(Some(value));
    let cumulative = value.pointer("/total/totalTokens").and_then(Value::as_u64);
    let provider = value
        .get("provider")
        .and_then(Value::as_str)
        .filter(|provider| !provider.trim().is_empty());
    let context_prefix = provider
        .map(|provider| format!("{provider} context"))
        .unwrap_or_else(|| "Current context".to_owned());
    let current_tokens = format_integer(i64::try_from(current.total_tokens).unwrap_or(i64::MAX));
    let mut label = if current.context_window > 0 {
        format!(
            "{context_prefix} {current_tokens} / {} tokens ({}%)",
            format_integer(i64::try_from(current.context_window).unwrap_or(i64::MAX)),
            current.percent
        )
    } else {
        format!("{context_prefix} {current_tokens} tokens")
    };
    if let Some(tokens_per_second) = value
        .get("tokensPerSecond")
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && *value > 0.0)
    {
        label.push_str(&format!(" · {tokens_per_second:.1} tok/s"));
    }
    let displayed_total = provider
        .and_then(|_| value.get("providerTotalTokens").and_then(Value::as_u64))
        .or(cumulative);
    if let Some(displayed_total) = displayed_total {
        label.push_str(&format!(
            " · {}total {} tokens",
            provider
                .map(|provider| format!("{provider} task "))
                .unwrap_or_else(|| "Thread ".to_owned()),
            format_integer(i64::try_from(displayed_total).unwrap_or(i64::MAX))
        ));
    }
    if provider.is_some()
        && let Some(cumulative) = cumulative.filter(|total| Some(*total) != displayed_total)
    {
        label.push_str(&format!(
            " · All Buddy models {} tokens",
            format_integer(i64::try_from(cumulative).unwrap_or(i64::MAX))
        ));
    }
    label
}

fn task_history_rollover_warning(size: Option<u64>) -> Option<String> {
    let bytes = size.filter(|bytes| *bytes >= TASK_ROLLOVER_WARN_BYTES)?;
    let gibibytes = bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    Some(format!(
        "History {gibibytes:.1} GiB · continuation recommended"
    ))
}

fn status_text(item: &Value) -> &str {
    item.get("status")
        .and_then(Value::as_str)
        .unwrap_or("running")
}

#[derive(Debug, PartialEq, Eq)]
struct ActivityPresentation {
    title: String,
    summary: String,
    detail: String,
    language: &'static str,
}

fn context_compaction_event(method: &str, params: &Value) -> Option<ContextCompactionEvent> {
    let phase = match method {
        "item/started" => ContextCompactionPhase::Started,
        "item/completed" => ContextCompactionPhase::Completed,
        // Older app-server builds emitted this deprecated terminal signal.
        "context/compacted" => ContextCompactionPhase::Completed,
        _ => return None,
    };
    let item = params.get("item");
    if method != "context/compacted"
        && item
            .and_then(|item| item.get("type"))
            .and_then(Value::as_str)
            != Some("contextCompaction")
    {
        return None;
    }
    let thread_id = params.get("threadId").and_then(Value::as_str)?.to_owned();
    let turn_id = params
        .get("turnId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let item_id = item
        .and_then(|item| item.get("id"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| format!("legacy-{thread_id}-{turn_id}"));
    Some(ContextCompactionEvent {
        phase,
        thread_id,
        turn_id,
        item_id,
    })
}

fn canonical_activity_presentation(item: &Value) -> Option<ActivityPresentation> {
    let kind = item.get("type").and_then(Value::as_str)?;
    let status_suffix = item
        .get("status")
        .and_then(Value::as_str)
        .map(|status| format!(" · {status}"))
        .unwrap_or_default();
    match kind {
        "hookPrompt" => {
            let fragments = item
                .get("fragments")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let detail = fragments
                .iter()
                .filter_map(|fragment| fragment.get("text").and_then(Value::as_str))
                .filter(|text| !text.trim().is_empty())
                .collect::<Vec<_>>()
                .join("\n\n");
            Some(ActivityPresentation {
                title: "Hook supplied context".into(),
                summary: format!(
                    "{} context fragment{}",
                    fragments.len(),
                    if fragments.len() == 1 { "" } else { "s" }
                ),
                detail: bounded_activity_detail(&detail),
                language: "text",
            })
        }
        "collabAgentToolCall" => {
            let tool = item.get("tool").and_then(Value::as_str).unwrap_or("agent");
            let title = match tool {
                "spawnAgent" => "Spawned agent",
                "sendInput" => "Sent input to agent",
                "resumeAgent" => "Resumed agent",
                "wait" => "Waited for agents",
                "closeAgent" => "Closed agent",
                _ => "Agent collaboration",
            };
            let receivers = item
                .get("receiverThreadIds")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or_default();
            let mut summary = Vec::new();
            if !receivers.is_empty() {
                summary.push(format!(
                    "{} agent{}",
                    receivers.len(),
                    if receivers.len() == 1 { "" } else { "s" }
                ));
            }
            if let Some(model) = item.get("model").and_then(Value::as_str) {
                summary.push(compact_activity_text(model, 80));
            }
            if let Some(effort) = item.get("reasoningEffort").and_then(Value::as_str) {
                summary.push(format!("{effort} reasoning"));
            }
            let mut details = Vec::new();
            if let Some(prompt) = item
                .get("prompt")
                .and_then(Value::as_str)
                .filter(|prompt| !prompt.trim().is_empty())
            {
                details.push(format!("Prompt\n{}", bounded_activity_detail(prompt)));
            }
            if let Some(states) = item.get("agentsStates").filter(|states| !states.is_null()) {
                details.push(format!("Agent states\n{}", pretty_value(states)));
            }
            Some(ActivityPresentation {
                title: format!("{title}{status_suffix}"),
                summary: summary.join(" · "),
                detail: bounded_activity_detail(&details.join("\n\n")),
                language: "json",
            })
        }
        "subAgentActivity" => {
            let activity_kind = item
                .get("kind")
                .and_then(Value::as_str)
                .unwrap_or("interacted");
            let title = match activity_kind {
                "started" => "Subagent started",
                "interrupted" => "Subagent interrupted",
                _ => "Subagent activity",
            };
            let summary = item
                .get("agentPath")
                .or_else(|| item.get("agentThreadId"))
                .map(value_to_text)
                .map(|value| compact_activity_text(&value, 180))
                .unwrap_or_default();
            Some(ActivityPresentation {
                title: title.into(),
                summary,
                detail: String::new(),
                language: "text",
            })
        }
        "sleep" => {
            let duration_ms = item
                .get("durationMs")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            Some(ActivityPresentation {
                title: "Waited".into(),
                summary: format_activity_duration(duration_ms),
                detail: String::new(),
                language: "text",
            })
        }
        "enteredReviewMode" | "exitedReviewMode" => {
            let review = item.get("review").filter(|value| !value.is_null());
            let summary = review
                .map(value_to_text)
                .map(|value| compact_activity_text(&value, 180))
                .unwrap_or_default();
            let detail = review
                .filter(|value| !value.is_string())
                .map(pretty_value)
                .map(|value| bounded_activity_detail(&value))
                .unwrap_or_default();
            Some(ActivityPresentation {
                title: if kind == "enteredReviewMode" {
                    "Entered review mode".into()
                } else {
                    "Exited review mode".into()
                },
                summary,
                detail,
                language: "json",
            })
        }
        "contextCompaction" => Some(ActivityPresentation {
            title: "Context automatically compacted".into(),
            summary: String::new(),
            detail: String::new(),
            language: "text",
        }),
        _ => None,
    }
}

fn compact_activity_text(text: &str, max_chars: usize) -> String {
    if text.trim().is_empty() {
        String::new()
    } else {
        compact_ui_text(text, max_chars)
    }
}

fn bounded_activity_detail(text: &str) -> String {
    const MAX_CHARS: usize = 12_000;
    let mut chars = text.chars();
    let mut bounded = chars.by_ref().take(MAX_CHARS).collect::<String>();
    if chars.next().is_some() {
        bounded.push_str("\n… activity detail truncated in the UI …");
    }
    bounded
}

fn format_activity_duration(duration_ms: u64) -> String {
    if duration_ms < 1_000 {
        return format!("{duration_ms} ms");
    }
    let seconds = duration_ms / 1_000;
    if seconds < 60 {
        return format!("{seconds} s");
    }
    let minutes = seconds / 60;
    let remainder = seconds % 60;
    if remainder == 0 {
        format!("{minutes} min")
    } else {
        format!("{minutes} min {remainder} s")
    }
}

fn approval_description(request: &ApprovalRequest) -> String {
    request
        .params
        .get("reason")
        .or_else(|| request.params.get("command"))
        .or_else(|| request.params.get("message"))
        .map(value_to_text)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| match request.method.as_str() {
            "item/tool/requestUserInput" => request
                .params
                .get("questions")
                .and_then(Value::as_array)
                .and_then(|values| values.first())
                .and_then(|value| value.get("question"))
                .and_then(Value::as_str)
                .unwrap_or("Codex needs your input")
                .to_owned(),
            _ => request.method.clone(),
        })
}

fn section_heading(title: &str) -> gtk::Label {
    let heading = gtk::Label::new(Some(title));
    heading.set_xalign(0.0);
    heading.add_css_class("heading");
    heading.set_margin_top(8);
    heading
}

fn plugin_section_title(installed_only: bool, count: usize) -> String {
    if installed_only {
        format!("Installed Plugins ({count})")
    } else {
        format!("Plugins ({count})")
    }
}

fn flatten_skills(values: &[Value]) -> Vec<Value> {
    let mut flattened = Vec::new();
    for value in values {
        if let Some(nested) = value.get("skills").and_then(Value::as_array) {
            flattened.extend(flatten_skills(nested));
        } else if value.get("name").is_some() || value.get("path").is_some() {
            flattened.push(value.clone());
        }
    }
    flattened
}

fn auth_canonical_label(value: &str) -> &str {
    match value {
        "notLoggedIn" => "sign-in required",
        "bearerToken" => "token",
        "oAuth" => "OAuth",
        "unsupported" => "not required",
        _ => value,
    }
}

fn elicitation_choices(property: &Value) -> Option<Vec<String>> {
    if let Some(values) = property.get("enum").and_then(Value::as_array) {
        return Some(
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
        );
    }
    if let Some(values) = property.get("oneOf").and_then(Value::as_array) {
        return Some(
            values
                .iter()
                .filter_map(|value| value.get("const").and_then(Value::as_str))
                .map(str::to_owned)
                .collect(),
        );
    }
    if let Some(values) = property.pointer("/items/anyOf").and_then(Value::as_array) {
        return Some(
            values
                .iter()
                .filter_map(|value| value.get("const").and_then(Value::as_str))
                .map(str::to_owned)
                .collect(),
        );
    }
    property
        .pointer("/items/enum")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
}

fn config_plugin_value<'a>(
    config: Option<&'a Value>,
    plugin_id: &str,
    path: &[&str],
) -> Option<&'a Value> {
    let config = config?;
    let mut value = config
        .get("config")
        .unwrap_or(config)
        .get("plugins")?
        .get(plugin_id)?;
    for segment in path {
        value = value.get(*segment)?;
    }
    Some(value)
}

fn show_value_dialog(parent: &adw::ApplicationWindow, title: &str, value: &Value) {
    let viewer = code_block(&pretty_value(value), "json");
    viewer.set_min_content_width(620);
    let dialog = adw::AlertDialog::new(Some(title), None);
    dialog.set_extra_child(Some(&viewer));
    dialog.add_response("close", "Close");
    dialog.set_close_response("close");
    dialog.choose(Some(parent), None::<&gio::Cancellable>, |_| {});
}

fn extension_summary(value: &Value) -> (String, String) {
    let name = ["displayName", "name", "id", "serverName", "path"]
        .into_iter()
        .find_map(|key| value.get(key).and_then(Value::as_str))
        .unwrap_or("Extension")
        .to_owned();
    let detail = ["description", "status", "version", "path"]
        .into_iter()
        .find_map(|key| value.get(key).map(value_to_text))
        .unwrap_or_default();
    (name, detail)
}

fn info_row(title: &str, subtitle: &str) -> gtk::Box {
    let row = gtk::Box::new(gtk::Orientation::Horizontal, 10);
    row.add_css_class("settings-card");
    let text = gtk::Box::new(gtk::Orientation::Vertical, 2);
    text.set_hexpand(true);
    let title = gtk::Label::new(Some(title));
    title.set_xalign(0.0);
    text.append(&title);
    if !subtitle.is_empty() {
        let subtitle = gtk::Label::new(Some(subtitle));
        subtitle.set_xalign(0.0);
        subtitle.set_wrap(true);
        subtitle.add_css_class("caption");
        text.append(&subtitle);
    }
    row.append(&text);
    row
}

fn empty_label(text: &str) -> gtk::Label {
    let label = gtk::Label::new(Some(text));
    label.set_xalign(0.0);
    label.set_wrap(true);
    label.add_css_class("muted");
    label
}

fn with_controller(weak: &Weak<Controller>, action: impl FnOnce(&Controller)) {
    if let Some(controller) = weak.upgrade() {
        action(&controller);
    }
}

fn icon_button(icon: &str, tooltip: &str) -> gtk::Button {
    gtk::Button::builder()
        .icon_name(icon)
        .tooltip_text(tooltip)
        .has_frame(false)
        .build()
}

fn branding_image(file_name: &str, tooltip: &str) -> gtk::Image {
    let installed_scalable = Path::new("/usr/share/icons/hicolor/scalable/apps").join(file_name);
    let installed_raster = Path::new("/usr/share/icons/hicolor/48x48/apps").join(file_name);
    let bundled = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("data/icons")
        .join(file_name);
    let image = gtk::Image::new();
    image.set_pixel_size(22);
    image.set_tooltip_text(Some(tooltip));
    if file_name == "chatgpt-symbol.ico" {
        image.add_css_class("brand-icon-chatgpt");
    }
    for path in [installed_scalable, installed_raster, bundled] {
        if let Ok(texture) = gdk::Texture::from_file(&gio::File::for_path(path)) {
            image.set_paintable(Some(&texture));
            return image;
        }
    }
    image.set_icon_name(Some("image-missing-symbolic"));
    image
}

fn labeled_icon_button(label: &str, icon: &str) -> gtk::Button {
    let content = gtk::Box::new(gtk::Orientation::Horizontal, 6);
    content.set_halign(gtk::Align::Center);
    content.append(&gtk::Image::from_icon_name(icon));
    content.append(&gtk::Label::new(Some(label)));
    gtk::Button::builder().child(&content).build()
}

fn set_subagents_toggle_appearance(toggle: &gtk::ToggleButton, enabled: bool) {
    toggle.set_label(if enabled {
        "Subagents: On"
    } else {
        "Subagents: Off"
    });
    toggle.set_tooltip_text(Some(if enabled {
        "Subagents are allowed for this task. Click to disable local and cloud delegation."
    } else {
        "Subagents are disabled for this task. Click to allow local and cloud delegation."
    }));
}

fn compact_combo(values: &[(&str, &str)]) -> gtk::ComboBoxText {
    let combo = gtk::ComboBoxText::new();
    for (id, label) in values {
        combo.append(Some(id), label);
    }
    combo.set_active(Some(0));
    combo
}

fn page_shell(title: &str, subtitle: &str) -> gtk::Box {
    let page = gtk::Box::new(gtk::Orientation::Vertical, 10);
    page.set_margin_top(18);
    page.set_margin_bottom(18);
    page.set_margin_start(18);
    page.set_margin_end(18);
    let title = gtk::Label::new(Some(title));
    title.set_xalign(0.0);
    title.add_css_class("panel-title");
    page.append(&title);
    let subtitle = gtk::Label::new(Some(subtitle));
    subtitle.set_xalign(0.0);
    subtitle.set_wrap(true);
    subtitle.add_css_class("muted");
    page.append(&subtitle);
    page
}

fn attach_setting(grid: &gtk::Grid, row: i32, title: &str, control: &impl IsA<gtk::Widget>) {
    let label = gtk::Label::new(Some(title));
    label.set_xalign(0.0);
    label.set_hexpand(true);
    grid.attach(&label, 0, row, 1, 1);
    grid.attach(control, 1, row, 1, 1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::RequestId;

    #[test]
    fn sleep_inhibition_tracks_tasks_and_remote_host_availability() {
        assert!(should_inhibit_sleep(true, &RemoteState::Connected, false));
        assert!(should_inhibit_sleep(true, &RemoteState::Disabled, true));
        assert!(!should_inhibit_sleep(false, &RemoteState::Connected, true));
        assert!(!should_inhibit_sleep(true, &RemoteState::Disabled, false));
    }

    #[test]
    fn subagents_metadata_is_serialized_as_a_string() {
        assert_eq!(
            subagents_enabled_metadata_value(true),
            Value::String("true".into())
        );
        assert_eq!(
            subagents_enabled_metadata_value(false),
            Value::String("false".into())
        );
    }

    #[test]
    fn weekly_usage_chooses_the_longest_rate_limit_window() {
        let limits = json!({
            "rateLimits": {
                "primary": {"usedPercent": 25, "windowDurationMins": 300},
                "secondary": {
                    "usedPercent": 63,
                    "windowDurationMins": 10080,
                    "resetsAt": 1_800_000_000
                }
            },
            "rateLimitResetCredits": {"availableCount": 2}
        });
        assert_eq!(
            weekly_limit_details(&limits),
            Some((37, Some(1_800_000_000), Some(10080)))
        );
        assert_eq!(reset_credit_count(&limits), 2);
    }

    #[test]
    fn five_hour_usage_reads_the_primary_five_hour_window() {
        let limits = json!({
            "rateLimits": {
                "primary": {
                    "usedPercent": 25,
                    "windowDurationMins": 300,
                    "resetsAt": 1_800_000_000
                },
                "secondary": {"usedPercent": 63, "windowDurationMins": 10080}
            }
        });
        assert_eq!(
            five_hour_limit_details(&limits),
            Some((75, Some(1_800_000_000)))
        );
    }

    #[test]
    fn five_hour_usage_is_hidden_when_primary_window_is_not_five_hours() {
        let limits = json!({
            "rateLimits": {
                "primary": {"usedPercent": 25, "windowDurationMins": 60}
            }
        });
        assert_eq!(five_hour_limit_details(&limits), None);
    }

    #[test]
    fn reset_timestamp_formats_in_the_requested_timezone() {
        let india = chrono::FixedOffset::east_opt(5 * 60 * 60 + 30 * 60).unwrap();
        assert_eq!(
            format_timestamp_in_timezone(0, &india),
            Some("Thu Jan  1, 05:30 +05:30".into())
        );

        let eastern = chrono::FixedOffset::west_opt(4 * 60 * 60).unwrap();
        assert_eq!(
            format_timestamp_in_timezone(0, &eastern),
            Some("Wed Dec 31, 20:00 -04:00".into())
        );
    }

    #[test]
    fn reset_timestamp_formatting_handles_invalid_epochs() {
        let timezone = chrono::FixedOffset::east_opt(0).unwrap();
        assert_eq!(format_timestamp_in_timezone(i64::MAX, &timezone), None);
    }

    #[test]
    fn one_time_reset_guard_waits_above_threshold_and_when_disarmed() {
        let limits = json!({
            "rateLimits": {
                "secondary": {"usedPercent": 97, "windowDurationMins": 10080}
            },
            "rateLimitResetCredits": {"availableCount": 1}
        });
        let mut guard = OneTimeResetGuard::default();
        assert_eq!(
            one_time_reset_decision(&guard, &limits),
            OneTimeResetDecision::Wait
        );
        guard.armed = true;
        assert_eq!(
            one_time_reset_decision(&guard, &limits),
            OneTimeResetDecision::Wait
        );
    }

    #[test]
    fn one_time_reset_guard_consumes_one_available_credit_at_two_percent() {
        let limits = json!({
            "rateLimits": {
                "secondary": {"usedPercent": 98, "windowDurationMins": 10080}
            },
            "rateLimitResetCredits": {
                "availableCount": 2,
                "credits": [
                    {"id": "spent", "status": "redeemed"},
                    {"id": "credit-1", "status": "available"}
                ]
            }
        });
        let guard = OneTimeResetGuard {
            armed: true,
            ..OneTimeResetGuard::default()
        };
        assert_eq!(
            one_time_reset_decision(&guard, &limits),
            OneTimeResetDecision::Consume {
                credit_id: Some("credit-1".into())
            }
        );
    }

    #[test]
    fn one_time_reset_guard_still_asks_backend_when_credit_summary_is_empty() {
        let limits = json!({
            "rateLimits": {
                "secondary": {"usedPercent": 99, "windowDurationMins": 10080}
            },
            "rateLimitResetCredits": {"availableCount": 0}
        });
        let guard = OneTimeResetGuard {
            armed: true,
            ..OneTimeResetGuard::default()
        };
        assert_eq!(
            one_time_reset_decision(&guard, &limits),
            OneTimeResetDecision::Consume { credit_id: None }
        );
    }

    #[test]
    fn pending_one_time_reset_reuses_idempotency_even_after_limits_change() {
        let limits = json!({
            "rateLimits": {
                "secondary": {"usedPercent": 0, "windowDurationMins": 10080}
            },
            "rateLimitResetCredits": {"availableCount": 0}
        });
        let guard = OneTimeResetGuard {
            armed: true,
            idempotency_key: Some("stable-key".into()),
            outcome: Some("pending".into()),
            ..OneTimeResetGuard::default()
        };
        assert_eq!(
            one_time_reset_decision(&guard, &limits),
            OneTimeResetDecision::Consume { credit_id: None }
        );
    }

    #[test]
    fn multi_bucket_codex_limit_takes_precedence() {
        let limits = json!({
            "rateLimits": {"secondary": {"usedPercent": 99}},
            "rateLimitsByLimitId": {
                "codex": {"secondary": {"usedPercent": 10, "windowDurationMins": 10080}}
            }
        });
        assert_eq!(weekly_limit_details(&limits), Some((90, None, Some(10080))));
    }

    #[test]
    fn history_bootstrap_repairs_rollouts_and_uses_interactive_sources() {
        let params = thread_list_params(false, "updated_at");
        assert_eq!(params.get("archived"), Some(&Value::Bool(false)));
        assert_eq!(
            params.get("sortKey").and_then(Value::as_str),
            Some("updated_at")
        );
        assert!(params.get("useStateDbOnly").is_none());
        assert!(params.get("sourceKinds").is_none());
    }

    #[test]
    fn periodic_history_refresh_uses_the_fast_state_database_path() {
        assert_eq!(
            thread_refresh_params(false, "updated_at").get("useStateDbOnly"),
            Some(&Value::Bool(true))
        );
        assert_eq!(
            subagent_thread_refresh_params().get("useStateDbOnly"),
            Some(&Value::Bool(true))
        );
    }

    #[test]
    fn initial_bootstrap_defers_heavy_secondary_pages() {
        let requests = initial_bootstrap_requests(false, "updated_at", "/workspace");
        let methods = requests
            .iter()
            .map(|(_, method, _)| *method)
            .collect::<Vec<_>>();
        for deferred in [
            "plugin/list",
            "skills/list",
            "mcpServerStatus/list",
            "app/list",
            "hooks/list",
        ] {
            assert!(!methods.contains(&deferred));
        }
        assert!(methods.contains(&"thread/list"));
        assert!(methods.contains(&"model/list"));
        assert!(methods.contains(&"account/read"));
    }

    #[test]
    fn installed_extension_view_avoids_the_large_remote_catalog() {
        let params = local_plugin_list_params("/workspace");
        assert_eq!(params.get("cwds"), Some(&json!(["/workspace"])));
        assert_eq!(
            params.get("marketplaceKinds"),
            Some(&json!(["local", "workspace-directory"]))
        );
    }

    #[test]
    fn loaded_qwen_tools_override_a_missing_or_stale_plugin_catalog() {
        assert_eq!(qwen_install_state(None, true, true), (true, true));
        assert_eq!(
            qwen_install_state(Some((false, false)), true, true),
            (true, true)
        );
        assert_eq!(
            qwen_install_state(Some((true, false)), false, false),
            (true, false)
        );
        assert_eq!(qwen_install_state(None, false, false), (false, false));
    }

    #[test]
    fn task_list_badges_direct_non_gpt_models() {
        assert_eq!(
            task_model_badge(Some(&TaskRoutingState::new(MODE_GEMINI))),
            Some("Gemini")
        );
        assert_eq!(
            task_model_badge(Some(&TaskRoutingState::new(MODE_OPENROUTER))),
            Some("OpenRouter Free")
        );
        assert_eq!(
            task_model_badge(Some(&TaskRoutingState::new(MODE_MISTRAL))),
            Some("Mistral AI")
        );
        assert_eq!(
            task_model_badge(Some(&TaskRoutingState::new(MODE_MANUAL))),
            None
        );
    }

    #[test]
    fn persisted_tasks_resume_before_starting_another_turn() {
        assert_eq!(
            thread_resume_params("thread-42"),
            json!({
                "threadId": "thread-42",
                "excludeTurns": true,
                "initialTurnsPage": {
                    "limit": TRANSCRIPT_PAGE_TURNS,
                    "itemsView": "summary",
                    "sortDirection": "desc"
                }
            })
        );
    }

    #[test]
    fn transcript_scroll_target_preserves_user_position_and_prepend_anchor() {
        assert_eq!(
            transcript_scroll_target(false, false, 420.0, 2_000.0, 2_400.0, 600.0),
            420.0
        );
        assert_eq!(
            transcript_scroll_target(false, true, 420.0, 2_000.0, 2_400.0, 600.0),
            820.0
        );
        assert_eq!(
            transcript_scroll_target(true, false, 420.0, 2_000.0, 2_400.0, 600.0),
            1_800.0
        );
    }

    #[test]
    fn transcript_scroll_easing_starts_smoothly_and_finishes_exactly() {
        assert_eq!(eased_transcript_scroll_progress(Duration::ZERO), 0.0);
        let midpoint = eased_transcript_scroll_progress(TRANSCRIPT_SCROLL_ANIMATION / 2);
        assert!(midpoint > 0.5 && midpoint < 1.0);
        assert_eq!(
            eased_transcript_scroll_progress(TRANSCRIPT_SCROLL_ANIMATION),
            1.0
        );
        assert_eq!(
            eased_transcript_scroll_progress(TRANSCRIPT_SCROLL_ANIMATION * 2),
            1.0
        );
    }

    #[test]
    fn lightweight_streaming_rows_only_apply_to_live_agent_messages() {
        let live = TranscriptRowContent::Item {
            item: json!({"type": "agentMessage"}),
            streamed_text: "Writing a summary".into(),
            show_reasoning: false,
            cwd: String::new(),
        };
        assert_eq!(
            streamed_agent_message_text(&live),
            Some("Writing a summary")
        );

        let complete = TranscriptRowContent::Item {
            item: json!({"type": "agentMessage", "text": "Complete summary"}),
            streamed_text: String::new(),
            show_reasoning: false,
            cwd: String::new(),
        };
        assert_eq!(streamed_agent_message_text(&complete), None);
    }

    #[test]
    fn model_bootstrap_requests_the_visible_account_catalog() {
        let params = model_list_params();
        assert_eq!(params.get("limit").and_then(Value::as_i64), Some(100));
        assert_eq!(params.get("includeHidden"), Some(&Value::Bool(false)));
    }

    #[test]
    fn model_picker_pins_terra_as_the_native_default() {
        let models = vec![
            json!({"id": "gpt-5.5", "displayName": "GPT-5.5", "isDefault": false}),
            json!({"id": "gpt-5.6-sol", "displayName": "GPT-5.6-Sol", "isDefault": true}),
        ];
        assert_eq!(
            default_model_option_label(&models),
            "Default · GPT-5.6 Terra"
        );
        assert_eq!(resolved_model_id(&models, ""), "gpt-5.6-terra");
        assert_eq!(resolved_model_id(&models, "gpt-5.6-luna"), "gpt-5.6-luna");
        assert_eq!(model_display_name("gpt-6-astra"), Some("GPT-6 Astra"));
        assert_eq!(resolved_model_id(&models, "gpt-6-astra"), "gpt-6-astra");
        assert!(COMPOSER_MODELS.contains(&(BACKEND_QWEN, "OpenCode (Qwen Local)")));
    }

    #[test]
    fn new_task_start_preserves_concrete_model_effort_and_speed() {
        let settings = TaskRuntimeSettings {
            model: "gpt-5.6-sol".into(),
            reasoning_effort: Some("max".into()),
            service_tier: Some("priority".into()),
            sandbox_policy: json!({"type": "workspaceWrite"}),
            approval_policy: json!("on-request"),
        };
        let params = thread_start_runtime_params(&settings);
        assert_eq!(params["model"], "gpt-5.6-sol");
        assert_eq!(params["config"]["model_reasoning_effort"], "max");
        assert_eq!(params["serviceTier"], "priority");
    }

    #[test]
    fn effort_picker_includes_the_server_max_mode() {
        assert_eq!(
            REASONING_EFFORT_OPTIONS,
            [
                ("low", "Light"),
                ("medium", "Medium"),
                ("high", "High"),
                ("xhigh", "Extra High"),
                ("max", "Max"),
                ("ultra", "Ultra"),
            ]
        );
    }

    #[test]
    fn effort_picker_follows_the_selected_models_server_capabilities() {
        let models = vec![json!({
            "id": "gpt-test",
            "isDefault": true,
            "supportedReasoningEfforts": [
                {"reasoningEffort": "low"},
                {"reasoningEffort": "xhigh"},
                {"reasoningEffort": "max"}
            ]
        })];
        assert_eq!(
            reasoning_effort_options_for_model(&models, ""),
            [("low", "Light"), ("xhigh", "Extra High"), ("max", "Max")]
        );
    }

    #[test]
    fn astra_effort_picker_uses_the_server_catalog_without_non_thinking() {
        let models = vec![json!({
            "id": "gpt-6-astra",
            "supportedReasoningEfforts": [
                {"reasoningEffort": "low"},
                {"reasoningEffort": "medium"},
                {"reasoningEffort": "high"},
                {"reasoningEffort": "xhigh"},
                {"reasoningEffort": "max"},
                {"reasoningEffort": "none"}
            ]
        })];
        assert_eq!(
            reasoning_effort_options_for_model(&models, "gpt-6-astra"),
            [
                ("low", "Light"),
                ("medium", "Medium"),
                ("high", "High"),
                ("xhigh", "Extra High"),
                ("max", "Max")
            ]
        );
    }

    #[test]
    fn delegate_models_have_backend_specific_effort_choices() {
        assert_eq!(
            reasoning_effort_options_for_model(&[], BACKEND_QWEN),
            [
                ("xhigh", "Extra High"),
                ("high", "High"),
                ("medium", "Medium"),
                ("low", "Low"),
                ("none", "Non-thinking"),
            ]
        );
        assert_eq!(
            reasoning_effort_options_for_model(&[], BACKEND_GEMINI),
            [
                ("minimal", "Minimal"),
                ("low", "Low"),
                ("medium", "Medium"),
                ("high", "High"),
            ]
        );
        assert_eq!(
            reasoning_effort_options_for_model(&[], BACKEND_OPENROUTER),
            [
                ("minimal", "Minimal"),
                ("low", "Low"),
                ("medium", "Medium"),
                ("high", "High"),
            ]
        );
        assert_eq!(
            reasoning_effort_options_for_model(&[], BACKEND_MISTRAL),
            [("high", "High"), ("none", "Non-thinking")]
        );
        assert_eq!(
            resolved_model_id(&[], BACKEND_QWEN),
            QWEN_ORCHESTRATOR_MODEL
        );
        assert_eq!(normalized_effort_for_model(BACKEND_QWEN, "xhigh"), "xhigh");
        assert_eq!(
            normalized_effort_for_model(BACKEND_QWEN, "unsupported"),
            "xhigh"
        );
        assert_eq!(routing_mode_for_model(BACKEND_GEMINI), MODE_GEMINI);
        assert_eq!(routing_mode_for_model(BACKEND_OPENROUTER), MODE_OPENROUTER);
        assert_eq!(routing_mode_for_model(BACKEND_MISTRAL), MODE_MISTRAL);
        assert_eq!(buddy_author("openrouter"), "OpenRouter Free");
        assert_eq!(buddy_author("mistral"), "Mistral AI");
        assert_eq!(
            normalized_effort_for_model(BACKEND_MISTRAL, "medium"),
            "high"
        );
        assert_eq!(normalized_effort_for_model(BACKEND_MISTRAL, "high"), "high");
    }

    #[test]
    fn delegate_models_have_bounded_access_choices() {
        assert_eq!(
            sandbox_options_for_model(BACKEND_QWEN),
            [
                ("read-only", "Read-only"),
                ("workspace-write", "Workspace write"),
                ("danger-full-access", "Full access"),
            ]
        );
        assert_eq!(
            sandbox_options_for_model("gpt-5.6-sol"),
            GPT_SANDBOX_OPTIONS
        );
    }

    #[test]
    fn buddy_progress_identifies_provider_and_current_work() {
        let plan = buddy_progress_plan("gemini", "read", "Inspecting src/ui/mod.rs");
        assert_eq!(plan.len(), 5);
        assert_eq!(plan[0]["status"], "completed");
        assert_eq!(plan[2]["status"], "inProgress");
        assert_eq!(plan[2]["step"], "Gemini: Inspecting src/ui/mod.rs");
    }

    #[test]
    fn qwen_activity_and_context_usage_are_measured_and_bounded() {
        let mut turn = Turn {
            id: "buddy-turn".into(),
            items: vec![buddy_activity_item("buddy-turn", "Qwen", "high")],
            ..Turn::default()
        };
        let first = crate::host::BuddyProgress {
            phase: "read_completed".into(),
            detail: "Inspecting src/ui/mod.rs".into(),
            model_calls: 1,
            prompt_tokens: 120,
            output_tokens: 30,
            total_tokens: 150,
            context_used_tokens: 150,
            context_window_tokens: 65_536,
            tokens_per_second: Some(42.5),
        };
        assert_eq!(update_buddy_activity(&mut turn, "Qwen", &first), 0);
        assert_eq!(turn.items[0]["reasoningKind"], "thinking");
        assert!(extract_reasoning(&turn.items[0]).contains("Read completed — Inspecting"));
        assert!(extract_reasoning(&turn.items[0]).contains("42.5 tok/s"));

        let first_usage = buddy_usage_from_progress(None, 0, "Qwen", &first).unwrap();
        assert_eq!(first_usage.pointer("/last/totalTokens"), Some(&json!(150)));
        assert_eq!(first_usage.pointer("/total/totalTokens"), Some(&json!(150)));
        assert_eq!(first_usage.get("providerTotalTokens"), Some(&json!(150)));
        assert_eq!(first_usage.get("tokensPerSecond"), Some(&json!(42.5)));

        let second = crate::host::BuddyProgress {
            phase: "complete".into(),
            detail: "Response ready".into(),
            prompt_tokens: 240,
            output_tokens: 60,
            total_tokens: 300,
            context_used_tokens: 175,
            context_window_tokens: 65_536,
            ..crate::host::BuddyProgress::default()
        };
        assert_eq!(update_buddy_activity(&mut turn, "Qwen", &second), 150);
        let second_usage =
            buddy_usage_from_progress(Some(&first_usage), 150, "Qwen", &second).unwrap();
        assert_eq!(second_usage.pointer("/last/totalTokens"), Some(&json!(175)));
        assert_eq!(
            second_usage.pointer("/total/totalTokens"),
            Some(&json!(300))
        );
        assert_eq!(second_usage.get("providerTotalTokens"), Some(&json!(300)));
    }

    #[test]
    fn saved_buddy_chat_restores_as_a_selectable_thread() {
        let turns = vec![Turn {
            id: "buddy-turn".into(),
            items: vec![json!({
                "type": "userMessage",
                "content": [{"type": "text", "text": "Continue this Qwen chat"}],
                "cwd": "/saved-workspace"
            })],
            ..Turn::default()
        }];
        let thread = restored_buddy_thread("buddy-thread", turns, "/workspace".into());
        assert_eq!(thread.id, "buddy-thread");
        assert_eq!(thread.name.as_deref(), Some("Continue this Qwen chat"));
        assert_eq!(thread.cwd, "/saved-workspace");
        assert_eq!(thread.turns.len(), 1);
        assert!(thread.native_buddy_only);
    }

    #[test]
    fn buddy_turn_is_durable_before_and_after_completion() {
        let mut stored = StoredState::default();
        upsert_buddy_turn(
            &mut stored,
            "buddy-thread",
            Turn {
                id: "turn".into(),
                status: json!({"type": "inProgress"}),
                ..Turn::default()
            },
        );
        upsert_buddy_turn(
            &mut stored,
            "buddy-thread",
            Turn {
                id: "turn".into(),
                status: json!({"type": "completed"}),
                ..Turn::default()
            },
        );

        let turns = &stored.buddy_turns["buddy-thread"];
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].status["type"], "completed");
    }

    #[test]
    fn buddy_context_preserves_local_model_authors_and_is_bounded() {
        let thread = ThreadSummary {
            turns: vec![Turn {
                items: vec![
                    json!({"type": "userMessage", "text": "hello"}),
                    json!({"type": "agentMessage", "author": "Gemini", "text": "hi"}),
                ],
                ..Turn::default()
            }],
            ..ThreadSummary::default()
        };
        assert_eq!(buddy_thread_context(&thread), "User: hello\n\nGemini: hi");

        let large = ThreadSummary {
            turns: vec![Turn {
                items: vec![json!({
                    "type": "agentMessage",
                    "author": "Qwen",
                    "text": "x".repeat(30_000)
                })],
                ..Turn::default()
            }],
            ..ThreadSummary::default()
        };
        assert!(buddy_thread_context(&large).len() <= 24_040);
    }

    #[test]
    fn failed_turn_detection_reads_terminal_status() {
        assert!(turn_completion_failed(&json!({
            "turn": {"status": "failed", "error": {"message": "boom"}}
        })));
        assert!(!turn_completion_failed(&json!({
            "turn": {"status": "completed", "error": null}
        })));
    }

    #[test]
    fn speed_picker_uses_the_server_priority_tier() {
        let models = vec![
            json!({
                "id": "gpt-fast",
                "isDefault": true,
                "serviceTiers": [{
                    "id": "priority",
                    "name": "Fast",
                    "description": "1.5x speed, increased usage"
                }]
            }),
            json!({"id": "gpt-standard", "serviceTiers": []}),
        ];
        assert_eq!(
            service_tier_options_for_model(&models, "gpt-fast"),
            [("standard", "Standard"), ("priority", "Fast")]
        );
        assert_eq!(
            service_tier_options_for_model(&models, "gpt-standard"),
            [("standard", "Standard")]
        );
        assert_eq!(service_tier_value("standard"), Value::Null);
        assert_eq!(service_tier_value("priority"), json!("priority"));
    }

    #[test]
    fn resumed_tasks_keep_their_authoritative_runtime_settings() {
        let (thread_id, settings) = task_runtime_settings_from_server(&json!({
            "thread": {"id": "thread-7"},
            "model": "gpt-5.6-sol",
            "reasoningEffort": "ultra",
            "serviceTier": "priority",
            "sandbox": {"type": "readOnly", "networkAccess": false},
            "approvalPolicy": "never"
        }))
        .expect("runtime settings");
        assert_eq!(thread_id, "thread-7");
        assert_eq!(settings.model, "gpt-5.6-sol");
        assert_eq!(settings.reasoning_effort.as_deref(), Some("ultra"));
        assert_eq!(settings.service_tier.as_deref(), Some("priority"));
        assert_eq!(sandbox_control_id(&settings.sandbox_policy), "read-only");
        assert_eq!(approval_control_id(&settings.approval_policy), "never");
    }

    #[test]
    fn standard_server_tier_never_becomes_a_third_picker_option() {
        let (_, settings) = task_runtime_settings_from_server(&json!({
            "threadId": "thread-standard",
            "threadSettings": {
                "model": "gpt-5.6-sol",
                "effort": "max",
                "serviceTier": "default",
                "sandboxPolicy": {"type": "workspaceWrite"},
                "approvalPolicy": "on-request"
            }
        }))
        .expect("runtime settings");
        assert_eq!(settings.service_tier, None);
        assert!(task_settings_update_params("thread-standard", &settings)["serviceTier"].is_null());
    }

    #[test]
    fn task_settings_updates_preserve_structured_server_policies() {
        let settings = TaskRuntimeSettings {
            model: "gpt-5.6-luna".into(),
            reasoning_effort: Some("max".into()),
            service_tier: None,
            sandbox_policy: json!({
                "type": "workspaceWrite",
                "networkAccess": false,
                "writableRoots": ["/workspace"]
            }),
            approval_policy: json!({"granular": {
                "mcp_elicitations": true,
                "rules": true,
                "sandbox_approval": true
            }}),
        };
        assert_eq!(approval_control_id(&settings.approval_policy), "granular");
        assert_eq!(
            sandbox_policy_for_selection("workspace-write", &settings.sandbox_policy),
            settings.sandbox_policy
        );
        let params = task_settings_update_params("thread-9", &settings);
        assert_eq!(params["threadId"], "thread-9");
        assert_eq!(params["model"], "gpt-5.6-luna");
        assert_eq!(params["effort"], "max");
        assert!(params["serviceTier"].is_null());
        assert_eq!(params["sandboxPolicy"]["writableRoots"][0], "/workspace");
        assert!(params["approvalPolicy"].is_object());
    }

    #[test]
    fn startup_page_mapping_includes_qwen_buddy() {
        assert_eq!(
            workspace_page_from_name(Some("qwen-buddy")),
            WorkspacePage::QwenBuddy
        );
    }

    #[test]
    fn startup_page_mapping_includes_chatgpt() {
        assert_eq!(
            workspace_page_from_name(Some("chatgpt")),
            WorkspacePage::Chatgpt
        );
        assert_eq!(crate::chatgpt::CHATGPT_URL, "https://chatgpt.com/");
    }

    #[test]
    fn startup_page_mapping_includes_sidebar_workspace_pages() {
        assert_eq!(
            workspace_page_from_name(Some("projects")),
            WorkspacePage::Projects
        );
        assert_eq!(
            workspace_page_from_name(Some("sites")),
            WorkspacePage::Sites
        );
        assert_eq!(
            workspace_page_from_name(Some("automations")),
            WorkspacePage::Automations
        );
    }

    #[test]
    fn plugin_heading_identifies_the_default_installed_filter() {
        assert_eq!(plugin_section_title(true, 13), "Installed Plugins (13)");
        assert_eq!(plugin_section_title(false, 42), "Plugins (42)");
    }

    #[test]
    fn task_context_menu_tracks_pin_state() {
        assert_eq!(thread_context_mouse_button(), 3);
        assert_eq!(pin_context_action_label(false), "Pin task");
        assert_eq!(pin_context_action_label(true), "Unpin task");
    }

    #[test]
    fn bulk_task_selection_filters_visible_rows_and_labels_delete_scope() {
        let mut state = AppState::default();
        state.set_threads(vec![
            ThreadSummary {
                id: "alpha".into(),
                name: Some("Alpha task".into()),
                cwd: "/workspace/one".into(),
                updated_at: 2,
                ..ThreadSummary::default()
            },
            ThreadSummary {
                id: "beta".into(),
                name: Some("Beta task".into()),
                cwd: "/workspace/two".into(),
                updated_at: 1,
                ..ThreadSummary::default()
            },
        ]);

        assert_eq!(
            visible_thread_ids(&state, &["beta".into()], ""),
            vec!["beta", "alpha"]
        );
        assert_eq!(
            visible_thread_ids(&state, &[], "workspace/one"),
            vec!["alpha"]
        );
        assert_eq!(bulk_delete_dialog_title(1), "Delete 1 selected task?");
        assert_eq!(bulk_delete_dialog_title(3), "Delete 3 selected tasks?");
        assert!(is_bulk_task_mutation(&PendingKind::BulkArchiveThread));
        assert!(is_bulk_task_mutation(&PendingKind::BulkUnarchiveThread));
        assert!(is_bulk_task_mutation(&PendingKind::BulkDeleteThread));
        assert!(!is_bulk_task_mutation(&PendingKind::ArchiveThread));
    }

    #[test]
    fn stale_steer_error_is_the_only_error_retried_as_a_new_turn() {
        assert!(is_no_active_turn_to_steer(
            -32600,
            "no active turn to steer"
        ));
        assert!(is_no_active_turn_to_steer(
            -32600,
            "No active turn exists to STEER for this thread"
        ));
        assert!(!is_no_active_turn_to_steer(
            -32601,
            "no active turn to steer"
        ));
        assert!(!is_no_active_turn_to_steer(-32600, "thread not found"));
    }

    #[test]
    fn mcp_reload_uses_the_current_app_server_method() {
        assert_eq!(MCP_RELOAD_METHOD, "config/mcpServer/reload");
        assert_ne!(MCP_RELOAD_METHOD, "mcpServer/refresh");
    }

    #[test]
    fn server_thread_search_requests_full_text_results() {
        let params = thread_search_params("needle", false, "updated_at");
        assert_eq!(params["searchTerm"], "needle");
        assert_eq!(params["sortKey"], "updated_at");
    }

    #[test]
    fn task_titles_are_bounded_before_gtk_and_pango_render_them() {
        assert_eq!(compact_ui_text("  one\n two\tthree  ", 40), "one two three");
        assert_eq!(compact_ui_text("abcdef", 4), "abcd…");
        assert_eq!(compact_ui_text("  \n\t", 4), "New task");
        assert!(compact_ui_text(&"♞".repeat(10_000), 180).chars().count() <= 181);
    }

    #[test]
    fn file_change_cards_render_diffs_instead_of_raw_protocol_json() {
        let item = json!({
            "changes": [
                {"path": "src/ui/mod.rs", "diff": "@@ -1 +1 @@\n-old\n+new"},
                {"path": "data/style.css", "diff": "@@ -2 +2 @@\n-a\n+b"}
            ]
        });
        let (summary, diff) = file_change_display(&item);
        assert_eq!(summary, "Edited src/ui/mod.rs and 1 more");
        assert!(diff.contains("# src/ui/mod.rs\n@@ -1 +1 @@"));
        assert!(diff.contains("# data/style.css\n@@ -2 +2 @@"));
        assert!(!diff.contains("\"diff\""));
        assert_eq!(
            file_change_paths(&item, "/workspace"),
            vec![
                PathBuf::from("/workspace/data/style.css"),
                PathBuf::from("/workspace/src/ui/mod.rs")
            ]
        );
    }

    #[test]
    fn every_canonical_structured_activity_has_a_native_presentation() {
        let cases = [
            (
                json!({
                    "type": "hookPrompt",
                    "fragments": [{"hookRunId": "one", "text": "Context"}]
                }),
                "Hook supplied context",
            ),
            (
                json!({
                    "type": "collabAgentToolCall",
                    "tool": "spawnAgent",
                    "receiverThreadIds": ["agent"],
                    "agentsStates": {"agent": {"status": "running"}},
                    "status": "completed",
                    "prompt": "Inspect the renderer"
                }),
                "Spawned agent · completed",
            ),
            (
                json!({
                    "type": "subAgentActivity",
                    "kind": "interrupted",
                    "agentPath": "Builder"
                }),
                "Subagent interrupted",
            ),
            (json!({"type": "sleep", "durationMs": 61_000}), "Waited"),
            (
                json!({"type": "enteredReviewMode", "review": "Review changes"}),
                "Entered review mode",
            ),
            (
                json!({"type": "exitedReviewMode", "review": "Review changes"}),
                "Exited review mode",
            ),
            (
                json!({"type": "contextCompaction"}),
                "Context automatically compacted",
            ),
        ];

        for (item, expected_title) in cases {
            let presentation = canonical_activity_presentation(&item).expect("presentation");
            assert_eq!(presentation.title, expected_title);
        }
        assert_eq!(
            canonical_activity_presentation(&json!({"type": "sleep", "durationMs": 61_000}))
                .unwrap()
                .summary,
            "1 min 1 s"
        );
        assert!(canonical_activity_presentation(&json!({"type": "unknown"})).is_none());
    }

    #[test]
    fn structured_activity_details_are_bounded_before_gtk_rendering() {
        let presentation = canonical_activity_presentation(&json!({
            "type": "collabAgentToolCall",
            "tool": "sendInput",
            "prompt": "x".repeat(20_000),
            "status": "inProgress"
        }))
        .unwrap();
        assert!(presentation.detail.chars().count() < 12_100);
        assert!(presentation.detail.contains("truncated"));
    }

    #[test]
    fn live_progress_uses_canonical_plan_and_diff_fields() {
        let plan = vec![
            json!({"step": "Inspect", "status": "completed"}),
            json!({"step": "Build", "status": "inProgress"}),
            json!({"step": "Verify", "status": "pending"}),
        ];
        assert_eq!(plan_position(&plan), (2, 3, Some("Build".to_owned())));
        let tooltip_steps = plan_tooltip_steps(&plan);
        assert_eq!(
            tooltip_steps,
            vec![
                PlanTooltipStep {
                    label: "Inspect".to_owned(),
                    state: PlanStepVisualState::Completed,
                },
                PlanTooltipStep {
                    label: "Build".to_owned(),
                    state: PlanStepVisualState::Current,
                },
                PlanTooltipStep {
                    label: "Verify".to_owned(),
                    state: PlanStepVisualState::Pending,
                },
            ]
        );
        assert_eq!(
            plan_tooltip_accessible_text(&tooltip_steps),
            "Completed: Inspect\nCurrent: Build\nPending: Verify"
        );
        let implicit_current = plan_tooltip_steps(&[
            json!({"step": "First", "status": "pending"}),
            json!({"step": "Second", "status": "pending"}),
        ]);
        assert_eq!(implicit_current[0].state, PlanStepVisualState::Current);
        let all_done = plan_tooltip_steps(&[
            json!({"step": "First", "status": "completed"}),
            json!({"step": "Second", "status": "completed"}),
        ]);
        assert!(
            all_done
                .iter()
                .all(|step| step.state == PlanStepVisualState::Completed)
        );
        let stats = diff_stats(
            "diff --git a/src/a.rs b/src/a.rs\n--- a/src/a.rs\n+++ b/src/a.rs\n-old\n+new\ndiff --git a/data/b.css b/data/b.css\n--- a/data/b.css\n+++ b/data/b.css\n+added",
        );
        assert_eq!(
            stats,
            DiffStats {
                files: 2,
                additions: 2,
                deletions: 1
            }
        );
    }

    #[test]
    fn image_items_resolve_to_clickable_local_sources() {
        let viewed = json!({"type": "imageView", "path": "shots/result.png"});
        assert_eq!(
            item_image_sources(&viewed, "/workspace"),
            vec!["/workspace/shots/result.png"]
        );
        let attached = json!({
            "type": "userMessage",
            "content": [
                {"type": "text", "text": "Review this"},
                {"type": "localImage", "path": "/tmp/reference.png"}
            ]
        });
        assert_eq!(
            item_image_sources(&attached, "/workspace"),
            vec!["/tmp/reference.png"]
        );
    }

    #[test]
    fn giant_rollouts_wait_for_explicit_activity_loading() {
        assert!(should_auto_hydrate_rollout_size(None));
        assert!(should_auto_hydrate_rollout_size(Some(
            MAX_AUTO_DETAIL_ROLLOUT_BYTES
        )));
        assert!(!should_auto_hydrate_rollout_size(Some(
            MAX_AUTO_DETAIL_ROLLOUT_BYTES + 1
        )));
    }

    #[test]
    fn giant_rollouts_still_receive_canonical_chat_refreshes() {
        let fixture = tempfile::NamedTempFile::new().unwrap();
        fixture
            .as_file()
            .set_len(MAX_AUTO_DETAIL_ROLLOUT_BYTES + 1)
            .unwrap();

        assert!(local_rollout_fingerprint(fixture.path()).is_some());
    }

    #[test]
    fn composer_enter_sends_and_shift_enter_keeps_a_newline() {
        assert!(composer_return_sends(
            gdk::Key::Return,
            gdk::ModifierType::empty()
        ));
        assert!(composer_return_sends(
            gdk::Key::KP_Enter,
            gdk::ModifierType::CONTROL_MASK
        ));
        assert!(!composer_return_sends(
            gdk::Key::Return,
            gdk::ModifierType::SHIFT_MASK
        ));
    }

    #[test]
    fn image_only_composer_input_omits_an_empty_text_part() {
        let inputs = make_inputs("", &["/tmp/pasted.png".into()]);
        assert_eq!(
            inputs,
            vec![json!({"type": "localImage", "path": "/tmp/pasted.png"})]
        );
    }

    #[test]
    fn completed_agent_message_reuses_the_streaming_row() {
        let streaming = json!({"id": "agent", "type": "agentMessage", "text": ""});
        let completed = json!({"id": "agent", "type": "agentMessage", "text": "Stable answer"});
        assert_eq!(
            transcript_item_fingerprint(&streaming, "Stable answer", true, "/workspace"),
            transcript_item_fingerprint(&completed, "", true, "/workspace")
        );
    }

    #[test]
    fn periodic_smoke_fixture_refresh_preserves_the_open_real_task() {
        let mut state = AppState::default();
        state.upsert_thread(ThreadSummary {
            id: "real-task".into(),
            ..ThreadSummary::default()
        });
        state.activate_thread("real-task".into());

        install_smoke_fixtures(&mut state);

        assert_eq!(state.active_thread_id.as_deref(), Some("real-task"));
        assert!(state.threads.contains_key(SMOKE_FIXTURE_THREAD_ID));
    }

    #[test]
    fn empty_top_level_user_text_falls_back_to_rich_content() {
        let item = json!({
            "type": "userMessage",
            "text": "",
            "content": [{"type": "text", "text": "Visible prompt"}]
        });
        assert_eq!(user_message_text(&item), "Visible prompt");
    }

    #[test]
    fn spawned_agent_summary_deduplicates_protocol_and_child_threads() {
        let parent = ThreadSummary {
            id: "root".into(),
            turns: vec![crate::model::Turn {
                id: "turn".into(),
                items: vec![json!({
                    "type": "collabAgentToolCall",
                    "tool": "spawnAgent",
                    "receiverThreadIds": ["child"],
                    "agentsStates": {"child": {"status": "running"}}
                })],
                ..crate::model::Turn::default()
            }],
            ..ThreadSummary::default()
        };
        let child = ThreadSummary {
            id: "child".into(),
            parent_thread_id: Some("root".into()),
            agent_nickname: Some("Builder".into()),
            status: json!({"type": "active", "activeFlags": []}),
            ..ThreadSummary::default()
        };
        let mut state = AppState::default();
        state.upsert_thread(parent.clone());
        state.upsert_thread(child);
        let summary = thread_agent_summary(&parent, &state);
        assert_eq!(summary.total, 1);
        assert_eq!(summary.running, 1);
        assert!(summary.tooltip.contains("Builder — Running"));
    }

    #[test]
    fn subagent_history_query_includes_every_agent_source_kind() {
        let params = subagent_thread_list_params();
        let sources = params.get("sourceKinds").unwrap().as_array().unwrap();
        assert!(sources.contains(&json!("subAgent")));
        assert!(sources.contains(&json!("subAgentThreadSpawn")));
        assert_eq!(params.get("limit"), Some(&json!(250)));
        let exact = task_subagent_params("root");
        assert_eq!(exact.get("ancestorThreadId"), Some(&json!("root")));
        assert_eq!(exact.get("limit"), Some(&json!(1000)));
        assert_eq!(exact.get("useStateDbOnly"), Some(&Value::Bool(true)));
    }

    #[test]
    fn goal_selector_maps_every_server_lifecycle_state() {
        assert_eq!(goal_status_label("active"), "Active");
        assert_eq!(goal_status_label("paused"), "Stopped");
        assert_eq!(goal_status_label("usageLimited"), "Usage limited");
        assert_eq!(goal_status_label("budgetLimited"), "Budget limited");
        assert!(goal_status_reason("active").is_none());
        assert!(
            goal_status_reason("usageLimited")
                .unwrap()
                .contains("usage")
        );
        assert!(
            goal_status_reason("budgetLimited")
                .unwrap()
                .contains("budget")
        );
    }

    #[test]
    fn running_task_detection_uses_the_server_thread_status() {
        let running = ThreadSummary {
            status: json!({"type": "active", "activeFlags": []}),
            ..ThreadSummary::default()
        };
        let idle = ThreadSummary {
            status: json!({"type": "idle"}),
            ..ThreadSummary::default()
        };
        assert!(thread_is_running(&running));
        assert!(!thread_is_running(&idle));

        let mut state = AppState {
            active_thread_id: Some("active".into()),
            active_turn_id: Some("turn".into()),
            ..AppState::default()
        };
        let active = ThreadSummary {
            id: "active".into(),
            status: json!({"type": "notLoaded"}),
            ..ThreadSummary::default()
        };
        assert!(thread_is_running_in_state("active", &active, &state));
        state.active_turn_id = None;
        state.turn_progress.insert(
            "active".into(),
            TurnProgress {
                turn_id: "turn".into(),
                ..TurnProgress::default()
            },
        );
        assert!(thread_is_running_in_state("active", &active, &state));
        state.turn_progress.clear();
        state.external_running_threads.insert("active".into());
        assert!(thread_is_running_in_state("active", &active, &state));
    }

    #[test]
    fn account_switch_ignores_running_tasks_owned_by_another_profile() {
        let foreign_thread = ThreadSummary {
            id: "foreign-active".into(),
            status: json!({"type": "active"}),
            ..ThreadSummary::default()
        };
        let mut stored = StoredState::default();
        stored.record_shared_tasks("other-profile", [foreign_thread.clone()]);
        let mut state = AppState::default();
        state
            .threads
            .insert(foreign_thread.id.clone(), foreign_thread);

        assert!(!has_current_profile_direct_running_work(&state, &stored));

        state.threads.insert(
            "current-active".into(),
            ThreadSummary {
                id: "current-active".into(),
                status: json!({"type": "active"}),
                ..ThreadSummary::default()
            },
        );
        assert!(has_current_profile_direct_running_work(&state, &stored));
    }

    #[test]
    fn remote_transport_handoff_waits_for_active_or_pending_turn_work() {
        let mut state = AppState::default();
        assert!(!remote_transition_is_busy(&state));

        state.active_turn_id = Some("turn".into());
        assert!(remote_transition_is_busy(&state));
        state.active_turn_id = None;

        state.pending.insert(
            RequestId::number(78),
            PendingKind::OpenThread("thread".into()),
        );
        assert!(remote_transition_is_busy(&state));
        state.pending.clear();
        assert!(!remote_transition_is_busy(&state));
    }

    #[test]
    fn failed_turns_explain_why_a_goal_stopped() {
        let usage = json!({
            "turn": {
                "status": "failed",
                "error": {"message": "limit", "codexErrorInfo": "usageLimitExceeded"}
            }
        });
        let disconnected = json!({
            "turn": {
                "status": "failed",
                "error": {
                    "message": "stream ended",
                    "codexErrorInfo": {"responseStreamDisconnected": {}}
                }
            }
        });
        assert_eq!(turn_goal_stop_reason(&usage).unwrap().0, "usageLimited");
        let (status, reason) = turn_goal_stop_reason(&disconnected).unwrap();
        assert_eq!(status, "blocked");
        assert!(reason.contains("connection"));
    }

    #[test]
    fn a_task_resumes_only_once_per_transport_connection() {
        let mut resumed = HashSet::new();
        assert!(task_needs_resume(&resumed, "thread"));
        resumed.insert("thread".into());
        assert!(!task_needs_resume(&resumed, "thread"));
        resumed.clear();
        assert!(task_needs_resume(&resumed, "thread"));
    }

    #[test]
    fn native_qwen_tasks_never_request_a_local_rollout_resume() {
        let mut resumed = HashSet::new();
        assert!(!task_needs_local_rollout_resume(
            &resumed,
            "qwen-thread",
            true
        ));
        assert!(task_needs_local_rollout_resume(
            &resumed,
            "codex-thread",
            false
        ));

        resumed.insert("codex-thread".into());
        assert!(!task_needs_local_rollout_resume(
            &resumed,
            "codex-thread",
            false
        ));
    }

    #[test]
    fn only_inactive_idle_threads_are_released() {
        let mut state = AppState::default();
        for (id, status) in [
            ("open", json!({"type": "idle"})),
            ("idle", json!({"type": "idle"})),
            ("running", json!({"type": "active"})),
        ] {
            state.upsert_thread(ThreadSummary {
                id: id.into(),
                status,
                ..ThreadSummary::default()
            });
            state.resumed_threads.insert(id.into());
        }
        state.activate_thread("open".into());

        state.upsert_thread(ThreadSummary {
            id: "buddy-only".into(),
            native_buddy_only: true,
            status: json!({"type": "idle"}),
            ..ThreadSummary::default()
        });
        state.resumed_threads.insert("buddy-only".into());

        assert_eq!(idle_resumed_thread_ids(&state), vec!["idle".to_owned()]);

        state.pending.insert(
            RequestId::number(77),
            PendingKind::UpdateThreadSettings("idle".into()),
        );
        assert!(idle_resumed_thread_ids(&state).is_empty());
    }

    #[test]
    fn remote_recovery_detects_stuck_relay_and_daemon_pressure() {
        assert!(remote_recovery_reason(&json!({"status": "connected"}), true).is_none());
        assert!(remote_recovery_reason(&json!({"status": "connecting"}), true).is_some());
        assert!(remote_recovery_reason(&json!({"status": "disabled"}), false).is_none());
        assert!(remote_recovery_reason(&json!({"status": "disabled"}), true).is_some());
        let reason = remote_recovery_reason(
            &json!({
                "status": "connected",
                "localDaemon": {"overloaded": true, "footprintMiB": 1900}
            }),
            true,
        )
        .unwrap();
        assert!(reason.contains("1900 MiB"));
    }

    #[test]
    fn macro_tool_items_feed_prompt_free_ab_telemetry() {
        let mut observation = MacroTurnObservation {
            started: Instant::now(),
            item_ids: HashSet::new(),
            tool_calls: 0,
            macro_used: false,
            peak_memory_bytes: None,
            workload_class: None,
            context_bytes_avoided: 0,
            cache_hits: 0,
            command_calls: 0,
            file_change_calls: 0,
            external_calls: 0,
        };
        observe_macro_item(
            &mut observation,
            &json!({
                "id": "tool-1",
                "type": "mcpToolCall",
                "tool": "mcp__codex_native_macro__codex_native_macro_run",
                "result": {
                    "structuredContent": {
                        "peakMemoryBytes": 268_435_456_u64,
                        "workloadClass": "read",
                        "contextBytesAvoided": 16_384_u64,
                        "cacheHits": 2
                    }
                }
            }),
        );
        observe_macro_item(
            &mut observation,
            &json!({
                "id": "tool-1",
                "type": "mcpToolCall",
                "tool": "mcp__codex_native_macro__codex_native_macro_run"
            }),
        );
        assert!(observation.macro_used);
        assert_eq!(observation.tool_calls, 1);
        assert_eq!(observation.peak_memory_bytes, Some(268_435_456));
        assert_eq!(observation.workload_class.as_deref(), Some("read"));
        assert_eq!(observation.context_bytes_avoided, 16_384);
        assert_eq!(observation.cache_hits, 2);
    }

    #[test]
    fn macro_ab_summary_compares_only_matching_groups() {
        let samples = vec![
            MacroExperimentSample {
                recorded_at: 1,
                group: "macro".into(),
                turn_succeeded: true,
                elapsed_ms: 100,
                total_tokens: Some(1_000),
                input_tokens: Some(700),
                cached_input_tokens: Some(200),
                output_tokens: Some(100),
                reasoning_output_tokens: None,
                tool_calls: 1,
                peak_memory_bytes: Some(128 * 1024 * 1024),
                workload_class: "read".into(),
                context_bytes_avoided: 4_096,
                cache_hits: 1,
            },
            MacroExperimentSample {
                recorded_at: 2,
                group: "baseline".into(),
                turn_succeeded: false,
                elapsed_ms: 300,
                total_tokens: Some(3_000),
                input_tokens: Some(2_000),
                cached_input_tokens: Some(500),
                output_tokens: Some(500),
                reasoning_output_tokens: None,
                tool_calls: 3,
                peak_memory_bytes: None,
                workload_class: "read".into(),
                context_bytes_avoided: 0,
                cache_hits: 0,
            },
        ];
        let macro_summary = macro_sample_summary(&samples, "macro");
        let baseline_summary = macro_sample_summary(&samples, "baseline");
        assert!(macro_summary.contains("100% completed"));
        assert!(macro_summary.contains("1000 avg tokens"));
        assert!(macro_summary.contains("input 700/cached 200/output 100"));
        assert!(macro_summary.contains("1024 context tokens avoided"));
        assert!(macro_summary.contains("1 cache hits"));
        assert!(macro_summary.contains("128 MiB peak"));
        assert!(baseline_summary.contains("0% completed"));
        assert!(baseline_summary.contains("3000 avg tokens"));
        assert!(!macro_matched_summary(&samples, "macro").contains("no matched"));
    }

    #[test]
    fn macro_token_breakdown_reads_app_server_categories() {
        let usage = turn_token_breakdown(&json!({
            "tokenUsage": {
                "last": {
                    "totalTokens": 1000,
                    "inputTokens": 700,
                    "cachedInputTokens": 200,
                    "outputTokens": 80,
                    "reasoningOutputTokens": 20
                }
            }
        }));
        assert_eq!(usage.total, Some(1_000));
        assert_eq!(usage.input, Some(700));
        assert_eq!(usage.cached_input, Some(200));
        assert_eq!(usage.output, Some(80));
        assert_eq!(usage.reasoning_output, Some(20));
    }

    #[test]
    fn token_savings_receipt_is_the_last_local_row_for_a_completed_turn() {
        let mut state = AppState::default();
        install_smoke_fixtures(&mut state);
        state.turn_progress.clear();
        let mut thread = state.threads[SMOKE_FIXTURE_THREAD_ID].clone();
        thread.turns[0].status = json!("completed");

        let mut routing_state = TaskRoutingState::new(MODE_QWEN_ASSIST);
        let route = routing::qwen_assist_route("gpt-5.6-sol", "max", None);
        routing_state.record_turn("codex-native-smoke-turn", route.clone());
        assert!(routing_state.record_token_savings(
            "codex-native-smoke-turn",
            routing::estimate_token_savings(&route, Some(10_000), "test telemetry"),
        ));

        let rows = transcript_row_specs(&thread, &state, true, Some(&routing_state));
        assert!(matches!(
            rows.last().map(|row| &row.content),
            Some(TranscriptRowContent::TokenSavings(_))
        ));
    }

    #[test]
    fn live_progress_stays_out_of_the_scrolling_transcript() {
        let thread = ThreadSummary {
            id: "task".into(),
            ..ThreadSummary::default()
        };
        let mut state = AppState::default();
        state.turn_progress.insert(
            thread.id.clone(),
            TurnProgress {
                turn_id: "turn".into(),
                ..TurnProgress::default()
            },
        );

        assert!(transcript_row_specs(&thread, &state, true, None).is_empty());
    }

    #[test]
    fn cumulative_savings_measurement_uses_only_the_turn_delta() {
        let observation = TokenSavingsObservation {
            turn_id: "turn".into(),
            before_cumulative_tokens: Some(25_000),
            latest_cumulative_tokens: Some(31_250),
            usage_seen: true,
            completed: true,
            qwen_event_ids: HashSet::new(),
            qwen: routing::QwenSavingsEvidence::default(),
        };
        assert_eq!(cumulative_turn_delta(&observation), Some(6_250));
    }

    #[test]
    fn standalone_macro_server_key_is_quoted() {
        assert_eq!(
            standalone_mcp_key("codex-native-macro"),
            "mcp_servers.\"codex-native-macro\""
        );
    }

    #[test]
    fn stop_target_clone_releases_state_before_request_mutation() {
        let state = RefCell::new(AppState {
            active_thread_id: Some("thread".into()),
            active_turn_id: Some("turn".into()),
            ..AppState::default()
        });
        assert_eq!(
            cloned_active_turn_target(&state),
            Some(("thread".into(), "turn".into()))
        );
        assert!(
            state.try_borrow_mut().is_ok(),
            "Stop must release its immutable state borrow before request() mutates pending state"
        );
    }

    #[test]
    fn top_bar_distinguishes_current_context_from_thread_total() {
        assert_eq!(
            context_usage_label(&json!({
                "total": {"totalTokens": 4_970_297},
                "last": {"totalTokens": 8_511},
                "modelContextWindow": 258_400
            })),
            "Current context 8,511 / 258,400 tokens (3%) · Thread total 4,970,297 tokens"
        );
        assert_eq!(
            context_usage_label(&json!({
                "provider": "Qwen",
                "providerTotalTokens": 12_345,
                "tokensPerSecond": 42.5,
                "total": {"totalTokens": 12_345},
                "last": {"totalTokens": 8_192},
                "modelContextWindow": 65_536
            })),
            "Qwen context 8,192 / 65,536 tokens (12%) · 42.5 tok/s · Qwen task total 12,345 tokens"
        );
    }

    #[test]
    fn giant_rollouts_receive_a_phase_rollover_warning() {
        assert_eq!(
            task_history_rollover_warning(Some(4_313_216_100)),
            Some("History 4.0 GiB · continuation recommended".into())
        );
        assert_eq!(
            task_history_rollover_warning(Some(TASK_ROLLOVER_WARN_BYTES - 1)),
            None
        );
    }

    #[test]
    fn canonical_compaction_events_require_the_structured_item_type() {
        let started = context_compaction_event(
            "item/started",
            &json!({
                "threadId": "thread",
                "turnId": "turn",
                "item": {"id": "compact", "type": "contextCompaction"}
            }),
        )
        .expect("compaction start");
        assert_eq!(started.phase, ContextCompactionPhase::Started);
        assert_eq!(started.thread_id, "thread");
        assert_eq!(started.turn_id, "turn");
        assert_eq!(started.item_id, "compact");

        let completed = context_compaction_event(
            "item/completed",
            &json!({
                "threadId": "thread",
                "turnId": "turn",
                "item": {"id": "compact", "type": "contextCompaction"}
            }),
        )
        .expect("compaction completion");
        assert_eq!(completed.phase, ContextCompactionPhase::Completed);
        assert!(
            context_compaction_event(
                "item/completed",
                &json!({
                    "threadId": "thread",
                    "turnId": "turn",
                    "item": {"id": "message", "type": "agentMessage"}
                }),
            )
            .is_none()
        );
    }
}
