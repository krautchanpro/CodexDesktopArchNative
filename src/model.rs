use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    path::PathBuf,
};

use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::plugin::PluginCatalog;
use crate::protocol::{PairingInfo, RequestId, RpcEnvelope, RpcRequest};
use crate::routing::{RouteDecision, TaskRoutingState};

const MAX_ITEMS_PER_TURN: usize = 160;
const MAX_ITEMS_PER_THREAD: usize = 480;
const MAX_STREAMED_MESSAGE_BYTES: usize = 512 * 1024;
const MAX_LIVE_DIFF_BYTES: usize = 512 * 1024;
const MAX_TERMINAL_OUTPUT_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConnectionState {
    #[default]
    Connecting,
    Ready,
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WorkspacePage {
    #[default]
    Chat,
    Chatgpt,
    Projects,
    Sites,
    Terminal,
    AgentWorkspace,
    Context,
    Extensions,
    QwenBuddy,
    ComputerUse,
    Automations,
    Remote,
    Diagnostics,
    Settings,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ThreadSummary {
    pub id: String,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub parent_thread_id: Option<String>,
    #[serde(default)]
    pub agent_nickname: Option<String>,
    #[serde(default)]
    pub agent_role: Option<String>,
    #[serde(default)]
    pub preview: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub cwd: String,
    #[serde(default)]
    pub path: Option<PathBuf>,
    #[serde(default)]
    pub created_at: i64,
    #[serde(default)]
    pub updated_at: i64,
    #[serde(default)]
    pub status: Value,
    #[serde(default)]
    pub git_info: Option<Value>,
    #[serde(default)]
    pub turns: Vec<Turn>,
    /// Native Buddy chats can exist before the app server materializes a first
    /// GPT turn. They must not be resumed or paged through app-server APIs.
    #[serde(skip)]
    pub native_buddy_only: bool,
}

impl ThreadSummary {
    pub fn title(&self) -> &str {
        self.name
            .as_deref()
            .filter(|name| !name.trim().is_empty())
            .or_else(|| (!self.preview.trim().is_empty()).then_some(self.preview.as_str()))
            .unwrap_or("New task")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Turn {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub items: Vec<Value>,
    #[serde(default)]
    pub status: Value,
    #[serde(default)]
    pub error: Option<Value>,
    #[serde(default)]
    pub started_at: Option<i64>,
    #[serde(default)]
    pub completed_at: Option<i64>,
    #[serde(default)]
    pub items_view: String,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TurnProgress {
    pub turn_id: String,
    pub plan: Vec<Value>,
    pub diff: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContextCompactionMeasurement {
    pub item_id: String,
    pub turn_id: String,
    pub generation_before: u64,
    pub before_tokens: u64,
    pub context_window: u64,
    pub completed: bool,
    pub checkpointed: bool,
    pub checkpoint_elapsed_ms: u64,
    pub checkpoint_hashed_bytes: u64,
    pub checkpoint_reused_evidence: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskRuntimeSettings {
    pub model: String,
    pub reasoning_effort: Option<String>,
    pub service_tier: Option<String>,
    pub sandbox_policy: Value,
    pub approval_policy: Value,
}

#[derive(Debug, Clone)]
pub enum PendingKind {
    Bootstrap(&'static str),
    OpenThread(String),
    UnsubscribeThread(String),
    ResumeAndSend {
        thread_id: String,
        prompt: String,
        attachments: Vec<String>,
    },
    ThreadTurns {
        thread_id: String,
        prepend: bool,
    },
    ThreadTurnDetails {
        thread_id: String,
    },
    SearchThreads {
        query: String,
    },
    StartThread {
        prompt: String,
        attachments: Vec<String>,
        settings: TaskRuntimeSettings,
        routing: Box<TaskRoutingState>,
    },
    SendTurn {
        thread_id: String,
        prompt: String,
        attachments: Vec<String>,
        route: RouteDecision,
    },
    SteerTurn {
        thread_id: String,
        prompt: String,
        attachments: Vec<String>,
        pending_item_id: String,
    },
    UpdateThreadSettings(String),
    Login,
    Logout,
    RenameThread,
    ArchiveThread,
    UnarchiveThread,
    DeleteThread,
    BulkArchiveThread,
    BulkUnarchiveThread,
    BulkDeleteThread,
    ForkThread,
    Goal(String),
    GoalStatus {
        thread_id: String,
        status: String,
    },
    GoalRead(String),
    CompactThread {
        thread_id: String,
        automatic: bool,
        checkpoint_id: String,
    },
    RollbackThread(String),
    AccountRateLimits,
    AccountUsage,
    ConsumeRateLimitResetCredit,
    VoiceStart {
        thread_id: String,
        audio: Value,
    },
    VoiceAppend(String),
    Hooks,
    Marketplace,
    PluginInstall(String),
    PluginUninstall(String),
    PluginEnable {
        plugin_id: String,
        enabled: bool,
    },
    AppEnable {
        app_id: String,
        enabled: bool,
    },
    PluginRead(String),
    SkillEnable {
        skill_name: String,
        enabled: bool,
    },
    McpOauth(String),
    McpRefresh,
    MacroConfig {
        enabled: bool,
    },
    ComputerPolicy,
    Generic,
}

#[derive(Debug, Clone)]
pub struct ApprovalRequest {
    pub id: RequestId,
    pub method: String,
    pub params: Value,
}

#[derive(Debug, Clone, Default)]
pub enum RemoteState {
    #[default]
    Unknown,
    Disabled,
    Connecting,
    Connected,
    Error(String),
}

#[derive(Debug, Default)]
pub struct AppState {
    pub connection: ConnectionState,
    pub page: WorkspacePage,
    pub platform_family: Option<String>,
    pub platform_os: Option<String>,
    pub threads: BTreeMap<String, ThreadSummary>,
    pub thread_order: Vec<String>,
    /// Server-ranked full-text search results are kept separately from the
    /// master task list so content-only hits and snippets survive rendering,
    /// while clearing the query instantly restores the complete task list.
    pub thread_search_query: Option<String>,
    pub thread_search_order: Vec<String>,
    pub thread_search_snippets: HashMap<String, String>,
    pub active_thread_id: Option<String>,
    pub active_turn_id: Option<String>,
    /// Threads resumed or started on the current app-server connection. This
    /// is cleared on disconnect so sends remain safe without paying the cost
    /// of `thread/resume` before every message.
    pub resumed_threads: HashSet<String>,
    /// Monotonic content revision used by the GTK layer to avoid rebuilding a
    /// large transcript for unrelated backend notifications.
    pub transcript_revision: u64,
    /// Opaque app-server cursors for loading older turns. Only the active
    /// thread retains full turn payloads; inactive summaries stay lightweight.
    pub transcript_cursors: HashMap<String, String>,
    pub transcript_complete: HashSet<String>,
    /// Independent cursor for hydrating full activity one turn at a time.
    /// Conversation summaries stay fast even for multi-hundred-megabyte rollouts.
    pub transcript_detail_cursors: HashMap<String, String>,
    pub transcript_detail_loading: HashSet<String>,
    pub transcript_detail_available: HashSet<String>,
    pub thread_token_usage: HashMap<String, Value>,
    /// Threads currently creating a checkpoint on a bounded host worker.
    pub context_checkpoint_inflight: HashSet<String>,
    /// Canonical app-server compactions awaiting completion or the first
    /// post-compaction usage measurement.
    pub context_compaction_measurements: HashMap<String, ContextCompactionMeasurement>,
    /// Highest prompted percentage per task, preventing repeated prompts while
    /// usage remains within the same threshold band.
    pub context_prompted_percent: HashMap<String, u8>,
    pub thread_goals: HashMap<String, Value>,
    /// Sticky runtime settings returned by thread/start, thread/resume, and
    /// thread/settings/updated. Keeping them per task prevents composer
    /// controls from inheriting whichever task happened to be opened last.
    pub task_runtime_settings: HashMap<String, TaskRuntimeSettings>,
    /// Live turn progress keyed by thread id. Plan and diff notifications are
    /// transient app-server state, so completed turns are removed immediately.
    pub turn_progress: HashMap<String, TurnProgress>,
    /// Threads whose latest local rollout lifecycle marker is task_started and
    /// whose rollout is held open by a Codex app-server process.
    pub external_running_threads: HashSet<String>,
    pub pending: HashMap<RequestId, PendingKind>,
    pub approvals: VecDeque<ApprovalRequest>,
    pub streaming_text: HashMap<String, String>,
    pub latest_diff: String,
    pub terminal_output: String,
    pub models: Vec<Value>,
    pub skills: Vec<Value>,
    pub plugins: PluginCatalog,
    pub plugins_full_catalog_loaded: bool,
    pub plugins_full_catalog_loading: bool,
    pub mcp_servers: Vec<Value>,
    pub apps: Vec<Value>,
    pub computer_report: Option<Value>,
    pub computer_busy: bool,
    pub qwen_report: Option<Value>,
    pub qwen_busy: bool,
    pub account: Option<Value>,
    pub account_rate_limits: Option<Value>,
    pub account_usage: Option<Value>,
    pub reset_credit_in_flight: bool,
    pub config: Option<Value>,
    pub config_requirements: Option<Value>,
    pub permission_profiles: Vec<Value>,
    pub experimental_features: Vec<Value>,
    pub collaboration_modes: Vec<Value>,
    pub model_provider_capabilities: Option<Value>,
    pub hooks: Vec<Value>,
    pub workspace_report: Option<Value>,
    pub workspace_session: Option<Value>,
    pub diagnostics: Option<Value>,
    pub update_report: Option<Value>,
    pub remote_doctor: Option<Value>,
    pub latest_appshot: Option<Value>,
    pub recording_session: Option<Value>,
    pub remote: RemoteState,
    pub remote_details: Option<Value>,
    pub remote_pairing: Option<PairingInfo>,
    pub remote_clients: Vec<Value>,
    pub logs: VecDeque<String>,
    pub last_error: Option<String>,
}

impl AppState {
    pub fn set_threads(&mut self, threads: Vec<ThreadSummary>) {
        let active_id = self.active_thread_id.clone();
        let active_thread = active_id
            .as_ref()
            .and_then(|id| self.threads.get(id))
            .cloned();
        let agent_threads = self
            .threads
            .values()
            .filter(|thread| thread.parent_thread_id.is_some())
            .cloned()
            .collect::<Vec<_>>();
        self.thread_order.clear();
        self.threads.clear();
        for mut thread in threads {
            if active_id.as_deref() == Some(thread.id.as_str()) && thread.turns.is_empty() {
                thread.turns = active_thread
                    .as_ref()
                    .map(|active| active.turns.clone())
                    .unwrap_or_default();
            }
            if thread.parent_thread_id.is_none() {
                self.thread_order.push(thread.id.clone());
            }
            self.threads.insert(thread.id.clone(), thread);
        }
        for thread in agent_threads {
            self.threads.entry(thread.id.clone()).or_insert(thread);
        }
        // A paginated, filtered, or temporarily incomplete thread/list result
        // must not evict the task the user is currently reading. Besides
        // making the transcript flash to the welcome page, eviction leaves a
        // stale active id that cannot be resumed safely for the next turn.
        if let Some(active_thread) = active_thread
            && !self.threads.contains_key(&active_thread.id)
        {
            if active_thread.parent_thread_id.is_none() {
                self.thread_order.push(active_thread.id.clone());
            }
            self.threads.insert(active_thread.id.clone(), active_thread);
        }
        self.sort_threads();
    }

    pub fn merge_threads(&mut self, threads: Vec<ThreadSummary>) {
        for thread in threads {
            let id = thread.id.clone();
            if !self.threads.contains_key(&id) && thread.parent_thread_id.is_none() {
                self.thread_order.push(id.clone());
            }
            self.threads.insert(id, thread);
        }
        self.sort_threads();
    }

    pub fn set_thread_search_results(
        &mut self,
        query: String,
        results: Vec<(ThreadSummary, String)>,
    ) {
        self.thread_search_query = Some(query);
        self.thread_search_order.clear();
        self.thread_search_snippets.clear();
        for (thread, snippet) in results {
            let id = thread.id.clone();
            let is_root = thread.parent_thread_id.is_none();
            self.upsert_thread(thread);
            if is_root && !self.thread_search_order.contains(&id) {
                self.thread_search_order.push(id.clone());
            }
            if !snippet.trim().is_empty() {
                self.thread_search_snippets.insert(id, snippet);
            }
        }
    }

    pub fn clear_thread_search(&mut self) {
        self.thread_search_query = None;
        self.thread_search_order.clear();
        self.thread_search_snippets.clear();
    }

    pub fn upsert_thread(&mut self, mut thread: ThreadSummary) {
        let id = thread.id.clone();
        let previous = self.threads.get(&id).cloned();
        if let Some(existing) = previous.as_ref() {
            if thread.turns.is_empty() {
                thread.turns = existing.turns.clone();
            }
            // Some live thread/resume responses omit the unstable rollout path.
            // Keep the path discovered by thread/list so the UI can make its
            // large-history loading decision without rescanning every rollout.
            if thread.path.is_none() {
                thread.path = existing.path.clone();
            }
        }
        let active_changed = self.active_thread_id.as_deref() == Some(id.as_str())
            && previous.as_ref() != Some(&thread);
        if !self.threads.contains_key(&id) && thread.parent_thread_id.is_none() {
            self.thread_order.insert(0, id.clone());
        }
        self.threads.insert(id, thread);
        self.sort_threads();
        if active_changed {
            self.mark_transcript_changed();
        }
    }

    pub fn active_thread(&self) -> Option<&ThreadSummary> {
        self.active_thread_id
            .as_ref()
            .and_then(|id| self.threads.get(id))
    }

    pub fn activate_thread(&mut self, id: String) {
        if self.active_thread_id.as_deref() == Some(id.as_str()) {
            return;
        }
        for (thread_id, thread) in &mut self.threads {
            // App-server transcripts can be reloaded after switching tasks, but
            // an in-flight Buddy turn exists only in Native until its provider
            // finishes. Keep that turn alive while Qwen/Gemini/etc. is working.
            if thread_id != &id && !self.external_running_threads.contains(thread_id) {
                thread.turns.clear();
            }
        }
        self.streaming_text.clear();
        self.active_turn_id = None;
        self.transcript_detail_cursors
            .retain(|thread_id, _| thread_id == &id);
        self.transcript_detail_loading
            .retain(|thread_id| thread_id == &id);
        self.transcript_detail_available
            .retain(|thread_id| thread_id == &id);
        self.active_thread_id = Some(id);
        self.mark_transcript_changed();
    }

    pub fn mark_transcript_changed(&mut self) {
        self.transcript_revision = self.transcript_revision.wrapping_add(1);
    }

    pub fn set_thread_turns(
        &mut self,
        thread_id: &str,
        mut turns: Vec<Turn>,
        next_cursor: Option<String>,
        prepend: bool,
    ) {
        for turn in &mut turns {
            trim_turn_payload(turn);
        }
        let Some(thread) = self.threads.get_mut(thread_id) else {
            return;
        };
        let detailed_turns = thread
            .turns
            .iter()
            .filter(|turn| turn.items_view != "summary")
            .map(|turn| (turn.id.clone(), turn.clone()))
            .collect::<HashMap<_, _>>();
        for turn in &mut turns {
            if let Some(detailed) = detailed_turns.get(&turn.id) {
                // Compact app-server pages do not consistently include the
                // itemsView marker. Merge their lifecycle and summary fields
                // into the already-detailed turn instead of letting an
                // omitted marker erase locally recovered conversation items.
                let incoming = std::mem::take(turn);
                *turn = detailed.clone();
                merge_progressive_turn(turn, incoming);
            }
        }
        if prepend {
            turns.append(&mut thread.turns);
        } else {
            let refreshed = turns
                .iter()
                .map(|turn| turn.id.clone())
                .collect::<HashSet<_>>();
            turns.extend(
                thread
                    .turns
                    .drain(..)
                    .filter(|turn| !refreshed.contains(&turn.id)),
            );
            turns.sort_by(|left, right| {
                left.started_at
                    .cmp(&right.started_at)
                    .then_with(|| left.id.cmp(&right.id))
            });
        }
        let mut seen = HashSet::new();
        turns.retain(|turn| seen.insert(turn.id.clone()));
        thread.turns = turns;
        trim_thread_payload(thread);
        if let Some(cursor) = next_cursor {
            self.transcript_cursors.insert(thread_id.to_owned(), cursor);
            self.transcript_complete.remove(thread_id);
        } else {
            self.transcript_cursors.remove(thread_id);
            self.transcript_complete.insert(thread_id.to_owned());
        }
        if self.active_thread_id.as_deref() == Some(thread_id) {
            self.mark_transcript_changed();
        }
    }

    pub fn merge_local_chat_turns(
        &mut self,
        thread_id: &str,
        recovered: Vec<Turn>,
    ) -> (usize, usize) {
        let Some(thread) = self.threads.get_mut(thread_id) else {
            return (0, 0);
        };
        let before_turns = thread.turns.len();
        let before_items = thread
            .turns
            .iter()
            .map(|turn| turn.items.len())
            .sum::<usize>();
        let mut changed = false;
        for recovered_turn in recovered {
            if let Some(existing) = thread
                .turns
                .iter_mut()
                .find(|turn| turn.id == recovered_turn.id)
            {
                let before = existing.clone();
                merge_progressive_turn(existing, recovered_turn);
                changed |= *existing != before;
            } else {
                thread.turns.push(recovered_turn);
                changed = true;
            }
        }
        thread.turns.sort_by(|left, right| {
            left.started_at
                .cmp(&right.started_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        for turn in &mut thread.turns {
            trim_turn_payload(turn);
        }
        trim_thread_payload(thread);
        let after_turns = thread.turns.len();
        let after_items = thread
            .turns
            .iter()
            .map(|turn| turn.items.len())
            .sum::<usize>();
        let added = (
            after_turns.saturating_sub(before_turns),
            after_items.saturating_sub(before_items),
        );
        if self.active_thread_id.as_deref() == Some(thread_id) && changed {
            self.mark_transcript_changed();
        }
        added
    }

    pub fn merge_thread_turn_details(
        &mut self,
        thread_id: &str,
        mut turn: Turn,
        next_cursor: Option<String>,
    ) {
        trim_turn_payload(&mut turn);
        turn.items_view = "full".into();
        let Some(thread) = self.threads.get_mut(thread_id) else {
            self.transcript_detail_loading.remove(thread_id);
            return;
        };
        if let Some(existing) = thread
            .turns
            .iter_mut()
            .find(|existing| existing.id == turn.id)
        {
            let pending_user_items = existing
                .items
                .iter()
                .filter(|item| pending_steer_item(item))
                .cloned()
                .collect::<Vec<_>>();
            *existing = turn;
            retain_unmatched_pending_user_items(existing, pending_user_items);
        } else {
            thread.turns.push(turn);
            thread.turns.sort_by(|left, right| {
                left.started_at
                    .cmp(&right.started_at)
                    .then_with(|| left.id.cmp(&right.id))
            });
        }
        trim_thread_payload(thread);
        self.transcript_detail_loading.remove(thread_id);
        self.transcript_detail_available.remove(thread_id);
        if let Some(cursor) = next_cursor {
            self.transcript_detail_cursors
                .insert(thread_id.to_owned(), cursor);
        } else {
            self.transcript_detail_cursors.remove(thread_id);
        }
        if self.active_thread_id.as_deref() == Some(thread_id) {
            self.mark_transcript_changed();
        }
    }

    pub fn turn_is_running(&self) -> bool {
        self.active_turn_id.is_some()
    }

    pub fn push_pending_steer(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        content: Vec<Value>,
        started_at: i64,
    ) -> bool {
        let Some(thread) = self.threads.get_mut(thread_id) else {
            return false;
        };
        let turn_index = thread
            .turns
            .iter()
            .position(|turn| turn.id == turn_id)
            .unwrap_or_else(|| {
                thread.turns.push(Turn {
                    id: turn_id.to_owned(),
                    status: serde_json::json!({"type": "inProgress"}),
                    started_at: Some(started_at),
                    items_view: "full".into(),
                    ..Turn::default()
                });
                thread.turns.len() - 1
            });
        thread.turns[turn_index].items.push(serde_json::json!({
            "id": item_id,
            "type": "userMessage",
            "content": content,
            "pendingSteer": true,
        }));
        trim_turn_payload(&mut thread.turns[turn_index]);
        trim_thread_payload(thread);
        if self.active_thread_id.as_deref() == Some(thread_id) {
            self.mark_transcript_changed();
        }
        true
    }

    pub fn remove_pending_steer(&mut self, thread_id: &str, item_id: &str) {
        let Some(thread) = self.threads.get_mut(thread_id) else {
            return;
        };
        let mut changed = false;
        for turn in &mut thread.turns {
            let before = turn.items.len();
            turn.items.retain(|item| {
                item.get("id").and_then(Value::as_str) != Some(item_id) || !pending_steer_item(item)
            });
            changed |= turn.items.len() != before;
        }
        if changed && self.active_thread_id.as_deref() == Some(thread_id) {
            self.mark_transcript_changed();
        }
    }

    /// Clear optimistic turn state after app-server reports that a steer target
    /// no longer exists. The caller can then retry the preserved input through
    /// `turn/start` without leaving a phantom spinner behind.
    pub fn clear_stale_turn(&mut self, thread_id: &str) {
        if self.active_thread_id.as_deref() == Some(thread_id) {
            self.active_turn_id = None;
        }
        self.turn_progress.remove(thread_id);
        self.external_running_threads.remove(thread_id);
        if let Some(thread) = self.threads.get_mut(thread_id) {
            thread.status = serde_json::json!({"type": "idle"});
            for turn in &mut thread.turns {
                let status = turn
                    .status
                    .as_str()
                    .or_else(|| turn.status.get("type").and_then(Value::as_str));
                if matches!(status, Some("active" | "inProgress" | "running")) {
                    turn.status = Value::String("interrupted".into());
                }
            }
        }
    }

    fn complete_turn_lifecycle(&mut self, thread_id: &str, completed_turn_id: Option<&str>) {
        let active_turn_id = (self.active_thread_id.as_deref() == Some(thread_id))
            .then(|| self.active_turn_id.clone())
            .flatten();
        let newer_turn_is_running = completed_turn_id.is_some_and(|completed_turn_id| {
            active_turn_id
                .as_deref()
                .is_some_and(|active_turn_id| active_turn_id != completed_turn_id)
        });
        if newer_turn_is_running {
            return;
        }

        if self.active_thread_id.as_deref() == Some(thread_id) {
            self.active_turn_id = None;
        }
        self.turn_progress.remove(thread_id);
        self.external_running_threads.remove(thread_id);

        let terminal_turn_id = completed_turn_id.or(active_turn_id.as_deref());
        if let Some(thread) = self.threads.get_mut(thread_id) {
            thread.status = serde_json::json!({"type": "idle"});
            for turn in &mut thread.turns {
                let status = turn
                    .status
                    .as_str()
                    .or_else(|| turn.status.get("type").and_then(Value::as_str));
                let is_running = matches!(status, Some("active" | "inProgress" | "running"));
                if is_running
                    && terminal_turn_id
                        .map(|turn_id| turn.id == turn_id)
                        .unwrap_or(true)
                {
                    turn.status = Value::String("completed".into());
                }
            }
        }
    }

    pub fn push_log(&mut self, line: String) {
        const MAX_LOG_LINES: usize = 500;
        if self.logs.len() == MAX_LOG_LINES {
            self.logs.pop_front();
        }
        self.logs.push_back(line);
    }

    pub fn apply_server_message(&mut self, message: &RpcEnvelope) {
        match message {
            RpcEnvelope::Request(request) => self.queue_server_request(request),
            RpcEnvelope::Notification(notification) => {
                self.apply_notification(&notification.method, &notification.params)
            }
            RpcEnvelope::Response(_) => {}
        }
    }

    fn sort_threads(&mut self) {
        self.thread_order.sort_by_key(|id| {
            std::cmp::Reverse(
                self.threads
                    .get(id)
                    .map(|thread| thread.updated_at)
                    .unwrap_or(0),
            )
        });
        self.thread_order.dedup();
    }

    fn queue_server_request(&mut self, request: &RpcRequest) {
        if !self
            .approvals
            .iter()
            .any(|approval| approval.id == request.id)
        {
            self.approvals.push_back(ApprovalRequest {
                id: request.id.clone(),
                method: request.method.clone(),
                params: request.params.clone(),
            });
        }
    }

    fn apply_notification(&mut self, method: &str, params: &Value) {
        let changes_transcript = matches!(
            method,
            "thread/started"
                | "thread/archived"
                | "thread/deleted"
                | "thread/name/updated"
                | "turn/started"
                | "turn/completed"
                | "item/started"
                | "item/completed"
                | "item/agentMessage/delta"
                | "turn/plan/updated"
                | "turn/diff/updated"
        );
        match method {
            "thread/started" => {
                if let Some(thread) = params.get("thread")
                    && let Ok(thread) = serde_json::from_value(thread.clone())
                {
                    self.upsert_thread(thread);
                }
            }
            "thread/status/changed" => {
                if let (Some(id), Some(status)) = (
                    params.get("threadId").and_then(Value::as_str),
                    params.get("status"),
                ) {
                    if let Some(thread) = self.threads.get_mut(id) {
                        thread.status = status.clone();
                    }
                    let status = status
                        .as_str()
                        .or_else(|| status.get("type").and_then(Value::as_str));
                    if status == Some("idle") {
                        self.complete_turn_lifecycle(id, None);
                    }
                }
            }
            "thread/archived" | "thread/deleted" => {
                if let Some(id) = params.get("threadId").and_then(Value::as_str) {
                    self.threads.remove(id);
                    self.thread_order.retain(|candidate| candidate != id);
                    self.turn_progress.remove(id);
                    self.task_runtime_settings.remove(id);
                    self.context_checkpoint_inflight.remove(id);
                    self.context_compaction_measurements.remove(id);
                    self.context_prompted_percent.remove(id);
                    if self.active_thread_id.as_deref() == Some(id) {
                        self.active_thread_id = None;
                        self.active_turn_id = None;
                    }
                }
            }
            "thread/unarchived" => {}
            "thread/name/updated" => {
                if let (Some(id), Some(name)) = (
                    params.get("threadId").and_then(Value::as_str),
                    params.get("name").and_then(Value::as_str),
                ) && let Some(thread) = self.threads.get_mut(id)
                {
                    thread.name = Some(name.to_owned());
                }
            }
            "turn/started" => {
                if let Some(thread_id) = params.get("threadId").and_then(Value::as_str)
                    && let Some(thread) = self.threads.get_mut(thread_id)
                {
                    thread.status = serde_json::json!({"type": "active", "activeFlags": []});
                }
                if let Some(thread_id) = params.get("threadId").and_then(Value::as_str) {
                    let progress = self.turn_progress.entry(thread_id.to_owned()).or_default();
                    progress.turn_id = params
                        .pointer("/turn/id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    progress.plan.clear();
                    progress.diff.clear();
                }
                if let Some(turn) = params.get("turn")
                    && let Ok(turn) = serde_json::from_value::<Turn>(turn.clone())
                {
                    self.active_turn_id = Some(turn.id.clone());
                    self.upsert_turn(params, turn);
                }
            }
            "turn/completed" => {
                let completed_turn = params
                    .get("turn")
                    .and_then(|turn| serde_json::from_value::<Turn>(turn.clone()).ok());
                let completed_turn_id = completed_turn
                    .as_ref()
                    .map(|turn| turn.id.as_str())
                    .filter(|turn_id| !turn_id.is_empty());
                if let Some(thread_id) = params.get("threadId").and_then(Value::as_str) {
                    self.complete_turn_lifecycle(thread_id, completed_turn_id);
                }
                if let Some(turn) = completed_turn {
                    self.upsert_turn(params, turn);
                }
            }
            "item/started" | "item/completed" => {
                if let Some(item) = params.get("item") {
                    let mut item = item.clone();
                    let item_id = item.get("id").and_then(Value::as_str).map(str::to_owned);
                    if method == "item/completed"
                        && let Some(item_id) = item_id
                    {
                        if let Some(streamed) = self.streaming_text.get(&item_id) {
                            preserve_streamed_agent_text(&mut item, streamed);
                        }
                        self.upsert_item(params, item);
                        self.streaming_text.remove(&item_id);
                    } else {
                        self.upsert_item(params, item);
                    }
                }
            }
            "item/agentMessage/delta" => {
                if let (Some(item_id), Some(delta)) = (
                    params.get("itemId").and_then(Value::as_str),
                    params.get("delta").and_then(Value::as_str),
                ) {
                    let stream = self.streaming_text.entry(item_id.to_owned()).or_default();
                    push_bounded(stream, delta, MAX_STREAMED_MESSAGE_BYTES);
                }
            }
            "turn/plan/updated" => {
                if let Some(thread_id) = params.get("threadId").and_then(Value::as_str) {
                    let progress = self.turn_progress.entry(thread_id.to_owned()).or_default();
                    if let Some(turn_id) = params.get("turnId").and_then(Value::as_str) {
                        progress.turn_id = turn_id.to_owned();
                    }
                    progress.plan = params
                        .get("plan")
                        .and_then(Value::as_array)
                        .map(|plan| plan.iter().take(100).cloned().collect())
                        .unwrap_or_default();
                }
            }
            "turn/diff/updated" => {
                let diff = params
                    .get("diff")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                self.latest_diff.clear();
                push_bounded(&mut self.latest_diff, diff, MAX_LIVE_DIFF_BYTES);
                if let Some(thread_id) = params.get("threadId").and_then(Value::as_str) {
                    let progress = self.turn_progress.entry(thread_id.to_owned()).or_default();
                    if let Some(turn_id) = params.get("turnId").and_then(Value::as_str) {
                        progress.turn_id = turn_id.to_owned();
                    }
                    progress.diff.clear();
                    push_bounded(&mut progress.diff, diff, MAX_LIVE_DIFF_BYTES);
                }
            }
            "thread/tokenUsage/updated" => {
                if let (Some(thread_id), Some(usage)) = (
                    params.get("threadId").and_then(Value::as_str),
                    params.get("tokenUsage"),
                ) {
                    self.thread_token_usage
                        .insert(thread_id.to_owned(), usage.clone());
                }
            }
            "thread/goal/updated" => {
                if let (Some(thread_id), Some(goal)) = (
                    params.get("threadId").and_then(Value::as_str),
                    params.get("goal"),
                ) {
                    self.thread_goals.insert(thread_id.to_owned(), goal.clone());
                }
            }
            "thread/goal/cleared" => {
                if let Some(thread_id) = params.get("threadId").and_then(Value::as_str) {
                    self.thread_goals.remove(thread_id);
                }
            }
            "command/exec/outputDelta"
            | "process/outputDelta"
            | "item/commandExecution/outputDelta" => {
                if let Some(delta) = params.get("delta").and_then(Value::as_str) {
                    push_bounded(&mut self.terminal_output, delta, MAX_TERMINAL_OUTPUT_BYTES);
                } else if let Some(chunk) = params.get("deltaBase64").and_then(Value::as_str)
                    && let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(chunk)
                {
                    push_bounded(
                        &mut self.terminal_output,
                        &String::from_utf8_lossy(&decoded),
                        MAX_TERMINAL_OUTPUT_BYTES,
                    );
                }
            }
            "serverRequest/resolved" => {
                if let Some(value) = params.get("requestId")
                    && let Ok(id) = serde_json::from_value::<RequestId>(value.clone())
                {
                    self.approvals.retain(|approval| approval.id != id);
                }
            }
            // The primary stdio app-server deliberately has remote control
            // disabled. Managed-host status is read from the daemon socket by
            // the backend, so its local notification must not overwrite that
            // authoritative state with `disabled`.
            "remoteControl/status/changed" => {}
            "account/updated" => {
                self.account = Some(params.clone());
            }
            "error" | "warning" | "guardianWarning" | "configWarning" => {
                self.last_error = params
                    .get("message")
                    .or_else(|| params.get("error"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
            }
            _ => {}
        }
        if changes_transcript {
            self.mark_transcript_changed();
        }
    }

    pub fn apply_remote_status(&mut self, value: &Value) {
        self.remote_details = Some(value.clone());
        self.remote = match value.get("status").and_then(Value::as_str) {
            Some("disabled") => RemoteState::Disabled,
            Some("connecting") => RemoteState::Connecting,
            Some("connected") => RemoteState::Connected,
            Some("errored") => RemoteState::Error(
                value
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("Remote connection failed")
                    .to_owned(),
            ),
            _ => RemoteState::Unknown,
        };
    }

    fn upsert_turn(&mut self, params: &Value, turn: Turn) {
        let thread_id = params
            .get("threadId")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| self.active_thread_id.clone());
        let Some(thread_id) = thread_id else {
            return;
        };
        let Some(thread) = self.threads.get_mut(&thread_id) else {
            return;
        };
        if let Some(existing) = thread.turns.iter_mut().find(|item| item.id == turn.id) {
            merge_progressive_turn(existing, turn);
        } else {
            thread.turns.push(turn);
            trim_thread_payload(thread);
        }
    }

    fn upsert_item(&mut self, params: &Value, item: Value) {
        let thread_id = params
            .get("threadId")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| self.active_thread_id.clone());
        let turn_id = params
            .get("turnId")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| self.active_turn_id.clone());
        let (Some(thread_id), Some(turn_id)) = (thread_id, turn_id) else {
            return;
        };
        let Some(thread) = self.threads.get_mut(&thread_id) else {
            return;
        };
        let Some(turn) = thread.turns.iter_mut().find(|turn| turn.id == turn_id) else {
            return;
        };
        remove_matching_pending_user_item(&mut turn.items, &item);
        let item_id = item.get("id").and_then(Value::as_str).unwrap_or_default();
        if let Some(existing) = turn
            .items
            .iter_mut()
            .find(|existing| existing.get("id").and_then(Value::as_str) == Some(item_id))
        {
            merge_progressive_value(existing, item);
        } else {
            turn.items.push(item);
            // Long-running turns can emit hundreds of tool and reasoning
            // items. Keep their activity bounded without evicting the user
            // and assistant messages that make up the visible conversation.
            trim_turn_payload(turn);
        }
        trim_thread_payload(thread);
    }
}

fn preserve_streamed_agent_text(item: &mut Value, streamed: &str) {
    if streamed.is_empty()
        || item.get("type").and_then(Value::as_str) != Some("agentMessage")
        || item
            .get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| !text.trim().is_empty())
    {
        return;
    }
    if let Some(item) = item.as_object_mut() {
        item.insert("text".into(), Value::String(streamed.to_owned()));
    }
}

fn merge_progressive_turn(existing: &mut Turn, incoming: Turn) {
    let local_recovery = incoming.items_view == "localRecovery";
    if !incoming.items_view.is_empty() && incoming.items_view != "summary" {
        // A compact server turn becomes detailed after full activity or
        // canonical-rollout recovery is merged into it. Promote the marker so
        // later summary pages cannot replace the newly durable conversation.
        existing.items_view = incoming.items_view.clone();
    }
    if !incoming.status.is_null() {
        existing.status = incoming.status;
    }
    if incoming.error.is_some() {
        existing.error = incoming.error;
    }
    if incoming.started_at.is_some() {
        existing.started_at = incoming.started_at;
    }
    if incoming.completed_at.is_some() {
        existing.completed_at = incoming.completed_at;
    }
    if local_recovery {
        merge_local_recovery_items(&mut existing.items, incoming.items);
        return;
    }
    for incoming_item in incoming.items {
        remove_matching_pending_user_item(&mut existing.items, &incoming_item);
        let item_id = incoming_item
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !item_id.is_empty()
            && let Some(existing_item) = existing
                .items
                .iter_mut()
                .find(|candidate| candidate.get("id").and_then(Value::as_str) == Some(item_id))
        {
            merge_progressive_value(existing_item, incoming_item);
        } else {
            existing.items.push(incoming_item);
        }
    }
}

fn pending_steer_item(item: &Value) -> bool {
    item.get("pendingSteer").and_then(Value::as_bool) == Some(true)
}

pub(crate) fn same_user_message(left: &Value, right: &Value) -> bool {
    left.get("type").and_then(Value::as_str) == Some("userMessage")
        && right.get("type").and_then(Value::as_str) == Some("userMessage")
        && user_message_signature(left) == user_message_signature(right)
}

fn user_message_signature(item: &Value) -> Vec<(String, String, String)> {
    let mut signature = item
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|part| {
            let kind = match part.get("type").and_then(Value::as_str) {
                Some("input_text") => "text",
                Some(kind) => kind,
                None => "",
            };
            let value = part
                .get("text")
                .or_else(|| part.get("path"))
                .or_else(|| part.get("url"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            let name = part.get("name").and_then(Value::as_str).unwrap_or_default();
            (kind.to_owned(), value.to_owned(), name.to_owned())
        })
        .collect::<Vec<_>>();
    if signature.is_empty()
        && let Some(text) = item.get("text").and_then(Value::as_str)
    {
        signature.push(("text".into(), text.into(), String::new()));
    }
    signature
}

fn same_recovered_message(left: &Value, right: &Value) -> bool {
    if same_user_message(left, right) {
        return true;
    }
    left.get("type").and_then(Value::as_str) == Some("agentMessage")
        && right.get("type").and_then(Value::as_str) == Some("agentMessage")
        && left
            .get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| {
                !text.is_empty() && right.get("text").and_then(Value::as_str) == Some(text)
            })
}

fn merge_recovered_message(existing: &mut Value, mut incoming: Value) {
    if let Some(incoming) = incoming.as_object_mut() {
        incoming.remove("id");
    }
    merge_progressive_value(existing, incoming);
}

fn merge_local_recovery_items(existing: &mut Vec<Value>, incoming: Vec<Value>) {
    let mut anchor = 0;
    for (incoming_index, incoming_item) in incoming.iter().enumerate() {
        remove_matching_pending_user_item(existing, incoming_item);
        let matching_index = existing
            .iter()
            .enumerate()
            .skip(anchor)
            .find(|(_, candidate)| same_recovered_message(candidate, incoming_item))
            .map(|(index, _)| index);
        if let Some(index) = matching_index {
            merge_recovered_message(&mut existing[index], incoming_item.clone());
            anchor = index + 1;
            continue;
        }

        let insert_at = incoming[incoming_index + 1..]
            .iter()
            .find_map(|later| {
                existing
                    .iter()
                    .enumerate()
                    .skip(anchor)
                    .find(|(_, candidate)| same_recovered_message(candidate, later))
                    .map(|(index, _)| index)
            })
            .unwrap_or(existing.len());
        existing.insert(insert_at, incoming_item.clone());
        anchor = insert_at + 1;
    }
}

fn remove_matching_pending_user_item(items: &mut Vec<Value>, incoming: &Value) {
    if pending_steer_item(incoming) {
        return;
    }
    if let Some(index) = items
        .iter()
        .position(|item| pending_steer_item(item) && same_user_message(item, incoming))
    {
        items.remove(index);
    }
}

fn retain_unmatched_pending_user_items(turn: &mut Turn, pending: Vec<Value>) {
    for pending_item in pending {
        if !turn
            .items
            .iter()
            .any(|item| same_user_message(item, &pending_item))
        {
            turn.items.push(pending_item);
        }
    }
}

/// App-server item notifications become progressively richer while a turn is
/// running, but completion payloads are allowed to omit fields already sent.
/// Merge updates without letting an empty late field erase visible content.
fn merge_progressive_value(target: &mut Value, update: Value) {
    match update {
        Value::Object(update) => {
            let Value::Object(target) = target else {
                *target = Value::Object(update);
                return;
            };
            for (key, value) in update {
                if let Some(existing) = target.get_mut(&key) {
                    merge_progressive_value(existing, value);
                } else {
                    target.insert(key, value);
                }
            }
        }
        update
            if progressive_payload_is_empty(&update) && !progressive_payload_is_empty(target) => {}
        update => *target = update,
    }
}

fn progressive_payload_is_empty(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(value) => value.trim().is_empty(),
        Value::Array(value) => value.is_empty(),
        Value::Object(value) => value.is_empty(),
        Value::Bool(_) | Value::Number(_) => false,
    }
}

fn trim_thread_payload(thread: &mut ThreadSummary) {
    let mut total = thread
        .turns
        .iter()
        .map(|turn| turn.items.len())
        .sum::<usize>();
    while total > MAX_ITEMS_PER_THREAD {
        let Some((turn_index, item_index)) =
            thread
                .turns
                .iter()
                .enumerate()
                .find_map(|(turn_index, turn)| {
                    turn.items
                        .iter()
                        .position(|item| !durable_transcript_item(item))
                        .map(|item_index| (turn_index, item_index))
                })
        else {
            break;
        };
        thread.turns[turn_index].items.remove(item_index);
        total -= 1;
    }
}

fn trim_turn_payload(turn: &mut Turn) {
    while turn.items.len() > MAX_ITEMS_PER_TURN {
        let Some(index) = turn
            .items
            .iter()
            .position(|item| !durable_transcript_item(item))
        else {
            break;
        };
        turn.items.remove(index);
    }
}

fn durable_transcript_item(item: &Value) -> bool {
    matches!(
        item.get("type").and_then(Value::as_str),
        Some("userMessage" | "agentMessage" | "plan" | "imageView")
    )
}

fn push_bounded(target: &mut String, value: &str, max_bytes: usize) {
    target.push_str(value);
    if target.len() <= max_bytes {
        return;
    }
    let mut start = target.len() - max_bytes;
    while !target.is_char_boundary(start) {
        start += 1;
    }
    target.drain(..start);
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::protocol::{RpcNotification, RpcRequest};

    #[test]
    fn duplicate_thread_notifications_are_idempotent() {
        let mut state = AppState::default();
        let message = RpcEnvelope::Notification(RpcNotification {
            method: "thread/started".into(),
            params: json!({"thread":{"id":"t1","preview":"Hello","updatedAt":4}}),
        });
        state.apply_server_message(&message);
        state.apply_server_message(&message);
        assert_eq!(state.thread_order, vec!["t1"]);
    }

    #[test]
    fn server_resolution_clears_string_id_approval() {
        let mut state = AppState::default();
        state.apply_server_message(&RpcEnvelope::Request(RpcRequest {
            id: RequestId::String("approval-12".into()),
            method: "item/commandExecution/requestApproval".into(),
            params: json!({}),
        }));
        state.apply_notification(
            "serverRequest/resolved",
            &json!({"requestId": "approval-12"}),
        );
        assert!(state.approvals.is_empty());
    }

    #[test]
    fn completed_turn_replaces_started_turn() {
        let mut state = AppState {
            active_thread_id: Some("t1".into()),
            ..AppState::default()
        };
        state.upsert_thread(ThreadSummary {
            id: "t1".into(),
            ..ThreadSummary::default()
        });
        state.apply_notification(
            "turn/started",
            &json!({"threadId":"t1","turn":{"id":"v1","items":[],"status":"inProgress"}}),
        );
        state.apply_notification(
            "turn/completed",
            &json!({"threadId":"t1","turn":{"id":"v1","items":[],"status":"completed"}}),
        );
        assert!(state.active_turn_id.is_none());
        assert_eq!(state.active_thread().unwrap().turns.len(), 1);
        assert_eq!(
            state.active_thread().unwrap().status.get("type"),
            Some(&Value::String("idle".into()))
        );
    }

    #[test]
    fn terminal_turn_event_clears_every_spinner_marker_without_a_turn_payload() {
        let mut state = AppState {
            active_thread_id: Some("t1".into()),
            active_turn_id: Some("v1".into()),
            ..AppState::default()
        };
        state.upsert_thread(ThreadSummary {
            id: "t1".into(),
            status: json!({"type": "active"}),
            turns: vec![Turn {
                id: "v1".into(),
                status: json!("inProgress"),
                ..Turn::default()
            }],
            ..ThreadSummary::default()
        });
        state.turn_progress.insert(
            "t1".into(),
            TurnProgress {
                turn_id: "v1".into(),
                ..TurnProgress::default()
            },
        );
        state.external_running_threads.insert("t1".into());

        state.apply_notification("turn/completed", &json!({"threadId": "t1"}));

        assert!(state.active_turn_id.is_none());
        assert!(!state.turn_progress.contains_key("t1"));
        assert!(!state.external_running_threads.contains("t1"));
        assert_eq!(state.threads["t1"].status["type"], "idle");
        assert_eq!(state.threads["t1"].turns[0].status, "completed");
    }

    #[test]
    fn late_completion_does_not_clear_a_newer_running_turn() {
        let mut state = AppState {
            active_thread_id: Some("t1".into()),
            active_turn_id: Some("new".into()),
            ..AppState::default()
        };
        state.upsert_thread(ThreadSummary {
            id: "t1".into(),
            status: json!({"type": "active"}),
            turns: vec![Turn {
                id: "new".into(),
                status: json!("inProgress"),
                ..Turn::default()
            }],
            ..ThreadSummary::default()
        });
        state.turn_progress.insert(
            "t1".into(),
            TurnProgress {
                turn_id: "new".into(),
                ..TurnProgress::default()
            },
        );
        state.external_running_threads.insert("t1".into());

        state.apply_notification(
            "turn/completed",
            &json!({
                "threadId": "t1",
                "turn": {"id": "old", "items": [], "status": "completed"}
            }),
        );

        assert_eq!(state.active_turn_id.as_deref(), Some("new"));
        assert!(state.turn_progress.contains_key("t1"));
        assert!(state.external_running_threads.contains("t1"));
        assert_eq!(state.threads["t1"].status["type"], "active");
        assert_eq!(state.threads["t1"].turns[0].status, "inProgress");
    }

    #[test]
    fn live_turn_progress_tracks_plan_and_diff_until_completion() {
        let mut state = AppState::default();
        state.upsert_thread(ThreadSummary {
            id: "t1".into(),
            ..ThreadSummary::default()
        });
        state.apply_notification(
            "turn/started",
            &json!({"threadId":"t1","turn":{"id":"v1","items":[],"status":"inProgress"}}),
        );
        state.apply_notification(
            "turn/plan/updated",
            &json!({
                "threadId": "t1",
                "turnId": "v1",
                "plan": [
                    {"step": "Inspect", "status": "completed"},
                    {"step": "Build", "status": "inProgress"},
                    {"step": "Verify", "status": "pending"}
                ]
            }),
        );
        state.apply_notification(
            "turn/diff/updated",
            &json!({
                "threadId": "t1",
                "turnId": "v1",
                "diff": "diff --git a/a.rs b/a.rs\n--- a/a.rs\n+++ b/a.rs\n-old\n+new"
            }),
        );

        let progress = state.turn_progress.get("t1").unwrap();
        assert_eq!(progress.turn_id, "v1");
        assert_eq!(progress.plan.len(), 3);
        assert!(progress.diff.contains("+new"));

        state.apply_notification(
            "turn/completed",
            &json!({"threadId":"t1","turn":{"id":"v1","items":[],"status":"completed"}}),
        );
        assert!(!state.turn_progress.contains_key("t1"));
    }

    #[test]
    fn interactive_history_refresh_preserves_separately_loaded_agents() {
        let mut state = AppState::default();
        state.merge_threads(vec![ThreadSummary {
            id: "agent".into(),
            parent_thread_id: Some("root".into()),
            agent_nickname: Some("Builder".into()),
            ..ThreadSummary::default()
        }]);
        state.set_threads(vec![ThreadSummary {
            id: "root".into(),
            ..ThreadSummary::default()
        }]);
        assert!(state.threads.contains_key("agent"));
        assert_eq!(state.thread_order, vec!["root"]);
    }

    #[test]
    fn paginated_history_refresh_keeps_the_active_task_and_transcript() {
        let mut state = AppState::default();
        state.upsert_thread(ThreadSummary {
            id: "active".into(),
            updated_at: 10,
            turns: vec![Turn {
                id: "turn".into(),
                items: vec![json!({"id": "message", "type": "agentMessage"})],
                ..Turn::default()
            }],
            ..ThreadSummary::default()
        });
        state.activate_thread("active".into());
        state.set_threads(vec![ThreadSummary {
            id: "newer-page-item".into(),
            updated_at: 20,
            ..ThreadSummary::default()
        }]);

        assert_eq!(state.active_thread_id.as_deref(), Some("active"));
        assert_eq!(state.active_thread().unwrap().turns.len(), 1);
        assert!(state.thread_order.contains(&"active".to_owned()));
    }

    #[test]
    fn switching_tasks_keeps_an_inflight_buddy_transcript() {
        let mut state = AppState::default();
        state.upsert_thread(ThreadSummary {
            id: "buddy".into(),
            turns: vec![Turn {
                id: "buddy-turn".into(),
                ..Turn::default()
            }],
            ..ThreadSummary::default()
        });
        state.upsert_thread(ThreadSummary {
            id: "other".into(),
            ..ThreadSummary::default()
        });
        state.active_thread_id = Some("buddy".into());
        state.external_running_threads.insert("buddy".into());

        state.activate_thread("other".into());

        assert_eq!(state.threads["buddy"].turns.len(), 1);
    }

    #[test]
    fn full_text_search_keeps_master_tasks_and_server_ranked_snippets() {
        let mut state = AppState::default();
        state.set_threads(vec![
            ThreadSummary {
                id: "alpha".into(),
                name: Some("Alpha title".into()),
                ..ThreadSummary::default()
            },
            ThreadSummary {
                id: "content-hit".into(),
                name: Some("Unrelated title".into()),
                turns: vec![Turn {
                    id: "loaded".into(),
                    ..Turn::default()
                }],
                ..ThreadSummary::default()
            },
        ]);

        state.set_thread_search_results(
            "needle".into(),
            vec![(
                ThreadSummary {
                    id: "content-hit".into(),
                    name: Some("Unrelated title".into()),
                    ..ThreadSummary::default()
                },
                "…the needle appears only inside this conversation…".into(),
            )],
        );

        assert_eq!(state.thread_search_order, vec!["content-hit"]);
        assert_eq!(
            state
                .thread_search_snippets
                .get("content-hit")
                .map(String::as_str),
            Some("…the needle appears only inside this conversation…")
        );
        assert!(state.thread_order.contains(&"alpha".to_owned()));
        assert_eq!(state.threads["content-hit"].turns[0].id, "loaded");

        state.clear_thread_search();
        assert!(state.thread_search_order.is_empty());
        assert!(state.threads.contains_key("alpha"));
        assert!(state.threads.contains_key("content-hit"));
    }

    #[test]
    fn pending_steer_is_visible_and_replaced_by_the_server_item() {
        let mut state = AppState {
            active_thread_id: Some("thread".into()),
            active_turn_id: Some("turn".into()),
            ..AppState::default()
        };
        state.upsert_thread(ThreadSummary {
            id: "thread".into(),
            turns: vec![Turn {
                id: "turn".into(),
                status: json!("inProgress"),
                ..Turn::default()
            }],
            ..ThreadSummary::default()
        });
        let content = vec![json!({"type": "text", "text": "follow up"})];

        assert!(state.push_pending_steer("thread", "turn", "native-steer", content.clone(), 10,));
        let pending = &state.threads["thread"].turns[0].items[0];
        assert_eq!(pending["pendingSteer"], true);

        state.upsert_item(
            &json!({"threadId": "thread", "turnId": "turn"}),
            json!({
                "id": "server-user",
                "type": "userMessage",
                "content": content,
            }),
        );

        let items = &state.threads["thread"].turns[0].items;
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["id"], "server-user");
        assert!(items[0].get("pendingSteer").is_none());
    }

    #[test]
    fn failed_pending_steer_can_be_removed_without_touching_other_chat() {
        let mut state = AppState {
            active_thread_id: Some("thread".into()),
            active_turn_id: Some("turn".into()),
            ..AppState::default()
        };
        state.upsert_thread(ThreadSummary {
            id: "thread".into(),
            turns: vec![Turn {
                id: "turn".into(),
                items: vec![json!({
                    "id": "existing-user",
                    "type": "userMessage",
                    "content": [{"type": "text", "text": "existing"}],
                })],
                ..Turn::default()
            }],
            ..ThreadSummary::default()
        });
        assert!(state.push_pending_steer(
            "thread",
            "turn",
            "native-steer",
            vec![json!({"type": "text", "text": "follow up"})],
            10,
        ));

        state.remove_pending_steer("thread", "native-steer");

        let items = &state.threads["thread"].turns[0].items;
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["id"], "existing-user");
    }

    #[test]
    fn stale_steer_cleanup_removes_every_local_running_marker() {
        let mut state = AppState {
            active_thread_id: Some("thread".into()),
            active_turn_id: Some("turn".into()),
            ..AppState::default()
        };
        state.upsert_thread(ThreadSummary {
            id: "thread".into(),
            status: json!({"type": "active"}),
            turns: vec![Turn {
                id: "turn".into(),
                status: json!("inProgress"),
                ..Turn::default()
            }],
            ..ThreadSummary::default()
        });
        state.turn_progress.insert(
            "thread".into(),
            TurnProgress {
                turn_id: "turn".into(),
                ..TurnProgress::default()
            },
        );
        state.external_running_threads.insert("thread".into());

        state.clear_stale_turn("thread");

        assert!(state.active_turn_id.is_none());
        assert!(!state.turn_progress.contains_key("thread"));
        assert!(!state.external_running_threads.contains("thread"));
        assert_eq!(state.threads["thread"].status["type"], "idle");
        assert_eq!(state.threads["thread"].turns[0].status, "interrupted");
    }

    #[test]
    fn metadata_resume_does_not_erase_loaded_turns() {
        let mut state = AppState::default();
        state.upsert_thread(ThreadSummary {
            id: "thread".into(),
            path: Some(PathBuf::from("/sessions/rollout-thread.jsonl")),
            turns: vec![Turn {
                id: "loaded".into(),
                ..Turn::default()
            }],
            ..ThreadSummary::default()
        });
        state.activate_thread("thread".into());
        state.upsert_thread(ThreadSummary {
            id: "thread".into(),
            name: Some("Resumed task".into()),
            ..ThreadSummary::default()
        });

        assert_eq!(state.active_thread().unwrap().turns[0].id, "loaded");
        assert_eq!(state.active_thread().unwrap().title(), "Resumed task");
        assert_eq!(
            state.active_thread().unwrap().path.clone(),
            Some(PathBuf::from("/sessions/rollout-thread.jsonl"))
        );
    }

    #[test]
    fn streamed_transcript_updates_increment_the_render_revision() {
        let mut state = AppState::default();
        let before = state.transcript_revision;
        state.apply_notification(
            "item/agentMessage/delta",
            &json!({"itemId": "message", "delta": "hello"}),
        );
        assert!(state.transcript_revision > before);
    }

    #[test]
    fn sparse_completion_keeps_progressive_messages_visible() {
        let mut state = AppState::default();
        state.upsert_thread(ThreadSummary {
            id: "thread".into(),
            ..ThreadSummary::default()
        });
        state.activate_thread("thread".into());
        state.apply_notification(
            "turn/started",
            &json!({
                "threadId": "thread",
                "turn": {"id": "turn", "items": [], "status": "inProgress"}
            }),
        );
        state.apply_notification(
            "item/started",
            &json!({
                "threadId": "thread",
                "turnId": "turn",
                "item": {
                    "id": "user",
                    "type": "userMessage",
                    "content": [{"type": "text", "text": "Keep this prompt"}]
                }
            }),
        );
        state.apply_notification(
            "item/started",
            &json!({
                "threadId": "thread",
                "turnId": "turn",
                "item": {"id": "agent", "type": "agentMessage", "text": ""}
            }),
        );
        state.apply_notification(
            "item/agentMessage/delta",
            &json!({"itemId": "agent", "delta": "Keep this answer"}),
        );
        state.apply_notification(
            "item/completed",
            &json!({
                "threadId": "thread",
                "turnId": "turn",
                "item": {"id": "agent", "type": "agentMessage"}
            }),
        );
        state.apply_notification(
            "turn/completed",
            &json!({
                "threadId": "thread",
                "turn": {"id": "turn", "items": [], "status": "completed"}
            }),
        );

        let turn = &state.active_thread().unwrap().turns[0];
        assert_eq!(turn.items.len(), 2);
        assert_eq!(
            turn.items[0]
                .pointer("/content/0/text")
                .and_then(Value::as_str),
            Some("Keep this prompt")
        );
        assert_eq!(
            turn.items[1].get("text").and_then(Value::as_str),
            Some("Keep this answer")
        );
        assert!(!state.streaming_text.contains_key("agent"));
    }

    #[test]
    fn activating_thread_evicts_inactive_transcripts() {
        let mut state = AppState::default();
        state.upsert_thread(ThreadSummary {
            id: "one".into(),
            turns: vec![Turn {
                id: "turn".into(),
                ..Turn::default()
            }],
            ..ThreadSummary::default()
        });
        state.upsert_thread(ThreadSummary {
            id: "two".into(),
            ..ThreadSummary::default()
        });
        state.activate_thread("two".into());
        assert!(state.threads["one"].turns.is_empty());
    }

    #[test]
    fn bounded_text_keeps_valid_utf8_tail() {
        let mut value = "a".repeat(12);
        push_bounded(&mut value, "éé", 5);
        assert!(value.len() <= 5);
        assert!(std::str::from_utf8(value.as_bytes()).is_ok());
    }

    #[test]
    fn transcript_payload_has_a_global_item_cap() {
        let mut state = AppState::default();
        state.upsert_thread(ThreadSummary {
            id: "thread".into(),
            ..ThreadSummary::default()
        });
        let turns = (0..10)
            .map(|turn| Turn {
                id: format!("turn-{turn}"),
                items: (0..100)
                    .map(|item| serde_json::json!({"id": format!("{turn}-{item}")}))
                    .collect(),
                ..Turn::default()
            })
            .collect();
        state.set_thread_turns("thread", turns, None, false);
        let count = state.threads["thread"]
            .turns
            .iter()
            .map(|turn| turn.items.len())
            .sum::<usize>();
        assert_eq!(count, MAX_ITEMS_PER_THREAD);
    }

    #[test]
    fn transcript_payload_caps_activity_without_deleting_chat() {
        let mut state = AppState::default();
        state.upsert_thread(ThreadSummary {
            id: "thread".into(),
            ..ThreadSummary::default()
        });
        let mut items = vec![json!({
            "id": "user",
            "type": "userMessage",
            "content": [{"type": "text", "text": "keep this prompt"}]
        })];
        items.extend((0..500).map(|index| {
            json!({
                "id": format!("command-{index}"),
                "type": "commandExecution"
            })
        }));
        items.push(json!({
            "id": "agent",
            "type": "agentMessage",
            "text": "keep this answer"
        }));

        state.set_thread_turns(
            "thread",
            vec![Turn {
                id: "turn".into(),
                items,
                ..Turn::default()
            }],
            None,
            false,
        );

        let items = &state.threads["thread"].turns[0].items;
        assert_eq!(items.len(), MAX_ITEMS_PER_TURN);
        assert!(items.iter().any(|item| item["id"] == "user"));
        assert!(items.iter().any(|item| item["id"] == "agent"));
    }

    #[test]
    fn live_activity_cap_never_evicts_chat_messages() {
        let mut state = AppState::default();
        state.upsert_thread(ThreadSummary {
            id: "thread".into(),
            ..ThreadSummary::default()
        });
        state.activate_thread("thread".into());
        state.apply_notification(
            "turn/started",
            &json!({
                "threadId": "thread",
                "turn": {"id": "turn", "items": [], "status": "inProgress"}
            }),
        );
        for item in [
            json!({
                "id": "user",
                "type": "userMessage",
                "content": [{"type": "text", "text": "keep this prompt"}]
            }),
            json!({
                "id": "agent",
                "type": "agentMessage",
                "text": "keep this answer"
            }),
        ] {
            state.apply_notification(
                "item/started",
                &json!({"threadId": "thread", "turnId": "turn", "item": item}),
            );
        }
        for index in 0..(MAX_ITEMS_PER_TURN + 40) {
            state.apply_notification(
                "item/started",
                &json!({
                    "threadId": "thread",
                    "turnId": "turn",
                    "item": {
                        "id": format!("command-{index}"),
                        "type": "commandExecution"
                    }
                }),
            );
        }

        let items = &state.active_thread().unwrap().turns[0].items;
        assert_eq!(items.len(), MAX_ITEMS_PER_TURN);
        assert!(items.iter().any(|item| item["id"] == "user"));
        assert!(items.iter().any(|item| item["id"] == "agent"));
    }

    #[test]
    fn older_transcript_pages_remain_loaded_and_deduplicated() {
        let mut state = AppState::default();
        state.upsert_thread(ThreadSummary {
            id: "thread".into(),
            ..ThreadSummary::default()
        });
        let page = |start: i64, end: i64| {
            (start..end)
                .map(|index| Turn {
                    id: format!("turn-{index}"),
                    started_at: Some(index),
                    items: vec![json!({
                        "id": format!("user-{index}"),
                        "type": "userMessage"
                    })],
                    ..Turn::default()
                })
                .collect::<Vec<_>>()
        };

        state.set_thread_turns("thread", page(20, 40), Some("page-2".into()), false);
        state.set_thread_turns("thread", page(0, 20), Some("page-3".into()), true);
        state.set_thread_turns("thread", page(-20, 1), None, true);
        state.set_thread_turns("thread", page(20, 40), Some("page-2".into()), false);

        let turns = &state.threads["thread"].turns;
        assert_eq!(turns.len(), 60);
        assert_eq!(turns.first().map(|turn| turn.id.as_str()), Some("turn--20"));
        assert_eq!(turns.last().map(|turn| turn.id.as_str()), Some("turn-39"));
        assert_eq!(turns.iter().filter(|turn| turn.id == "turn-0").count(), 1);
    }

    #[test]
    fn detailed_turn_page_replaces_summary_and_survives_summary_refresh() {
        let mut state = AppState::default();
        state.upsert_thread(ThreadSummary {
            id: "thread".into(),
            ..ThreadSummary::default()
        });
        state.activate_thread("thread".into());
        let summary = Turn {
            id: "turn".into(),
            items: vec![json!({"id": "answer", "type": "agentMessage"})],
            items_view: "summary".into(),
            ..Turn::default()
        };
        state.set_thread_turns("thread", vec![summary.clone()], None, false);
        state.transcript_detail_loading.insert("thread".into());
        state.merge_thread_turn_details(
            "thread",
            Turn {
                id: "turn".into(),
                items: vec![
                    json!({"id": "command", "type": "commandExecution"}),
                    json!({"id": "answer", "type": "agentMessage"}),
                ],
                items_view: "full".into(),
                ..Turn::default()
            },
            Some("older-detail".into()),
        );

        assert_eq!(state.threads["thread"].turns[0].items.len(), 2);
        assert_eq!(
            state
                .transcript_detail_cursors
                .get("thread")
                .map(String::as_str),
            Some("older-detail")
        );
        assert!(!state.transcript_detail_loading.contains("thread"));

        state.set_thread_turns("thread", vec![summary], None, false);
        assert_eq!(state.threads["thread"].turns[0].items_view, "full");
        assert_eq!(state.threads["thread"].turns[0].items.len(), 2);
    }

    #[test]
    fn markerless_summary_refresh_preserves_recovered_chat() {
        let mut state = AppState::default();
        state.upsert_thread(ThreadSummary {
            id: "thread".into(),
            ..ThreadSummary::default()
        });
        state.activate_thread("thread".into());
        state.set_thread_turns(
            "thread",
            vec![Turn {
                id: "turn".into(),
                items_view: "summary".into(),
                ..Turn::default()
            }],
            None,
            false,
        );
        state.merge_local_chat_turns(
            "thread",
            vec![Turn {
                id: "turn".into(),
                items: vec![
                    json!({"id": "user", "type": "userMessage", "text": "prompt"}),
                    json!({"id": "agent", "type": "agentMessage", "text": "answer"}),
                ],
                items_view: "localRecovery".into(),
                ..Turn::default()
            }],
        );
        assert_eq!(state.threads["thread"].turns[0].items_view, "localRecovery");

        state.set_thread_turns(
            "thread",
            vec![Turn {
                id: "turn".into(),
                status: json!("completed"),
                // The app server can omit itemsView on a compact page.
                items_view: String::new(),
                ..Turn::default()
            }],
            None,
            false,
        );

        let turn = &state.threads["thread"].turns[0];
        assert_eq!(turn.items.len(), 2);
        assert_eq!(turn.items_view, "localRecovery");
        assert_eq!(turn.status, json!("completed"));
    }

    #[test]
    fn local_rollout_chat_fills_stale_projection_without_erasing_tool_items() {
        let mut state = AppState::default();
        state.upsert_thread(ThreadSummary {
            id: "thread".into(),
            turns: vec![Turn {
                id: "turn-1".into(),
                items: vec![json!({
                    "id": "command-1",
                    "type": "commandExecution",
                    "status": "completed"
                })],
                status: json!("inProgress"),
                started_at: Some(10),
                ..Turn::default()
            }],
            ..ThreadSummary::default()
        });
        state.activate_thread("thread".into());

        let added = state.merge_local_chat_turns(
            "thread",
            vec![
                Turn {
                    id: "turn-1".into(),
                    items: vec![
                        json!({"id":"user-1","type":"userMessage","text":"prompt"}),
                        json!({"id":"agent-1","type":"agentMessage","text":"answer"}),
                    ],
                    status: json!("interrupted"),
                    started_at: Some(10),
                    ..Turn::default()
                },
                Turn {
                    id: "turn-2".into(),
                    items: vec![json!({
                        "id": "user-2",
                        "type": "userMessage",
                        "text": "continued"
                    })],
                    status: json!("completed"),
                    started_at: Some(20),
                    completed_at: Some(30),
                    ..Turn::default()
                },
            ],
        );

        assert_eq!(added, (1, 3));
        assert_eq!(state.threads["thread"].turns.len(), 2);
        assert_eq!(
            state.threads["thread"].turns[0].status,
            json!("interrupted")
        );
        assert!(
            state.threads["thread"].turns[0]
                .items
                .iter()
                .any(|item| item["type"] == "commandExecution")
        );
        assert!(
            state.threads["thread"].turns[0]
                .items
                .iter()
                .any(|item| item["text"] == "answer")
        );
    }

    #[test]
    fn local_rollout_chat_deduplicates_projected_messages_with_different_ids() {
        let mut state = AppState::default();
        state.upsert_thread(ThreadSummary {
            id: "thread".into(),
            turns: vec![Turn {
                id: "turn-1".into(),
                items: vec![
                    json!({
                        "id": "projected-user",
                        "type": "userMessage",
                        "content": [{"type":"text", "text":"prompt"}]
                    }),
                    json!({
                        "id": "projected-agent",
                        "type": "agentMessage",
                        "text": "answer"
                    }),
                ],
                items_view: "summary".into(),
                ..Turn::default()
            }],
            ..ThreadSummary::default()
        });
        state.activate_thread("thread".into());

        let added = state.merge_local_chat_turns(
            "thread",
            vec![Turn {
                id: "turn-1".into(),
                items: vec![
                    json!({
                        "id": "recovered-user",
                        "type": "userMessage",
                        "content": [{"type":"text", "text":"prompt"}]
                    }),
                    json!({
                        "id": "recovered-agent",
                        "type": "agentMessage",
                        "text": "answer"
                    }),
                ],
                items_view: "localRecovery".into(),
                ..Turn::default()
            }],
        );

        assert_eq!(added, (0, 0));
        let items = &state.threads["thread"].turns[0].items;
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["id"], "projected-user");
        assert_eq!(items[1]["id"], "projected-agent");
    }

    #[test]
    fn canonical_recovery_restores_user_before_an_already_projected_answer() {
        let mut state = AppState::default();
        state.upsert_thread(ThreadSummary {
            id: "thread".into(),
            turns: vec![Turn {
                id: "turn-1".into(),
                items: vec![json!({
                    "id": "projected-agent",
                    "type": "agentMessage",
                    "text": "answer"
                })],
                items_view: "summary".into(),
                ..Turn::default()
            }],
            ..ThreadSummary::default()
        });

        let added = state.merge_local_chat_turns(
            "thread",
            vec![Turn {
                id: "turn-1".into(),
                items: vec![
                    json!({
                        "id": "recovered-user",
                        "type": "userMessage",
                        "content": [{"type":"text", "text":"prompt", "text_elements":[]}]
                    }),
                    json!({
                        "id": "recovered-agent",
                        "type": "agentMessage",
                        "text": "answer"
                    }),
                ],
                items_view: "localRecovery".into(),
                ..Turn::default()
            }],
        );

        assert_eq!(added, (0, 1));
        let items = &state.threads["thread"].turns[0].items;
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["id"], "recovered-user");
        assert_eq!(items[1]["id"], "projected-agent");
    }

    #[test]
    fn canonical_user_message_replaces_metadata_different_pending_copy() {
        let mut items = vec![json!({
            "id": "native-steer",
            "type": "userMessage",
            "pendingSteer": true,
            "content": [{"type":"text", "text":"prompt", "text_elements":[]}]
        })];
        let canonical = json!({
            "id": "server-user",
            "type": "userMessage",
            "content": [{"type":"input_text", "text":"prompt"}]
        });

        remove_matching_pending_user_item(&mut items, &canonical);

        assert!(items.is_empty());
    }
}
