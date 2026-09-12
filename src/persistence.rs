use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::PathBuf,
};

use anyhow::{Context, anyhow};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::context::{
    self, ContextCheckpointReceipt, ContextThreadMetrics, LeanContextPreferences,
};
use crate::model::{ThreadSummary, Turn};
use crate::routing::{self, MODE_MANUAL, TaskRoutingState};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub name: String,
    pub path: PathBuf,
    #[serde(default)]
    pub additional_paths: Vec<PathBuf>,
    #[serde(default)]
    pub remote_host: Option<String>,
}

/// A Codex authentication boundary. The primary profile intentionally keeps
/// using the user's existing Codex home; added profiles receive an isolated
/// private home so their OAuth tokens, remote-control socket, and history do
/// not overlap.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AccountProfile {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub codex_home: Option<PathBuf>,
}

/// A lightweight local index of a cloud task. The originating profile remains
/// the only profile that can resume the corresponding app-server thread.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct SharedTaskRecord {
    pub owner_account_id: String,
    pub thread: ThreadSummary,
}

impl AccountProfile {
    pub const PRIMARY_ID: &'static str = "primary";

    pub fn primary() -> Self {
        Self {
            id: Self::PRIMARY_ID.into(),
            label: "Primary account".into(),
            codex_home: None,
        }
    }

    pub fn additional(number: usize) -> Self {
        let id = format!("account-{}", uuid::Uuid::new_v4().simple());
        let root = dirs::data_local_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join("codex-native/accounts")
            .join(&id)
            .join("codex");
        Self {
            id,
            label: format!("ChatGPT account {number}"),
            codex_home: Some(root),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Preferences {
    #[serde(default = "default_theme")]
    pub theme: String,
    #[serde(default = "default_follow_up")]
    pub follow_up_behavior: String,
    #[serde(default = "default_true")]
    pub prevent_sleep_while_running: bool,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default = "default_effort")]
    pub reasoning_effort: String,
    #[serde(default = "default_service_tier")]
    pub service_tier: String,
    /// Routing policy applied to newly created tasks. Existing tasks without
    /// an explicit entry remain Manual for backward compatibility.
    #[serde(default = "default_routing_mode")]
    pub routing_mode: String,
    #[serde(default = "default_sandbox")]
    pub sandbox: String,
    #[serde(default = "default_approval")]
    pub approval_policy: String,
    #[serde(default)]
    pub codex_binary: Option<PathBuf>,
    #[serde(default = "default_true")]
    pub desktop_notifications: bool,
    #[serde(default = "default_true")]
    pub show_reasoning: bool,
    #[serde(default)]
    pub remote_autostart: bool,
    /// Retained only to migrate profiles from the retired patched iOS router.
    /// Stock Remote Control always preserves the model and effort selected by
    /// the originating Codex client.
    #[serde(default)]
    pub remote_ios_auto_routing: bool,
    #[serde(default)]
    pub browser_command: Option<PathBuf>,
    #[serde(default)]
    pub editor_command: Option<PathBuf>,
    #[serde(default = "default_true")]
    pub resource_monitor: bool,
    #[serde(default)]
    pub qwen_buddy: QwenBuddyPreferences,
    #[serde(default)]
    pub lean_context: LeanContextPreferences,
    #[serde(default)]
    pub macro_executor: MacroExecutorPreferences,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QwenBuddyPreferences {
    #[serde(default)]
    pub routing_enabled: bool,
    #[serde(default = "default_true")]
    pub luna_enabled: bool,
    #[serde(default = "default_true")]
    pub condenser_enabled: bool,
    #[serde(default = "default_true")]
    pub sol_enabled: bool,
    #[serde(default = "default_true")]
    pub gpu_guard: bool,
    #[serde(default = "default_qwen_mode")]
    pub cli_mode: String,
    #[serde(default = "default_qwen_tools")]
    pub cli_tools: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MacroExecutorPreferences {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_macro_parallel")]
    pub max_parallel: u8,
    #[serde(default = "default_macro_memory")]
    pub memory_mib: u32,
    #[serde(default = "default_macro_timeout")]
    pub timeout_seconds: u32,
}

impl Default for MacroExecutorPreferences {
    fn default() -> Self {
        Self {
            enabled: false,
            max_parallel: default_macro_parallel(),
            memory_mib: default_macro_memory(),
            timeout_seconds: default_macro_timeout(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MacroExperimentSample {
    pub recorded_at: i64,
    pub group: String,
    pub turn_succeeded: bool,
    pub elapsed_ms: u64,
    #[serde(default)]
    pub total_tokens: Option<u64>,
    #[serde(default)]
    pub input_tokens: Option<u64>,
    #[serde(default)]
    pub cached_input_tokens: Option<u64>,
    #[serde(default)]
    pub output_tokens: Option<u64>,
    #[serde(default)]
    pub reasoning_output_tokens: Option<u64>,
    pub tool_calls: u32,
    #[serde(default)]
    pub peak_memory_bytes: Option<u64>,
    #[serde(default)]
    pub workload_class: String,
    #[serde(default)]
    pub context_bytes_avoided: u64,
    #[serde(default)]
    pub cache_hits: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct MacroExperimentLedger {
    #[serde(default)]
    pub samples: Vec<MacroExperimentSample>,
}

/// Prompt-free measurements of coordinated Codex compactions. No task IDs,
/// prompts, paths, commands, source, or model output are stored.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LeanExperimentSample {
    pub recorded_at: i64,
    pub group: String,
    pub reduced_context: bool,
    pub before_tokens: u64,
    pub after_tokens: u64,
    pub context_window: u64,
    pub tokens_removed: u64,
    pub checkpoint_elapsed_ms: u64,
    pub checkpoint_hashed_bytes: u64,
    pub checkpoint_reused_evidence: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct LeanExperimentLedger {
    #[serde(default)]
    pub samples: Vec<LeanExperimentSample>,
}

impl LeanExperimentLedger {
    pub fn record(&mut self, sample: LeanExperimentSample) {
        const MAX_SAMPLES: usize = 200;
        self.samples.push(sample);
        if self.samples.len() > MAX_SAMPLES {
            self.samples.drain(..self.samples.len() - MAX_SAMPLES);
        }
    }
}

impl MacroExperimentLedger {
    pub fn record(&mut self, sample: MacroExperimentSample) {
        const MAX_SAMPLES: usize = 200;
        self.samples.push(sample);
        if self.samples.len() > MAX_SAMPLES {
            self.samples.drain(..self.samples.len() - MAX_SAMPLES);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OneTimeResetGuard {
    #[serde(default)]
    pub armed: bool,
    #[serde(default = "default_reset_guard_threshold")]
    pub threshold_percent: u8,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub armed_at: Option<i64>,
    #[serde(default)]
    pub attempted_at: Option<i64>,
    #[serde(default)]
    pub completed_at: Option<i64>,
    #[serde(default)]
    pub outcome: Option<String>,
}

impl Default for OneTimeResetGuard {
    fn default() -> Self {
        Self {
            armed: false,
            threshold_percent: default_reset_guard_threshold(),
            idempotency_key: None,
            armed_at: None,
            attempted_at: None,
            completed_at: None,
            outcome: None,
        }
    }
}

impl Default for QwenBuddyPreferences {
    fn default() -> Self {
        Self {
            routing_enabled: true,
            luna_enabled: true,
            condenser_enabled: true,
            sol_enabled: true,
            gpu_guard: true,
            cli_mode: default_qwen_mode(),
            cli_tools: default_qwen_tools(),
        }
    }
}

impl Default for Preferences {
    fn default() -> Self {
        Self {
            theme: default_theme(),
            follow_up_behavior: default_follow_up(),
            prevent_sleep_while_running: true,
            model: default_model(),
            reasoning_effort: default_effort(),
            service_tier: default_service_tier(),
            routing_mode: default_routing_mode(),
            sandbox: default_sandbox(),
            approval_policy: default_approval(),
            codex_binary: None,
            desktop_notifications: true,
            show_reasoning: true,
            remote_autostart: false,
            remote_ios_auto_routing: false,
            browser_command: None,
            editor_command: None,
            resource_monitor: true,
            qwen_buddy: QwenBuddyPreferences::default(),
            lean_context: LeanContextPreferences::default(),
            macro_executor: MacroExecutorPreferences::default(),
        }
    }
}

fn default_theme() -> String {
    "system".into()
}
fn default_follow_up() -> String {
    "steer".into()
}
fn default_model() -> String {
    String::new()
}
fn default_effort() -> String {
    "high".into()
}
fn default_service_tier() -> String {
    "standard".into()
}
fn default_routing_mode() -> String {
    MODE_MANUAL.into()
}
fn default_sandbox() -> String {
    "workspace-write".into()
}
fn default_approval() -> String {
    "on-request".into()
}
fn default_true() -> bool {
    true
}

fn default_account_profiles() -> Vec<AccountProfile> {
    vec![AccountProfile::primary()]
}

fn default_active_account_id() -> String {
    AccountProfile::PRIMARY_ID.into()
}
fn default_qwen_mode() -> String {
    "sol".into()
}
fn default_qwen_tools() -> String {
    "read".into()
}
fn default_reset_guard_threshold() -> u8 {
    2
}
fn default_macro_parallel() -> u8 {
    3
}
fn default_macro_memory() -> u32 {
    2_048
}
fn default_macro_timeout() -> u32 {
    300
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct StoredState {
    #[serde(default)]
    pub projects: Vec<Project>,
    #[serde(default)]
    pub preferences: Preferences,
    #[serde(default = "default_account_profiles")]
    pub account_profiles: Vec<AccountProfile>,
    #[serde(default = "default_active_account_id")]
    pub active_account_id: String,
    /// Native-owned, account-aware task index. This makes the task list
    /// available while another account profile is selected without copying
    /// OAuth credentials or attempting to share server-side ChatGPT threads.
    #[serde(default)]
    pub shared_task_history: BTreeMap<String, SharedTaskRecord>,
    #[serde(default)]
    pub last_project: Option<PathBuf>,
    #[serde(default)]
    pub pinned_threads: Vec<String>,
    /// Durable explanations for goals that were stopped outside their normal completion path.
    #[serde(default)]
    pub goal_stop_reasons: BTreeMap<String, String>,
    #[serde(default)]
    pub show_archived_threads: bool,
    #[serde(default = "default_thread_sort")]
    pub thread_sort: String,
    #[serde(default)]
    pub last_thread_by_project: BTreeMap<String, String>,
    /// Small durable receipts only. Full checkpoint payloads live under the
    /// private XDG state directory and are written away from the GTK thread.
    #[serde(default)]
    pub context_checkpoints: BTreeMap<String, ContextCheckpointReceipt>,
    #[serde(default)]
    pub context_metrics: BTreeMap<String, ContextThreadMetrics>,
    /// Exact derived payloads consumed by the native client. Keeping one
    /// serialized value prevents native turns from reinjecting equivalent
    /// text after a restart.
    #[serde(default)]
    pub lean_context_default_payload: String,
    #[serde(default)]
    pub lean_context_thread_payloads: BTreeMap<String, String>,
    /// Bounded, prompt-free measurements comparing Codex compactions with and
    /// without a preceding Lean checkpoint.
    #[serde(default)]
    pub lean_experiment: LeanExperimentLedger,
    /// Bounded, prompt-free A/B measurements for normal versus macro-assisted
    /// turns. No thread IDs, prompts, paths, source, commands, or output live
    /// in this ledger.
    #[serde(default)]
    pub macro_experiment: MacroExperimentLedger,
    /// Client-side routing policy and bounded per-turn route receipts. The
    /// app-server remains authoritative for runtime settings shared with iOS.
    #[serde(default)]
    pub task_routing: BTreeMap<String, TaskRoutingState>,
    /// Bounded Native-only turns produced by quota-independent Qwen and Gemini
    /// sessions. The app-server does not reliably accept injected history while
    /// a fresh thread is still initializing its MCP servers.
    #[serde(default)]
    pub buddy_turns: BTreeMap<String, Vec<Turn>>,
    /// Tasks whose continuation belongs to a direct non-GPT Buddy provider,
    /// not to Codex's app-server. Kept separately from the transcript because
    /// `ThreadSummary::native_buddy_only` is intentionally runtime-only.
    #[serde(default)]
    pub native_buddy_threads: BTreeSet<String>,
    /// Measured current-window and cumulative token usage for Native-only
    /// Buddy tasks, keyed by thread ID. Values mirror app-server token usage
    /// so the shared task context meter can render them without special state.
    #[serde(default)]
    pub buddy_token_usage: BTreeMap<String, Value>,
    /// User-authorized, idempotent, one-shot weekly-limit reset guard.
    #[serde(default)]
    pub one_time_reset_guard: OneTimeResetGuard,
}

impl StoredState {
    pub fn record_shared_tasks(
        &mut self,
        owner_account_id: &str,
        threads: impl IntoIterator<Item = ThreadSummary>,
    ) -> bool {
        let mut changed = false;
        for mut thread in threads {
            // List results are summaries. Persisting turns would duplicate the
            // canonical profile-local rollout files and grow this index on
            // every refresh.
            thread.turns.clear();
            thread.native_buddy_only = false;
            let record = SharedTaskRecord {
                owner_account_id: owner_account_id.to_owned(),
                thread,
            };
            let replace = self
                .shared_task_history
                .get(&record.thread.id)
                .is_none_or(|current| current != &record);
            if replace {
                self.shared_task_history
                    .insert(record.thread.id.clone(), record);
                changed = true;
            }
        }
        changed
    }

    pub fn shared_tasks_except(&self, account_id: &str) -> Vec<ThreadSummary> {
        self.shared_task_history
            .values()
            .filter(|record| record.owner_account_id != account_id)
            .map(|record| record.thread.clone())
            .collect()
    }

    pub fn task_owner(&self, thread_id: &str) -> Option<&str> {
        self.shared_task_history
            .get(thread_id)
            .map(|record| record.owner_account_id.as_str())
    }

    pub fn prepare_for_save(&mut self) -> bool {
        let mut changed = false;
        if self.account_profiles.is_empty() {
            self.account_profiles.push(AccountProfile::primary());
            changed = true;
        }
        if !self
            .account_profiles
            .iter()
            .any(|profile| profile.id == AccountProfile::PRIMARY_ID)
        {
            self.account_profiles.insert(0, AccountProfile::primary());
            changed = true;
        }
        if !self
            .account_profiles
            .iter()
            .any(|profile| profile.id == self.active_account_id)
        {
            self.active_account_id = AccountProfile::PRIMARY_ID.into();
            changed = true;
        }
        for turns in self.buddy_turns.values_mut() {
            if turns.len() > 80 {
                turns.drain(..turns.len() - 80);
                changed = true;
            }
        }
        let before_usage = self.buddy_token_usage.len();
        self.buddy_token_usage
            .retain(|thread_id, _| self.buddy_turns.contains_key(thread_id));
        changed |= self.buddy_token_usage.len() != before_usage;
        let before_native_buddy = self.native_buddy_threads.len();
        self.native_buddy_threads
            .retain(|thread_id| self.buddy_turns.contains_key(thread_id));
        changed |= self.native_buddy_threads.len() != before_native_buddy;
        if self.preferences.remote_ios_auto_routing {
            self.preferences.remote_ios_auto_routing = false;
            changed = true;
        }
        let normalized_default = routing::normalize_mode(&self.preferences.routing_mode).to_owned();
        if self.preferences.routing_mode != normalized_default {
            self.preferences.routing_mode = normalized_default;
            changed = true;
        }
        for routing_state in self.task_routing.values_mut() {
            let normalized = routing::normalize_mode(&routing_state.mode).to_owned();
            if routing_state.mode != normalized {
                routing_state.mode = normalized;
                changed = true;
            }
        }
        let before = self.preferences.lean_context.clone();
        self.preferences.lean_context.normalize();
        changed |= before != self.preferences.lean_context;
        for metrics in self.context_metrics.values_mut() {
            changed |= metrics.migrate();
        }

        let default_payload = context::routing_context(&self.preferences.lean_context, None);
        let thread_payloads = self
            .context_checkpoints
            .iter()
            .map(|(thread_id, receipt)| {
                (
                    thread_id.clone(),
                    context::routing_context(&self.preferences.lean_context, Some(receipt)),
                )
            })
            .collect::<BTreeMap<_, _>>();
        if self.lean_context_default_payload != default_payload {
            self.lean_context_default_payload = default_payload;
            changed = true;
        }
        if self.lean_context_thread_payloads != thread_payloads {
            self.lean_context_thread_payloads = thread_payloads;
            changed = true;
        }
        changed
    }

    pub fn lean_context_payload(&self, thread_id: &str) -> Option<&str> {
        self.preferences.lean_context.enabled.then(|| {
            self.lean_context_thread_payloads
                .get(thread_id)
                .map(String::as_str)
                .unwrap_or(self.lean_context_default_payload.as_str())
        })
    }
}

fn default_thread_sort() -> String {
    "updated_at".into()
}

pub fn config_dir() -> anyhow::Result<PathBuf> {
    dirs::config_dir()
        .map(|path| path.join("codex-native"))
        .ok_or_else(|| anyhow!("XDG config directory is unavailable"))
}

pub fn state_path() -> anyhow::Result<PathBuf> {
    Ok(config_dir()?.join("state.json"))
}

pub fn load_state() -> StoredState {
    let mut state = state_path().and_then(read_json).unwrap_or_else(|error| {
        tracing::warn!(%error, "using default application state");
        StoredState::default()
    });
    if state.prepare_for_save()
        && let Err(error) = save_state(&state)
    {
        tracing::warn!(%error, "failed to persist migrated application state");
    }
    state
}

pub fn save_state(state: &StoredState) -> anyhow::Result<()> {
    write_json_atomic(&state_path()?, state)
}

pub fn read_json<T: DeserializeOwned>(path: PathBuf) -> anyhow::Result<T> {
    let bytes = fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("failed to parse {}", path.display()))
}

pub fn write_json_atomic<T: Serialize>(path: &PathBuf, value: &T) -> anyhow::Result<()> {
    let parent = path.parent().context("state path has no parent")?;
    fs::create_dir_all(parent).with_context(|| format!("failed to create {}", parent.display()))?;
    let temp_path = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value)?;
    let mut file = fs::File::create(&temp_path)
        .with_context(|| format!("failed to create {}", temp_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temp_path, path)
        .with_context(|| format!("failed to replace {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preferences_are_backward_compatible() {
        let state: StoredState = serde_json::from_str(r#"{"projects":[]}"#).unwrap();
        assert_eq!(state.preferences.sandbox, "workspace-write");
        assert_eq!(state.preferences.service_tier, "standard");
        assert_eq!(state.preferences.routing_mode, MODE_MANUAL);
        assert!(!state.preferences.remote_ios_auto_routing);
        assert!(state.preferences.qwen_buddy.routing_enabled);
        assert!(state.preferences.prevent_sleep_while_running);
        assert!(state.goal_stop_reasons.is_empty());
        assert!(state.preferences.lean_context.enabled);
        assert_eq!(state.preferences.lean_context.mode, "auto");
        assert!(state.context_checkpoints.is_empty());
        assert!(state.context_metrics.is_empty());
        assert!(!state.preferences.macro_executor.enabled);
        assert_eq!(state.preferences.macro_executor.max_parallel, 3);
        assert_eq!(state.preferences.macro_executor.memory_mib, 2_048);
        assert!(state.macro_experiment.samples.is_empty());
        assert!(state.task_routing.is_empty());
        assert!(state.buddy_turns.is_empty());
        assert!(state.buddy_token_usage.is_empty());
        assert!(state.native_buddy_threads.is_empty());
        assert!(!state.one_time_reset_guard.armed);
        assert_eq!(state.one_time_reset_guard.threshold_percent, 2);
        assert_eq!(state.account_profiles, vec![AccountProfile::primary()]);
        assert_eq!(state.active_account_id, AccountProfile::PRIMARY_ID);
        assert!(state.shared_task_history.is_empty());
    }

    #[test]
    fn additional_account_profiles_have_isolated_codex_homes() {
        let profile = AccountProfile::additional(2);
        assert!(profile.id.starts_with("account-"));
        let home = profile.codex_home.expect("secondary profile has a home");
        assert!(home.ends_with(format!("accounts/{}/codex", profile.id)));
        assert_ne!(profile.id, AccountProfile::PRIMARY_ID);
    }

    #[test]
    fn shared_task_history_keeps_owner_and_summary_without_turns() {
        let mut state = StoredState::default();
        let thread = ThreadSummary {
            id: "thread-a".into(),
            preview: "Plan account switching".into(),
            turns: vec![Turn {
                id: "turn-a".into(),
                ..Turn::default()
            }],
            ..ThreadSummary::default()
        };

        assert!(state.record_shared_tasks(AccountProfile::PRIMARY_ID, [thread]));
        assert_eq!(
            state.task_owner("thread-a"),
            Some(AccountProfile::PRIMARY_ID)
        );
        assert!(state.shared_tasks_except("secondary")[0].turns.is_empty());
        assert!(!state.record_shared_tasks(
            AccountProfile::PRIMARY_ID,
            [ThreadSummary {
                id: "thread-a".into(),
                preview: "Plan account switching".into(),
                ..ThreadSummary::default()
            }]
        ));
    }

    #[test]
    fn buddy_history_is_bounded_across_restarts() {
        let mut state = StoredState::default();
        state.buddy_turns.insert(
            "thread".into(),
            (0..100)
                .map(|index| Turn {
                    id: format!("buddy-{index}"),
                    ..Turn::default()
                })
                .collect(),
        );
        assert!(state.prepare_for_save());
        let turns = &state.buddy_turns["thread"];
        assert_eq!(turns.len(), 80);
        assert_eq!(turns[0].id, "buddy-20");
    }

    #[test]
    fn buddy_usage_is_retained_only_for_saved_buddy_tasks() {
        let mut state = StoredState::default();
        state
            .buddy_turns
            .insert("kept".into(), vec![Turn::default()]);
        state
            .buddy_token_usage
            .insert("kept".into(), serde_json::json!({"provider": "Qwen"}));
        state
            .buddy_token_usage
            .insert("orphaned".into(), serde_json::json!({"provider": "Qwen"}));

        assert!(state.prepare_for_save());
        assert!(state.buddy_token_usage.contains_key("kept"));
        assert!(!state.buddy_token_usage.contains_key("orphaned"));
    }

    #[test]
    fn native_buddy_task_identity_survives_restart_cleanup() {
        let mut state = StoredState::default();
        state
            .buddy_turns
            .insert("keep".into(), vec![Turn::default()]);
        state.native_buddy_threads.insert("keep".into());
        state.native_buddy_threads.insert("orphaned".into());

        assert!(state.prepare_for_save());
        assert!(state.native_buddy_threads.contains("keep"));
        assert!(!state.native_buddy_threads.contains("orphaned"));
    }

    #[test]
    fn retired_remote_router_preference_is_forced_off() {
        let mut migrated: StoredState =
            serde_json::from_str(r#"{"preferences":{"remoteIosAutoRouting":true}}"#).unwrap();
        assert!(migrated.prepare_for_save());
        assert!(!migrated.preferences.remote_ios_auto_routing);
    }

    #[test]
    fn legacy_model_router_profiles_migrate_to_manual() {
        let mut state: StoredState = serde_json::from_str(
            r#"{
                "preferences":{"routingMode":"auto-saver"},
                "taskRouting":{"thread-1":{"mode":"auto-saver"}}
            }"#,
        )
        .unwrap();
        assert!(state.prepare_for_save());
        assert_eq!(state.preferences.routing_mode, routing::MODE_MANUAL);
        assert_eq!(state.task_routing["thread-1"].mode, routing::MODE_MANUAL);
    }

    #[test]
    fn macro_experiment_ledger_is_bounded() {
        let mut ledger = MacroExperimentLedger::default();
        for index in 0..250 {
            ledger.record(MacroExperimentSample {
                recorded_at: index,
                group: "baseline".into(),
                turn_succeeded: true,
                elapsed_ms: 10,
                total_tokens: Some(20),
                input_tokens: Some(10),
                cached_input_tokens: Some(5),
                output_tokens: Some(5),
                reasoning_output_tokens: None,
                tool_calls: 1,
                peak_memory_bytes: None,
                workload_class: "read".into(),
                context_bytes_avoided: 0,
                cache_hits: 0,
            });
        }
        assert_eq!(ledger.samples.len(), 200);
        assert_eq!(ledger.samples[0].recorded_at, 50);
    }

    #[test]
    fn lean_experiment_ledger_is_bounded_and_prompt_free() {
        let mut ledger = LeanExperimentLedger::default();
        for index in 0..250 {
            ledger.record(LeanExperimentSample {
                recorded_at: index,
                group: "checkpointed".into(),
                reduced_context: true,
                before_tokens: 200,
                after_tokens: 20,
                context_window: 300,
                tokens_removed: 180,
                checkpoint_elapsed_ms: 5,
                checkpoint_hashed_bytes: 10,
                checkpoint_reused_evidence: 1,
            });
        }
        assert_eq!(ledger.samples.len(), 200);
        assert_eq!(ledger.samples[0].recorded_at, 50);
    }

    #[test]
    fn prepared_state_exposes_one_exact_payload_across_native_restarts() {
        let mut state = StoredState::default();
        state.context_checkpoints.insert(
            "thread".into(),
            ContextCheckpointReceipt {
                id: "checkpoint".into(),
                thread_id: "thread".into(),
                objective: "Keep native task context stable".into(),
                ..ContextCheckpointReceipt::default()
            },
        );
        assert!(state.prepare_for_save());
        assert_eq!(
            state.lean_context_payload("thread"),
            state
                .lean_context_thread_payloads
                .get("thread")
                .map(String::as_str)
        );
        assert_eq!(
            state.lean_context_payload("other"),
            Some(state.lean_context_default_payload.as_str())
        );
        assert!(
            state
                .lean_context_payload("thread")
                .is_some_and(|payload| payload.contains("checkpoint"))
        );
    }
}
