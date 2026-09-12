use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{Context, anyhow};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::model::{TaskRuntimeSettings, ThreadSummary};

const CHECKPOINT_VERSION: u32 = 1;
const MAX_CHECKPOINTS_PER_THREAD: usize = 8;
const MAX_EVIDENCE_FILES: usize = 32;
const MAX_HASH_FILE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_RECENT_USER_MESSAGES: usize = 4;
const MAX_RECENT_ASSISTANT_MESSAGES: usize = 3;
const MAX_MESSAGE_CHARS: usize = 4_000;
const MAX_TEST_RECORDS: usize = 12;
pub const LEAN_CONTEXT_POLICY_VERSION: u8 = 2;
pub const CONTEXT_METRICS_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LeanContextPreferences {
    /// Version 2 coordinates checkpoints with Codex's model-aware compactor
    /// instead of running a second early automatic compactor.
    #[serde(default)]
    pub policy_version: u8,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default = "default_threshold")]
    pub compact_threshold_percent: u8,
    #[serde(default = "default_minimum_tokens")]
    pub minimum_context_tokens: u64,
    #[serde(default = "default_evidence_budget")]
    pub evidence_token_budget: u32,
    #[serde(default = "default_command_budget")]
    pub command_output_token_budget: u32,
    #[serde(default = "default_condensation_target")]
    pub condensation_target_tokens: u32,
    #[serde(default = "default_true")]
    pub qwen_condense_large_files: bool,
}

impl Default for LeanContextPreferences {
    fn default() -> Self {
        Self {
            policy_version: LEAN_CONTEXT_POLICY_VERSION,
            enabled: true,
            mode: default_mode(),
            compact_threshold_percent: default_threshold(),
            minimum_context_tokens: default_minimum_tokens(),
            evidence_token_budget: default_evidence_budget(),
            command_output_token_budget: default_command_budget(),
            condensation_target_tokens: default_condensation_target(),
            qwen_condense_large_files: true,
        }
    }
}

impl LeanContextPreferences {
    pub fn normalize(&mut self) {
        if self.policy_version < LEAN_CONTEXT_POLICY_VERSION {
            self.policy_version = LEAN_CONTEXT_POLICY_VERSION;
            self.compact_threshold_percent = default_threshold();
        }
        if !matches!(self.mode.as_str(), "observe" | "prompt" | "auto") {
            self.mode = default_mode();
        }
        self.compact_threshold_percent = self.compact_threshold_percent.clamp(70, 92);
        self.minimum_context_tokens = self.minimum_context_tokens.clamp(4_000, 1_000_000);
        self.evidence_token_budget = self.evidence_token_budget.clamp(500, 8_000);
        self.command_output_token_budget = self.command_output_token_budget.clamp(250, 4_000);
        self.condensation_target_tokens = self.condensation_target_tokens.clamp(200, 2_000);
    }
}

fn default_true() -> bool {
    true
}

fn default_mode() -> String {
    "auto".into()
}

fn default_threshold() -> u8 {
    // Codex currently compacts near 95% of the effective model window. This
    // leaves enough time for a bounded background checkpoint without forcing
    // an extra compaction.
    85
}

fn default_minimum_tokens() -> u64 {
    20_000
}

fn default_evidence_budget() -> u32 {
    2_000
}

fn default_command_budget() -> u32 {
    1_000
}

fn default_condensation_target() -> u32 {
    600
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ContextUsageSnapshot {
    /// Tokens currently occupying the model context window. This is not the
    /// thread-lifetime cumulative total when the app-server supplies `last`.
    pub total_tokens: u64,
    pub context_window: u64,
    pub percent: u8,
    /// True only when the app-server supplied the v2 current-window `last`
    /// measurement. A legacy cumulative total is display-only.
    #[serde(default)]
    pub current_window: bool,
}

pub const NO_IMMEDIATE_COMPACTION_REDUCTION: &str =
    "The backend reported no immediate context reduction after compaction.";

pub fn usage_snapshot(value: Option<&Value>) -> ContextUsageSnapshot {
    let Some(value) = value else {
        return ContextUsageSnapshot::default();
    };
    // `total` is cumulative over the life of the thread. Lean Context needs
    // the current model-window measurement in `last`; otherwise a long-running
    // task remains pinned at 100% even immediately after a successful compact.
    let current_tokens = value.pointer("/last/totalTokens").and_then(Value::as_u64);
    let total_tokens = current_tokens
        .or_else(|| value.pointer("/total/totalTokens").and_then(Value::as_u64))
        .unwrap_or(0);
    let context_window = value
        .get("modelContextWindow")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let percent = total_tokens
        .saturating_mul(100)
        .checked_div(context_window)
        .unwrap_or(0)
        .min(100) as u8;
    ContextUsageSnapshot {
        total_tokens,
        context_window,
        percent,
        current_window: current_tokens.is_some(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionGuards {
    pub connection_ready: bool,
    pub thread_running: bool,
    pub pending_approval: bool,
    pub request_inflight: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionDecision {
    Disabled,
    WaitingForUsage,
    BelowThreshold(u8),
    Blocked(&'static str),
    Observe(u8),
    Prompt(u8),
    Checkpoint(u8),
}

pub fn compaction_decision(
    preferences: &LeanContextPreferences,
    usage: ContextUsageSnapshot,
    guards: CompactionGuards,
) -> CompactionDecision {
    if !preferences.enabled {
        return CompactionDecision::Disabled;
    }
    if !usage.current_window || usage.context_window == 0 || usage.total_tokens == 0 {
        return CompactionDecision::WaitingForUsage;
    }
    if usage.total_tokens < preferences.minimum_context_tokens
        || usage.percent < preferences.compact_threshold_percent
    {
        return CompactionDecision::BelowThreshold(usage.percent);
    }
    if !guards.connection_ready {
        return CompactionDecision::Blocked("waiting for the Codex connection");
    }
    if guards.thread_running {
        return CompactionDecision::Blocked("a turn is active");
    }
    if guards.pending_approval {
        return CompactionDecision::Blocked("an approval is pending");
    }
    if guards.request_inflight {
        return CompactionDecision::Blocked("a checkpoint or compaction is already running");
    }
    match preferences.mode.as_str() {
        "observe" => CompactionDecision::Observe(usage.percent),
        "prompt" => CompactionDecision::Prompt(usage.percent),
        _ => CompactionDecision::Checkpoint(usage.percent),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeContextSnapshot {
    pub model: String,
    pub reasoning_effort: Option<String>,
    pub service_tier: Option<String>,
    pub sandbox_policy: String,
    pub approval_policy: String,
}

impl From<&TaskRuntimeSettings> for RuntimeContextSnapshot {
    fn from(value: &TaskRuntimeSettings) -> Self {
        Self {
            model: value.model.clone(),
            reasoning_effort: value.reasoning_effort.clone(),
            service_tier: value.service_tier.clone(),
            sandbox_policy: compact_json(&value.sandbox_policy, 600),
            approval_policy: compact_json(&value.approval_policy, 300),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct EvidenceFingerprint {
    pub path: String,
    pub status: String,
    pub sha256: Option<String>,
    pub bytes: u64,
    pub modified_at: Option<i64>,
    #[serde(default)]
    pub device: Option<u64>,
    #[serde(default)]
    pub inode: Option<u64>,
    #[serde(default)]
    pub modified_seconds: Option<i64>,
    #[serde(default)]
    pub modified_nanoseconds: Option<i64>,
    #[serde(default)]
    pub changed_seconds: Option<i64>,
    #[serde(default)]
    pub changed_nanoseconds: Option<i64>,
    pub unchanged: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ContextCheckpointRequest {
    pub thread_id: String,
    pub title: String,
    pub cwd: PathBuf,
    pub trigger: String,
    pub objective: String,
    pub recent_user_messages: Vec<String>,
    pub recent_assistant_messages: Vec<String>,
    pub tests: Vec<String>,
    pub file_paths: Vec<PathBuf>,
    pub runtime: Option<RuntimeContextSnapshot>,
    pub usage: ContextUsageSnapshot,
    pub source_turn_count: usize,
    #[serde(default)]
    pub context_generation: u64,
    pub previous_evidence: Vec<EvidenceFingerprint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ContextCheckpoint {
    pub version: u32,
    pub id: String,
    pub created_at: i64,
    pub request: ContextCheckpointRequest,
    pub evidence: Vec<EvidenceFingerprint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ContextCheckpointReceipt {
    pub id: String,
    pub thread_id: String,
    pub created_at: i64,
    pub trigger: String,
    pub path: String,
    pub objective: String,
    pub latest_constraint: String,
    pub source_turn_count: usize,
    #[serde(default)]
    pub context_generation: u64,
    pub usage: ContextUsageSnapshot,
    pub evidence: Vec<EvidenceFingerprint>,
    pub test_count: usize,
    #[serde(default)]
    pub elapsed_ms: u64,
    #[serde(default)]
    pub hashed_bytes: u64,
    #[serde(default)]
    pub reused_evidence: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ContextThreadMetrics {
    #[serde(default)]
    pub schema_version: u32,
    #[serde(default)]
    pub checkpoint_count: u64,
    #[serde(default)]
    pub compaction_count: u64,
    #[serde(default)]
    pub observed_tokens_removed: u64,
    #[serde(default)]
    pub last_before_tokens: u64,
    #[serde(default)]
    pub last_after_tokens: u64,
    #[serde(default)]
    pub last_compacted_at: Option<i64>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub context_generation: u64,
    #[serde(default)]
    pub last_checkpoint_generation: Option<u64>,
    #[serde(default)]
    pub last_compaction_id: Option<String>,
    #[serde(default)]
    pub successful_measurements: u64,
    #[serde(default)]
    pub failed_measurements: u64,
}

impl Default for ContextThreadMetrics {
    fn default() -> Self {
        Self {
            schema_version: CONTEXT_METRICS_VERSION,
            checkpoint_count: 0,
            compaction_count: 0,
            observed_tokens_removed: 0,
            last_before_tokens: 0,
            last_after_tokens: 0,
            last_compacted_at: None,
            last_error: None,
            context_generation: 0,
            last_checkpoint_generation: None,
            last_compaction_id: None,
            successful_measurements: 0,
            failed_measurements: 0,
        }
    }
}

impl ContextThreadMetrics {
    pub fn migrate(&mut self) -> bool {
        if self.schema_version >= CONTEXT_METRICS_VERSION {
            return false;
        }
        // Version 1 counted request acceptance as completion and could use
        // cumulative lifetime usage. Those counters are not trustworthy.
        *self = Self::default();
        true
    }
}

pub fn checkpoint_request(
    thread: &ThreadSummary,
    goal: Option<&Value>,
    runtime: Option<&TaskRuntimeSettings>,
    usage: Option<&Value>,
    trigger: &str,
    previous: Option<&ContextCheckpointReceipt>,
    context_generation: u64,
) -> ContextCheckpointRequest {
    let mut users = Vec::new();
    let mut assistants = Vec::new();
    let mut tests = Vec::new();
    let mut paths = BTreeSet::new();

    for turn in thread.turns.iter().rev() {
        for item in turn.items.iter().rev() {
            match item.get("type").and_then(Value::as_str).unwrap_or_default() {
                "userMessage" if users.len() < MAX_RECENT_USER_MESSAGES => {
                    let text = item_message_text(item);
                    if !text.is_empty() {
                        users.push(limit_chars(&text, MAX_MESSAGE_CHARS));
                    }
                }
                "agentMessage" if assistants.len() < MAX_RECENT_ASSISTANT_MESSAGES => {
                    let text = item_message_text(item);
                    if !text.is_empty() {
                        assistants.push(limit_chars(&text, MAX_MESSAGE_CHARS));
                    }
                }
                "fileChange" => collect_file_paths(item, &mut paths),
                "commandExecution" | "command" | "commandExec"
                    if tests.len() < MAX_TEST_RECORDS =>
                {
                    tests.push(command_record(item));
                }
                _ => {}
            }
        }
        if users.len() >= MAX_RECENT_USER_MESSAGES
            && assistants.len() >= MAX_RECENT_ASSISTANT_MESSAGES
            && tests.len() >= MAX_TEST_RECORDS
            && paths.len() >= MAX_EVIDENCE_FILES
        {
            break;
        }
    }
    users.reverse();
    assistants.reverse();
    tests.reverse();

    let objective = goal
        .and_then(|goal| goal.get("objective"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| thread.title())
        .to_owned();

    ContextCheckpointRequest {
        thread_id: thread.id.clone(),
        title: thread.title().to_owned(),
        cwd: PathBuf::from(&thread.cwd),
        trigger: trigger.to_owned(),
        objective: limit_chars(&objective, 2_000),
        recent_user_messages: users,
        recent_assistant_messages: assistants,
        tests,
        file_paths: paths.into_iter().take(MAX_EVIDENCE_FILES).collect(),
        runtime: runtime.map(RuntimeContextSnapshot::from),
        usage: usage_snapshot(usage),
        source_turn_count: thread.turns.len(),
        context_generation,
        previous_evidence: previous
            .map(|value| value.evidence.clone())
            .unwrap_or_default(),
    }
}

pub fn create_checkpoint(
    request: &ContextCheckpointRequest,
) -> anyhow::Result<ContextCheckpointReceipt> {
    let started = Instant::now();
    let created_at = Utc::now().timestamp();
    let evidence = fingerprint_evidence(request);
    let mut digest = Sha256::new();
    digest.update(serde_json::to_vec(request)?);
    digest.update(serde_json::to_vec(&evidence)?);
    digest.update(created_at.to_le_bytes());
    let id = format!("{:x}", digest.finalize())[..16].to_owned();
    let checkpoint = ContextCheckpoint {
        version: CHECKPOINT_VERSION,
        id: id.clone(),
        created_at,
        request: request.clone(),
        evidence: evidence.clone(),
    };

    let directory = checkpoint_directory(&request.thread_id)?;
    fs::create_dir_all(&directory)
        .with_context(|| format!("failed to create {}", directory.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
    }
    let path = directory.join(format!("{created_at}-{id}.json"));
    let temp = directory.join(format!(".{id}.tmp"));
    let bytes = serde_json::to_vec_pretty(&checkpoint)?;
    let mut file =
        File::create(&temp).with_context(|| format!("failed to create {}", temp.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temp, &path).with_context(|| format!("failed to install {}", path.display()))?;
    retain_recent_checkpoints(&directory, MAX_CHECKPOINTS_PER_THREAD)?;

    Ok(ContextCheckpointReceipt {
        id,
        thread_id: request.thread_id.clone(),
        created_at,
        trigger: request.trigger.clone(),
        path: path.to_string_lossy().into_owned(),
        objective: limit_chars(&request.objective, 600),
        latest_constraint: request
            .recent_user_messages
            .last()
            .map(|value| limit_chars(value, 800))
            .unwrap_or_default(),
        source_turn_count: request.source_turn_count,
        context_generation: request.context_generation,
        usage: request.usage,
        hashed_bytes: evidence
            .iter()
            .filter(|item| item.status == "hashed")
            .map(|item| item.bytes)
            .sum(),
        reused_evidence: evidence
            .iter()
            .filter(|item| item.status == "reusedHash")
            .count()
            .try_into()
            .unwrap_or(u32::MAX),
        evidence,
        test_count: request.tests.len(),
        elapsed_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
    })
}

pub fn routing_context(
    preferences: &LeanContextPreferences,
    receipt: Option<&ContextCheckpointReceipt>,
) -> String {
    let mut preferences = preferences.clone();
    preferences.normalize();
    let condenser = if preferences.qwen_condense_large_files {
        format!(
            "For a large safe text/source/log file, prefer the OpenCode local-Qwen worker in read-only mode when available; target {} tokens, require file identity and line citations, then verify decisive lines directly.",
            preferences.condensation_target_tokens
        )
    } else {
        "Use bounded local reads for large files; never load one wholesale.".to_owned()
    };
    let mut text = format!(
        "Lean Context is enabled. Use targeted rg/rg --files and bounded source ranges. Keep ordinary evidence near {} tokens and command output near {} tokens; expand only for a named ambiguity. Reuse unchanged hash-backed evidence. Preserve user constraints, decisions, exact diffs, failures, approvals, task settings, and unresolved blockers. Raw history remains authoritative. Rehydrate exact evidence when uncertain, source hashes changed, cross-file impact is plausible, tests disagree, or risk is high. Codex remains sole writer and final verifier. {condenser}",
        preferences.evidence_token_budget, preferences.command_output_token_budget
    );
    if let Some(receipt) = receipt {
        text.push_str(&format!(
            " Latest verified checkpoint {} covers {} loaded turns at {}% context. Objective: {}.",
            receipt.id, receipt.source_turn_count, receipt.usage.percent, receipt.objective
        ));
        if !receipt.latest_constraint.is_empty() {
            text.push_str(&format!(
                " Latest user constraint: {}.",
                receipt.latest_constraint
            ));
        }
        let unchanged = receipt
            .evidence
            .iter()
            .filter(|item| item.unchanged && item.sha256.is_some())
            .take(8)
            .map(|item| {
                format!(
                    "{}#{}",
                    item.path,
                    item.sha256
                        .as_deref()
                        .unwrap_or_default()
                        .get(..12)
                        .unwrap_or_default()
                )
            })
            .collect::<Vec<_>>();
        if !unchanged.is_empty() {
            text.push_str(&format!(" Unchanged evidence: {}.", unchanged.join(", ")));
        }
    }
    limit_chars(&text, 3_600)
}

fn checkpoint_directory(thread_id: &str) -> anyhow::Result<PathBuf> {
    let root = dirs::state_dir()
        .or_else(dirs::data_local_dir)
        .ok_or_else(|| anyhow!("XDG state directory is unavailable"))?
        .join("codex-native/context");
    let thread_hash = format!("{:x}", Sha256::digest(thread_id.as_bytes()));
    Ok(root.join(&thread_hash[..20]))
}

fn retain_recent_checkpoints(directory: &Path, keep: usize) -> anyhow::Result<()> {
    let mut files = fs::read_dir(directory)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|value| value == "json"))
        .collect::<Vec<_>>();
    files.sort();
    let remove_count = files.len().saturating_sub(keep);
    for path in files.into_iter().take(remove_count) {
        fs::remove_file(&path)
            .with_context(|| format!("failed to remove expired checkpoint {}", path.display()))?;
    }
    Ok(())
}

pub fn delete_thread_checkpoints(thread_id: &str) -> anyhow::Result<bool> {
    let directory = checkpoint_directory(thread_id)?;
    if !directory.exists() {
        return Ok(false);
    }
    fs::remove_dir_all(&directory)
        .with_context(|| format!("failed to remove {}", directory.display()))?;
    Ok(true)
}

fn fingerprint_evidence(request: &ContextCheckpointRequest) -> Vec<EvidenceFingerprint> {
    let canonical_root = request.cwd.canonicalize().ok();
    let previous = request
        .previous_evidence
        .iter()
        .map(|value| (value.path.as_str(), value))
        .collect::<BTreeMap<_, _>>();
    request
        .file_paths
        .iter()
        .take(MAX_EVIDENCE_FILES)
        .map(|path| {
            let resolved = if path.is_absolute() {
                path.clone()
            } else {
                request.cwd.join(path)
            };
            let display = resolved.to_string_lossy().into_owned();
            let mut result = EvidenceFingerprint {
                path: display.clone(),
                status: "missing".into(),
                ..EvidenceFingerprint::default()
            };
            let Ok(metadata) = fs::symlink_metadata(&resolved) else {
                return result;
            };
            result.bytes = metadata.len();
            result.modified_at = metadata
                .modified()
                .ok()
                .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|value| value.as_secs() as i64);
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                result.status = "unsupported".into();
                return result;
            }
            let Ok(canonical) = resolved.canonicalize() else {
                result.status = "unreadable".into();
                return result;
            };
            if canonical_root
                .as_ref()
                .is_none_or(|root| !canonical.starts_with(root))
            {
                result.status = "outsideWorkspace".into();
                return result;
            }
            if sensitive_path(&canonical) {
                result.status = "sensitiveSkipped".into();
                return result;
            }
            if metadata.len() > MAX_HASH_FILE_BYTES {
                result.status = "largeFile".into();
            } else {
                populate_file_identity(&metadata, &mut result);
                if let Some(prior) = previous.get(display.as_str())
                    && prior.sha256.is_some()
                    && same_file_identity(prior, &result)
                {
                    result.status = "reusedHash".into();
                    result.sha256.clone_from(&prior.sha256);
                    result.unchanged = true;
                    return result;
                }
                match sha256_file(&canonical) {
                    Ok(hash) => {
                        let stable = fs::metadata(&canonical)
                            .ok()
                            .is_some_and(|after| file_identity_matches(&result, &after));
                        if stable {
                            result.status = "hashed".into();
                            result.sha256 = Some(hash);
                        } else {
                            result.status = "changedDuringHash".into();
                        }
                    }
                    Err(_) => result.status = "unreadable".into(),
                }
            }
            if let Some(prior) = previous.get(display.as_str()) {
                result.unchanged = result.sha256.is_some()
                    && result.sha256 == prior.sha256
                    && result.bytes == prior.bytes;
            }
            result
        })
        .collect()
}

#[cfg(unix)]
fn populate_file_identity(metadata: &fs::Metadata, result: &mut EvidenceFingerprint) {
    use std::os::unix::fs::MetadataExt;

    result.device = Some(metadata.dev());
    result.inode = Some(metadata.ino());
    result.modified_seconds = Some(metadata.mtime());
    result.modified_nanoseconds = Some(metadata.mtime_nsec());
    result.changed_seconds = Some(metadata.ctime());
    result.changed_nanoseconds = Some(metadata.ctime_nsec());
}

#[cfg(not(unix))]
fn populate_file_identity(_metadata: &fs::Metadata, _result: &mut EvidenceFingerprint) {}

fn same_file_identity(previous: &EvidenceFingerprint, current: &EvidenceFingerprint) -> bool {
    previous.device.is_some()
        && previous.device == current.device
        && previous.inode == current.inode
        && previous.bytes == current.bytes
        && previous.modified_seconds == current.modified_seconds
        && previous.modified_nanoseconds == current.modified_nanoseconds
        && previous.changed_seconds == current.changed_seconds
        && previous.changed_nanoseconds == current.changed_nanoseconds
}

fn file_identity_matches(before: &EvidenceFingerprint, after: &fs::Metadata) -> bool {
    let mut current = EvidenceFingerprint {
        bytes: after.len(),
        ..EvidenceFingerprint::default()
    };
    populate_file_identity(after, &mut current);
    same_file_identity(before, &current)
}

fn sha256_file(path: &Path) -> anyhow::Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn sensitive_path(path: &Path) -> bool {
    let lower = path.to_string_lossy().to_ascii_lowercase();
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    lower.contains("/.git/")
        || name == ".env"
        || name.starts_with(".env.")
        || name.contains("credential")
        || name.contains("secret")
        || name.ends_with(".pem")
        || name.ends_with(".key")
        || name == "id_rsa"
        || name == "id_ed25519"
}

fn collect_file_paths(item: &Value, paths: &mut BTreeSet<PathBuf>) {
    for change in item
        .get("changes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        for key in ["path", "movePath", "move_path"] {
            if let Some(path) = change.get(key).and_then(Value::as_str) {
                paths.insert(PathBuf::from(path));
            }
        }
    }
}

fn item_message_text(item: &Value) -> String {
    if let Some(text) = item.get("text").and_then(Value::as_str) {
        return text.trim().to_owned();
    }
    item.get("content")
        .and_then(Value::as_array)
        .map(|parts| {
            parts
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
        .trim()
        .to_owned()
}

fn command_record(item: &Value) -> String {
    let command = item
        .get("command")
        .or_else(|| item.get("commandLine"))
        .or_else(|| item.get("cmd"))
        .map(|value| match value {
            Value::String(value) => value.clone(),
            Value::Array(values) => values
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(" "),
            _ => compact_json(value, 500),
        })
        .unwrap_or_else(|| "command".into());
    let status = item
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let output = item
        .get("aggregatedOutput")
        .or_else(|| item.get("output"))
        .or_else(|| item.get("stderr"))
        .map(|value| compact_json(value, 800))
        .unwrap_or_default();
    limit_chars(&format!("{status}: {command}\n{output}"), 1_200)
}

fn compact_json(value: &Value, max_chars: usize) -> String {
    let value = value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string());
    limit_chars(&value, max_chars)
}

fn limit_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }
    let mut result = value.chars().take(max_chars).collect::<String>();
    result.push('…');
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Turn;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn defaults_choose_pre_rollover_checkpointing() {
        let preferences = LeanContextPreferences::default();
        assert!(preferences.enabled);
        assert_eq!(preferences.mode, "auto");
        assert_eq!(preferences.compact_threshold_percent, 85);
        assert_eq!(preferences.policy_version, LEAN_CONTEXT_POLICY_VERSION);
    }

    #[test]
    fn decision_blocks_active_work_and_approvals() {
        let preferences = LeanContextPreferences::default();
        let usage = ContextUsageSnapshot {
            total_tokens: 85_000,
            context_window: 100_000,
            percent: 85,
            current_window: true,
        };
        assert_eq!(
            compaction_decision(
                &preferences,
                usage,
                CompactionGuards {
                    connection_ready: true,
                    thread_running: true,
                    pending_approval: false,
                    request_inflight: false,
                }
            ),
            CompactionDecision::Blocked("a turn is active")
        );
        assert_eq!(
            compaction_decision(
                &preferences,
                usage,
                CompactionGuards {
                    connection_ready: true,
                    thread_running: false,
                    pending_approval: true,
                    request_inflight: false,
                }
            ),
            CompactionDecision::Blocked("an approval is pending")
        );
    }

    #[test]
    fn cumulative_only_usage_is_display_only() {
        let value = json!({
            "total": {"totalTokens": 75_000},
            "modelContextWindow": 100_000
        });
        assert_eq!(
            usage_snapshot(Some(&value)),
            ContextUsageSnapshot {
                total_tokens: 75_000,
                context_window: 100_000,
                percent: 75,
                current_window: false,
            }
        );
        assert_eq!(
            compaction_decision(
                &LeanContextPreferences::default(),
                usage_snapshot(Some(&value)),
                CompactionGuards {
                    connection_ready: true,
                    thread_running: false,
                    pending_approval: false,
                    request_inflight: false,
                },
            ),
            CompactionDecision::WaitingForUsage
        );
    }

    #[test]
    fn usage_snapshot_prefers_current_window_over_cumulative_thread_total() {
        let value = json!({
            "total": {"totalTokens": 4_970_297},
            "last": {"totalTokens": 8_511},
            "modelContextWindow": 258_400
        });
        assert_eq!(
            usage_snapshot(Some(&value)),
            ContextUsageSnapshot {
                total_tokens: 8_511,
                context_window: 258_400,
                percent: 3,
                current_window: true,
            }
        );
    }

    #[test]
    fn pre_v2_metrics_are_reset_instead_of_reused() {
        let mut metrics = ContextThreadMetrics {
            schema_version: 1,
            checkpoint_count: 1_418,
            compaction_count: 1_416,
            last_before_tokens: 4_970_297,
            ..ContextThreadMetrics::default()
        };
        assert!(metrics.migrate());
        assert_eq!(metrics, ContextThreadMetrics::default());
        assert!(!metrics.migrate());
    }

    #[test]
    fn pre_v2_policy_moves_to_safe_checkpoint_default() {
        let mut preferences = LeanContextPreferences {
            policy_version: 1,
            compact_threshold_percent: 70,
            ..LeanContextPreferences::default()
        };
        preferences.normalize();
        assert_eq!(preferences.policy_version, LEAN_CONTEXT_POLICY_VERSION);
        assert_eq!(preferences.compact_threshold_percent, 85);
    }

    #[test]
    fn checkpoint_fingerprints_safe_workspace_files() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("source.rs");
        fs::write(&source, "fn main() {}\n").unwrap();
        let request = ContextCheckpointRequest {
            thread_id: "thread".into(),
            title: "Task".into(),
            cwd: directory.path().to_path_buf(),
            trigger: "test".into(),
            objective: "Test hashing".into(),
            recent_user_messages: vec![],
            recent_assistant_messages: vec![],
            tests: vec![],
            file_paths: vec![source.clone()],
            runtime: None,
            usage: ContextUsageSnapshot::default(),
            source_turn_count: 0,
            context_generation: 0,
            previous_evidence: vec![],
        };
        let evidence = fingerprint_evidence(&request);
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].status, "hashed");
        assert_eq!(evidence[0].sha256.as_deref().map(str::len), Some(64));
    }

    #[test]
    fn routing_context_keeps_authority_and_rehydration_guards() {
        let text = routing_context(&LeanContextPreferences::default(), None);
        assert!(text.contains("Codex remains sole writer"));
        assert!(text.contains("Rehydrate exact evidence"));
        assert!(text.chars().count() <= 3_601);
    }

    #[test]
    fn preferences_enforce_hard_budget_ranges() {
        let mut preferences = LeanContextPreferences {
            mode: "unknown".into(),
            compact_threshold_percent: 100,
            minimum_context_tokens: 1,
            evidence_token_budget: 100_000,
            command_output_token_budget: 1,
            condensation_target_tokens: 10_000,
            ..LeanContextPreferences::default()
        };
        preferences.normalize();
        assert_eq!(preferences.mode, "auto");
        assert_eq!(preferences.compact_threshold_percent, 92);
        assert_eq!(preferences.minimum_context_tokens, 4_000);
        assert_eq!(preferences.evidence_token_budget, 8_000);
        assert_eq!(preferences.command_output_token_budget, 250);
        assert_eq!(preferences.condensation_target_tokens, 2_000);
    }

    #[test]
    fn repeated_evidence_is_hash_aware() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("source.rs");
        fs::write(&source, "fn main() {}\n").unwrap();
        let mut request = ContextCheckpointRequest {
            thread_id: "thread".into(),
            title: "Task".into(),
            cwd: directory.path().to_path_buf(),
            trigger: "test".into(),
            objective: "Test hashing".into(),
            recent_user_messages: vec![],
            recent_assistant_messages: vec![],
            tests: vec![],
            file_paths: vec![source],
            runtime: None,
            usage: ContextUsageSnapshot::default(),
            source_turn_count: 0,
            context_generation: 0,
            previous_evidence: vec![],
        };
        let first = fingerprint_evidence(&request);
        request.previous_evidence = first.clone();
        let repeated = fingerprint_evidence(&request);
        assert_eq!(first[0].sha256, repeated[0].sha256);
        assert!(repeated[0].unchanged);
        assert_eq!(repeated[0].status, "reusedHash");
    }

    #[test]
    fn evidence_hashing_skips_sensitive_and_outside_paths() {
        let workspace = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let secret = workspace.path().join(".env");
        let external = outside.path().join("outside.rs");
        fs::write(&secret, "TOKEN=never-hash-this\n").unwrap();
        fs::write(&external, "outside\n").unwrap();
        let request = ContextCheckpointRequest {
            thread_id: "thread".into(),
            title: "Task".into(),
            cwd: workspace.path().to_path_buf(),
            trigger: "test".into(),
            objective: "Test scope".into(),
            recent_user_messages: vec![],
            recent_assistant_messages: vec![],
            tests: vec![],
            file_paths: vec![secret, external],
            runtime: None,
            usage: ContextUsageSnapshot::default(),
            source_turn_count: 0,
            context_generation: 0,
            previous_evidence: vec![],
        };
        let evidence = fingerprint_evidence(&request);
        assert_eq!(evidence[0].status, "sensitiveSkipped");
        assert_eq!(evidence[1].status, "outsideWorkspace");
        assert!(evidence.iter().all(|item| item.sha256.is_none()));
    }

    #[test]
    fn checkpoint_input_is_structurally_bounded() {
        let turns = (0..20)
            .map(|index| Turn {
                id: format!("turn-{index}"),
                items: vec![
                    json!({"type": "userMessage", "text": "u".repeat(6_000)}),
                    json!({"type": "agentMessage", "text": "a".repeat(6_000)}),
                    json!({"type": "commandExecution", "command": "cargo test", "output": "x".repeat(3_000)}),
                ],
                ..Turn::default()
            })
            .collect();
        let thread = ThreadSummary {
            id: "thread".into(),
            name: Some("Bounded task".into()),
            cwd: "/tmp".into(),
            turns,
            ..ThreadSummary::default()
        };
        let request = checkpoint_request(&thread, None, None, None, "test", None, 7);
        assert_eq!(request.context_generation, 7);
        assert_eq!(request.recent_user_messages.len(), MAX_RECENT_USER_MESSAGES);
        assert_eq!(
            request.recent_assistant_messages.len(),
            MAX_RECENT_ASSISTANT_MESSAGES
        );
        assert_eq!(request.tests.len(), MAX_TEST_RECORDS);
        assert!(
            request
                .recent_user_messages
                .iter()
                .all(|message| message.chars().count() <= MAX_MESSAGE_CHARS + 1)
        );
        assert!(
            request
                .tests
                .iter()
                .all(|test| test.chars().count() <= 1_201)
        );
    }
}
