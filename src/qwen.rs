use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use chrono::Utc;
use serde_json::{Map, Value, json};

use crate::persistence::QwenBuddyPreferences;
use crate::routing::{MODE_QWEN_ASSIST, QwenSavingsEvidence};

const QUALIFIED_MODEL: &str = "local-qwen-delegate:qwen3.8-27b-ud-q6-k-m-text";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticQwenRoute {
    Luna,
    Condenser,
    Sol,
    Gemini,
}

impl AutomaticQwenRoute {
    pub fn label(self) -> &'static str {
        match self {
            Self::Luna => "Qwen direct delegate",
            Self::Condenser => "Qwen condenser",
            Self::Sol => "Qwen coding agent",
            Self::Gemini => "Gemini delegate",
        }
    }

    pub fn from_label(label: &str) -> Option<Self> {
        match label {
            "Qwen direct delegate" | "Qwen Luna" => Some(Self::Luna),
            "Qwen condenser" | "Qwen Condenser" => Some(Self::Condenser),
            "Qwen coding agent" | "Qwen Sol" | "Qwen Sol subagent" => Some(Self::Sol),
            "Gemini delegate" => Some(Self::Gemini),
            _ => None,
        }
    }
}

pub fn automatic_route(
    mode: &str,
    prompt: &str,
    attachment_count: usize,
    preferences: &QwenBuddyPreferences,
    plugin_ready: bool,
    gpu_blocked: bool,
) -> Option<AutomaticQwenRoute> {
    if mode != MODE_QWEN_ASSIST
        || !preferences.routing_enabled
        || !plugin_ready
        || (preferences.gpu_guard && gpu_blocked)
    {
        return None;
    }

    let prompt = prompt.to_ascii_lowercase();
    if contains_any(
        &prompt,
        &[
            "password",
            "secret",
            "api key",
            "access token",
            "authentication",
            "authorization",
            "security",
            "vulnerability",
            "migration",
            "production",
            "deploy",
            "publish",
            "commit",
            "push ",
            "architecture",
            "cross-project",
            "sudo ",
            "install ",
            "installing ",
            "dependency upgrade",
            "package upgrade",
            "delete ",
            "remove all",
        ],
    ) {
        return None;
    }

    let supplied_text_task = contains_any(
        &prompt,
        &[
            "summarize",
            "extract",
            "classify",
            "rewrite",
            "format",
            "description",
            "dialogue",
            "explain this",
        ],
    );
    let large_evidence_task = contains_any(
        &prompt,
        &[
            "large file",
            "large log",
            "long log",
            "transcript",
            "crash log",
        ],
    ) || (prompt.len() > 1_800
        && contains_any(&prompt, &["file", "log", "source", "history"]));
    let coding_action = contains_any(
        &prompt,
        &[
            "fix ",
            "fix this",
            "implement ",
            "fix this file",
            "fix this function",
            "implement this function",
            "debug ",
            "diagnose ",
            "investigate ",
            "refactor ",
            "optimize ",
            "review ",
            "trace ",
            "resolve ",
            "write a test",
            "add a test",
            "one-file",
            "single file",
        ],
    );
    let coding_subject = contains_any(
        &prompt,
        &[
            "code",
            "function",
            "method",
            "class",
            "module",
            "file",
            "bug",
            "test",
            "build",
            "compiler",
            "project",
            "repository",
            "repo",
            "ui",
            "app",
            "service",
            "src/",
        ],
    );
    let bounded_coding_subagent = coding_action
        && coding_subject
        && prompt.len() >= 24
        && prompt.len() <= 3_200
        && attachment_count <= 4;

    if preferences.condenser_enabled && large_evidence_task {
        Some(AutomaticQwenRoute::Condenser)
    } else if preferences.luna_enabled
        && supplied_text_task
        && prompt.len() <= 2_400
        && attachment_count <= 2
    {
        Some(AutomaticQwenRoute::Luna)
    } else if preferences.sol_enabled && bounded_coding_subagent {
        Some(AutomaticQwenRoute::Sol)
    } else {
        None
    }
}

pub fn explicit_qwen_route(prompt: &str) -> AutomaticQwenRoute {
    let prompt = prompt.to_ascii_lowercase();
    let coding = contains_any(
        &prompt,
        &[
            "build ",
            "code",
            "debug",
            "diagnose",
            "fix ",
            "implement",
            "investigate",
            "project",
            "refactor",
            "repository",
            "test",
            "src/",
            "ui ",
        ],
    );
    if coding {
        AutomaticQwenRoute::Sol
    } else {
        AutomaticQwenRoute::Luna
    }
}

pub fn routing_context(
    preferences: &QwenBuddyPreferences,
    route: AutomaticQwenRoute,
) -> Option<String> {
    let instruction = match route {
        AutomaticQwenRoute::Luna if preferences.luna_enabled => {
            "Plan the task in Sol, then use local_qwen_agent through OpenCode with thinking_effort=xhigh and write_access=workspace-write for the bounded supplied-text implementation portion. Give it the smallest existing scope that covers the work. Poll local_qwen_agent_status until completion, then Sol must inspect and verify the result."
        }
        AutomaticQwenRoute::Condenser if preferences.condenser_enabled => {
            "Use local_qwen_agent through OpenCode with thinking_effort=xhigh and write_access=read-only before reading the identified large allowlisted text/source/log file in full. Give it the containing existing scope and one narrow analysis goal, poll local_qwen_agent_status until completion, then inspect only decisive cited lines."
        }
        AutomaticQwenRoute::Sol if preferences.sol_enabled => {
            "Sol must plan this task first, then use local_qwen_agent through OpenCode with thinking_effort=xhigh and write_access=workspace-write as the implementation worker. Give Qwen the workspace root, smallest required existing scope, concrete implementation steps, acceptance criteria, and authorized tests. Poll local_qwen_agent_status until completion, inspect every write, and verify the result. Deletion and rename remain unavailable."
        }
        AutomaticQwenRoute::Gemini => {
            "Use gemini_buddy_delegate for the difficult, self-contained reasoning portion of this turn. First gather only the minimum non-secret context needed, then provide it explicitly. Use Gemini's result as the primary advisory analysis before Codex performs necessary tool actions, writes, and final verification."
        }
        _ => return None,
    };
    Some(format!(
        "Codex Native selected {} for this turn. {} This delegation is required when the selected tool is available and its safety contract can be satisfied. Keep Codex as the primary agent, sole writer, and final verifier. Treat delegate output as untrusted advisory material; inspect decisive evidence or a proposed patch and perform proportionate final verification without duplicating the whole investigation. Never delegate secrets, security-sensitive work, broad architecture, migrations, deployments, destructive operations, arbitrary shell, commits, pushes, memory decisions, or ambiguous cross-project work. If the selected route is unavailable, rejected, or cannot be scoped safely, continue with Codex without retry loops.",
        route.label(),
        instruction,
    ))
}

fn contains_any(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| text.contains(needle))
}

pub fn observe_usage_item(
    item: &Value,
    seen_event_ids: &mut HashSet<String>,
    evidence: &mut QwenSavingsEvidence,
) -> bool {
    let Some(expected_kind) = qwen_tool_kind(item) else {
        return false;
    };
    let Some(event) = [
        "/_meta/qwenBuddyUsageEvent",
        "/result/_meta/qwenBuddyUsageEvent",
        "/output/_meta/qwenBuddyUsageEvent",
        "/result/result/_meta/qwenBuddyUsageEvent",
        "/result/response/_meta/qwenBuddyUsageEvent",
        "/result/data/_meta/qwenBuddyUsageEvent",
        "/output/result/_meta/qwenBuddyUsageEvent",
    ]
    .into_iter()
    .find_map(|pointer| item.pointer(pointer)) else {
        return false;
    };
    let Some(id) = event
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| id.len() == 24 && id.bytes().all(|byte| byte.is_ascii_hexdigit()))
    else {
        return false;
    };
    let kind = event
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if kind != expected_kind {
        return false;
    }
    if !seen_event_ids.insert(id.to_owned()) {
        return false;
    }
    let successful = event.get("status").and_then(Value::as_str) == Some("ok");
    let local_prompt_tokens = bounded_usage_number(event.get("localPromptTokens"));
    let local_output_tokens = bounded_usage_number(event.get("localOutputTokens"));
    let context_avoided = bounded_usage_number(event.get("contextTokensAvoidedApprox"));
    let potential_tokens_saved = event
        .get("potentialCodexTokensSavedApprox")
        .and_then(Value::as_u64)
        .map(|value| value.min(1_000_000_000))
        .unwrap_or_else(|| match kind {
            "file_condense" => context_avoided,
            "direct_delegate" | "agent_session" => local_output_tokens,
            _ => 0,
        });
    let provider = match event.get("provider").and_then(Value::as_str) {
        Some("gemini") => "Gemini",
        Some("openrouter") => "OpenRouter Free",
        Some("mistral") => "Mistral AI",
        _ => "Qwen",
    };
    let (route, model_calls) = match kind {
        "direct_delegate" => (
            provider.to_owned(),
            bounded_usage_number(event.get("modelCalls")).min(u64::from(u32::MAX)) as u32,
        ),
        "file_condense" => (
            format!("{provider} condenser"),
            bounded_usage_number(event.get("modelCalls")).min(u64::from(u32::MAX)) as u32,
        ),
        "agent_session" => (
            format!("{provider} coding agent"),
            bounded_usage_number(event.get("modelCalls")).min(u64::from(u32::MAX)) as u32,
        ),
        _ => return false,
    };
    evidence.record(
        &route,
        successful,
        model_calls,
        local_prompt_tokens,
        local_output_tokens,
        if successful {
            potential_tokens_saved
        } else {
            0
        },
    );
    true
}

fn qwen_tool_kind(item: &Value) -> Option<&'static str> {
    let name = [
        item.get("tool").and_then(Value::as_str),
        item.get("name").and_then(Value::as_str),
        item.pointer("/result/tool").and_then(Value::as_str),
        item.pointer("/result/name").and_then(Value::as_str),
    ]
    .into_iter()
    .flatten()
    .find(|name| name.contains("local_qwen_"))?;
    if name.ends_with("local_qwen_delegate") {
        Some("direct_delegate")
    } else if name.ends_with("local_qwen_condense_file") {
        Some("file_condense")
    } else if name.ends_with("local_qwen_agent") {
        Some("agent_session")
    } else {
        None
    }
}

fn bounded_usage_number(value: Option<&Value>) -> u64 {
    value
        .and_then(Value::as_u64)
        .unwrap_or(0)
        .min(1_000_000_000)
}

pub fn inspect() -> anyhow::Result<Value> {
    let runtime = inspect_runtime();
    let gpu = inspect_gpu();
    let usage = inspect_usage();
    let ready = runtime.get("status").and_then(Value::as_str) == Some("ready");

    Ok(json!({
        "generatedAt": Utc::now(),
        "status": if ready { "ready" } else { "degraded" },
        "qualifiedModel": QUALIFIED_MODEL,
        "runtime": runtime,
        "gpu": gpu,
        "usage": usage,
        "authority": {
            "primaryAgent": "Codex",
            "soleWriter": "Codex",
            "finalVerifier": "Codex",
            "qwenRole": "read-only local delegate",
            "geminiRole": "read-only difficult-task delegate"
        }
    }))
}

fn inspect_runtime() -> Value {
    let Some(audit) = find_qwen_buddy_audit() else {
        return unavailable_runtime("Qwen Buddy plugin audit script was not found");
    };
    let Some(node) = find_program("node") else {
        return unavailable_runtime("Node.js was not found");
    };
    let mut command = Command::new(node);
    command.arg(audit);
    command_json_report(&mut command)
        .unwrap_or_else(|| unavailable_runtime("Qwen Buddy health audit failed"))
}

fn unavailable_runtime(reason: &str) -> Value {
    json!({
        "backend": "llama_cpp",
        "status": "unavailable",
        "modelInstalled": false,
        "modelRunning": false,
        "modelUnloaded": true,
        "wakePlacementReady": false,
        "runtimeContextTokens": 32768,
        "reason": reason,
    })
}

fn find_qwen_buddy_audit() -> Option<PathBuf> {
    env::var_os("QWEN_BUDDY_PLUGIN_PATH")
        .map(PathBuf::from)
        .into_iter()
        .chain(dirs::home_dir().map(|home| home.join("plugins/local-qwen-delegate")))
        .map(|root| root.join("scripts/audit.mjs"))
        .find(|path| path.is_file())
}

fn inspect_gpu() -> Value {
    let workloads = game_engine_workloads();
    let Some(binary) = find_program("nvidia-smi") else {
        return json!({
            "available": false,
            "pressure": "unknown",
            "routingBlocked": !workloads.is_empty(),
            "heavyWorkloads": workloads,
            "processes": [],
        });
    };
    let metrics = {
        let mut command = Command::new(&binary);
        command.args([
            "--query-gpu=memory.total,memory.used,memory.free,utilization.gpu",
            "--format=csv,noheader,nounits",
        ]);
        command_text(&mut command).and_then(|text| parse_gpu_metrics(&text))
    };
    let processes = {
        let mut command = Command::new(&binary);
        command.args([
            "--query-compute-apps=pid,process_name,used_gpu_memory",
            "--format=csv,noheader,nounits",
        ]);
        command_text(&mut command)
            .map(|text| parse_gpu_processes(&text))
            .unwrap_or_default()
    };
    let Some((total, used, free, utilization)) = metrics else {
        return json!({
            "available": false,
            "pressure": "unknown",
            "routingBlocked": !workloads.is_empty(),
            "heavyWorkloads": workloads,
            "processes": processes,
        });
    };
    let (pressure, routing_blocked) =
        classify_gpu_pressure(total, used, free, utilization, !workloads.is_empty());
    json!({
        "available": true,
        "memoryTotalMiB": total,
        "memoryUsedMiB": used,
        "memoryFreeMiB": free,
        "utilizationPercent": utilization,
        "pressure": pressure,
        "routingBlocked": routing_blocked,
        "heavyWorkloads": workloads,
        "processes": processes,
    })
}

fn parse_gpu_metrics(output: &str) -> Option<(u64, u64, u64, u64)> {
    let values = output.lines().next()?.split(',').collect::<Vec<_>>();
    if values.len() != 4 {
        return None;
    }
    Some((
        values[0].trim().parse().ok()?,
        values[1].trim().parse().ok()?,
        values[2].trim().parse().ok()?,
        values[3].trim().parse().ok()?,
    ))
}

fn parse_gpu_processes(output: &str) -> Vec<Value> {
    output
        .lines()
        .filter_map(|line| {
            let mut fields = line.splitn(3, ',').map(str::trim);
            let pid = fields.next()?.parse::<u64>().ok()?;
            let raw_name = fields.next()?;
            let name = Path::new(raw_name)
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or(raw_name)
                .chars()
                .take(80)
                .collect::<String>();
            let memory = fields.next()?.parse::<u64>().ok()?;
            Some(json!({"pid": pid, "name": name, "memoryMiB": memory}))
        })
        .collect()
}

fn classify_gpu_pressure(
    total_mib: u64,
    used_mib: u64,
    free_mib: u64,
    utilization: u64,
    game_engine_running: bool,
) -> (&'static str, bool) {
    if game_engine_running {
        return ("high", true);
    }
    if total_mib == 0 {
        return ("unknown", false);
    }
    let used_percent = used_mib.saturating_mul(100) / total_mib;
    if used_percent >= 82 || utilization >= 90 || free_mib < 1_600 {
        ("high", true)
    } else if used_percent >= 55 || utilization >= 55 || free_mib < 4_500 {
        ("elevated", false)
    } else {
        ("low", false)
    }
}

fn game_engine_workloads() -> Vec<String> {
    let mut found = BTreeSet::new();
    let Ok(entries) = fs::read_dir("/proc") else {
        return Vec::new();
    };
    for entry in entries.flatten() {
        if !entry
            .file_name()
            .to_string_lossy()
            .chars()
            .all(|character| character.is_ascii_digit())
        {
            continue;
        }
        let path = entry.path();
        let comm = fs::read_to_string(path.join("comm")).unwrap_or_default();
        let cmdline = fs::read(path.join("cmdline"))
            .map(|bytes| String::from_utf8_lossy(&bytes).replace('\0', " "))
            .unwrap_or_default();
        let process = format!("{comm} {cmdline}").to_ascii_lowercase();
        for (needle, label) in [
            ("unityhub", "Unity"),
            ("/unity", "Unity"),
            ("godot", "Godot"),
            ("blender", "Blender"),
            ("unrealeditor", "Unreal Engine"),
            ("ue4editor", "Unreal Engine"),
            ("ue5editor", "Unreal Engine"),
        ] {
            if process.contains(needle) {
                found.insert(label.to_owned());
            }
        }
    }
    found.into_iter().collect()
}

fn inspect_usage() -> Value {
    let state_root = env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|home| home.join(".local/state")));
    let plugin_state = state_root
        .as_ref()
        .map(|root| root.join("local-qwen-delegate/usage.json"))
        .and_then(read_json);
    let task_state = state_root
        .as_ref()
        .map(|root| root.join("local-qwen-delegate/codex-task-usage/sessions.json"))
        .and_then(read_json);
    let lifetime = plugin_state
        .as_ref()
        .and_then(|state| state.get("lifetime"))
        .cloned()
        .unwrap_or_else(|| json!({}));
    let (local_tokens, reduction_percent) = usage_summary(&lifetime);
    let task_totals = task_state
        .as_ref()
        .map(aggregate_task_usage)
        .unwrap_or_else(|| json!({}));
    json!({
        "available": plugin_state.is_some(),
        "updatedAt": plugin_state.as_ref().and_then(|state| state.get("updatedAt")),
        "retainedSessions": plugin_state
            .as_ref()
            .and_then(|state| state.get("sessions"))
            .and_then(Value::as_object)
            .map(Map::len)
            .unwrap_or(0),
        "lifetime": lifetime,
        "localTokens": local_tokens,
        "estimatedContextReductionPercent": reduction_percent,
        "codexTaskTotals": task_totals,
        "methodology": {
            "exact": "Prompt/output tokens and call counts are measured for Qwen, Gemini, OpenRouter, and Mistral.",
            "estimated": "GPT tokens avoided count all successful non-GPT prompt and output tokens; condenser context is not double-counted.",
            "excluded": "Unobservable counterfactual GPT reasoning and rejected or failed Buddy calls are excluded."
        }
    })
}

fn usage_summary(lifetime: &Value) -> (u64, u64) {
    let prompt = lifetime
        .get("localPromptTokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output = lifetime
        .get("localOutputTokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let source = lifetime
        .get("sourceTokensApprox")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let avoided = lifetime
        .get("avoidedTokensApprox")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let reduction = avoided.saturating_mul(100).checked_div(source).unwrap_or(0);
    (prompt.saturating_add(output), reduction.min(100))
}

fn aggregate_task_usage(state: &Value) -> Value {
    let keys = [
        "toolUses",
        "successfulUses",
        "rejectedUses",
        "failedUses",
        "lunaUses",
        "condenseUses",
        "solUses",
        "modelCalls",
        "localPromptTokens",
        "localOutputTokens",
        "qwenTokens",
        "geminiTokens",
        "openrouterTokens",
        "mistralTokens",
        "contextTokensAvoidedApprox",
        "potentialCodexTokensSavedApprox",
        "directSavingsUnquantifiedCalls",
        "agentSavingsUnquantifiedCalls",
    ];
    let mut totals = BTreeMap::<&str, u64>::new();
    if let Some(sessions) = state.get("sessions").and_then(Value::as_object) {
        for session in sessions.values() {
            for key in keys {
                let value = session
                    .get("totals")
                    .and_then(|value| value.get(key))
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let total = totals.entry(key).or_default();
                *total = total.saturating_add(value);
            }
        }
    }
    serde_json::to_value(totals).unwrap_or_else(|_| json!({}))
}

fn read_json(path: PathBuf) -> Option<Value> {
    fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
}

fn command_json_report(command: &mut Command) -> Option<Value> {
    let output = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .ok()?;
    serde_json::from_slice(&output.stdout).ok()
}

fn command_text(command: &mut Command) -> Option<String> {
    let output = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if !stdout.is_empty() {
        return Some(stdout);
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    (!stderr.is_empty()).then_some(stderr)
}

fn find_program(name: &str) -> Option<PathBuf> {
    let path = Path::new(name);
    if path.components().count() > 1 && path.is_file() {
        return Some(path.to_owned());
    }
    env::var_os("PATH")
        .into_iter()
        .flat_map(|paths| env::split_paths(&paths).collect::<Vec<_>>())
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gpu_guard_blocks_game_engines_and_saturated_vram() {
        assert_eq!(
            classify_gpu_pressure(16_000, 2_000, 14_000, 5, true),
            ("high", true)
        );
        assert_eq!(
            classify_gpu_pressure(16_000, 15_000, 1_000, 10, false),
            ("high", true)
        );
        assert_eq!(
            classify_gpu_pressure(16_000, 2_000, 14_000, 5, false),
            ("low", false)
        );
    }

    #[test]
    fn usage_summary_is_conservative() {
        let usage = json!({
            "localPromptTokens": 1200,
            "localOutputTokens": 300,
            "sourceTokensApprox": 10_000,
            "avoidedTokensApprox": 7_500
        });
        assert_eq!(usage_summary(&usage), (1_500, 75));
    }

    #[test]
    fn qwen_assist_selects_local_routes_and_respects_gpu_guard() {
        let mut preferences = QwenBuddyPreferences::default();
        assert_eq!(
            automatic_route(
                MODE_QWEN_ASSIST,
                "Summarize this supplied text",
                0,
                &preferences,
                true,
                false,
            ),
            Some(AutomaticQwenRoute::Luna)
        );
        assert!(
            automatic_route(
                crate::routing::MODE_MANUAL,
                "Summarize this supplied text",
                0,
                &preferences,
                true,
                false,
            )
            .is_none()
        );
        assert!(
            automatic_route(
                MODE_QWEN_ASSIST,
                "Summarize this supplied text",
                0,
                &preferences,
                true,
                true,
            )
            .is_none()
        );
        assert!(
            automatic_route(
                MODE_QWEN_ASSIST,
                "Deploy this security migration",
                0,
                &preferences,
                true,
                false,
            )
            .is_none()
        );
        assert_eq!(
            automatic_route(
                MODE_QWEN_ASSIST,
                "Investigate why the task switching bug occurs across the app",
                0,
                &preferences,
                true,
                false,
            ),
            Some(AutomaticQwenRoute::Sol)
        );
        assert!(
            automatic_route(MODE_QWEN_ASSIST, "Fix typo", 0, &preferences, true, false,).is_none(),
            "tiny work should stay with Codex"
        );
        preferences.routing_enabled = false;
        assert!(
            automatic_route(
                MODE_QWEN_ASSIST,
                "Read this large crash log",
                0,
                &preferences,
                true,
                false,
            )
            .is_none()
        );
    }

    #[test]
    fn route_context_names_only_the_selected_qwen_lane() {
        let preferences = QwenBuddyPreferences::default();
        let context =
            routing_context(&preferences, AutomaticQwenRoute::Luna).expect("Luna route context");
        assert!(context.contains("local_qwen_agent through OpenCode"));
        assert!(context.contains("local_qwen_agent_status"));
        assert!(!context.contains("local_qwen_delegate"));
        assert!(context.contains("delegation is required"));

        let sol =
            routing_context(&preferences, AutomaticQwenRoute::Sol).expect("Sol route context");
        assert!(sol.contains("implementation worker"));
        assert!(sol.contains("thinking_effort=xhigh"));
        assert!(sol.contains("write_access=workspace-write"));
        assert!(sol.contains("local_qwen_agent_status"));
        assert!(!sol.contains("local_qwen_agent_decide"));
        assert!(sol.contains("plan this task first"));
        assert!(sol.contains("inspect every write"));
    }

    #[test]
    fn hidden_qwen_usage_events_are_deduplicated_and_conservatively_counted() {
        let event = json!({
            "type": "mcpToolCall",
            "tool": "mcp__local-qwen-delegate__local_qwen_agent",
            "result": {
                "_meta": {
                    "qwenBuddyUsageEvent": {
                        "schemaVersion": 2,
                        "id": "abcdef0123456789abcdef01",
                        "kind": "agent_session",
                        "status": "ok",
                        "modelCalls": 4,
                        "localPromptTokens": 3200,
                        "localOutputTokens": 700,
                        "potentialCodexTokensSavedApprox": 700
                    }
                }
            }
        });
        let mut seen = HashSet::new();
        let mut evidence = QwenSavingsEvidence::default();
        assert!(observe_usage_item(&event, &mut seen, &mut evidence));
        assert!(!observe_usage_item(&event, &mut seen, &mut evidence));
        assert_eq!(evidence.successful_uses, 1);
        assert_eq!(evidence.model_calls, 4);
        assert_eq!(evidence.local_tokens(), 3_900);
        assert_eq!(evidence.potential_tokens_saved, 700);
        assert_eq!(evidence.routes, vec!["Qwen coding agent"]);

        let mismatched = json!({
            "type": "mcpToolCall",
            "tool": "local_qwen_delegate",
            "_meta": {
                "qwenBuddyUsageEvent": {
                    "id": "1234567890abcdef12345678",
                    "kind": "agent_session",
                    "status": "ok",
                    "localOutputTokens": 9999
                }
            }
        });
        assert!(!observe_usage_item(&mismatched, &mut seen, &mut evidence));
        assert_eq!(evidence.potential_tokens_saved, 700);
    }
}
