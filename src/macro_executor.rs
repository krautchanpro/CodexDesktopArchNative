use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    env, fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    path::{Component, Path, PathBuf},
    process::{Command as StdCommand, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow, bail};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::{Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, mpsc},
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use uuid::Uuid;

const SERVER_NAME: &str = "Codex Native Macro Executor";
const TOOL_NAME: &str = "codex_native_macro_run";
const STATS_TOOL_NAME: &str = "codex_native_macro_stats";
const OUTPUT_TOOL_NAME: &str = "codex_native_macro_output";
const MAX_STEPS: usize = 32;
const MAX_FILE_WRITES: usize = 16;
const MAX_WRITE_BYTES: usize = 2 * 1024 * 1024;
const MAX_READ_BYTES: usize = 256 * 1024;
const MAX_STRUCTURED_OUTPUT_BYTES: usize = 256 * 1024;
const MAX_COMMAND_OUTPUT_BYTES: usize = 512 * 1024;
const DEFAULT_OUTPUT_PREVIEW_TOKENS: usize = 2_048;
const MAX_ARTIFACT_READ_BYTES: usize = 256 * 1024;
const MAX_ARTIFACT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_ARTIFACT_FILES: usize = 256;
const ARTIFACT_RETENTION_SECONDS: u64 = 7 * 24 * 60 * 60;
const MAX_CACHE_ENTRIES: usize = 128;
const MAX_TELEMETRY_BYTES: u64 = 1024 * 1024;
const MAX_TELEMETRY_SAMPLES: usize = 200;
const MAX_SEARCH_FILES: usize = 20_000;
const MAX_SEARCH_FILE_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacroServerConfig {
    pub max_parallel: usize,
    pub memory_mib: u32,
    pub timeout_seconds: u64,
    pub tasks_max: u32,
    pub allowed_roots: Vec<PathBuf>,
}

impl Default for MacroServerConfig {
    fn default() -> Self {
        Self {
            max_parallel: 3,
            memory_mib: 2_048,
            timeout_seconds: 300,
            tasks_max: 96,
            allowed_roots: Vec::new(),
        }
    }
}

impl MacroServerConfig {
    pub fn from_args(args: impl Iterator<Item = String>) -> anyhow::Result<Self> {
        let mut config = Self::default();
        let mut args = args.peekable();
        while let Some(argument) = args.next() {
            let value = match argument.as_str() {
                "--max-parallel" | "--memory-mib" | "--timeout-seconds" | "--tasks-max"
                | "--allowed-root" => args
                    .next()
                    .with_context(|| format!("{argument} requires a value"))?,
                unknown => bail!("unknown macro-server argument: {unknown}"),
            };
            match argument.as_str() {
                "--max-parallel" => {
                    config.max_parallel = value
                        .parse::<usize>()
                        .context("invalid --max-parallel value")?
                }
                "--memory-mib" => {
                    config.memory_mib =
                        value.parse::<u32>().context("invalid --memory-mib value")?
                }
                "--timeout-seconds" => {
                    config.timeout_seconds = value
                        .parse::<u64>()
                        .context("invalid --timeout-seconds value")?
                }
                "--tasks-max" => {
                    config.tasks_max = value.parse::<u32>().context("invalid --tasks-max value")?
                }
                "--allowed-root" => {
                    if config.allowed_roots.len() >= 32 {
                        bail!("at most 32 macro workspace roots may be configured");
                    }
                    config.allowed_roots.push(PathBuf::from(value));
                }
                _ => unreachable!(),
            }
        }
        config.max_parallel = config.max_parallel.clamp(1, 4);
        config.memory_mib = config.memory_mib.clamp(512, 8_192);
        config.timeout_seconds = config.timeout_seconds.clamp(30, 1_800);
        config.tasks_max = config.tasks_max.clamp(16, 256);
        let mut allowed_roots = Vec::new();
        for root in config.allowed_roots {
            if !root.is_absolute() {
                bail!("macro allowed roots must be absolute: {}", root.display());
            }
            let canonical = root.canonicalize().with_context(|| {
                format!("macro allowed root is unavailable: {}", root.display())
            })?;
            if !canonical.is_dir() {
                bail!(
                    "macro allowed root is not a directory: {}",
                    canonical.display()
                );
            }
            if !allowed_roots.contains(&canonical) {
                allowed_roots.push(canonical);
            }
        }
        config.allowed_roots = allowed_roots;
        Ok(config)
    }
}

pub fn run_cli(args: impl Iterator<Item = String>) -> anyhow::Result<()> {
    let config = MacroServerConfig::from_args(args)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.max_parallel.saturating_add(1))
        .thread_name("codex-native-macro")
        .build()
        .context("failed to start the macro executor runtime")?;
    runtime.block_on(serve_stdio(config))
}

async fn serve_stdio(config: MacroServerConfig) -> anyhow::Result<()> {
    let coordinator = Arc::new(MacroCoordinator::new(config));
    let (output_tx, mut output_rx) = mpsc::unbounded_channel::<Value>();
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::BufWriter::new(tokio::io::stdout());
        while let Some(message) = output_rx.recv().await {
            let mut bytes = serde_json::to_vec(&message)?;
            bytes.push(b'\n');
            stdout.write_all(&bytes).await?;
            stdout.flush().await?;
        }
        anyhow::Ok(())
    });

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut active = HashMap::<String, JoinHandle<()>>::new();
    while let Some(line) = lines
        .next_line()
        .await
        .context("failed to read MCP input")?
    {
        active.retain(|_, task| !task.is_finished());
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            continue;
        };
        let id = message.get("id").cloned();
        let params = message.get("params").cloned().unwrap_or_else(|| json!({}));
        match method {
            "initialize" => {
                if let Some(id) = id {
                    send_result(
                        &output_tx,
                        id,
                        json!({
                            "protocolVersion": params
                                .get("protocolVersion")
                                .cloned()
                                .unwrap_or_else(|| json!("2025-11-25")),
                            "capabilities": {"tools": {}},
                            "serverInfo": {
                                "name": SERVER_NAME,
                                "version": env!("CARGO_PKG_VERSION")
                            },
                            "instructions": "Use codex_native_macro_run for every eligible workflow with two or more predictable bounded workspace operations. Fall back to normal Codex tools for one action, adaptive exploration, UI/external MCP work, network access, credentials, or privileged/interactive commands. Prefer compact output policies and durable output handles for large results. Use dry_run before uncertain mutations and expected_sha256 for stale-write protection. Independent reads may run together; mutations remain locked. Never bypass approval or sandbox policy."
                        }),
                    );
                }
            }
            "ping" => {
                if let Some(id) = id {
                    send_result(&output_tx, id, json!({}));
                }
            }
            "tools/list" => {
                if let Some(id) = id {
                    send_result(&output_tx, id, json!({"tools": tool_definitions()}));
                }
            }
            "tools/call" => {
                let Some(id) = id else {
                    continue;
                };
                let key = request_key(&id);
                let coordinator = coordinator.clone();
                let output = output_tx.clone();
                let task_id = id.clone();
                let task = tokio::spawn(async move {
                    let result = handle_tool_call(coordinator, &params, output.clone()).await;
                    match result {
                        Ok(result) => send_result(&output, task_id, result),
                        Err(error) => send_error(&output, task_id, -32602, &format!("{error:#}")),
                    }
                });
                active.insert(key, task);
            }
            "notifications/cancelled" | "$/cancelRequest" => {
                let cancelled_id = params
                    .get("requestId")
                    .or_else(|| params.get("id"))
                    .cloned();
                if let Some(cancelled_id) = cancelled_id
                    && let Some(task) = active.remove(&request_key(&cancelled_id))
                {
                    task.abort();
                }
            }
            _ => {
                if let Some(id) = id {
                    send_error(&output_tx, id, -32601, "Method not found");
                }
            }
        }
    }

    for (_, task) in active {
        task.abort();
    }
    drop(output_tx);
    writer.await.context("MCP writer task failed")??;
    Ok(())
}

fn request_key(id: &Value) -> String {
    serde_json::to_string(id).unwrap_or_else(|_| "invalid".into())
}

fn send_result(output: &mpsc::UnboundedSender<Value>, id: Value, result: Value) {
    let _ = output.send(json!({"jsonrpc": "2.0", "id": id, "result": result}));
}

fn send_error(output: &mpsc::UnboundedSender<Value>, id: Value, code: i64, message: &str) {
    let _ = output.send(json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message}
    }));
}

fn tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "name": TOOL_NAME,
            "title": "Run a Bounded Workspace Macro",
            "description": "Execute a validated dependency graph of workspace reads, searches, hash-checked patches, atomic file writes, and sandboxed argv commands in one model-visible call. Required for eligible workflows with two or more predictable bounded workspace operations; use normal tools when work is adaptive or unsupported. Compact previews, durable output handles, conditions, read caching, and dry-run mutation plans reduce context without removing evidence. Commands have no network, host home, credentials, capabilities, or shell interpolation. Normal approval remains in force.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "required": ["workspace_root", "steps"],
                "properties": {
                    "workspace_root": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Absolute canonical project directory already in scope for the current task."
                    },
                    "label": {
                        "type": "string",
                        "maxLength": 120,
                        "description": "Short purpose label shown in this thread result. The local telemetry ledger does not retain it."
                    },
                    "max_parallel": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 4
                    },
                    "stop_on_error": {
                        "type": "boolean",
                        "description": "Defaults true. Failed prerequisites always skip their dependents."
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "Validate the full graph and return its mutation plan without changing files or running mutating commands."
                    },
                    "steps": {
                        "type": "array",
                        "minItems": 2,
                        "maxItems": MAX_STEPS,
                        "items": {
                            "type": "object",
                            "additionalProperties": false,
                            "required": ["id", "action"],
                            "properties": {
                                "id": {
                                    "type": "string",
                                    "pattern": "^[A-Za-z0-9_-]{1,64}$"
                                },
                                "depends_on": {
                                    "type": "array",
                                    "maxItems": MAX_STEPS,
                                    "uniqueItems": true,
                                    "items": {"type": "string"}
                                },
                                "when": {
                                    "type": "object",
                                    "additionalProperties": false,
                                    "required": ["step"],
                                    "properties": {
                                        "step": {"type": "string"},
                                        "status": {
                                            "type": "string",
                                            "enum": ["succeeded", "failed", "skipped"]
                                        },
                                        "exit_code": {"type": "integer"},
                                        "output_contains": {"type": "string", "maxLength": 1024},
                                        "output_not_contains": {"type": "string", "maxLength": 1024}
                                    }
                                },
                                "cache": {
                                    "type": "string",
                                    "enum": ["auto", "off", "use", "refresh"],
                                    "description": "auto caches built-in reads; use also caches explicitly read-only commands; refresh recomputes and replaces a cache entry."
                                },
                                "output": {
                                    "type": "object",
                                    "additionalProperties": false,
                                    "properties": {
                                        "mode": {
                                            "type": "string",
                                            "enum": ["auto", "full", "head_tail", "errors_only", "matches_only", "none"]
                                        },
                                        "max_output_tokens": {
                                            "type": "integer",
                                            "minimum": 64,
                                            "maximum": 16384
                                        },
                                        "head_lines": {"type": "integer", "minimum": 1, "maximum": 2000},
                                        "tail_lines": {"type": "integer", "minimum": 1, "maximum": 2000},
                                        "contains": {"type": "string", "maxLength": 1024},
                                        "save_full_output": {"type": "boolean"}
                                    }
                                },
                                "action": {
                                    "oneOf": [
                                        {
                                            "type": "object",
                                            "additionalProperties": false,
                                            "required": ["kind", "path"],
                                            "properties": {
                                                "kind": {"const": "read_file"},
                                                "path": {"type": "string"},
                                                "start_line": {"type": "integer", "minimum": 1},
                                                "end_line": {"type": "integer", "minimum": 1}
                                            }
                                        },
                                        {
                                            "type": "object",
                                            "additionalProperties": false,
                                            "required": ["kind", "query"],
                                            "properties": {
                                                "kind": {"const": "search"},
                                                "query": {"type": "string", "minLength": 1},
                                                "paths": {
                                                    "type": "array",
                                                    "maxItems": 16,
                                                    "items": {"type": "string"}
                                                },
                                                "case_sensitive": {"type": "boolean"},
                                                "max_results": {
                                                    "type": "integer",
                                                    "minimum": 1,
                                                    "maximum": 500
                                                }
                                            }
                                        },
                                        {
                                            "type": "object",
                                            "additionalProperties": false,
                                            "required": ["kind"],
                                            "properties": {
                                                "kind": {"const": "list_files"},
                                                "paths": {
                                                    "type": "array",
                                                    "maxItems": 16,
                                                    "items": {"type": "string"}
                                                },
                                                "contains": {"type": "string"},
                                                "max_results": {
                                                    "type": "integer",
                                                    "minimum": 1,
                                                    "maximum": 2000
                                                }
                                            }
                                        },
                                        {
                                            "type": "object",
                                            "additionalProperties": false,
                                            "required": ["kind", "files"],
                                            "properties": {
                                                "kind": {"const": "write_files"},
                                                "files": {
                                                    "type": "array",
                                                    "minItems": 1,
                                                    "maxItems": MAX_FILE_WRITES,
                                                    "items": {
                                                        "type": "object",
                                                        "additionalProperties": false,
                                                        "required": ["path", "content"],
                                                        "properties": {
                                                            "path": {"type": "string"},
                                                            "content": {"type": "string"},
                                                            "expected_sha256": {
                                                                "type": "string",
                                                                "description": "Lowercase SHA-256 of the current file, or missing for a new file."
                                                            }
                                                        }
                                                    }
                                                },
                                                "create_parents": {"type": "boolean"}
                                            }
                                        },
                                        {
                                            "type": "object",
                                            "additionalProperties": false,
                                            "required": ["kind", "patch", "expected_files"],
                                            "properties": {
                                                "kind": {"const": "apply_patch"},
                                                "patch": {"type": "string", "minLength": 1},
                                                "expected_files": {
                                                    "type": "array",
                                                    "minItems": 1,
                                                    "maxItems": MAX_FILE_WRITES,
                                                    "items": {
                                                        "type": "object",
                                                        "additionalProperties": false,
                                                        "required": ["path", "sha256"],
                                                        "properties": {
                                                            "path": {"type": "string"},
                                                            "sha256": {
                                                                "type": "string",
                                                                "description": "Lowercase SHA-256 of the current file, or missing for a new file."
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "type": "object",
                                            "additionalProperties": false,
                                            "required": ["kind", "argv"],
                                            "properties": {
                                                "kind": {"const": "run_command"},
                                                "argv": {
                                                    "type": "array",
                                                    "minItems": 1,
                                                    "maxItems": 64,
                                                    "items": {"type": "string", "maxLength": 4096}
                                                },
                                                "cwd": {"type": "string"},
                                                "mutating": {
                                                    "type": "boolean",
                                                    "description": "False mounts the workspace read-only. True mounts it writable and takes the workspace-wide exclusive lock."
                                                },
                                                "timeout_seconds": {
                                                    "type": "integer",
                                                    "minimum": 1,
                                                    "maximum": 1800
                                                },
                                                "max_output_bytes": {
                                                    "type": "integer",
                                                    "minimum": 1024,
                                                    "maximum": MAX_COMMAND_OUTPUT_BYTES
                                                }
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            },
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": true,
                "idempotentHint": false,
                "openWorldHint": false
            }
        }),
        json!({
            "name": STATS_TOOL_NAME,
            "title": "Show Macro Experiment Telemetry",
            "description": "Read aggregate retained macro execution measurements. No prompts, paths, source, command arguments, or output are stored.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": {}
            },
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        }),
        json!({
            "name": OUTPUT_TOOL_NAME,
            "title": "Read Retained Macro Output",
            "description": "Read a bounded line range from an opaque durable macro output handle. Handles contain no workspace path and expire automatically.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "required": ["handle"],
                "properties": {
                    "handle": {"type": "string", "pattern": "^out_[a-f0-9]{32}$"},
                    "start_line": {"type": "integer", "minimum": 1},
                    "end_line": {"type": "integer", "minimum": 1},
                    "max_bytes": {
                        "type": "integer",
                        "minimum": 1024,
                        "maximum": MAX_ARTIFACT_READ_BYTES
                    }
                }
            },
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        }),
    ]
}

async fn handle_tool_call(
    coordinator: Arc<MacroCoordinator>,
    params: &Value,
    output: mpsc::UnboundedSender<Value>,
) -> anyhow::Result<Value> {
    match params.get("name").and_then(Value::as_str) {
        Some(TOOL_NAME) => {
            let request: MacroRequest = serde_json::from_value(
                params
                    .get("arguments")
                    .cloned()
                    .unwrap_or_else(|| json!({})),
            )
            .context("invalid macro arguments")?;
            let progress_token = params.pointer("/_meta/progressToken").cloned();
            let result = coordinator.run(request, progress_token, output).await?;
            let summary = format!(
                "Macro {}: {} succeeded, {} failed, {} skipped in {} ms. Estimated model round trips avoided: {}; context preview avoided about {} bytes; cache hits: {}. Inspect structuredContent and retained output handles for exact evidence.",
                result.status,
                result.succeeded_steps,
                result.failed_steps,
                result.skipped_steps,
                result.elapsed_ms,
                result.estimated_round_trips_avoided,
                result.context_bytes_avoided,
                result.cache_hits,
            );
            Ok(json!({
                "content": [{"type": "text", "text": summary}],
                "structuredContent": result,
                "_meta": {
                    "codexNativeMacro": {
                        "schemaVersion": 2,
                        "canonicalThreadTransport": "official-app-server",
                        "network": "disabled",
                        "sandbox": "bubblewrap+systemd"
                    }
                }
            }))
        }
        Some(STATS_TOOL_NAME) => {
            let stats = coordinator.telemetry_summary().await?;
            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": "Macro telemetry loaded; inspect structuredContent. No prompts, paths, source, commands, or output are retained."
                }],
                "structuredContent": stats
            }))
        }
        Some(OUTPUT_TOOL_NAME) => {
            let request: OutputReadRequest = serde_json::from_value(
                params
                    .get("arguments")
                    .cloned()
                    .unwrap_or_else(|| json!({})),
            )
            .context("invalid output-read arguments")?;
            let result = read_output_artifact(&request)?;
            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": result.get("output").and_then(Value::as_str).unwrap_or("")
                }],
                "structuredContent": result
            }))
        }
        Some(name) => bail!("unknown tool: {name}"),
        None => bail!("tools/call omitted the tool name"),
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MacroRequest {
    workspace_root: String,
    #[serde(default)]
    label: String,
    #[serde(default)]
    max_parallel: Option<usize>,
    #[serde(default = "default_true")]
    stop_on_error: bool,
    #[serde(default)]
    dry_run: bool,
    steps: Vec<MacroStep>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MacroStep {
    id: String,
    #[serde(default)]
    depends_on: Vec<String>,
    #[serde(default)]
    when: Option<StepCondition>,
    #[serde(default)]
    cache: CachePolicy,
    #[serde(default)]
    output: StepOutputOptions,
    action: MacroAction,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct StepCondition {
    step: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    exit_code: Option<i32>,
    #[serde(default)]
    output_contains: String,
    #[serde(default)]
    output_not_contains: String,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CachePolicy {
    #[default]
    Auto,
    Off,
    Use,
    Refresh,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct StepOutputOptions {
    #[serde(default)]
    mode: OutputMode,
    #[serde(default)]
    max_output_tokens: Option<usize>,
    #[serde(default)]
    head_lines: Option<usize>,
    #[serde(default)]
    tail_lines: Option<usize>,
    #[serde(default)]
    contains: String,
    #[serde(default)]
    save_full_output: bool,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum OutputMode {
    #[default]
    Auto,
    Full,
    HeadTail,
    ErrorsOnly,
    MatchesOnly,
    None,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum MacroAction {
    ReadFile {
        path: String,
        #[serde(default)]
        start_line: Option<usize>,
        #[serde(default)]
        end_line: Option<usize>,
    },
    Search {
        query: String,
        #[serde(default)]
        paths: Vec<String>,
        #[serde(default)]
        case_sensitive: bool,
        #[serde(default)]
        max_results: Option<usize>,
    },
    ListFiles {
        #[serde(default)]
        paths: Vec<String>,
        #[serde(default)]
        contains: String,
        #[serde(default)]
        max_results: Option<usize>,
    },
    WriteFiles {
        files: Vec<FileWrite>,
        #[serde(default)]
        create_parents: bool,
    },
    ApplyPatch {
        patch: String,
        expected_files: Vec<ExpectedFileHash>,
    },
    RunCommand {
        argv: Vec<String>,
        #[serde(default)]
        cwd: String,
        #[serde(default)]
        mutating: bool,
        #[serde(default)]
        timeout_seconds: Option<u64>,
        #[serde(default)]
        max_output_bytes: Option<usize>,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FileWrite {
    path: String,
    content: String,
    #[serde(default)]
    expected_sha256: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ExpectedFileHash {
    path: String,
    sha256: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputReadRequest {
    handle: String,
    #[serde(default)]
    start_line: Option<usize>,
    #[serde(default)]
    end_line: Option<usize>,
    #[serde(default)]
    max_bytes: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct MacroResult {
    invocation_id: String,
    status: String,
    label: String,
    started_at: String,
    elapsed_ms: u64,
    step_count: usize,
    succeeded_steps: usize,
    failed_steps: usize,
    skipped_steps: usize,
    max_parallel: usize,
    estimated_round_trips_avoided: usize,
    peak_memory_bytes: Option<u64>,
    output_bytes: u64,
    preview_bytes: u64,
    context_bytes_avoided: u64,
    output_truncated: bool,
    cache_hits: usize,
    artifact_count: usize,
    dry_run: bool,
    workload_class: String,
    mutation_plan: Vec<MutationPlanEntry>,
    steps: Vec<StepResult>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct StepResult {
    id: String,
    status: String,
    elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit_code: Option<i32>,
    output_bytes: u64,
    preview_bytes: u64,
    output_truncated: bool,
    cache_hit: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_handle: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    peak_memory_bytes: Option<u64>,
    #[serde(skip)]
    raw_output: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct MutationPlanEntry {
    step: String,
    action: String,
    targets: Vec<String>,
    lock: String,
    stale_write_protected: bool,
}

impl StepResult {
    fn skipped(id: String, reason: impl Into<String>) -> Self {
        Self {
            id,
            status: "skipped".into(),
            elapsed_ms: 0,
            output: None,
            error: Some(reason.into()),
            exit_code: None,
            output_bytes: 0,
            preview_bytes: 0,
            output_truncated: false,
            cache_hit: false,
            output_handle: None,
            peak_memory_bytes: None,
            raw_output: None,
        }
    }

    fn failed(id: String, elapsed: Duration, error: impl Into<String>) -> Self {
        Self {
            id,
            status: "failed".into(),
            elapsed_ms: duration_millis(elapsed),
            output: None,
            error: Some(error.into()),
            exit_code: None,
            output_bytes: 0,
            preview_bytes: 0,
            output_truncated: false,
            cache_hit: false,
            output_handle: None,
            peak_memory_bytes: None,
            raw_output: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TelemetryRecord {
    schema_version: u8,
    invocation_id: String,
    timestamp: String,
    status: String,
    elapsed_ms: u64,
    step_count: usize,
    succeeded_steps: usize,
    failed_steps: usize,
    skipped_steps: usize,
    max_parallel: usize,
    estimated_round_trips_avoided: usize,
    peak_memory_bytes: Option<u64>,
    output_bytes: u64,
    #[serde(default)]
    preview_bytes: u64,
    #[serde(default)]
    context_bytes_avoided: u64,
    output_truncated: bool,
    #[serde(default)]
    cache_hits: usize,
    #[serde(default)]
    artifact_count: usize,
    #[serde(default)]
    dry_run: bool,
    #[serde(default)]
    workload_class: String,
}

#[derive(Debug, Clone)]
struct CachedStepResult {
    result: StepResult,
}

#[derive(Debug, Default)]
struct ReadCache {
    entries: HashMap<String, CachedStepResult>,
    order: VecDeque<String>,
}

impl ReadCache {
    fn get(&mut self, key: &str) -> Option<StepResult> {
        let result = self.entries.get(key)?.result.clone();
        self.order.retain(|candidate| candidate != key);
        self.order.push_back(key.to_owned());
        Some(result)
    }

    fn insert(&mut self, key: String, result: StepResult) {
        self.entries
            .insert(key.clone(), CachedStepResult { result });
        self.order.retain(|candidate| candidate != &key);
        self.order.push_back(key);
        while self.entries.len() > MAX_CACHE_ENTRIES {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
    }
}

#[derive(Debug, Default)]
struct LockManager {
    workspaces: Mutex<HashMap<PathBuf, Arc<WorkspaceLocks>>>,
}

impl LockManager {
    async fn workspace(&self, path: &Path) -> Arc<WorkspaceLocks> {
        let mut workspaces = self.workspaces.lock().await;
        workspaces
            .entry(path.to_path_buf())
            .or_insert_with(|| Arc::new(WorkspaceLocks::default()))
            .clone()
    }
}

#[derive(Debug, Default)]
struct WorkspaceLocks {
    global: Arc<RwLock<()>>,
    files: Mutex<HashMap<PathBuf, Arc<RwLock<()>>>>,
}

impl WorkspaceLocks {
    async fn read_workspace(&self) -> OwnedRwLockReadGuard<()> {
        self.global.clone().read_owned().await
    }

    async fn write_workspace(&self) -> OwnedRwLockWriteGuard<()> {
        self.global.clone().write_owned().await
    }

    async fn read_files(&self, paths: &[PathBuf]) -> Vec<OwnedRwLockReadGuard<()>> {
        let locks = self.file_locks(paths).await;
        let mut guards = Vec::with_capacity(locks.len());
        for lock in locks {
            guards.push(lock.read_owned().await);
        }
        guards
    }

    async fn write_files(&self, paths: &[PathBuf]) -> Vec<OwnedRwLockWriteGuard<()>> {
        let locks = self.file_locks(paths).await;
        let mut guards = Vec::with_capacity(locks.len());
        for lock in locks {
            guards.push(lock.write_owned().await);
        }
        guards
    }

    async fn file_locks(&self, paths: &[PathBuf]) -> Vec<Arc<RwLock<()>>> {
        let mut paths = paths.to_vec();
        paths.sort();
        paths.dedup();
        let mut files = self.files.lock().await;
        if files.len() > 2_048 {
            files.retain(|_, lock| Arc::strong_count(lock) > 1);
        }
        paths
            .into_iter()
            .map(|path| {
                files
                    .entry(path)
                    .or_insert_with(|| Arc::new(RwLock::new(())))
                    .clone()
            })
            .collect()
    }
}

#[derive(Debug)]
struct MacroCoordinator {
    config: MacroServerConfig,
    locks: LockManager,
    read_cache: Arc<Mutex<ReadCache>>,
    telemetry_lock: Mutex<()>,
}

impl MacroCoordinator {
    fn new(config: MacroServerConfig) -> Self {
        Self {
            config,
            locks: LockManager::default(),
            read_cache: Arc::new(Mutex::new(ReadCache::default())),
            telemetry_lock: Mutex::new(()),
        }
    }

    async fn run(
        &self,
        request: MacroRequest,
        progress_token: Option<Value>,
        output: mpsc::UnboundedSender<Value>,
    ) -> anyhow::Result<MacroResult> {
        validate_request(&request)?;
        let (workspace, lock_root) =
            canonical_workspace(&request.workspace_root, &self.config.allowed_roots)?;
        let max_parallel = request
            .max_parallel
            .unwrap_or(self.config.max_parallel)
            .clamp(1, self.config.max_parallel);
        let invocation_id = Uuid::new_v4().simple().to_string();
        let started_wall = Utc::now();
        let started = Instant::now();
        cleanup_output_artifacts();
        let mutation_plan = build_mutation_plan(&request)?;
        if !mutation_plan.is_empty() {
            progress(
                &output,
                progress_token.as_ref(),
                0,
                request.steps.len(),
                &format_mutation_plan(&mutation_plan, request.dry_run),
            );
        }
        let workspace_locks = self.locks.workspace(&lock_root).await;
        let mut step_states = request
            .steps
            .iter()
            .cloned()
            .map(|step| (step.id.clone(), (step, RuntimeStepState::Pending)))
            .collect::<BTreeMap<_, _>>();
        let mut join_set = JoinSet::<StepResult>::new();
        let mut running = 0_usize;
        let mut finished = Vec::<StepResult>::new();
        let mut stop_scheduling = false;

        loop {
            mark_failed_dependents(&mut step_states);
            mark_unmet_conditions(&mut step_states, &finished);
            if !stop_scheduling {
                let runnable = step_states
                    .iter()
                    .filter(|(_, (step, state))| {
                        matches!(state, RuntimeStepState::Pending)
                            && step
                                .depends_on
                                .iter()
                                .all(|dependency| dependency_ready(step, dependency, &step_states))
                    })
                    .map(|(id, _)| id.clone())
                    .take(max_parallel.saturating_sub(running))
                    .collect::<Vec<_>>();
                for id in runnable {
                    let Some((step, state)) = step_states.get_mut(&id) else {
                        continue;
                    };
                    *state = RuntimeStepState::Running;
                    running += 1;
                    progress(
                        &output,
                        progress_token.as_ref(),
                        finished.len(),
                        request.steps.len(),
                        &format!("Starting {} ({})", step.id, action_label(&step.action)),
                    );
                    let step = step.clone();
                    let workspace = workspace.clone();
                    let workspace_locks = workspace_locks.clone();
                    let config = self.config.clone();
                    let cache = self.read_cache.clone();
                    let invocation_id = invocation_id.clone();
                    let dry_run = request.dry_run;
                    join_set.spawn(async move {
                        execute_step(
                            step,
                            workspace,
                            workspace_locks,
                            config,
                            cache,
                            &invocation_id,
                            dry_run,
                        )
                        .await
                    });
                }
            }

            if running == 0 {
                break;
            }
            let Some(joined) = join_set.join_next().await else {
                break;
            };
            running = running.saturating_sub(1);
            let result = match joined {
                Ok(result) => result,
                Err(error) if error.is_cancelled() => {
                    continue;
                }
                Err(error) => StepResult::failed(
                    "unknown".into(),
                    Duration::ZERO,
                    format!("macro worker failed: {error}"),
                ),
            };
            let succeeded = result.status == "succeeded";
            if let Some((_, state)) = step_states.get_mut(&result.id) {
                *state = if succeeded {
                    RuntimeStepState::Succeeded
                } else {
                    RuntimeStepState::Failed
                };
            }
            progress(
                &output,
                progress_token.as_ref(),
                finished.len() + 1,
                request.steps.len(),
                &format!("{} {}", result.id, result.status),
            );
            if !succeeded && request.stop_on_error {
                stop_scheduling = true;
                join_set.abort_all();
            }
            finished.push(result);
        }

        for (id, (_, state)) in &step_states {
            if matches!(state, RuntimeStepState::Pending | RuntimeStepState::Running) {
                finished.push(StepResult::skipped(
                    id.clone(),
                    if stop_scheduling {
                        "not started because stop_on_error halted the macro"
                    } else {
                        "not started because a prerequisite failed"
                    },
                ));
            } else if let RuntimeStepState::Skipped(reason) = state {
                finished.push(StepResult::skipped(id.clone(), reason.clone()));
            }
        }
        finished.sort_by_key(|result| {
            request
                .steps
                .iter()
                .position(|step| step.id == result.id)
                .unwrap_or(usize::MAX)
        });

        let succeeded_steps = finished
            .iter()
            .filter(|result| result.status == "succeeded")
            .count();
        let failed_steps = finished
            .iter()
            .filter(|result| result.status == "failed")
            .count();
        let skipped_steps = finished
            .iter()
            .filter(|result| result.status == "skipped")
            .count();
        let output_bytes = finished.iter().map(|step| step.output_bytes).sum::<u64>();
        let preview_bytes = finished.iter().map(|step| step.preview_bytes).sum::<u64>();
        let context_bytes_avoided = output_bytes.saturating_sub(preview_bytes);
        let output_truncated = finished.iter().any(|step| step.output_truncated);
        let cache_hits = finished.iter().filter(|step| step.cache_hit).count();
        let artifact_count = finished
            .iter()
            .filter(|step| step.output_handle.is_some())
            .count();
        let peak_memory_bytes = finished
            .iter()
            .filter_map(|step| step.peak_memory_bytes)
            .max();
        let status = if failed_steps > 0 {
            "failed"
        } else if skipped_steps > 0 {
            "partial"
        } else {
            "completed"
        };
        let result = MacroResult {
            invocation_id: invocation_id.clone(),
            status: status.into(),
            label: compact_label(&request.label),
            started_at: started_wall.to_rfc3339(),
            elapsed_ms: duration_millis(started.elapsed()),
            step_count: request.steps.len(),
            succeeded_steps,
            failed_steps,
            skipped_steps,
            max_parallel,
            estimated_round_trips_avoided: request.steps.len().saturating_sub(1),
            peak_memory_bytes,
            output_bytes,
            preview_bytes,
            context_bytes_avoided,
            output_truncated,
            cache_hits,
            artifact_count,
            dry_run: request.dry_run,
            workload_class: workload_class(&request.steps),
            mutation_plan,
            steps: finished,
        };
        if let Err(error) = self.record_telemetry(&result).await {
            eprintln!("codex-native macro telemetry unavailable: {error:#}");
        }
        Ok(result)
    }

    async fn record_telemetry(&self, result: &MacroResult) -> anyhow::Result<()> {
        let _guard = self.telemetry_lock.lock().await;
        let path = telemetry_path()?;
        if path
            .metadata()
            .is_ok_and(|metadata| metadata.len() >= MAX_TELEMETRY_BYTES)
        {
            let previous = path.with_extension("jsonl.previous");
            if previous.is_file() {
                fs::remove_file(&previous)
                    .with_context(|| format!("failed to rotate {}", previous.display()))?;
            }
            fs::rename(&path, &previous)
                .with_context(|| format!("failed to rotate {}", path.display()))?;
        }
        let record = TelemetryRecord {
            schema_version: 2,
            invocation_id: result.invocation_id.clone(),
            timestamp: Utc::now().to_rfc3339(),
            status: result.status.clone(),
            elapsed_ms: result.elapsed_ms,
            step_count: result.step_count,
            succeeded_steps: result.succeeded_steps,
            failed_steps: result.failed_steps,
            skipped_steps: result.skipped_steps,
            max_parallel: result.max_parallel,
            estimated_round_trips_avoided: result.estimated_round_trips_avoided,
            peak_memory_bytes: result.peak_memory_bytes,
            output_bytes: result.output_bytes,
            preview_bytes: result.preview_bytes,
            context_bytes_avoided: result.context_bytes_avoided,
            output_truncated: result.output_truncated,
            cache_hits: result.cache_hits,
            artifact_count: result.artifact_count,
            dry_run: result.dry_run,
            workload_class: result.workload_class.clone(),
        };
        append_private_json_line(&path, &record)
    }

    async fn telemetry_summary(&self) -> anyhow::Result<Value> {
        let _guard = self.telemetry_lock.lock().await;
        let path = telemetry_path()?;
        let records = read_telemetry(&path)?;
        let completed = records
            .iter()
            .filter(|record| record.status == "completed")
            .count();
        let total_elapsed_ms = records.iter().map(|record| record.elapsed_ms).sum::<u64>();
        let total_steps = records
            .iter()
            .map(|record| record.step_count)
            .sum::<usize>();
        let rounds_avoided = records
            .iter()
            .map(|record| record.estimated_round_trips_avoided)
            .sum::<usize>();
        let peak_memory_bytes = records
            .iter()
            .filter_map(|record| record.peak_memory_bytes)
            .max();
        let total_output_bytes = records
            .iter()
            .map(|record| record.output_bytes)
            .sum::<u64>();
        let total_preview_bytes = records
            .iter()
            .map(|record| record.preview_bytes)
            .sum::<u64>();
        let context_bytes_avoided = records
            .iter()
            .map(|record| record.context_bytes_avoided)
            .sum::<u64>();
        let cache_hits = records
            .iter()
            .map(|record| record.cache_hits)
            .sum::<usize>();
        let artifact_count = records
            .iter()
            .map(|record| record.artifact_count)
            .sum::<usize>();
        let elapsed_samples = records
            .iter()
            .map(|record| record.elapsed_ms)
            .collect::<Vec<_>>();
        Ok(json!({
            "schemaVersion": 2,
            "sampleCount": records.len(),
            "completedCount": completed,
            "completionRate": if records.is_empty() {
                0.0
            } else {
                completed as f64 / records.len() as f64
            },
            "totalSteps": total_steps,
            "estimatedRoundTripsAvoided": rounds_avoided,
            "averageElapsedMs": if records.is_empty() {
                0
            } else {
                total_elapsed_ms / records.len() as u64
            },
            "medianElapsedMs": percentile(&elapsed_samples, 50),
            "p95ElapsedMs": percentile(&elapsed_samples, 95),
            "peakMemoryBytes": peak_memory_bytes,
            "totalOutputBytes": total_output_bytes,
            "totalPreviewBytes": total_preview_bytes,
            "estimatedContextBytesAvoided": context_bytes_avoided,
            "estimatedContextTokensAvoided": context_bytes_avoided / 4,
            "cacheHits": cache_hits,
            "artifactCount": artifact_count,
            "retention": MAX_TELEMETRY_SAMPLES,
            "privacy": "No prompts, paths, source, command arguments, or command output are retained.",
            "comparison": "Compare these macro samples with Codex Native's per-turn app-server token, elapsed-time, and outcome telemetry before enabling the feature by default."
        }))
    }
}

#[derive(Debug, Clone)]
enum RuntimeStepState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Skipped(String),
}

fn mark_failed_dependents(states: &mut BTreeMap<String, (MacroStep, RuntimeStepState)>) {
    loop {
        let failed = states
            .iter()
            .filter(|(_, (_, state))| {
                matches!(
                    state,
                    RuntimeStepState::Failed | RuntimeStepState::Skipped(_)
                )
            })
            .map(|(id, _)| id.clone())
            .collect::<HashSet<_>>();
        let skipped = states
            .iter()
            .filter(|(_, (step, state))| {
                matches!(state, RuntimeStepState::Pending)
                    && step.depends_on.iter().any(|dependency| {
                        failed.contains(dependency)
                            && step
                                .when
                                .as_ref()
                                .is_none_or(|condition| condition.step != *dependency)
                    })
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        if skipped.is_empty() {
            break;
        }
        for id in skipped {
            if let Some((_, state)) = states.get_mut(&id) {
                *state = RuntimeStepState::Skipped("a prerequisite failed or was skipped".into());
            }
        }
    }
}

fn dependency_ready(
    step: &MacroStep,
    dependency: &str,
    states: &BTreeMap<String, (MacroStep, RuntimeStepState)>,
) -> bool {
    states.get(dependency).is_some_and(|(_, state)| {
        if step
            .when
            .as_ref()
            .is_some_and(|condition| condition.step == dependency)
        {
            matches!(
                state,
                RuntimeStepState::Succeeded
                    | RuntimeStepState::Failed
                    | RuntimeStepState::Skipped(_)
            )
        } else {
            matches!(state, RuntimeStepState::Succeeded)
        }
    })
}

fn mark_unmet_conditions(
    states: &mut BTreeMap<String, (MacroStep, RuntimeStepState)>,
    finished: &[StepResult],
) {
    let snapshots = states
        .iter()
        .map(|(id, (_, state))| (id.clone(), state.clone()))
        .collect::<HashMap<_, _>>();
    for (step, state) in states.values_mut() {
        if !matches!(state, RuntimeStepState::Pending) {
            continue;
        }
        let Some(condition) = &step.when else {
            continue;
        };
        let Some(condition_state) = snapshots.get(&condition.step) else {
            continue;
        };
        if !matches!(
            condition_state,
            RuntimeStepState::Succeeded | RuntimeStepState::Failed | RuntimeStepState::Skipped(_)
        ) {
            continue;
        }
        let result = finished.iter().find(|result| result.id == condition.step);
        if !condition_matches(condition, condition_state, result) {
            *state =
                RuntimeStepState::Skipped(format!("condition on {} was not met", condition.step));
        }
    }
}

fn condition_matches(
    condition: &StepCondition,
    state: &RuntimeStepState,
    result: Option<&StepResult>,
) -> bool {
    let status = match state {
        RuntimeStepState::Succeeded => "succeeded",
        RuntimeStepState::Failed => "failed",
        RuntimeStepState::Skipped(_) => "skipped",
        RuntimeStepState::Pending | RuntimeStepState::Running => return false,
    };
    if condition
        .status
        .as_deref()
        .is_some_and(|expected| expected != status)
    {
        return false;
    }
    if condition
        .exit_code
        .is_some_and(|expected| result.and_then(|value| value.exit_code) != Some(expected))
    {
        return false;
    }
    let output = result
        .and_then(|value| value.raw_output.as_deref().or(value.output.as_deref()))
        .unwrap_or("");
    if !condition.output_contains.is_empty() && !output.contains(&condition.output_contains) {
        return false;
    }
    if !condition.output_not_contains.is_empty() && output.contains(&condition.output_not_contains)
    {
        return false;
    }
    true
}

fn validate_request(request: &MacroRequest) -> anyhow::Result<()> {
    if request.steps.len() < 2 {
        bail!("a macro requires at least two steps; use a normal Codex tool for one action");
    }
    if request.steps.len() > MAX_STEPS {
        bail!("a macro may contain at most {MAX_STEPS} steps");
    }
    if request.label.chars().count() > 120 {
        bail!("macro label exceeds 120 characters");
    }
    let mut ids = HashSet::new();
    for step in &request.steps {
        if !valid_step_id(&step.id) {
            bail!("invalid step id: {}", step.id);
        }
        if !ids.insert(step.id.clone()) {
            bail!("duplicate step id: {}", step.id);
        }
        if step.depends_on.len() > MAX_STEPS {
            bail!("step {} has too many dependencies", step.id);
        }
        let unique = step.depends_on.iter().collect::<HashSet<_>>();
        if unique.len() != step.depends_on.len() {
            bail!("step {} repeats a dependency", step.id);
        }
        if let Some(condition) = &step.when {
            if condition.step == step.id {
                bail!("step {} condition cannot reference itself", step.id);
            }
            if !step.depends_on.contains(&condition.step) {
                bail!(
                    "step {} condition must reference one of its dependencies",
                    step.id
                );
            }
            if condition
                .status
                .as_deref()
                .is_some_and(|status| !matches!(status, "succeeded" | "failed" | "skipped"))
            {
                bail!("step {} condition status is invalid", step.id);
            }
            if condition.output_contains.len() > 1_024
                || condition.output_not_contains.len() > 1_024
            {
                bail!(
                    "step {} condition output filter exceeds 1024 bytes",
                    step.id
                );
            }
        }
        validate_output_options(&step.output)?;
        if matches!(step.cache, CachePolicy::Use | CachePolicy::Refresh)
            && !action_is_cacheable(&step.action, true)
        {
            bail!("step {} cannot cache a mutating action", step.id);
        }
        validate_action(&step.action)?;
    }
    for step in &request.steps {
        for dependency in &step.depends_on {
            if dependency == &step.id {
                bail!("step {} cannot depend on itself", step.id);
            }
            if !ids.contains(dependency) {
                bail!("step {} depends on unknown step {dependency}", step.id);
            }
        }
    }
    validate_acyclic(&request.steps)
}

fn validate_acyclic(steps: &[MacroStep]) -> anyhow::Result<()> {
    let mut indegree = steps
        .iter()
        .map(|step| (step.id.clone(), step.depends_on.len()))
        .collect::<HashMap<_, _>>();
    let mut dependents = HashMap::<String, Vec<String>>::new();
    for step in steps {
        for dependency in &step.depends_on {
            dependents
                .entry(dependency.clone())
                .or_default()
                .push(step.id.clone());
        }
    }
    let mut ready = indegree
        .iter()
        .filter(|(_, count)| **count == 0)
        .map(|(id, _)| id.clone())
        .collect::<VecDeque<_>>();
    let mut visited = 0;
    while let Some(id) = ready.pop_front() {
        visited += 1;
        for dependent in dependents.get(&id).into_iter().flatten() {
            let count = indegree.get_mut(dependent).expect("validated dependency");
            *count -= 1;
            if *count == 0 {
                ready.push_back(dependent.clone());
            }
        }
    }
    if visited != steps.len() {
        bail!("macro dependency graph contains a cycle");
    }
    Ok(())
}

fn valid_step_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn validate_action(action: &MacroAction) -> anyhow::Result<()> {
    match action {
        MacroAction::ReadFile {
            path,
            start_line,
            end_line,
        } => {
            validate_relative_path(path)?;
            if let (Some(start), Some(end)) = (start_line, end_line)
                && (start == &0 || end < start)
            {
                bail!("read_file line range is invalid");
            }
        }
        MacroAction::Search {
            query,
            paths,
            max_results,
            ..
        } => {
            if query.is_empty() || query.len() > 1_024 {
                bail!("search query must contain 1 to 1024 bytes");
            }
            validate_paths(paths)?;
            if max_results.is_some_and(|limit| !(1..=500).contains(&limit)) {
                bail!("search max_results must be between 1 and 500");
            }
        }
        MacroAction::ListFiles {
            paths,
            contains,
            max_results,
        } => {
            validate_paths(paths)?;
            if contains.len() > 256 {
                bail!("list_files contains filter exceeds 256 bytes");
            }
            if max_results.is_some_and(|limit| !(1..=2_000).contains(&limit)) {
                bail!("list_files max_results must be between 1 and 2000");
            }
        }
        MacroAction::WriteFiles { files, .. } => {
            if files.is_empty() || files.len() > MAX_FILE_WRITES {
                bail!("write_files requires 1 to {MAX_FILE_WRITES} files");
            }
            let mut bytes = 0_usize;
            let mut paths = HashSet::new();
            for file in files {
                validate_relative_path(&file.path)?;
                if !paths.insert(file.path.as_str()) {
                    bail!("write_files repeats path {}", file.path);
                }
                bytes = bytes.saturating_add(file.content.len());
                if let Some(expected) = &file.expected_sha256 {
                    validate_expected_hash(expected)?;
                }
            }
            if bytes > MAX_WRITE_BYTES {
                bail!("write_files content exceeds {MAX_WRITE_BYTES} bytes");
            }
        }
        MacroAction::ApplyPatch {
            patch,
            expected_files,
        } => {
            if patch.is_empty() || patch.len() > MAX_WRITE_BYTES {
                bail!("apply_patch patch must contain 1 to {MAX_WRITE_BYTES} bytes");
            }
            if expected_files.is_empty() || expected_files.len() > MAX_FILE_WRITES {
                bail!("apply_patch expected_files requires 1 to {MAX_FILE_WRITES} entries");
            }
            let patch_paths = patch_paths(patch)?;
            let mut expected_paths = HashSet::new();
            for expected in expected_files {
                validate_relative_path(&expected.path)?;
                validate_expected_hash(&expected.sha256)?;
                if !expected_paths.insert(expected.path.as_str()) {
                    bail!("apply_patch repeats expected path {}", expected.path);
                }
            }
            for path in &patch_paths {
                if !expected_paths.contains(path.as_str()) {
                    bail!("apply_patch lacks expected_sha256 for {path}");
                }
            }
            for path in expected_paths {
                if !patch_paths.iter().any(|candidate| candidate == path) {
                    bail!("apply_patch expected_files includes unpatched path {path}");
                }
            }
        }
        MacroAction::RunCommand {
            argv,
            cwd,
            timeout_seconds,
            max_output_bytes,
            ..
        } => {
            validate_argv(argv)?;
            if !cwd.is_empty() {
                validate_relative_path(cwd)?;
            }
            if timeout_seconds.is_some_and(|seconds| !(1..=1_800).contains(&seconds)) {
                bail!("command timeout_seconds must be between 1 and 1800");
            }
            if max_output_bytes
                .is_some_and(|bytes| !(1_024..=MAX_COMMAND_OUTPUT_BYTES).contains(&bytes))
            {
                bail!(
                    "command max_output_bytes must be between 1024 and {MAX_COMMAND_OUTPUT_BYTES}"
                );
            }
        }
    }
    Ok(())
}

fn validate_output_options(options: &StepOutputOptions) -> anyhow::Result<()> {
    if options
        .max_output_tokens
        .is_some_and(|tokens| !(64..=16_384).contains(&tokens))
    {
        bail!("max_output_tokens must be between 64 and 16384");
    }
    if options
        .head_lines
        .is_some_and(|lines| !(1..=2_000).contains(&lines))
        || options
            .tail_lines
            .is_some_and(|lines| !(1..=2_000).contains(&lines))
    {
        bail!("head_lines and tail_lines must be between 1 and 2000");
    }
    if options.contains.len() > 1_024 {
        bail!("output contains filter exceeds 1024 bytes");
    }
    if options.mode == OutputMode::MatchesOnly && options.contains.is_empty() {
        bail!("matches_only output requires contains");
    }
    Ok(())
}

fn validate_expected_hash(value: &str) -> anyhow::Result<()> {
    if value == "missing"
        || (value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()))
    {
        return Ok(());
    }
    bail!("expected_sha256 must be 64 lowercase hexadecimal characters or missing")
}

fn validate_paths(paths: &[String]) -> anyhow::Result<()> {
    if paths.len() > 16 {
        bail!("at most 16 paths may be supplied");
    }
    for path in paths {
        validate_relative_path(path)?;
    }
    Ok(())
}

fn validate_relative_path(value: &str) -> anyhow::Result<()> {
    if value.is_empty() || value.len() > 4_096 {
        bail!("workspace path must contain 1 to 4096 bytes");
    }
    let path = Path::new(value);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        bail!("workspace paths must be relative and cannot contain '..': {value}");
    }
    Ok(())
}

fn validate_argv(argv: &[String]) -> anyhow::Result<()> {
    if argv.is_empty() || argv.len() > 64 {
        bail!("argv requires 1 to 64 elements");
    }
    let mut total = 0_usize;
    for argument in argv {
        if argument.is_empty() || argument.len() > 4_096 || argument.contains('\0') {
            bail!("every argv element must contain 1 to 4096 non-NUL bytes");
        }
        total = total.saturating_add(argument.len());
    }
    if total > 32 * 1024 {
        bail!("argv exceeds 32768 bytes");
    }
    let program = Path::new(&argv[0])
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(&argv[0])
        .to_ascii_lowercase();
    if matches!(
        program.as_str(),
        "bash"
            | "sh"
            | "zsh"
            | "fish"
            | "sudo"
            | "doas"
            | "su"
            | "systemctl"
            | "systemd-run"
            | "bwrap"
            | "unshare"
            | "mount"
            | "umount"
            | "pacman"
            | "curl"
            | "wget"
            | "ssh"
            | "scp"
            | "sftp"
            | "nc"
            | "ncat"
            | "socat"
            | "rm"
            | "unlink"
            | "shred"
    ) {
        bail!("command {program} is blocked by the macro sandbox policy");
    }
    if program == "git"
        && argv.get(1).is_some_and(|subcommand| {
            matches!(
                subcommand.as_str(),
                "commit"
                    | "push"
                    | "pull"
                    | "fetch"
                    | "clone"
                    | "reset"
                    | "clean"
                    | "checkout"
                    | "switch"
                    | "restore"
                    | "rebase"
                    | "merge"
                    | "cherry-pick"
                    | "worktree"
            )
        })
    {
        bail!("mutating or networked Git subcommands are blocked in macro execution");
    }
    Ok(())
}

fn canonical_workspace(
    value: &str,
    allowed_roots: &[PathBuf],
) -> anyhow::Result<(PathBuf, PathBuf)> {
    let path = Path::new(value);
    if !path.is_absolute() {
        bail!("workspace_root must be absolute");
    }
    let canonical = path
        .canonicalize()
        .with_context(|| format!("workspace is unavailable: {}", path.display()))?;
    if !canonical.is_dir() {
        bail!("workspace_root is not a directory: {}", canonical.display());
    }
    let lock_root = allowed_roots
        .iter()
        .filter(|root| {
            canonical.as_path() == root.as_path() || canonical.starts_with(root.as_path())
        })
        .max_by_key(|root| root.components().count())
        .cloned()
        .ok_or_else(|| {
            anyhow!(
                "workspace_root is not inside a project explicitly saved in Codex Native: {}",
                canonical.display()
            )
        })?;
    Ok((canonical, lock_root))
}

fn resolve_existing(workspace: &Path, relative: &str) -> anyhow::Result<PathBuf> {
    validate_relative_path(relative)?;
    let path = workspace.join(relative);
    let canonical = path
        .canonicalize()
        .with_context(|| format!("workspace path is unavailable: {relative}"))?;
    if !canonical.starts_with(workspace) {
        bail!("workspace path resolves outside the selected project: {relative}");
    }
    Ok(canonical)
}

fn resolve_write_target(
    workspace: &Path,
    relative: &str,
    create_parents: bool,
) -> anyhow::Result<PathBuf> {
    validate_relative_path(relative)?;
    let lexical = workspace.join(relative);
    let parent = lexical
        .parent()
        .context("write target has no parent directory")?;
    if create_parents {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let canonical_parent = parent
        .canonicalize()
        .with_context(|| format!("write parent is unavailable: {}", parent.display()))?;
    if !canonical_parent.starts_with(workspace) {
        bail!("write target resolves outside the selected project: {relative}");
    }
    let name = lexical
        .file_name()
        .context("write target has no filename")?;
    let target = canonical_parent.join(name);
    if target.is_symlink() {
        let canonical = target
            .canonicalize()
            .with_context(|| format!("write symlink is unavailable: {relative}"))?;
        if !canonical.starts_with(workspace) {
            bail!("write symlink resolves outside the selected project: {relative}");
        }
        return Ok(canonical);
    }
    Ok(target)
}

async fn execute_step(
    step: MacroStep,
    workspace: PathBuf,
    locks: Arc<WorkspaceLocks>,
    config: MacroServerConfig,
    cache: Arc<Mutex<ReadCache>>,
    invocation_id: &str,
    dry_run: bool,
) -> StepResult {
    let started = Instant::now();
    let id = step.id;
    let output_options = step.output;
    let cache_policy = step.cache;
    let action = step.action;
    let cache_key = if action_is_cacheable(
        &action,
        matches!(cache_policy, CachePolicy::Use | CachePolicy::Refresh),
    ) {
        let cache_workspace = workspace.clone();
        let cache_action = action.clone();
        tokio::task::spawn_blocking(move || cache_key(&cache_workspace, &cache_action))
            .await
            .ok()
            .and_then(Result::ok)
    } else {
        None
    };
    if cache_policy != CachePolicy::Refresh
        && cache_policy != CachePolicy::Off
        && let Some(key) = cache_key.as_deref()
        && let Some(mut result) = cache.lock().await.get(key)
    {
        result.id = id;
        result.elapsed_ms = duration_millis(started.elapsed());
        result.cache_hit = true;
        result.raw_output = result.output.clone();
        let result_id = result.id.clone();
        if let Err(error) =
            finalize_step_output(invocation_id, &result_id, &output_options, &mut result)
        {
            return StepResult::failed(result.id, started.elapsed(), format!("{error:#}"));
        }
        return result;
    }
    let result = match action {
        MacroAction::ReadFile {
            path,
            start_line,
            end_line,
        } => execute_read_file(&workspace, &locks, &path, start_line, end_line).await,
        MacroAction::Search {
            query,
            paths,
            case_sensitive,
            max_results,
        } => {
            execute_search(
                &workspace,
                &locks,
                &query,
                &paths,
                case_sensitive,
                max_results.unwrap_or(200),
            )
            .await
        }
        MacroAction::ListFiles {
            paths,
            contains,
            max_results,
        } => {
            execute_list_files(
                &workspace,
                &locks,
                &paths,
                &contains,
                max_results.unwrap_or(1_000),
            )
            .await
        }
        MacroAction::WriteFiles {
            files,
            create_parents,
        } => {
            if dry_run {
                execute_write_files_dry_run(&workspace, &locks, &files, create_parents).await
            } else {
                execute_write_files(&workspace, &locks, files, create_parents).await
            }
        }
        MacroAction::ApplyPatch {
            patch,
            expected_files,
        } => {
            if dry_run {
                execute_apply_patch(&workspace, &locks, &patch, &expected_files, true).await
            } else {
                execute_apply_patch(&workspace, &locks, &patch, &expected_files, false).await
            }
        }
        MacroAction::RunCommand {
            argv,
            cwd,
            mutating,
            timeout_seconds,
            max_output_bytes,
        } => {
            if dry_run && mutating {
                validate_command_dry_run(&workspace, &argv, &cwd)
            } else {
                execute_command(
                    &workspace,
                    &locks,
                    CommandExecution {
                        argv: &argv,
                        relative_cwd: &cwd,
                        mutating,
                        timeout_seconds: timeout_seconds
                            .unwrap_or(config.timeout_seconds)
                            .min(config.timeout_seconds),
                        max_output_bytes: max_output_bytes.unwrap_or(128 * 1024),
                    },
                    &config,
                )
                .await
            }
        }
    };
    match result {
        Ok(mut result) => {
            if result.status == "succeeded"
                && let Some(key) = cache_key
                && cache_policy != CachePolicy::Off
            {
                cache.lock().await.insert(key, result.clone());
            }
            result.id = id.clone();
            result.elapsed_ms = duration_millis(started.elapsed());
            result.raw_output = result.output.clone();
            if let Err(error) =
                finalize_step_output(invocation_id, &id, &output_options, &mut result)
            {
                return StepResult::failed(id, started.elapsed(), format!("{error:#}"));
            }
            result
        }
        Err(error) => StepResult::failed(id, started.elapsed(), format!("{error:#}")),
    }
}

fn dry_run_result(output: String) -> StepResult {
    let bytes = output.len() as u64;
    success_result(output, bytes, false, None, None)
}

fn validate_command_dry_run(
    workspace: &Path,
    argv: &[String],
    relative_cwd: &str,
) -> anyhow::Result<StepResult> {
    validate_argv(argv)?;
    if !relative_cwd.is_empty() {
        let cwd = resolve_existing(workspace, relative_cwd)?;
        if !cwd.is_dir() {
            bail!("command cwd is not a directory: {relative_cwd}");
        }
    }
    Ok(dry_run_result(format!(
        "dry-run: validated mutating command {}",
        argv.first().map(String::as_str).unwrap_or("unknown")
    )))
}

async fn execute_read_file(
    workspace: &Path,
    locks: &WorkspaceLocks,
    relative: &str,
    start_line: Option<usize>,
    end_line: Option<usize>,
) -> anyhow::Result<StepResult> {
    let path = resolve_existing(workspace, relative)?;
    if !path.is_file() {
        bail!("read_file target is not a regular file: {relative}");
    }
    let _workspace = locks.read_workspace().await;
    let _files = locks.read_files(std::slice::from_ref(&path)).await;
    let metadata = path.metadata()?;
    if metadata.len() > MAX_READ_BYTES as u64 {
        bail!("read_file target exceeds {MAX_READ_BYTES} bytes");
    }
    let text = fs::read_to_string(&path)
        .with_context(|| format!("failed to read UTF-8 file {relative}"))?;
    let start = start_line.unwrap_or(1);
    let end = end_line.unwrap_or(usize::MAX);
    let output = text
        .lines()
        .enumerate()
        .filter(|(index, _)| {
            let line = index + 1;
            line >= start && line <= end
        })
        .map(|(index, line)| format!("{}:{}", index + 1, line))
        .collect::<Vec<_>>()
        .join("\n");
    let (output, output_bytes, truncated) = bound_text_output(output, MAX_STRUCTURED_OUTPUT_BYTES);
    Ok(success_result(output, output_bytes, truncated, None, None))
}

async fn execute_search(
    workspace: &Path,
    locks: &WorkspaceLocks,
    query: &str,
    paths: &[String],
    case_sensitive: bool,
    max_results: usize,
) -> anyhow::Result<StepResult> {
    let _workspace = locks.read_workspace().await;
    let workspace = workspace.to_path_buf();
    let query = query.to_owned();
    let paths = paths.to_vec();
    let result = tokio::task::spawn_blocking(move || {
        search_workspace(&workspace, &query, &paths, case_sensitive, max_results)
    })
    .await
    .context("search worker failed")??;
    let (result, bytes, truncated) = bound_text_output(result, MAX_STRUCTURED_OUTPUT_BYTES);
    Ok(success_result(result, bytes, truncated, None, None))
}

async fn execute_list_files(
    workspace: &Path,
    locks: &WorkspaceLocks,
    paths: &[String],
    contains: &str,
    max_results: usize,
) -> anyhow::Result<StepResult> {
    let _workspace = locks.read_workspace().await;
    let workspace = workspace.to_path_buf();
    let paths = paths.to_vec();
    let contains = contains.to_owned();
    let result = tokio::task::spawn_blocking(move || {
        list_workspace_files(&workspace, &paths, &contains, max_results)
    })
    .await
    .context("file-list worker failed")??;
    let (result, bytes, truncated) = bound_text_output(result, MAX_STRUCTURED_OUTPUT_BYTES);
    Ok(success_result(result, bytes, truncated, None, None))
}

async fn execute_write_files_dry_run(
    workspace: &Path,
    locks: &WorkspaceLocks,
    files: &[FileWrite],
    create_parents: bool,
) -> anyhow::Result<StepResult> {
    let _workspace = locks.read_workspace().await;
    let targets = files
        .iter()
        .map(|file| {
            resolve_write_target_dry_run(workspace, &file.path, create_parents)
                .map(|target| (target, file))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let paths = targets
        .iter()
        .map(|(target, _)| target.clone())
        .collect::<Vec<_>>();
    let _files = locks.write_files(&paths).await;
    for (target, file) in &targets {
        if let Some(expected) = &file.expected_sha256 {
            verify_expected_hash(target, &file.path, expected)?;
        }
    }
    Ok(dry_run_result(format!(
        "dry-run: validated {} atomic file write(s); supplied hashes match",
        files.len()
    )))
}

fn resolve_write_target_dry_run(
    workspace: &Path,
    relative: &str,
    create_parents: bool,
) -> anyhow::Result<PathBuf> {
    validate_relative_path(relative)?;
    let lexical = workspace.join(relative);
    let parent = lexical
        .parent()
        .context("write target has no parent directory")?;
    if parent.is_dir() {
        return resolve_write_target(workspace, relative, false);
    }
    if !create_parents {
        bail!("write parent is unavailable: {}", parent.display());
    }
    let mut existing = parent;
    while !existing.exists() {
        existing = existing
            .parent()
            .context("write parent has no existing ancestor")?;
    }
    let canonical = existing.canonicalize()?;
    if !canonical.starts_with(workspace) {
        bail!("write target resolves outside the selected project: {relative}");
    }
    Ok(lexical)
}

async fn execute_write_files(
    workspace: &Path,
    locks: &WorkspaceLocks,
    files: Vec<FileWrite>,
    create_parents: bool,
) -> anyhow::Result<StepResult> {
    let _workspace = locks.read_workspace().await;
    let targets = files
        .iter()
        .map(|file| {
            resolve_write_target(workspace, &file.path, create_parents).map(|target| {
                (
                    target,
                    file.path.clone(),
                    file.content.clone(),
                    file.expected_sha256.clone(),
                )
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let paths = targets
        .iter()
        .map(|(target, _, _, _)| target.clone())
        .collect::<Vec<_>>();
    let _files = locks.write_files(&paths).await;
    for (target, relative, _, expected) in &targets {
        if let Some(expected) = expected {
            verify_expected_hash(target, relative, expected)?;
        }
    }
    let targets = targets
        .into_iter()
        .map(|(target, relative, content, _)| (target, relative, content))
        .collect();
    let summaries = tokio::task::spawn_blocking(move || atomic_write_many(targets))
        .await
        .context("file-write worker failed")??;
    let output = summaries.join("\n");
    let bytes = output.len() as u64;
    Ok(success_result(output, bytes, false, None, None))
}

async fn execute_apply_patch(
    workspace: &Path,
    locks: &WorkspaceLocks,
    patch: &str,
    expected_files: &[ExpectedFileHash],
    dry_run: bool,
) -> anyhow::Result<StepResult> {
    let relative_paths = patch_paths(patch)?;
    let targets = relative_paths
        .iter()
        .map(|relative| {
            if workspace.join(relative).is_symlink() {
                bail!("apply_patch refuses symlink target {relative}");
            }
            resolve_write_target(workspace, relative, false)
                .map(|target| (target, relative.clone()))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let paths = targets
        .iter()
        .map(|(target, _)| target.clone())
        .collect::<Vec<_>>();
    let _workspace = locks.read_workspace().await;
    let _files = locks.write_files(&paths).await;
    for expected in expected_files {
        let target = targets
            .iter()
            .find(|(_, relative)| relative == &expected.path)
            .map(|(target, _)| target)
            .with_context(|| format!("patch target {} was not resolved", expected.path))?;
        verify_expected_hash(target, &expected.path, &expected.sha256)?;
    }
    let workspace = workspace.to_path_buf();
    let patch = patch.to_owned();
    tokio::task::spawn_blocking(move || run_git_apply(&workspace, &patch, dry_run))
        .await
        .context("apply_patch worker failed")??;
    let output = if dry_run {
        format!(
            "dry-run: patch validated for {} file(s); hashes match",
            relative_paths.len()
        )
    } else {
        format!(
            "applied hash-checked patch to {} file(s)",
            relative_paths.len()
        )
    };
    let bytes = output.len() as u64;
    Ok(success_result(output, bytes, false, None, None))
}

fn run_git_apply(workspace: &Path, patch: &str, check_only: bool) -> anyhow::Result<()> {
    run_git_apply_once(workspace, patch, true)?;
    if !check_only {
        run_git_apply_once(workspace, patch, false)?;
    }
    Ok(())
}

fn run_git_apply_once(workspace: &Path, patch: &str, check_only: bool) -> anyhow::Result<()> {
    let git = find_program("git").context("git is required for apply_patch")?;
    let mut command = StdCommand::new(git);
    command
        .env_clear()
        .env("LANG", "C.UTF-8")
        .arg("apply")
        .arg("--recount")
        .arg("--whitespace=nowarn");
    if check_only {
        command.arg("--check");
    }
    let mut child = command
        .current_dir(workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to start git apply")?;
    child
        .stdin
        .as_mut()
        .context("git apply stdin unavailable")?
        .write_all(patch.as_bytes())?;
    let output = child
        .wait_with_output()
        .context("git apply failed to wait")?;
    if output.status.success() {
        return Ok(());
    }
    bail!(
        "git apply {} failed: {}",
        if check_only { "check" } else { "write" },
        truncate_text(&String::from_utf8_lossy(&output.stderr), 2_000)
    )
}

fn verify_expected_hash(target: &Path, relative: &str, expected: &str) -> anyhow::Result<()> {
    let actual = if target.is_file() {
        sha256_file(target)?
    } else if !target.exists() {
        "missing".into()
    } else {
        bail!("hash target is not a regular file: {relative}");
    };
    if actual != expected {
        bail!("stale write refused for {relative}: expected {expected}, found {actual}");
    }
    Ok(())
}

fn sha256_file(path: &Path) -> anyhow::Result<String> {
    let bytes = fs::read(path).with_context(|| format!("failed to hash {}", path.display()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn patch_paths(patch: &str) -> anyhow::Result<Vec<String>> {
    let mut paths = Vec::new();
    for line in patch.lines() {
        let Some(raw) = line
            .strip_prefix("--- ")
            .or_else(|| line.strip_prefix("+++ "))
        else {
            continue;
        };
        let raw = raw.split('\t').next().unwrap_or(raw).trim();
        if raw == "/dev/null" {
            continue;
        }
        if raw.starts_with('"') || raw.contains('\\') {
            bail!("apply_patch does not accept quoted or escaped paths");
        }
        let relative = raw
            .strip_prefix("a/")
            .or_else(|| raw.strip_prefix("b/"))
            .unwrap_or(raw);
        validate_relative_path(relative)?;
        if !paths.iter().any(|path| path == relative) {
            paths.push(relative.to_owned());
        }
    }
    if paths.is_empty() {
        bail!("apply_patch contains no unified-diff file headers");
    }
    if paths.len() > MAX_FILE_WRITES {
        bail!("apply_patch changes more than {MAX_FILE_WRITES} files");
    }
    Ok(paths)
}

struct CommandExecution<'a> {
    argv: &'a [String],
    relative_cwd: &'a str,
    mutating: bool,
    timeout_seconds: u64,
    max_output_bytes: usize,
}

async fn execute_command(
    workspace: &Path,
    locks: &WorkspaceLocks,
    execution: CommandExecution<'_>,
    config: &MacroServerConfig,
) -> anyhow::Result<StepResult> {
    validate_argv(execution.argv)?;
    let cwd = if execution.relative_cwd.is_empty() {
        workspace.to_path_buf()
    } else {
        let cwd = resolve_existing(workspace, execution.relative_cwd)?;
        if !cwd.is_dir() {
            bail!("command cwd is not a directory: {}", execution.relative_cwd);
        }
        cwd
    };
    let relative_cwd = cwd
        .strip_prefix(workspace)
        .context("command cwd escaped the workspace")?;
    if execution.mutating {
        let _workspace = locks.write_workspace().await;
        run_sandboxed_command(
            workspace,
            relative_cwd,
            execution.argv,
            true,
            execution.timeout_seconds,
            execution.max_output_bytes,
            config,
        )
        .await
    } else {
        let _workspace = locks.read_workspace().await;
        run_sandboxed_command(
            workspace,
            relative_cwd,
            execution.argv,
            false,
            execution.timeout_seconds,
            execution.max_output_bytes,
            config,
        )
        .await
    }
}

fn success_result(
    output: String,
    output_bytes: u64,
    output_truncated: bool,
    exit_code: Option<i32>,
    peak_memory_bytes: Option<u64>,
) -> StepResult {
    StepResult {
        id: String::new(),
        status: "succeeded".into(),
        elapsed_ms: 0,
        output: (!output.is_empty()).then_some(output),
        error: None,
        exit_code,
        output_bytes,
        preview_bytes: output_bytes,
        output_truncated,
        cache_hit: false,
        output_handle: None,
        peak_memory_bytes,
        raw_output: None,
    }
}

fn search_workspace(
    workspace: &Path,
    query: &str,
    paths: &[String],
    case_sensitive: bool,
    max_results: usize,
) -> anyhow::Result<String> {
    let roots = search_roots(workspace, paths)?;
    let query_cmp = if case_sensitive {
        query.to_owned()
    } else {
        query.to_lowercase()
    };
    let mut files_seen = 0_usize;
    let mut results = Vec::new();
    for root in roots {
        walk_files(&root, &mut files_seen, |path| {
            if results.len() >= max_results {
                return Ok(false);
            }
            let metadata = path.metadata()?;
            if metadata.len() > MAX_SEARCH_FILE_BYTES {
                return Ok(true);
            }
            let Ok(text) = fs::read_to_string(path) else {
                return Ok(true);
            };
            for (index, line) in text.lines().enumerate() {
                let matches = if case_sensitive {
                    line.contains(&query_cmp)
                } else {
                    line.to_lowercase().contains(&query_cmp)
                };
                if matches {
                    let relative = path.strip_prefix(workspace).unwrap_or(path);
                    results.push(format!(
                        "{}:{}:{}",
                        relative.display(),
                        index + 1,
                        truncate_text(line, 500)
                    ));
                    if results.len() >= max_results {
                        return Ok(false);
                    }
                }
            }
            Ok(true)
        })?;
        if results.len() >= max_results || files_seen >= MAX_SEARCH_FILES {
            break;
        }
    }
    Ok(results.join("\n"))
}

fn list_workspace_files(
    workspace: &Path,
    paths: &[String],
    contains: &str,
    max_results: usize,
) -> anyhow::Result<String> {
    let roots = search_roots(workspace, paths)?;
    let mut files_seen = 0_usize;
    let mut results = Vec::new();
    for root in roots {
        walk_files(&root, &mut files_seen, |path| {
            let relative = path.strip_prefix(workspace).unwrap_or(path);
            let display = relative.to_string_lossy();
            if contains.is_empty() || display.contains(contains) {
                results.push(display.into_owned());
            }
            Ok(results.len() < max_results)
        })?;
        if results.len() >= max_results || files_seen >= MAX_SEARCH_FILES {
            break;
        }
    }
    results.sort();
    results.dedup();
    Ok(results.join("\n"))
}

fn search_roots(workspace: &Path, paths: &[String]) -> anyhow::Result<Vec<PathBuf>> {
    if paths.is_empty() {
        return Ok(vec![workspace.to_path_buf()]);
    }
    paths
        .iter()
        .map(|path| resolve_existing(workspace, path))
        .collect()
}

fn walk_files(
    root: &Path,
    files_seen: &mut usize,
    mut visit: impl FnMut(&Path) -> anyhow::Result<bool>,
) -> anyhow::Result<()> {
    let mut pending = VecDeque::from([root.to_path_buf()]);
    while let Some(path) = pending.pop_front() {
        if *files_seen >= MAX_SEARCH_FILES {
            break;
        }
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_file() {
            *files_seen += 1;
            if !visit(&path)? {
                break;
            }
            continue;
        }
        if !metadata.is_dir() {
            continue;
        }
        if path != root && ignored_directory(&path) {
            continue;
        }
        let mut entries = fs::read_dir(&path)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        entries.sort();
        pending.extend(entries);
    }
    Ok(())
}

fn ignored_directory(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            matches!(
                name,
                ".git" | ".hg" | ".svn" | "target" | "node_modules" | ".cache"
            )
        })
}

fn atomic_write_many(targets: Vec<(PathBuf, String, String)>) -> anyhow::Result<Vec<String>> {
    let mut staged = Vec::new();
    for (target, relative, content) in targets {
        let parent = target.parent().context("write target has no parent")?;
        let temporary = parent.join(format!(
            ".codex-native-macro-{}.tmp",
            Uuid::new_v4().simple()
        ));
        let result = (|| {
            let mut file = fs::File::create(&temporary)
                .with_context(|| format!("failed to create {}", temporary.display()))?;
            let permissions = target
                .metadata()
                .map(|metadata| metadata.permissions())
                .unwrap_or_else(|_| fs::Permissions::from_mode(0o644));
            file.set_permissions(permissions)?;
            file.write_all(content.as_bytes())?;
            file.sync_all()?;
            Ok::<_, anyhow::Error>(())
        })();
        if let Err(error) = result {
            for (temporary, _, _) in &staged {
                let _ = fs::remove_file(temporary);
            }
            let _ = fs::remove_file(&temporary);
            return Err(error);
        }
        staged.push((temporary, target, relative));
    }
    let mut summaries = Vec::new();
    for (temporary, target, relative) in staged {
        fs::rename(&temporary, &target)
            .with_context(|| format!("failed to replace {}", target.display()))?;
        summaries.push(format!("wrote {relative}"));
    }
    Ok(summaries)
}

async fn run_sandboxed_command(
    workspace: &Path,
    relative_cwd: &Path,
    argv: &[String],
    writable: bool,
    timeout_seconds: u64,
    max_output_bytes: usize,
    config: &MacroServerConfig,
) -> anyhow::Result<StepResult> {
    let systemd_run =
        find_program("systemd-run").context("systemd-run is required for macro commands")?;
    let bubblewrap = find_program("bwrap").context("bubblewrap is required for macro commands")?;
    let invocation = Uuid::new_v4().simple().to_string();
    let unit_base = format!("codex-native-macro-{invocation}");
    let unit = format!("{unit_base}.service");
    let sandbox_root = macro_sandbox_root()?.join(&invocation);
    let private_home = sandbox_root.join("home");
    fs::create_dir_all(&private_home)
        .with_context(|| format!("failed to create {}", private_home.display()))?;
    fs::set_permissions(&sandbox_root, fs::Permissions::from_mode(0o700))?;
    fs::set_permissions(&private_home, fs::Permissions::from_mode(0o700))?;
    let cleanup = SandboxCleanup {
        unit: unit.clone(),
        root: sandbox_root,
    };

    let sandbox_args = sandbox_arguments(workspace, relative_cwd, &private_home, writable, argv)?;
    let mut command = Command::new(systemd_run);
    command
        .args(["--user", "--quiet", "--collect", "--wait", "--pipe"])
        .arg(format!("--unit={unit_base}"))
        .arg("--property=Description=Codex Native bounded macro command")
        .arg(format!("--property=MemoryMax={}M", config.memory_mib))
        .arg("--property=MemorySwapMax=0")
        .arg(format!("--property=TasksMax={}", config.tasks_max))
        .arg("--property=NoNewPrivileges=yes")
        .arg("--property=KillMode=mixed")
        .arg("--property=TimeoutStopSec=3s")
        .arg(format!("--property=RuntimeMaxSec={timeout_seconds}s"))
        .arg(bubblewrap)
        .args(sandbox_args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = command
        .spawn()
        .context("failed to start the bounded macro command")?;
    let stdout = child.stdout.take().context("macro stdout unavailable")?;
    let stderr = child.stderr.take().context("macro stderr unavailable")?;
    let stdout_reader = tokio::spawn(read_bounded(stdout, max_output_bytes));
    let stderr_reader = tokio::spawn(read_bounded(stderr, max_output_bytes));
    let status = match timeout(Duration::from_secs(timeout_seconds + 5), child.wait()).await {
        Ok(result) => result.context("failed to wait for macro command")?,
        Err(_) => {
            let _ = child.start_kill();
            stop_unit(&unit).await;
            let _ = timeout(Duration::from_secs(3), child.wait()).await;
            drop(cleanup);
            bail!("macro command timed out after {timeout_seconds} seconds");
        }
    };
    let stdout = stdout_reader
        .await
        .context("macro stdout reader failed")??;
    let mut stderr = stderr_reader
        .await
        .context("macro stderr reader failed")??;
    let peak_memory_bytes = strip_time_metric(&mut stderr.bytes);
    let mut output = String::new();
    if !stdout.bytes.is_empty() {
        output.push_str(&String::from_utf8_lossy(&stdout.bytes));
    }
    if !stderr.bytes.is_empty() {
        if !output.is_empty() && !output.ends_with('\n') {
            output.push('\n');
        }
        output.push_str("[stderr]\n");
        output.push_str(&String::from_utf8_lossy(&stderr.bytes));
    }
    let mut truncated = stdout.truncated || stderr.truncated;
    if output.len() > max_output_bytes {
        output = truncate_utf8_bytes(&output, max_output_bytes);
        truncated = true;
    }
    if truncated {
        if !output.ends_with('\n') {
            output.push('\n');
        }
        output.push_str("[output truncated by Codex Native]");
    }
    let total_bytes = stdout.total_bytes.saturating_add(stderr.total_bytes);
    let preview_bytes = output.len() as u64;
    drop(cleanup);
    if !status.success() {
        return Ok(StepResult {
            id: String::new(),
            status: "failed".into(),
            elapsed_ms: 0,
            output: (!output.is_empty()).then_some(output),
            error: Some(format!("command exited with {status}")),
            exit_code: status.code(),
            output_bytes: total_bytes,
            preview_bytes,
            output_truncated: truncated,
            cache_hit: false,
            output_handle: None,
            peak_memory_bytes,
            raw_output: None,
        });
    }
    Ok(success_result(
        output,
        total_bytes,
        truncated,
        status.code(),
        peak_memory_bytes,
    ))
}

fn sandbox_arguments(
    workspace: &Path,
    relative_cwd: &Path,
    private_home: &Path,
    writable: bool,
    argv: &[String],
) -> anyhow::Result<Vec<String>> {
    let mut args = vec![
        "--die-with-parent".into(),
        "--new-session".into(),
        "--unshare-user".into(),
        "--unshare-pid".into(),
        "--unshare-ipc".into(),
        "--unshare-uts".into(),
        "--unshare-cgroup-try".into(),
        "--unshare-net".into(),
        "--cap-drop".into(),
        "ALL".into(),
        "--ro-bind".into(),
        "/usr".into(),
        "/usr".into(),
        "--symlink".into(),
        "usr/bin".into(),
        "/bin".into(),
        "--symlink".into(),
        "usr/lib".into(),
        "/lib".into(),
        "--symlink".into(),
        "usr/lib".into(),
        "/lib64".into(),
        "--ro-bind".into(),
        "/etc".into(),
        "/etc".into(),
        "--proc".into(),
        "/proc".into(),
        "--dev".into(),
        "/dev".into(),
        "--tmpfs".into(),
        "/tmp".into(),
        "--dir".into(),
        "/run".into(),
        "--dir".into(),
        "/home".into(),
        "--bind".into(),
        private_home.to_string_lossy().into_owned(),
        "/home/codex".into(),
        if writable { "--bind" } else { "--ro-bind" }.into(),
        workspace.to_string_lossy().into_owned(),
        "/workspace".into(),
    ];
    append_rust_toolchain_mounts(&mut args);
    let cwd = if relative_cwd.as_os_str().is_empty() {
        "/workspace".into()
    } else {
        format!("/workspace/{}", relative_cwd.to_string_lossy())
    };
    args.extend([
        "--chdir".into(),
        cwd,
        "--clearenv".into(),
        "--setenv".into(),
        "HOME".into(),
        "/home/codex".into(),
        "--setenv".into(),
        "USER".into(),
        "codex".into(),
        "--setenv".into(),
        "LOGNAME".into(),
        "codex".into(),
        "--setenv".into(),
        "PATH".into(),
        "/opt/cargo-bin:/usr/local/bin:/usr/bin".into(),
        "--setenv".into(),
        "LANG".into(),
        env::var("LANG").unwrap_or_else(|_| "C.UTF-8".into()),
        "--setenv".into(),
        "CARGO_TERM_COLOR".into(),
        "never".into(),
        "--hostname".into(),
        "codex-macro".into(),
    ]);
    if dirs::home_dir()
        .map(|home| home.join(".rustup").is_dir())
        .unwrap_or(false)
    {
        args.extend([
            "--setenv".into(),
            "RUSTUP_HOME".into(),
            "/opt/rustup".into(),
            "--setenv".into(),
            "CARGO_HOME".into(),
            "/opt/cargo-home".into(),
        ]);
    }
    if Path::new("/usr/bin/time").is_file() {
        args.extend([
            "/usr/bin/time".into(),
            "-f".into(),
            "\n__CODEX_NATIVE_MAX_RSS_KIB__=%M".into(),
            "--".into(),
        ]);
    }
    args.extend(argv.iter().cloned());
    Ok(args)
}

fn append_rust_toolchain_mounts(args: &mut Vec<String>) {
    let Some(home) = dirs::home_dir() else {
        return;
    };
    let rustup = home.join(".rustup");
    let cargo = home.join(".cargo");
    let cargo_bin = cargo.join("bin");
    if !rustup.is_dir() || !cargo_bin.is_dir() {
        return;
    }
    args.extend([
        "--dir".into(),
        "/opt".into(),
        "--ro-bind".into(),
        rustup.to_string_lossy().into_owned(),
        "/opt/rustup".into(),
        "--ro-bind".into(),
        cargo_bin.to_string_lossy().into_owned(),
        "/opt/cargo-bin".into(),
        "--dir".into(),
        "/opt/cargo-home".into(),
    ]);
    for directory in ["registry", "git"] {
        let source = cargo.join(directory);
        if source.is_dir() {
            args.extend([
                "--ro-bind".into(),
                source.to_string_lossy().into_owned(),
                format!("/opt/cargo-home/{directory}"),
            ]);
        }
    }
}

#[derive(Debug)]
struct SandboxCleanup {
    unit: String,
    root: PathBuf,
}

impl Drop for SandboxCleanup {
    fn drop(&mut self) {
        let valid_unit = self.unit.starts_with("codex-native-macro-")
            && self.unit.ends_with(".service")
            && self
                .unit
                .trim_start_matches("codex-native-macro-")
                .trim_end_matches(".service")
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit());
        if valid_unit {
            let _ = std::process::Command::new("systemctl")
                .args(["--user", "stop", &self.unit])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn();
        }
        if self
            .root
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                name.len() == 32 && name.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
            && self
                .root
                .parent()
                .is_some_and(|parent| parent.ends_with("codex-native/macro-sandboxes"))
        {
            let _ = fs::remove_dir_all(&self.root);
        }
    }
}

async fn stop_unit(unit: &str) {
    if !unit.starts_with("codex-native-macro-") || !unit.ends_with(".service") {
        return;
    }
    let _ = Command::new("systemctl")
        .args(["--user", "stop", unit])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
}

#[derive(Debug)]
struct BoundedOutput {
    bytes: Vec<u8>,
    total_bytes: u64,
    truncated: bool,
}

async fn read_bounded(
    mut reader: impl AsyncRead + Unpin,
    limit: usize,
) -> anyhow::Result<BoundedOutput> {
    let mut kept = Vec::with_capacity(limit.min(64 * 1024));
    let mut total = 0_u64;
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        total = total.saturating_add(read as u64);
        if kept.len() < limit {
            let remaining = limit - kept.len();
            kept.extend_from_slice(&buffer[..read.min(remaining)]);
        }
    }
    Ok(BoundedOutput {
        bytes: kept,
        total_bytes: total,
        truncated: total > limit as u64,
    })
}

fn strip_time_metric(stderr: &mut Vec<u8>) -> Option<u64> {
    const MARKER: &[u8] = b"__CODEX_NATIVE_MAX_RSS_KIB__=";
    let position = stderr
        .windows(MARKER.len())
        .rposition(|window| window == MARKER)?;
    let number_start = position + MARKER.len();
    let number_end = stderr[number_start..]
        .iter()
        .position(|byte| !byte.is_ascii_digit())
        .map(|offset| number_start + offset)
        .unwrap_or(stderr.len());
    let kib = std::str::from_utf8(&stderr[number_start..number_end])
        .ok()?
        .parse::<u64>()
        .ok()?;
    let line_start = stderr[..position]
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map(|index| index + 1)
        .unwrap_or(position);
    let line_end = stderr[number_end..]
        .iter()
        .position(|byte| *byte == b'\n')
        .map(|offset| number_end + offset + 1)
        .unwrap_or(stderr.len());
    stderr.drain(line_start..line_end);
    Some(kib.saturating_mul(1024))
}

fn action_label(action: &MacroAction) -> &'static str {
    match action {
        MacroAction::ReadFile { .. } => "read",
        MacroAction::Search { .. } => "search",
        MacroAction::ListFiles { .. } => "list",
        MacroAction::WriteFiles { .. } => "write",
        MacroAction::ApplyPatch { .. } => "patch",
        MacroAction::RunCommand { mutating: true, .. } => "mutating command",
        MacroAction::RunCommand {
            mutating: false, ..
        } => "read-only command",
    }
}

fn progress(
    output: &mpsc::UnboundedSender<Value>,
    progress_token: Option<&Value>,
    completed: usize,
    total: usize,
    message: &str,
) {
    let Some(progress_token) = progress_token else {
        return;
    };
    let _ = output.send(json!({
        "jsonrpc": "2.0",
        "method": "notifications/progress",
        "params": {
            "progressToken": progress_token,
            "progress": completed,
            "total": total,
            "message": message
        }
    }));
}

fn find_program(name: &str) -> Option<PathBuf> {
    let direct = Path::new("/usr/bin").join(name);
    if direct.is_file() {
        return Some(direct);
    }
    env::var_os("PATH").and_then(|path| {
        env::split_paths(&path)
            .map(|directory| directory.join(name))
            .find(|candidate| candidate.is_file())
    })
}

fn macro_sandbox_root() -> anyhow::Result<PathBuf> {
    dirs::state_dir()
        .or_else(dirs::data_local_dir)
        .map(|path| path.join("codex-native/macro-sandboxes"))
        .ok_or_else(|| anyhow!("XDG state directory is unavailable"))
}

fn telemetry_path() -> anyhow::Result<PathBuf> {
    dirs::state_dir()
        .or_else(dirs::data_local_dir)
        .map(|path| path.join("codex-native/macro-telemetry.jsonl"))
        .ok_or_else(|| anyhow!("XDG state directory is unavailable"))
}

fn append_private_json_line<T: Serialize>(path: &Path, value: &T) -> anyhow::Result<()> {
    let parent = path.parent().context("telemetry path has no parent")?;
    fs::create_dir_all(parent).with_context(|| format!("failed to create {}", parent.display()))?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_data()?;
    Ok(())
}

fn read_telemetry(path: &Path) -> anyhow::Result<Vec<TelemetryRecord>> {
    if !path.is_file() {
        return Ok(Vec::new());
    }
    let file = fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let mut records = std::io::BufRead::lines(reader)
        .map_while(Result::ok)
        .filter_map(|line| serde_json::from_str::<TelemetryRecord>(&line).ok())
        .collect::<Vec<_>>();
    if records.len() > MAX_TELEMETRY_SAMPLES {
        records.drain(..records.len() - MAX_TELEMETRY_SAMPLES);
    }
    Ok(records)
}

fn compact_label(label: &str) -> String {
    truncate_text(&label.split_whitespace().collect::<Vec<_>>().join(" "), 120)
}

fn truncate_text(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let mut result = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        result.push('…');
    }
    result
}

fn truncate_utf8_bytes(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut boundary = max_bytes;
    while boundary > 0 && !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value[..boundary].to_owned()
}

fn bound_text_output(value: String, max_bytes: usize) -> (String, u64, bool) {
    let total = value.len() as u64;
    if value.len() <= max_bytes {
        return (value, total, false);
    }
    (truncate_utf8_bytes(&value, max_bytes), total, true)
}

fn action_is_cacheable(action: &MacroAction, include_read_only_commands: bool) -> bool {
    matches!(
        action,
        MacroAction::ReadFile { .. } | MacroAction::Search { .. } | MacroAction::ListFiles { .. }
    ) || (include_read_only_commands
        && matches!(
            action,
            MacroAction::RunCommand {
                mutating: false,
                ..
            }
        ))
}

fn cache_key(workspace: &Path, action: &MacroAction) -> anyhow::Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(b"codex-native-macro-cache-v1\0");
    hasher.update(serde_json::to_vec(action)?);
    hasher.update(b"\0");
    match action {
        MacroAction::ReadFile { path, .. } => {
            let path = resolve_existing(workspace, path)?;
            hasher.update(fs::read(path)?);
        }
        MacroAction::Search { paths, .. } | MacroAction::ListFiles { paths, .. } => {
            hasher.update(workspace_metadata_fingerprint(workspace, paths)?);
        }
        MacroAction::RunCommand {
            mutating: false, ..
        } => {
            hasher.update(workspace_metadata_fingerprint(workspace, &[])?);
        }
        _ => bail!("action is not cacheable"),
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn workspace_metadata_fingerprint(workspace: &Path, paths: &[String]) -> anyhow::Result<Vec<u8>> {
    let roots = search_roots(workspace, paths)?;
    let mut files_seen = 0_usize;
    let mut entries = Vec::<(String, u64, u128)>::new();
    for root in roots {
        walk_files(&root, &mut files_seen, |path| {
            let metadata = path.metadata()?;
            let modified = metadata
                .modified()
                .ok()
                .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|value| value.as_nanos())
                .unwrap_or(0);
            entries.push((
                path.strip_prefix(workspace)
                    .unwrap_or(path)
                    .to_string_lossy()
                    .into_owned(),
                metadata.len(),
                modified,
            ));
            Ok(true)
        })?;
    }
    entries.sort();
    let mut hasher = Sha256::new();
    for (path, bytes, modified) in entries {
        hasher.update(path.as_bytes());
        hasher.update([0]);
        hasher.update(bytes.to_le_bytes());
        hasher.update(modified.to_le_bytes());
    }
    Ok(hasher.finalize().to_vec())
}

fn finalize_step_output(
    invocation_id: &str,
    step_id: &str,
    options: &StepOutputOptions,
    result: &mut StepResult,
) -> anyhow::Result<()> {
    let raw = result.output.clone().unwrap_or_default();
    result.raw_output = (!raw.is_empty()).then_some(raw.clone());
    if raw.is_empty() {
        result.preview_bytes = 0;
        return Ok(());
    }
    let max_bytes = options
        .max_output_tokens
        .map(|tokens| tokens.saturating_mul(4))
        .unwrap_or_else(|| {
            if options.mode == OutputMode::Full {
                MAX_STRUCTURED_OUTPUT_BYTES
            } else {
                DEFAULT_OUTPUT_PREVIEW_TOKENS * 4
            }
        })
        .clamp(256, MAX_STRUCTURED_OUTPUT_BYTES);
    let mode = if options.mode == OutputMode::Auto {
        if raw.len() <= max_bytes {
            OutputMode::Full
        } else {
            OutputMode::HeadTail
        }
    } else {
        options.mode
    };
    let mut preview = match mode {
        OutputMode::Auto => unreachable!(),
        OutputMode::Full => raw.clone(),
        OutputMode::HeadTail => head_tail_output(
            &raw,
            options.head_lines.unwrap_or(40),
            options.tail_lines.unwrap_or(40),
        ),
        OutputMode::ErrorsOnly if result.status == "failed" => head_tail_output(
            &raw,
            options.head_lines.unwrap_or(80),
            options.tail_lines.unwrap_or(80),
        ),
        OutputMode::ErrorsOnly => filter_output_lines(&raw, |line| {
            let line = line.to_ascii_lowercase();
            ["error", "failed", "warning", "panic"]
                .iter()
                .any(|needle| line.contains(needle))
        }),
        OutputMode::MatchesOnly => {
            let needle = options.contains.to_ascii_lowercase();
            filter_output_lines(&raw, |line| line.to_ascii_lowercase().contains(&needle))
        }
        OutputMode::None => String::new(),
    };
    if preview.len() > max_bytes {
        preview = if matches!(mode, OutputMode::HeadTail | OutputMode::ErrorsOnly) {
            truncate_head_tail_bytes(&preview, max_bytes)
        } else {
            truncate_utf8_bytes(&preview, max_bytes)
        };
    }
    let preview_was_reduced = preview != raw;
    if options.save_full_output || preview_was_reduced || result.output_truncated {
        result.output_handle = Some(save_output_artifact(invocation_id, step_id, &raw)?);
    }
    result.preview_bytes = preview.len() as u64;
    result.output_truncated |= preview_was_reduced;
    result.output = (!preview.is_empty()).then_some(preview);
    Ok(())
}

fn filter_output_lines(raw: &str, mut keep: impl FnMut(&str) -> bool) -> String {
    raw.lines()
        .filter(|line| keep(line))
        .collect::<Vec<_>>()
        .join("\n")
}

fn truncate_head_tail_bytes(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    const MARKER: &str = "\n[preview byte limit; middle omitted]\n";
    if max_bytes <= MARKER.len() + 2 {
        return truncate_utf8_bytes(value, max_bytes);
    }
    let available = max_bytes - MARKER.len();
    let head_bytes = available / 2;
    let tail_bytes = available - head_bytes;
    let head = truncate_utf8_bytes(value, head_bytes);
    let mut tail_start = value.len().saturating_sub(tail_bytes);
    while tail_start < value.len() && !value.is_char_boundary(tail_start) {
        tail_start += 1;
    }
    format!("{head}{MARKER}{}", &value[tail_start..])
}

fn head_tail_output(raw: &str, head_lines: usize, tail_lines: usize) -> String {
    let lines = raw.lines().collect::<Vec<_>>();
    if lines.len() <= head_lines.saturating_add(tail_lines) {
        return raw.to_owned();
    }
    let omitted = lines.len() - head_lines - tail_lines;
    let mut selected = lines[..head_lines]
        .iter()
        .map(|line| (*line).to_owned())
        .collect::<Vec<_>>();
    selected.push(format!("[{omitted} lines omitted; full output retained]"));
    selected.extend(
        lines[lines.len() - tail_lines..]
            .iter()
            .map(|line| (*line).to_owned()),
    );
    selected.join("\n")
}

fn output_artifact_root() -> anyhow::Result<PathBuf> {
    dirs::state_dir()
        .or_else(dirs::data_local_dir)
        .map(|path| path.join("codex-native/macro-outputs"))
        .ok_or_else(|| anyhow!("XDG state directory is unavailable"))
}

fn save_output_artifact(
    _invocation_id: &str,
    _step_id: &str,
    output: &str,
) -> anyhow::Result<String> {
    let root = output_artifact_root()?;
    save_output_artifact_at(&root, output)
}

fn save_output_artifact_at(root: &Path, output: &str) -> anyhow::Result<String> {
    fs::create_dir_all(root)?;
    fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
    let handle = format!("out_{}", Uuid::new_v4().simple());
    let path = root.join(format!("{handle}.txt"));
    let mut file = fs::File::create(&path)?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    file.write_all(output.as_bytes())?;
    file.sync_data()?;
    Ok(handle)
}

fn read_output_artifact(request: &OutputReadRequest) -> anyhow::Result<Value> {
    let root = output_artifact_root()?;
    read_output_artifact_at(&root, request)
}

fn read_output_artifact_at(root: &Path, request: &OutputReadRequest) -> anyhow::Result<Value> {
    validate_output_handle(&request.handle)?;
    let path = root.join(format!("{}.txt", request.handle));
    let metadata = fs::symlink_metadata(&path).with_context(|| {
        format!(
            "macro output handle is missing or expired: {}",
            request.handle
        )
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        bail!("macro output handle is not a regular retained artifact");
    }
    let text = fs::read_to_string(&path).context("retained macro output is not UTF-8")?;
    let start = request.start_line.unwrap_or(1);
    let end = request.end_line.unwrap_or(usize::MAX);
    if end < start {
        bail!("output line range is invalid");
    }
    let selected = text
        .lines()
        .enumerate()
        .filter(|(index, _)| {
            let line = index + 1;
            line >= start && line <= end
        })
        .map(|(index, line)| format!("{}:{}", index + 1, line))
        .collect::<Vec<_>>()
        .join("\n");
    let max_bytes = request
        .max_bytes
        .unwrap_or(64 * 1024)
        .clamp(1_024, MAX_ARTIFACT_READ_BYTES);
    let (output, selected_bytes, truncated) = bound_text_output(selected, max_bytes);
    Ok(json!({
        "handle": request.handle,
        "output": output,
        "selectedBytes": selected_bytes,
        "artifactBytes": metadata.len(),
        "truncated": truncated,
        "retentionSeconds": ARTIFACT_RETENTION_SECONDS
    }))
}

fn validate_output_handle(handle: &str) -> anyhow::Result<()> {
    if handle.len() == 36
        && handle.starts_with("out_")
        && handle[4..].bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Ok(());
    }
    bail!("invalid macro output handle")
}

fn cleanup_output_artifacts() {
    let Ok(root) = output_artifact_root() else {
        return;
    };
    let Ok(read_dir) = fs::read_dir(&root) else {
        return;
    };
    let now = std::time::SystemTime::now();
    let mut files = read_dir
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).ok()?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return None;
            }
            let modified = metadata.modified().unwrap_or(std::time::UNIX_EPOCH);
            Some((path, metadata.len(), modified))
        })
        .collect::<Vec<_>>();
    for (path, _, modified) in &files {
        if now
            .duration_since(*modified)
            .is_ok_and(|age| age.as_secs() > ARTIFACT_RETENTION_SECONDS)
        {
            let _ = fs::remove_file(path);
        }
    }
    files.retain(|(path, _, _)| path.is_file());
    files.sort_by_key(|(_, _, modified)| *modified);
    let mut total = files.iter().map(|(_, bytes, _)| *bytes).sum::<u64>();
    while files.len() > MAX_ARTIFACT_FILES || total > MAX_ARTIFACT_BYTES {
        let (path, bytes, _) = files.remove(0);
        if fs::remove_file(path).is_ok() {
            total = total.saturating_sub(bytes);
        }
    }
}

fn build_mutation_plan(request: &MacroRequest) -> anyhow::Result<Vec<MutationPlanEntry>> {
    request
        .steps
        .iter()
        .filter_map(|step| {
            let entry = match &step.action {
                MacroAction::WriteFiles { files, .. } => MutationPlanEntry {
                    step: step.id.clone(),
                    action: "write_files".into(),
                    targets: files.iter().map(|file| file.path.clone()).collect(),
                    lock: "sorted-file-exclusive".into(),
                    stale_write_protected: files.iter().all(|file| file.expected_sha256.is_some()),
                },
                MacroAction::ApplyPatch {
                    patch,
                    expected_files,
                } => MutationPlanEntry {
                    step: step.id.clone(),
                    action: "apply_patch".into(),
                    targets: match patch_paths(patch) {
                        Ok(paths) => paths,
                        Err(error) => return Some(Err(error)),
                    },
                    lock: "sorted-file-exclusive".into(),
                    stale_write_protected: !expected_files.is_empty(),
                },
                MacroAction::RunCommand {
                    argv,
                    cwd,
                    mutating: true,
                    ..
                } => MutationPlanEntry {
                    step: step.id.clone(),
                    action: format!(
                        "run_command:{}",
                        argv.first().map(String::as_str).unwrap_or("unknown")
                    ),
                    targets: vec![if cwd.is_empty() {
                        ".".into()
                    } else {
                        cwd.clone()
                    }],
                    lock: "workspace-exclusive".into(),
                    stale_write_protected: false,
                },
                _ => return None,
            };
            Some(Ok(entry))
        })
        .collect()
}

fn format_mutation_plan(plan: &[MutationPlanEntry], dry_run: bool) -> String {
    format!(
        "{} mutation plan: {} step(s), {} target(s)",
        if dry_run { "Dry-run" } else { "Validated" },
        plan.len(),
        plan.iter().map(|entry| entry.targets.len()).sum::<usize>()
    )
}

fn workload_class(steps: &[MacroStep]) -> String {
    let mut reads = 0_usize;
    let mut mutations = 0_usize;
    for step in steps {
        match &step.action {
            MacroAction::ReadFile { .. }
            | MacroAction::Search { .. }
            | MacroAction::ListFiles { .. }
            | MacroAction::RunCommand {
                mutating: false, ..
            } => reads += 1,
            MacroAction::WriteFiles { .. }
            | MacroAction::ApplyPatch { .. }
            | MacroAction::RunCommand { mutating: true, .. } => mutations += 1,
        }
    }
    match (reads > 0, mutations > 0) {
        (true, false) => "read",
        (false, true) => "mutate",
        (true, true) => "mixed",
        (false, false) => "empty",
    }
    .into()
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
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

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn request(steps: Vec<MacroStep>) -> MacroRequest {
        MacroRequest {
            workspace_root: "/tmp".into(),
            label: "test".into(),
            max_parallel: Some(3),
            stop_on_error: true,
            dry_run: false,
            steps,
        }
    }

    fn read_step(id: &str, dependencies: &[&str]) -> MacroStep {
        MacroStep {
            id: id.into(),
            depends_on: dependencies.iter().map(|value| (*value).into()).collect(),
            when: None,
            cache: CachePolicy::Auto,
            output: StepOutputOptions::default(),
            action: MacroAction::ReadFile {
                path: "README.md".into(),
                start_line: None,
                end_line: None,
            },
        }
    }

    #[test]
    fn server_arguments_are_clamped() {
        let config = MacroServerConfig::from_args(
            [
                "--max-parallel",
                "99",
                "--memory-mib",
                "64",
                "--timeout-seconds",
                "99999",
                "--tasks-max",
                "2",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap();
        assert_eq!(
            config,
            MacroServerConfig {
                max_parallel: 4,
                memory_mib: 512,
                timeout_seconds: 1_800,
                tasks_max: 16,
                allowed_roots: Vec::new(),
            }
        );
    }

    #[test]
    fn graph_rejects_cycles_and_unknown_dependencies() {
        let cyclic = request(vec![read_step("a", &["b"]), read_step("b", &["a"])]);
        assert!(
            validate_request(&cyclic)
                .unwrap_err()
                .to_string()
                .contains("cycle")
        );

        let unknown = request(vec![read_step("a", &[]), read_step("b", &["missing"])]);
        assert!(
            validate_request(&unknown)
                .unwrap_err()
                .to_string()
                .contains("unknown step")
        );
    }

    #[test]
    fn paths_and_dangerous_commands_are_rejected() {
        assert!(validate_relative_path("../secret").is_err());
        assert!(validate_relative_path("/etc/passwd").is_err());
        assert!(validate_argv(&["bash".into(), "-lc".into(), "true".into()]).is_err());
        assert!(validate_argv(&["git".into(), "push".into()]).is_err());
        assert!(validate_argv(&["cargo".into(), "test".into()]).is_ok());
    }

    #[test]
    fn write_targets_cannot_escape_through_symlinks() {
        let temporary = tempdir().unwrap();
        let workspace = temporary.path().join("workspace");
        let outside = temporary.path().join("outside");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, workspace.join("escape")).unwrap();
        let workspace = workspace.canonicalize().unwrap();
        assert!(resolve_write_target(&workspace, "escape/file", false).is_err());
    }

    #[test]
    fn workspace_must_be_inside_an_explicit_allowed_root() {
        let temporary = tempdir().unwrap();
        let allowed = temporary.path().join("allowed");
        let child = allowed.join("child");
        let outside = temporary.path().join("outside");
        fs::create_dir_all(&child).unwrap();
        fs::create_dir_all(&outside).unwrap();
        let allowed = allowed.canonicalize().unwrap();
        assert_eq!(
            canonical_workspace(child.to_str().unwrap(), std::slice::from_ref(&allowed)).unwrap(),
            (child.canonicalize().unwrap(), allowed.clone())
        );
        assert!(canonical_workspace(outside.to_str().unwrap(), &[allowed]).is_err());
        assert!(canonical_workspace(child.to_str().unwrap(), &[]).is_err());
    }

    #[test]
    fn bounded_reader_drains_but_retains_only_the_limit() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let result = runtime
            .block_on(read_bounded(std::io::Cursor::new(vec![b'x'; 4096]), 128))
            .unwrap();
        assert_eq!(result.bytes.len(), 128);
        assert_eq!(result.total_bytes, 4096);
        assert!(result.truncated);
    }

    #[test]
    fn time_metric_is_removed_from_visible_stderr() {
        let mut stderr = b"warning\n__CODEX_NATIVE_MAX_RSS_KIB__=1234\n".to_vec();
        assert_eq!(strip_time_metric(&mut stderr), Some(1_263_616));
        assert_eq!(stderr, b"warning\n");
    }

    #[test]
    fn sandbox_is_networkless_and_does_not_mount_host_home() {
        let temporary = tempdir().unwrap();
        let workspace = temporary.path().join("workspace");
        let private_home = temporary.path().join("home");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&private_home).unwrap();
        let args = sandbox_arguments(
            &workspace,
            Path::new(""),
            &private_home,
            false,
            &["cargo".into(), "test".into()],
        )
        .unwrap();
        assert!(args.iter().any(|argument| argument == "--unshare-net"));
        assert!(args.windows(3).any(|window| {
            window[0] == "--ro-bind"
                && window[1] == workspace.to_string_lossy()
                && window[2] == "/workspace"
        }));
        assert!(!args.iter().any(|argument| argument == &home_path_string()));
    }

    fn home_path_string() -> String {
        dirs::home_dir()
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn independent_file_reads_share_workspace_lock() {
        let locks = WorkspaceLocks::default();
        let first = locks.read_workspace().await;
        let second = timeout(Duration::from_millis(50), locks.read_workspace())
            .await
            .unwrap();
        drop(first);
        drop(second);
    }

    #[test]
    fn compact_output_retains_exact_artifact_for_later_reads() {
        let temporary = tempdir().unwrap();
        let output = (1..=200)
            .map(|line| format!("line {line}"))
            .collect::<Vec<_>>()
            .join("\n");
        let handle = save_output_artifact_at(temporary.path(), &output).unwrap();
        let result = read_output_artifact_at(
            temporary.path(),
            &OutputReadRequest {
                handle: handle.clone(),
                start_line: Some(99),
                end_line: Some(101),
                max_bytes: Some(4_096),
            },
        )
        .unwrap();
        assert_eq!(result["handle"], handle);
        assert_eq!(result["output"], "99:line 99\n100:line 100\n101:line 101");
        assert!(!result["truncated"].as_bool().unwrap());
        let preview = head_tail_output(&output, 2, 2);
        assert!(preview.contains("196 lines omitted"));
        assert!(preview.contains("line 200"));
    }

    #[tokio::test]
    async fn stale_hash_blocks_atomic_file_write() {
        let temporary = tempdir().unwrap();
        let workspace = temporary.path().canonicalize().unwrap();
        let path = workspace.join("value.txt");
        fs::write(&path, "current").unwrap();
        let error = execute_write_files(
            &workspace,
            &WorkspaceLocks::default(),
            vec![FileWrite {
                path: "value.txt".into(),
                content: "replacement".into(),
                expected_sha256: Some(format!("{:064x}", 1)),
            }],
            false,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("stale write refused"));
        assert_eq!(fs::read_to_string(path).unwrap(), "current");
    }

    #[tokio::test]
    async fn hash_checked_patch_supports_dry_run_then_apply() {
        let temporary = tempdir().unwrap();
        let workspace = temporary.path().canonicalize().unwrap();
        let path = workspace.join("value.txt");
        fs::write(&path, "old\n").unwrap();
        let expected = sha256_file(&path).unwrap();
        let patch = "--- a/value.txt\n+++ b/value.txt\n@@ -1 +1 @@\n-old\n+new\n";
        let files = vec![ExpectedFileHash {
            path: "value.txt".into(),
            sha256: expected,
        }];
        let locks = WorkspaceLocks::default();
        execute_apply_patch(&workspace, &locks, patch, &files, true)
            .await
            .unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "old\n");
        execute_apply_patch(&workspace, &locks, patch, &files, false)
            .await
            .unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "new\n");
    }

    #[test]
    fn conditions_can_branch_on_failed_step_output() {
        let condition = StepCondition {
            step: "build".into(),
            status: Some("failed".into()),
            exit_code: Some(1),
            output_contains: "expected failure".into(),
            output_not_contains: "panic".into(),
        };
        let mut result = StepResult::failed("build".into(), Duration::ZERO, "failed");
        result.exit_code = Some(1);
        result.raw_output = Some("expected failure".into());
        assert!(condition_matches(
            &condition,
            &RuntimeStepState::Failed,
            Some(&result)
        ));
    }

    #[test]
    fn read_cache_key_changes_with_file_content() {
        let temporary = tempdir().unwrap();
        let workspace = temporary.path().canonicalize().unwrap();
        fs::write(workspace.join("value.txt"), "first").unwrap();
        let action = MacroAction::ReadFile {
            path: "value.txt".into(),
            start_line: None,
            end_line: None,
        };
        let first = cache_key(&workspace, &action).unwrap();
        fs::write(workspace.join("value.txt"), "second").unwrap();
        let second = cache_key(&workspace, &action).unwrap();
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn reusable_cache_serves_unchanged_read_step() {
        let temporary = tempdir().unwrap();
        let workspace = temporary.path().canonicalize().unwrap();
        fs::write(workspace.join("value.txt"), "stable\n").unwrap();
        let make_step = || MacroStep {
            id: "read".into(),
            depends_on: Vec::new(),
            when: None,
            cache: CachePolicy::Auto,
            output: StepOutputOptions::default(),
            action: MacroAction::ReadFile {
                path: "value.txt".into(),
                start_line: None,
                end_line: None,
            },
        };
        let locks = Arc::new(WorkspaceLocks::default());
        let cache = Arc::new(Mutex::new(ReadCache::default()));
        let first = execute_step(
            make_step(),
            workspace.clone(),
            locks.clone(),
            MacroServerConfig::default(),
            cache.clone(),
            "first",
            false,
        )
        .await;
        let second = execute_step(
            make_step(),
            workspace,
            locks,
            MacroServerConfig::default(),
            cache,
            "second",
            false,
        )
        .await;
        assert!(!first.cache_hit);
        assert!(second.cache_hit);
        assert_eq!(first.output, second.output);
    }

    #[tokio::test]
    async fn write_dry_run_validates_without_creating_parents() {
        let temporary = tempdir().unwrap();
        let workspace = temporary.path().canonicalize().unwrap();
        let files = vec![FileWrite {
            path: "new/child.txt".into(),
            content: "value".into(),
            expected_sha256: Some("missing".into()),
        }];
        execute_write_files_dry_run(&workspace, &WorkspaceLocks::default(), &files, true)
            .await
            .unwrap();
        assert!(!workspace.join("new").exists());
    }

    #[test]
    fn dry_run_plan_lists_locks_and_stale_write_protection() {
        let mut request = request(vec![
            read_step("inspect", &[]),
            MacroStep {
                id: "change".into(),
                depends_on: vec!["inspect".into()],
                when: None,
                cache: CachePolicy::Off,
                output: StepOutputOptions::default(),
                action: MacroAction::WriteFiles {
                    files: vec![FileWrite {
                        path: "value.txt".into(),
                        content: "new".into(),
                        expected_sha256: Some("missing".into()),
                    }],
                    create_parents: false,
                },
            },
        ]);
        request.dry_run = true;
        let plan = build_mutation_plan(&request).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].lock, "sorted-file-exclusive");
        assert!(plan[0].stale_write_protected);
    }

    #[test]
    fn tool_schema_exposes_every_context_saving_feature() {
        let schema = serde_json::to_string(&tool_definitions()).unwrap();
        for feature in [
            OUTPUT_TOOL_NAME,
            "max_output_tokens",
            "save_full_output",
            "apply_patch",
            "expected_sha256",
            "output_contains",
            "\"refresh\"",
            "dry_run",
        ] {
            assert!(schema.contains(feature), "missing schema feature {feature}");
        }
    }

    #[test]
    fn telemetry_percentiles_are_deterministic() {
        assert_eq!(percentile(&[10, 20, 30, 40], 50), 30);
        assert_eq!(percentile(&[10, 20, 30, 40], 95), 40);
        assert_eq!(percentile(&[], 95), 0);
    }
}
