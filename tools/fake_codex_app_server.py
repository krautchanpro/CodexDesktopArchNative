#!/usr/bin/env python3
"""Deterministic app-server fixture for Codex Native UI smoke tests.

It never contacts a model or account. The fixture deliberately rejects
turn/start until thread/resume has succeeded so the existing-task composer
smoke catches regressions that would surface as "thread not found".
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any


THREAD_ID = "existing-task-send-fixture"
SECOND_THREAD_ID = "second-existing-task-fixture"
STALE_STEER_THREAD_ID = "stale-steer-recovery-fixture"
HISTORY_RECOVERY_THREAD_ID = "019f9f86-8517-7bd3-ae7a-43146df7e5f7"
BULK_ARCHIVE_THREAD_IDS = (
    "bulk-archive-fixture-one",
    "bulk-archive-fixture-two",
)
BULK_DELETE_THREAD_IDS = (
    "bulk-delete-fixture-one",
    "bulk-delete-fixture-two",
)


def fixture_thread(thread_id: str, name: str, updated_at: int) -> dict[str, Any]:
    return {
        "id": thread_id,
        "preview": name,
        "name": name,
        "cwd": str(Path(__file__).resolve().parents[1]),
        "createdAt": 1,
        "updatedAt": updated_at,
        "status": {"type": "idle"},
        "turns": [],
    }


THREADS: dict[str, dict[str, Any]] = {
    THREAD_ID: fixture_thread(THREAD_ID, "Existing task send fixture", 3),
    SECOND_THREAD_ID: fixture_thread(
        SECOND_THREAD_ID, "Second existing task fixture", 2
    ),
    STALE_STEER_THREAD_ID: fixture_thread(
        STALE_STEER_THREAD_ID, "Stale steer recovery fixture", 1
    ),
    BULK_ARCHIVE_THREAD_IDS[0]: fixture_thread(
        BULK_ARCHIVE_THREAD_IDS[0], "Bulk archive fixture one", 7
    ),
    BULK_ARCHIVE_THREAD_IDS[1]: fixture_thread(
        BULK_ARCHIVE_THREAD_IDS[1], "Bulk archive fixture two", 6
    ),
    BULK_DELETE_THREAD_IDS[0]: fixture_thread(
        BULK_DELETE_THREAD_IDS[0], "Bulk delete fixture one", 5
    ),
    BULK_DELETE_THREAD_IDS[1]: fixture_thread(
        BULK_DELETE_THREAD_IDS[1], "Bulk delete fixture two", 4
    ),
}
ARCHIVED_THREADS: dict[str, dict[str, Any]] = {}

THREAD_SETTINGS: dict[str, dict[str, Any]] = {
    THREAD_ID: {
        "model": "gpt-smoke",
        "effort": "max",
        # Current app-server reports the normal tier as "default" even though
        # settings/update accepts null for Standard.
        "serviceTier": "default",
        "sandboxPolicy": {"type": "workspaceWrite"},
        "approvalPolicy": "on-request",
    },
    SECOND_THREAD_ID: {
        "model": "gpt-smoke-alt",
        "effort": "low",
        "serviceTier": "priority",
        "sandboxPolicy": {"type": "readOnly"},
        "approvalPolicy": "never",
    },
    STALE_STEER_THREAD_ID: {
        "model": "gpt-smoke",
        "effort": "high",
        "serviceTier": "default",
        "sandboxPolicy": {"type": "workspaceWrite"},
        "approvalPolicy": "on-request",
    },
    BULK_ARCHIVE_THREAD_IDS[0]: {
        "model": "gpt-smoke",
        "effort": "medium",
        "serviceTier": "default",
        "sandboxPolicy": {"type": "workspaceWrite"},
        "approvalPolicy": "on-request",
    },
    BULK_ARCHIVE_THREAD_IDS[1]: {
        "model": "gpt-smoke",
        "effort": "medium",
        "serviceTier": "default",
        "sandboxPolicy": {"type": "workspaceWrite"},
        "approvalPolicy": "on-request",
    },
    BULK_DELETE_THREAD_IDS[0]: {
        "model": "gpt-smoke",
        "effort": "medium",
        "serviceTier": "default",
        "sandboxPolicy": {"type": "workspaceWrite"},
        "approvalPolicy": "on-request",
    },
    BULK_DELETE_THREAD_IDS[1]: {
        "model": "gpt-smoke",
        "effort": "medium",
        "serviceTier": "default",
        "sandboxPolicy": {"type": "workspaceWrite"},
        "approvalPolicy": "on-request",
    },
}


def install_history_recovery_fixture() -> None:
    codex_home = os.environ.get("CODEX_HOME")
    if not codex_home:
        raise RuntimeError(
            "CODEX_HOME is required with CODEX_NATIVE_FAKE_HISTORY_RECOVERY"
        )
    directory = Path(codex_home) / "sessions" / "2026" / "07" / "26"
    directory.mkdir(parents=True, exist_ok=True)
    rollout = directory / (
        "rollout-2026-07-26T00-00-00-"
        f"{HISTORY_RECOVERY_THREAD_ID}.jsonl"
    )
    turn_id = "019f9f86-857b-7493-898e-0b93180d2a18"
    records = [
        {
            "ordinal": 0,
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": None,
                "rate_limits": None,
            },
        },
        {
            "ordinal": 1,
            "type": "event_msg",
            "payload": {
                "type": "task_started",
                "turn_id": turn_id,
                "started_at": 10,
            },
        },
        {
            "ordinal": 2,
            "type": "event_msg",
            "payload": {
                "type": "item_completed",
                "thread_id": HISTORY_RECOVERY_THREAD_ID,
                "turn_id": turn_id,
                "item": {
                    "type": "UserMessage",
                    "id": "history-recovery-user-1",
                    "content": [
                        {
                            "type": "text",
                            "text": "Canonical iOS prompt missing from the stale projection.",
                        }
                    ],
                },
            },
        },
        {
            "ordinal": 3,
            "type": "event_msg",
            "payload": {
                "type": "item_completed",
                "thread_id": HISTORY_RECOVERY_THREAD_ID,
                "turn_id": turn_id,
                "item": {
                    "type": "AgentMessage",
                    "id": "history-recovery-agent-1",
                    "content": [
                        {
                            "type": "Text",
                            "text": "Canonical iOS answer restored from the rollout.",
                        }
                    ],
                    "phase": "final_answer",
                },
            },
        },
        {
            "ordinal": 4,
            "type": "event_msg",
            "payload": {
                "type": "task_complete",
                "turn_id": turn_id,
                "started_at": 10,
                "completed_at": 20,
            },
        },
    ]
    rollout.write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )
    first_line_bytes = len(
        (json.dumps(records[0], separators=(",", ":")) + "\n").encode("utf-8")
    )
    history_database = Path(codex_home) / "thread_history_1.sqlite"
    with sqlite3.connect(history_database) as database:
        database.executescript(
            """
            CREATE TABLE thread_history_projection_state (
                thread_id TEXT PRIMARY KEY,
                next_rollout_byte_offset INTEGER NOT NULL,
                next_rollout_ordinal INTEGER NOT NULL
            );
            CREATE TABLE thread_items (
                thread_id TEXT NOT NULL,
                turn_id TEXT NOT NULL,
                item_id TEXT NOT NULL,
                rollout_ordinal INTEGER NOT NULL,
                created_at_ms INTEGER NOT NULL,
                item_json TEXT NOT NULL,
                item_type TEXT NOT NULL DEFAULT '',
                PRIMARY KEY(thread_id, turn_id, item_id)
            );
            CREATE TABLE thread_turns (
                thread_id TEXT NOT NULL,
                turn_id TEXT NOT NULL,
                rollout_ordinal INTEGER NOT NULL,
                status TEXT NOT NULL,
                error_json TEXT,
                started_at INTEGER,
                completed_at INTEGER,
                duration_ms INTEGER,
                first_user_item_id TEXT,
                final_agent_item_id TEXT,
                PRIMARY KEY(thread_id, turn_id)
            );
            """
        )
        database.execute(
            "INSERT INTO thread_history_projection_state VALUES (?, ?, ?)",
            (HISTORY_RECOVERY_THREAD_ID, first_line_bytes, 0),
        )
    thread = fixture_thread(
        HISTORY_RECOVERY_THREAD_ID,
        "Stale iOS history recovery fixture",
        8,
    )
    thread["path"] = str(rollout)
    THREADS[HISTORY_RECOVERY_THREAD_ID] = thread
    THREAD_SETTINGS[HISTORY_RECOVERY_THREAD_ID] = {
        "model": "gpt-smoke",
        "effort": "max",
        "serviceTier": "default",
        "sandboxPolicy": {"type": "workspaceWrite"},
        "approvalPolicy": "on-request",
    }


def resumed_thread_result(thread_id: str) -> dict[str, Any]:
    settings = THREAD_SETTINGS[thread_id]
    response = {
        "thread": THREADS[thread_id],
        "model": settings["model"],
        "reasoningEffort": settings["effort"],
        "serviceTier": settings["serviceTier"],
        "sandbox": settings["sandboxPolicy"],
        "approvalPolicy": settings["approvalPolicy"],
    }
    if thread_id == SECOND_THREAD_ID:
        # Exercise the normal modern-server path: resume supplies only a
        # conversational summary, after which Native must automatically ask
        # for the recent full activity page.
        response["initialTurnsPage"] = {
            "data": [
                {
                    "id": f"{thread_id}-turn",
                    "status": "completed",
                    "items": [
                        {
                            "id": f"{thread_id}-summary",
                            "type": "agentMessage",
                            "text": "Second task conversational summary only.",
                        }
                    ],
                }
            ],
            "nextCursor": None,
        }
    return response


def emit_thread_settings(thread_id: str) -> None:
    emit(
        {
            "method": "thread/settings/updated",
            "params": {
                "threadId": thread_id,
                "threadSettings": THREAD_SETTINGS[thread_id],
            },
        }
    )


def emit(message: dict[str, Any]) -> None:
    print(json.dumps(message, separators=(",", ":")), flush=True)


def record(method: str, params: dict[str, Any]) -> None:
    path = os.environ.get("CODEX_NATIVE_FAKE_LOG")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps({"method": method, "params": params}) + "\n")


def result(request_id: Any, value: Any) -> None:
    emit({"id": request_id, "result": value})


def error(request_id: Any, code: int, message: str) -> None:
    emit({"id": request_id, "error": {"code": code, "message": message}})


def main() -> int:
    if sys.argv[1:] != ["app-server", "--stdio"]:
        return 0
    if os.environ.get("CODEX_NATIVE_FAKE_HISTORY_RECOVERY") == "1":
        install_history_recovery_fixture()

    resumed: set[str] = set()
    goals: dict[str, dict[str, Any]] = {}
    cumulative_tokens: dict[str, int] = {}
    current_context_tokens: dict[str, int] = {}
    new_thread_count = 0
    turn_count = 0
    for raw_line in sys.stdin:
        try:
            request = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        request_id = request.get("id")
        method = request.get("method", "")
        params = request.get("params") or {}
        if request_id is None:
            continue
        record(method, params)

        if method == "initialize":
            result(
                request_id,
                {"platform": {"family": "unix", "os": "linux"}},
            )
        elif method == "thread/list":
            threads = (
                []
                if params.get("sourceKinds") or params.get("ancestorThreadId")
                else list(
                    (
                        ARCHIVED_THREADS
                        if params.get("archived")
                        else THREADS
                    ).values()
                )
            )
            result(request_id, {"data": threads, "nextCursor": None})
        elif method == "thread/archive":
            thread_id = params.get("threadId")
            thread = THREADS.pop(thread_id, None)
            if thread is None:
                error(request_id, -32004, "thread not found")
                continue
            ARCHIVED_THREADS[thread_id] = thread
            resumed.discard(thread_id)
            result(request_id, {})
            emit(
                {
                    "method": "thread/archived",
                    "params": {"threadId": thread_id},
                }
            )
        elif method == "thread/unarchive":
            thread_id = params.get("threadId")
            thread = ARCHIVED_THREADS.pop(thread_id, None)
            if thread is None:
                error(request_id, -32004, "thread not found")
                continue
            THREADS[thread_id] = thread
            result(request_id, {})
            emit(
                {
                    "method": "thread/unarchived",
                    "params": {"threadId": thread_id},
                }
            )
        elif method == "thread/delete":
            thread_id = params.get("threadId")
            thread = THREADS.pop(thread_id, None)
            if thread is None:
                thread = ARCHIVED_THREADS.pop(thread_id, None)
            if thread is None:
                error(request_id, -32004, "thread not found")
                continue
            resumed.discard(thread_id)
            goals.pop(thread_id, None)
            result(request_id, {})
            emit(
                {
                    "method": "thread/deleted",
                    "params": {"threadId": thread_id},
                }
            )
        elif method == "thread/resume":
            thread_id = params.get("threadId")
            thread = THREADS.get(thread_id)
            if thread is None:
                error(request_id, -32004, "thread not found")
                continue
            resumed.add(thread_id)
            result(request_id, resumed_thread_result(thread_id))
            if thread_id == STALE_STEER_THREAD_ID:
                # Reproduce a stale local running marker: the client believes a
                # turn is active, while the server no longer has one to steer.
                emit(
                    {
                        "method": "turn/started",
                        "params": {
                            "threadId": thread_id,
                            "turn": {
                                "id": "stale-local-turn",
                                "status": "inProgress",
                                "items": [],
                            },
                        },
                    }
                )
        elif method == "thread/start":
            new_thread_count += 1
            thread_id = f"auto-routing-new-task-{new_thread_count}"
            thread = fixture_thread(
                thread_id,
                f"Auto routing new task {new_thread_count}",
                10 + new_thread_count,
            )
            THREADS[thread_id] = thread
            THREAD_SETTINGS[thread_id] = {
                "model": params.get("model") or "gpt-smoke",
                "effort": (params.get("config") or {}).get(
                    "model_reasoning_effort", "medium"
                ),
                "serviceTier": (
                    "default"
                    if params.get("serviceTier") is None
                    else params.get("serviceTier")
                ),
                "sandboxPolicy": params.get("sandboxPolicy")
                or {
                    "type": {
                        "workspace-write": "workspaceWrite",
                        "read-only": "readOnly",
                        "danger-full-access": "dangerFullAccess",
                    }.get(params.get("sandbox"), "workspaceWrite")
                },
                "approvalPolicy": params.get("approvalPolicy") or "on-request",
            }
            resumed.add(thread_id)
            result(request_id, resumed_thread_result(thread_id))
        elif method == "thread/settings/update":
            thread_id = params.get("threadId")
            if thread_id not in THREAD_SETTINGS:
                error(request_id, -32004, "thread not found")
                continue
            settings = THREAD_SETTINGS[thread_id]
            for key in (
                "model",
                "effort",
                "serviceTier",
                "sandboxPolicy",
                "approvalPolicy",
            ):
                if key in params:
                    settings[key] = (
                        "default"
                        if key == "serviceTier" and params[key] is None
                        else params[key]
                    )
            result(request_id, {})
            emit_thread_settings(thread_id)
        elif method == "thread/inject_items":
            thread_id = params.get("threadId")
            if thread_id not in THREADS:
                error(request_id, -32004, "thread not found")
                continue
            result(request_id, {})
        elif method == "thread/turns/list":
            thread_id = params.get("threadId")
            if thread_id not in THREADS:
                error(request_id, -32004, "thread not found")
                continue
            result(
                request_id,
                {
                    "data": [
                        {
                            "id": f"{thread_id}-turn",
                            "status": "completed",
                            "items": [
                                {
                                    "id": f"{thread_id}-message",
                                    "type": "agentMessage",
                                    "text": (
                                        f"{THREADS[thread_id]['name']} loaded from "
                                        "the local smoke fixture."
                                    ),
                                }
                            ],
                        }
                    ],
                    "nextCursor": None,
                },
            )
        elif method == "thread/goal/get":
            result(request_id, {"goal": goals.get(params.get("threadId"))})
        elif method == "thread/goal/set":
            thread_id = params.get("threadId")
            if thread_id not in THREADS:
                error(request_id, -32004, "thread not found")
                continue
            goal = dict(goals.get(thread_id) or {})
            if "objective" in params:
                goal["objective"] = params["objective"]
            if not goal.get("objective"):
                error(request_id, -32602, "goal objective is required")
                continue
            if "tokenBudget" in params:
                goal["tokenBudget"] = params["tokenBudget"]
            goal["status"] = params.get("status", goal.get("status", "active"))
            goal.setdefault("tokensUsed", 0)
            goal.setdefault("timeUsedSeconds", 0)
            goals[thread_id] = goal
            result(request_id, {"goal": goal})
            emit(
                {
                    "method": "thread/goal/updated",
                    "params": {"threadId": thread_id, "goal": goal},
                }
            )
        elif method == "thread/goal/clear":
            thread_id = params.get("threadId")
            goals.pop(thread_id, None)
            result(request_id, {"goal": None})
            emit(
                {
                    "method": "thread/goal/cleared",
                    "params": {"threadId": thread_id},
                }
            )
        elif method == "turn/start":
            thread_id = params.get("threadId")
            if thread_id not in resumed:
                error(request_id, -32004, "thread not found")
                continue
            turn_count += 1
            turn_id = f"smoke-turn-{turn_count}"
            user_id = f"smoke-user-message-{turn_count}"
            agent_id = f"smoke-agent-message-{turn_count}"
            result(request_id, {"turn": {"id": turn_id}})
            user = {
                "id": user_id,
                "type": "userMessage",
                "content": params.get("input") or [],
            }
            agent = {
                "id": agent_id,
                "type": "agentMessage",
            }
            started_turn = {
                "id": turn_id,
                "status": "inProgress",
                "items": [],
            }
            emit(
                {
                    "method": "turn/started",
                    "params": {"threadId": thread_id, "turn": started_turn},
                }
            )
            emit(
                {
                    "method": "item/started",
                    "params": {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "item": user,
                    },
                }
            )
            time.sleep(0.12)
            emit(
                {
                    "method": "item/completed",
                    "params": {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "item": {"id": user_id, "type": "userMessage"},
                    },
                }
            )
            emit(
                {
                    "method": "item/started",
                    "params": {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "item": {**agent, "text": ""},
                    },
                }
            )
            for delta in ("Message accepted ", "after thread/resume."):
                emit(
                    {
                        "method": "item/agentMessage/delta",
                        "params": {
                            "threadId": thread_id,
                            "turnId": turn_id,
                            "itemId": agent_id,
                            "delta": delta,
                        },
                    }
                )
                time.sleep(0.12)
            emit(
                {
                    "method": "item/completed",
                    "params": {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "item": agent,
                    },
                }
            )
            qwen_context = (params.get("additionalContext") or {}).get(
                "codex-native.qwen-buddy-routing"
            )
            qwen_value = (
                qwen_context.get("value", "")
                if isinstance(qwen_context, dict)
                else ""
            )
            if qwen_value:
                if "local_qwen_agent" in qwen_value:
                    qwen_kind = "agent_session"
                    qwen_tool = (
                        "mcp__local-qwen-delegate__local_qwen_agent"
                    )
                    qwen_prompt_tokens = 3200
                    qwen_output_tokens = 700
                    qwen_saved_tokens = 700
                    qwen_calls = 4
                else:
                    raise RuntimeError("local Qwen routing did not select the OpenCode agent")
                qwen_item = {
                    "id": f"qwen-usage-{turn_id}",
                    "type": "mcpToolCall",
                    "tool": qwen_tool,
                    "status": "completed",
                    "result": {
                        "_meta": {
                            "qwenBuddyUsageEvent": {
                                "schemaVersion": 2,
                                "id": "abcdef0123456789abcdef01",
                                "kind": qwen_kind,
                                "status": "ok",
                                "modelCalls": qwen_calls,
                                "localPromptTokens": qwen_prompt_tokens,
                                "localOutputTokens": qwen_output_tokens,
                                "contextTokensAvoidedApprox": (
                                    qwen_saved_tokens
                                    if qwen_kind == "file_condense"
                                    else 0
                                ),
                                "potentialCodexTokensSavedApprox": (
                                    qwen_saved_tokens
                                ),
                            }
                        }
                    },
                }
                emit(
                    {
                        "method": "item/completed",
                        "params": {
                            "threadId": thread_id,
                            "turnId": turn_id,
                            "item": qwen_item,
                        },
                    }
                )
            emit(
                {
                    "method": "turn/completed",
                    "params": {
                        "threadId": thread_id,
                        "turn": {
                            "id": turn_id,
                            "status": "completed",
                            "items": [],
                        },
                    },
                }
            )
            # Deterministic pressure signal for Lean Context smoke coverage.
            # This is local fixture data and never consumes account usage.
            # `total` is the thread-lifetime cumulative amount; `last` is the
            # latest model call and therefore the useful current-window
            # pressure signal. Keep them deliberately far apart so smoke
            # catches clients that accidentally compact from cumulative usage.
            cumulative_tokens[thread_id] = 4_970_297
            current_context_tokens[thread_id] = 86_000
            emit(
                {
                    "method": "thread/tokenUsage/updated",
                    "params": {
                        "threadId": thread_id,
                        "tokenUsage": {
                            "total": {
                                "totalTokens": cumulative_tokens[thread_id]
                            },
                            "last": {
                                "totalTokens": current_context_tokens[thread_id]
                            },
                            "modelContextWindow": 100000,
                        },
                    },
                }
            )
            # The real Codex core owns automatic compaction. Model that
            # canonical lifecycle without waiting for a client compact RPC.
            time.sleep(0.4)
            compaction_item = {
                "id": f"context-compaction-auto-{turn_id}",
                "type": "contextCompaction",
            }
            emit(
                {
                    "method": "item/started",
                    "params": {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "item": compaction_item,
                    },
                }
            )
            emit(
                {
                    "method": "item/completed",
                    "params": {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "item": compaction_item,
                    },
                }
            )
            current_context_tokens[thread_id] = 30_400
            usage_notification = {
                "method": "thread/tokenUsage/updated",
                "params": {
                    "threadId": thread_id,
                    "tokenUsage": {
                        "total": {"totalTokens": cumulative_tokens[thread_id]},
                        "last": {
                            "totalTokens": current_context_tokens[thread_id]
                        },
                        "modelContextWindow": 100000,
                    },
                },
            }
            emit(usage_notification)
            emit(usage_notification)
        elif method == "thread/compact/start":
            thread_id = params.get("threadId")
            if thread_id not in THREADS:
                error(request_id, -32004, "thread not found")
                continue
            result(
                request_id,
                {
                    "threadId": thread_id,
                    "status": "started",
                    "sameThread": True,
                },
            )
            turn_id = f"manual-compaction-turn-{request_id}"
            compaction_item = {
                "id": f"context-compaction-manual-{request_id}",
                "type": "contextCompaction",
            }
            emit(
                {
                    "method": "item/started",
                    "params": {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "item": compaction_item,
                    },
                }
            )
            emit(
                {
                    "method": "item/completed",
                    "params": {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "item": compaction_item,
                    },
                }
            )
            current_context_tokens[thread_id] = max(
                4_000, int(current_context_tokens.get(thread_id, 60_000) * 0.4)
            )
            usage_notification = {
                "method": "thread/tokenUsage/updated",
                "params": {
                    "threadId": thread_id,
                    "tokenUsage": {
                        "total": {
                            "totalTokens": cumulative_tokens.get(thread_id, 4_970_297)
                        },
                        "last": {
                            "totalTokens": current_context_tokens[thread_id]
                        },
                        "modelContextWindow": 100000,
                    },
                },
            }
            emit(usage_notification)
            # A duplicate telemetry notification must not cause a second
            # automatic compaction when the current window already shrank.
            emit(usage_notification)
        elif method == "turn/steer":
            if params.get("threadId") == STALE_STEER_THREAD_ID:
                error(request_id, -32600, "no active turn to steer")
            else:
                result(request_id, {})
        elif method == "model/list":
            result(
                request_id,
                {
                    "data": [
                        {
                            "id": "gpt-smoke",
                            "displayName": "GPT Smoke",
                            "isDefault": True,
                            "supportedReasoningEfforts": [
                                {"reasoningEffort": effort}
                                for effort in (
                                    "low",
                                    "medium",
                                    "high",
                                    "xhigh",
                                    "max",
                                    "ultra",
                                )
                            ],
                            "serviceTiers": [
                                {"id": "priority", "name": "Fast"}
                            ],
                        },
                        {
                            "id": "gpt-smoke-alt",
                            "displayName": "GPT Smoke Alt",
                            "isDefault": False,
                            "supportedReasoningEfforts": [
                                {"reasoningEffort": effort}
                                for effort in ("low", "high", "max")
                            ],
                            "serviceTiers": [
                                {"id": "priority", "name": "Fast"}
                            ],
                        },
                        *[
                            {
                                "id": f"gpt-5.6-{family}",
                                "displayName": f"GPT-5.6 {family.title()}",
                                "isDefault": False,
                                "supportedReasoningEfforts": [
                                    {"reasoningEffort": effort}
                                    for effort in (
                                        "low",
                                        "medium",
                                        "high",
                                        "xhigh",
                                        "max",
                                        "ultra",
                                    )
                                ],
                                "serviceTiers": [
                                    {"id": "priority", "name": "Fast"}
                                ],
                            }
                            for family in ("luna", "terra", "sol")
                        ],
                    ]
                },
            )
        elif method == "plugin/list":
            result(
                request_id,
                {
                    "marketplaces": [
                        {
                            "name": "openai-bundled",
                            "plugins": [
                                {
                                    "id": "sites@openai-bundled",
                                    "name": "sites",
                                    "installed": True,
                                    "enabled": True,
                                    "localVersion": "0.1.30",
                                    "interface": {
                                        "displayName": "Sites",
                                        "shortDescription": "Build and deploy websites with Sites.",
                                    },
                                    "source": {"type": "remote"},
                                }
                            ],
                        }
                    ],
                    "marketplaceLoadErrors": [],
                },
            )
        elif method == "mcpServerStatus/list":
            if os.environ.get("CODEX_NATIVE_FAKE_QWEN_READY"):
                result(
                    request_id,
                    {
                        "data": [
                            {
                                "name": "local-qwen-delegate",
                                "status": "ready",
                                "tools": {
                                    "local_qwen_agent": {},
                                    "local_qwen_agent_status": {},
                                    "gemini_buddy_delegate": {},
                                },
                            }
                        ]
                    },
                )
            else:
                result(request_id, {"data": []})
        elif method in {
            "skills/list",
            "app/list",
            "permissionProfile/list",
            "experimentalFeature/list",
            "collaborationMode/list",
            "hooks/list",
        }:
            result(request_id, {"data": []})
        elif method == "account/rateLimits/read":
            weekly_used = int(
                os.environ.get("CODEX_NATIVE_FAKE_WEEKLY_USED_PERCENT", "40")
            )
            reset_credits = int(
                os.environ.get("CODEX_NATIVE_FAKE_RESET_CREDITS", "2")
            )
            result(
                request_id,
                {
                    "rateLimits": {
                        "secondary": {
                            "usedPercent": weekly_used,
                            "windowDurationMins": 10080,
                            "resetsAt": 1_800_000_000,
                        }
                    },
                    "rateLimitResetCredits": {
                        "availableCount": reset_credits,
                        "credits": [
                            {"id": "fixture-credit", "status": "available"}
                        ]
                        if reset_credits > 0
                        else [],
                    },
                },
            )
        elif method == "account/rateLimitResetCredit/consume":
            # Fixture-only success. No account or network is ever contacted.
            reset_credits = int(
                os.environ.get("CODEX_NATIVE_FAKE_RESET_CREDITS", "2")
            )
            result(
                request_id,
                {"outcome": "reset" if reset_credits > 0 else "noCredit"},
            )
        elif method == "account/usage/read":
            result(request_id, {})
        elif method == "account/read":
            result(request_id, {"account": None})
        else:
            result(request_id, {})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
