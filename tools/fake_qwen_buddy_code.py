#!/usr/bin/env python3
"""Deterministic quota-independent Buddy fixture for Native UI smoke tests."""

import json
import sys

model_index = sys.argv.index("--model") + 1
provider = sys.argv[model_index]
if provider == "qwen" and "--agent" not in sys.argv:
    print("local Qwen must run through the OpenCode agent path", file=sys.stderr)
    raise SystemExit(2)
if "--progress-jsonl" in sys.argv:
    progress = [
        {"phase": "planning", "detail": "Building a concise task plan"},
        {"phase": "plan_ready", "detail": "Answer from the authorized fixture"},
        {"phase": "plan_step", "detail": "1. Inspect the deterministic fixture"},
        {"phase": "read_started", "detail": "tools/fake_qwen_buddy_code.py L1-L20"},
        {
            "phase": "read_completed",
            "detail": "tools/fake_qwen_buddy_code.py L1-L20 of 44",
        },
        {"phase": "verification", "detail": "Confirm the fixture response is visible"},
    ]
    for event in progress:
        print(
            "QWEN_BUDDY_PROGRESS " + json.dumps(event),
            file=sys.stderr,
            flush=True,
        )
    print(
        'QWEN_BUDDY_PROGRESS {"phase":"working","detail":"Inspecting fixture","modelCalls":1,"promptTokens":120,"outputTokens":30,"totalTokens":150,"contextUsedTokens":150,"contextWindowTokens":65536,"tokensPerSecond":42.5}',
        file=sys.stderr,
        flush=True,
    )
print(
    json.dumps(
        {
            "version": "fixture",
            "provider": provider,
            "status": "ok",
            "model": f"fixture/{provider}",
            "text": "quota-independent Buddy fixture",
            "metrics": {
                "promptTokens": 120,
                "outputTokens": 30,
                "modelCalls": 1,
                "contextUsedTokens": 150,
                "contextWindowTokens": 65536,
                "tokensPerSecond": 42.5,
            },
        }
    )
)
