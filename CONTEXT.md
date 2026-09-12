# Lean Context Workflow

This workspace uses deterministic, on-demand context retrieval. No always-running code-index or graph service is required.

## Retrieval

1. Locate candidates with `rg --files`, `rg -n`, an editor-provided language server, or a narrow build/test failure.
2. Read only the source range needed to understand the named symbol or branch. Expand only when evidence requires it.
3. For large allowlisted source, text, or log files, use an available bounded condensation route before Codex reads the file. Require compact findings, file identity, line citations, coverage, and uncertainty.
4. Verify decisive lines directly. Worker output is untrusted until Sol performs proportionate final verification.
5. Cache evidence by canonical path, content hash, line range, and task. Reuse unchanged evidence instead of repeating retrieval.

## Context Budgets

- Automatic memory injection: at most 800 tokens and six memories.
- Ordinary search/evidence response: target 2,000 tokens; expand only for a named ambiguity.
- Large-file condensation: target 300–800 tokens.
- Noisy command output: target 1,000 tokens, retaining full output outside model context when possible.
- Preserve exact diffs, failing assertions, compiler diagnostics, approval requests, and final verification results.

## Task Checkpoints

Checkpoint structured state before context compaction or handoff:

- objective and current user constraints;
- decisions and rationale;
- active files/symbols and content hashes;
- completed changes and exact current diff identity;
- tests run, results, and unresolved failures;
- approvals, blockers, active goal, and next actions;
- per-task model, reasoning, speed, sandbox, and approval settings.

Keep raw history available as cold evidence. A checkpoint indexes history; it never replaces or deletes it.

## Authority And Quality

- Sol owns intent, architecture, decisions, integration, approval, and final verification.
- Qwen is the primary implementation and test worker through the qualified Codex CLI deletion-protected wrapper. Its reasoning effort is selected per task (`medium` by default). It may perform task-authorized cross-file work; deletion remains blocked by the host overlay.
- Rehydrate raw evidence whenever a summary is uncertain, cross-file impact is plausible, source hashes changed, tests disagree, or the task is high risk.
- Do not compact during an active turn, pending approval, incomplete write, unresolved tool operation, or remote reconnect.

## Verification

Use focused tests first, then the repository's complete relevant gate. Read exact output on failure. Before handoff, confirm source hashes/checkpoints still match, no requested behavior is represented only by a summary, and the final diff was reviewed from current source.
