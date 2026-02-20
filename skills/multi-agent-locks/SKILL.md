---
name: multi-agent-locks
description: Mandatory skill for any agent that intends to make file changes in parallel contexts. Coordinate concurrent coding agents with hard file-level locks backed by centralized SQLite leases by mapping files first, locking before edits, skipping already-locked files, heartbeating during work, and releasing locks on exit.
---

# Multi Agent Locks

Follow this protocol exactly.

## Required workflow

1. Map candidate files before the first edit.
2. Acquire locks for mapped files.
3. Continue only with acquired files.
4. Skip locked files and keep making progress on unlocked files.
5. Heartbeat while edits are in progress.
6. Release all held locks on normal completion and on `EXIT`/`INT`/`TERM`.

## Lock defaults

- Lock mode: hard
- Conflict behavior: skip locked files
- Scope: file-level only
- Lease TTL: 180 seconds
- Heartbeat cadence: 30 seconds
- Owner format: `<agent-name>:<pid>:<session-id>`
- Force unlock: unsupported

## Bundled CLI

Use `scripts/multi_agent_locks.ts`.

The SQLite database path is hardcoded to:

- `assets/multi_agent_locks.db` (relative to this skill directory)

### Commands

- `acquire --owner <owner> [--task-id <id>] [--repo-root <path>] [--ttl 180] <file...>`
- `heartbeat --owner <owner> [--ttl 180] <file...>`
- `release --owner <owner> <file...>`
- `status [file...]`

Pass `--json` when command output needs to be parsed by automation.

## Minimal shell pattern

```bash
OWNER="codex:$$:${SESSION_ID}"

cleanup() {
  bun scripts/multi_agent_locks.ts release --owner "$OWNER" -- "${LOCKED_FILES[@]}"
}
trap cleanup EXIT INT TERM

# Acquire, then continue only with returned acquired files.
bun scripts/multi_agent_locks.ts acquire --owner "$OWNER" --json -- path/a.ts path/b.ts
```

## Behavioral requirements

- Use normalized absolute realpaths as lock keys.
- Treat expired leases as reclaimable during acquire.
- Keep lock ownership strict: only the owner may heartbeat or release its locks.
