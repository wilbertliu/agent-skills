#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Sequence

DEFAULT_TTL_SECONDS = 180
DB_ENV_VAR = "MULTI_AGENT_LOCKS_DB"


def normalize_path(raw_path: str) -> str:
    return str(Path(raw_path).expanduser().resolve(strict=False))


def normalize_paths(paths: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for path in paths:
        resolved = normalize_path(path)
        if resolved in seen:
            continue
        seen.add(resolved)
        normalized.append(resolved)
    return normalized


def now_ts() -> int:
    return int(time.time())


def validate_owner(owner: str) -> None:
    # Required format: <agent-name>:<pid>:<session-id>
    if owner.count(":") != 2:
        raise ValueError("owner must use format <agent-name>:<pid>:<session-id>")


def connect_db(db_path: str) -> sqlite3.Connection:
    absolute_path = Path(db_path).expanduser().resolve(strict=False)
    absolute_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(absolute_path), timeout=5, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS file_locks (
            file_path TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            task_id TEXT,
            repo_root TEXT,
            acquired_at INTEGER NOT NULL,
            heartbeat_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_file_locks_expires_at
            ON file_locks (expires_at)
        """
    )
    return conn


def cleanup_expired(conn: sqlite3.Connection, ts: int) -> None:
    conn.execute("DELETE FROM file_locks WHERE expires_at <= ?", (ts,))


def acquire_locks(
    conn: sqlite3.Connection,
    files: Sequence[str],
    owner: str,
    ttl: int,
    task_id: str | None,
    repo_root: str | None,
) -> dict[str, object]:
    timestamp = now_ts()
    expires_at = timestamp + ttl
    acquired: list[str] = []
    locked: list[dict[str, object]] = []

    conn.execute("BEGIN IMMEDIATE")
    try:
        cleanup_expired(conn, timestamp)
        for file_path in normalize_paths(files):
            row = conn.execute(
                "SELECT owner_id, expires_at FROM file_locks WHERE file_path = ?",
                (file_path,),
            ).fetchone()

            if row is None:
                conn.execute(
                    """
                    INSERT INTO file_locks (
                        file_path, owner_id, task_id, repo_root,
                        acquired_at, heartbeat_at, expires_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        file_path,
                        owner,
                        task_id,
                        repo_root,
                        timestamp,
                        timestamp,
                        expires_at,
                    ),
                )
                acquired.append(file_path)
                continue

            existing_owner = str(row["owner_id"])
            if existing_owner == owner:
                conn.execute(
                    """
                    UPDATE file_locks
                    SET heartbeat_at = ?, expires_at = ?, task_id = ?, repo_root = ?
                    WHERE file_path = ?
                    """,
                    (timestamp, expires_at, task_id, repo_root, file_path),
                )
                acquired.append(file_path)
                continue

            locked.append(
                {
                    "file": file_path,
                    "owner": existing_owner,
                    "expires_at": int(row["expires_at"]),
                    "expires_in": max(0, int(row["expires_at"]) - timestamp),
                }
            )

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return {
        "acquired": acquired,
        "locked": locked,
        "owner": owner,
        "ttl_seconds": ttl,
        "timestamp": timestamp,
    }


def heartbeat_locks(
    conn: sqlite3.Connection,
    files: Sequence[str],
    owner: str,
    ttl: int,
) -> dict[str, object]:
    timestamp = now_ts()
    expires_at = timestamp + ttl
    renewed: list[str] = []
    skipped: list[dict[str, object]] = []

    conn.execute("BEGIN IMMEDIATE")
    try:
        cleanup_expired(conn, timestamp)
        for file_path in normalize_paths(files):
            updated = conn.execute(
                """
                UPDATE file_locks
                SET heartbeat_at = ?, expires_at = ?
                WHERE file_path = ? AND owner_id = ?
                """,
                (timestamp, expires_at, file_path, owner),
            ).rowcount

            if updated:
                renewed.append(file_path)
                continue

            row = conn.execute(
                "SELECT owner_id, expires_at FROM file_locks WHERE file_path = ?",
                (file_path,),
            ).fetchone()
            if row is None:
                skipped.append({"file": file_path, "reason": "not_locked"})
            else:
                skipped.append(
                    {
                        "file": file_path,
                        "reason": "owned_by_other",
                        "owner": str(row["owner_id"]),
                        "expires_at": int(row["expires_at"]),
                    }
                )

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return {
        "renewed": renewed,
        "skipped": skipped,
        "owner": owner,
        "ttl_seconds": ttl,
        "timestamp": timestamp,
    }


def release_locks(
    conn: sqlite3.Connection,
    files: Sequence[str],
    owner: str,
) -> dict[str, object]:
    timestamp = now_ts()
    released: list[str] = []
    skipped: list[dict[str, object]] = []

    conn.execute("BEGIN IMMEDIATE")
    try:
        cleanup_expired(conn, timestamp)
        for file_path in normalize_paths(files):
            deleted = conn.execute(
                "DELETE FROM file_locks WHERE file_path = ? AND owner_id = ?",
                (file_path, owner),
            ).rowcount

            if deleted:
                released.append(file_path)
                continue

            row = conn.execute(
                "SELECT owner_id, expires_at FROM file_locks WHERE file_path = ?",
                (file_path,),
            ).fetchone()
            if row is None:
                skipped.append({"file": file_path, "reason": "not_locked"})
            else:
                skipped.append(
                    {
                        "file": file_path,
                        "reason": "owned_by_other",
                        "owner": str(row["owner_id"]),
                        "expires_at": int(row["expires_at"]),
                    }
                )

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return {
        "released": released,
        "skipped": skipped,
        "owner": owner,
        "timestamp": timestamp,
    }


def status_locks(conn: sqlite3.Connection, files: Sequence[str]) -> dict[str, object]:
    timestamp = now_ts()
    conn.execute("BEGIN IMMEDIATE")
    try:
        cleanup_expired(conn, timestamp)

        normalized = normalize_paths(files)
        if normalized:
            placeholders = ",".join("?" for _ in normalized)
            rows = conn.execute(
                f"""
                SELECT file_path, owner_id, task_id, repo_root,
                       acquired_at, heartbeat_at, expires_at
                FROM file_locks
                WHERE file_path IN ({placeholders})
                ORDER BY file_path
                """,
                tuple(normalized),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT file_path, owner_id, task_id, repo_root,
                       acquired_at, heartbeat_at, expires_at
                FROM file_locks
                ORDER BY file_path
                """
            ).fetchall()

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    locks = []
    for row in rows:
        expires_at = int(row["expires_at"])
        locks.append(
            {
                "file": str(row["file_path"]),
                "owner": str(row["owner_id"]),
                "task_id": row["task_id"],
                "repo_root": row["repo_root"],
                "acquired_at": int(row["acquired_at"]),
                "heartbeat_at": int(row["heartbeat_at"]),
                "expires_at": expires_at,
                "expires_in": max(0, expires_at - timestamp),
            }
        )

    return {"locks": locks, "timestamp": timestamp}


def render_text(payload: dict[str, object], command: str) -> None:
    if command == "acquire":
        acquired = payload.get("acquired", [])
        locked = payload.get("locked", [])
        for path in acquired:
            print(f"ACQUIRED {path}")
        for entry in locked:
            print(
                "LOCKED {file} owner={owner} expires_in={expires_in}".format(
                    **entry
                )
            )
        if not acquired and not locked:
            print("NO_FILES")
        return

    if command == "heartbeat":
        renewed = payload.get("renewed", [])
        skipped = payload.get("skipped", [])
        for path in renewed:
            print(f"HEARTBEAT {path}")
        for entry in skipped:
            reason = entry.get("reason", "unknown")
            owner = entry.get("owner")
            if owner:
                print(f"SKIPPED {entry['file']} reason={reason} owner={owner}")
            else:
                print(f"SKIPPED {entry['file']} reason={reason}")
        if not renewed and not skipped:
            print("NO_FILES")
        return

    if command == "release":
        released = payload.get("released", [])
        skipped = payload.get("skipped", [])
        for path in released:
            print(f"RELEASED {path}")
        for entry in skipped:
            reason = entry.get("reason", "unknown")
            owner = entry.get("owner")
            if owner:
                print(f"SKIPPED {entry['file']} reason={reason} owner={owner}")
            else:
                print(f"SKIPPED {entry['file']} reason={reason}")
        if not released and not skipped:
            print("NO_FILES")
        return

    if command == "status":
        locks = payload.get("locks", [])
        if not locks:
            print("NO_ACTIVE_LOCKS")
            return
        for lock in locks:
            print(
                "{file}\towner={owner}\texpires_in={expires_in}\ttask_id={task_id}\trepo_root={repo_root}".format(
                    **lock
                )
            )
        return


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Hard file-level lock coordinator for concurrent coding agents",
    )

    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument(
        "--db",
        help=f"SQLite DB path (or set {DB_ENV_VAR})",
    )
    common_parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON output for automation",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    acquire_parser = subparsers.add_parser("acquire", parents=[common_parser], help="Acquire locks")
    acquire_parser.add_argument("--owner", required=True)
    acquire_parser.add_argument("--task-id")
    acquire_parser.add_argument("--repo-root")
    acquire_parser.add_argument("--ttl", type=int, default=DEFAULT_TTL_SECONDS)
    acquire_parser.add_argument("files", nargs="+")

    heartbeat_parser = subparsers.add_parser("heartbeat", parents=[common_parser], help="Refresh lock leases")
    heartbeat_parser.add_argument("--owner", required=True)
    heartbeat_parser.add_argument("--ttl", type=int, default=DEFAULT_TTL_SECONDS)
    heartbeat_parser.add_argument("files", nargs="+")

    release_parser = subparsers.add_parser("release", parents=[common_parser], help="Release locks held by owner")
    release_parser.add_argument("--owner", required=True)
    release_parser.add_argument("files", nargs="+")

    status_parser = subparsers.add_parser("status", parents=[common_parser], help="Show active locks")
    status_parser.add_argument("files", nargs="*")

    return parser.parse_args(argv)


def resolve_db_path(cli_value: str | None) -> str:
    value = cli_value or os.environ.get(DB_ENV_VAR)
    if not value:
        raise ValueError(
            f"missing database path: set --db or {DB_ENV_VAR}"
        )
    return str(Path(value).expanduser().resolve(strict=False))


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)

    try:
        db_path = resolve_db_path(args.db)

        if hasattr(args, "owner"):
            validate_owner(args.owner)

        if hasattr(args, "ttl") and args.ttl < 1:
            raise ValueError("ttl must be >= 1")

        conn = connect_db(db_path)
        try:
            if args.command == "acquire":
                payload = acquire_locks(
                    conn,
                    files=args.files,
                    owner=args.owner,
                    ttl=args.ttl,
                    task_id=args.task_id,
                    repo_root=(normalize_path(args.repo_root) if args.repo_root else None),
                )
            elif args.command == "heartbeat":
                payload = heartbeat_locks(
                    conn,
                    files=args.files,
                    owner=args.owner,
                    ttl=args.ttl,
                )
            elif args.command == "release":
                payload = release_locks(
                    conn,
                    files=args.files,
                    owner=args.owner,
                )
            elif args.command == "status":
                payload = status_locks(conn, files=args.files)
            else:
                raise ValueError(f"unsupported command: {args.command}")
        finally:
            conn.close()

        if args.json:
            print(json.dumps(payload, separators=(",", ":"), sort_keys=True))
        else:
            render_text(payload, args.command)

    except (ValueError, sqlite3.Error) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
