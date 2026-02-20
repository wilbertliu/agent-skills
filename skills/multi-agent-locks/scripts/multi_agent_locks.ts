#!/usr/bin/env bun

import { Database } from "bun:sqlite";
import { mkdirSync } from "node:fs";
import { homedir } from "node:os";
import path from "node:path";

const DEFAULT_TTL_SECONDS = 180;
const DB_ENV_VAR = "MULTI_AGENT_LOCKS_DB";

type Command = "acquire" | "heartbeat" | "release" | "status";

type AcquireArgs = {
  command: "acquire";
  db?: string;
  json: boolean;
  owner: string;
  taskId?: string;
  repoRoot?: string;
  ttl: number;
  files: string[];
};

type HeartbeatArgs = {
  command: "heartbeat";
  db?: string;
  json: boolean;
  owner: string;
  ttl: number;
  files: string[];
};

type ReleaseArgs = {
  command: "release";
  db?: string;
  json: boolean;
  owner: string;
  files: string[];
};

type StatusArgs = {
  command: "status";
  db?: string;
  json: boolean;
  files: string[];
};

type ParsedArgs = AcquireArgs | HeartbeatArgs | ReleaseArgs | StatusArgs;

type LockRow = {
  file_path: string;
  owner_id: string;
  task_id: string | null;
  repo_root: string | null;
  acquired_at: number;
  heartbeat_at: number;
  expires_at: number;
};

function nowTs(): number {
  return Math.floor(Date.now() / 1000);
}

function expandUser(rawPath: string): string {
  if (rawPath === "~") {
    return homedir();
  }
  if (rawPath.startsWith("~/")) {
    return path.join(homedir(), rawPath.slice(2));
  }
  return rawPath;
}

function normalizePath(rawPath: string): string {
  return path.resolve(expandUser(rawPath));
}

function normalizePaths(paths: string[]): string[] {
  const seen = new Set<string>();
  const normalized: string[] = [];
  for (const rawPath of paths) {
    const resolved = normalizePath(rawPath);
    if (seen.has(resolved)) {
      continue;
    }
    seen.add(resolved);
    normalized.push(resolved);
  }
  return normalized;
}

function validateOwner(owner: string): void {
  if (owner.split(":").length !== 3) {
    throw new Error("owner must use format <agent-name>:<pid>:<session-id>");
  }
}

function withImmediateTransaction<T>(db: Database, callback: () => T): T {
  db.exec("BEGIN IMMEDIATE");
  try {
    const result = callback();
    db.exec("COMMIT");
    return result;
  } catch (error) {
    try {
      db.exec("ROLLBACK");
    } catch {
      // Ignore rollback failures and rethrow the original error.
    }
    throw error;
  }
}

function connectDb(dbPath: string): Database {
  const absolutePath = normalizePath(dbPath);
  mkdirSync(path.dirname(absolutePath), { recursive: true });

  const db = new Database(absolutePath, { create: true });
  db.exec("PRAGMA journal_mode=WAL");
  db.exec("PRAGMA busy_timeout=5000");
  db.exec(`
    CREATE TABLE IF NOT EXISTS file_locks (
      file_path TEXT PRIMARY KEY,
      owner_id TEXT NOT NULL,
      task_id TEXT,
      repo_root TEXT,
      acquired_at INTEGER NOT NULL,
      heartbeat_at INTEGER NOT NULL,
      expires_at INTEGER NOT NULL
    )
  `);
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_file_locks_expires_at
      ON file_locks (expires_at)
  `);

  return db;
}

function cleanupExpired(db: Database, timestamp: number): void {
  db.query("DELETE FROM file_locks WHERE expires_at <= ?").run(timestamp);
}

function acquireLocks(
  db: Database,
  files: string[],
  owner: string,
  ttl: number,
  taskId: string | undefined,
  repoRoot: string | undefined,
): Record<string, unknown> {
  const timestamp = nowTs();
  const expiresAt = timestamp + ttl;
  const acquired: string[] = [];
  const locked: Array<Record<string, unknown>> = [];

  withImmediateTransaction(db, () => {
    cleanupExpired(db, timestamp);

    for (const filePath of normalizePaths(files)) {
      const row = db
        .query("SELECT owner_id, expires_at FROM file_locks WHERE file_path = ?")
        .get(filePath) as { owner_id: string; expires_at: number } | null;

      if (!row) {
        db.query(`
          INSERT INTO file_locks (
            file_path, owner_id, task_id, repo_root,
            acquired_at, heartbeat_at, expires_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?)
        `).run(filePath, owner, taskId ?? null, repoRoot ?? null, timestamp, timestamp, expiresAt);
        acquired.push(filePath);
        continue;
      }

      if (row.owner_id === owner) {
        db.query(`
          UPDATE file_locks
          SET heartbeat_at = ?, expires_at = ?, task_id = ?, repo_root = ?
          WHERE file_path = ?
        `).run(timestamp, expiresAt, taskId ?? null, repoRoot ?? null, filePath);
        acquired.push(filePath);
        continue;
      }

      locked.push({
        file: filePath,
        owner: row.owner_id,
        expires_at: row.expires_at,
        expires_in: Math.max(0, row.expires_at - timestamp),
      });
    }
  });

  return {
    acquired,
    locked,
    owner,
    ttl_seconds: ttl,
    timestamp,
  };
}

function heartbeatLocks(
  db: Database,
  files: string[],
  owner: string,
  ttl: number,
): Record<string, unknown> {
  const timestamp = nowTs();
  const expiresAt = timestamp + ttl;
  const renewed: string[] = [];
  const skipped: Array<Record<string, unknown>> = [];

  withImmediateTransaction(db, () => {
    cleanupExpired(db, timestamp);

    for (const filePath of normalizePaths(files)) {
      const updateResult = db.query(`
        UPDATE file_locks
        SET heartbeat_at = ?, expires_at = ?
        WHERE file_path = ? AND owner_id = ?
      `).run(timestamp, expiresAt, filePath, owner);

      if (updateResult.changes > 0) {
        renewed.push(filePath);
        continue;
      }

      const row = db
        .query("SELECT owner_id, expires_at FROM file_locks WHERE file_path = ?")
        .get(filePath) as { owner_id: string; expires_at: number } | null;

      if (!row) {
        skipped.push({ file: filePath, reason: "not_locked" });
      } else {
        skipped.push({
          file: filePath,
          reason: "owned_by_other",
          owner: row.owner_id,
          expires_at: row.expires_at,
        });
      }
    }
  });

  return {
    renewed,
    skipped,
    owner,
    ttl_seconds: ttl,
    timestamp,
  };
}

function releaseLocks(
  db: Database,
  files: string[],
  owner: string,
): Record<string, unknown> {
  const timestamp = nowTs();
  const released: string[] = [];
  const skipped: Array<Record<string, unknown>> = [];

  withImmediateTransaction(db, () => {
    cleanupExpired(db, timestamp);

    for (const filePath of normalizePaths(files)) {
      const deleteResult = db
        .query("DELETE FROM file_locks WHERE file_path = ? AND owner_id = ?")
        .run(filePath, owner);

      if (deleteResult.changes > 0) {
        released.push(filePath);
        continue;
      }

      const row = db
        .query("SELECT owner_id, expires_at FROM file_locks WHERE file_path = ?")
        .get(filePath) as { owner_id: string; expires_at: number } | null;

      if (!row) {
        skipped.push({ file: filePath, reason: "not_locked" });
      } else {
        skipped.push({
          file: filePath,
          reason: "owned_by_other",
          owner: row.owner_id,
          expires_at: row.expires_at,
        });
      }
    }
  });

  return {
    released,
    skipped,
    owner,
    timestamp,
  };
}

function statusLocks(db: Database, files: string[]): Record<string, unknown> {
  const timestamp = nowTs();
  const normalized = normalizePaths(files);

  const rows = withImmediateTransaction(db, () => {
    cleanupExpired(db, timestamp);

    if (normalized.length === 0) {
      return db.query(`
        SELECT file_path, owner_id, task_id, repo_root,
               acquired_at, heartbeat_at, expires_at
        FROM file_locks
        ORDER BY file_path
      `).all() as LockRow[];
    }

    const placeholders = normalized.map(() => "?").join(",");
    return db.query(`
      SELECT file_path, owner_id, task_id, repo_root,
             acquired_at, heartbeat_at, expires_at
      FROM file_locks
      WHERE file_path IN (${placeholders})
      ORDER BY file_path
    `).all(...normalized) as LockRow[];
  });

  const locks = rows.map((row) => ({
    file: row.file_path,
    owner: row.owner_id,
    task_id: row.task_id,
    repo_root: row.repo_root,
    acquired_at: row.acquired_at,
    heartbeat_at: row.heartbeat_at,
    expires_at: row.expires_at,
    expires_in: Math.max(0, row.expires_at - timestamp),
  }));

  return { locks, timestamp };
}

function renderText(payload: Record<string, unknown>, command: Command): void {
  if (command === "acquire") {
    const acquired = (payload.acquired as string[]) ?? [];
    const locked = (payload.locked as Array<Record<string, unknown>>) ?? [];
    for (const lockedPath of acquired) {
      console.log(`ACQUIRED ${lockedPath}`);
    }
    for (const entry of locked) {
      console.log(
        `LOCKED ${entry.file} owner=${entry.owner} expires_in=${entry.expires_in}`,
      );
    }
    if (acquired.length === 0 && locked.length === 0) {
      console.log("NO_FILES");
    }
    return;
  }

  if (command === "heartbeat") {
    const renewed = (payload.renewed as string[]) ?? [];
    const skipped = (payload.skipped as Array<Record<string, unknown>>) ?? [];
    for (const renewedPath of renewed) {
      console.log(`HEARTBEAT ${renewedPath}`);
    }
    for (const entry of skipped) {
      const reason = String(entry.reason ?? "unknown");
      if (entry.owner) {
        console.log(`SKIPPED ${entry.file} reason=${reason} owner=${entry.owner}`);
      } else {
        console.log(`SKIPPED ${entry.file} reason=${reason}`);
      }
    }
    if (renewed.length === 0 && skipped.length === 0) {
      console.log("NO_FILES");
    }
    return;
  }

  if (command === "release") {
    const released = (payload.released as string[]) ?? [];
    const skipped = (payload.skipped as Array<Record<string, unknown>>) ?? [];
    for (const releasedPath of released) {
      console.log(`RELEASED ${releasedPath}`);
    }
    for (const entry of skipped) {
      const reason = String(entry.reason ?? "unknown");
      if (entry.owner) {
        console.log(`SKIPPED ${entry.file} reason=${reason} owner=${entry.owner}`);
      } else {
        console.log(`SKIPPED ${entry.file} reason=${reason}`);
      }
    }
    if (released.length === 0 && skipped.length === 0) {
      console.log("NO_FILES");
    }
    return;
  }

  if (command === "status") {
    const locks = (payload.locks as Array<Record<string, unknown>>) ?? [];
    if (locks.length === 0) {
      console.log("NO_ACTIVE_LOCKS");
      return;
    }
    for (const lock of locks) {
      console.log(
        `${lock.file}\towner=${lock.owner}\texpires_in=${lock.expires_in}\ttask_id=${lock.task_id}\trepo_root=${lock.repo_root}`,
      );
    }
  }
}

function parseIntOption(raw: string, optionName: string): number {
  const value = Number.parseInt(raw, 10);
  if (!Number.isInteger(value)) {
    throw new Error(`${optionName} must be an integer`);
  }
  return value;
}

function optionValue(args: string[], index: number, optionName: string): string {
  const value = args[index + 1];
  if (!value || value.startsWith("--")) {
    throw new Error(`${optionName} requires a value`);
  }
  return value;
}

function parseSubcommandArgs(
  command: Command,
  args: string[],
): Omit<AcquireArgs, "command"> | Omit<HeartbeatArgs, "command"> | Omit<ReleaseArgs, "command"> | Omit<StatusArgs, "command"> {
  let db: string | undefined;
  let json = false;
  let owner: string | undefined;
  let taskId: string | undefined;
  let repoRoot: string | undefined;
  let ttl = DEFAULT_TTL_SECONDS;
  const files: string[] = [];

  let index = 0;
  while (index < args.length) {
    const token = args[index];
    if (token === "--") {
      files.push(...args.slice(index + 1));
      break;
    }

    if (token.startsWith("--")) {
      switch (token) {
        case "--db":
          db = optionValue(args, index, "--db");
          index += 2;
          continue;
        case "--json":
          json = true;
          index += 1;
          continue;
        case "--owner":
          owner = optionValue(args, index, "--owner");
          index += 2;
          continue;
        case "--task-id":
          taskId = optionValue(args, index, "--task-id");
          index += 2;
          continue;
        case "--repo-root":
          repoRoot = optionValue(args, index, "--repo-root");
          index += 2;
          continue;
        case "--ttl":
          ttl = parseIntOption(optionValue(args, index, "--ttl"), "--ttl");
          index += 2;
          continue;
        default:
          throw new Error(`unknown option: ${token}`);
      }
    }

    files.push(token);
    index += 1;
  }

  if (command === "acquire") {
    if (!owner) {
      throw new Error("missing required option: --owner");
    }
    if (files.length === 0) {
      throw new Error("acquire requires at least one file");
    }
    return { db, json, owner, taskId, repoRoot, ttl, files };
  }

  if (command === "heartbeat") {
    if (!owner) {
      throw new Error("missing required option: --owner");
    }
    if (files.length === 0) {
      throw new Error("heartbeat requires at least one file");
    }
    return { db, json, owner, ttl, files };
  }

  if (command === "release") {
    if (!owner) {
      throw new Error("missing required option: --owner");
    }
    if (files.length === 0) {
      throw new Error("release requires at least one file");
    }
    return { db, json, owner, files };
  }

  return { db, json, files };
}

function parseArgs(argv: string[]): ParsedArgs {
  const commandToken = argv[0];
  if (!commandToken) {
    throw new Error("missing command: expected one of acquire|heartbeat|release|status");
  }

  if (
    commandToken !== "acquire" &&
    commandToken !== "heartbeat" &&
    commandToken !== "release" &&
    commandToken !== "status"
  ) {
    throw new Error(`unsupported command: ${commandToken}`);
  }

  const parsed = parseSubcommandArgs(commandToken, argv.slice(1));

  if (commandToken === "acquire") {
    return { command: "acquire", ...(parsed as Omit<AcquireArgs, "command">) };
  }
  if (commandToken === "heartbeat") {
    return { command: "heartbeat", ...(parsed as Omit<HeartbeatArgs, "command">) };
  }
  if (commandToken === "release") {
    return { command: "release", ...(parsed as Omit<ReleaseArgs, "command">) };
  }
  return { command: "status", ...(parsed as Omit<StatusArgs, "command">) };
}

function resolveDbPath(cliValue: string | undefined): string {
  const value = cliValue ?? process.env[DB_ENV_VAR];
  if (!value) {
    throw new Error(`missing database path: set --db or ${DB_ENV_VAR}`);
  }
  return normalizePath(value);
}

function sortJson(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sortJson);
  }
  if (!value || typeof value !== "object") {
    return value;
  }
  const sortedEntries = Object.entries(value as Record<string, unknown>)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, nested]) => [key, sortJson(nested)]);
  return Object.fromEntries(sortedEntries);
}

function main(argv: string[]): number {
  try {
    const args = parseArgs(argv);
    const dbPath = resolveDbPath(args.db);

    if ("owner" in args) {
      validateOwner(args.owner);
    }
    if ("ttl" in args && args.ttl < 1) {
      throw new Error("ttl must be >= 1");
    }

    const db = connectDb(dbPath);
    try {
      let payload: Record<string, unknown>;
      if (args.command === "acquire") {
        payload = acquireLocks(
          db,
          args.files,
          args.owner,
          args.ttl,
          args.taskId,
          args.repoRoot ? normalizePath(args.repoRoot) : undefined,
        );
      } else if (args.command === "heartbeat") {
        payload = heartbeatLocks(db, args.files, args.owner, args.ttl);
      } else if (args.command === "release") {
        payload = releaseLocks(db, args.files, args.owner);
      } else {
        payload = statusLocks(db, args.files);
      }

      if (args.json) {
        console.log(JSON.stringify(sortJson(payload)));
      } else {
        renderText(payload, args.command);
      }
    } finally {
      db.close();
    }

    return 0;
  } catch (error) {
    if (error instanceof Error) {
      console.error(`ERROR: ${error.message}`);
    } else {
      console.error(`ERROR: ${String(error)}`);
    }
    return 1;
  }
}

process.exitCode = main(process.argv.slice(2));
