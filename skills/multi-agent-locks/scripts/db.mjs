import { mkdirSync } from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

import { HARD_CODED_DB_PATH, normalizePath } from "./paths.mjs";

export function resolveDbPath() {
  return normalizePath(HARD_CODED_DB_PATH);
}

export function withImmediateTransaction(db, callback) {
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

export function connectDb(dbPath = resolveDbPath()) {
  const absolutePath = normalizePath(dbPath);
  mkdirSync(path.dirname(absolutePath), { recursive: true });

  const db = new DatabaseSync(absolutePath);
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

export function cleanupExpired(db, timestamp) {
  db.prepare("DELETE FROM file_locks WHERE expires_at <= ?").run(timestamp);
}
