import { cleanupExpired, withImmediateTransaction } from "./db.mjs";
import { normalizePaths, nowTs } from "./paths.mjs";

export function acquireLocks(db, files, owner, ttl, taskId, repoRoot) {
  const timestamp = nowTs();
  const expiresAt = timestamp + ttl;
  const acquired = [];
  const locked = [];

  const selectOwnerByFile = db.prepare(
    "SELECT owner_id, expires_at FROM file_locks WHERE file_path = ?",
  );
  const insertLock = db.prepare(`
    INSERT INTO file_locks (
      file_path, owner_id, task_id, repo_root,
      acquired_at, heartbeat_at, expires_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
  `);
  const updateOwnedLock = db.prepare(`
    UPDATE file_locks
    SET heartbeat_at = ?, expires_at = ?, task_id = ?, repo_root = ?
    WHERE file_path = ?
  `);

  withImmediateTransaction(db, () => {
    cleanupExpired(db, timestamp);

    for (const filePath of normalizePaths(files)) {
      const row = selectOwnerByFile.get(filePath);

      if (!row) {
        insertLock.run(
          filePath,
          owner,
          taskId ?? null,
          repoRoot ?? null,
          timestamp,
          timestamp,
          expiresAt,
        );
        acquired.push(filePath);
        continue;
      }

      if (row.owner_id === owner) {
        updateOwnedLock.run(
          timestamp,
          expiresAt,
          taskId ?? null,
          repoRoot ?? null,
          filePath,
        );
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

export function heartbeatLocks(db, files, owner, ttl) {
  const timestamp = nowTs();
  const expiresAt = timestamp + ttl;
  const renewed = [];
  const skipped = [];

  const updateOwnedLock = db.prepare(`
    UPDATE file_locks
    SET heartbeat_at = ?, expires_at = ?
    WHERE file_path = ? AND owner_id = ?
  `);
  const selectOwnerByFile = db.prepare(
    "SELECT owner_id, expires_at FROM file_locks WHERE file_path = ?",
  );

  withImmediateTransaction(db, () => {
    cleanupExpired(db, timestamp);

    for (const filePath of normalizePaths(files)) {
      const updateResult = updateOwnedLock.run(timestamp, expiresAt, filePath, owner);

      if (updateResult.changes > 0) {
        renewed.push(filePath);
        continue;
      }

      const row = selectOwnerByFile.get(filePath);
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

export function releaseLocks(db, files, owner) {
  const timestamp = nowTs();
  const released = [];
  const skipped = [];

  const deleteOwnedLock = db.prepare(
    "DELETE FROM file_locks WHERE file_path = ? AND owner_id = ?",
  );
  const selectOwnerByFile = db.prepare(
    "SELECT owner_id, expires_at FROM file_locks WHERE file_path = ?",
  );

  withImmediateTransaction(db, () => {
    cleanupExpired(db, timestamp);

    for (const filePath of normalizePaths(files)) {
      const deleteResult = deleteOwnedLock.run(filePath, owner);

      if (deleteResult.changes > 0) {
        released.push(filePath);
        continue;
      }

      const row = selectOwnerByFile.get(filePath);
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

export function statusLocks(db, files) {
  const timestamp = nowTs();
  const normalized = normalizePaths(files);

  const rows = withImmediateTransaction(db, () => {
    cleanupExpired(db, timestamp);

    if (normalized.length === 0) {
      return db
        .prepare(`
          SELECT file_path, owner_id, task_id, repo_root,
                 acquired_at, heartbeat_at, expires_at
          FROM file_locks
          ORDER BY file_path
        `)
        .all();
    }

    const placeholders = normalized.map(() => "?").join(",");
    return db
      .prepare(`
        SELECT file_path, owner_id, task_id, repo_root,
               acquired_at, heartbeat_at, expires_at
        FROM file_locks
        WHERE file_path IN (${placeholders})
        ORDER BY file_path
      `)
      .all(...normalized);
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
