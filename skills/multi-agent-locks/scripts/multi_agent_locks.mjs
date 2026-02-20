#!/usr/bin/env node

import { parseArgs } from "./cli.mjs";
import { connectDb, resolveDbPath } from "./db.mjs";
import { acquireLocks, heartbeatLocks, releaseLocks, statusLocks } from "./locks.mjs";
import { normalizePath, validateOwner } from "./paths.mjs";
import { renderText, sortJson } from "./render.mjs";

function main(argv) {
  try {
    const args = parseArgs(argv);
    const db = connectDb(resolveDbPath());

    try {
      if ("owner" in args) {
        validateOwner(args.owner);
      }
      if ("ttl" in args && args.ttl < 1) {
        throw new Error("ttl must be >= 1");
      }

      let payload;
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
