import { homedir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const DEFAULT_TTL_SECONDS = 180;

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const SKILL_DIR = path.resolve(SCRIPT_DIR, "..");

export const HARD_CODED_DB_PATH = path.join(
  SKILL_DIR,
  "assets",
  "multi_agent_locks.db",
);

function expandUser(rawPath) {
  if (rawPath === "~") {
    return homedir();
  }
  if (rawPath.startsWith("~/")) {
    return path.join(homedir(), rawPath.slice(2));
  }
  return rawPath;
}

export function normalizePath(rawPath) {
  return path.resolve(expandUser(rawPath));
}

export function normalizePaths(paths) {
  const seen = new Set();
  const normalized = [];
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

export function nowTs() {
  return Math.floor(Date.now() / 1000);
}

export function validateOwner(owner) {
  if (owner.split(":").length !== 3) {
    throw new Error("owner must use format <agent-name>:<pid>:<session-id>");
  }
}
