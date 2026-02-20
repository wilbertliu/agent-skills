import { DEFAULT_TTL_SECONDS } from "./paths.mjs";

function parseIntOption(raw, optionName) {
  const value = Number.parseInt(raw, 10);
  if (!Number.isInteger(value)) {
    throw new Error(`${optionName} must be an integer`);
  }
  return value;
}

function optionValue(args, index, optionName) {
  const value = args[index + 1];
  if (!value || value.startsWith("--")) {
    throw new Error(`${optionName} requires a value`);
  }
  return value;
}

function parseSubcommandArgs(command, args) {
  let json = false;
  let owner;
  let taskId;
  let repoRoot;
  let ttl = DEFAULT_TTL_SECONDS;
  const files = [];

  let index = 0;
  while (index < args.length) {
    const token = args[index];
    if (token === "--") {
      files.push(...args.slice(index + 1));
      break;
    }

    if (token.startsWith("--")) {
      switch (token) {
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
    return { json, owner, taskId, repoRoot, ttl, files };
  }

  if (command === "heartbeat") {
    if (!owner) {
      throw new Error("missing required option: --owner");
    }
    if (files.length === 0) {
      throw new Error("heartbeat requires at least one file");
    }
    return { json, owner, ttl, files };
  }

  if (command === "release") {
    if (!owner) {
      throw new Error("missing required option: --owner");
    }
    if (files.length === 0) {
      throw new Error("release requires at least one file");
    }
    return { json, owner, files };
  }

  return { json, files };
}

export function parseArgs(argv) {
  const command = argv[0];
  if (!command) {
    throw new Error("missing command: expected one of acquire|heartbeat|release|status");
  }

  if (
    command !== "acquire" &&
    command !== "heartbeat" &&
    command !== "release" &&
    command !== "status"
  ) {
    throw new Error(`unsupported command: ${command}`);
  }

  return { command, ...parseSubcommandArgs(command, argv.slice(1)) };
}
