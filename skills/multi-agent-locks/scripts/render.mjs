export function renderText(payload, command) {
  if (command === "acquire") {
    const acquired = payload.acquired ?? [];
    const locked = payload.locked ?? [];
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
    const renewed = payload.renewed ?? [];
    const skipped = payload.skipped ?? [];
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
    const released = payload.released ?? [];
    const skipped = payload.skipped ?? [];
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
    const locks = payload.locks ?? [];
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

export function sortJson(value) {
  if (Array.isArray(value)) {
    return value.map(sortJson);
  }
  if (!value || typeof value !== "object") {
    return value;
  }
  const sortedEntries = Object.entries(value)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, nested]) => [key, sortJson(nested)]);
  return Object.fromEntries(sortedEntries);
}
