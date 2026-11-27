#!/usr/bin/env bun
/**
 * claude-peers broker daemon
 *
 * A singleton HTTP server on localhost:7899 backed by SQLite.
 * Tracks all registered Claude Code peers and routes messages between them.
 *
 * Auto-launched by the MCP server if not already running.
 * Run directly: bun broker.ts
 */

import { Database } from "bun:sqlite";
import { timingSafeEqual } from "node:crypto";
import type {
  RegisterRequest,
  RegisterResponse,
  HeartbeatRequest,
  SetSummaryRequest,
  ListPeersRequest,
  SendMessageRequest,
  PollMessagesRequest,
  PollMessagesResponse,
  Peer,
  Message,
} from "./shared/types.ts";

const PORT = parseInt(process.env.CLAUDE_PEERS_PORT ?? "7899", 10);
const DB_PATH = process.env.CLAUDE_PEERS_DB ?? `${process.env.HOME}/.claude-peers.db`;
const HUB_TOKEN = process.env.CLAUDE_PEERS_HUB_TOKEN ?? "";
const CF_ACCESS_TEAM_DOMAIN = (process.env.CLAUDE_PEERS_CF_ACCESS_TEAM_DOMAIN ?? "").trim().replace(/\/+$/, "");
const CF_ACCESS_AUD = (process.env.CLAUDE_PEERS_CF_ACCESS_AUD ?? "").trim();
const REMOTE_HUB = HUB_TOKEN.length > 0;
const STALE_PEER_TIMEOUT_MS = parseInt(
  process.env.CLAUDE_PEERS_STALE_TIMEOUT_MS ?? "90000",
  10,
);

// --- Database setup ---

const db = new Database(DB_PATH);
db.run("PRAGMA journal_mode = WAL");
db.run("PRAGMA busy_timeout = 3000");

db.run(`
  CREATE TABLE IF NOT EXISTS peers (
    id TEXT PRIMARY KEY,
    pid INTEGER NOT NULL,
    cwd TEXT NOT NULL,
    git_root TEXT,
    tty TEXT,
    summary TEXT NOT NULL DEFAULT '',
    registered_at TEXT NOT NULL,
    last_seen TEXT NOT NULL
  )
`);

db.run(`
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_id TEXT NOT NULL,
    to_id TEXT NOT NULL,
    text TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (from_id) REFERENCES peers(id),
    FOREIGN KEY (to_id) REFERENCES peers(id)
  )
`);

function hasColumn(table: string, column: string): boolean {
  const columns = db.query(`PRAGMA table_info(${table})`).all() as { name: string }[];
  return columns.some((candidate) => candidate.name === column);
}

// Additive migrations keep existing broker databases compatible.
if (!hasColumn("peers", "alias")) {
  db.run("ALTER TABLE peers ADD COLUMN alias TEXT");
}
if (!hasColumn("peers", "alias_key")) {
  db.run("ALTER TABLE peers ADD COLUMN alias_key TEXT");
}
for (const [column, definition] of [
  ["managed", "INTEGER NOT NULL DEFAULT 0"],
  ["role", "TEXT NOT NULL DEFAULT 'peer'"],
  ["status", "TEXT NOT NULL DEFAULT 'online'"],
]) {
  if (!hasColumn("peers", column)) {
    db.run(`ALTER TABLE peers ADD COLUMN ${column} ${definition}`);
  }
}
if (!hasColumn("messages", "from_alias")) {
  db.run("ALTER TABLE messages ADD COLUMN from_alias TEXT");
}
if (!hasColumn("messages", "from_kind")) {
  db.run("ALTER TABLE messages ADD COLUMN from_kind TEXT NOT NULL DEFAULT 'peer'");
}
const migrateAliases = db.transaction(() => {
  db.run(`
    UPDATE messages
    SET from_kind = 'system'
    WHERE NOT EXISTS (SELECT 1 FROM peers WHERE peers.id = messages.from_id)
  `);
  db.run("DROP INDEX IF EXISTS idx_peers_alias_nocase");
  const peers = db
    .query("SELECT id, alias FROM peers ORDER BY registered_at ASC, id ASC")
    .all() as { id: string; alias: string | null }[];
  const peerIds = new Set(peers.map((peer) => peer.id));
  const usedKeys = new Set<string>();

  for (const peer of peers) {
    const normalized = peer.alias?.trim().normalize("NFKC") ?? "";
    if (!normalized || !isValidAlias(normalized)) {
      db.run("UPDATE peers SET alias = NULL, alias_key = NULL WHERE id = ?", [peer.id]);
      continue;
    }
    const key = aliasKey(normalized);
    const shadowsAnotherId = peerIds.has(key) && key !== peer.id;
    if (usedKeys.has(key) || shadowsAnotherId) {
      // Keep the peer addressable by ID instead of failing broker startup.
      db.run("UPDATE peers SET alias = NULL, alias_key = NULL WHERE id = ?", [peer.id]);
      continue;
    }
    db.run("UPDATE peers SET alias = ?, alias_key = ? WHERE id = ?", [normalized, key, peer.id]);
    usedKeys.add(key);
  }

  db.run(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_peers_alias_key
    ON peers(alias_key)
    WHERE alias_key IS NOT NULL
  `);
});
migrateAliases();

// Offline managed entries are durable address-book entries, not live processes.
function cleanStalePeers() {
  const peers = db.query("SELECT * FROM peers").all() as PeerWithAlias[];
  let changed = false;
  for (const peer of peers) {
    if (peer.status === "offline") continue;
    let stale = false;
    if (REMOTE_HUB) {
      const lastSeen = Date.parse(peer.last_seen);
      stale = !Number.isFinite(lastSeen) || Date.now() - lastSeen > STALE_PEER_TIMEOUT_MS;
    } else if (peer.pid > 0) {
      try {
        process.kill(peer.pid, 0);
      } catch {
        stale = true;
      }
    }
    if (!stale) continue;
    if (peer.managed) {
      db.run("UPDATE peers SET status = 'offline', pid = 0 WHERE id = ?", [peer.id]);
    } else {
      db.run("DELETE FROM messages WHERE to_id = ? AND delivered = 0", [peer.id]);
      db.run("DELETE FROM peers WHERE id = ?", [peer.id]);
    }
    changed = true;
  }
  if (changed) broadcastSnapshot();
}

// --- Prepared statements ---

const insertPeer = db.prepare(`
  INSERT INTO peers (id, pid, cwd, git_root, tty, summary, registered_at, last_seen, alias, alias_key)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const updateLastSeen = db.prepare(`
  UPDATE peers SET last_seen = ? WHERE id = ?
`);

const updateSummary = db.prepare(`
  UPDATE peers SET summary = ? WHERE id = ?
`);

const updateAlias = db.prepare(`
  UPDATE peers SET alias = ?, alias_key = ? WHERE id = ?
`);

const deletePeer = db.prepare(`
  DELETE FROM peers WHERE id = ?
`);

const selectAllPeers = db.prepare(`
  SELECT id, pid, cwd, git_root, tty, summary, registered_at, last_seen, alias, managed, role, status FROM peers
`);

const selectPeersByDirectory = db.prepare(`
  SELECT id, pid, cwd, git_root, tty, summary, registered_at, last_seen, alias, managed, role, status
  FROM peers WHERE cwd = ?
`);

const selectPeersByGitRoot = db.prepare(`
  SELECT id, pid, cwd, git_root, tty, summary, registered_at, last_seen, alias, managed, role, status
  FROM peers WHERE git_root = ?
`);

const insertMessage = db.prepare(`
  INSERT INTO messages (from_id, to_id, text, sent_at, delivered, from_alias, from_kind)
  VALUES (?, ?, ?, ?, 0, ?, ?)
`);

const selectUndelivered = db.prepare(`
  SELECT * FROM messages WHERE to_id = ? AND delivered = 0 ORDER BY sent_at ASC
`);

const markDelivered = db.prepare(`
  UPDATE messages SET delivered = 1 WHERE id = ?
`);

// --- Generate peer ID ---

function generateId(): string {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  while (true) {
    let id = "";
    for (let i = 0; i < 8; i++) {
      id += chars[Math.floor(Math.random() * chars.length)];
    }
    const collision = db
      .query("SELECT id FROM peers WHERE id = ? OR alias_key = ?")
      .get(id, id);
    if (!collision && !isRevoked(id)) {
      return id;
    }
  }
}

// --- Request handlers ---

type RegisterWithAliasRequest = RegisterRequest & { logical_name?: string | null };
type PeerWithAlias = Peer & {
  alias: string | null;
  managed: number;
  role: string;
  status: "online" | "offline";
};
type SetAliasRequest = { id: string; alias: string };

class AliasConflictError extends Error {
  constructor(public alias: string) {
    super(`Alias ${alias} is already in use`);
  }
}

class AliasValidationError extends Error {}
class PeerNotFoundError extends Error {}
class RequestValidationError extends Error {}
class PeerRevokedError extends Error {}

const MAX_TAG_LENGTH = 64;
const revokedPeerIds = new Map<string, number>();
const REVOKED_TTL_MS = 10 * 60_000;

function isRevoked(id: string): boolean {
  const now = Date.now();
  for (const [peerId, revokedAt] of revokedPeerIds) {
    if (now - revokedAt >= REVOKED_TTL_MS) revokedPeerIds.delete(peerId);
  }
  return revokedPeerIds.has(id);
}

function getPeer(id: string): PeerWithAlias | null {
  return db.query("SELECT * FROM peers WHERE id = ?").get(id) as PeerWithAlias | null;
}

function resolvePeer(address: string): PeerWithAlias | null {
  return getPeer(address) ?? db.query("SELECT * FROM peers WHERE alias_key = ?")
    .get(aliasKey(address)) as PeerWithAlias | null;
}

function normalizeAlias(value: unknown): string | null {
  if (value === undefined || value === null) {
    return null;
  }
  if (typeof value !== "string") {
    throw new AliasValidationError("Alias must be a string");
  }
  const alias = value.trim().normalize("NFKC");
  if (!alias) {
    throw new AliasValidationError("Alias must not be empty");
  }
  if (!isValidAlias(alias)) {
    throw new AliasValidationError(
      "Alias must be 1-64 characters using ASCII letters, digits, spaces, or . _ @ -",
    );
  }
  return alias;
}

function isValidAlias(alias: string): boolean {
  return /^[A-Za-z0-9][A-Za-z0-9._@ -]{0,63}$/.test(alias);
}

function aliasKey(alias: string): string {
  return alias.normalize("NFKC").toLowerCase();
}

function findAliasConflict(alias: string, excludeId?: string): { id: string } | null {
  const key = aliasKey(alias);
  if (excludeId) {
    return db
      .query("SELECT id FROM peers WHERE (alias_key = ? OR id = ?) AND id <> ?")
      .get(key, key, excludeId) as { id: string } | null;
  }
  return db.query("SELECT id FROM peers WHERE alias_key = ? OR id = ?").get(key, key) as {
    id: string;
  } | null;
}

function handleRegister(body: RegisterWithAliasRequest): RegisterResponse & { alias: string | null } {
  cleanStalePeers();
  const id = generateId();
  const now = new Date().toISOString();
  const alias = normalizeAlias(body.logical_name);
  const namedPeer = alias ? db.query("SELECT * FROM peers WHERE alias_key = ?")
    .get(aliasKey(alias)) as PeerWithAlias | null : null;
  if (namedPeer?.managed && namedPeer.status === "offline") {
    db.run(`UPDATE peers SET status = 'online', pid = ?, cwd = ?, git_root = ?,
      tty = ?, summary = ?, last_seen = ? WHERE id = ?`, [
      body.pid, body.cwd, body.git_root ?? null, body.tty ?? null,
      body.summary ?? "", now, namedPeer.id,
    ]);
    broadcastSnapshot();
    return { id: namedPeer.id, alias: namedPeer.alias };
  }

  // A remote PID alone is not globally unique, but PID + cwd is sufficient
  // to collapse retries from the same running gateway after a brief broker
  // outage without conflating the current Mac/A1/Proxmox nodes.
  const existing = body.pid > 0
    ? (REMOTE_HUB
      ? db.query("SELECT * FROM peers WHERE status = 'online' AND pid = ? AND cwd = ?")
          .get(body.pid, body.cwd)
      : db.query("SELECT * FROM peers WHERE status = 'online' AND pid = ?").get(body.pid)) as PeerWithAlias | null
    : null;
  if (alias && findAliasConflict(alias, existing?.id)) {
    throw new AliasConflictError(alias);
  }
  if (existing?.managed) {
    const updatedAlias = alias ?? existing.alias;
    db.run(`UPDATE peers SET alias = ?, alias_key = ?, cwd = ?, git_root = ?, tty = ?,
      summary = ?, last_seen = ? WHERE id = ?`, [updatedAlias,
      updatedAlias ? aliasKey(updatedAlias) : null, body.cwd, body.git_root ?? null,
      body.tty ?? null, body.summary ?? "", now, existing.id]);
    broadcastSnapshot();
    return { id: existing.id, alias: updatedAlias };
  }
  if (existing) deletePeer.run(existing.id);

  insertPeer.run(
    id,
    body.pid,
    body.cwd,
    body.git_root,
    body.tty,
    body.summary,
    now,
    now,
    alias,
    alias ? aliasKey(alias) : null,
  );
  broadcastSnapshot();
  return { id, alias };
}

function handleHeartbeat(body: HeartbeatRequest): { ok: boolean; reregister?: boolean } {
  if (isRevoked(body.id)) throw new PeerRevokedError();
  const result = updateLastSeen.run(new Date().toISOString(), body.id);
  if (result.changes === 0) {
    return { ok: false, reregister: true };
  }
  broadcastSnapshot();
  return { ok: true };
}

function handleSetSummary(body: SetSummaryRequest): void {
  updateSummary.run(body.summary, body.id);
  broadcastSnapshot();
}

function handleSetAlias(body: SetAliasRequest): { ok: true; alias: string } {
  const alias = normalizeAlias(body.alias);
  if (!alias) {
    throw new AliasValidationError("Alias must not be empty");
  }
  const peer = db.query("SELECT id FROM peers WHERE id = ?").get(body.id) as { id: string } | null;
  if (!peer) {
    throw new PeerNotFoundError(`Peer ${body.id} not found`);
  }
  if (findAliasConflict(alias, body.id)) {
    throw new AliasConflictError(alias);
  }
  updateAlias.run(alias, aliasKey(alias), body.id);
  broadcastSnapshot();
  return { ok: true, alias };
}

function handleListPeers(body: ListPeersRequest): PeerWithAlias[] {
  cleanStalePeers();
  let peers: PeerWithAlias[];

  switch (body.scope) {
    case "machine":
      peers = selectAllPeers.all() as PeerWithAlias[];
      break;
    case "directory":
      peers = selectPeersByDirectory.all(body.cwd) as PeerWithAlias[];
      break;
    case "repo":
      if (body.git_root) {
        peers = selectPeersByGitRoot.all(body.git_root) as PeerWithAlias[];
      } else {
        // No git root, fall back to directory
        peers = selectPeersByDirectory.all(body.cwd) as PeerWithAlias[];
      }
      break;
    default:
      peers = selectAllPeers.all() as PeerWithAlias[];
  }

  // Exclude the requesting peer
  if (body.exclude_id) {
    peers = peers.filter((p) => p.id !== body.exclude_id);
  }

  return peers;
}

function handleSendMessage(
  body: SendMessageRequest,
): { ok: boolean; error?: string; to_id?: string; to_alias?: string | null } {
  const sender = db.query("SELECT id, alias FROM peers WHERE id = ?").get(body.from_id) as {
    id: string;
    alias: string | null;
  } | null;

  // Preserve exact-ID compatibility, then resolve a human-readable alias.
  const target = (db.query("SELECT id, alias FROM peers WHERE id = ?").get(body.to_id) ??
    db.query("SELECT id, alias FROM peers WHERE alias_key = ?").get(aliasKey(body.to_id))) as {
      id: string;
      alias: string | null;
    } | null;
  if (!target) {
    return { ok: false, error: `Peer ${body.to_id} not found` };
  }

  const inserted = insertMessage.run(
    body.from_id,
    target.id,
    body.text,
    new Date().toISOString(),
    sender?.alias ?? null,
    sender ? "peer" : "system",
  );
  notifyMessage(target.id, Number(inserted.lastInsertRowid));
  return { ok: true, to_id: target.id, to_alias: target.alias };
}

function handlePollMessages(body: PollMessagesRequest & { mark_delivered?: boolean }): PollMessagesResponse {
  const messages = selectUndelivered.all(body.id) as (Message & { from_alias: string | null })[];

  if (body.mark_delivered !== false) {
    for (const msg of messages) markDelivered.run(msg.id);
    if (messages.length) broadcastSnapshot();
  }

  return { messages };
}

function handleUnregister(body: { id: string }): void {
  const peer = getPeer(body.id);
  if (peer?.managed) {
    db.run("UPDATE peers SET status = 'offline', pid = 0 WHERE id = ?", [body.id]);
  } else {
    deletePeer.run(body.id);
  }
  broadcastSnapshot();
}

// --- Dashboard data and server-sent events ---

type Controller = ReadableStreamDefaultController<Uint8Array>;
const peerSubscribers = new Map<string, Set<Controller>>();
const dashboardSubscribers = new Set<Controller>();
const subscriberCleanup = new Map<Controller, () => void>();
const encoder = new TextEncoder();

function dashboardPeers() {
  return db.query(`SELECT p.id, p.alias, p.pid, p.cwd, p.git_root, p.tty, p.summary,
    p.role, p.managed, p.status, p.registered_at, p.last_seen,
    (SELECT COUNT(*) FROM messages m WHERE m.to_id = p.id AND m.delivered = 0)
      AS undelivered_count FROM peers p ORDER BY p.registered_at, p.id`).all() as
        (PeerWithAlias & { undelivered_count: number })[];
}

const messageProjection = `SELECT m.id, m.from_id, m.from_alias, m.from_kind,
  m.to_id, p.alias AS to_alias, m.text, m.sent_at, m.delivered
  FROM messages m LEFT JOIN peers p ON p.id = m.to_id`;

function snapshot() {
  const peers = dashboardPeers() as (PeerWithAlias & { undelivered_count: number })[];
  const counts = db.query(`SELECT COUNT(*) AS total,
    COALESCE(SUM(CASE WHEN delivered = 0 THEN 1 ELSE 0 END), 0) AS undelivered,
    COALESCE(SUM(CASE WHEN sent_at >= ? THEN 1 ELSE 0 END), 0) AS per_min
    FROM messages`).get(new Date(Date.now() - 60_000).toISOString()) as {
      total: number; undelivered: number; per_min: number;
    };
  return {
    ts: new Date().toISOString(),
    hub: {
      remote: REMOTE_HUB, port: PORT,
      sse_peer_clients: Array.from(peerSubscribers.values()).reduce((n, set) => n + set.size, 0),
      sse_dashboard_clients: dashboardSubscribers.size,
      stale_timeout_ms: STALE_PEER_TIMEOUT_MS,
    },
    stats: {
      peers_total: peers.length,
      peers_online: peers.filter((p) => p.status === "online").length,
      peers_offline: peers.filter((p) => p.status === "offline").length,
      messages_total: counts.total, messages_undelivered: counts.undelivered,
      msg_per_min: counts.per_min,
      delivery_rate: counts.total ? (counts.total - counts.undelivered) / counts.total : 0,
    },
    peers,
    recent_messages: db.query(`${messageProjection} ORDER BY m.id DESC LIMIT 100`).all(),
    topology_edges: db.query(`SELECT m.from_id, m.to_id,
      COALESCE(sender.alias, m.from_alias) AS from_alias, target.alias AS to_alias,
      COUNT(*) AS count, SUM(CASE WHEN m.delivered = 0 THEN 1 ELSE 0 END) AS undelivered,
      MAX(m.sent_at) AS last_sent_at FROM messages m
      LEFT JOIN peers sender ON sender.id = m.from_id
      LEFT JOIN peers target ON target.id = m.to_id GROUP BY m.from_id, m.to_id`).all(),
  };
}

function event(name: string, data: unknown): string {
  return `event: ${name}\ndata: ${JSON.stringify(data)}\n\n`;
}

function push(controller: Controller, data: string) {
  try {
    controller.enqueue(encoder.encode(data));
  } catch {
    subscriberCleanup.get(controller)?.();
  }
}

function broadcastSnapshot() {
  if (!dashboardSubscribers.size) return;
  const data = event("snapshot", snapshot());
  for (const controller of dashboardSubscribers) push(controller, data);
}

function notifyMessage(peerId: string, messageId: number) {
  const hint = event("wake", { type: "messages", peer_id: peerId });
  for (const controller of peerSubscribers.get(peerId) ?? []) push(controller, hint);
  if (dashboardSubscribers.size) {
    const message = db.query(`${messageProjection} WHERE m.id = ?`).get(messageId);
    for (const controller of dashboardSubscribers) push(controller, event("message", message));
  }
  broadcastSnapshot();
}

function openStream(req: Request, subscribers: Set<Controller>, initial: () => string,
  intervalMs: number, onEmpty?: () => void): Response {
  let cleanup = () => {};
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      subscribers.add(controller);
      const timer = setInterval(() => push(controller, ": keepalive\n\n"), intervalMs);
      const abort = () => {
        cleanup();
        try { controller.close(); } catch { /* Already cancelled. */ }
      };
      cleanup = () => {
        clearInterval(timer);
        subscribers.delete(controller);
        subscriberCleanup.delete(controller);
        req.signal.removeEventListener("abort", abort);
        if (!subscribers.size) onEmpty?.();
      };
      subscriberCleanup.set(controller, cleanup);
      req.signal.addEventListener("abort", abort, { once: true });
      if (req.signal.aborted) abort();
      else push(controller, initial());
    },
    cancel() { cleanup(); },
  });
  return new Response(stream, { headers: {
    "Content-Type": "text/event-stream", "Cache-Control": "no-cache", "Connection": "keep-alive",
  } });
}

function requiredAlias(value: unknown): string {
  const alias = normalizeAlias(value);
  if (!alias) throw new AliasValidationError("Alias is required");
  return alias;
}

function normalizeTag(value: unknown, fallback = "peer"): string {
  if (value === undefined) return fallback;
  if (typeof value !== "string") {
    throw new RequestValidationError("Tag must be a string");
  }
  const trimmed = value.trim();
  if (!trimmed) return fallback;
  if (trimmed.length > MAX_TAG_LENGTH || /[\x00-\x1f\x7f]/.test(trimmed)) {
    throw new RequestValidationError("Tag must be at most 64 printable characters");
  }
  return trimmed;
}

function validatePeerFields(body: Record<string, unknown>) {
  if ("role" in body) {
    normalizeTag(body.role);
  }
  for (const field of ["summary", "cwd"]) {
    if (field in body && typeof body[field] !== "string") {
      throw new RequestValidationError(`${field} must be a string`);
    }
  }
  if ("managed" in body && body.managed !== 0 && body.managed !== 1 &&
      body.managed !== false && body.managed !== true) {
    throw new RequestValidationError("managed must be a boolean or 0/1");
  }
}

function createManagedPeer(body: Record<string, unknown>) {
  const alias = requiredAlias(body.alias);
  validatePeerFields(body);
  if (findAliasConflict(alias)) throw new AliasConflictError(alias);
  const id = generateId();
  const now = new Date().toISOString();
  const role = normalizeTag(body.role, "peer");
  db.run(`INSERT INTO peers (id, alias, alias_key, pid, cwd, summary, role,
    managed, status, registered_at, last_seen) VALUES (?, ?, ?, 0, ?, ?, ?, 1, 'offline', ?, ?)`,
    [id, alias, aliasKey(alias), body.cwd as string ?? "", body.summary as string ?? "",
      role, now, now]);
  broadcastSnapshot();
  return dashboardPeers().find((peer) => peer.id === id);
}

function patchPeer(id: string, body: Record<string, unknown>) {
  const peer = getPeer(id);
  if (!peer) throw new PeerNotFoundError(`Peer ${id} not found`);
  validatePeerFields(body);
  const alias = "alias" in body ? requiredAlias(body.alias) : peer.alias;
  if (alias && findAliasConflict(alias, id)) throw new AliasConflictError(alias);
  const role = "role" in body ? normalizeTag(body.role, "peer") : peer.role;
  db.run(`UPDATE peers SET alias = ?, alias_key = ?, summary = ?, role = ?, cwd = ?,
    managed = ? WHERE id = ?`, [alias, alias ? aliasKey(alias) : null,
    body.summary as string ?? peer.summary, role,
    body.cwd as string ?? peer.cwd, "managed" in body ? Number(body.managed) : peer.managed, id]);
  broadcastSnapshot();
  return dashboardPeers().find((candidate) => candidate.id === id);
}

function removePeer(id: string, force: boolean): Response {
  const peer = getPeer(id);
  if (!peer) throw new PeerNotFoundError(`Peer ${id} not found`);
  if (peer.status === "online" && !force) {
    return Response.json({ error: "peer_online",
      message: "Stop peer or pass force=1 to delete an online peer" }, { status: 409 });
  }
  revokedPeerIds.set(id, Date.now());
  for (const controller of peerSubscribers.get(id) ?? []) {
    push(controller, event("revoked", { reason: "deleted", peer_id: id }));
    try { controller.close(); } catch { /* Already closed. */ }
    subscriberCleanup.get(controller)?.();
  }
  db.transaction(() => {
    db.run("DELETE FROM messages WHERE to_id = ? AND delivered = 0", [id]);
    deletePeer.run(id);
  })();
  broadcastSnapshot();
  return Response.json({ ok: true, id });
}

function validToken(candidate: string): boolean {
  const actual = Buffer.from(candidate, "utf8");
  const expected = Buffer.from(HUB_TOKEN, "utf8");
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

type JwkKey = JsonWebKey & { kid?: string; kty?: string };
let cfJwksCache: { keys: JwkKey[]; fetchedAt: number } | null = null;
const CF_JWKS_TTL_MS = 60 * 60_000;

function extractCfJwt(req: Request): string {
  const headerJwt = (req.headers.get("Cf-Access-Jwt-Assertion") ?? "").trim();
  if (headerJwt) return headerJwt;
  const cookie = req.headers.get("Cookie") ?? "";
  for (const part of cookie.split(";")) {
    const trimmed = part.trim();
    if (trimmed.startsWith("CF_Authorization=")) {
      return trimmed.slice("CF_Authorization=".length).trim();
    }
  }
  return "";
}

function b64urlDecode(segment: string): Uint8Array {
  const padded = segment.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat((4 - (segment.length % 4)) % 4);
  return new Uint8Array(Buffer.from(padded, "base64"));
}

async function getCfJwks(forceRefresh = false): Promise<JwkKey[]> {
  if (!CF_ACCESS_TEAM_DOMAIN) return [];
  const now = Date.now();
  if (!forceRefresh && cfJwksCache && now - cfJwksCache.fetchedAt < CF_JWKS_TTL_MS) {
    return cfJwksCache.keys;
  }
  const resp = await fetch(`${CF_ACCESS_TEAM_DOMAIN}/cdn-cgi/access/certs`, {
    signal: AbortSignal.timeout(5000),
  });
  if (!resp.ok) throw new Error(`JWKS fetch HTTP ${resp.status}`);
  const data = await resp.json() as { keys?: JwkKey[] };
  const keys = Array.isArray(data.keys) ? data.keys : [];
  cfJwksCache = { keys, fetchedAt: now };
  return keys;
}

async function verifyCloudflareAccess(req: Request): Promise<string | null> {
  if (!CF_ACCESS_TEAM_DOMAIN || !CF_ACCESS_AUD) return null;
  const jwt = extractCfJwt(req);
  if (!jwt) {
    console.error("[cf-access] reject: missing_jwt");
    return null;
  }
  const parts = jwt.split(".");
  if (parts.length !== 3) {
    console.error("[cf-access] reject: malformed_jwt");
    return null;
  }
  try {
    const header = JSON.parse(Buffer.from(b64urlDecode(parts[0])).toString("utf8")) as { alg?: string; kid?: string };
    const payload = JSON.parse(Buffer.from(b64urlDecode(parts[1])).toString("utf8")) as {
      aud?: string | string[];
      iss?: string;
      exp?: number;
      nbf?: number;
      email?: string;
    };
    if (header.alg !== "RS256" || !header.kid) {
      console.error("[cf-access] reject: bad_alg_or_kid");
      return null;
    }
    const nowSec = Math.floor(Date.now() / 1000);
    if (typeof payload.exp !== "number" || payload.exp <= nowSec) {
      console.error("[cf-access] reject: expired");
      return null;
    }
    if (typeof payload.nbf === "number" && payload.nbf > nowSec + 60) {
      console.error("[cf-access] reject: not_before");
      return null;
    }
    const normIss = (payload.iss ?? "").replace(/\/+$/, "");
    if (normIss !== CF_ACCESS_TEAM_DOMAIN) {
      console.error("[cf-access] reject: iss_mismatch", normIss);
      return null;
    }
    const audList = Array.isArray(payload.aud) ? payload.aud : typeof payload.aud === "string" ? [payload.aud] : [];
    if (!audList.includes(CF_ACCESS_AUD)) {
      console.error("[cf-access] reject: aud_mismatch");
      return null;
    }

    let keys = await getCfJwks(false);
    let jwk = keys.find((k) => k.kid === header.kid);
    if (!jwk) {
      keys = await getCfJwks(true);
      jwk = keys.find((k) => k.kid === header.kid);
    }
    if (!jwk) {
      console.error("[cf-access] reject: kid_not_found");
      return null;
    }
    const key = await crypto.subtle.importKey(
      "jwk",
      jwk,
      { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
      false,
      ["verify"],
    );
    const sig = b64urlDecode(parts[2]);
    const data = new TextEncoder().encode(`${parts[0]}.${parts[1]}`);
    const valid = await crypto.subtle.verify("RSASSA-PKCS1-v1_5", key, sig, data);
    if (!valid) {
      console.error("[cf-access] reject: bad_signature");
      return null;
    }
    return typeof payload.email === "string" && payload.email ? payload.email : "cf-access";
  } catch (err) {
    console.error("[cf-access] reject: exception", err instanceof Error ? err.message : String(err));
    return null;
  }
}

// --- HTTP Server ---

async function route(req: Request, url: URL): Promise<Response> {
  const rawPath = url.pathname;
  const path = rawPath.startsWith("/dashboard/api/") ? rawPath.slice("/dashboard".length) : rawPath;
  const method = req.method;
  const api = path.startsWith("/api/");
  const publicShell = method === "GET" && (path === "/health" || path === "/dashboard");
  if (HUB_TOKEN && !publicShell) {
    const auth = req.headers.get("Authorization") ?? "";
    const bearer = auth.startsWith("Bearer ") ? auth.slice(7) : "";
    const sse = method === "GET" && (path === "/events" || path === "/api/stream");
    const tokenOk = validToken(bearer) || (sse && validToken(url.searchParams.get("token") ?? ""));
    if (!tokenOk) {
      const cfUser = api ? await verifyCloudflareAccess(req) : null;
      if (!cfUser) {
        return Response.json({ error: "unauthorized" }, { status: 401 });
      }
    }
  }
  if (api && req.headers.has("Origin")) {
    let sameHost = false;
    try {
      sameHost = new URL(req.headers.get("Origin")!).host.toLowerCase() ===
        req.headers.get("Host")?.toLowerCase();
    } catch { /* Malformed and opaque origins are rejected. */ }
    if (!sameHost) return Response.json({ error: "origin_mismatch" }, { status: 403 });
  }
  if (api && (method === "POST" || method === "PATCH") &&
      !req.headers.get("Content-Type")?.toLowerCase().startsWith("application/json")) {
    return Response.json({ error: "unsupported_media_type" }, { status: 415 });
  }
  try {
    if (method === "GET") {
      switch (path) {
        case "/health":
          return Response.json({ status: "ok", peers: (selectAllPeers.all() as Peer[]).length });
        case "/dashboard": {
          const file = Bun.file(new URL("./dashboard.html", import.meta.url));
          return new Response(await file.exists() ? file : "<!doctype html><html><head><title>Peers Dashboard</title></head><body><h1>Peers Dashboard</h1></body></html>",
            { headers: { "Content-Type": "text/html; charset=utf-8", "Referrer-Policy": "no-referrer" } });
        }
        case "/api/peers":
          cleanStalePeers();
          return Response.json({ peers: dashboardPeers() });
        case "/api/snapshot":
          cleanStalePeers();
          return Response.json(snapshot());
        case "/api/stream":
          cleanStalePeers();
          return openStream(req, dashboardSubscribers, () => event("snapshot", snapshot()), 20_000);
        case "/events": {
          const id = url.searchParams.get("id") ?? "";
          if (isRevoked(id)) throw new PeerRevokedError();
          if (!id || !getPeer(id)) throw new PeerNotFoundError("Peer not found");
          let subscribers = peerSubscribers.get(id);
          if (!subscribers) peerSubscribers.set(id, subscribers = new Set());
          return openStream(req, subscribers, () => ": connected\n\n", 25_000,
            () => peerSubscribers.delete(id));
        }
      }
      if (!api) return new Response("claude-peers broker");
    }
    const peerRoute = /^\/api\/peers\/([^/]+)$/.exec(path);
    if (method === "DELETE" && peerRoute) {
      return removePeer(decodeURIComponent(peerRoute[1]), url.searchParams.get("force") === "1");
    }
    if (method === "POST" || method === "PATCH") {
      const body = await req.json();
      if (!body || typeof body !== "object" || Array.isArray(body)) {
        throw new RequestValidationError("Expected a JSON object");
      }
      if (method === "PATCH" && peerRoute) {
        return Response.json(patchPeer(decodeURIComponent(peerRoute[1]), body));
      }
      if (method === "POST") switch (path) {
        case "/api/peers":
          return Response.json(createManagedPeer(body), { status: 201 });
        case "/api/messages": {
          if (typeof body.to_id !== "string" || typeof body.text !== "string") {
            throw new RequestValidationError("to_id and text must be strings");
          }
          const target = resolvePeer(body.to_id);
          if (!target) throw new PeerNotFoundError("Peer not found");
          const result = insertMessage.run("dashboard", target.id, body.text,
            new Date().toISOString(), "Dashboard", "system");
          const id = Number(result.lastInsertRowid);
          notifyMessage(target.id, id);
          return Response.json({ ok: true, id, to_id: target.id, to_alias: target.alias }, { status: 201 });
        }
        case "/register":
          return Response.json(handleRegister(body));
        case "/heartbeat":
          return Response.json(handleHeartbeat(body));
        case "/set-summary":
          handleSetSummary(body);
          return Response.json({ ok: true });
        case "/set-alias":
          return Response.json(handleSetAlias(body));
        case "/list-peers":
          return Response.json(handleListPeers(body));
        case "/send-message":
          return Response.json(handleSendMessage(body));
        case "/poll-messages":
          return Response.json(handlePollMessages(body));
        case "/mark-message-delivered":
          if (!Number.isInteger(body.id) || body.id <= 0) {
            throw new RequestValidationError("id must be a positive integer");
          }
          markDelivered.run(body.id);
          broadcastSnapshot();
          return Response.json({ ok: true });
        case "/unregister":
          handleUnregister(body);
          return Response.json({ ok: true });
      }
    }
    return Response.json({ error: "not found" }, { status: 404 });
  } catch (e) {
    if (e instanceof PeerRevokedError) return Response.json({ error: "peer_revoked" }, { status: 410 });
    if (e instanceof AliasConflictError) {
      return Response.json({ error: "alias_conflict", alias: e.alias }, { status: 409 });
    }
    if (e instanceof AliasValidationError) {
      return Response.json({ error: "invalid_alias", message: e.message }, { status: 400 });
    }
    if (e instanceof RequestValidationError || e instanceof SyntaxError) {
      return Response.json({ error: "invalid_request", message: e.message }, { status: 400 });
    }
    if (e instanceof PeerNotFoundError) {
      return Response.json({ error: "peer_not_found", message: e.message }, { status: 404 });
    }
    // Do not include request URLs or request contents in logs or error responses.
    return Response.json({ error: "internal_error" }, { status: 500 });
  }
}

cleanStalePeers();
setInterval(cleanStalePeers, 30_000);

Bun.serve({
  port: PORT,
  hostname: "127.0.0.1",
  // SSE streams have their own keepalives; the HTTP idle timeout must not close them.
  idleTimeout: 0,
  async fetch(req) {
    const url = new URL(req.url);
    const response = await route(req, url);
    if (url.pathname.startsWith("/api/")) response.headers.set("Cache-Control", "no-store");
    return response;
  },
});

console.error(`[claude-peers broker] listening on 127.0.0.1:${PORT} (db: ${DB_PATH})`);
