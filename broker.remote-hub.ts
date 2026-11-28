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
import { createHash, timingSafeEqual } from "node:crypto";
import { mkdirSync, unlinkSync } from "node:fs";
import { dirname, join } from "node:path";
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
const CF_ACCESS_JWKS_FILE = (process.env.CLAUDE_PEERS_CF_ACCESS_JWKS_FILE ?? "").trim();
const REMOTE_HUB = HUB_TOKEN.length > 0;
const STALE_PEER_TIMEOUT_MS = parseInt(
  process.env.CLAUDE_PEERS_STALE_TIMEOUT_MS ?? "90000",
  10,
);
// Attachment storage limits. Files live on disk next to the DB; only metadata is in SQLite.
function envNumber(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") return fallback;
  const value = Number(raw);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}
const ATTACHMENT_MAX_BYTES = envNumber("CLAUDE_PEERS_ATTACHMENT_MAX_BYTES", 20 * 1024 * 1024);
const ATTACHMENT_DIR = process.env.CLAUDE_PEERS_ATTACHMENT_DIR ?? join(dirname(DB_PATH), "attachments");
const ATTACHMENT_RETENTION_HOURS = envNumber("CLAUDE_PEERS_ATTACHMENT_RETENTION_HOURS", 168);
const ATTACHMENT_GC_MS = Math.max(100, envNumber("CLAUDE_PEERS_ATTACHMENT_GC_SECONDS", 300) * 1000);
const ATTACHMENT_ORPHAN_HOURS = 24;
const MAX_ATTACHMENTS_PER_MESSAGE = 5;
const MAX_ATTACHMENT_NAME_LENGTH = 128;

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
// JSON array of {id, name, size, mime}; resolved broker-side from the attachments table.
if (!hasColumn("messages", "attachments")) {
  db.run("ALTER TABLE messages ADD COLUMN attachments TEXT");
}
for (const [column, definition] of [
  ["kind", "TEXT NOT NULL DEFAULT 'peer'"],
  ["host_id", "TEXT"],
  ["agent_id", "TEXT"],
]) {
  if (!hasColumn("peers", column)) {
    db.run(`ALTER TABLE peers ADD COLUMN ${column} ${definition}`);
  }
}
// Agents reported by an OpenBot host daemon; replaced on every report.
db.run(`
  CREATE TABLE IF NOT EXISTS host_agents (
    host_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (host_id, agent_id)
  )
`);
db.run(`
  CREATE TRIGGER IF NOT EXISTS peers_host_agents_cleanup
  AFTER DELETE ON peers
  BEGIN
    DELETE FROM host_agents WHERE host_id = OLD.id;
  END
`);
// Uploaded files referenced by messages; the blob itself is `<ATTACHMENT_DIR>/<id>`.
db.run(`
  CREATE TABLE IF NOT EXISTS attachments (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    size INTEGER NOT NULL,
    mime TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    uploaded_by TEXT,
    created_at TEXT NOT NULL
  )
`);
mkdirSync(ATTACHMENT_DIR, { recursive: true });
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
    // OpenBot children are served by their host; only the host goes stale.
    if (peer.kind === "openbot") continue;
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
    if (peer.managed || peer.kind === "openbot-host") {
      db.run("UPDATE peers SET status = 'offline', pid = 0 WHERE id = ?", [peer.id]);
      if (peer.kind === "openbot-host") setHostChildrenStatus(peer.id, "offline");
    } else {
      deletePeerCascade(peer.id);
    }
    changed = true;
  }
  if (changed) broadcastSnapshot();
}

// --- Prepared statements ---

const insertPeer = db.prepare(`
  INSERT INTO peers (id, pid, cwd, git_root, tty, summary, registered_at, last_seen, alias, alias_key,
    managed, kind, host_id, agent_id)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
  SELECT id, pid, cwd, git_root, tty, summary, registered_at, last_seen, alias, managed, role, status,
    kind, host_id, agent_id FROM peers
`);

const selectPeersByDirectory = db.prepare(`
  SELECT id, pid, cwd, git_root, tty, summary, registered_at, last_seen, alias, managed, role, status,
    kind, host_id, agent_id FROM peers WHERE cwd = ?
`);

const selectPeersByGitRoot = db.prepare(`
  SELECT id, pid, cwd, git_root, tty, summary, registered_at, last_seen, alias, managed, role, status,
    kind, host_id, agent_id FROM peers WHERE git_root = ?
`);

const insertMessage = db.prepare(`
  INSERT INTO messages (from_id, to_id, text, sent_at, delivered, from_alias, from_kind, attachments)
  VALUES (?, ?, ?, ?, 0, ?, ?, ?)
`);

const insertAttachment = db.prepare(`
  INSERT INTO attachments (id, name, size, mime, sha256, uploaded_by, created_at)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

const selectAttachment = db.prepare(`
  SELECT id, name, size, mime, sha256, uploaded_by, created_at FROM attachments WHERE id = ?
`);

const deleteAttachment = db.prepare(`
  DELETE FROM attachments WHERE id = ?
`);

// Attachment ids that may be dropped: (a) referenced by no message and past the orphan
// window, or (b) referenced, but by no message newer than the retention window.
const selectExpiredAttachmentIds = db.prepare(`
  SELECT a.id,
    EXISTS (SELECT 1 FROM messages m, json_each(m.attachments) j WHERE j.value ->> 'id' = a.id) AS referenced
  FROM attachments a
  WHERE
    (a.created_at < ? AND NOT EXISTS (
      SELECT 1 FROM messages m, json_each(m.attachments) j WHERE j.value ->> 'id' = a.id))
    OR (EXISTS (SELECT 1 FROM messages m, json_each(m.attachments) j WHERE j.value ->> 'id' = a.id)
      AND NOT EXISTS (
        SELECT 1 FROM messages m, json_each(m.attachments) j
        WHERE j.value ->> 'id' = a.id AND m.sent_at >= ?))
`);

// OpenBot hosts poll on behalf of their children; the IN list varies per host,
// so the statement is built from the resolved child IDs (never from input).
function selectUndeliveredForHost(hostId: string): MessageWithAttachments[] {
  const ids = [hostId, ...hostChildren(hostId).map((child) => child.id)];
  return (db.query(
    `SELECT * FROM messages WHERE to_id IN (${ids.map(() => "?").join(", ")})
     AND delivered = 0 ORDER BY sent_at ASC, id ASC`,
  ).all(...ids) as MessageRow[]).map(toMessage);
}

const selectUndeliveredRows = db.prepare(`
  SELECT * FROM messages WHERE to_id = ? AND delivered = 0 ORDER BY sent_at ASC
`);

function selectUndeliveredMessages(peerId: string): MessageWithAttachments[] {
  return (selectUndeliveredRows.all(peerId) as MessageRow[]).map(toMessage);
}

// OpenBot hosts poll on behalf of their children; the IN list varies per host,
// so the statement is built from the resolved child IDs (never from input).
type MessageRow = Message & { from_alias: string | null; attachments?: string | null };

// Every message payload the broker hands out carries `attachments` as a parsed array.
type MessageWithAttachments = Omit<MessageRow, "attachments"> & { attachments: AttachmentRef[] };

function parseAttachments(json: string | null | undefined): AttachmentRef[] {
  if (!json) return [];
  try {
    const parsed = JSON.parse(json);
    return Array.isArray(parsed) ? parsed.filter((entry) => entry && typeof entry.id === "string") : [];
  } catch {
    return [];
  }
}

function toMessage(row: MessageRow): MessageWithAttachments {
  const { attachments, ...rest } = row;
  return { ...rest, attachments: parseAttachments(attachments) };
}

function insertMessageRow(fromId: string, toId: string, text: string, fromAlias: string | null,
  fromKind: string, attachments: AttachmentRef[]): number {
  const inserted = insertMessage.run(fromId, toId, text, new Date().toISOString(), fromAlias, fromKind,
    attachments.length ? JSON.stringify(attachments) : null);
  return Number(inserted.lastInsertRowid);
}

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

type RegisterWithAliasRequest = RegisterRequest & {
  logical_name?: string | null;
  kind?: string;
  agents?: HostAgent[];
  host_id?: string;
  agent_id?: string;
};
type PeerKind = "peer" | "openbot-host" | "openbot";
type HostAgent = { id: string; name: string };
type PeerWithAlias = Peer & {
  alias: string | null;
  managed: number;
  role: string;
  status: "online" | "offline";
  kind: PeerKind;
  host_id: string | null;
  agent_id: string | null;
};
type SetAliasRequest = { id: string; alias: string };
type AttachmentRow = {
  id: string; name: string; size: number; mime: string; sha256: string;
  uploaded_by: string | null; created_at: string;
};
// Metadata stored alongside a message and returned to clients (no sha256/owner).
type AttachmentRef = Pick<AttachmentRow, "id" | "name" | "size" | "mime">;

class AliasConflictError extends Error {
  constructor(public alias: string) {
    super(`Alias ${alias} is already in use`);
  }
}

class AliasValidationError extends Error {}
class PeerNotFoundError extends Error {}
class RequestValidationError extends Error {}
class AttachmentNotFoundError extends Error {
  constructor(public id: string) {
    super(`Attachment ${id} not found`);
  }
}
class PeerRevokedError extends Error {}
// Dashboard-side OpenBot host/agent validation; maps to HTTP 400 with the
// message as both `error` and `message` so the UI can show it verbatim.
class HostValidationError extends Error {
  constructor(message: string, public extra: Record<string, unknown> = {}) {
    super(message);
  }
}

const PEER_KINDS: readonly PeerKind[] = ["peer", "openbot-host", "openbot"];
const MAX_HOST_AGENTS = 200;
const MAX_AGENT_FIELD_LENGTH = 128;

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

function normalizeKind(value: unknown): PeerKind {
  if (value === undefined || value === null) return "peer";
  if (typeof value !== "string" || !(PEER_KINDS as readonly string[]).includes(value)) {
    throw new RequestValidationError("kind must be one of peer, openbot-host, openbot");
  }
  return value as PeerKind;
}

function normalizeAgentId(value: unknown, field = "agent_id"): string | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== "string") {
    throw new RequestValidationError(`${field} must be a string`);
  }
  const trimmed = value.trim();
  if (!trimmed || trimmed.length > MAX_AGENT_FIELD_LENGTH) {
    throw new RequestValidationError(`${field} must be 1-128 characters`);
  }
  return trimmed;
}

function normalizeHostId(value: unknown): string | null {
  return normalizeAgentId(value, "host_id");
}

// Validates the `agents` list an OpenBot host reports. Returns null when absent.
function normalizeAgents(value: unknown): HostAgent[] | null {
  if (value === undefined || value === null) return null;
  if (!Array.isArray(value)) {
    throw new RequestValidationError("agents must be an array of {id, name}");
  }
  if (value.length > MAX_HOST_AGENTS) {
    throw new RequestValidationError(`agents must contain at most ${MAX_HOST_AGENTS} entries`);
  }
  const seen = new Set<string>();
  return value.map((entry) => {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      throw new RequestValidationError("agents entries must be {id, name} objects");
    }
    if (entry.id == null || entry.name == null) {
      throw new RequestValidationError("agents entries must have non-empty id and name");
    }
    const id = normalizeAgentId(entry.id, "agents[].id")!;
    const name = normalizeAgentId(entry.name, "agents[].name")!;
    if (seen.has(id)) {
      throw new RequestValidationError(`agents contains duplicate id ${id}`);
    }
    seen.add(id);
    return { id, name };
  });
}

function replaceHostAgents(hostId: string, agents: HostAgent[]) {
  db.transaction(() => {
    db.run("DELETE FROM host_agents WHERE host_id = ?", [hostId]);
    const insert = db.prepare("INSERT INTO host_agents (host_id, agent_id, name) VALUES (?, ?, ?)");
    for (const agent of agents) insert.run(hostId, agent.id, agent.name);
  })();
}

function hostAgents(hostId: string): HostAgent[] {
  return db.query("SELECT agent_id AS id, name FROM host_agents WHERE host_id = ? ORDER BY agent_id")
    .all(hostId) as HostAgent[];
}

function hostChildren(hostId: string): { id: string; alias: string | null; agent_id: string | null }[] {
  return db.query(
    "SELECT id, alias, agent_id FROM peers WHERE host_id = ? AND kind = 'openbot' ORDER BY registered_at, id",
  ).all(hostId) as { id: string; alias: string | null; agent_id: string | null }[];
}

function setHostChildrenStatus(hostId: string, status: "online" | "offline") {
  db.run("UPDATE peers SET status = ?, pid = 0 WHERE host_id = ? AND kind = 'openbot' AND status <> ?",
    [status, hostId, status]);
}

// Removes a peer plus, for a host, its children and their undelivered messages.
// host_agents rows are removed by the peers AFTER DELETE trigger.
function deletePeerCascade(id: string) {
  db.transaction(() => {
    const childIds = (db.query("SELECT id FROM peers WHERE host_id = ?").all(id) as { id: string }[])
      .map((child) => child.id);
    for (const target of [id, ...childIds]) {
      db.run("DELETE FROM messages WHERE to_id = ? AND delivered = 0", [target]);
    }
    db.run("DELETE FROM peers WHERE host_id = ?", [id]);
    deletePeer.run(id);
  })();
}

function handleRegister(body: RegisterWithAliasRequest): RegisterResponse & { alias: string | null } {
  cleanStalePeers();
  const id = generateId();
  const now = new Date().toISOString();
  const alias = normalizeAlias(body.logical_name);
  const kind = normalizeKind(body.kind);
  const agents = normalizeAgents(body.agents);
  const hostId = normalizeHostId(body.host_id);
  const agentId = normalizeAgentId(body.agent_id);
  if (agents && kind !== "openbot-host") {
    throw new RequestValidationError("agents may only be reported by kind openbot-host");
  }
  // Hosts are durable address-book entries (their children depend on them),
  // so they survive going stale like dashboard-managed peers.
  const managed = kind === "openbot-host" ? 1 : 0;
  const namedPeer = alias ? db.query("SELECT * FROM peers WHERE alias_key = ?")
    .get(aliasKey(alias)) as PeerWithAlias | null : null;
  // A host may also adopt an offline legacy "peer" entry: that is the migration
  // path when a machine switches from OPENBOT_AGENT mode to host mode. The
  // reverse is refused so children are never left without their host.
  const adoptable = namedPeer?.managed && namedPeer.status === "offline" &&
    (namedPeer.kind === kind || (namedPeer.kind === "peer" && kind === "openbot-host"));
  if (adoptable) {
    db.run(`UPDATE peers SET status = 'online', pid = ?, cwd = ?, git_root = ?,
      tty = ?, summary = ?, last_seen = ?, kind = ?, host_id = ?, agent_id = ? WHERE id = ?`, [
      body.pid, body.cwd, body.git_root ?? null, body.tty ?? null,
      body.summary ?? "", now, kind, hostId, agentId, namedPeer.id,
    ]);
    if (kind === "openbot-host") {
      if (agents) replaceHostAgents(namedPeer.id, agents);
      setHostChildrenStatus(namedPeer.id, "online");
    }
    broadcastSnapshot();
    return { id: namedPeer.id, alias: namedPeer.alias };
  }

  // A remote PID alone is not globally unique, but PID + cwd is sufficient
  // to collapse retries from the same running gateway after a brief broker
  // outage without conflating the current Mac/A1/Proxmox nodes.
  const existing = body.pid > 0
    ? (REMOTE_HUB
      ? db.query("SELECT * FROM peers WHERE status = 'online' AND pid = ? AND cwd = ? AND kind = ?")
          .get(body.pid, body.cwd, kind)
      : db.query("SELECT * FROM peers WHERE status = 'online' AND pid = ? AND kind = ?").get(body.pid, kind)) as PeerWithAlias | null
    : null;
  if (alias && findAliasConflict(alias, existing?.id)) {
    throw new AliasConflictError(alias);
  }
  if (existing?.managed) {
    const updatedAlias = alias ?? existing.alias;
    db.run(`UPDATE peers SET alias = ?, alias_key = ?, cwd = ?, git_root = ?, tty = ?,
      summary = ?, last_seen = ?, kind = ?, host_id = ?, agent_id = ? WHERE id = ?`, [updatedAlias,
      updatedAlias ? aliasKey(updatedAlias) : null, body.cwd, body.git_root ?? null,
      body.tty ?? null, body.summary ?? "", now, kind, hostId, agentId, existing.id]);
    if (kind === "openbot-host" && agents) replaceHostAgents(existing.id, agents);
    broadcastSnapshot();
    return { id: existing.id, alias: updatedAlias };
  }
  if (existing) deletePeerCascade(existing.id);

  insertPeer.run(
    id,
    body.pid,
    body.cwd,
    body.git_root ?? null,
    body.tty ?? null,
    body.summary ?? "",
    now,
    now,
    alias,
    alias ? aliasKey(alias) : null,
    managed,
    kind,
    hostId,
    agentId,
  );
  if (kind === "openbot-host" && agents) replaceHostAgents(id, agents);
  broadcastSnapshot();
  return { id, alias };
}

function handleHeartbeat(body: HeartbeatRequest & { agents?: HostAgent }): {
  ok: boolean; reregister?: boolean; served_peers?: { id: string; alias: string | null; agent_id: string | null }[];
} {
  if (isRevoked(body.id)) throw new PeerRevokedError();
  const agents = normalizeAgents(body.agents);
  const result = updateLastSeen.run(new Date().toISOString(), body.id);
  if (result.changes === 0) {
    return { ok: false, reregister: true };
  }
  const peer = getPeer(body.id);
  if (!peer) return { ok: false, reregister: true };
  if (agents && peer.kind !== "openbot-host") {
    throw new RequestValidationError("agents may only be reported by kind openbot-host");
  }
  if (agents) replaceHostAgents(peer.id, agents);
  broadcastSnapshot();
  if (peer.kind !== "openbot-host") return { ok: true };
  return { ok: true, served_peers: hostChildren(peer.id) };
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

  const attachments = resolveAttachmentIds(body.attachments);
  const id = insertMessageRow(body.from_id, target.id, body.text, sender?.alias ?? null,
    sender ? "peer" : "system", attachments);
  notifyMessage(target.id, id);
  return { ok: true, to_id: target.id, to_alias: target.alias };
}

function handlePollMessages(body: PollMessagesRequest & { mark_delivered?: boolean }): PollMessagesResponse {
  const peer = getPeer(body.id);
  const messages = peer?.kind === "openbot-host"
    ? selectUndeliveredForHost(peer.id)
    : selectUndeliveredMessages(body.id);

  if (body.mark_delivered !== false) {
    for (const msg of messages) markDelivered.run(msg.id);
    if (messages.length) broadcastSnapshot();
  }

  return { messages };
}

function handleUnregister(body: { id: string }): void {
  const peer = getPeer(body.id);
  if (peer?.managed || peer?.kind === "openbot-host") {
    db.run("UPDATE peers SET status = 'offline', pid = 0 WHERE id = ?", [body.id]);
    if (peer?.kind === "openbot-host") setHostChildrenStatus(body.id, "offline");
  } else {
    deletePeerCascade(body.id);
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
  const peers = db.query(`SELECT p.id, p.alias, p.pid, p.cwd, p.git_root, p.tty, p.summary,
    p.role, p.managed, p.status, p.registered_at, p.last_seen, p.kind, p.host_id, p.agent_id,
    (SELECT COUNT(*) FROM messages m WHERE m.to_id = p.id AND m.delivered = 0)
      AS undelivered_count FROM peers p ORDER BY p.registered_at, p.id`).all() as
        (PeerWithAlias & { undelivered_count: number; agents: HostAgent[] })[];
  for (const peer of peers) {
    peer.agents = peer.kind === "openbot-host" ? hostAgents(peer.id) : [];
  }
  return peers;
}

const messageProjection = `SELECT m.id, m.from_id, m.from_alias, m.from_kind,
  m.to_id, p.alias AS to_alias, m.text, m.sent_at, m.delivered, m.attachments
  FROM messages m LEFT JOIN peers p ON p.id = m.to_id`;

const selectRecentMessages = db.prepare(`${messageProjection} ORDER BY m.id DESC LIMIT ?`);
const selectMessageById = db.prepare(`${messageProjection} WHERE m.id = ?`);

function recentMessages(limit: number): MessageWithAttachments[] {
  return (selectRecentMessages.all(limit) as MessageRow[]).map(toMessage);
}

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
    recent_messages: recentMessages(100),
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
  // An OpenBot child is served through its host's SSE connection.
  const hostId = getPeer(peerId)?.host_id;
  if (hostId && hostId !== peerId) {
    for (const controller of peerSubscribers.get(hostId) ?? []) push(controller, hint);
  }
  if (dashboardSubscribers.size) {
    const row = selectMessageById.get(messageId) as MessageRow | null;
    const message = row ? toMessage(row) : null;
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
  const kind = normalizeKind(body.kind);
  const hostId = normalizeHostId(body.host_id);
  const agentId = normalizeAgentId(body.agent_id);
  if ("agents" in body) {
    throw new RequestValidationError("agents can only be reported by the host daemon");
  }
  let status: "online" | "offline" = "offline";
  let role = normalizeTag(body.role, "peer");
  if (kind === "openbot") {
    role = normalizeTag(body.role, "openbot");
    if (!hostId || !agentId) {
      throw new HostValidationError("host_id and agent_id are required for kind openbot");
    }
    const host = getPeer(hostId);
    if (!host) throw new HostValidationError("host not found", { host_id: hostId });
    if (host.kind !== "openbot-host") {
      throw new HostValidationError("host is not an OpenBot host", { host_id: hostId });
    }
    if (host.status !== "online") {
      throw new HostValidationError(
        "host is offline; install and start the bridge on that machine first",
        { host_id: hostId });
    }
    if (!hostAgents(hostId).some((agent) => agent.id === agentId)) {
      throw new HostValidationError("agent not found on host", { host_id: hostId, agent_id: agentId });
    }
    status = host.status;
  } else if (kind !== "peer" || hostId || agentId) {
    throw new HostValidationError(
      "only kind openbot accepts host_id and agent_id; hosts register from the daemon");
  }
  if (findAliasConflict(alias)) throw new AliasConflictError(alias);
  const id = generateId();
  const now = new Date().toISOString();
  db.run(`INSERT INTO peers (id, alias, alias_key, pid, cwd, summary, role,
    managed, status, registered_at, last_seen, kind, host_id, agent_id)
    VALUES (?, ?, ?, 0, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?)`,
    [id, alias, aliasKey(alias), body.cwd as string ?? "", body.summary as string ?? "",
      role, status, now, now, kind, hostId, agentId]);
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
  const removedIds = peer.kind === "openbot-host" ? [id, ...hostChildren(id).map((c) => c.id)] : [id];
  for (const removedId of removedIds) {
    revokedPeerIds.set(removedId, Date.now());
    for (const controller of peerSubscribers.get(removedId) ?? []) {
      push(controller, event("revoked", { reason: "deleted", peer_id: removedId }));
      try { controller.close(); } catch { /* Already closed. */ }
      subscriberCleanup.get(controller)?.();
    }
  }
  deletePeerCascade(id);
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
  try {
    const resp = await fetch(`${CF_ACCESS_TEAM_DOMAIN}/cdn-cgi/access/certs`, {
      signal: AbortSignal.timeout(5000),
    });
    if (!resp.ok) throw new Error(`JWKS fetch HTTP ${resp.status}`);
    const data = await resp.json() as { keys?: JwkKey[] };
    const keys = Array.isArray(data.keys) ? data.keys : [];
    cfJwksCache = { keys, fetchedAt: now };
    return keys;
  } catch (err) {
    if (CF_ACCESS_JWKS_FILE) {
      const file = Bun.file(CF_ACCESS_JWKS_FILE);
      if (await file.exists()) {
        const data = await file.json() as { keys?: JwkKey[] };
        const keys = Array.isArray(data.keys) ? data.keys : [];
        cfJwksCache = { keys, fetchedAt: now };
        return keys;
      }
    }
    throw err;
  }
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

// --- Attachments ---

const ATTACHMENT_ID_PATTERN = /^[0-9a-f]{32}$/;
const ATTACHMENT_MIME_ALLOWLIST = /^(image\/|text\/|audio\/|video\/|application\/(pdf|json|zip|octet-stream|vnd\.openxmlformats-officedocument\.|msword$))/;
const ATTACHMENT_EXTENSION_MIMES: Record<string, string> = {
  png: "image/png", jpg: "image/jpeg", jpeg: "image/jpeg", gif: "image/gif", webp: "image/webp",
  svg: "image/svg+xml", pdf: "application/pdf", txt: "text/plain", md: "text/plain", csv: "text/csv",
  json: "application/json", zip: "application/zip",
  docx: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  pptx: "application/vnd.openxmlformats-officedocument.presentationml.presentation",
  mp3: "audio/mpeg", wav: "audio/wav", m4a: "audio/mp4", mp4: "video/mp4", mov: "video/quicktime",
  webm: "video/webm",
};

function attachmentPath(id: string): string {
  return join(ATTACHMENT_DIR, id);
}

// Lowercased type without parameters ("Text/Plain; charset=utf-8" -> "text/plain").
function normalizeMime(mime: string): string {
  return mime.split(";")[0].trim().toLowerCase();
}

function inferAttachmentMime(name: string): string {
  const extension = name.includes(".") ? name.split(".").pop()!.toLowerCase() : "";
  return ATTACHMENT_EXTENSION_MIMES[extension] ?? "application/octet-stream";
}

// Basename only, no control characters, bounded length; never empty.
function sanitizeAttachmentName(raw: string): string {
  const base = raw.split(/[\\/]/).pop() ?? "";
  const cleaned = Array.from(base.replace(/[\x00-\x1f\x7f]/g, "").trim()).slice(0, MAX_ATTACHMENT_NAME_LENGTH).join("");
  return cleaned || "file";
}

function contentDisposition(name: string): string {
  const ascii = name.replace(/[^\x20-\x7e]/g, "_").replace(/(["\\])/g, "\\$1");
  const base = `attachment; filename="${ascii}"`;
  return /[^\x20-\x7e]/.test(name) ? `${base}; filename*=UTF-8''${encodeURIComponent(name).replace(/'/g, "%27")}` : base;
}

// Validates the optional `attachments` id list on an outgoing message and resolves
// each id to stored metadata; client-supplied names are never trusted.
function resolveAttachmentIds(value: unknown): AttachmentRef[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value) || value.some((entry) => typeof entry !== "string" || !entry)) {
    throw new RequestValidationError("attachments must be an array of attachment ids");
  }
  if (value.length > MAX_ATTACHMENTS_PER_MESSAGE) {
    throw new RequestValidationError(`attachments must contain at most ${MAX_ATTACHMENTS_PER_MESSAGE} entries`);
  }
  const refs: AttachmentRef[] = [];
  for (const id of new Set(value as string[])) {
    const row = selectAttachment.get(id) as AttachmentRow | null;
    if (!row) throw new AttachmentNotFoundError(id);
    refs.push({ id: row.id, name: row.name, size: row.size, mime: row.mime });
  }
  return refs;
}

// Minimal multipart/form-data parser. Bun's formData() derives File.type from the
// filename and drops the part's declared Content-Type, which the allowlist needs.
type MultipartPart = { name: string; filename: string | null; type: string; data: Uint8Array };

function parseMultipart(body: Uint8Array, contentTypeHeader: string): MultipartPart[] {
  const match = /boundary=(?:"([^"]+)"|([^;]+))/i.exec(contentTypeHeader);
  if (!match) throw new RequestValidationError("Malformed multipart body");
  const boundary = `--${(match[1] ?? match[2]).trim()}`;
  // latin1 keeps byte offsets identical to the raw body so parts can be sliced without copying.
  const text = Buffer.from(body).toString("latin1");
  const utf8 = (value: string) => Buffer.from(value, "latin1").toString("utf8");
  const parts: MultipartPart[] = [];
  let cursor = text.indexOf(boundary);
  if (cursor === -1) throw new RequestValidationError("Malformed multipart body");
  while (cursor !== -1) {
    cursor += boundary.length;
    if (text.startsWith("--", cursor)) break; // Closing delimiter.
    if (text.startsWith("\r\n", cursor)) cursor += 2;
    else if (text.startsWith("\n", cursor)) cursor += 1;
    else break;
    const headerEnd = text.indexOf("\r\n\r\n", cursor);
    if (headerEnd === -1) break;
    const dataStart = headerEnd + 4;
    const next = text.indexOf(`\r\n${boundary}`, dataStart);
    if (next === -1) break;
    let name = "";
    let filename: string | null = null;
    let type = "";
    for (const line of text.slice(cursor, headerEnd).split("\r\n")) {
      const sep = line.indexOf(":");
      if (sep === -1) continue;
      const key = line.slice(0, sep).trim().toLowerCase();
      const value = line.slice(sep + 1).trim();
      if (key === "content-disposition") {
        // Quoted parameter values escape '"' as '\"' (RFC 2578). Backslashes are kept
        // literal because clients send Windows paths (C:\dir\file) unescaped.
        name = /\bname="((?:[^"\\]|\\.)*)"/i.exec(value)?.[1].replace(/\\"/g, "\"") ?? name;
        const rawFilename = /\bfilename="((?:[^"\\]|\\.)*)"/i.exec(value)?.[1];
        if (rawFilename !== undefined) filename = rawFilename.replace(/\\"/g, "\"");
      } else if (key === "content-type") {
        type = normalizeMime(value);
      }
    }
    parts.push({ name: utf8(name), filename: filename === null ? null : utf8(filename), type, data: body.subarray(dataStart, next) });
    cursor = next + 2;
  }
  if (!parts.length) throw new RequestValidationError("Malformed multipart body");
  return parts;
}

async function handleUploadAttachment(req: Request): Promise<Response> {
  // Keep the raw header for boundary parsing: boundaries are case-sensitive
  // (browsers send "----WebKitFormBoundary…"), only the media type check is case-insensitive.
  const rawContentType = req.headers.get("Content-Type") ?? "";
  if (!rawContentType.toLowerCase().startsWith("multipart/form-data")) {
    throw new RequestValidationError("Expected multipart/form-data");
  }
  // Cheap rejection before buffering; the exact size is re-checked on the parsed part.
  const declaredLength = Number(req.headers.get("Content-Length") ?? "0");
  if (declaredLength > ATTACHMENT_MAX_BYTES + 64 * 1024) {
    throw new RequestValidationError(`File exceeds the ${ATTACHMENT_MAX_BYTES} byte limit`);
  }
  const body = new Uint8Array(await req.arrayBuffer());
  const parts = parseMultipart(body, rawContentType);
  const file = parts.find((part) => part.name === "file" && part.filename !== null);
  if (!file) {
    throw new RequestValidationError("Missing file field 'file'");
  }
  if (file.data.length === 0) throw new RequestValidationError("File is empty");
  if (file.data.length > ATTACHMENT_MAX_BYTES) {
    throw new RequestValidationError(`File exceeds the ${ATTACHMENT_MAX_BYTES} byte limit`);
  }
  const name = sanitizeAttachmentName(file.filename!);
  const mime = file.type || inferAttachmentMime(name);
  if (!ATTACHMENT_MIME_ALLOWLIST.test(mime)) {
    throw new RequestValidationError(`File type ${mime} is not allowed`);
  }
  const bytes = file.data;
  const sha256 = createHash("sha256").update(bytes).digest("hex");
  const id = Buffer.from(crypto.getRandomValues(new Uint8Array(16))).toString("hex");
  const uploader = parts.find((part) => part.name === "from_id");
  await Bun.write(attachmentPath(id), bytes);
  try {
    insertAttachment.run(id, name, bytes.length, mime, sha256,
      uploader ? Buffer.from(uploader.data).toString("utf8").slice(0, 128) : null, new Date().toISOString());
  } catch (err) {
    try { unlinkSync(attachmentPath(id)); } catch { /* Best effort. */ }
    throw err;
  }
  console.error(`[attachments] stored ${id} (${mime}, ${bytes.length} bytes) as ${name}`);
  return Response.json({ id, name, size: bytes.length, mime, sha256 }, { status: 201 });
}

async function handleDownloadAttachment(id: string, method: string): Promise<Response> {
  const row = ATTACHMENT_ID_PATTERN.test(id) ? selectAttachment.get(id) as AttachmentRow | null : null;
  if (!row) throw new AttachmentNotFoundError(id);
  const file = Bun.file(attachmentPath(id));
  if (!(await file.exists())) throw new AttachmentNotFoundError(id);
  const headers = {
    "Content-Type": row.mime,
    "Content-Length": String(row.size),
    "Content-Disposition": contentDisposition(row.name),
    "X-Content-Type-Options": "nosniff",
    "Cache-Control": "no-store",
  };
  // HEAD answers with the same headers and no body.
  return new Response(method === "HEAD" ? null : file, { headers });
}

// Drops orphaned uploads after 24h and attachments whose referencing messages are
// all older than the retention window. Failures are logged, never fatal.
function gcAttachments() {
  try {
    const now = Date.now();
    const orphanCutoff = new Date(now - ATTACHMENT_ORPHAN_HOURS * 3_600_000).toISOString();
    const retentionCutoff = new Date(now - ATTACHMENT_RETENTION_HOURS * 3_600_000).toISOString();
    const expired = selectExpiredAttachmentIds.all(orphanCutoff, retentionCutoff) as { id: string; referenced: number }[];
    for (const { id, referenced } of expired) {
      deleteAttachment.run(id);
      try { unlinkSync(attachmentPath(id)); } catch { /* File already gone. */ }
      console.error(`[attachments] gc deleted ${id} (${referenced ? "retention expired" : "unreferenced"})`);
    }
  } catch (err) {
    console.error("[attachments] gc failed", err instanceof Error ? err.message : String(err));
  }
}

// --- Routing ---

async function route(req: Request, url: URL): Promise<Response> {
  const rawPath = url.pathname;
  // `/dashboard/api/*` is the dashboard's prefixed view of the same API; attachments
  // answer on both `/attachments` and `/api/attachments` so the prefix works there too.
  let path = rawPath.startsWith("/dashboard/api/") ? rawPath.slice("/dashboard".length) : rawPath;
  if (path === "/api/attachments" || path.startsWith("/api/attachments/")) path = path.slice(4);
  const method = req.method;
  // Same-origin checks and no-store headers apply to the dashboard-facing API surface.
  const api = path.startsWith("/api/") || path.startsWith("/attachments");
  const publicShell = method === "GET" && (path === "/health" || path === "/dashboard");
  const attachmentDownload = method === "GET" || method === "HEAD" ? /^\/attachments\/([^/]+)$/.exec(path) : null;
  if (HUB_TOKEN && !publicShell) {
    const auth = req.headers.get("Authorization") ?? "";
    const bearer = auth.startsWith("Bearer ") ? auth.slice(7) : "";
    const sse = method === "GET" && (path === "/events" || path === "/api/stream");
    // Downloads also accept the SSE-style query token so the dashboard can embed them.
    const tokenOk = validToken(bearer) || ((sse || attachmentDownload) && validToken(url.searchParams.get("token") ?? ""));
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
  // Multipart upload is the one non-JSON POST.
  const isUpload = method === "POST" && path === "/attachments";
  if (api && !isUpload && (method === "POST" || method === "PATCH") &&
      !req.headers.get("Content-Type")?.toLowerCase().startsWith("application/json")) {
    return Response.json({ error: "unsupported_media_type" }, { status: 415 });
  }
  try {
    if (attachmentDownload) {
      return await handleDownloadAttachment(decodeURIComponent(attachmentDownload[1]), method);
    }
    if (isUpload) return await handleUploadAttachment(req);
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
        case "/api/messages": {
          // Dashboard history (same projection as snapshot.recent_messages).
          const requested = Number(url.searchParams.get("limit") ?? "200");
          const limit = Number.isInteger(requested) && requested > 0 ? Math.min(requested, 500) : 200;
          return Response.json({ messages: recentMessages(limit) });
        }
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
          const attachments = resolveAttachmentIds(body.attachments);
          const id = insertMessageRow("dashboard", target.id, body.text, "Dashboard", "system", attachments);
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
    if (e instanceof AttachmentNotFoundError) {
      return Response.json({ error: "attachment not found", id: e.id }, { status: 404 });
    }
    if (e instanceof HostValidationError) {
      return Response.json({ error: e.message, message: e.message, ...e.extra }, { status: 400 });
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
gcAttachments();
setInterval(gcAttachments, ATTACHMENT_GC_MS);

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
