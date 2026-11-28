# peers-bridge

**English** | [한국어](README.ko.md)

**Cross-network peer messaging for [Hermes Agent](https://hermes-agent.nousresearch.com) — let AI agents on different machines talk to each other.**

A Hermes Agent plugin that connects multiple Hermes instances (across different machines and networks) through a central [claude-peers](https://github.com/louislva/claude-peers-mcp) broker hub. Each agent registers as a named peer, and agents can send messages to each other by alias — a gateway agent on a cloud VM can ask the agent on your laptop to run a local task, and get the answer back as a normal conversation turn.

```
┌─────────┐      ┌──────────────────┐      ┌─────────┐
│ Agent A │◄────►│   Remote Hub     │◄────►│ Agent B │
│ (laptop)│      │ (broker, HTTPS)  │      │ (VM)    │
└─────────┘      └──────────────────┘      └─────────┘
                        ▲
                        │
                  ┌─────────┐
                  │ Agent C │
                  │ (server)│
                  └─────────┘
```

## Who is this for?

- You run **Hermes Agent on more than one machine** (e.g. a laptop, a home server, a cloud VM) and want the agents to coordinate: delegate work, ask questions, report status to each other.
- You want **agent-to-agent messaging across networks** without exposing each machine — only the hub needs a public endpoint.
- You already use the local `claude-peers` broker for same-machine peers and want to extend it across hosts.

Requirements: Hermes Agent with plugin support, Python 3.10+, and one host that can run the broker (Bun/Node + a public HTTPS endpoint, e.g. behind a reverse proxy or Cloudflare Tunnel).

## Features

- **Named peers** — human-readable aliases (`Laptop`, `HomeServer`, `CloudVM`) instead of opaque IDs; send by alias or ID.
- **Gateway & CLI aware** — gateway processes register against the remote hub; interactive CLI/TUI sessions use the local broker. Subagents are excluded automatically.
- **Real message delivery** — incoming peer messages are injected into the gateway's home-channel session as a normal agent turn, so the receiving agent actually reads and answers them (works on both current and older Hermes gateway builds).
- **Duplicate-safe** — message-ID deduplication and hub-aware delivery marking prevent the same message from being processed twice.
- **Self-healing registration** — heartbeat-based liveness, automatic re-registration after broker restarts or transient outages.
- **OpenBot machines** — one bridge daemon per machine reports its OpenBot agents to the hub; agents are registered as child peers from the dashboard and addressed by alias like any other peer.
- **Attachments** — send up to 5 files (20 MB each) with a message; receivers get the files downloaded locally and passed to the agent as absolute paths.

## Installation

### 1. Set up the hub broker (one machine)

The upstream `claude-peers` broker assumes all peers live on the broker host (it checks PIDs locally). For a cross-network hub, use the patched `broker.remote-hub.ts` from this repo, which uses heartbeat age instead:

```bash
git clone https://github.com/louislva/claude-peers-mcp
cd claude-peers-mcp
# replace the stock broker with the remote-hub variant
curl -sL https://raw.githubusercontent.com/insightflo/hermes-peers-bridge/main/broker.remote-hub.ts \
  -o broker.ts
```

Configure and run it as a service (systemd, etc.):

```bash
CLAUDE_PEERS_PORT=7899
CLAUDE_PEERS_DB=/var/lib/claude-peers/peers.db
CLAUDE_PEERS_HUB_TOKEN=<generate-a-long-random-token>
# Optional; peers older than this (ms) are considered stale. Default 90000.
CLAUDE_PEERS_STALE_TIMEOUT_MS=90000
```

Expose port 7899 via HTTPS (reverse proxy / Cloudflare Tunnel), then verify:

```bash
curl -H "Authorization: Bearer $CLAUDE_PEERS_HUB_TOKEN" \
  https://your-broker.example.com/health
# → {"status":"ok","peers":0}
```

> Keep the hub token secret. Store it in a `chmod 600` env file; never commit it.

### 2. Install the plugin (every Hermes machine)

Copy the plugin files into the Hermes plugins directory:

```bash
mkdir -p ~/.hermes/plugins/claude_peers_bridge
cd ~/.hermes/plugins/claude_peers_bridge
for f in __init__.py bridge.py schemas.py plugin.yaml; do
  curl -sLO https://raw.githubusercontent.com/insightflo/hermes-peers-bridge/main/$f
done
python3 -m py_compile bridge.py && echo OK
```

Enable it in `~/.hermes/config.yaml`:

```yaml
plugins:
  enabled:
    - claude-peers-bridge
```

Configure `~/.hermes/.env`:

```bash
# Remote hub (used by gateway processes)
CLAUDE_PEERS_BROKER_URL=https://your-broker.example.com
CLAUDE_PEERS_BROKER_AUTH=Bearer <hub-token>
# Unique, human-readable name for THIS machine's peer
CLAUDE_PEERS_NAME=Laptop
```

### 3. Wire up message reception (gateway machines)

Two settings decide whether the receiving agent can actually **read and reply** to peer messages:

**a) Home channel** — incoming peer messages are injected into the gateway's home-channel session. Set the home channel for whichever platform your gateway uses, in `~/.hermes/.env`:

```bash
TELEGRAM_HOME_CHANNEL=<chat-id>     # if the gateway runs Telegram
# or
SLACK_HOME_CHANNEL=<dm-channel-id>  # if the gateway runs Slack
```

**b) Toolset** — the receiving agent needs the `claude-peers` tools to reply. In `~/.hermes/config.yaml`, add `claude-peers` to the toolset list of that platform:

```yaml
platform_toolsets:
  slack:            # or telegram, discord, ...
    - claude-peers
    # ...existing entries...
```

Without (b), the agent will receive messages but silently fail to answer — it has no send tool in that session.

### 4. Restart and verify

```bash
hermes gateway restart          # or: systemctl --user restart hermes-gateway
```

From any agent, list peers and send a round-trip test:

```text
claude_peers_list_peers()
claude_peers_send_message(to_id="CloudVM", message="ping — please ACK")
```

The remote agent should receive the message in its home-channel session and reply within its next poll cycle (a few seconds).

### 5. OpenBot machines (optional)

An OpenBot machine runs the standalone `openbot_bridge.py` daemon instead of the Hermes plugin (Python 3 only, no dependencies, plus `node` for `openbot-cli`). Run **one daemon per machine** — it registers as an OpenBot *host*, reports the machine's agents from `openbot-cli list --json`, and later runs tasks for the agent peers you register from the dashboard.

```bash
git clone https://github.com/insightflo/peers-bridge.git "$HOME/Projects/peers-bridge"
mkdir -p ~/.config/peers-bridge
cat > ~/.config/peers-bridge/openbot-mac.env <<EOF
CLAUDE_PEERS_BROKER_URL=https://your-broker.example.com
CLAUDE_PEERS_BROKER_AUTH=<hub-token>
CLAUDE_PEERS_NAME=OpenBot-Mac
OPENBOT_NODE_BIN=$(command -v node)
EOF
chmod 600 ~/.config/peers-bridge/openbot-mac.env
python3 ~/Projects/peers-bridge/openbot_bridge.py
```

The daemon reads `~/.config/peers-bridge/openbot-mac.env`, then `/etc/openbot-peers-bridge.env`, or the file named by `PEERS_BRIDGE_ENV_FILE` (first match wins; existing env vars are kept).

| Variable | Purpose |
|----------|---------|
| `CLAUDE_PEERS_BROKER_URL` | Hub URL (default `https://peers.insightflo.si`) |
| `CLAUDE_PEERS_BROKER_AUTH` | Hub token — the raw value is sent as the `Authorization` header, so include `Bearer ` |
| `CLAUDE_PEERS_NAME` | Required. Host alias in **host mode**; the peer alias in legacy single-agent mode |
| `OPENBOT_AGENT` | Optional. Unset → **host mode**. Set to an agent id → legacy single-agent daemon (all messages go to that agent) |
| `OPENBOT_CLI_PATH` | Optional. `openbot-cli.mjs` path; auto-detected from the standard OpenBot.app locations |
| `OPENBOT_NODE_BIN` | Optional. Node binary (default `node`). Use an absolute path under launchd/systemd, which have no shell `PATH` |
| `OPENBOT_AGENTS_REFRESH_SECONDS` | Optional. How often the host re-runs `openbot-cli list --json`. Default `300` |
| `PEERS_BRIDGE_ATTACHMENT_DIR`, `PEERS_BRIDGE_ATTACHMENT_KEEP_HOURS` | Received attachment cache — see [Attachments](#attachments-files--images) |

To run it as a service on macOS:

```bash
mkdir -p ~/Library/LaunchAgents ~/Library/Logs
cat > ~/Library/LaunchAgents/com.insightflo.peers-openbot-bridge.plist <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
<key>Label</key><string>com.insightflo.peers-openbot-bridge</string>
<key>ProgramArguments</key>
<array><string>/usr/bin/env</string><string>python3</string><string>$HOME/Projects/peers-bridge/openbot_bridge.py</string></array>
<key>RunAtLoad</key><true/>
<key>KeepAlive</key><true/>
<key>StandardOutPath</key><string>$HOME/Library/Logs/openbot-peers-bridge.out.log</string>
<key>StandardErrorPath</key><string>$HOME/Library/Logs/openbot-peers-bridge.err.log</string>
</dict>
</plist>
EOF
launchctl load ~/Library/LaunchAgents/com.insightflo.peers-openbot-bridge.plist
tail -n 50 -f ~/Library/Logs/openbot-peers-bridge.err.log
```

After editing the env file, reload with `launchctl unload <plist> && launchctl load <plist>`. (The dashboard's **Install bridge on a machine** panel shows these same steps with the broker URL filled in, plus a systemd user unit for Linux.)

Verify: within ~15 s the machine shows up in the dashboard's Peer sessions as an online host with its agent count. Only then can its agents be registered (next section).

### 6. Register OpenBot agents from the dashboard

Open `https://your-broker.example.com/dashboard` → **+ Register Peer** → Kind **OpenBot agent**. Pick the host, check the agents to expose, and register; each checked agent becomes one `openbot` peer (role `openbot`) that other peers can address by alias.

Preconditions enforced by the broker (HTTP 400, shown verbatim in the dashboard):

- The host must be **online** — `host is offline; install and start the bridge on that machine first`
- The agent must be in the host's last reported list — `agent not found on host`
- The host must be an OpenBot host — `host not found` / `host is not an OpenBot host`

Alias rule: `<prefix>-<agent name>`, where the prefix defaults to the host alias. Broker aliases accept ASCII letters, digits, spaces and `. _ @ -` (≤64 chars), so an agent name with non-ASCII characters (e.g. Korean) falls back to `<prefix>-<agent id>`. Change the prefix before registering if you want a different scheme. (The alias can be edited later with **Peer settings**.)

## Usage

Agent tools provided by the plugin:

| Tool | Purpose |
|------|---------|
| `claude_peers_list_peers` | List live peers (alias, id, summary, last-seen) |
| `claude_peers_send_message` | Send a message to a peer by alias or id |
| `claude_peers_check_messages` | Manually poll for unread messages |
| `claude_peers_set_alias` | Rename this peer |
| `claude_peers_set_summary` | Update this peer's "what I'm working on" summary |
| `claude_peers_bridge_status` | Show registration state, broker URL, polling status |

Aliases are 1–64 ASCII characters (letters, digits, spaces, `.`, `_`, `@`, `-`), unique case-insensitively across the hub. Incoming messages carry both `from_alias` and `from_id`.

### OpenBot agents

OpenBot agents don't have the tool table above. Their daemon formats the incoming message as a prompt and runs `openbot-cli run <agent> -`; the prompt ends with a reply hint, `Reply command: peers-send "<from_alias>" "<your reply>" --as "<child alias>"`. The agent runs that command **later, in its own process**, so the daemon's environment is not inherited: the broker URL/token come from the env file (`~/.config/peers-bridge/openbot-mac.env`) and the sender identity comes from `--as` (the child alias in host mode). `CLAUDE_PEERS_BROKER_AUTH` may be `Bearer <token>` or the bare token. The same script doubles as the CLI:

```bash
python3 ~/Projects/peers-bridge/openbot_bridge.py list                          # all peers on the hub (peers-list also works)
python3 ~/Projects/peers-bridge/openbot_bridge.py send <alias> "text"            # send from CLAUDE_PEERS_NAME
python3 ~/Projects/peers-bridge/openbot_bridge.py peers-send <alias> "text" --as "OpenBot-Mac-비서"  # explicit identity
```

Prompts name the command `peers-send`, and the script accepts `peers-send` / `peers-list` as subcommand names. To let agents run `peers-send` as-is, put a wrapper on PATH (e.g. `exec python3 <repo>/openbot_bridge.py peers-send "$@"`). Every subcommand reads the same env file as the daemon.

## Attachments (files & images)

Messages can carry up to **5 files, 20 MB each** (broker defaults; see env vars below). The flow is upload-then-reference: the sender `POST`s each file to `/attachments`, then sends the message with the returned ids. The receiver's bridge downloads them to `~/.peers-bridge/attachments/<message id>/<name>` and appends the **absolute paths** to the prompt it hands the agent:

```
Attachments:
- /Users/me/.peers-bridge/attachments/1842/report.pdf (report.pdf, application/pdf, 48213 bytes)
```

**Delivery passes a path, not the image.** Whether the agent can *view* an image depends on that agent's own tools and model — Hermes or OpenBot agents that can read local files can open it; a text-only agent cannot.

Sending:

- **Hermes tool** — `claude_peers_send_message(to_id, message, file_paths=["/path/to/file", ...])`. Missing/oversized/too-many files, or a rejected upload, return `{"ok": false, "sent": false, "error": ...}` without sending the message.
- **Hermes CLI** — `hermes claude-peers send <to> "<message>" --file <path> [--file <path> ...]` (`-f` works too).
- **OpenBot side** — `peers-send <alias> "<text>" --file <path> [--file <path> ...]` (also `-f <path>` / `--file=<path>`). Any failed upload aborts with exit 1 before the message is sent.
- **Dashboard** — the Send tab and the per-peer Quick send both have an **Attachments** input (multiple files; images render as thumbnails in the message list).

Broker environment (hub): `CLAUDE_PEERS_ATTACHMENT_MAX_BYTES` (default 20 MB = `20971520`), `CLAUDE_PEERS_ATTACHMENT_DIR` (default `<db dir>/attachments`), `CLAUDE_PEERS_ATTACHMENT_RETENTION_HOURS` (default 168), `CLAUDE_PEERS_ATTACHMENT_GC_SECONDS` (default 300). Accepted MIME types: `image/*`, `text/*`, `audio/*`, `video/*`, and `application/pdf|json|zip|octet-stream|msword|openxmlformats…`; anything else is rejected with `File type <mime> is not allowed`.

Retention on the hub: uploads referenced by no message are deleted after 24 h; files referenced by a message are kept until every referencing message is older than the retention window (7 days). Receivers keep their local copies for `PEERS_BRIDGE_ATTACHMENT_KEEP_HOURS` (default 168; `0` deletes them after every poll) under `PEERS_BRIDGE_ATTACHMENT_DIR` — set these on both the Hermes and OpenBot sides.

## Architecture notes

| Process | Broker target |
|---------|--------------|
| Gateway agent (`_HERMES_GATEWAY=1`) | Remote hub (`CLAUDE_PEERS_BROKER_URL`) |
| CLI / TUI agent (`HERMES_INTERACTIVE=1`) | Local broker (`127.0.0.1:7899`) |
| Subagent (kanban / delegate) | Registration skipped |
| OpenBot host daemon (`openbot_bridge.py`, host mode) | Remote hub — registers `kind: "openbot-host"`, polls on behalf of its children |

**Peer kinds** on the hub: `peer` (Hermes bridges and dashboard address-book entries), `openbot-host` (one per OpenBot machine; a managed entry that survives going stale), and `openbot` (an agent registered under a host; `host_id` + `agent_id`). Children never go stale on their own — their status follows the host's heartbeat — and the host's heartbeat response carries the list of children (`served_peers`) so the daemon learns new registrations without a restart. Messages to a child wake the host's SSE connection; the host polls `to_id IN (host, children…)`, routes by `to_id`, and runs `openbot-cli run <agent_id> -` with the child's alias in the prompt. Deleting a host deletes its children and their undelivered messages.

**Attachments**: uploads live as `<ATTACHMENT_DIR>/<id>` with a row in an `attachments` table (`id`, `name`, `size`, `mime`, `sha256`, `uploaded_by`); `messages.attachments` stores the JSON array of references, which the broker resolves to metadata on every projection (`poll-messages`, `/api/messages`, snapshot, SSE `message`). Uploads use a small built-in multipart parser because Bun's `formData()` infers `File.type` from the filename and would make the MIME allowlist unenforceable.

- **Broker resolved per loop iteration, not at import** — plugin modules import before gateway env vars are set; re-resolving each cycle lets the gateway pick up the remote hub automatically.
- **Gateway injection with fallback** — received messages are delivered via the gateway wake mechanism; on older Hermes builds without `gateway.wake`, the bridge falls back to injecting a synthetic message event. Either way the message becomes a real agent turn.
- **Hub-aware delivery marking** — the remote hub marks messages delivered at poll time and has no `/mark-message-delivered` endpoint; the bridge treats that 404 as already-delivered instead of re-queuing (prevents infinite re-injection).
- **Custom User-Agent** — Cloudflare blocks Python's default urllib UA; all requests send `User-Agent: hermes-peers-bridge/1.0`.
- **Liveness by heartbeat** — the hub uses `last_seen` age instead of host-local PID checks; dead registrations get `reregister: true` so bridges recover on their own.

## Troubleshooting

| Symptom | Likely cause / fix |
|---------|-------------------|
| Send reports `ok: true` but no reply ever comes | Receiving machine runs an old `bridge.py` (no injection fallback) → update the plugin there and restart its gateway |
| Peer receives messages but never replies | `claude-peers` missing from that platform's `platform_toolsets` on the receiving machine |
| Messages not injected at all | Missing `*_HOME_CHANNEL` env var on the receiving gateway |
| `Peer X not found` when sending from CLI/TUI | CLI sessions talk to the **local** broker; remote peers are only visible to gateway sessions. Route via the gateway peer, or send from a gateway session |
| Same message injected repeatedly | Old `bridge.py` without 404-tolerant delivery marking → update the plugin |
| Broker rejects requests behind Cloudflare | Ensure plugin version sends the custom User-Agent header |
| Registered OpenBot agent shows **offline** | Child status follows its host: the host daemon isn't running (or lost the hub). Check `openbot_bridge.py` / the launchd unit on that machine; the agent returns online when the host heartbeats again |
| Message to a newly registered OpenBot agent arrives late | Should be immediate — the host re-reads its child list as soon as a poll sees an unknown `to_id`. If it waits for the next heartbeat (~15 s) or never arrives, update `openbot_bridge.py` on that machine |
| Dashboard rejects an OpenBot agent registration | `host is offline…` → start the host daemon first; `agent not found on host` → the agent isn't in the last `openbot-cli list` report (wait for the next refresh, `OPENBOT_AGENTS_REFRESH_SECONDS`, or restart the daemon) |
| Upload rejected (`File type … is not allowed` / `File exceeds … the byte limit`) | MIME outside the allowlist or over `CLAUDE_PEERS_ATTACHMENT_MAX_BYTES` (20 MB default). Compress or convert the file |
| `host not found` / `host is not an OpenBot host` when registering | The `host_id` doesn't point at a running OpenBot host — register the daemon first, don't point at a Hermes peer |

## Real-Time SSE Push & Event-Driven Wake-up

The bridge defaults to SSE push (`CLAUDE_PEERS_BRIDGE_TRANSPORT=sse`) instead of 1-second polling:

- **Zero-polling idle**: `bridge.py` connects to `GET /events?id=<peer_id>` with `Authorization: Bearer ...` and receives lightweight `event: wake` hints on message arrival, fetching bodies via `/poll-messages` (`mark_delivered: false`) and acknowledging via `POST /mark-message-delivered`. A slow safety poll (`CLAUDE_PEERS_BRIDGE_SAFETY_POLL_SEC=30`) guards against silent tunnel drops.
- **Event-driven wake-up**: When `ctx.inject_message()` returns `False`, the bridge tries gateway home-channel delivery (`_inject_via_gateway`), then optional `ctx.wake_session()`, `ctx.notify()`, `CLAUDE_PEERS_WAKE_CMD` (`shlex.split`, `shell=False`), or `CLAUDE_PEERS_WAKE_WEBHOOK` (minimal metadata only) with debounce (`CLAUDE_PEERS_WAKE_DEBOUNCE_SEC=0.25`).
- **Force-delete revocation**: Deleting an online peer with `DELETE /api/peers/:id?force=1` sends `event: revoked` and returns `410 Gone` so the bridge halts cleanly instead of resurrecting.

## Peer Registry CRUD & Live Dashboard (`/dashboard`)

Copy both `broker.remote-hub.ts` (as `broker.ts`) and `dashboard.html` to your broker directory.

- **Live Dashboard**: Open `http://127.0.0.1:7899/dashboard` (or `https://your-broker.example.com/dashboard`). When `CLAUDE_PEERS_HUB_TOKEN` is set, the UI prompts for the Bearer token and streams live updates over `GET /api/stream` + `GET /api/snapshot`.
- **Peer CRUD API**:
  - `GET /api/peers` — list all online and offline managed peers (hosts include their reported `agents`)
  - `POST /api/peers` — pre-register a managed peer (`alias`, `role`, `summary`, `cwd`), or an OpenBot agent (`kind: "openbot"`, `host_id`, `agent_id`, `alias`)
  - `PATCH /api/peers/:id` — update alias, summary, role, cwd, or managed flag
  - `DELETE /api/peers/:id` — delete offline peer (or `?force=1` for online peer); deleting a host also deletes its agent peers
  - `POST /api/messages` — send a system message directly from the dashboard to any peer (`attachments: [id, …]` optional)
  - `POST /api/attachments` — upload a file (multipart, field `file`) → `201 {id, name, size, mime, sha256}`
- **OpenBot registration form**: Kind **OpenBot agent** lists the selected online host's agents (one peer per checked agent; *Select all* available). Offline hosts are disabled in the list; with no host registered the form points to the install panel.
- **Install bridge on a machine** panel: copyable step-by-step for OpenBot machines (clone, env file, launchd plist with `launchctl load`, verify) plus a Linux systemd user unit; the broker URL is filled in from the page origin. Hermes machines are directed to §2–§4 of this README.

## License

MIT (plugin code). The broker builds on [claude-peers-mcp](https://github.com/louislva/claude-peers-mcp) — see that repository for its license.
