#!/usr/bin/env python3
"""Standalone zero-dependency SSE bridge daemon connecting OpenBot to peers-bridge."""

from __future__ import annotations

import json
import mimetypes
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any

USER_AGENT = "peers-bridge/1.0"
DEFAULT_ATTACHMENT_DIR = Path.home() / ".peers-bridge" / "attachments"
DEFAULT_ATTACHMENT_KEEP_HOURS = 168.0
ATTACHMENT_DOWNLOAD_TIMEOUT_SECONDS = 30.0
ATTACHMENT_UPLOAD_TIMEOUT_SECONDS = 60.0
MAX_ATTACHMENT_NAME_LENGTH = 128
BACKOFF_SECONDS = (1.0, 2.0, 5.0, 15.0)
DEFAULT_AGENTS_REFRESH_SECONDS = 300.0
LIST_AGENTS_TIMEOUT_SECONDS = 15.0
MAX_HOST_AGENTS = 200
MAX_AGENT_FIELD_LENGTH = 128


def sanitize_attachment_name(raw: str) -> str:
    """Reduce a broker-supplied attachment name to a safe file basename.

    Path components (both separators) are dropped so `../../x.png` cannot escape
    the per-message directory; control characters are removed and the length is
    capped. Never returns an empty string.
    """
    name = str(raw or "").replace("\\", "/").rsplit("/", 1)[-1]
    name = "".join(ch for ch in name if ch.isprintable() and not ch.isspace() or ch == " ")
    name = name.strip().strip(".")[:MAX_ATTACHMENT_NAME_LENGTH]
    return name or "attachment"


def _format_task(
    message: dict[str, Any],
    self_alias: str | None = None,
    local_attachments: list[dict[str, Any]] | None = None,
) -> str:
    """Build the prompt handed to the OpenBot agent on stdin.

    `local_attachments` lists already-downloaded files (`path`, `name`, `mime`,
    `size`) or failed downloads (`name`, `error`). Without it the output is
    byte-identical to the pre-attachment prompt.
    """
    from_id = str(message.get("from_id") or "unknown")
    from_alias = str(message.get("from_alias") or from_id)
    from_kind = str(message.get("from_kind") or "peer")
    sent_at = str(message.get("sent_at") or "")
    text = str(message.get("text") or "")
    has_attachments = bool(local_attachments)
    # The agent replies through `peers-send`, which resolves its own peer id from
    # the CLAUDE_PEERS_NAME alias. Host mode must name the child peer here so the
    # reply is attributed to the agent, not to the host daemon.
    if from_kind == "peer":
        reply_hint = f'Reply command: peers-send "{from_alias}" "<your reply>"'
        if has_attachments:
            reply_hint += " [--file <path>]..."
        if self_alias:
            # The agent runs this later in its own process, without the daemon's
            # env, so the sending identity must be explicit on the command line.
            reply_hint += f' --as "{self_alias}"'
    else:
        reply_hint = "This message was sent from the dashboard/system."
    header = f"[peers-bridge] Incoming peer message\n"
    if self_alias:
        header += f"self_alias: {self_alias}\n"
    prompt = (
        header
        + f"from_alias: {from_alias}\n"
        f"from_id: {from_id}\n"
        f"from_kind: {from_kind}\n"
        f"sent_at: {sent_at}\n\n"
        f"{text}\n\n"
        f"{reply_hint}\n"
    )
    if has_attachments:
        prompt += "\nAttachments:\n"
        for item in local_attachments:
            if item.get("error"):
                prompt += f"- {item.get('name')}: download failed ({item['error']})\n"
            else:
                prompt += f"- {item['path']} ({item.get('name')}, {item.get('mime')}, {item.get('size')} bytes)\n"
    return prompt


def _load_env_defaults() -> None:
    candidates = []
    explicit = os.environ.get("PEERS_BRIDGE_ENV_FILE", "").strip()
    if explicit:
        candidates.append(Path(explicit))
    candidates.extend(
        [
            Path.home() / ".config" / "peers-bridge" / "openbot-mac.env",
            Path("/etc/openbot-peers-bridge.env"),
        ]
    )
    for path in candidates:
        if not path.is_file():
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                val = val[1:-1]
            os.environ.setdefault(key, val)
        break


def _default_cli_path() -> str:
    candidates = [
        "/Applications/OpenBot.app/Contents/Resources/cli/openbot-cli.mjs",
        "/opt/OpenBot/app/resources/cli/openbot-cli.mjs",
    ]
    for path in candidates:
        if Path(path).is_file():
            return path
    return candidates[0]


def list_openbot_agents(
    node_bin: str,
    cli_path: str,
    timeout: float = LIST_AGENTS_TIMEOUT_SECONDS,
) -> tuple[list[dict[str, str]], str | None]:
    """Return `({id, name}` agents reported by `openbot-cli list --json` and an error message.

    Never raises: an unreachable OpenBot, non-zero exit or malformed output yields
    `([], error)` so the host daemon can keep running and report an empty list.
    Entries are trimmed, de-duplicated by id and capped to the broker's limits.
    """
    argv = [node_bin, cli_path, "list", "--json"]
    try:
        proc = subprocess.run(
            argv,
            text=True,
            shell=False,
            capture_output=True,
            timeout=timeout,
        )
    except Exception as exc:
        return [], f"openbot-cli list failed: {exc}"
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        return [], f"openbot-cli list exit {proc.returncode}: {err[:200]}"
    try:
        data = json.loads(proc.stdout.decode("utf-8", errors="replace") if isinstance(proc.stdout, bytes) else proc.stdout)
        raw_agents = data.get("agents") if isinstance(data, dict) else None
    except ValueError as exc:
        return [], f"openbot-cli list returned invalid JSON: {exc}"
    agents: list[dict[str, str]] = []
    seen: set[str] = set()
    for entry in raw_agents if isinstance(raw_agents, list) else []:
        if not isinstance(entry, dict):
            continue
        agent_id = str(entry.get("id") or "").strip()
        if not agent_id or agent_id in seen:
            continue
        name = str(entry.get("name") or "").strip()[:MAX_AGENT_FIELD_LENGTH] or agent_id
        seen.add(agent_id)
        agents.append({"id": agent_id[:MAX_AGENT_FIELD_LENGTH], "name": name})
        if len(agents) >= MAX_HOST_AGENTS:
            break
    return agents, None


class OpenBotPeersDaemon:
    """Maintain SSE connection to peers-bridge hub and dispatch messages to OpenBot CLI."""

    def __init__(
        self,
        *,
        broker_url: str,
        broker_auth: str | None,
        alias: str,
        summary: str,
        agent: str | None,
        cli_path: str,
        node_bin: str = "node",
        heartbeat_interval: float = 15.0,
        safety_poll_interval: float = 30.0,
        run_timeout: float = 30.0,
        agents_refresh_seconds: float = DEFAULT_AGENTS_REFRESH_SECONDS,
        attachment_dir: Path | str | None = None,
        attachment_keep_hours: float | None = None,
    ) -> None:
        """`agent` set → legacy single-agent daemon; `agent` None → host mode.

        Host mode registers as kind "openbot-host", reports the machine's OpenBot
        agents, and dispatches messages to the child peers the dashboard attached
        to this host (`served`: child peer id → agent id).
        """
        self.broker_url = broker_url.rstrip("/")
        self.broker_auth = broker_auth
        self.alias = alias
        self.summary = summary
        self.agent = (agent or "").strip() or None
        self.cli_path = cli_path
        self.node_bin = node_bin
        self.heartbeat_interval = heartbeat_interval
        self.safety_poll_interval = safety_poll_interval
        self.run_timeout = run_timeout
        self.agents_refresh_seconds = agents_refresh_seconds
        # Local cache of received attachments, one directory per message id.
        self.attachment_dir = Path(
            attachment_dir or os.environ.get("PEERS_BRIDGE_ATTACHMENT_DIR", "").strip() or DEFAULT_ATTACHMENT_DIR
        ).expanduser()
        if attachment_keep_hours is None:
            try:
                attachment_keep_hours = float(os.environ.get("PEERS_BRIDGE_ATTACHMENT_KEEP_HOURS", "").strip() or DEFAULT_ATTACHMENT_KEEP_HOURS)
            except ValueError:
                attachment_keep_hours = DEFAULT_ATTACHMENT_KEEP_HOURS
        self.attachment_keep_hours = max(0.0, attachment_keep_hours)

        self.cwd = os.getcwd()
        self.peer_id: str | None = None
        self.sse_connected = False
        self.revoked = False
        self.last_error: str | None = None

        # Host-mode state (unused in legacy mode).
        self.agents: list[dict[str, str]] = []
        self.served: dict[str, str] = {}  # child peer id -> agent id
        self.served_alias: dict[str, str] = {}  # child peer id -> alias
        self._agents_dirty = True  # include `agents` in the next register/heartbeat
        self._last_agents_refresh = 0.0
        self._unknown_target_warned: set[Any] = set()

        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._delivered_ids: set[int] = set()
        self._sse_thread: threading.Thread | None = None

    def _headers(self, *, json_body: bool = False, accept_sse: bool = False) -> dict[str, str]:
        headers = {"User-Agent": USER_AGENT}
        if json_body:
            headers["Content-Type"] = "application/json"
        if accept_sse:
            headers["Accept"] = "text/event-stream"
        if self.broker_auth:
            headers["Authorization"] = self.broker_auth
        return headers

    def _post_json(self, path: str, body: dict[str, Any], timeout: float = 8.0) -> dict[str, Any]:
        req = urllib.request.Request(
            f"{self.broker_url}{path}",
            data=json.dumps(body).encode("utf-8"),
            headers=self._headers(json_body=True),
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if path == "/heartbeat" and exc.code == 410:
                self._revoke()
                raise RuntimeError("peer revoked by broker") from exc
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"broker {path} failed: HTTP {exc.code} {detail}") from exc

    def _revoke(self) -> None:
        with self._lock:
            self.revoked = True
            self.peer_id = None
            self.sse_connected = False
            self._delivered_ids.clear()
            self.last_error = "peer revoked by broker"
        self._stop_event.set()
        self._wake_event.set()

    @property
    def host_mode(self) -> bool:
        return self.agent is None

    def _reset_session(self) -> None:
        with self._lock:
            self.peer_id = None
            self._delivered_ids.clear()
            self.served = {}
            self.served_alias = {}
            self._agents_dirty = True

    def refresh_agents(self, *, force: bool = False) -> None:
        """Host mode: re-run `openbot-cli list --json` when the refresh interval elapsed.

        Failures keep the previous list (the broker treats an omitted `agents` as
        "unchanged") and only record `last_error`, so a transient OpenBot outage
        never unregisters the agents.
        """
        if not self.host_mode:
            return
        now = time.monotonic()
        if not force and self._last_agents_refresh and now - self._last_agents_refresh < self.agents_refresh_seconds:
            return
        self._last_agents_refresh = now
        agents, error = list_openbot_agents(self.node_bin, self.cli_path)
        if error:
            self.last_error = error
            print(f"[openbot-bridge] {error}", file=sys.stderr, flush=True)
            return
        self.agents = agents
        self._agents_dirty = True

    def register(self) -> str:
        with self._lock:
            if self.revoked:
                raise RuntimeError("peer revoked by broker")
            if self.peer_id:
                return self.peer_id
        body: dict[str, Any] = {
            "pid": os.getpid(),
            "parent_pid": os.getppid(),
            "logical_name": self.alias,
            "cwd": self.cwd,
            "git_root": None,
            "tty": None,
            "summary": self.summary,
        }
        if self.host_mode:
            self.refresh_agents(force=True)
            body["kind"] = "openbot-host"
            body["agents"] = list(self.agents)
        payload = self._post_json("/register", body)
        peer_id = payload.get("id")
        if not isinstance(peer_id, str) or not peer_id:
            raise RuntimeError(f"register failed: {payload}")
        with self._lock:
            self.peer_id = peer_id
            self._delivered_ids.clear()
            if self.host_mode:
                self._agents_dirty = False
        if self.host_mode:
            print(
                f"[openbot-bridge] registered host alias={self.alias} id={peer_id} agents={len(self.agents)}",
                flush=True,
            )
        else:
            print(f"[openbot-bridge] registered alias={self.alias} id={peer_id}", flush=True)
        return peer_id

    def heartbeat(self) -> None:
        with self._lock:
            peer_id = self.peer_id
        if not peer_id:
            return
        body: dict[str, Any] = {"id": peer_id}
        if self.host_mode:
            self.refresh_agents()
            with self._lock:
                if self._agents_dirty:
                    body["agents"] = list(self.agents)
                    self._agents_dirty = False
        self._send_heartbeat(body)

    def refresh_served(self) -> None:
        """Heartbeat without the agent-list refresh (no CLI subprocess).

        Used when a poll reveals a child id that is not in `served` yet, so a
        freshly registered agent is served immediately instead of after the
        next scheduled heartbeat."""
        with self._lock:
            peer_id = self.peer_id
        if peer_id:
            self._send_heartbeat({"id": peer_id})

    def _send_heartbeat(self, body: dict[str, Any]) -> None:
        payload = self._post_json("/heartbeat", body)
        if payload.get("reregister"):
            self._reset_session()
            self.register()
            return
        served = payload.get("served_peers")
        if self.host_mode and isinstance(served, list):
            self._set_served(served)

    def _set_served(self, served_peers: list[Any]) -> None:
        """Replace `served`/`served_alias` wholesale from a heartbeat response."""
        served: dict[str, str] = {}
        served_alias: dict[str, str] = {}
        for entry in served_peers:
            if not isinstance(entry, dict):
                continue
            child_id = str(entry.get("id") or "").strip()
            agent_id = str(entry.get("agent_id") or "").strip()
            if not child_id or not agent_id:
                continue
            served[child_id] = agent_id
            served_alias[child_id] = str(entry.get("alias") or "").strip() or self.alias
        with self._lock:
            self.served = served
            self.served_alias = served_alias
            self._unknown_target_warned.clear()

    def unregister(self) -> None:
        with self._lock:
            peer_id = self.peer_id
            self.peer_id = None
            self.sse_connected = False
        if not peer_id:
            return
        try:
            self._post_json("/unregister", {"id": peer_id}, timeout=3.0)
        except Exception:
            pass

    def poll_and_deliver(self) -> int:
        with self._lock:
            peer_id = self.peer_id
        if not peer_id:
            return 0
        payload = self._post_json(
            "/poll-messages",
            {"id": peer_id, "mark_delivered": False},
        )
        if payload.get("reregister"):
            self._reset_session()
            self.register()
            return 0
        messages = payload.get("messages") or []
        # A child registered from the dashboard is only learned through the
        # heartbeat response; refresh immediately instead of waiting up to the
        # next scheduled heartbeat (15 s) when a message targets an unknown peer.
        if self.host_mode and self._has_unknown_target(messages):
            self.refresh_served()
        delivered = 0
        for message in messages:
            msg_id = message.get("id")
            if isinstance(msg_id, int) and msg_id in self._delivered_ids:
                continue
            if not self._resolve_target(message):
                continue
            local_attachments = self._download_attachments(message)
            if self.dispatch_to_openbot(message, local_attachments=local_attachments):
                if isinstance(msg_id, int):
                    self._post_json("/mark-message-delivered", {"id": msg_id})
                    self._delivered_ids.add(msg_id)
                delivered += 1
        try:
            cleanup_attachment_dirs(self.attachment_dir, self.attachment_keep_hours)
        except Exception as exc:  # retention sweep must never break delivery
            print(f"[openbot-bridge] attachment cleanup failed: {exc}", file=sys.stderr, flush=True)
        return delivered

    def _download_attachments(self, message: dict[str, Any]) -> list[dict[str, Any]]:
        """Fetch the message's attachments into `<attachment_dir>/<msg_id>/<name>`.

        Returns one entry per attachment: `{path, name, mime, size}` on success
        (or when an identical-size file is already cached) or `{name, error}` when
        the download failed. Never raises; failures are logged and recorded in
        `last_error` while the message still gets delivered.
        """
        raw = message.get("attachments")
        if not isinstance(raw, list) or not raw:
            return []
        key = str(message.get("id") or message.get("to_id") or "unknown")
        try:
            msg_dir = self.attachment_dir / key
            ensure_private_dir(msg_dir)
        except OSError as exc:
            self.last_error = f"attachment dir {msg_dir} unavailable: {exc}"
            print(f"[openbot-bridge] {self.last_error}", file=sys.stderr, flush=True)
            return [
                {"name": sanitize_attachment_name(att.get("name") or "attachment"), "error": str(exc)}
                for att in raw if isinstance(att, dict)
            ]
        results: list[dict[str, Any]] = []
        for att in raw:
            if not isinstance(att, dict):
                continue
            name = sanitize_attachment_name(att.get("name") or "attachment")
            size = att.get("size")
            dest = msg_dir / name
            if dest.is_file() and (not isinstance(size, int) or dest.stat().st_size == size):
                results.append({"path": str(dest.resolve()), "name": name, "mime": att.get("mime"), "size": dest.stat().st_size})
                continue
            try:
                written = download_attachment(self.broker_url, self.broker_auth, str(att.get("id") or ""), dest)
            except Exception as exc:
                reason = _http_error_detail(exc) if isinstance(exc, urllib.error.HTTPError) else str(exc) or exc.__class__.__name__
                self.last_error = f"attachment download failed ({name}): {reason}"
                print(f"[openbot-bridge] {self.last_error}", file=sys.stderr, flush=True)
                results.append({"name": name, "error": reason})
                continue
            results.append({"path": str(dest.resolve()), "name": name, "mime": att.get("mime"), "size": written})
        return results

    def _has_unknown_target(self, messages: list[dict[str, Any]]) -> bool:
        with self._lock:
            own_id = self.peer_id
            return any(
                str(message.get("to_id") or "") not in (own_id, "")
                and str(message.get("to_id") or "") not in self.served
                for message in messages
            )

    def _resolve_target(self, message: dict[str, Any]) -> bool:
        """Decide whether `message` can be dispatched now.

        Legacy mode dispatches everything to `self.agent`. Host mode routes by
        `to_id`: own id → drop + mark delivered; a served child → its agent; an
        unknown id → leave undelivered (poll_and_deliver has already refreshed
        the served list once, so it resolves on a later poll).
        """
        if not self.host_mode:
            return True
        to_id = str(message.get("to_id") or "")
        msg_id = message.get("id")
        with self._lock:
            own_id = self.peer_id
            known = to_id in self.served
        if to_id == own_id:
            print(
                f"[openbot-bridge] warning: message addressed to host, not an agent; dropping msg={msg_id}",
                file=sys.stderr,
                flush=True,
            )
            if isinstance(msg_id, int):
                self._post_json("/mark-message-delivered", {"id": msg_id})
                self._delivered_ids.add(msg_id)
            return False
        if not known:
            if msg_id not in self._unknown_target_warned:
                self._unknown_target_warned.add(msg_id)
                print(
                    f"[openbot-bridge] message msg={msg_id} targets unknown peer {to_id!r}; waiting for heartbeat",
                    file=sys.stderr,
                    flush=True,
                )
            return False
        return True

    def dispatch_to_openbot(
        self,
        message: dict[str, Any],
        local_attachments: list[dict[str, Any]] | None = None,
    ) -> bool:
        """Run the agent with the formatted prompt; `local_attachments` come from `_download_attachments`."""
        to_id = str(message.get("to_id") or "")
        with self._lock:
            served = self.served.get(to_id)
            reply_alias = self.served_alias.get(to_id)
        if self.host_mode and served:
            agent = served
            reply_alias = reply_alias or self.alias
        else:
            agent = self.agent or ""
            reply_alias = self.alias
        prompt = _format_task(
            message,
            self_alias=reply_alias if self.host_mode else None,
            local_attachments=local_attachments,
        )
        argv = [self.node_bin, self.cli_path, "run", agent, "-"]
        # Explicit identity for the agent's own `peers-send` replies: broker access
        # plus the child peer alias so /list-peers resolves to the child id.
        env = os.environ.copy()
        env["CLAUDE_PEERS_BROKER_URL"] = self.broker_url
        if self.broker_auth:
            env["CLAUDE_PEERS_BROKER_AUTH"] = self.broker_auth
        else:
            env.pop("CLAUDE_PEERS_BROKER_AUTH", None)
        env["CLAUDE_PEERS_NAME"] = reply_alias
        try:
            proc = subprocess.run(
                argv,
                input=prompt,
                text=True,
                shell=False,
                capture_output=True,
                timeout=self.run_timeout,
                env=env,
            )
        except Exception as exc:
            self.last_error = f"openbot-cli execution failed: {exc}"
            print(f"[openbot-bridge] {self.last_error}", file=sys.stderr, flush=True)
            return False
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout or "").strip()
            self.last_error = f"openbot-cli exit {proc.returncode}: {err[:200]}"
            print(f"[openbot-bridge] {self.last_error}", file=sys.stderr, flush=True)
            return False
        print(
            f"[openbot-bridge] delivered msg={message.get('id')} to agent={agent}",
            flush=True,
        )
        return True

    def _sse_loop(self) -> None:
        backoff_idx = 0
        while not self._stop_event.is_set():
            with self._lock:
                peer_id = self.peer_id
            if not peer_id:
                self._stop_event.wait(0.25)
                continue
            url = f"{self.broker_url}/events?id={urllib.parse.quote(peer_id, safe='')}"
            req = urllib.request.Request(url, headers=self._headers(accept_sse=True), method="GET")
            try:
                with urllib.request.urlopen(req, timeout=45.0) as resp:
                    self.sse_connected = True
                    backoff_idx = 0
                    self._wake_event.set()
                    event_name = ""
                    data_lines: list[str] = []
                    while not self._stop_event.is_set():
                        with self._lock:
                            if self.peer_id != peer_id:
                                break
                        raw = resp.readline()
                        if not raw:
                            break
                        line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                        if line == "":
                            data = "\n".join(data_lines)
                            if event_name == "revoked":
                                self._revoke()
                                return
                            if event_name == "wake" or '"messages"' in data:
                                self._wake_event.set()
                            event_name = ""
                            data_lines = []
                        elif line.startswith(":"):
                            continue
                        elif line.startswith("event:"):
                            event_name = line[6:].strip()
                        elif line.startswith("data:"):
                            data_lines.append(line[5:].strip())
            except urllib.error.HTTPError as exc:
                self.sse_connected = False
                if exc.code == 410:
                    self._revoke()
                    return
                if exc.code == 404:
                    self._reset_session()
                    self._wake_event.set()
                self.last_error = f"sse HTTP {exc.code}"
            except Exception as exc:
                self.sse_connected = False
                self.last_error = f"sse error: {exc}"
            finally:
                self.sse_connected = False
            delay = BACKOFF_SECONDS[min(backoff_idx, len(BACKOFF_SECONDS) - 1)]
            backoff_idx += 1
            self._stop_event.wait(delay)

    def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        if self._sse_thread and self._sse_thread.is_alive():
            self._sse_thread.join(timeout=2.0)
        self.unregister()

    def run_forever(self) -> None:
        self._sse_thread = threading.Thread(
            target=self._sse_loop,
            name=f"openbot-sse-{self.alias}",
            daemon=True,
        )
        self._sse_thread.start()
        last_heartbeat = 0.0
        last_poll = 0.0
        try:
            while not self._stop_event.is_set():
                try:
                    self.register()
                    now = time.monotonic()
                    if now - last_heartbeat >= self.heartbeat_interval:
                        self.heartbeat()
                        last_heartbeat = now
                    should_poll = self._wake_event.is_set() or (
                        now - last_poll >= (self.safety_poll_interval if self.sse_connected else 2.0)
                    )
                    if should_poll:
                        self._wake_event.clear()
                        self.poll_and_deliver()
                        last_poll = time.monotonic()
                except Exception as exc:
                    self.last_error = str(exc)
                    print(f"[openbot-bridge] loop error: {exc}", file=sys.stderr, flush=True)
                    if self.revoked:
                        break
                    self._stop_event.wait(2.0)
                    continue
                next_hb = max(0.2, self.heartbeat_interval - (time.monotonic() - last_heartbeat))
                poll_gap = self.safety_poll_interval if self.sse_connected else 2.0
                next_poll = max(0.2, poll_gap - (time.monotonic() - last_poll))
                self._wake_event.wait(min(next_hb, next_poll))
        finally:
            self.stop()


def _api_request(
    broker_url: str,
    broker_auth: str | None,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    timeout: float = 8.0,
) -> dict[str, Any]:
    headers = {"User-Agent": USER_AGENT}
    if broker_auth:
        headers["Authorization"] = broker_auth
    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        f"{broker_url.rstrip('/')}{path}",
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"broker {path} failed: HTTP {exc.code} {detail}") from exc


def _broker_headers(broker_auth: str | None, **extra: str) -> dict[str, str]:
    headers = {"User-Agent": USER_AGENT}
    if broker_auth:
        headers["Authorization"] = broker_auth
    headers.update(extra)
    return headers


def _http_error_detail(exc: urllib.error.HTTPError) -> str:
    try:
        detail = exc.read().decode("utf-8", errors="replace").strip()
    except Exception:  # body already consumed or unreadable
        detail = ""
    return f"HTTP {exc.code} {detail}".strip()


def download_attachment(
    broker_url: str,
    broker_auth: str | None,
    attachment_id: str,
    dest_path: Path,
    timeout: float = ATTACHMENT_DOWNLOAD_TIMEOUT_SECONDS,
) -> int:
    """GET `/attachments/:id` from the broker and write it atomically to `dest_path`.

    Returns the number of bytes written. Raises `RuntimeError` on HTTP errors and
    whatever `urllib` raises on network/timeout failures so callers can mark the
    attachment as failed without crashing the poll loop.
    """
    url = f"{broker_url.rstrip('/')}/attachments/{urllib.parse.quote(str(attachment_id), safe='')}"
    req = urllib.request.Request(url, headers=_broker_headers(broker_auth), method="GET")
    part_path = dest_path.with_name(dest_path.name + ".part")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            with open(part_path, "wb") as fh:
                shutil.copyfileobj(resp, fh)
                written = fh.tell()
        if written == 0:
            raise RuntimeError("broker returned an empty attachment body")
        os.replace(part_path, dest_path)
    except BaseException:
        # Never leave a partial file behind where a later poll could mistake it for a complete one.
        try:
            part_path.unlink()
        except OSError:
            pass
        raise
    return written


def _multipart_part(boundary: str, name: str, value: bytes | str, *, filename: str | None = None, content_type: str | None = None) -> bytes:
    disposition = f'Content-Disposition: form-data; name="{name}"'
    if filename is not None:
        disposition += f'; filename="{str(filename).replace(chr(34), chr(39))}"'
    lines = [f"--{boundary}", disposition]
    if content_type:
        lines.append(f"Content-Type: {content_type}")
    data = value.encode("utf-8") if isinstance(value, str) else value
    return ("\r\n".join(lines) + "\r\n\r\n").encode("utf-8") + data + b"\r\n"


def upload_attachment(
    broker_url: str,
    broker_auth: str | None,
    path: str,
    from_id: str | None = None,
    timeout: float = ATTACHMENT_UPLOAD_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """POST a local file to `/attachments` (hand-built multipart, stdlib only).

    Returns the broker's 201 body `{id, name, size, mime, sha256}`. Raises
    `OSError` when the file cannot be read and `RuntimeError` on HTTP errors.
    """
    src = Path(path)
    data = src.read_bytes()
    if not data:
        raise RuntimeError(f"attachment {src} is empty")
    name = src.name
    # Only declare a Content-Type the broker allowlist certainly accepts; otherwise
    # let the broker infer it from the file extension.
    guessed, _ = mimetypes.guess_type(name)
    if guessed is not None and not re.match(r"^(image/|text/|application/(pdf|json)$)", guessed):
        guessed = None
    boundary = "----peers-bridge-" + uuid.uuid4().hex
    body = _multipart_part(boundary, "file", data, filename=name, content_type=guessed)
    if from_id:
        body += _multipart_part(boundary, "from_id", str(from_id))
    body += f"--{boundary}--\r\n".encode("ascii")
    url = f"{broker_url.rstrip('/')}/attachments"
    req = urllib.request.Request(
        url,
        data=body,
        headers=_broker_headers(broker_auth, **{"Content-Type": f"multipart/form-data; boundary={boundary}"}),
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"broker /attachments failed: {_http_error_detail(exc)}") from exc
    if not isinstance(payload, dict) or not payload.get("id"):
        raise RuntimeError(f"broker /attachments returned no id: {payload}")
    return payload


def ensure_private_dir(path: Path) -> None:
    """Create `path` and any missing parents with mode 0700 (each new level only)."""
    missing: list[Path] = []
    current = path.expanduser()
    while not current.exists():
        missing.append(current)
        if current.parent == current:
            break
        current = current.parent
    for directory in reversed(missing):
        try:
            os.mkdir(directory, 0o700)
        except FileExistsError:
            pass


def cleanup_attachment_dirs(base_dir: Path, keep_hours: float, *, now: float | None = None) -> int:
    """Delete per-message attachment directories older than `keep_hours`.

    Opportunistic and non-fatal: returns the number of directories removed and
    swallows individual removal errors.
    """
    if keep_hours < 0 or not base_dir.is_dir():
        return 0
    cutoff = (time.time() if now is None else now) - keep_hours * 3600
    removed = 0
    try:
        entries = list(base_dir.iterdir())
    except OSError:
        return 0
    for entry in entries:
        try:
            if not entry.is_dir() or entry.is_symlink() or entry.stat().st_mtime > cutoff:
                continue
            shutil.rmtree(entry, ignore_errors=True)
            if not entry.exists():
                removed += 1
        except OSError:
            continue
    return removed


def cli_list(broker_url: str, broker_auth: str | None) -> int:
    peers = _api_request(broker_url, broker_auth, "POST", "/list-peers", {"scope": "machine"})
    if not isinstance(peers, list) or not peers:
        print("No peers registered.")
        return 0
    for peer in peers:
        alias = peer.get("alias") or peer.get("id")
        status = peer.get("status") or "unknown"
        tag = peer.get("role") or "peer"
        summary = peer.get("summary") or ""
        print(f"{alias}\t{status}\t[{tag}]\t{summary}")
    return 0


def cli_send(
    broker_url: str,
    broker_auth: str | None,
    self_alias: str,
    to_id: str,
    message: str,
    files: list[str] | None = None,
    as_alias: str | None = None,
) -> int:
    """Send `message` to `to_id`, optionally uploading `files` as attachments.

    `as_alias` overrides `self_alias` (the sender identity); host mode uses it so
    an agent replies as its own child peer rather than as the host.

    Every file is uploaded first; any failure (unreadable file, rejected upload)
    aborts with exit 1 before the message is sent, so peers never see a message
    that references attachments that do not exist.
    """
    self_alias = (as_alias or "").strip() or self_alias
    from_id = self_alias or "cli"
    if self_alias:
        try:
            peers = _api_request(broker_url, broker_auth, "POST", "/list-peers", {"scope": "machine"})
            if isinstance(peers, list):
                for peer in peers:
                    if (peer.get("alias") or "").lower() == self_alias.lower():
                        from_id = peer["id"]
                        break
        except Exception:
            pass
    attachment_ids: list[str] = []
    for path in files or []:
        try:
            uploaded = upload_attachment(broker_url, broker_auth, path, from_id=str(from_id))
        except (OSError, RuntimeError, ValueError) as exc:
            print(f"Attachment upload failed for {path!r}: {exc}", file=sys.stderr)
            return 1
        attachment_ids.append(str(uploaded["id"]))
        print(f"Uploaded {Path(path).name} ({uploaded.get('size')} bytes) as attachment {uploaded['id']}", file=sys.stderr)
    body: dict[str, Any] = {
        "from_id": from_id,
        "from_pid": os.getpid(),
        "to_id": to_id,
        "text": message,
    }
    if attachment_ids:
        body["attachments"] = attachment_ids
    result = _api_request(broker_url, broker_auth, "POST", "/send-message", body)
    if not result.get("ok"):
        print(json.dumps(result, ensure_ascii=False), file=sys.stderr)
        return 1
    target = result.get("to_alias") or result.get("to_id") or to_id
    print(f"Sent to {target} ({result.get('to_id')})")
    return 0


def _normalize_broker_auth(value: str) -> str | None:
    """Accept a bare hub token or a full header value (`Bearer <token>`)."""
    value = value.strip()
    if not value:
        return None
    if " " in value:
        return value
    return f"Bearer {value}"


def main(argv: list[str] | None = None) -> int:
    _load_env_defaults()
    args = list(sys.argv[1:] if argv is None else argv)
    broker_url = os.environ.get("CLAUDE_PEERS_BROKER_URL", "https://peers.insightflo.si").strip()
    broker_auth = _normalize_broker_auth(os.environ.get("CLAUDE_PEERS_BROKER_AUTH", ""))
    alias = os.environ.get("CLAUDE_PEERS_NAME", "").strip()

    if args and args[0] in ("list", "peers-list"):
        return cli_list(broker_url, broker_auth)
    if args and args[0] in ("send", "peers-send"):
        # Positionals: <to_alias_or_id> then the message words; `--file/-f <path>`
        # (repeatable, also `--file=<path>`) is collected for upload and
        # `--as <alias>` (also `--as=<alias>`) sets the sender identity.
        files: list[str] = []
        positional: list[str] = []
        as_alias: str | None = None
        idx = 1
        while idx < len(args):
            arg = args[idx]
            if arg == "--as":
                if idx + 1 >= len(args):
                    print(f"Usage: peers-send <to_alias_or_id> <message> [--file <path>]... [--as <alias>] ({arg} needs an alias)", file=sys.stderr)
                    return 2
                as_alias = args[idx + 1]
                idx += 2
                continue
            if arg.startswith("--as="):
                as_alias = arg[len("--as="):]
                idx += 1
                continue
            if arg == "--file" or arg == "-f":
                if idx + 1 >= len(args):
                    print(f"Usage: peers-send <to_alias_or_id> <message> [--file <path>]... ({arg} needs a path)", file=sys.stderr)
                    return 2
                files.append(args[idx + 1])
                idx += 2
                continue
            if arg.startswith("--file="):
                files.append(arg[len("--file="):])
                idx += 1
                continue
            positional.append(arg)
            idx += 1
        if len(positional) < 2:
            print("Usage: peers-send <to_alias_or_id> <message> [--file <path>]... [--as <alias>]", file=sys.stderr)
            return 2
        return cli_send(broker_url, broker_auth, alias, positional[0], " ".join(positional[1:]), files or None, as_alias)

    if not alias:
        print(
            "CLAUDE_PEERS_NAME is required (OPENBOT_AGENT is optional: "
            "set it to serve one agent, omit it to run as this machine's OpenBot host)",
            file=sys.stderr,
        )
        return 2
    agent = os.environ.get("OPENBOT_AGENT", "").strip() or None
    summary = os.environ.get("CLAUDE_PEERS_SUMMARY", "").strip()
    if not summary:
        summary = f"OpenBot ({agent})" if agent else "OpenBot host"
    cli_path = os.environ.get("OPENBOT_CLI_PATH", "").strip() or _default_cli_path()
    node_bin = os.environ.get("OPENBOT_NODE_BIN", "node").strip() or "node"
    try:
        agents_refresh_seconds = float(os.environ.get("OPENBOT_AGENTS_REFRESH_SECONDS", "").strip() or DEFAULT_AGENTS_REFRESH_SECONDS)
    except ValueError:
        agents_refresh_seconds = DEFAULT_AGENTS_REFRESH_SECONDS
    agents_refresh_seconds = max(0.0, agents_refresh_seconds)
    daemon = OpenBotPeersDaemon(
        broker_url=broker_url,
        broker_auth=broker_auth,
        alias=alias,
        summary=summary,
        agent=agent,
        cli_path=cli_path,
        node_bin=node_bin,
        agents_refresh_seconds=agents_refresh_seconds,
    )

    def _handle_sig(_signum, _frame):
        daemon._stop_event.set()
        daemon._wake_event.set()

    signal.signal(signal.SIGTERM, _handle_sig)
    signal.signal(signal.SIGINT, _handle_sig)
    daemon.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
