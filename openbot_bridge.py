#!/usr/bin/env python3
"""Standalone zero-dependency SSE bridge daemon connecting OpenBot to peers-bridge."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

USER_AGENT = "peers-bridge/1.0"
BACKOFF_SECONDS = (1.0, 2.0, 5.0, 15.0)


def _format_task(message: dict[str, Any]) -> str:
    from_id = str(message.get("from_id") or "unknown")
    from_alias = str(message.get("from_alias") or from_id)
    from_kind = str(message.get("from_kind") or "peer")
    sent_at = str(message.get("sent_at") or "")
    text = str(message.get("text") or "")
    reply_hint = (
        f'Reply command: peers-send "{from_alias}" "<your reply>"'
        if from_kind == "peer"
        else "This message was sent from the dashboard/system."
    )
    return (
        "[peers-bridge] Incoming peer message\n"
        f"from_alias: {from_alias}\n"
        f"from_id: {from_id}\n"
        f"from_kind: {from_kind}\n"
        f"sent_at: {sent_at}\n\n"
        f"{text}\n\n"
        f"{reply_hint}\n"
    )


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


class OpenBotPeersDaemon:
    """Maintain SSE connection to peers-bridge hub and dispatch messages to OpenBot CLI."""

    def __init__(
        self,
        *,
        broker_url: str,
        broker_auth: str | None,
        alias: str,
        summary: str,
        agent: str,
        cli_path: str,
        node_bin: str = "node",
        heartbeat_interval: float = 15.0,
        safety_poll_interval: float = 30.0,
        run_timeout: float = 30.0,
    ) -> None:
        self.broker_url = broker_url.rstrip("/")
        self.broker_auth = broker_auth
        self.alias = alias
        self.summary = summary
        self.agent = agent
        self.cli_path = cli_path
        self.node_bin = node_bin
        self.heartbeat_interval = heartbeat_interval
        self.safety_poll_interval = safety_poll_interval
        self.run_timeout = run_timeout

        self.cwd = os.getcwd()
        self.peer_id: str | None = None
        self.sse_connected = False
        self.revoked = False
        self.last_error: str | None = None

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

    def register(self) -> str:
        with self._lock:
            if self.revoked:
                raise RuntimeError("peer revoked by broker")
            if self.peer_id:
                return self.peer_id
        payload = self._post_json(
            "/register",
            {
                "pid": os.getpid(),
                "parent_pid": os.getppid(),
                "logical_name": self.alias,
                "cwd": self.cwd,
                "git_root": None,
                "tty": None,
                "summary": self.summary,
            },
        )
        peer_id = payload.get("id")
        if not isinstance(peer_id, str) or not peer_id:
            raise RuntimeError(f"register failed: {payload}")
        with self._lock:
            self.peer_id = peer_id
            self._delivered_ids.clear()
        print(f"[openbot-bridge] registered alias={self.alias} id={peer_id}", flush=True)
        return peer_id

    def heartbeat(self) -> None:
        with self._lock:
            peer_id = self.peer_id
        if not peer_id:
            return
        payload = self._post_json("/heartbeat", {"id": peer_id})
        if payload.get("reregister"):
            with self._lock:
                self.peer_id = None
                self._delivered_ids.clear()
            self.register()

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
            with self._lock:
                self.peer_id = None
                self._delivered_ids.clear()
            self.register()
            return 0
        messages = payload.get("messages") or []
        delivered = 0
        for message in messages:
            msg_id = message.get("id")
            if isinstance(msg_id, int) and msg_id in self._delivered_ids:
                continue
            if self.dispatch_to_openbot(message):
                if isinstance(msg_id, int):
                    self._post_json("/mark-message-delivered", {"id": msg_id})
                    self._delivered_ids.add(msg_id)
                delivered += 1
        return delivered

    def dispatch_to_openbot(self, message: dict[str, Any]) -> bool:
        prompt = _format_task(message)
        argv = [self.node_bin, self.cli_path, "run", self.agent, "-"]
        try:
            proc = subprocess.run(
                argv,
                input=prompt,
                text=True,
                shell=False,
                capture_output=True,
                timeout=self.run_timeout,
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
            f"[openbot-bridge] delivered msg={message.get('id')} to agent={self.agent}",
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
                    with self._lock:
                        self.peer_id = None
                        self._delivered_ids.clear()
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
) -> int:
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
    result = _api_request(
        broker_url,
        broker_auth,
        "POST",
        "/send-message",
        {
            "from_id": from_id,
            "from_pid": os.getpid(),
            "to_id": to_id,
            "text": message,
        },
    )
    if not result.get("ok"):
        print(json.dumps(result, ensure_ascii=False), file=sys.stderr)
        return 1
    target = result.get("to_alias") or result.get("to_id") or to_id
    print(f"Sent to {target} ({result.get('to_id')})")
    return 0


def main(argv: list[str] | None = None) -> int:
    _load_env_defaults()
    args = list(sys.argv[1:] if argv is None else argv)
    broker_url = os.environ.get("CLAUDE_PEERS_BROKER_URL", "https://peers.insightflo.si").strip()
    broker_auth = os.environ.get("CLAUDE_PEERS_BROKER_AUTH", "").strip() or None
    alias = os.environ.get("CLAUDE_PEERS_NAME", "").strip()

    if args and args[0] == "list":
        return cli_list(broker_url, broker_auth)
    if args and args[0] == "send":
        if len(args) < 3:
            print("Usage: peers-send <to_alias_or_id> <message>", file=sys.stderr)
            return 2
        return cli_send(broker_url, broker_auth, alias, args[1], " ".join(args[2:]))

    agent = os.environ.get("OPENBOT_AGENT", "").strip()
    if not alias or not agent:
        print("CLAUDE_PEERS_NAME and OPENBOT_AGENT are required", file=sys.stderr)
        return 2
    summary = os.environ.get("CLAUDE_PEERS_SUMMARY", f"OpenBot ({agent})").strip()
    cli_path = os.environ.get("OPENBOT_CLI_PATH", "").strip() or _default_cli_path()
    node_bin = os.environ.get("OPENBOT_NODE_BIN", "node").strip() or "node"
    daemon = OpenBotPeersDaemon(
        broker_url=broker_url,
        broker_auth=broker_auth,
        alias=alias,
        summary=summary,
        agent=agent,
        cli_path=cli_path,
        node_bin=node_bin,
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
