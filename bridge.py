"""Background bridge between Hermes and the claude-peers broker."""

from __future__ import annotations

import atexit
import json
import mimetypes
import os
import queue
import re
import shlex
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_BROKER_PORT = int(os.environ.get("CLAUDE_PEERS_PORT", "7899"))

# Broker target is decided at __init__ time, not module-import time,
# because plugin modules can be imported before _HERMES_GATEWAY is set.
DEFAULT_POLL_INTERVAL = float(
    os.environ.get("CLAUDE_PEERS_BRIDGE_POLL_INTERVAL_SEC", "1.0")
)
DEFAULT_HEARTBEAT_INTERVAL = float(
    os.environ.get("CLAUDE_PEERS_BRIDGE_HEARTBEAT_INTERVAL_SEC", "15.0")
)
DEFAULT_SUMMARY = os.environ.get("CLAUDE_PEERS_SUMMARY", "Hermes Agent")

# Attachments are stored on the broker and referenced by id. Received files are
# cached locally so the agent can open them by path; sent files are uploaded first.
DEFAULT_ATTACHMENT_DIR = Path.home() / ".peers-bridge" / "attachments"
DEFAULT_ATTACHMENT_KEEP_HOURS = 168.0
ATTACHMENT_DOWNLOAD_TIMEOUT_SECONDS = 30.0
ATTACHMENT_UPLOAD_TIMEOUT_SECONDS = 60.0
MAX_ATTACHMENTS_PER_MESSAGE = 5
MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024
MAX_ATTACHMENT_NAME_LENGTH = 128


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


def _http_error_detail(exc: urllib.error.HTTPError) -> str:
    try:
        detail = exc.read().decode("utf-8", errors="replace").strip()
    except Exception:  # body already consumed or unreadable
        detail = ""
    try:
        payload = json.loads(detail)
        if isinstance(payload, dict):
            # Broker errors are `{error, message}`; show those instead of raw JSON.
            detail = ": ".join(
                str(payload[key]) for key in ("error", "message") if payload.get(key)
            ) or detail
    except (ValueError, TypeError):
        pass
    return f"HTTP {exc.code} {detail}".strip()


def _attachment_error_reason(exc: BaseException) -> str:
    """Human-readable reason for a failed attachment transfer (never raises)."""
    if isinstance(exc, urllib.error.HTTPError):
        return _http_error_detail(exc)
    if isinstance(exc, urllib.error.URLError):
        return str(exc.reason or exc)
    return str(exc) or exc.__class__.__name__


def _attachment_headers(broker_auth: str | None, **extra: str) -> dict[str, str]:
    headers = {"User-Agent": "peers-bridge/1.0"}
    if broker_auth:
        headers["Authorization"] = broker_auth
    headers.update(extra)
    return headers


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
    req = urllib.request.Request(url, headers=_attachment_headers(broker_auth), method="GET")
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


def _multipart_part(
    boundary: str,
    name: str,
    value: bytes | str,
    *,
    filename: str | None = None,
    content_type: str | None = None,
) -> bytes:
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
    `RuntimeError` when the file is unreadable/empty or the broker rejects it.
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
        headers=_attachment_headers(
            broker_auth, **{"Content-Type": f"multipart/form-data; boundary={boundary}"}
        ),
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        exc.close()
        raise RuntimeError(f"broker /attachments failed: {_http_error_detail(exc)}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"broker /attachments failed: {exc.reason}") from exc
    if not isinstance(payload, dict) or not payload.get("id"):
        raise RuntimeError(f"broker /attachments returned no id: {payload}")
    return payload


def cleanup_attachment_dirs(base_dir: Path, keep_hours: float, *, now: float | None = None) -> int:
    """Delete per-message attachment directories older than `keep_hours`.

    Opportunistic and non-fatal: returns the number of directories removed and
    swallows individual removal errors. `keep_hours` of 0 removes everything.
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


def _resolve_broker():
    """Return (broker_url, is_remote, broker_auth) based on current env.

    Gateway agents connect to the REMOTE broker (cross-network hub).
    CLI agents connect to the LOCAL broker (intra-network peers).
    """
    is_gateway = os.environ.get("_HERMES_GATEWAY") == "1"
    remote_url = os.environ.get("CLAUDE_PEERS_BROKER_URL")
    if is_gateway and remote_url:
        return remote_url, True, os.environ.get("CLAUDE_PEERS_BROKER_AUTH")
    return f"http://127.0.0.1:{DEFAULT_BROKER_PORT}", False, None


def _is_subagent() -> bool:
    """Return True if this process is a kanban worker, delegate child, or
    other subagent that should NOT register as a peer.

    Only gateway agents and user-launched CLI agents should register.
    """
    if os.environ.get("HERMES_KANBAN_TASK"):
        return True
    if os.environ.get("HERMES_DELEGATE_CHILD") == "1":
        return True
    return False


def _json_response(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _git_root(cwd: str) -> str | None:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None
    result = proc.stdout.strip()
    return result or None


def _tty_name() -> str | None:
    for fd in (0, 1, 2):
        try:
            return os.ttyname(fd)
        except OSError:
            continue
    return None


def _logical_name(prefix: str | None) -> str | None:
    explicit = os.environ.get("CLAUDE_PEERS_NAME")
    if explicit:
        return explicit.strip() or None

    prefix = (prefix or os.environ.get("CLAUDE_PEERS_NAME_PREFIX") or "hermes").strip()
    if not prefix:
        return None

    surface = os.environ.get("CMUX_SURFACE_ID")
    if surface:
        suffix = surface.strip().lower()[:8]
    else:
        tty = _tty_name()
        if tty:
            suffix = Path(tty).name.lower()
        else:
            suffix = str(os.getpid())
    return f"{prefix}@{suffix}"


def _find_broker_script() -> Path | None:
    env_home = os.environ.get("CLAUDE_PEERS_HOME")
    candidates = []
    if env_home:
        candidates.append(Path(env_home) / "broker.ts")
    candidates.append(
        Path.home() / ".local" / "share" / "claude-peers-mcp" / "broker.ts"
    )
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


@dataclass
class BrokerPeerState:
    peer_id: str | None = None
    logical_name: str | None = None
    summary: str = DEFAULT_SUMMARY
    broker_url: str = ""
    is_remote: bool = False
    broker_auth: str | None = None
    running: bool = False
    autostart: bool = True
    last_error: str | None = None


class ClaudePeersBridge:
    """Register Hermes as a claude-peers peer and inject incoming messages."""

    def __init__(self, ctx):
        self._ctx = ctx
        # Subagents (kanban workers, delegate children) must not register as peers.
        is_sub = _is_subagent()
        # Resolve broker target at init time (not module-import time).
        broker_url, is_remote, broker_auth = _resolve_broker()
        self._state = BrokerPeerState(
            logical_name=_logical_name(
                os.environ.get("CLAUDE_PEERS_NAME_PREFIX") or "hermes"
            ),
            broker_url=broker_url,
            is_remote=is_remote,
            broker_auth=broker_auth,
            autostart=(not is_sub)
            and (os.environ.get("CLAUDE_PEERS_BRIDGE_AUTOSTART", "1") != "0"),
        )
        if is_sub:
            self._state.last_error = "subagent detected (HERMES_KANBAN_TASK/HERMES_DELEGATE_CHILD) — peer registration skipped"
        self._cwd = os.getcwd()
        self._git_root = _git_root(self._cwd)
        self._tty = _tty_name()
        self._pending_injections: "queue.Queue[tuple[int | None, str]]" = queue.Queue()
        # Dedupe guard: poll runs with mark_delivered=False, so the same
        # undelivered message comes back every iteration until injection
        # succeeds. Without this set the pending queue grows one duplicate
        # per poll tick.
        self._queued_message_ids: set[int] = set()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self._lifecycle_lock = threading.RLock()
        self._transport = os.environ.get("CLAUDE_PEERS_BRIDGE_TRANSPORT", "sse").lower()
        if self._transport not in {"sse", "poll"}:
            self._transport = "sse"
        self._safety_poll_sec = max(
            0.01, float(os.environ.get("CLAUDE_PEERS_BRIDGE_SAFETY_POLL_SEC", "30.0"))
        )
        self._wake_debounce_sec = max(
            0.0, float(os.environ.get("CLAUDE_PEERS_WAKE_DEBOUNCE_SEC", "0.25"))
        )
        self._wake_cmd = os.environ.get("CLAUDE_PEERS_WAKE_CMD", "")
        self._wake_webhook = os.environ.get("CLAUDE_PEERS_WAKE_WEBHOOK", "")
        self._wake_session = getattr(ctx, "wake_session", None)
        self._notify = getattr(ctx, "notify", None)
        self._wake_lock = threading.Lock()
        self._last_wake = float("-inf")
        self._wake_event = threading.Event()
        self._sse_connected = False
        self._sse_thread: threading.Thread | None = None
        self._sse_response = None
        self._revoked = False
        self._queued_message_ids: set[int] = set()
        # Accepted injections awaiting acknowledgement must never be injected again.
        self._injected_message_ids: set[int] = set()
        # Local cache of received attachments, one directory per message id.
        self._attachment_dir = Path(
            os.environ.get("PEERS_BRIDGE_ATTACHMENT_DIR", "").strip()
            or DEFAULT_ATTACHMENT_DIR
        ).expanduser()
        try:
            keep_hours = float(
                os.environ.get("PEERS_BRIDGE_ATTACHMENT_KEEP_HOURS", "").strip()
                or DEFAULT_ATTACHMENT_KEEP_HOURS
            )
        except ValueError:
            keep_hours = DEFAULT_ATTACHMENT_KEEP_HOURS
        self._attachment_keep_hours = max(0.0, keep_hours)
        atexit.register(self.stop)

    @property
    def state(self) -> BrokerPeerState:
        return self._state

    def start(self) -> None:
        with self._lifecycle_lock:
            self._start()

    def _start(self) -> None:
        with self._lock:
            # Do not create overlapping generations if a previous stop is finishing.
            if any(t and t.is_alive() for t in (self._thread, self._sse_thread)):
                return
            self._revoked = False
            self._state.last_error = None
            self._stop_event.clear()
            self._wake_event.clear()
            self._thread = threading.Thread(
                target=self._run_loop, name="claude-peers-bridge", daemon=True
            )
            self._state.running = True
            self._thread.start()
            if self._transport == "sse":
                self._sse_thread = threading.Thread(
                    target=self._sse_loop, name="claude-peers-sse", daemon=True
                )
                self._sse_thread.start()

    def stop(self) -> None:
        with self._lifecycle_lock:
            self._stop()

    def _stop(self) -> None:
        with self._lock:
            self._stop_event.set()
            self._wake_event.set()
            self._close_sse_stream()
            threads = (self._thread, self._sse_thread)
            self._state.running = False
        for thread in threads:
            if (
                thread
                and thread is not threading.current_thread()
                and thread.is_alive()
            ):
                thread.join(timeout=6.0)
        try:
            self._unregister()
        except Exception:
            pass

    def status_payload(self) -> dict[str, Any]:
        with self._lock:
            return {
                "ok": True,
                "running": self._state.running,
                "peer_id": self._state.peer_id,
                "logical_name": self._state.logical_name,
                "summary": self._state.summary,
                "broker_url": self._state.broker_url,
                "is_remote": self._state.is_remote,
                "cwd": self._cwd,
                "git_root": self._git_root,
                "tty": self._tty,
                "last_error": self._state.last_error,
                "autostart": self._state.autostart,
                "transport": self._transport,
                "sse_connected": self._sse_connected,
                "wake_configured": bool(
                    callable(self._wake_session)
                    or callable(self._notify)
                    or self._wake_cmd.strip()
                    or self._wake_webhook
                ),
            }

    def list_peers(self, scope: str = "machine") -> str:
        with self._lock:
            self._ensure_registered()
            payload = self._post_json(
                "/list-peers",
                {
                    "scope": scope,
                    "cwd": self._cwd,
                    "git_root": self._git_root,
                    "exclude_id": self._state.peer_id,
                },
            )
        return _json_response({"ok": True, "scope": scope, "peers": payload})

    def send_message(
        self, to_id: str, message: str, file_paths: list[str] | str | None = None
    ) -> str:
        """Send `message` to `to_id`, optionally uploading `file_paths` first.

        Attachment problems (missing/oversized file, too many files, rejected
        upload) return an error payload instead of raising and never send the
        message. Broker failures on `/send-message` itself still raise.
        """
        paths = self._normalize_file_paths(file_paths)
        if isinstance(paths, str):
            return _json_response({"ok": False, "sent": False, "error": paths})
        with self._lock:
            self._ensure_registered()
            body: dict[str, Any] = {
                "from_id": self._state.peer_id,
                "from_pid": os.getpid(),
                "to_id": to_id,
                "text": message,
            }
            if paths:
                problem = self._validate_file_paths(paths)
                if problem:
                    return _json_response({"ok": False, "sent": False, "error": problem})
                attachment_ids: list[str] = []
                for path in paths:
                    try:
                        uploaded = upload_attachment(
                            self._state.broker_url,
                            self._state.broker_auth,
                            path,
                            from_id=self._state.peer_id,
                        )
                    except Exception as exc:
                        return _json_response(
                            {
                                "ok": False,
                                "sent": False,
                                "error": f"attachment upload failed for {path}: {exc}",
                            }
                        )
                    attachment_ids.append(str(uploaded["id"]))
                body["attachments"] = attachment_ids
            payload = self._post_json("/send-message", body)
        return _json_response(payload)

    @staticmethod
    def _normalize_file_paths(file_paths: list[str] | str | None) -> list[str] | str:
        if file_paths is None:
            return []
        if isinstance(file_paths, str):
            file_paths = [file_paths]
        if not isinstance(file_paths, list) or not all(isinstance(p, str) for p in file_paths):
            return "file_paths must be an array of local file path strings"
        paths = [p.strip() for p in file_paths if p and p.strip()]
        if len(paths) > MAX_ATTACHMENTS_PER_MESSAGE:
            return (
                f"too many attachments: {len(paths)} given, "
                f"at most {MAX_ATTACHMENTS_PER_MESSAGE} are allowed"
            )
        return paths

    @staticmethod
    def _validate_file_paths(paths: list[str]) -> str | None:
        """Return an error string for the first unusable path, or None when all are sendable."""
        for path in paths:
            src = Path(path).expanduser()
            if not src.is_file():
                return f"attachment not found or not a regular file: {path}"
            try:
                size = src.stat().st_size
            except OSError as exc:
                return f"attachment {path} is unreadable: {exc}"
            if size == 0:
                return f"attachment {path} is empty"
            if size > MAX_ATTACHMENT_BYTES:
                return (
                    f"attachment {path} is {size} bytes; the broker limit is "
                    f"{MAX_ATTACHMENT_BYTES} bytes ({MAX_ATTACHMENT_BYTES // (1024 * 1024)} MB)"
                )
        return None

    def check_messages(self) -> str:
        with self._lock:
            self._ensure_registered()
            payload = self._poll_messages()
        return _json_response({"ok": True, "messages": payload})

    def set_summary(self, summary: str) -> str:
        with self._lock:
            self._state.summary = summary
            self._ensure_registered()
            payload = self._post_json(
                "/set-summary", {"id": self._state.peer_id, "summary": summary}
            )
        return _json_response(payload)

    def set_alias(self, alias: str) -> str:
        with self._lock:
            self._ensure_registered()
            payload = self._post_json(
                "/set-alias", {"id": self._state.peer_id, "alias": alias}
            )
            accepted_alias = payload.get("alias")
            if isinstance(accepted_alias, str):
                self._state.logical_name = accepted_alias
        return _json_response(payload)

    def _close_sse_stream(self) -> None:
        # shutdown unblocks readline before close takes the buffered reader lock.
        response = self._sse_response
        self._sse_response = None
        self._sse_connected = False
        if response is not None:
            try:
                response.fp.raw._sock.shutdown(socket.SHUT_RDWR)
            except (AttributeError, OSError):
                pass
            response.close()

    def _set_peer_id(self, peer_id: str | None) -> None:
        with self._lock:
            if peer_id == self._state.peer_id:
                return
            self._state.peer_id = peer_id
            self._queued_message_ids.clear()
            self._injected_message_ids.clear()
            while True:
                try:
                    self._pending_injections.get_nowait()
                except queue.Empty:
                    break
            self._close_sse_stream()
            self._wake_event.set()

    def _revoke(self) -> None:
        with self._lock:
            self._revoked = True
            self._stop_event.set()
            self._set_peer_id(None)
            self._state.running = False
            self._state.last_error = "peer revoked by broker"
            self._wake_event.set()

    def _sse_loop(self) -> None:
        backoff = (1.0, 2.0, 5.0, 15.0)
        attempt = 0
        while not self._stop_event.is_set():
            with self._lock:
                peer_id = self._state.peer_id
                url = self._state.broker_url
                auth = self._state.broker_auth
            if not peer_id:
                self._stop_event.wait(0.1)
                continue
            headers = {"User-Agent": "peers-bridge/1.0", "Accept": "text/event-stream"}
            if auth:
                headers["Authorization"] = auth
            request = urllib.request.Request(
                f"{url}/events?{urllib.parse.urlencode({'id': peer_id})}",
                headers=headers,
            )
            response = None
            try:
                response = urllib.request.urlopen(request, timeout=5.0)
                with self._lock:
                    if self._stop_event.is_set() or self._state.peer_id != peer_id:
                        continue
                    self._sse_response = response
                    self._sse_connected = True
                    # Bound connection establishment, then allow an idle stream.
                    # stop/re-registration explicitly shuts this socket down.
                    try:
                        response.fp.raw._sock.settimeout(None)
                    except AttributeError:
                        pass
                    self._wake_event.set()  # Catch up once after every reconnect.
                while not self._stop_event.is_set():
                    line = response.readline()
                    if not line:
                        break
                    with self._lock:
                        if self._state.peer_id != peer_id:
                            break
                        attempt = 0
                        field, _, value = (
                            line.decode("utf-8", errors="replace")
                            .rstrip("\r\n")
                            .partition(":")
                        )
                        value = value.lstrip()
                        if field == "event" and value == "revoked":
                            self._revoke()
                            return
                        if (field == "event" and value == "wake") or (
                            field == "data" and "messages" in value
                        ):
                            self._wake_event.set()
            except urllib.error.HTTPError as exc:
                exc.close()
                with self._lock:
                    if self._state.peer_id == peer_id:
                        if exc.code == 410:
                            self._revoke()
                            return
                        if exc.code == 404:
                            self._set_peer_id(None)
                        self._state.last_error = (
                            f"broker /events failed: HTTP {exc.code}"
                        )
            except Exception as exc:
                with self._lock:
                    if not self._stop_event.is_set() and self._state.peer_id == peer_id:
                        self._state.last_error = f"broker /events failed: {exc}"
            finally:
                with self._lock:
                    if self._sse_response is response:
                        self._sse_response = None
                        self._sse_connected = False
                    self._wake_event.set()  # Resume legacy polling on disconnect.
                if response is not None:
                    response.close()
            if self._stop_event.wait(backoff[min(attempt, len(backoff) - 1)]):
                break
            attempt += 1

    def _run_loop(self) -> None:
        last_heartbeat = last_poll = float("-inf")
        try:
            while not self._stop_event.is_set():
                # All wake producers use the same lock: do not lose a wake
                # arriving between observing the flag and clearing it.
                with self._lock:
                    wake = self._wake_event.is_set()
                    self._wake_event.clear()
                failed = False
                try:
                    with self._lock:
                        if self._stop_event.is_set():
                            break
                        new_url, new_remote, new_auth = _resolve_broker()
                        if (new_url, new_remote, new_auth) != (
                            self._state.broker_url,
                            self._state.is_remote,
                            self._state.broker_auth,
                        ):
                            self._unregister()
                            self._state.broker_url = new_url
                            self._state.is_remote = new_remote
                            self._state.broker_auth = new_auth
                        self._ensure_registered(verify_heartbeat=False)
                        now = time.monotonic()
                        if now - last_heartbeat >= DEFAULT_HEARTBEAT_INTERVAL:
                            last_heartbeat = now
                            self._heartbeat()
                        interval = (
                            self._safety_poll_sec
                            if self._transport == "sse" and self._sse_connected
                            else DEFAULT_POLL_INTERVAL
                        )
                        if wake or now - last_poll >= interval:
                            messages = self._poll_messages(mark_delivered=False)
                            last_poll = time.monotonic()
                            for message in messages:
                                self._queue_injection(
                                    self._format_injected_message(message),
                                    message_id=message.get("id"),
                                )
                            self._cleanup_attachments()
                    self._drain_pending_injections()
                except Exception as exc:
                    failed = True
                    with self._lock:
                        if not self._revoked:
                            self._state.last_error = str(exc)
                with self._lock:
                    now = time.monotonic()
                    interval = (
                        self._safety_poll_sec
                        if self._transport == "sse" and self._sse_connected
                        else DEFAULT_POLL_INTERVAL
                    )
                    delay = (
                        min(
                            last_heartbeat + DEFAULT_HEARTBEAT_INTERVAL,
                            last_poll + interval,
                        )
                        - now
                    )
                    if not self._pending_injections.empty():
                        delay = min(delay, DEFAULT_POLL_INTERVAL)
                    if failed:
                        delay = DEFAULT_POLL_INTERVAL
                self._wake_event.wait(max(0.0, delay))
        finally:
            with self._lock:
                self._state.running = False

    def _queue_injection(self, content: str, message_id: int | None = None) -> None:
        with self._lock:
            if message_id is not None:
                if message_id in self._queued_message_ids:
                    return
                self._queued_message_ids.add(message_id)
            self._pending_injections.put((message_id, content))

    def _drain_pending_injections(self) -> None:
        while not self._stop_event.is_set():
            with self._lock:
                try:
                    message_id, content = self._pending_injections.get_nowait()
                except queue.Empty:
                    return
                try:
                    already_injected = (
                        message_id is not None
                        and message_id in self._injected_message_ids
                    )
                    injected = already_injected
                    if not injected:
                        injected = bool(self._ctx.inject_message(content, role="system"))
                        if not injected:
                            injected = self._inject_via_gateway(content)
                    if not injected:
                        self._pending_injections.put((message_id, content))
                        idle = True
                    else:
                        idle = False
                        if message_id is not None:
                            self._injected_message_ids.add(message_id)
                            self._mark_message_delivered(message_id)
                            self._queued_message_ids.discard(message_id)
                            self._injected_message_ids.discard(message_id)
                except Exception as exc:
                    self._state.last_error = (
                        f"delivery failed for message {message_id}: {exc}"
                    )
                    self._pending_injections.put((message_id, content))
                    return
            if idle:
                self._wake_idle_session()
                return

    def _wake_idle_session(self) -> None:
        if not self.status_payload()["wake_configured"] or not self._wake_lock.acquire(
            blocking=False
        ):
            return
        try:
            now = time.monotonic()
            if now - self._last_wake < self._wake_debounce_sec:
                return
            if self._stop_event.wait(self._wake_debounce_sec):
                return
            self._last_wake = time.monotonic()
            with self._lock:
                peer_id = self._state.peer_id
                count = self._pending_injections.qsize()
            if not count:
                return
            if callable(self._wake_session):
                self._wake_session()
            elif callable(self._notify):
                self._notify("Peer messages pending")
            if self._wake_cmd:
                try:
                    argv = shlex.split(self._wake_cmd)
                except ValueError as exc:
                    with self._lock:
                        self._state.last_error = f"invalid wake command: {exc}"
                    return
                if argv:
                    subprocess.run(
                        argv,
                        shell=False,
                        timeout=5.0,
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
            if self._wake_webhook:
                if urllib.parse.urlsplit(self._wake_webhook).scheme not in {
                    "http",
                    "https",
                }:
                    raise ValueError("wake webhook must use http:// or https://")
                request = urllib.request.Request(
                    self._wake_webhook,
                    method="POST",
                    data=json.dumps(
                        {
                            "event": "peer_messages_pending",
                            "peer_id": peer_id,
                            "count": count,
                        }
                    ).encode("utf-8"),
                    headers={
                        "Content-Type": "application/json",
                        "User-Agent": "peers-bridge/1.0",
                    },
                )
                with urllib.request.urlopen(request, timeout=3.0):
                    pass
        except Exception as exc:
            with self._lock:
                self._state.last_error = f"wake failed: {exc}"
        finally:
            self._wake_lock.release()

    def _inject_via_gateway(self, content: str) -> bool:
        """Deliver an incoming peer message inside a gateway process.

        ``ctx.inject_message`` only works in interactive CLI sessions
        (``_cli_ref``). In the gateway there is no CLI, so we wake the home
        channel session via the same ``deliver_wake`` path the kanban
        notifier uses: schedule a synthetic internal MessageEvent on the
        gateway event loop. The agent then sees the peer message and can
        reply with ``claude_peers_send_message``.

        Returns True once the wake turn was scheduled on the loop.
        """
        import asyncio

        try:
            from gateway.run import _gateway_runner_ref, _home_target_env_var, _home_thread_env_var
            from gateway.session import SessionSource
        except Exception:
            return False
        # gateway.wake only exists in newer Hermes builds. Fall back to the
        # synthetic-MessageEvent path (what deliver_wake does for push
        # adapters) so older gateways (e.g. A1) still inject peer messages.
        try:
            from gateway.wake import adapter_supports_push, deliver_wake
        except Exception:
            deliver_wake = None

            def adapter_supports_push(adapter):
                return bool(getattr(adapter, "supports_async_delivery", True))

        runner = None
        try:
            runner = _gateway_runner_ref()
        except Exception:
            runner = None
        if runner is None:
            return False
        loop = getattr(runner, "_gateway_loop", None)
        if loop is None or loop.is_closed():
            return False
        adapters = getattr(runner, "adapters", {}) or {}
        for platform, adapter in adapters.items():
            if adapter is None or not adapter_supports_push(adapter):
                continue
            platform_name = platform.value if hasattr(platform, "value") else str(platform)
            try:
                chat_id = (os.environ.get(_home_target_env_var(platform_name)) or "").strip()
            except Exception:
                chat_id = ""
            if not chat_id:
                continue
            try:
                thread_id = (os.environ.get(_home_thread_env_var(platform_name)) or "").strip() or None
            except Exception:
                thread_id = None
            source = SessionSource(
                platform=platform,
                chat_id=chat_id,
                chat_type="private",
                thread_id=thread_id,
            )
            try:
                if deliver_wake is not None:
                    _coro = deliver_wake(adapter, text=content, source=source)
                else:
                    from gateway.platforms.base import MessageEvent, MessageType

                    _coro = adapter.handle_message(
                        MessageEvent(
                            text=content,
                            message_type=MessageType.TEXT,
                            source=source,
                            internal=True,
                        )
                    )
                future = asyncio.run_coroutine_threadsafe(_coro, loop)
            except Exception as exc:
                self._state.last_error = f"gateway wake scheduling failed: {exc}"
                continue

            def _log_wake_result(fut) -> None:
                try:
                    fut.result()
                except Exception as exc:  # pragma: no cover - best-effort logging
                    self._state.last_error = f"gateway wake turn failed: {exc}"

            future.add_done_callback(_log_wake_result)
            return True
        self._state.last_error = (
            "gateway injection failed: no push-capable adapter with a configured home channel"
        )
        return False

    def _format_injected_message(self, message: dict[str, Any]) -> str:
        from_id = message.get("from_id", "unknown")
        from_alias = message.get("from_alias") or from_id
        from_kind = message.get("from_kind", "peer")
        sent_at = message.get("sent_at", "")
        raw_attachments = message.get("attachments")
        has_attachments = isinstance(raw_attachments, list) and bool(raw_attachments)
        reply_note = (
            f"If a reply is needed, use the tool `claude_peers_send_message` with to_id=`{from_id}`."
            + (
                f" Its optional file_paths parameter attaches up to {MAX_ATTACHMENTS_PER_MESSAGE} "
                "local files when relevant."
                if has_attachments
                else ""
            )
            if from_kind == "peer"
            else "This sender is marked as system-only. Do not reply through peer messaging."
        )
        text = (
            "[claude-peers] New incoming message\n"
            f"from_alias: {from_alias}\n"
            f"from_id: {from_id}\n"
            f"from_kind: {from_kind}\n"
            f"sent_at: {sent_at}\n\n"
            f"{message.get('text', '')}\n\n"
            f"{reply_note}"
        )
        if not has_attachments:
            return text
        return text + "\n\n" + self._format_attachment_block(
            self._download_attachments(message)
        )

    @staticmethod
    def _format_attachment_block(attachments: list[dict[str, Any]]) -> str:
        lines = []
        for item in attachments:
            if "error" in item:
                lines.append(f"- {item['name']}: download failed ({item['error']})")
            else:
                lines.append(
                    f"- {item['path']} ({item['name']}, {item['mime']}, {item['size']} bytes)"
                )
        return "Attachments:\n" + "\n".join(lines)

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
        key = sanitize_attachment_name(str(message.get("id") if message.get("id") is not None else "unknown"))
        try:
            msg_dir = self._attachment_dir / key
            ensure_private_dir(msg_dir)
        except OSError as exc:
            self._log_attachment_error(f"attachment dir {msg_dir} unavailable: {exc}")
            return [
                {"name": sanitize_attachment_name(att.get("name") or "attachment"), "error": str(exc)}
                for att in raw
                if isinstance(att, dict)
            ]
        results: list[dict[str, Any]] = []
        for att in raw:
            if not isinstance(att, dict):
                continue
            name = sanitize_attachment_name(att.get("name") or "attachment")
            size = att.get("size")
            dest = msg_dir / name
            if dest.is_file() and (not isinstance(size, int) or dest.stat().st_size == size):
                results.append(
                    {
                        "path": str(dest.resolve()),
                        "name": name,
                        "mime": att.get("mime"),
                        "size": dest.stat().st_size,
                    }
                )
                continue
            try:
                written = download_attachment(
                    self._state.broker_url,
                    self._state.broker_auth,
                    str(att.get("id") or ""),
                    dest,
                )
            except Exception as exc:
                reason = _attachment_error_reason(exc)
                self._log_attachment_error(f"attachment download failed ({name}): {reason}")
                results.append({"name": name, "error": reason})
                continue
            results.append(
                {"path": str(dest.resolve()), "name": name, "mime": att.get("mime"), "size": written}
            )
        return results

    def _log_attachment_error(self, text: str) -> None:
        with self._lock:
            self._state.last_error = text
        print(f"[claude-peers-bridge] {text}", file=sys.stderr, flush=True)

    def _cleanup_attachments(self) -> None:
        """Drop cached attachment directories past their retention; never fatal."""
        try:
            cleanup_attachment_dirs(self._attachment_dir, self._attachment_keep_hours)
        except Exception as exc:
            print(f"[claude-peers-bridge] attachment cleanup failed: {exc}", file=sys.stderr, flush=True)

    def _ensure_registered(self, verify_heartbeat: bool = True) -> None:
        with self._lock:
            if self._revoked:
                raise RuntimeError("peer revoked by broker")
            if self._state.peer_id:
                if not verify_heartbeat:
                    return
                try:
                    self._heartbeat()
                except RuntimeError:
                    if self._revoked:
                        raise
                    self._set_peer_id(None)
                else:
                    return
            self._ensure_broker()
            payload = self._post_json(
                "/register",
                {
                    "pid": os.getpid(),
                    "parent_pid": os.getppid(),
                    "logical_name": self._state.logical_name,
                    "cwd": self._cwd,
                    "git_root": self._git_root,
                    "tty": self._tty,
                    "summary": self._state.summary,
                },
            )
            peer_id = payload.get("id")
            if not peer_id:
                raise RuntimeError(f"Broker register failed: {payload}")
            self._set_peer_id(peer_id)
            self._state.last_error = None
            registered_alias = payload.get("alias")
            if isinstance(registered_alias, str):
                self._state.logical_name = registered_alias

    def _heartbeat(self) -> None:
        with self._lock:
            if not self._state.peer_id:
                return
            payload = self._post_json("/heartbeat", {"id": self._state.peer_id})
            if payload.get("reregister"):
                self._set_peer_id(None)
                self._ensure_registered(verify_heartbeat=False)

    def _poll_messages(self, *, mark_delivered: bool = True) -> list[dict[str, Any]]:
        with self._lock:
            if not self._state.peer_id:
                return []
            payload = self._post_json(
                "/poll-messages",
                {"id": self._state.peer_id, "mark_delivered": mark_delivered},
            )
            if payload.get("reregister"):
                self._set_peer_id(None)
                self._ensure_registered(verify_heartbeat=False)
                return []
            return payload.get("messages", []) or []

    def _mark_message_delivered(self, message_id: int) -> None:
        try:
            self._post_json("/mark-message-delivered", {"id": message_id})
        except RuntimeError as exc:
            # The remote hub broker (broker.remote-hub.ts) marks messages
            # delivered at poll time and has no /mark-message-delivered
            # endpoint (HTTP 404). Treat that as already-delivered instead
            # of failing — otherwise the message is requeued and injected
            # repeatedly.
            if "404" in str(exc):
                return
            raise

    def _unregister(self) -> None:
        with self._lock:
            if not self._state.peer_id:
                return
            try:
                self._post_json("/unregister", {"id": self._state.peer_id})
            finally:
                self._set_peer_id(None)

    def _ensure_broker(self) -> None:
        if self._health_ok():
            return
        if self._state.is_remote:
            raise RuntimeError(
                f"Remote broker at {self._state.broker_url} is not reachable. "
                "Check CLAUDE_PEERS_BROKER_URL and network/tunnel connectivity."
            )
        broker_script = _find_broker_script()
        if broker_script is None:
            raise RuntimeError(
                "claude-peers broker.ts not found. Set CLAUDE_PEERS_HOME or install claude-peers-mcp first."
            )
        try:
            subprocess.Popen(
                ["bun", str(broker_script)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(
                "bun is required to launch the claude-peers broker"
            ) from exc

        deadline = time.time() + 3.0
        while time.time() < deadline:
            if self._health_ok():
                return
            time.sleep(0.1)
        raise RuntimeError(
            f"claude-peers broker did not become healthy at {self._state.broker_url}"
        )

    def _health_ok(self) -> bool:
        try:
            req = urllib.request.Request(f"{self._state.broker_url}/health")
            req.add_header("User-Agent", "peers-bridge/1.0")
            if self._state.broker_auth:
                req.add_header("Authorization", self._state.broker_auth)
            with urllib.request.urlopen(req, timeout=3.0) as response:
                data = json.loads(response.read().decode("utf-8"))
            return data.get("status") == "ok"
        except Exception:
            return False

    def _post_json(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        data = json.dumps(body).encode("utf-8")
        headers = {"Content-Type": "application/json", "User-Agent": "peers-bridge/1.0"}
        if self._state.broker_auth:
            headers["Authorization"] = self._state.broker_auth
        request = urllib.request.Request(
            f"{self._state.broker_url}{path}",
            data=data,
            headers=headers,
            method="POST",
        )
        timeout = 5.0 if self._state.is_remote else 2.0
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if path == "/heartbeat" and exc.code == 410:
                self._revoke()
                exc.close()
                raise RuntimeError("peer revoked by broker") from exc
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"broker {path} failed: HTTP {exc.code} {detail}"
            ) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"broker {path} failed: {exc.reason}") from exc
