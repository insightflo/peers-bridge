"""Attachment receive (download + prompt path injection) and send (upload) for bridge.py."""

from __future__ import annotations

import io
import json
import os
import tempfile
import threading
import time
import unittest
import urllib.error
import sys
import urllib.error
from pathlib import Path
from unittest import mock

PACKAGE_PARENT = Path(__file__).resolve().parents[2]
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))

from peers_bridge import bridge as bridge_module
from peers_bridge.bridge import ClaudePeersBridge
from peers_bridge import schemas


class Context:
    def __init__(self, accepted=True):
        self.accepted = accepted
        self.injected = []
        self.received = threading.Event()

    def inject_message(self, content, role="system"):
        self.injected.append((role, content))
        self.received.set()
        return self.accepted


def _message(**overrides):
    message = {
        "id": 42,
        "from_id": "abc12345",
        "from_alias": "Proxmox",
        "from_kind": "peer",
        "sent_at": "2026-07-22T00:00:00Z",
        "text": "hello",
    }
    message.update(overrides)
    return message


class AttachmentTestCase(unittest.TestCase):
    def make_bridge(self, ctx=None, **env) -> ClaudePeersBridge:
        self.attach_dir = Path(self.tempdir.name) / "attachments"
        settings = {
            "CLAUDE_PEERS_NAME": "A1",
            "CLAUDE_PEERS_BRIDGE_AUTOSTART": "0",
            "CLAUDE_PEERS_BRIDGE_TRANSPORT": "sse",
            "PEERS_BRIDGE_ATTACHMENT_DIR": str(self.attach_dir),
            "PEERS_BRIDGE_ATTACHMENT_KEEP_HOURS": "168",
        }
        settings.update(env)
        with (
            mock.patch.dict(os.environ, settings, clear=False),
            mock.patch.object(bridge_module, "_git_root", return_value=None),
        ):
            bridge = ClaudePeersBridge(ctx or Context())
        bridge._ensure_broker = mock.Mock()
        bridge._post_json = mock.Mock(return_value={"ok": True, "messages": []})
        resolve = mock.patch.object(
            bridge_module,
            "_resolve_broker",
            return_value=(bridge.state.broker_url, False, "Bearer test-token"),
        )
        resolve.start()
        self.addCleanup(resolve.stop)
        bridge._state.broker_auth = "Bearer test-token"
        bridge._set_peer_id("peer-one")
        self._current_bridge = bridge
        self.addCleanup(bridge.stop)
        return bridge

    def delivered_calls(self):
        return [
            c for c in self._current_bridge._post_json.call_args_list
            if c.args[0] == "/mark-message-delivered"
        ]

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)

    def writes(self, files: dict[str, bytes], *, fail: dict[str, Exception] | None = None):
        """Stub urlopen for GET /attachments/<id>; `fail` maps ids to exceptions."""
        fail = fail or {}
        calls = []

        def fake_urlopen(request, timeout=None):
            url = request.full_url if hasattr(request, "full_url") else str(request)
            attachment_id = url.rsplit("/", 1)[-1]
            calls.append((url, timeout, request.get_header("Authorization")))
            if attachment_id in fail:
                raise fail[attachment_id]
            return io.BytesIO(files[attachment_id])

        patcher = mock.patch.object(bridge_module.urllib.request, "urlopen", side_effect=fake_urlopen)
        urlopen = patcher.start()
        self.addCleanup(patcher.stop)
        urlopen.calls = calls
        return urlopen


class ReceiveTests(AttachmentTestCase):
    def test_two_attachments_are_downloaded_and_injected_with_absolute_paths(self):
        bridge = self.make_bridge()
        self.writes({"a1": b"PNGDATA", "b2": b'{"k":1}'})
        message = _message(
            attachments=[
                {"id": "a1", "name": "shot.png", "size": 7, "mime": "image/png"},
                {"id": "b2", "name": "data.json", "size": 7, "mime": "application/json"},
            ]
        )

        content = bridge._format_injected_message(message)
        bridge._queue_injection(content, message_id=42)
        bridge._drain_pending_injections()

        shot = self.attach_dir / "42" / "shot.png"
        data = self.attach_dir / "42" / "data.json"
        self.assertEqual(b"PNGDATA", shot.read_bytes())
        self.assertEqual(b'{"k":1}', data.read_bytes())
        self.assertEqual(0o700, (self.attach_dir / "42").stat().st_mode & 0o777)
        self.assertFalse(shot.with_name("shot.png.part").exists())
        self.assertTrue(str(shot.resolve()).startswith(str(self.attach_dir.resolve()) + os.sep))
        self.assertTrue(
            content.endswith(
                "Attachments:\n"
                f"- {shot.resolve()} (shot.png, image/png, 7 bytes)\n"
                f"- {data.resolve()} (data.json, application/json, 7 bytes)"
            ),
            content,
        )
        self.assertIn("file_paths parameter attaches up to 5 local files", content)
        self.assertEqual(1, len(bridge._ctx.injected))
        self.assertEqual([mock.call("/mark-message-delivered", {"id": 42})], self.delivered_calls())

    def test_run_loop_downloads_and_delivers_message_with_attachments(self):
        bridge = self.make_bridge()
        self.writes({"a1": b"PDFDATA"})
        message = _message(attachments=[{"id": "a1", "name": "doc.pdf", "size": 7, "mime": "application/pdf"}])
        bridge._post_json.side_effect = lambda path, body: (
            {"ok": True, "messages": [message]} if path == "/poll-messages" else {"ok": True}
        )
        thread = threading.Thread(target=bridge._run_loop, daemon=True)
        thread.start()
        try:
            self.assertTrue(bridge._ctx.received.wait(3))
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline and not any(
                c.args[0] == "/mark-message-delivered" for c in bridge._post_json.call_args_list
            ):
                time.sleep(0.01)
        finally:
            bridge._stop_event.set()
            bridge._wake_event.set()
            thread.join(3)
        self.assertEqual(b"PDFDATA", (self.attach_dir / "42" / "doc.pdf").read_bytes())
        sent = [c for c in bridge._post_json.call_args_list if c.args[0] == "/send-message"]
        self.assertEqual([], sent)
        self.assertIn("doc.pdf", bridge._ctx.injected[0][1])
        self.assertEqual([mock.call("/mark-message-delivered", {"id": 42})], self.delivered_calls())

    def test_download_failure_is_reported_but_message_still_injected(self):
        bridge = self.make_bridge()
        exc = urllib.error.HTTPError(
            "http://broker/attachments/gone", 404, "Not Found", {}, io.BytesIO(b'{"error":"attachment not found"}')
        )
        self.writes({"a1": b"OK"}, fail={"gone": exc})
        message = _message(
            attachments=[
                {"id": "a1", "name": "ok.txt", "size": 2, "mime": "text/plain"},
                {"id": "gone", "name": "missing.txt", "size": 5, "mime": "text/plain"},
            ]
        )

        content = bridge._format_injected_message(message)
        bridge._queue_injection(content, message_id=42)
        bridge._drain_pending_injections()

        ok = (self.attach_dir / "42" / "ok.txt").resolve()
        self.assertTrue(
            content.endswith(
                "Attachments:\n"
                f"- {ok} (ok.txt, text/plain, 2 bytes)\n"
                "- missing.txt: download failed (HTTP 404 attachment not found)"
            ),
            content,
        )
        self.assertIn("attachment download failed (missing.txt)", bridge.state.last_error)
        self.assertEqual(1, len(bridge._ctx.injected))
        self.assertEqual([mock.call("/mark-message-delivered", {"id": 42})], self.delivered_calls())
        self.assertFalse((self.attach_dir / "42" / "missing.txt").exists())

    def test_network_failure_uses_reason_and_missing_dir_does_not_block(self):
        bridge = self.make_bridge()
        self.writes({}, fail={"x": urllib.error.URLError("connection refused")})
        content = bridge._format_injected_message(
            _message(attachments=[{"id": "x", "name": "x.bin"}])
        )
        self.assertTrue(content.endswith("Attachments:\n- x.bin: download failed (connection refused)"), content)

        with mock.patch.object(bridge_module, "ensure_private_dir", side_effect=OSError("read-only fs")):
            content = bridge._format_injected_message(
                _message(attachments=[{"id": "x", "name": "x.bin"}])
            )
        self.assertTrue(content.endswith("Attachments:\n- x.bin: download failed (read-only fs)"), content)

    def test_no_attachments_leaves_injected_text_unchanged(self):
        bridge = self.make_bridge()
        urlopen = self.writes({})
        before = _message()
        expected = (
            "[claude-peers] New incoming message\n"
            "from_alias: Proxmox\n"
            "from_id: abc12345\n"
            "from_kind: peer\n"
            "sent_at: 2026-07-22T00:00:00Z\n\n"
            "hello\n\n"
            "If a reply is needed, use the tool `claude_peers_send_message` with to_id=`abc12345`."
        )
        self.assertEqual(expected, bridge._format_injected_message(before))
        self.assertEqual(expected, bridge._format_injected_message(_message(attachments=[])))
        self.assertEqual([], urlopen.calls)
        self.assertFalse(self.attach_dir.exists())

    def test_existing_same_size_file_skips_redownload(self):
        bridge = self.make_bridge()
        urlopen = self.writes({"a1": b"same"})
        message = _message(attachments=[{"id": "a1", "name": "note.txt", "size": 4, "mime": "text/plain"}])
        bridge._format_injected_message(message)
        bridge._format_injected_message(message)
        self.assertEqual(1, len(urlopen.calls))
        self.assertEqual(30.0, urlopen.calls[0][1])
        self.assertEqual("Bearer test-token", urlopen.calls[0][2])

    def test_attachment_filename_sanitization_cannot_escape_message_dir(self):
        bridge = self.make_bridge()
        self.writes({"a1": b"X", "a2": b"Y", "a3": b"Z", "a4": b"W", "a5": b"V", "a6": b"U"})
        message = _message(
            attachments=[
                {"id": "a1", "name": "../../etc/passwd.png", "size": 1},
                {"id": "a2", "name": "..\\..\\win\\bad.txt", "size": 1},
                {"id": "a3", "name": "..", "size": 1},
                {"id": "a4", "name": "control\x00chars\n", "size": 1},
                {"id": "a5", "name": "", "size": 1},
                {"id": "a6", "name": "x" * 300, "size": 1},
            ],
        )
        content = bridge._format_injected_message(message)
        self.assertEqual(["42"], [p.name for p in self.attach_dir.iterdir()])
        msg_dir = self.attach_dir / "42"
        self.assertEqual(
            {"passwd.png", "bad.txt", "attachment", "controlchars", "x" * bridge_module.MAX_ATTACHMENT_NAME_LENGTH},
            {p.name for p in msg_dir.iterdir()},
        )
        self.assertFalse((Path(self.tempdir.name) / "etc").exists())
        self.writes({"a1": b"X"})
        bridge._format_injected_message(_message(id="../../evil", attachments=[{"id": "a1", "name": "x.bin", "size": 1}]))
        self.assertEqual({"42", "evil"}, {p.name for p in self.attach_dir.iterdir()})
        self.assertEqual((self.attach_dir / "evil" / "x.bin").read_bytes(), b"X")
        lines = [line for line in content.splitlines() if line.startswith("- ")]
        self.assertEqual(6, len(lines))
        for line in lines:
            self.assertTrue(line.startswith(f"- {msg_dir.resolve()}/"), line)
            self.assertNotIn("/../", line)

    def test_cleanup_removes_only_old_message_dirs(self):
        bridge = self.make_bridge(PEERS_BRIDGE_ATTACHMENT_KEEP_HOURS="1")
        old = self.attach_dir / "1"
        new = self.attach_dir / "2"
        for d in (old, new):
            d.mkdir(parents=True)
        past = time.time() - 10 * 3600
        os.utime(old, (past, past))
        self.assertEqual(1, bridge_module.cleanup_attachment_dirs(self.attach_dir, 1))
        self.assertFalse(old.exists())
        self.assertTrue(new.exists())

        with mock.patch.object(bridge_module, "cleanup_attachment_dirs", side_effect=OSError("boom")):
            bridge._cleanup_attachments()  # must not raise
        self.assertEqual(1, bridge_module.cleanup_attachment_dirs(self.attach_dir, 0, now=time.time() + 10 * 3600))
        self.assertFalse(new.exists())
        self.assertEqual(0, bridge_module.cleanup_attachment_dirs(self.attach_dir, 1))


class SendTests(AttachmentTestCase):
    def make_file(self, name: str, data: bytes = b"HELLO") -> Path:
        path = Path(self.tempdir.name) / name
        path.write_bytes(data)
        return path

    def uploads(self, responses=None):
        """Stub urlopen for POST /attachments; returns the mock with .requests."""
        responses = responses or {}

        def fake_urlopen(request, timeout=None):
            body = request.data
            payload = responses.get(id(body), {"id": f"id{len(calls)}", "name": "x", "size": 1, "mime": "text/plain", "sha256": "00"})
            calls.append(request)
            return io.BytesIO(json.dumps(payload).encode())

        calls = []
        patcher = mock.patch.object(bridge_module.urllib.request, "urlopen", side_effect=fake_urlopen)
        urlopen = patcher.start()
        self.addCleanup(patcher.stop)
        urlopen.requests = calls
        return urlopen

    def test_file_paths_are_uploaded_then_sent_with_ids(self):
        bridge = self.make_bridge()
        urlopen = self.uploads()
        first = self.make_file("report.pdf", b"%PDF-1.4")
        second = self.make_file("notes.txt", b"notes")

        result = bridge.send_message("peer-two", "see attached", file_paths=[str(first), str(second)])

        self.assertEqual(2, len(urlopen.requests))
        body = urlopen.requests[0].data
        self.assertIn(b'Content-Disposition: form-data; name="file"; filename="report.pdf"', body)
        self.assertIn(b"%PDF-1.4", body)
        self.assertIn(b'Content-Disposition: form-data; name="from_id"', body)
        self.assertIn(b"peer-one", body)
        self.assertTrue(urlopen.requests[0].get_header("Content-type").startswith("multipart/form-data; boundary=----peers-bridge-"))
        self.assertEqual("Bearer test-token", urlopen.requests[0].get_header("Authorization"))
        self.assertEqual("peer-one", bridge._post_json.call_args.args[1]["from_id"])
        self.assertEqual("/send-message", bridge._post_json.call_args.args[0])
        self.assertEqual(
            {
                "from_id": "peer-one",
                "to_id": "peer-two",
                "text": "see attached",
                "attachments": ["id0", "id1"],
            },
            {k: v for k, v in bridge._post_json.call_args.args[1].items() if k != "from_pid"},
        )
        self.assertEqual({"ok": True, "messages": []}, json.loads(result))  # broker /send-message payload is passed through

    def test_content_type_is_declared_only_for_certain_types(self):
        bridge = self.make_bridge()
        urlopen = self.uploads()
        bridge.send_message("peer-two", "m", file_paths=[str(self.make_file("a.png")), str(self.make_file("b.bin")), str(self.make_file("c.txt"))])
        self.assertIn(b"Content-Type: image/png\r\n", urlopen.requests[0].data)
        self.assertNotIn(b"Content-Type:", urlopen.requests[1].data.split(b"\r\n\r\n")[0])
        self.assertIn(b"Content-Type: text/plain\r\n", urlopen.requests[2].data)

    def test_missing_file_returns_error_without_upload_or_send(self):
        bridge = self.make_bridge()
        urlopen = self.uploads()
        result = bridge.send_message("peer-two", "m", file_paths=[str(Path(self.tempdir.name) / "nope.txt")])
        payload = json.loads(result)
        self.assertFalse(payload["ok"])
        self.assertIn("not found or not a regular file", payload["error"])
        self.assertEqual([], urlopen.requests)
        self.assertEqual([], [c for c in bridge._post_json.call_args_list if c.args[0] == "/send-message"])

    def test_directory_and_empty_file_are_rejected(self):
        bridge = self.make_bridge()
        urlopen = self.uploads()
        result = json.loads(bridge.send_message("peer-two", "m", file_paths=[self.tempdir.name]))
        self.assertIn("not a regular file", result["error"])
        empty = self.make_file("empty.txt", b"")
        result = json.loads(bridge.send_message("peer-two", "m", file_paths=[str(empty)]))
        self.assertIn("is empty", result["error"])
        self.assertEqual([], urlopen.requests)

    def test_too_many_files_rejected_before_any_upload(self):
        bridge = self.make_bridge()
        urlopen = self.uploads()
        paths = [str(self.make_file(f"f{i}.txt")) for i in range(6)]
        result = json.loads(bridge.send_message("peer-two", "m", file_paths=paths))
        self.assertIn("too many attachments: 6 given, at most 5 are allowed", result["error"])
        self.assertEqual([], urlopen.requests)
        self.assertEqual([], bridge._post_json.call_args_list)  # rejected before any broker call
        bridge.send_message("peer-two", "m", file_paths=paths[:5])
        self.assertEqual(5, len(urlopen.requests))

    def test_oversized_file_rejected_before_upload(self):
        bridge = self.make_bridge()
        urlopen = self.uploads()
        big = self.make_file("big.bin", b"x" * 2048)
        with mock.patch.object(bridge_module, "MAX_ATTACHMENT_BYTES", 1024):
            result = json.loads(bridge.send_message("peer-two", "m", file_paths=[str(big)]))
        self.assertIn("2048 bytes; the broker limit is 1024 bytes", result["error"])
        self.assertEqual([], urlopen.requests)

    def test_upload_failure_does_not_send(self):
        bridge = self.make_bridge()
        urlopen = self.uploads()
        urlopen.side_effect = urllib.error.HTTPError(
            "http://broker/attachments", 400, "Bad Request", {}, io.BytesIO(b'{"error":"invalid_request","message":"type not allowed"}')
        )
        ok = self.make_file("a.txt")
        result = json.loads(bridge.send_message("peer-two", "m", file_paths=[str(ok)]))
        self.assertFalse(result["sent"])
        self.assertIn("attachment upload failed for", result["error"])
        self.assertIn("HTTP 400", result["error"])
        self.assertEqual([], [c for c in bridge._post_json.call_args_list if c.args[0] == "/send-message"])

        urlopen.side_effect = urllib.error.URLError("timeout")
        result = json.loads(bridge.send_message("peer-two", "m", file_paths=[str(ok)]))
        self.assertIn("timeout", result["error"])

    def test_upload_without_id_is_an_error(self):
        bridge = self.make_bridge()
        urlopen = self.uploads()
        urlopen.side_effect = None
        urlopen.return_value = io.BytesIO(b'{"error":"nope"}')
        result = json.loads(bridge.send_message("peer-two", "m", file_paths=[str(self.make_file("a.txt"))]))
        self.assertIn("returned no id", result["error"])

    def test_single_string_path_and_bad_types(self):
        bridge = self.make_bridge()
        urlopen = self.uploads()
        bridge.send_message("peer-two", "m", file_paths=str(self.make_file("one.txt")))
        self.assertEqual(["id0"], bridge._post_json.call_args.args[1]["attachments"])
        result = json.loads(bridge.send_message("peer-two", "m", file_paths=[42]))
        self.assertIn("array of local file path strings", result["error"])
        self.assertEqual(1, len(urlopen.requests))

    def test_send_without_file_paths_is_unchanged(self):
        bridge = self.make_bridge()
        urlopen = self.uploads()
        result = bridge.send_message("peer-two", "plain")
        self.assertEqual({"ok": True, "messages": []}, json.loads(result))
        self.assertEqual([], urlopen.requests)
        self.assertEqual(
            {"from_id": "peer-one", "to_id": "peer-two", "text": "plain"},
            {k: v for k, v in bridge._post_json.call_args.args[1].items() if k != "from_pid"},
        )

    def test_schema_declares_optional_file_paths(self):
        props = schemas.CLAUDE_PEERS_SEND_MESSAGE["parameters"]["properties"]
        self.assertEqual(["to_id", "message"], schemas.CLAUDE_PEERS_SEND_MESSAGE["parameters"]["required"])
        self.assertEqual("array", props["file_paths"]["type"])
        self.assertEqual("string", props["file_paths"]["items"]["type"])
        self.assertIn("file_paths", schemas.CLAUDE_PEERS_SEND_MESSAGE["description"])

    def test_cli_send_accepts_repeated_file_flags(self):
        import argparse

        import peers_bridge as plugin

        parser = argparse.ArgumentParser()
        subparser = parser.add_subparsers().add_parser("claude-peers")
        plugin._setup_cli(subparser)
        args = parser.parse_args(["claude-peers", "send", "peer-two", "hi", "--file", "a.txt", "-f", "b.txt"])
        self.assertEqual(["a.txt", "b.txt"], args.files)
        args = parser.parse_args(["claude-peers", "send", "peer-two", "hi"])
        self.assertEqual([], args.files)


if __name__ == "__main__":
    unittest.main()
