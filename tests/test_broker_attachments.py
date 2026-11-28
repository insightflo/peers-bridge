"""Attachment upload, download, message references and GC in broker.remote-hub.ts.

Spawns the broker with bun on a free port and a temp DB (same pattern as
tests/test_broker_openbot_host.py). Multipart bodies are built by hand because
urllib has no multipart helper. The GC interval is set to 1s so expiry can be
observed within a test; the broker's SQLite is in WAL mode so a second
connection can age rows while the broker runs.
"""
from __future__ import annotations

import hashlib
import json
import os
import socket
import sqlite3
import subprocess
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
BROKER_PATH = REPO_ROOT / "broker.remote-hub.ts"
TOKEN = "test-hub-token"
LIVE_PID = os.getpid()  # Alive for the whole run so the local stale check never reaps peers.
PNG_BYTES = b"\x89PNG\r\n\x1a\n" + b"\x00\x00\x00\rIHDR" + b"\x00" * 20 + b"payload"
BOUNDARY = "----peers-bridge-test"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def multipart(*parts: tuple[str, str | None, str | None, bytes], boundary: str = BOUNDARY) -> tuple[bytes, str]:
    """Build a multipart/form-data body from (field, filename, content_type, data) tuples."""
    chunks: list[bytes] = []
    for field, filename, content_type, data in parts:
        disposition = f'form-data; name="{field}"' + (
            f'; filename="{filename.replace(chr(34), chr(92) + chr(34))}"' if filename is not None else "")
        headers = f"Content-Disposition: {disposition}\r\n"
        if content_type:
            headers += f"Content-Type: {content_type}\r\n"
        chunks.append(f"--{boundary}\r\n{headers}\r\n".encode() + data + b"\r\n")
    chunks.append(f"--{boundary}--\r\n".encode())
    return b"".join(chunks), f"multipart/form-data; boundary={boundary}"


class BrokerAttachmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "peers.db"
        self.attachment_dir = Path(self.tempdir.name) / "attachments"
        self._start_broker()

    def _start_broker(self, **extra_env: str) -> None:
        self.port = _free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        env = os.environ.copy()
        env.update(
            {
                "CLAUDE_PEERS_PORT": str(self.port),
                "CLAUDE_PEERS_DB": str(self.db_path),
                "CLAUDE_PEERS_HUB_TOKEN": TOKEN,
                "CLAUDE_PEERS_STALE_TIMEOUT_MS": "60000",
                "CLAUDE_PEERS_ATTACHMENT_MAX_BYTES": "1000000",
                "CLAUDE_PEERS_ATTACHMENT_DIR": str(self.attachment_dir),
                "CLAUDE_PEERS_ATTACHMENT_GC_SECONDS": "1",
            }
        )
        env.update(extra_env)
        self.process = subprocess.Popen(
            ["bun", str(BROKER_PATH)],
            cwd=REPO_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.time() + 5
        while time.time() < deadline:
            if self.process.poll() is not None:
                stdout, stderr = self.process.communicate()
                self.fail(f"broker exited early\nstdout={stdout}\nstderr={stderr}")
            try:
                urllib.request.urlopen(f"{self.base_url}/health", timeout=0.2).read()
                break
            except Exception:
                time.sleep(0.05)
        else:
            self.fail("broker did not become healthy")

    def _stop_broker(self) -> None:
        self.process.terminate()
        try:
            self.process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.communicate()

    def tearDown(self) -> None:
        self._stop_broker()
        self.tempdir.cleanup()

    # --- helpers ---

    def raw(self, method: str, path: str, data: bytes | None = None, headers: dict | None = None,
            auth: bool = True) -> tuple[int, dict, bytes]:
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=data,
            method=method,
            headers={"Authorization": f"Bearer {TOKEN}" if auth else "", **(headers or {})},
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, dict(response.headers), response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers), exc.read()

    def request(self, method: str, path: str, body=None, expected_status: int = 200):
        data = json.dumps(body).encode() if body is not None else None
        status, _, payload = self.raw(method, path, data, {"Content-Type": "application/json"})
        parsed = json.loads(payload.decode())
        self.assertEqual(expected_status, status, parsed)
        return parsed

    def post(self, path: str, body: dict, expected_status: int = 200):
        return self.request("POST", path, body, expected_status)

    def upload(self, name: str = "pic.png", content: bytes = PNG_BYTES, content_type: str | None = "image/png",
               path: str = "/attachments", expected_status: int = 201) -> dict:
        body, content_type_header = multipart(("file", name, content_type, content))
        status, _, payload = self.raw("POST", path, body, {"Content-Type": content_type_header})
        parsed = json.loads(payload.decode())
        self.assertEqual(expected_status, status, parsed)
        return parsed

    def register(self, alias: str) -> str:
        return self.post(
            "/register",
            {
                "pid": LIVE_PID,
                "parent_pid": 1,
                "logical_name": alias,
                "cwd": f"/peer/{alias}",
                "git_root": None,
                "tty": None,
                "summary": f"peer {alias}",
            },
        )["id"]

    def db_connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=5)
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def wait_until(self, predicate, timeout: float = 6.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(0.1)
        return predicate()

    # --- tests ---

    def test_upload_png_stores_file_and_metadata(self) -> None:
        uploaded = self.upload()
        self.assertEqual("pic.png", uploaded["name"])
        self.assertEqual(len(PNG_BYTES), uploaded["size"])
        self.assertEqual("image/png", uploaded["mime"])
        self.assertEqual(hashlib.sha256(PNG_BYTES).hexdigest(), uploaded["sha256"])
        self.assertRegex(uploaded["id"], r"^[0-9a-f]{32}$")
        stored = self.attachment_dir / uploaded["id"]
        self.assertTrue(stored.exists(), "blob must be written as <attachment_dir>/<id>")
        self.assertEqual(PNG_BYTES, stored.read_bytes())
        rows = self.db_connect().execute(
            "SELECT name, size, mime, sha256 FROM attachments WHERE id = ?", (uploaded["id"],)
        ).fetchall()
        self.assertEqual([("pic.png", len(PNG_BYTES), "image/png", uploaded["sha256"])], rows)

    def test_upload_accepts_mixed_case_boundary_like_browsers(self) -> None:
        # Chrome/Firefox send "----WebKitFormBoundaryA1b2C3d4"-style boundaries; the
        # boundary is case-sensitive and must not be lower-cased with the media type.
        body, content_type = multipart(
            ("file", "pic.png", "image/png", PNG_BYTES),
            boundary="----WebKitFormBoundaryA1b2C3d4E5F6",
        )
        status, _, payload = self.raw("POST", "/attachments", body, {"Content-Type": content_type})
        self.assertEqual(201, status, payload.decode())
        self.assertEqual("image/png", json.loads(payload.decode())["mime"])

    def test_upload_works_under_dashboard_prefix_and_api_alias(self) -> None:
        for path in ("/dashboard/api/attachments", "/api/attachments"):
            with self.subTest(path=path):
                uploaded = self.upload(path=path)
                self.assertEqual("image/png", uploaded["mime"])

    def test_upload_requires_hub_token(self) -> None:
        body, content_type = multipart(("file", "pic.png", "image/png", PNG_BYTES))
        status, _, _ = self.raw("POST", "/attachments", body, {"Content-Type": content_type}, auth=False)
        self.assertEqual(401, status)

    def test_upload_mime_rules(self) -> None:
        for disallowed in ("application/x-sh", "application/x-executable", "application/x-mach-binary",
                           "application/x-dosexec", "application/vnd.microsoft.portable-executable"):
            with self.subTest(mime=disallowed):
                payload = self.upload(content_type=disallowed, expected_status=400)
                self.assertEqual("invalid_request", payload["error"])
        for allowed, expected in (("Application/PDF; charset=binary", "application/pdf"), ("text/csv", "text/csv"),
                                  ("audio/mpeg", "audio/mpeg"), ("video/mp4", "video/mp4"),
                                  ("application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                                   "application/vnd.openxmlformats-officedocument.wordprocessingml.document")):
            with self.subTest(mime=allowed):
                self.assertEqual(expected, self.upload(content_type=allowed)["mime"])

    def test_upload_infers_mime_from_extension_and_defaults_to_octet_stream(self) -> None:
        self.assertEqual("image/jpeg", self.upload(name="photo.JPG", content_type=None)["mime"])
        self.assertEqual("application/pdf", self.upload(name="doc.pdf", content_type=None)["mime"])
        self.assertEqual("text/plain", self.upload(name="notes.md", content_type=None)["mime"])
        self.assertEqual("application/zip", self.upload(name="bundle.zip", content_type=None)["mime"])
        self.assertEqual("application/octet-stream", self.upload(name="no-extension", content_type=None)["mime"])
        self.assertEqual("application/octet-stream", self.upload(name="weird.xyz", content_type=None)["mime"])

    def test_upload_sanitizes_name(self) -> None:
        self.assertEqual("evil.png", self.upload(name="../../etc/evil.png")["name"])
        self.assertEqual("evil.png", self.upload(name="C:\\temp\\evil.png")["name"])
        self.assertEqual("a.png", self.upload(name="a\x00\x01\x7f.png")["name"])
        self.assertEqual("file", self.upload(name="   ")["name"])
        self.assertEqual(128, len(self.upload(name="x" * 500 + ".png")["name"]))

    def test_upload_rejects_bad_requests(self) -> None:
        no_file, content_type = multipart(("other", "pic.png", "image/png", PNG_BYTES))
        status, _, payload = self.raw("POST", "/attachments", no_file, {"Content-Type": content_type})
        self.assertEqual(400, status)
        self.assertEqual("invalid_request", json.loads(payload)["error"])
        self.assertEqual("invalid_request", self.upload(content=b"", expected_status=400)["error"])
        status, _, _ = self.raw("POST", "/attachments", b"{}", {"Content-Type": "application/json"})
        self.assertEqual(400, status)
        status, _, _ = self.raw("POST", "/attachments", b"garbage", {"Content-Type": "multipart/form-data"})
        self.assertEqual(400, status)

    def test_upload_rejects_file_over_max_bytes(self) -> None:
        self._stop_broker()
        self._start_broker(CLAUDE_PEERS_ATTACHMENT_MAX_BYTES="10")
        payload = self.upload(content=b"x" * 11, expected_status=400)
        self.assertEqual("invalid_request", payload["error"])
        self.assertEqual(10, self.upload(content=b"x" * 10)["size"])

    def test_download_streams_file_with_headers(self) -> None:
        uploaded = self.upload(name="report.pdf", content=b"%PDF-1.4 body", content_type="application/pdf")
        status, headers, body = self.raw("GET", f"/attachments/{uploaded['id']}")
        self.assertEqual(200, status)
        self.assertEqual(b"%PDF-1.4 body", body)
        self.assertEqual("application/pdf", headers["Content-Type"])
        self.assertEqual('attachment; filename="report.pdf"', headers["Content-Disposition"])
        status, head_headers, head_body = self.raw("HEAD", f"/attachments/{uploaded['id']}")
        self.assertEqual(200, status)
        self.assertEqual("", head_body.decode())
        self.assertEqual(headers["Content-Disposition"], head_headers["Content-Disposition"])
        self.assertEqual(headers["Content-Type"], head_headers["Content-Type"])

    def test_download_quotes_filename_and_requires_auth(self) -> None:
        uploaded = self.upload(name='quote"name.png')
        status, headers, _ = self.raw("GET", f"/attachments/{uploaded['id']}")
        self.assertEqual(200, status)
        self.assertEqual('attachment; filename="quote\\"name.png"', headers["Content-Disposition"])
        status, _, _ = self.raw("GET", f"/attachments/{uploaded['id']}", auth=False)
        self.assertEqual(401, status)
        status, _, _ = self.raw("GET", f"/attachments/{uploaded['id']}?token={TOKEN}", auth=False)
        self.assertEqual(200, status)

    def test_download_unknown_id_is_404(self) -> None:
        for path in ("/attachments/" + "0" * 32, "/attachments/not-an-id", "/attachments/..%2Fpeers.db"):
            with self.subTest(path=path):
                status, _, payload = self.raw("GET", path)
                self.assertEqual(404, status)
                self.assertEqual("attachment not found", json.loads(payload)["error"])
        missing_blob = self.upload()
        (self.attachment_dir / missing_blob["id"]).unlink()
        status, _, payload = self.raw("GET", f"/attachments/{missing_blob['id']}")
        self.assertEqual(404, status)
        self.assertEqual("attachment not found", json.loads(payload)["error"])

    def test_send_message_with_attachment_is_polled_with_metadata(self) -> None:
        sender, receiver = self.register("sender"), self.register("receiver")
        uploaded = self.upload(name="shot.png")
        result = self.post("/send-message", {"from_id": sender, "to_id": "receiver", "text": "see attached",
                                             "attachments": [uploaded["id"]]})
        self.assertTrue(result["ok"])
        polled = self.post("/poll-messages", {"id": receiver})
        self.assertEqual(1, len(polled["messages"]))
        message = polled["messages"][0]
        self.assertEqual([{"id": uploaded["id"], "name": "shot.png", "size": len(PNG_BYTES), "mime": "image/png"}],
                         message["attachments"])
        # Messages without attachments project an empty array rather than null.
        self.post("/send-message", {"from_id": sender, "to_id": "receiver", "text": "plain"})
        plain = self.post("/poll-messages", {"id": receiver})["messages"][0]
        self.assertEqual("plain", plain["text"])
        self.assertEqual([], plain["attachments"])

    def test_dashboard_history_and_quick_send_include_attachments(self) -> None:
        sender, receiver = self.register("sender"), self.register("receiver")
        uploaded = self.upload(name="shot.png")
        self.post("/send-message", {"from_id": sender, "to_id": "receiver", "text": "peer message",
                                    "attachments": [uploaded["id"]]})
        self.request("POST", "/api/messages", {"to_id": sender, "text": "dashboard message",
                                               "attachments": [uploaded["id"]]}, 201)
        history = self.request("GET", "/api/messages")["messages"]
        self.assertEqual({"peer message", "dashboard message"}, {m["text"] for m in history})
        for message in history:
            self.assertEqual(uploaded["id"], message["attachments"][0]["id"])
            self.assertEqual("shot.png", message["attachments"][0]["name"])
        snapshot = self.request("GET", "/api/snapshot")
        self.assertEqual(2, len(snapshot["recent_messages"]))
        self.assertTrue(all(m["attachments"][0]["id"] == uploaded["id"] for m in snapshot["recent_messages"]))

    def test_send_message_attachment_validation(self) -> None:
        sender, receiver = self.register("sender"), self.register("receiver")
        uploaded = self.upload()
        payload = self.request("POST", "/send-message", {"from_id": sender, "to_id": "receiver", "text": "x",
                                                           "attachments": [uploaded["id"], "deadbeef" * 4]}, 404)
        self.assertEqual({"error": "attachment not found", "id": "deadbeef" * 4}, payload)
        for bad in ([uploaded["id"]] * 6, "not-a-list", [1], [""]):
            with self.subTest(attachments=bad):
                payload = self.request("POST", "/send-message", {"from_id": sender, "to_id": "receiver", "text": "x",
                                                                   "attachments": bad}, 400)
                self.assertEqual("invalid_request", payload["error"])
        self.assertEqual([], self.post("/poll-messages", {"id": receiver})["messages"])

    def test_gc_deletes_old_unreferenced_attachment_only(self) -> None:
        sender, receiver = self.register("sender"), self.register("receiver")
        referenced = self.upload(name="referenced.png")
        orphan = self.upload(name="orphan.png")
        self.post("/send-message", {"from_id": sender, "to_id": "receiver", "text": "keep", "attachments": [referenced["id"]]})
        time.sleep(1.5)  # At least one GC cycle: fresh uploads must survive.
        self.assertEqual(200, self.raw("GET", f"/attachments/{orphan['id']}")[0])

        with self.db_connect() as connection:
            connection.execute("UPDATE attachments SET created_at = ? WHERE id = ?",
                               ("2020-01-01T00:00:00.000Z", orphan["id"]))

        def orphan_gone() -> bool:
            return self.raw("GET", f"/attachments/{orphan['id']}")[0] == 404
        self.assertTrue(self.wait_until(orphan_gone), "attachment unreferenced for >24h must be collected")
        self.assertFalse((self.attachment_dir / orphan["id"]).exists(), "blob must be removed with the row")
        with self.db_connect() as connection:
            self.assertEqual([], connection.execute("SELECT id FROM attachments WHERE id = ?", (orphan["id"],)).fetchall())
        self.assertEqual(200, self.raw("GET", f"/attachments/{referenced['id']}")[0])

    def test_gc_deletes_attachments_whose_messages_passed_retention(self) -> None:
        sender, receiver = self.register("sender"), self.register("receiver")
        old = self.upload(name="old.png")
        recent = self.upload(name="recent.png")
        self.post("/send-message", {"from_id": sender, "to_id": "receiver", "text": "old", "attachments": [old["id"]]})
        self.post("/send-message", {"from_id": sender, "to_id": "receiver", "text": "recent", "attachments": [recent["id"]]})
        with self.db_connect() as connection:
            connection.execute("UPDATE messages SET sent_at = ? WHERE text = 'old'", ("2020-01-01T00:00:00.000Z",))

        def old_gone() -> bool:
            return self.raw("GET", f"/attachments/{old['id']}")[0] == 404
        self.assertTrue(self.wait_until(old_gone), "attachment of an expired message must be collected")
        self.assertEqual(200, self.raw("GET", f"/attachments/{recent['id']}")[0])

    def test_schema_gains_attachments_table_and_column(self) -> None:
        with self.db_connect() as connection:
            columns = {row[1] for row in connection.execute("PRAGMA table_info(messages)")}
            self.assertIn("attachments", columns)
            table = connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'attachments'").fetchall()
            self.assertEqual([("attachments",)], table)
            column_info = {row[1]: row[2].upper() for row in connection.execute("PRAGMA table_info(attachments)")}
            self.assertEqual({"id": "TEXT", "name": "TEXT", "size": "INTEGER", "mime": "TEXT", "sha256": "TEXT",
                              "uploaded_by": "TEXT", "created_at": "TEXT"}, column_info)


if __name__ == "__main__":
    unittest.main()
