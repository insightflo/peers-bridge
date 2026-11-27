from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from tests.test_broker_aliases import BROKER_PATH, REPO_ROOT, TOKEN, _create_legacy_db, _free_port


class BrokerPushCrudTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "peers.db"
        _create_legacy_db(self.db_path)
        self.streams = []
        self.process = None
        self.start_broker()

    def start_broker(self, token=TOKEN):
        self.port = _free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        env = dict(os.environ, CLAUDE_PEERS_PORT=str(self.port),
                   CLAUDE_PEERS_DB=str(self.db_path), CLAUDE_PEERS_HUB_TOKEN=token,
                   CLAUDE_PEERS_STALE_TIMEOUT_MS="60000")
        self.process = subprocess.Popen(["bun", str(BROKER_PATH)], cwd=REPO_ROOT,
                                        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.fail(f"broker exited: {self.process.communicate()!r}")
            try:
                with urllib.request.urlopen(self.base + "/health", timeout=0.2):
                    return
            except OSError:
                time.sleep(0.02)
        self.fail("broker did not become healthy")

    def stop_broker(self):
        if self.process:
            self.process.terminate()
            try:
                self.process.communicate(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.communicate(timeout=2)
            self.process = None

    def tearDown(self):
        for stream in self.streams:
            stream.close()
        self.stop_broker()
        self.tempdir.cleanup()

    def request(self, method, path, body=None, status=200, *, auth=True, headers=None, raw=None):
        request_headers = {"Authorization": f"Bearer {TOKEN}"} if auth else {}
        if body is not None:
            request_headers["Content-Type"] = "application/json"
        request_headers.update(headers or {})
        data = raw if raw is not None else json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(self.base + path, data=data,
                                         headers=request_headers, method=method)
        try:
            response = urllib.request.urlopen(request, timeout=2)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            content = response.read()
            self.assertEqual(status, response.status, content)
            result_headers = response.headers
        if path.startswith("/api/"):
            self.assertEqual("no-store", result_headers.get("Cache-Control"))
        if result_headers.get_content_type() == "application/json":
            return json.loads(content)
        return content, result_headers

    def post(self, path, body, status=200, **kwargs):
        return self.request("POST", path, body, status, **kwargs)

    def create(self, alias="Node", **fields):
        return self.post("/api/peers", dict(alias=alias, **fields), 201)

    def register(self, alias="Node", pid=1234567, cwd="/node"):
        return self.post("/register", {"logical_name": alias, "pid": pid, "cwd": cwd,
                                      "git_root": "/git", "tty": "tty1", "summary": "running"})

    def peers(self):
        return self.request("GET", "/api/peers")["peers"]

    def snapshot(self):
        return self.request("GET", "/api/snapshot")

    def sql(self, query, args=()):
        with sqlite3.connect(self.db_path) as db:
            return db.execute(query, args).fetchall()

    def stream(self, path, *, query_auth=False):
        if query_auth:
            path += ("&" if "?" in path else "?") + "token=" + TOKEN
        request = urllib.request.Request(self.base + path,
            headers={} if query_auth else {"Authorization": f"Bearer {TOKEN}"})
        response = urllib.request.urlopen(request, timeout=2)
        self.streams.append(response)
        self.assertEqual("text/event-stream", response.headers.get_content_type())
        self.assertEqual("no-store" if path.startswith("/api/") else "no-cache",
                         response.headers.get("Cache-Control"))
        self.assertEqual("keep-alive", response.headers.get("Connection"))
        return response

    def frame(self, stream):
        lines = []
        while True:
            line = stream.readline()
            self.assertTrue(line, "SSE stream closed before a complete frame")
            if line == b"\n":
                return b"".join(lines).decode()
            lines.append(line)

    def event(self, stream, name):
        frame = self.frame(stream)
        lines = frame.splitlines()
        self.assertEqual(f"event: {name}", lines[0], frame)
        return json.loads(lines[1].removeprefix("data: "))

    def test_additive_schema_defaults_and_restart_preserve_managed_peer(self):
        peer = self.create()
        self.assertEqual((1, "peer", "offline", 0),
                         (peer["managed"], peer["role"], peer["status"], peer["pid"]))
        columns = {row[1] for row in self.sql("PRAGMA table_info(peers)")}
        self.assertTrue({"managed", "role", "status"}.issubset(columns))
        self.stop_broker()
        self.start_broker()
        self.assertEqual(peer, self.peers()[0])
        runtime = self.register("Runtime")
        row = next(p for p in self.peers() if p["id"] == runtime["id"])
        self.assertEqual((0, "peer", "online"), (row["managed"], row["role"], row["status"]))

    def test_crud_lifecycle_and_field_validation(self):
        peer = self.create("  Ｎode  ", role="gateway", summary="planned", cwd="/old")
        self.assertEqual("Node", peer["alias"])
        self.assertEqual(0, peer["undelivered_count"])
        expected = {"id", "alias", "pid", "cwd", "git_root", "tty", "summary", "role",
                    "managed", "status", "registered_at", "last_seen", "undelivered_count"}
        self.assertEqual(expected, set(self.peers()[0]))
        updated = self.request("PATCH", "/api/peers/" + peer["id"],
            {"alias": "Renamed", "role": "service", "summary": "new", "cwd": "/new", "managed": False})
        self.assertEqual(("Renamed", "service", "new", "/new", 0),
                         tuple(updated[k] for k in ("alias", "role", "summary", "cwd", "managed")))
        for role in ("peer", "gateway", "cli", "service"):
            self.request("PATCH", "/api/peers/" + peer["id"], {"role": role})
        self.request("DELETE", "/api/peers/" + peer["id"])
        self.assertEqual([], self.peers())
        self.request("PATCH", "/api/peers/missing", {}, 404)
        self.request("DELETE", "/api/peers/missing", status=404)
        for body in ({}, {"alias": None}, {"alias": ""}, {"alias": "é"},
                     {"alias": 4}, {"alias": "Node", "role": "invalid"},
                     {"alias": "Node", "summary": None}, {"alias": "Node", "cwd": 3}):
            self.post("/api/peers", body, 400)

    def test_conflicts_and_invalid_patch_preserve_peer(self):
        a = self.create("First")
        b = self.create("Second")
        self.post("/api/peers", {"alias": "FIRST"}, 409)
        self.post("/api/peers", {"alias": a["id"]}, 409)
        for patch, status in (({"alias": "first"}, 409), ({"alias": a["id"]}, 409),
                              ({"alias": None}, 400), ({"role": "admin"}, 400),
                              ({"managed": "yes"}, 400), ({"cwd": None}, 400)):
            self.request("PATCH", "/api/peers/" + b["id"], patch, status)
        self.assertEqual(b, next(p for p in self.peers() if p["id"] == b["id"]))

    def test_managed_expiry_reclaims_same_id_and_preserves_pending_messages(self):
        peer = self.create(role="gateway")
        self.post("/api/messages", {"to_id": "node", "text": "queued"}, 201)
        registered = self.register()
        self.assertEqual(peer["id"], registered["id"])
        self.post("/register", {"logical_name": "node", "pid": 2345678, "cwd": "/other"}, 409)
        self.sql("UPDATE peers SET last_seen = '2000-01-01T00:00:00Z' WHERE id = ?", (peer["id"],))
        offline = self.peers()[0]
        self.assertEqual(("offline", 0, 1), (offline["status"], offline["pid"], offline["undelivered_count"]))
        reclaimed = self.register("NODE", pid=2345678, cwd="/other")
        self.assertEqual(peer["id"], reclaimed["id"])
        online = self.peers()[0]
        self.assertEqual(("online", 2345678, "/other", "/git", "tty1", "running", "gateway"),
            tuple(online[k] for k in ("status", "pid", "cwd", "git_root", "tty", "summary", "role")))
        self.assertEqual(peer["registered_at"], online["registered_at"])
        self.assertEqual(1, online["undelivered_count"])

    def test_unmanaged_expiry_deletes_pending_but_keeps_delivered_history(self):
        peer = self.register()
        first = self.post("/api/messages", {"to_id": peer["id"], "text": "history"}, 201)
        self.post("/mark-message-delivered", {"id": first["id"]})
        self.post("/api/messages", {"to_id": peer["id"], "text": "pending"}, 201)
        self.sql("UPDATE peers SET last_seen = 'invalid' WHERE id = ?", (peer["id"],))
        self.assertEqual([], self.peers())
        self.assertEqual([("history", 1)], self.sql("SELECT text, delivered FROM messages"))

    def test_local_liveness_preserves_offline_and_nonpositive_pid_rows(self):
        self.stop_broker()
        self.start_broker(token="")
        offline = self.create("Offline")
        managed = self.create("Managed")
        self.register("Managed", pid=os.getpid())
        dead = subprocess.Popen(["true"])
        dead.wait()
        self.sql("UPDATE peers SET pid = ? WHERE id = ?", (dead.pid, managed["id"]))
        unmanaged = self.register("Dead", pid=dead.pid)
        zero = self.register("Zero", pid=0)
        negative = self.register("Negative", pid=-1)
        rows = {p["id"]: p for p in self.peers()}
        self.assertEqual("offline", rows[managed["id"]]["status"])
        self.assertIn(offline["id"], rows)
        self.assertIn(zero["id"], rows)
        self.assertIn(negative["id"], rows)
        self.assertNotIn(unmanaged["id"], rows)
        listed = self.post("/list-peers", {"scope": "machine", "cwd": "/"})
        self.assertEqual(set(rows), {p["id"] for p in listed})

    def test_poll_peek_explicit_ack_and_default_delivery(self):
        peer = self.create()
        message = self.post("/api/messages", {"to_id": "NODE", "text": "hello"}, 201)
        for _ in range(2):
            inbox = self.post("/poll-messages", {"id": peer["id"], "mark_delivered": False})["messages"]
            self.assertEqual([message["id"]], [m["id"] for m in inbox])
        self.assertEqual(1, self.snapshot()["stats"]["messages_undelivered"])
        self.post("/mark-message-delivered", {"id": message["id"]})
        self.assertEqual([], self.post("/poll-messages", {"id": peer["id"]})["messages"])
        self.post("/api/messages", {"to_id": peer["id"], "text": "default"}, 201)
        self.assertEqual(1, len(self.post("/poll-messages", {"id": peer["id"]})["messages"]))
        self.assertEqual([], self.post("/poll-messages", {"id": peer["id"]})["messages"])
        self.assertEqual(1, self.snapshot()["stats"]["delivery_rate"])
        self.post("/mark-message-delivered", {"id": "1"}, 400)

    def test_sse_wake_is_a_hint_and_delete_revokes_all_streams(self):
        peer = self.register()
        streams = [self.stream("/events?id=" + peer["id"], query_auth=bool(i)) for i in range(2)]
        for stream in streams:
            self.assertEqual(": connected\n", self.frame(stream))
        self.post("/send-message", {"from_id": "cli", "from_pid": 1,
                                   "to_id": "node", "text": "wake"})
        for stream in streams:
            self.assertEqual({"type": "messages", "peer_id": peer["id"]}, self.event(stream, "wake"))
        self.assertEqual(1, self.snapshot()["stats"]["messages_undelivered"])
        self.request("DELETE", "/api/peers/" + peer["id"], status=409)
        self.request("DELETE", "/api/peers/" + peer["id"] + "?force=true", status=409)
        self.request("DELETE", "/api/peers/" + peer["id"] + "?force=1")
        for stream in streams:
            self.assertEqual({"reason": "deleted", "peer_id": peer["id"]}, self.event(stream, "revoked"))
            self.assertEqual(b"", stream.read())
        self.assertEqual(0, self.snapshot()["hub"]["sse_peer_clients"])
        self.assertEqual([], self.sql("SELECT * FROM messages"))
        self.request("GET", "/events?id=" + peer["id"], status=410)
        self.assertEqual({"error": "peer_revoked"}, self.post("/heartbeat", {"id": peer["id"]}, 410))
        self.assertEqual({"ok": False, "reregister": True}, self.post("/heartbeat", {"id": "unknown"}))
        self.request("GET", "/events", status=404)
        self.request("GET", "/events?id=unknown", status=404)

    def test_dashboard_stream_broadcasts_snapshot_message_delivery_and_peer_changes(self):
        stream = self.stream("/api/stream", query_auth=True)
        initial = self.event(stream, "snapshot")
        self.assertEqual(1, initial["hub"]["sse_dashboard_clients"])
        peer = self.create()
        self.assertEqual(peer["id"], self.event(stream, "snapshot")["peers"][0]["id"])
        peer_stream = self.stream("/events?id=" + peer["id"])
        self.frame(peer_stream)
        message = self.post("/api/messages", {"to_id": "node", "text": "dashboard"}, 201)
        self.event(peer_stream, "wake")
        pushed = self.event(stream, "message")
        self.assertEqual(("dashboard", "Dashboard", "system", peer["id"], "Node", message["id"]),
            tuple(pushed[k] for k in ("from_id", "from_alias", "from_kind", "to_id", "to_alias", "id")))
        snap = self.event(stream, "snapshot")
        self.assertEqual((1, 0, 1, 1, 1, 1, 0), tuple(snap["stats"][k] for k in
            ("peers_total", "peers_online", "peers_offline", "messages_total", "messages_undelivered", "msg_per_min", "delivery_rate")))
        self.assertEqual(1, snap["topology_edges"][0]["count"])
        self.assertEqual(1, snap["topology_edges"][0]["undelivered"])
        self.assertEqual(pushed, snap["recent_messages"][0])
        self.post("/mark-message-delivered", {"id": message["id"]})
        self.assertEqual(0, self.event(stream, "snapshot")["stats"]["messages_undelivered"])
        self.request("PATCH", "/api/peers/" + peer["id"], {"summary": "updated"})
        self.assertEqual("updated", self.event(stream, "snapshot")["peers"][0]["summary"])
        self.register()
        self.assertEqual("online", self.event(stream, "snapshot")["peers"][0]["status"])
        self.post("/send-message", {"from_id": peer["id"], "to_id": peer["id"], "text": "peer"})
        self.assertEqual("peer", self.event(stream, "message")["from_kind"])
        self.event(stream, "snapshot")
        self.sql("UPDATE peers SET last_seen = '2000-01-01T00:00:00Z'")
        self.peers()
        self.assertEqual("offline", self.event(stream, "snapshot")["peers"][0]["status"])

    def test_managed_online_registration_retry_keeps_identity(self):
        peer = self.create()
        first = self.register()
        second = self.register()
        self.assertEqual(peer["id"], first["id"])
        self.assertEqual(first, second)
        self.assertEqual(1, self.peers()[0]["managed"])

    def test_sse_keepalives_follow_configured_intervals(self):
        peer = self.create()
        started = time.monotonic()
        stream = self.stream("/events?id=" + peer["id"])
        self.frame(stream)
        dashboard = self.stream("/api/stream")
        self.event(dashboard, "snapshot")
        # Only this test waits for the real server timers; other SSE reads fail fast.
        for response in (stream, dashboard):
            response.fp.raw._sock.settimeout(30)
        self.assertEqual(": keepalive\n", self.frame(dashboard))
        self.assertGreaterEqual(time.monotonic() - started, 18)
        self.assertEqual(": keepalive\n", self.frame(stream))
        self.assertGreaterEqual(time.monotonic() - started, 23)

    def test_stream_disconnect_removes_subscribers(self):
        peer = self.create()
        stream = self.stream("/events?id=" + peer["id"])
        self.frame(stream)
        dashboard = self.stream("/api/stream")
        self.event(dashboard, "snapshot")
        self.assertEqual(1, self.snapshot()["hub"]["sse_peer_clients"])
        stream.close()
        dashboard.close()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            hub = self.snapshot()["hub"]
            if hub["sse_peer_clients"] == hub["sse_dashboard_clients"] == 0:
                break
            time.sleep(0.02)
        else:
            self.fail(f"subscribers leaked: {hub}")

    def test_unknown_dashboard_target_does_not_insert_and_recent_messages_limit(self):
        peer = self.create()
        self.post("/api/messages", {"to_id": "unknown", "text": "lost"}, 404)
        self.assertEqual([], self.sql("SELECT * FROM messages"))
        for i in range(102):
            self.post("/api/messages", {"to_id": peer["id"], "text": str(i)}, 201)
        snap = self.snapshot()
        self.assertEqual(100, len(snap["recent_messages"]))
        self.assertEqual("101", snap["recent_messages"][0]["text"])
        self.assertEqual(102, snap["topology_edges"][0]["count"])
        self.assertEqual(102, snap["stats"]["messages_total"])

    def test_auth_requires_bearer_except_public_shells_and_sse_query(self):
        self.request("GET", "/health", auth=False)
        html, headers = self.request("GET", "/dashboard", auth=False)
        self.assertIn(b"html", html.lower())
        self.assertEqual("no-referrer", headers.get("Referrer-Policy"))
        self.assertEqual("text/html", headers.get_content_type())
        for method, path, body in (("GET", "/api/peers", None), ("GET", "/api/snapshot", None),
            ("GET", "/events?id=unknown", None), ("GET", "/api/stream", None),
            ("POST", "/register", {}), ("POST", "/api/peers", {"alias": "No"}),
            ("PATCH", "/api/peers/unknown", {}), ("DELETE", "/api/peers/unknown", None),
            ("POST", "/health", {}), ("PATCH", "/dashboard", {})):
            self.request(method, path, body, 401, auth=False)
        for path in ("/api/snapshot", "/api/peers"):
            self.request("GET", path + "?token=" + TOKEN, status=401, auth=False)
        self.request("POST", "/api/peers?token=" + TOKEN, {"alias": "No"}, 401, auth=False)
        for wrong in ("Bearer wrong", "Bearer " + "x" * len(TOKEN), "Basic " + TOKEN):
            self.request("GET", "/api/snapshot", status=401, headers={"Authorization": wrong})

    def test_content_type_origin_and_malformed_json_enforcement(self):
        for method in ("POST", "PATCH"):
            for content_type in ("text/plain", "application/x-www-form-urlencoded", ""):
                self.request(method, "/api/peers", {}, 415, headers={"Content-Type": content_type})
        for method, path, body in (("GET", "/api/snapshot", None), ("POST", "/api/peers", {}),
                                  ("PATCH", "/api/peers/no", {}), ("DELETE", "/api/peers/no", None)):
            for origin in ("http://evil.test", "null", "malformed", "http://127.0.0.1:1"):
                self.request(method, path, body, 403, headers={"Origin": origin})
        self.post("/api/peers", {"alias": "Allowed"}, 201,
                  headers={"Origin": self.base, "Content-Type": "application/json; charset=utf-8"})
        for raw in (b"{", b"null", b"[]", b'"text"'):
            self.request("POST", "/api/peers", status=400,
                         headers={"Content-Type": "application/json"}, raw=raw)
        self.request("GET", "/api/missing", status=404)


if __name__ == "__main__":
    unittest.main()
