"""OpenBot host-mode registration and routing in broker.remote-hub.ts.

Spawns the broker with bun on a free port and a temp DB (same pattern as
tests/test_broker_aliases.py). Local mode staleness is pid-based, so offline
transitions are driven through /unregister instead of waiting for staleness.
"""
from __future__ import annotations

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
# A pid that is alive for the whole test run, so the local stale check
# (process.kill(pid, 0)) never reaps the host behind our back.
LIVE_PID = os.getpid()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


AGENTS = [{"id": "alpha", "name": "Alpha"}, {"id": "beta", "name": "Beta Bot"}]


class BrokerOpenBotHostTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "peers.db"
        self._start_broker()

    def _start_broker(self) -> None:
        self.port = _free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        env = os.environ.copy()
        env.update(
            {
                "CLAUDE_PEERS_PORT": str(self.port),
                "CLAUDE_PEERS_DB": str(self.db_path),
                "CLAUDE_PEERS_HUB_TOKEN": TOKEN,
                "CLAUDE_PEERS_STALE_TIMEOUT_MS": "60000",
            }
        )
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
                with urllib.request.urlopen(f"{self.base_url}/health", timeout=0.2):
                    break
            except Exception:
                time.sleep(0.05)
        else:
            self.fail("broker did not become healthy")

    def _stop_broker(self) -> None:
        self.process.terminate()
        try:
            self.process.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.communicate(timeout=2)

    def tearDown(self) -> None:
        self._stop_broker()
        self.tempdir.cleanup()

    # --- helpers ---

    def request(self, method: str, path: str, body=None, expected_status: int = 200):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=data,
            headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=2) as response:
                status = response.status
                payload = json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            status = exc.code
            payload = json.loads(exc.read().decode())
        self.assertEqual(expected_status, status, payload)
        return payload

    def post(self, path: str, body: dict, expected_status: int = 200):
        return self.request("POST", path, body, expected_status)

    def register_host(self, alias: str = "Mac", agents=AGENTS, cwd: str = "/host/mac") -> str:
        payload = self.post(
            "/register",
            {
                "pid": LIVE_PID,
                "parent_pid": 1,
                "logical_name": alias,
                "cwd": cwd,
                "git_root": None,
                "tty": None,
                "summary": "OpenBot host",
                "kind": "openbot-host",
                "agents": agents,
            },
        )
        return payload["id"]

    def create_child(self, host_id: str, agent_id: str, alias: str = "Beta") -> dict:
        return self.post(
            "/api/peers",
            {"alias": alias, "kind": "openbot", "host_id": host_id, "agent_id": agent_id,
             "summary": "managed openbot agent"},
            201,
        )

    def peers(self) -> dict[str, dict]:
        return {peer["id"]: peer for peer in self.request("GET", "/api/peers")["peers"]}

    # --- tests ---

    def test_host_registration_reports_agents_and_heartbeat_replaces_them(self) -> None:
        host_id = self.register_host()

        snapshot = self.request("GET", "/api/snapshot")
        host = next(peer for peer in snapshot["peers"] if peer["id"] == host_id)
        self.assertEqual("openbot-host", host["kind"])
        self.assertEqual(1, host["managed"])
        self.assertIsNone(host["host_id"])
        self.assertEqual(AGENTS, host["agents"])
        self.assertTrue(all(peer["agents"] == [] for peer in snapshot["peers"]
                            if peer["id"] != host_id), "non-hosts get an empty agents list")

        beat = self.post("/heartbeat", {"id": host_id, "agents": [{"id": "gamma", "name": "Gamma"}]})
        self.assertTrue(beat["ok"])
        self.assertEqual([], beat["served_peers"])
        self.assertEqual([{"id": "gamma", "name": "Gamma"}], self.peers()[host_id]["agents"])

        # Heartbeat without `agents` keeps the last reported list.
        self.post("/heartbeat", {"id": host_id})
        self.assertEqual([{"id": "gamma", "name": "Gamma"}], self.peers()[host_id]["agents"])

        listed = self.post("/list-peers", {"scope": "machine", "cwd": "/", "git_root": None,
                                           "exclude_id": None})
        self.assertEqual("openbot-host", listed[0]["kind"])

    def test_plain_peer_heartbeat_has_no_served_peers(self) -> None:
        peer = self.post("/register", {"pid": LIVE_PID, "logical_name": "Plain", "cwd": "/p"})
        self.assertEqual("peer", self.peers()[peer["id"]]["kind"])
        self.assertNotIn("served_peers", self.post("/heartbeat", {"id": peer["id"]}))

    def test_dashboard_creates_online_child_of_host(self) -> None:
        host_id = self.register_host()

        child = self.create_child(host_id, "beta", alias="Beta Bot")

        self.assertEqual(("openbot", host_id, "beta", "online", "openbot", 0, 1),
                         tuple(child[k] for k in ("kind", "host_id", "agent_id", "status", "role",
                                                  "pid", "managed")))
        stored = self.peers()[child["id"]]
        self.assertEqual("online", stored["status"])
        self.assertEqual("beta", stored["agent_id"])

        explicit_role = self.create_child(host_id, "alpha", alias="Alpha Custom")
        self.assertEqual("openbot", explicit_role["role"])

        beat = self.post("/heartbeat", {"id": host_id})
        served = {peer["id"]: peer for peer in beat["served_peers"]}
        self.assertEqual({child["id"], explicit_role["id"]}, set(served))
        self.assertEqual(("Beta Bot", "beta"),
                         (served[child["id"]]["alias"], served[child["id"]]["agent_id"]))

    def test_dashboard_child_validation_failures(self) -> None:
        host_id = self.register_host()
        base = {"alias": "X", "kind": "openbot"}

        self.assertEqual("host not found", self.post(
            "/api/peers", dict(base, host_id="missing", agent_id="beta"), 400)["error"])
        self.assertEqual("host_id and agent_id are required for kind openbot", self.post(
            "/api/peers", dict(base, host_id=host_id), 400)["error"])

        # A managed plain peer is not a host.
        plain = self.post("/api/peers", {"alias": "Plain"}, 201)
        self.assertEqual("host is not an OpenBot host", self.post(
            "/api/peers", dict(base, host_id=plain["id"], agent_id="beta"), 400)["error"])

        unknown_agent = self.post("/api/peers", dict(base, host_id=host_id, agent_id="nope"), 400)
        self.assertEqual(("agent not found on host", "nope"),
                         (unknown_agent["error"], unknown_agent["agent_id"]))

        self.post("/unregister", {"id": host_id})
        offline = self.post("/api/peers", dict(base, host_id=host_id, agent_id="beta"), 400)
        self.assertEqual("host is offline; install and start the bridge on that machine first",
                         offline["error"])
        rejected = self.post("/api/peers", {"alias": "Y", "kind": "openbot-host"}, 400)
        self.assertEqual("only kind openbot accepts host_id and agent_id; hosts register from the daemon",
                         rejected["error"])
        self.assertEqual({"Plain", "Mac"}, {peer["alias"] for peer in self.peers().values()})

    def test_alias_conflict_rules_still_apply_to_children(self) -> None:
        host_id = self.register_host(alias="Mac")
        child = self.create_child(host_id, "beta", alias="Beta")
        self.assertEqual("alias_conflict", self.post(
            "/api/peers", {"alias": "beta", "kind": "openbot", "host_id": host_id,
                           "agent_id": "alpha"}, 409)["error"])
        # A daemon cannot take over a child entry by registering with its alias.
        conflict = self.post("/register", {"pid": LIVE_PID, "logical_name": "Beta", "cwd": "/x",
                                           "kind": "openbot-host", "agents": []}, 409)
        self.assertEqual("alias_conflict", conflict["error"])
        self.assertEqual("openbot", self.peers()[child["id"]]["kind"])

    def test_host_polls_messages_addressed_to_its_children(self) -> None:
        host_id = self.register_host()
        child = self.create_child(host_id, "beta", alias="Beta")

        self.post("/send-message", {"from_id": "cli", "from_pid": 1, "to_id": "beta", "text": "for beta"})
        self.post("/send-message", {"from_id": "cli", "from_pid": 1, "to_id": host_id,
                                    "text": "for host"})

        via_host = self.post("/poll-messages", {"id": host_id, "mark_delivered": False})["messages"]
        self.assertEqual({(child["id"], "for beta"), (host_id, "for host")},
                         {(msg["to_id"], msg["text"]) for msg in via_host})

        via_child = self.post("/poll-messages", {"id": child["id"], "mark_delivered": False})["messages"]
        self.assertEqual([("for beta", child["id"])], [(m["text"], m["to_id"]) for m in via_child])

        delivered = self.post("/poll-messages", {"id": host_id})["messages"]
        self.assertEqual(2, len(delivered))
        self.assertEqual([], self.post("/poll-messages", {"id": host_id})["messages"])
        self.assertEqual([], self.post("/poll-messages", {"id": child["id"]})["messages"])

    def test_wake_for_child_message_is_delivered_to_host_subscribers(self) -> None:
        host_id = self.register_host()
        child = self.create_child(host_id, "beta")

        import threading
        events: list[str] = []
        ready = threading.Event()

        def listen() -> None:
            request = urllib.request.Request(f"{self.base_url}/events?id={host_id}",
                                             headers={"Authorization": f"Bearer {TOKEN}"})
            with urllib.request.urlopen(request, timeout=5) as response:
                while True:
                    frame = response.readline().decode()
                    if not frame:
                        break
                    if frame.startswith(":"):
                        ready.set()
                    elif frame.startswith("data:"):
                        events.append(frame[len("data:"):].strip())

        thread = threading.Thread(target=listen, daemon=True)
        thread.start()
        self.assertTrue(ready.wait(2), "SSE did not connect")
        time.sleep(0.1)
        self.post("/send-message", {"from_id": "cli", "from_pid": 1, "to_id": child["id"],
                                    "text": "wake up"})
        deadline = time.time() + 3
        while time.time() < deadline and len(events) < 1:
            time.sleep(0.02)
        self.assertEqual([{"type": "messages", "peer_id": child["id"]}], [json.loads(e) for e in events])

    def test_host_offline_and_back_online_propagates_to_children(self) -> None:
        host_id = self.register_host()
        child = self.create_child(host_id, "beta")
        other_host = self.register_host(alias="Linux", cwd="/host/linux")
        other_child = self.create_child(other_host, "beta", alias="Linux Beta")

        self.post("/unregister", {"id": host_id})
        peers = self.peers()
        self.assertEqual(("offline", 0), (peers[host_id]["status"], peers[host_id]["pid"]))
        self.assertEqual("offline", peers[child["id"]]["status"])
        self.assertEqual("online", peers[other_child["id"]]["status"])

        # A heartbeat does not bring an unregistered host (or its children) back online;
        # only re-registration does. served_peers is still reported.
        beat = self.post("/heartbeat", {"id": host_id})
        self.assertEqual([child["id"]], [peer["id"] for peer in beat["served_peers"]])
        self.assertEqual("offline", self.peers()[child["id"]]["status"])

        reRegistered = self.register_host(agents=[{"id": "beta", "name": "Beta v2"}])
        self.assertEqual(host_id, reRegistered)
        peers = self.peers()
        self.assertEqual("online", peers[host_id]["status"])
        self.assertEqual("online", peers[child["id"]]["status"])
        self.assertEqual([{"id": "beta", "name": "Beta v2"}], peers[host_id]["agents"])
        self.assertEqual([child["id"]], [p["id"] for p in self.post("/heartbeat", {"id": host_id})
                                          ["served_peers"]])

    def test_deleting_host_deletes_children_and_agent_list(self) -> None:
        host_id = self.register_host()
        child = self.create_child(host_id, "beta")
        self.post("/send-message", {"from_id": "cli", "from_pid": 1, "to_id": child["id"],
                                    "text": "queued"})

        self.request("DELETE", f"/api/peers/{host_id}?force=1")
        self.assertEqual({}, self.peers())
        db = sqlite3.connect(self.db_path)
        host_agents = db.execute("SELECT COUNT(*) FROM host_agents WHERE host_id = ?",
                                 (host_id,)).fetchone()[0]
        queued = db.execute("SELECT COUNT(*) FROM messages WHERE to_id = ?", (child["id"],)).fetchone()[0]
        db.close()
        self.assertEqual((0, 0), (host_agents, queued))

        # Deleting only a child leaves the host and its agent list intact.
        host_id = self.register_host()
        child = self.create_child(host_id, "beta")
        self.request("DELETE", f"/api/peers/{child['id']}?force=1")
        peers = self.peers()
        self.assertEqual([host_id], list(peers))
        self.assertEqual(AGENTS, peers[host_id]["agents"])

    def test_invalid_kind_and_agent_payloads_are_rejected(self) -> None:
        host_body = {"pid": LIVE_PID, "logical_name": "Mac", "cwd": "/host/mac",
                     "kind": "openbot-host"}

        for kind in ("robot", "", 7, ["openbot"]):
            payload = self.post("/register", dict(host_body, kind=kind, agents=[]), 400)
            self.assertEqual("invalid_request", payload["error"])

        for agents in ({"id": "a"}, "alpha", [{"id": "a"}], [{"id": "", "name": "N"}],
                       [{"id": "a", "name": 3}], [{"id": "a", "name": "A"}, {"id": "a", "name": "B"}],
                       [{"id": "x" * 129, "name": "N"}], [{"id": "a", "name": "A"}] * 201, ["a"]):
            payload = self.post("/register", dict(host_body, agents=agents), 400)
            self.assertEqual("invalid_request", payload["error"])

        # Non-hosts may not carry agents, on register or heartbeat.
        plain = self.post("/register", {"pid": LIVE_PID, "logical_name": "Plain", "cwd": "/p"})
        self.assertEqual("invalid_request", self.post(
            "/register", {"pid": LIVE_PID, "logical_name": "P2", "cwd": "/p2", "agents": []}, 400)["error"])
        self.assertEqual("invalid_request", self.post(
            "/heartbeat", {"id": plain["id"], "agents": []}, 400)["error"])

        # Agents are optional for a host; an empty list is valid.
        host_id = self.post("/register", host_body)["id"]
        self.assertEqual([], self.peers()[host_id]["agents"])
        self.assertEqual("invalid_request", self.post(
            "/api/peers", {"alias": "Z", "agents": []}, 400)["error"])

    def test_schema_gains_openbot_columns_and_host_agents_table(self) -> None:
        db = sqlite3.connect(self.db_path)
        peer_columns = {row[1] for row in db.execute("PRAGMA table_info(peers)")}
        host_agent_columns = [row[1] for row in db.execute("PRAGMA table_info(host_agents)")]
        db.close()
        self.assertTrue({"kind", "host_id", "agent_id"} <= peer_columns)
        self.assertEqual(["host_id", "agent_id", "name"], host_agent_columns)


if __name__ == "__main__":
    unittest.main()
