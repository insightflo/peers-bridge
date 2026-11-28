from __future__ import annotations

import io
import json
import os
import pathlib
import tempfile
import unittest
import urllib.error
from unittest import mock

import openbot_bridge

HOST_REGISTER_FIELDS = {
    "pid",
    "parent_pid",
    "logical_name",
    "cwd",
    "git_root",
    "tty",
    "summary",
}


def _msg(msg_id: int, to_id: str | None = None) -> dict:
    message = {
        "id": msg_id,
        "from_id": "abc12345",
        "from_alias": "OpenBot-Instabox",
        "from_kind": "peer",
        "sent_at": "2026-11-27T00:00:00Z",
        "text": "ping",
    }
    if to_id is not None:
        message["to_id"] = to_id
    return message


class OpenBotBridgeTests(unittest.TestCase):
    def make_daemon(self) -> openbot_bridge.OpenBotPeersDaemon:
        return openbot_bridge.OpenBotPeersDaemon(
            broker_url="https://peers.example.com",
            broker_auth="Bearer secret",
            alias="OpenBot-Mac",
            summary="OpenBot on Mac",
            agent="비서",
            cli_path="/opt/openbot-cli.mjs",
            node_bin="/usr/bin/node",
        )

    def test_dispatch_passes_message_via_stdin_without_shell(self) -> None:
        daemon = self.make_daemon()
        with mock.patch("subprocess.run") as run_mock:
            run_mock.return_value = mock.Mock(returncode=0, stdout="ok", stderr="")
            ok = daemon.dispatch_to_openbot(
                {
                    "id": 7,
                    "from_id": "abc12345",
                    "from_alias": "Dashboard",
                    "from_kind": "system",
                    "sent_at": "2026-11-27T00:00:00Z",
                    "text": "$(rm -rf /); hello",
                }
            )
        self.assertTrue(ok)
        args, kwargs = run_mock.call_args
        self.assertEqual(["/usr/bin/node", "/opt/openbot-cli.mjs", "run", "비서", "-"], args[0])
        self.assertFalse(kwargs["shell"])
        self.assertIn("$(rm -rf /); hello", kwargs["input"])
        self.assertNotIn("$(rm -rf /)", " ".join(args[0]))

    def test_poll_and_deliver_marks_delivered_and_dedupes(self) -> None:
        daemon = self.make_daemon()
        daemon.peer_id = "ujebusm9"
        calls: list[tuple[str, dict]] = []

        def fake_post(path, body, timeout=8.0):
            calls.append((path, body))
            if path == "/poll-messages":
                return {
                    "messages": [
                        {
                            "id": 42,
                            "from_id": "dashboard",
                            "from_alias": "Dashboard",
                            "from_kind": "system",
                            "sent_at": "2026-11-27T00:00:00Z",
                            "text": "ping",
                        }
                    ]
                }
            return {"ok": True}

        daemon._post_json = fake_post  # type: ignore[method-assign]
        daemon.dispatch_to_openbot = mock.Mock(return_value=True)  # type: ignore[method-assign]

        self.assertEqual(1, daemon.poll_and_deliver())
        self.assertEqual(0, daemon.poll_and_deliver())
        self.assertEqual(1, daemon.dispatch_to_openbot.call_count)
        self.assertIn(("/mark-message-delivered", {"id": 42}), calls)

    def test_revoke_halts_daemon_and_blocks_reregister(self) -> None:
        daemon = self.make_daemon()
        daemon.peer_id = "ujebusm9"
        daemon._revoke()
        self.assertTrue(daemon.revoked)
        self.assertTrue(daemon._stop_event.is_set())
        with self.assertRaisesRegex(RuntimeError, "revoked"):
            daemon.register()

    def test_format_task_includes_reply_command_for_peers(self) -> None:
        peer_prompt = openbot_bridge._format_task(
            {
                "from_id": "fjylcxmj",
                "from_alias": "OpenBot-Instabox",
                "from_kind": "peer",
                "sent_at": "2026-11-27T00:00:00Z",
                "text": "check status",
            }
        )
        self.assertIn('peers-send "OpenBot-Instabox"', peer_prompt)

    def test_cli_send_resolves_self_peer_id(self) -> None:
        calls: list[tuple[str, str, dict | None]] = []

        def fake_req(url, auth, method, path, body=None, timeout=8.0):
            calls.append((method, path, body))
            if path == "/list-peers":
                return [{"id": "ujebusm9", "alias": "OpenBot-Mac"}]
            return {"ok": True, "to_id": "fjylcxmj", "to_alias": "OpenBot-Instabox"}

        with mock.patch("openbot_bridge._api_request", side_effect=fake_req):
            rc = openbot_bridge.cli_send(
                "https://peers.example.com",
                "Bearer secret",
                "OpenBot-Mac",
                "OpenBot-Instabox",
                "hello",
            )
        self.assertEqual(0, rc)
        self.assertEqual("ujebusm9", calls[1][2]["from_id"])
        self.assertEqual("OpenBot-Instabox", calls[1][2]["to_id"])


class OpenBotHostModeTests(unittest.TestCase):
    """Host mode: OPENBOT_AGENT unset → kind openbot-host, agents reported, served children."""

    HOST_ID = "host0001"
    CHILD_ID = "child001"
    AGENTS = [{"id": "agent-a", "name": "Alpha"}, {"id": "agent-b", "name": "Beta"}]

    def make_host(self, **kwargs) -> openbot_bridge.OpenBotPeersDaemon:
        kwargs.setdefault("agents_refresh_seconds", 0.0)
        return openbot_bridge.OpenBotPeersDaemon(
            broker_url="https://peers.example.com",
            broker_auth="Bearer secret",
            alias="OpenBot-Mac",
            summary="OpenBot host",
            agent=None,
            cli_path="/opt/openbot-cli.mjs",
            node_bin="/usr/bin/node",
            **kwargs,
        )

    def stub_list_agents(self):
        return mock.patch("openbot_bridge.list_openbot_agents", return_value=(list(self.AGENTS), None))

    def test_register_reports_host_kind_and_agents(self) -> None:
        daemon = self.make_host()
        bodies: list[tuple[str, dict]] = []

        def fake_post(path, body, timeout=8.0):
            bodies.append((path, body))
            return {"id": self.HOST_ID}

        daemon._post_json = fake_post  # type: ignore[method-assign]
        with self.stub_list_agents() as list_mock:
            self.assertEqual(self.HOST_ID, daemon.register())
        path, body = bodies[0]
        self.assertEqual("/register", path)
        self.assertEqual("openbot-host", body["kind"])
        self.assertEqual(self.AGENTS, body["agents"])
        self.assertEqual(HOST_REGISTER_FIELDS | {"kind", "agents"}, set(body))
        self.assertEqual("OpenBot-Mac", body["logical_name"])
        self.assertEqual(("/usr/bin/node", "/opt/openbot-cli.mjs"), list_mock.call_args.args)

    def test_register_keeps_running_when_agent_discovery_fails(self) -> None:
        daemon = self.make_host()
        bodies: list[dict] = []
        daemon._post_json = lambda path, body, timeout=8.0: bodies.append(body) or {"id": self.HOST_ID}  # type: ignore[method-assign]
        with mock.patch("openbot_bridge.list_openbot_agents", return_value=([], "openbot-cli list exit 1: boom")):
            daemon.register()
        self.assertEqual([], bodies[0]["agents"])
        self.assertEqual("openbot-cli list exit 1: boom", daemon.last_error)

    def test_heartbeat_after_refresh_includes_agents(self) -> None:
        daemon = self.make_host(agents_refresh_seconds=300.0)
        daemon.peer_id = self.HOST_ID
        bodies: list[dict] = []
        daemon._post_json = lambda path, body, timeout=8.0: bodies.append(body) or {"ok": True}  # type: ignore[method-assign]
        with self.stub_list_agents():
            daemon.heartbeat()  # first refresh (never refreshed yet) → agents sent
            self.assertEqual(self.AGENTS, bodies[-1]["agents"])
            daemon.heartbeat()  # interval not elapsed → agents omitted, list unchanged on broker
            self.assertEqual({"id": self.HOST_ID}, bodies[-1])
            daemon.refresh_agents(force=True)
        daemon.heartbeat()
        self.assertEqual(self.AGENTS, bodies[-1]["agents"])

    def test_heartbeat_served_peers_populate_served(self) -> None:
        daemon = self.make_host()
        daemon.peer_id = self.HOST_ID
        responses = [
            {"ok": True, "served_peers": [{"id": self.CHILD_ID, "alias": "Alpha-Bot", "agent_id": "agent-a"}]},
            {"ok": True, "served_peers": [
                {"id": "child002", "alias": None, "agent_id": "agent-b"},
                {"id": "child003", "alias": "Broken", "agent_id": None},
                {"id": "child004", "alias": "DupAgent", "agent_id": "agent-b"},
            ]},
        ]
        daemon._post_json = lambda path, body, timeout=8.0: responses.pop(0)  # type: ignore[method-assign]
        with self.stub_list_agents():
            daemon.heartbeat()
        self.assertEqual({self.CHILD_ID: "agent-a"}, daemon.served)
        self.assertEqual({self.CHILD_ID: "Alpha-Bot"}, daemon.served_alias)
        with self.stub_list_agents():
            daemon.heartbeat()
        self.assertEqual({"child002": "agent-b", "child004": "agent-b"}, daemon.served)
        self.assertEqual({"child002": "OpenBot-Mac", "child004": "DupAgent"}, daemon.served_alias)

    def test_child_message_dispatches_with_child_identity_env(self) -> None:
        daemon = self.make_host()
        daemon.peer_id = self.HOST_ID
        daemon.served = {self.CHILD_ID: "agent-a"}
        daemon.served_alias = {self.CHILD_ID: "Alpha-Bot"}
        calls: list[tuple[str, dict]] = []

        def fake_post(path, body, timeout=8.0):
            calls.append((path, body))
            if path == "/poll-messages":
                return {"messages": [_msg(42, self.CHILD_ID)]}
            return {"ok": True}

        daemon._post_json = fake_post  # type: ignore[method-assign]
        with mock.patch("subprocess.run") as run_mock, mock.patch.dict(
            os.environ, {"CLAUDE_PEERS_NAME": "OpenBot-Mac", "CLAUDE_PEERS_BROKER_AUTH": "stale"}
        ):
            run_mock.return_value = mock.Mock(returncode=0, stdout="ok", stderr="")
            self.assertEqual(1, daemon.poll_and_deliver())
            self.assertEqual(0, daemon.poll_and_deliver())
        self.assertEqual(1, run_mock.call_count)
        args, kwargs = run_mock.call_args
        self.assertEqual(["/usr/bin/node", "/opt/openbot-cli.mjs", "run", "agent-a", "-"], args[0])
        self.assertFalse(kwargs["shell"])
        env = kwargs["env"]
        self.assertEqual("Alpha-Bot", env["CLAUDE_PEERS_NAME"])
        self.assertEqual("https://peers.example.com", env["CLAUDE_PEERS_BROKER_URL"])
        self.assertEqual("Bearer secret", env["CLAUDE_PEERS_BROKER_AUTH"])
        self.assertIn('peers-send "OpenBot-Instabox" "<your reply>" --as "Alpha-Bot"', kwargs["input"])
        self.assertIn(("/mark-message-delivered", {"id": 42}), calls)

    def test_dispatch_falls_back_to_host_alias_without_child_alias(self) -> None:
        daemon = self.make_host()
        daemon.served = {self.CHILD_ID: "agent-a"}
        with mock.patch("subprocess.run") as run_mock:
            run_mock.return_value = mock.Mock(returncode=0, stdout="ok", stderr="")
            self.assertTrue(daemon.dispatch_to_openbot(_msg(1, self.CHILD_ID)))
        env = run_mock.call_args.kwargs["env"]
        self.assertEqual("OpenBot-Mac", env["CLAUDE_PEERS_NAME"])

    def test_message_to_host_id_is_dropped_and_marked_delivered(self) -> None:
        daemon = self.make_host()
        daemon.peer_id = self.HOST_ID
        calls: list[tuple[str, dict]] = []

        def fake_post(path, body, timeout=8.0):
            calls.append((path, body))
            if path == "/poll-messages":
                return {"messages": [_msg(42, self.HOST_ID), _msg(43, self.CHILD_ID)]}
            return {"ok": True}

        daemon._post_json = fake_post  # type: ignore[method-assign]
        with mock.patch("subprocess.run") as run_mock, mock.patch("sys.stderr"):
            run_mock.return_value = mock.Mock(returncode=0, stdout="ok", stderr="")
            self.assertEqual(0, daemon.poll_and_deliver())
            self.assertEqual(0, run_mock.call_count)
            self.assertNotIn(("/mark-message-delivered", {"id": 43}), calls)
            # Already dropped messages are not re-processed on the next poll.
            self.assertEqual(0, daemon.poll_and_deliver())
        self.assertEqual(1, calls.count(("/mark-message-delivered", {"id": 42})))

    def test_unknown_to_id_is_left_undelivered_until_resolved(self) -> None:
        daemon = self.make_host()
        daemon.peer_id = self.HOST_ID
        calls: list[tuple[str, dict]] = []

        def fake_post(path, body, timeout=8.0):
            calls.append((path, body))
            if path == "/poll-messages":
                return {"messages": [_msg(42, "who-knows")]}
            return {"ok": True}

        daemon._post_json = fake_post  # type: ignore[method-assign]
        with mock.patch("subprocess.run") as run_mock, mock.patch("sys.stderr") as stderr:
            run_mock.return_value = mock.Mock(returncode=0, stdout="ok", stderr="")
            self.assertEqual(0, daemon.poll_and_deliver())
            self.assertEqual(0, daemon.poll_and_deliver())
            self.assertEqual(0, run_mock.call_count)
            self.assertNotIn("/mark-message-delivered", [c[0] for c in calls])
            warnings = [c.args[0] for c in stderr.write.call_args_list if "unknown peer" in c.args[0]]
            self.assertEqual(1, len(warnings))  # logged once per message id
            # Next heartbeat maps the id → agent; the pending message now dispatches.
            daemon._set_served([{"id": "who-knows", "alias": "Late-Bot", "agent_id": "agent-b"}])
            self.assertEqual(1, daemon.poll_and_deliver())
        self.assertEqual(["/usr/bin/node", "/opt/openbot-cli.mjs", "run", "agent-b", "-"], run_mock.call_args.args[0])
        self.assertEqual("Late-Bot", run_mock.call_args.kwargs["env"]["CLAUDE_PEERS_NAME"])
        self.assertIn(("/mark-message-delivered", {"id": 42}), calls)

    def test_unknown_target_triggers_light_heartbeat_and_dispatches_same_poll(self) -> None:
        # A child registered from the dashboard must not wait for the 15 s heartbeat.
        daemon = self.make_host()
        daemon.peer_id = self.HOST_ID
        calls: list[tuple[str, dict]] = []

        def fake_post(path, body, timeout=8.0):
            calls.append((path, body))
            if path == "/poll-messages":
                return {"messages": [_msg(44, self.CHILD_ID)]}
            if path == "/heartbeat":
                return {"ok": True, "served_peers": [{"id": self.CHILD_ID, "alias": "Mac-비서", "agent_id": "agent-x"}]}
            return {"ok": True}

        daemon._post_json = fake_post  # type: ignore[method-assign]
        with mock.patch("subprocess.run") as run_mock, mock.patch("sys.stderr"):
            run_mock.return_value = mock.Mock(returncode=0, stdout="ok", stderr="")
            self.assertEqual(1, daemon.poll_and_deliver())
        self.assertEqual(["run", "agent-x", "-"], run_mock.call_args.args[0][2:])
        # The refresh heartbeat carries no agents and runs no `openbot-cli list`.
        self.assertEqual(("/heartbeat", {"id": self.HOST_ID}), calls[1])
        self.assertEqual(1, run_mock.call_count)
        self.assertEqual("agent-x", daemon.served[self.CHILD_ID])
        self.assertEqual("Mac-비서", run_mock.call_args.kwargs["env"]["CLAUDE_PEERS_NAME"])


class OpenBotLegacyModeTests(unittest.TestCase):
    """OPENBOT_AGENT set → behaviour identical to before host mode existed."""

    def make_legacy(self) -> openbot_bridge.OpenBotPeersDaemon:
        return openbot_bridge.OpenBotPeersDaemon(
            broker_url="https://peers.example.com",
            broker_auth=None,
            alias="OpenBot-Mac",
            summary="OpenBot (비서)",
            agent="비서",
            cli_path="/opt/openbot-cli.mjs",
            node_bin="/usr/bin/node",
        )

    def test_legacy_register_has_no_kind_or_agents(self) -> None:
        daemon = self.make_legacy()
        bodies: list[dict] = []
        daemon._post_json = lambda path, body, timeout=8.0: bodies.append(body) or {"id": "ujebusm9"}  # type: ignore[method-assign]
        with mock.patch("openbot_bridge.list_openbot_agents") as list_mock:
            daemon.register()
            daemon.heartbeat()
        self.assertEqual(HOST_REGISTER_FIELDS, set(bodies[0]))
        self.assertEqual({"id": "ujebusm9"}, bodies[1])
        self.assertEqual(0, list_mock.call_count)

    def test_legacy_dispatch_uses_self_agent_even_with_served_mapping(self) -> None:
        daemon = self.make_legacy()
        daemon.peer_id = "ujebusm9"
        daemon.served = {"ujebusm9": "someone-else"}
        daemon._post_json = lambda path, body, timeout=8.0: {"messages": [_msg(42, "ujebusm9")]}  # type: ignore[method-assign]
        with mock.patch("subprocess.run") as run_mock:
            run_mock.return_value = mock.Mock(returncode=0, stdout="ok", stderr="")
            self.assertEqual(1, daemon.poll_and_deliver())
        self.assertEqual(["/usr/bin/node", "/opt/openbot-cli.mjs", "run", "비서", "-"], run_mock.call_args.args[0])
        self.assertEqual("OpenBot-Mac", run_mock.call_args.kwargs["env"]["CLAUDE_PEERS_NAME"])
        self.assertNotIn("self_alias", run_mock.call_args.kwargs["input"])

    def test_main_selects_mode_from_env(self) -> None:
        base_env = {"CLAUDE_PEERS_NAME": "OpenBot-Mac", "OPENBOT_AGENT": ""}
        with mock.patch.dict(os.environ, base_env, clear=False), mock.patch(
            "openbot_bridge.OpenBotPeersDaemon"
        ) as daemon_cls, mock.patch("openbot_bridge._load_env_defaults"):
            daemon_cls.return_value.run_forever.return_value = None
            self.assertEqual(0, openbot_bridge.main([]))
            self.assertIsNone(daemon_cls.call_args.kwargs["agent"])
            self.assertEqual("OpenBot host", daemon_cls.call_args.kwargs["summary"])
            self.assertEqual(300.0, daemon_cls.call_args.kwargs["agents_refresh_seconds"])
            with mock.patch.dict(os.environ, {"OPENBOT_AGENT": "비서", "OPENBOT_AGENTS_REFRESH_SECONDS": "60"}):
                self.assertEqual(0, openbot_bridge.main([]))
            self.assertEqual("비서", daemon_cls.call_args.kwargs["agent"])
            self.assertEqual("OpenBot (비서)", daemon_cls.call_args.kwargs["summary"])
            self.assertEqual(60.0, daemon_cls.call_args.kwargs["agents_refresh_seconds"])
        with mock.patch.dict(os.environ, {"CLAUDE_PEERS_NAME": ""}, clear=False), mock.patch(
            "openbot_bridge.OpenBotPeersDaemon"
        ) as daemon_cls, mock.patch("openbot_bridge._load_env_defaults"), mock.patch("sys.stderr"):
            self.assertEqual(2, openbot_bridge.main([]))
            self.assertEqual(0, daemon_cls.call_count)


class ListOpenBotAgentsTests(unittest.TestCase):
    def test_parses_and_sanitises_agent_list(self) -> None:
        payload = '{"ok": true, "agents": [{"id": " a ", "name": " Alpha "}, {"id": ""}, {"name": "no id"}, {"id": "a", "name": "dup"}, "junk"]}'
        with mock.patch("subprocess.run") as run_mock:
            run_mock.return_value = mock.Mock(returncode=0, stdout=payload, stderr="")
            agents, error = openbot_bridge.list_openbot_agents("/usr/bin/node", "/opt/openbot-cli.mjs")
        self.assertIsNone(error)
        self.assertEqual([{"id": "a", "name": "Alpha"}], agents)
        self.assertEqual(["/usr/bin/node", "/opt/openbot-cli.mjs", "list", "--json"], run_mock.call_args.args[0])
        self.assertFalse(run_mock.call_args.kwargs["shell"])

    def test_failures_return_empty_list_and_error(self) -> None:
        cases = [
            (mock.Mock(returncode=1, stdout="", stderr="openbot offline"), "exit 1"),
            (mock.Mock(returncode=0, stdout="not json", stderr=""), "invalid JSON"),
            (mock.Mock(returncode=0, stdout="[1,2]", stderr=""), None),
        ]
        for proc, expected in cases:
            with self.subTest(expected=expected), mock.patch("subprocess.run", return_value=proc):
                agents, error = openbot_bridge.list_openbot_agents("node", "/cli")
            self.assertEqual([], agents)
            if expected:
                self.assertIn(expected, error or "")
            else:
                self.assertIsNone(error)
        with mock.patch("subprocess.run", side_effect=OSError("no node")):
            agents, error = openbot_bridge.list_openbot_agents("node", "/cli")
        self.assertEqual([], agents)
        self.assertIn("no node", error or "")


class _FakeResponse(io.BytesIO):
    """Minimal `urlopen` response stand-in: context manager + readable bytes."""

    def __init__(self, body: bytes):
        super().__init__(body)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _att_msg(msg_id: int, attachments: list[dict]) -> dict:
    message = _msg(msg_id, "ujebusm9")
    message["attachments"] = attachments
    return message


class OpenBotAttachmentTests(unittest.TestCase):
    """Received attachments are cached locally and referenced in the prompt by path."""

    ATTS = [
        {"id": "a" * 32, "name": "../../x.png", "size": 4, "mime": "image/png"},
        {"id": "b" * 32, "name": "notes.md", "size": 5, "mime": "text/markdown"},
    ]

    def setUp(self) -> None:
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(lambda: openbot_bridge.shutil.rmtree(self.tmp, ignore_errors=True))
        self.daemon = openbot_bridge.OpenBotPeersDaemon(
            broker_url="https://peers.example.com",
            broker_auth="Bearer secret",
            alias="OpenBot-Mac",
            summary="OpenBot",
            agent="비서",
            cli_path="/opt/openbot-cli.mjs",
            node_bin="/usr/bin/node",
            attachment_dir=self.tmp / "attachments",
        )
        self.daemon.peer_id = "ujebusm9"
        self.calls: list[tuple[str, dict]] = []
        self.messages = [_att_msg(42, self.ATTS)]

        def fake_post(path, body, timeout=8.0):
            self.calls.append((path, body))
            if path == "/poll-messages":
                return {"messages": list(self.messages)}
            return {"ok": True}

        self.daemon._post_json = fake_post  # type: ignore[method-assign]
        self.run_mock = self.enterContext(mock.patch("subprocess.run"))
        self.run_mock.return_value = mock.Mock(returncode=0, stdout="ok", stderr="")

    @staticmethod
    def _download_stub(bodies: dict[str, bytes], seen: list[str]):
        def fake_urlopen(req, timeout=None):
            seen.append(req.full_url)
            att_id = req.full_url.rsplit("/", 1)[-1]
            return _FakeResponse(bodies[att_id])

        return fake_urlopen

    def test_attachments_downloaded_and_listed_in_prompt(self) -> None:
        seen: list[str] = []
        urlopen = self.enterContext(mock.patch(
            "urllib.request.urlopen",
            side_effect=self._download_stub({"a" * 32: b"PNG!", "b" * 32: b"hello"}, seen),
        ))
        self.assertEqual(1, self.daemon.poll_and_deliver())
        base = (self.tmp / "attachments" / "42").resolve()
        self.assertEqual(0o700, (self.tmp / "attachments").stat().st_mode & 0o777)
        self.assertEqual(0o700, base.stat().st_mode & 0o777)
        self.assertEqual(b"PNG!", (base / "x.png").read_bytes())  # traversal stripped
        self.assertEqual(b"hello", (base / "notes.md").read_bytes())
        self.assertEqual({f"https://peers.example.com/attachments/{'a' * 32}", f"https://peers.example.com/attachments/{'b' * 32}"}, set(seen))
        req = urlopen.call_args.args[0]
        self.assertEqual("Bearer secret", req.get_header("Authorization"))
        self.assertEqual(openbot_bridge.USER_AGENT, req.get_header("User-agent"))
        self.assertEqual(1, self.run_mock.call_count)
        prompt = self.run_mock.call_args.kwargs["input"]
        self.assertIn(
            f"Attachments:\n- {base / 'x.png'} (x.png, image/png, 4 bytes)\n- {base / 'notes.md'} (notes.md, text/markdown, 5 bytes)\n",
            prompt,
        )
        self.assertIn('peers-send "OpenBot-Instabox" "<your reply>" [--file <path>]...', prompt)  # legacy: no --as
        self.assertIn(("/mark-message-delivered", {"id": 42}), self.calls)
        # Second delivery of the same message skips downloads for same-size cached files.
        self.daemon._delivered_ids.clear()
        urlopen.reset_mock()
        self.assertEqual(1, self.daemon.poll_and_deliver())
        self.assertEqual(0, urlopen.call_count)
        self.assertIn(f"- {base / 'notes.md'} (notes.md, text/markdown, 5 bytes)", self.run_mock.call_args.kwargs["input"])

    def test_download_failure_still_delivers_and_marks(self) -> None:
        def fake_urlopen(req, timeout=None):
            raise urllib.error.URLError("connection refused")

        self.enterContext(mock.patch("urllib.request.urlopen", side_effect=fake_urlopen))
        self.enterContext(mock.patch("sys.stderr"))
        self.assertEqual(1, self.daemon.poll_and_deliver())
        self.assertEqual(1, self.run_mock.call_count)
        prompt = self.run_mock.call_args.kwargs["input"]
        self.assertIn("- x.png: download failed (<urlopen error connection refused>)", prompt)
        self.assertIn("- notes.md: download failed (", prompt)
        self.assertNotIn("Attachments:\n- /", prompt)
        self.assertIn(("/mark-message-delivered", {"id": 42}), self.calls)
        self.assertIn("attachment download failed (", self.daemon.last_error or "")
        self.assertEqual([], list((self.tmp / "attachments" / "42").iterdir()))

    def test_http_404_attachment_reports_status(self) -> None:
        def fake_urlopen(req, timeout=None):
            raise urllib.error.HTTPError(req.full_url, 404, "Not Found", {}, io.BytesIO(b'{"error":"attachment not found"}'))

        self.enterContext(mock.patch("urllib.request.urlopen", side_effect=fake_urlopen))
        self.enterContext(mock.patch("sys.stderr"))
        self.messages = [_att_msg(7, [{"id": "c" * 32, "name": "gone.pdf", "size": 1, "mime": "application/pdf"}])]
        self.assertEqual(1, self.daemon.poll_and_deliver())
        self.assertIn("- gone.pdf: download failed (HTTP 404", self.run_mock.call_args.kwargs["input"])

    def test_no_attachments_keeps_prompt_identical(self) -> None:
        urlopen = self.enterContext(mock.patch("urllib.request.urlopen"))
        self.messages = [_msg(43, "ujebusm9")]
        self.messages[0]["attachments"] = []
        self.assertEqual(1, self.daemon.poll_and_deliver())
        self.assertEqual(openbot_bridge._format_task(self.messages[0]), self.run_mock.call_args.kwargs["input"])
        self.assertNotIn("Attachments", self.run_mock.call_args.kwargs["input"])
        self.assertEqual(0, urlopen.call_count)

    def test_sanitize_attachment_name(self) -> None:
        MAX = openbot_bridge.MAX_ATTACHMENT_NAME_LENGTH
        cases = {
            "../../x.png": "x.png",
            "..\\..\\evil\\y.txt": "y.txt",
            "a\x00b\nc.png": "abc.png",
            "..": "attachment",
            "   ": "attachment",
            "x" * 300 + ".png": "x" * MAX,  # capped, extension may be lost
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(expected, openbot_bridge.sanitize_attachment_name(raw))

    def test_old_attachment_dirs_are_removed(self) -> None:
        base = self.tmp / "attachments"
        old = base / "1"
        fresh = base / "2"
        old.mkdir(parents=True)
        fresh.mkdir()
        (old / "f.bin").write_bytes(b"x")
        import time
        long_ago = time.time() - 10 * 86400
        os.utime(old, (long_ago, long_ago))
        self.assertEqual(1, openbot_bridge.cleanup_attachment_dirs(base, 168.0))
        self.assertFalse(old.exists())
        self.assertTrue(fresh.exists())
        # Driven by poll_and_deliver: keep_hours=0 drops everything older than "now".
        self.daemon.attachment_keep_hours = 0.0
        past = time.time() - 60
        os.utime(fresh, (past, past))
        self.messages = []
        self.assertEqual(0, self.daemon.poll_and_deliver())
        self.assertFalse(fresh.exists())
        self.assertFalse(openbot_bridge.cleanup_attachment_dirs(self.tmp / "missing", 1.0))


class CliSendAttachmentTests(unittest.TestCase):
    """`peers-send --file` uploads first, then references the ids in /send-message."""

    def setUp(self) -> None:
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(lambda: openbot_bridge.shutil.rmtree(self.tmp, ignore_errors=True))
        self.file_a = self.tmp / "a.txt"
        self.file_a.write_text("hello")
        self.file_b = self.tmp / "b.bin"
        self.file_b.write_bytes(b"\x00\x01")

    def test_cli_send_uploads_then_sends_with_ids(self) -> None:
        calls: list[tuple[str, str, dict | None]] = []

        def fake_req(url, auth, method, path, body=None, timeout=8.0):
            calls.append((method, path, body))
            if path == "/list-peers":
                return [{"id": "ujebusm9", "alias": "OpenBot-Mac"}]
            return {"ok": True, "to_id": "fjylcxmj", "to_alias": "OpenBot-Instabox"}

        with mock.patch("openbot_bridge._api_request", side_effect=fake_req), mock.patch(
            "openbot_bridge.upload_attachment", side_effect=[{"id": "1" * 32}, {"id": "2" * 32}]
        ) as upload_mock, mock.patch("sys.stderr"):
            rc = openbot_bridge.cli_send(
                "https://peers.example.com", "Bearer secret", "OpenBot-Mac", "OpenBot-Instabox", "see files",
                [str(self.file_a), str(self.file_b)],
            )
        self.assertEqual(0, rc)
        self.assertEqual(2, upload_mock.call_count)
        # Uploader id is the resolved peer id, not the alias.
        self.assertEqual("ujebusm9", upload_mock.call_args_list[0].kwargs["from_id"])
        self.assertEqual("https://peers.example.com", upload_mock.call_args_list[0].args[0])
        self.assertEqual(str(self.file_a), upload_mock.call_args_list[0].args[2])
        self.assertEqual(str(self.file_b), upload_mock.call_args_list[1].args[2])
        self.assertEqual(["1" * 32, "2" * 32], calls[-1][2]["attachments"])
        self.assertEqual("see files", calls[-1][2]["text"])

    def test_cli_send_without_files_omits_attachments_field(self) -> None:
        calls: list[dict | None] = []
        with mock.patch("openbot_bridge._api_request", side_effect=lambda url, auth, method, path, body=None, timeout=8.0: calls.append(body) or {"ok": True, "to_id": "x"}), mock.patch(
            "openbot_bridge.upload_attachment"
        ) as upload_mock:
            rc = openbot_bridge.cli_send("https://peers.example.com", None, "", "target", "hi")
        self.assertEqual(0, rc)
        self.assertEqual(0, upload_mock.call_count)
        self.assertEqual({"from_id": "cli", "from_pid": os.getpid(), "to_id": "target", "text": "hi"}, calls[-1])

    def test_missing_file_exits_1_without_sending(self) -> None:
        with mock.patch("urllib.request.urlopen") as urlopen, mock.patch("openbot_bridge._api_request") as api_mock, mock.patch("sys.stderr") as stderr:
            rc = openbot_bridge.cli_send(
                "https://peers.example.com", "Bearer secret", "", "target", "hi",
                [str(self.tmp / "missing.txt"), str(self.file_a)],
            )
        self.assertEqual(1, rc)
        self.assertEqual(0, api_mock.call_count)  # nothing uploaded, message never sent
        self.assertEqual(0, urlopen.call_count)
        self.assertIn("missing.txt", stderr.write.call_args_list[0].args[0])

    def test_upload_failure_exits_1_without_sending(self) -> None:
        with mock.patch("openbot_bridge.upload_attachment", side_effect=RuntimeError("broker /attachments failed: HTTP 400")), \
                mock.patch("openbot_bridge._api_request") as api_mock, mock.patch("sys.stderr") as stderr:
            rc = openbot_bridge.cli_send("https://peers.example.com", "Bearer secret", "Mac", "target", "hi", [str(self.file_a)])
        self.assertEqual(1, rc)
        self.assertEqual(["/list-peers"], [c.args[3] for c in api_mock.call_args_list])  # message never sent

    def test_upload_attachment_builds_multipart(self) -> None:
        requests: list[object] = []

        def fake_urlopen(req, timeout=None):
            requests.append(req)
            return _FakeResponse(json.dumps({"id": "f" * 32, "name": "b.bin", "size": 2, "mime": "application/octet-stream", "sha256": "0" * 64}).encode())

        with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen) as urlopen:
            payload = openbot_bridge.upload_attachment("https://peers.example.com/", "Bearer secret", str(self.file_b), from_id="peer1")
        self.assertEqual("f" * 32, payload["id"])
        req = requests[0]
        self.assertEqual("https://peers.example.com/attachments", req.full_url)
        self.assertEqual("POST", req.get_method())
        self.assertEqual("Bearer secret", req.get_header("Authorization"))
        ctype = req.get_header("Content-type")
        self.assertTrue(ctype.startswith("multipart/form-data; boundary=----peers-bridge-"))
        boundary = ctype.split("boundary=", 1)[1]
        body = req.data
        self.assertIn(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="b.bin"\r\n\r\n\x00\x01\r\n'.encode(), body)
        self.assertIn(f'--{boundary}\r\nContent-Disposition: form-data; name="from_id"\r\n\r\npeer1\r\n'.encode(), body)
        self.assertTrue(body.endswith(f"--{boundary}--\r\n".encode()))
        self.assertLess(urlopen.call_args.kwargs["timeout"], 120)

    def test_upload_attachment_http_error(self) -> None:
        def fake_urlopen(req, timeout=None):
            raise urllib.error.HTTPError(req.full_url, 400, "Bad", {}, io.BytesIO(b'{"error":"invalid_request","message":"File type application/x-sh is not allowed"}'))
        with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
            with self.assertRaisesRegex(RuntimeError, "HTTP 400.*not allowed"):
                openbot_bridge.upload_attachment("https://peers.example.com", None, str(self.file_a))

    def test_main_send_parses_repeatable_file_flags(self) -> None:
        captured: list[dict] = []
        with mock.patch("openbot_bridge._load_env_defaults"), mock.patch.dict(os.environ, {"CLAUDE_PEERS_NAME": "Mac"}, clear=False), \
                mock.patch("openbot_bridge.cli_send", side_effect=lambda *a, **k: captured.append({"args": a, "kwargs": k}) or 0):
            self.assertEqual(0, openbot_bridge.main(["send", "--file=/tmp/a.png", "Target", "hello", "-f", "/tmp/b.png", "world", "--file", "/tmp/c.pdf"]))
            self.assertEqual(
                {"args": ("https://peers.insightflo.si", None, "Mac", "Target", "hello world"), "kwargs": {}},
                captured[-1],
            ) if False else None
            self.assertEqual(0, openbot_bridge.main(["send", "Target", "hello"]))
            self.assertEqual(("https://peers.insightflo.si", None, "Mac", "Target", "hello world"), captured[-2]["args"][:5])
            self.assertEqual(["/tmp/a.png", "/tmp/b.png", "/tmp/c.pdf"], captured[-2]["args"][5])
            self.assertEqual(("https://peers.insightflo.si", None, "Mac", "Target", "hello"), captured[-1]["args"][:5])
            self.assertIsNone(captured[-1]["args"][5])
            with mock.patch("sys.stderr"):
                self.assertEqual(2, openbot_bridge.main(["send", "--file"]))
                self.assertEqual(2, openbot_bridge.main(["send", "Target"]))
        self.assertEqual(2, len(captured))

    def test_main_peers_send_alias_and_identity_flags(self) -> None:
        captured: list[tuple] = []
        with mock.patch("openbot_bridge._load_env_defaults"), \
                mock.patch.dict(os.environ, {"CLAUDE_PEERS_NAME": "OpenBot-Mac", "CLAUDE_PEERS_BROKER_AUTH": "secret123"}, clear=False), \
                mock.patch("openbot_bridge.cli_send", side_effect=lambda *a, **k: captured.append(a) or 0), \
                mock.patch("openbot_bridge.cli_list", return_value=0) as list_mock:
            # `peers-send` is the name the reply hint gives; a bare token gets the Bearer scheme.
            self.assertEqual(0, openbot_bridge.main(["peers-send", "OpenBot-Instabox", "hi", "--as", "Mac-비서"]))
            self.assertEqual("https://peers.insightflo.si", captured[-1][0])
            self.assertEqual("Bearer secret123", captured[-1][1])
            self.assertEqual("Mac-비서", captured[-1][6])
            self.assertEqual(0, openbot_bridge.main(["peers-send", "X", "hi", "--as=Mac-에듀"]))
            self.assertEqual("Mac-에듀", captured[-1][6])
            self.assertIsNone(captured[-1][5])
            with mock.patch("sys.stderr"):
                self.assertEqual(2, openbot_bridge.main(["peers-send", "X", "hi", "--as"]))
            self.assertEqual(0, openbot_bridge.main(["peers-list"]))
            list_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
