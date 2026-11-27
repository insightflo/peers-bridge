from __future__ import annotations

import unittest
from unittest import mock

import openbot_bridge


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


if __name__ == "__main__":
    unittest.main()
