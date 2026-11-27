from __future__ import annotations

import io
import json
import os
import queue
import threading
import time
import unittest
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

from peers_bridge import bridge as bridge_module
from peers_bridge.bridge import ClaudePeersBridge


class Context:
    def __init__(self, accepted=True):
        self.accepted = accepted
        self.injected = []
        self.received = threading.Event()

    def inject_message(self, content, role="system"):
        self.injected.append((role, content))
        self.received.set()
        return self.accepted


class BridgeTestCase(unittest.TestCase):
    def make_bridge(self, ctx=None, **env):
        settings = {
            "CLAUDE_PEERS_BRIDGE_AUTOSTART": "0",
            "CLAUDE_PEERS_BRIDGE_TRANSPORT": "sse",
            "CLAUDE_PEERS_BRIDGE_SAFETY_POLL_SEC": "30",
            "CLAUDE_PEERS_WAKE_DEBOUNCE_SEC": "0",
            "CLAUDE_PEERS_WAKE_CMD": "",
            "CLAUDE_PEERS_WAKE_WEBHOOK": "",
        }
        settings.update(env)
        with (
            mock.patch.dict(os.environ, settings),
            mock.patch.object(bridge_module, "_git_root", return_value=None),
        ):
            bridge = ClaudePeersBridge(ctx or Context())
        bridge._ensure_broker = mock.Mock()
        bridge._post_json = mock.Mock(return_value={"ok": True, "messages": []})
        bridge._set_peer_id("peer-one")
        self.addCleanup(bridge.stop)
        return bridge


class BridgePushTests(BridgeTestCase):
    def test_background_registration_skips_heartbeat_but_send_verifies(self):
        bridge = self.make_bridge()
        bridge._ensure_registered(verify_heartbeat=False)
        bridge._post_json.assert_not_called()
        bridge.send_message("other", "hello")
        self.assertEqual(
            ["/heartbeat", "/send-message"],
            [c.args[0] for c in bridge._post_json.call_args_list],
        )
        self.assertEqual("peer-one", bridge._post_json.call_args.args[1]["from_id"])

    def test_explicit_tools_all_verify_liveness(self):
        bridge = self.make_bridge()
        for call in (
            bridge.list_peers,
            bridge.check_messages,
            lambda: bridge.set_summary("summary"),
            lambda: bridge.set_alias("alias"),
        ):
            bridge._post_json.reset_mock()
            call()
            self.assertEqual("/heartbeat", bridge._post_json.call_args_list[0].args[0])

    def test_stale_sender_is_reregistered_before_send(self):
        bridge = self.make_bridge()
        bridge._post_json.side_effect = [
            {"reregister": True},
            {"id": "peer-new"},
            {"ok": True},
        ]
        bridge.send_message("other", "hello")
        self.assertEqual("peer-new", bridge._post_json.call_args.args[1]["from_id"])

    def test_duplicate_pending_message_injects_once(self):
        bridge = self.make_bridge()
        bridge._queue_injection("one", 7)
        bridge._queue_injection("duplicate", 7)
        bridge._drain_pending_injections()
        self.assertEqual([("system", "one")], bridge._ctx.injected)
        self.assertEqual(set(), bridge._queued_message_ids)

    def test_failed_ack_retries_ack_without_reinjecting(self):
        bridge = self.make_bridge()
        bridge._mark_message_delivered = mock.Mock(
            side_effect=[RuntimeError("offline"), None]
        )
        bridge._queue_injection("one", 7)
        bridge._drain_pending_injections()
        self.assertEqual({7}, bridge._queued_message_ids)
        bridge._queue_injection("duplicate from safety poll", 7)
        bridge._drain_pending_injections()
        self.assertEqual([("system", "one")], bridge._ctx.injected)
        self.assertEqual(
            [mock.call(7), mock.call(7)], bridge._mark_message_delivered.call_args_list
        )
        self.assertFalse(bridge._queued_message_ids)

    def test_injection_exception_preserves_retry(self):
        bridge = self.make_bridge()
        bridge._ctx.inject_message = mock.Mock(side_effect=[RuntimeError("busy"), True])
        bridge._queue_injection("one", 7)
        bridge._drain_pending_injections()
        self.assertEqual({7}, bridge._queued_message_ids)
        bridge._drain_pending_injections()
        self.assertEqual(2, bridge._ctx.inject_message.call_count)
        self.assertFalse(bridge._queued_message_ids)

    def test_peer_change_clears_old_pending_delivery_state(self):
        bridge = self.make_bridge()
        bridge._queue_injection("old", 7)
        bridge._injected_message_ids.add(7)
        bridge._set_peer_id("peer-new")
        self.assertFalse(bridge._queued_message_ids)
        self.assertFalse(bridge._injected_message_ids)
        self.assertTrue(bridge._pending_injections.empty())
        self.assertTrue(bridge._wake_event.is_set())

    def test_idle_context_is_woken_after_injection_attempt(self):
        ctx = Context(accepted=False)
        ctx.wake_session = mock.Mock(
            side_effect=lambda: self.assertEqual(1, len(ctx.injected))
        )
        bridge = self.make_bridge(ctx)
        bridge._queue_injection("pending", 1)
        bridge._drain_pending_injections()
        ctx.wake_session.assert_called_once_with()
        self.assertEqual({1}, bridge._queued_message_ids)

    def test_notify_fallback_and_missing_hooks_are_optional(self):
        ctx = Context(accepted=False)
        ctx.notify = mock.Mock()
        bridge = self.make_bridge(ctx)
        bridge._queue_injection("pending", 1)
        bridge._drain_pending_injections()
        ctx.notify.assert_called_once_with("Peer messages pending")
        bridge = self.make_bridge(Context(accepted=False))
        bridge._queue_injection("pending", 1)
        bridge._drain_pending_injections()
        self.assertEqual({1}, bridge._queued_message_ids)
        self.assertFalse(bridge.status_payload()["wake_configured"])

    def test_wake_command_is_split_and_never_uses_remote_content(self):
        bridge = self.make_bridge(
            Context(accepted=False),
            CLAUDE_PEERS_WAKE_CMD='wake-tool --title "peer pending"',
        )
        bridge._queue_injection('$(touch /tmp/unsafe); alias "bad"', 7)
        with mock.patch.object(bridge_module.subprocess, "run") as run:
            bridge._drain_pending_injections()
        run.assert_called_once_with(
            ["wake-tool", "--title", "peer pending"],
            shell=False,
            timeout=5.0,
            stdin=bridge_module.subprocess.DEVNULL,
            stdout=bridge_module.subprocess.DEVNULL,
            stderr=bridge_module.subprocess.DEVNULL,
        )

    def test_empty_or_malformed_command_does_not_execute(self):
        for command in ("", "   ", 'wake "unterminated'):
            with self.subTest(command=command):
                bridge = self.make_bridge(
                    Context(accepted=False), CLAUDE_PEERS_WAKE_CMD=command
                )
                bridge._queue_injection("pending", 1)
                with mock.patch.object(bridge_module.subprocess, "run") as run:
                    bridge._drain_pending_injections()
                run.assert_not_called()
                if "unterminated" in command:
                    self.assertIn("invalid wake command", bridge.state.last_error)

    def test_webhook_contains_only_minimal_metadata(self):
        bridge = self.make_bridge(
            Context(accepted=False),
            CLAUDE_PEERS_WAKE_WEBHOOK="https://example.invalid/wake",
        )
        bridge._queue_injection("secret remote message", 1)
        with mock.patch.object(bridge_module.urllib.request, "urlopen") as urlopen:
            bridge._drain_pending_injections()
        request = urlopen.call_args.args[0]
        self.assertEqual(
            {"event": "peer_messages_pending", "peer_id": "peer-one", "count": 1},
            json.loads(request.data),
        )
        self.assertEqual("POST", request.method)
        self.assertEqual("peers-bridge/1.0", request.get_header("User-agent"))
        self.assertEqual(3.0, urlopen.call_args.kwargs["timeout"])

    def test_non_http_webhook_is_rejected(self):
        bridge = self.make_bridge(
            Context(accepted=False), CLAUDE_PEERS_WAKE_WEBHOOK="file:///tmp/wake"
        )
        bridge._queue_injection("pending", 1)
        with mock.patch.object(bridge_module.urllib.request, "urlopen") as urlopen:
            bridge._drain_pending_injections()
        urlopen.assert_not_called()
        self.assertIn("http:// or https://", bridge.state.last_error)

    def test_debounce_and_single_flight_suppress_duplicate_wakes(self):
        ctx = Context(accepted=False)
        ctx.wake_session = mock.Mock()
        bridge = self.make_bridge(ctx, CLAUDE_PEERS_WAKE_DEBOUNCE_SEC="0.01")
        bridge._queue_injection("pending", 1)
        bridge._drain_pending_injections()
        bridge._drain_pending_injections()
        ctx.wake_session.assert_called_once()
        bridge._last_wake = float("-inf")
        bridge._wake_lock.acquire()
        try:
            bridge._wake_idle_session()
        finally:
            bridge._wake_lock.release()
        ctx.wake_session.assert_called_once()

    def test_wake_failure_preserves_queue(self):
        ctx = Context(accepted=False)
        ctx.wake_session = mock.Mock(side_effect=RuntimeError("unavailable"))
        bridge = self.make_bridge(ctx)
        bridge._queue_injection("pending", 1)
        bridge._drain_pending_injections()
        self.assertEqual({1}, bridge._queued_message_ids)
        self.assertIn("wake failed", bridge.state.last_error)

    def test_sse_404_clears_identity_and_backs_off(self):
        bridge = self.make_bridge()
        bridge._queue_injection("old", 1)
        waits = []

        def wait(delay):
            waits.append(delay)
            self.assertIsNone(bridge.state.peer_id)
            self.assertFalse(bridge._queued_message_ids)
            self.assertTrue(bridge._wake_event.is_set())
            bridge._ensure_registered(verify_heartbeat=False)
            return len(waits) == 4

        bridge._post_json.return_value = {"id": "peer-new"}
        with (
            mock.patch.object(bridge._stop_event, "wait", side_effect=wait),
            mock.patch.object(
                bridge_module.urllib.request,
                "urlopen",
                side_effect=lambda *a, **k: (_ for _ in ()).throw(
                    urllib.error.HTTPError(
                        a[0].full_url, 404, "missing", {}, io.BytesIO()
                    )
                ),
            ),
        ):
            bridge._sse_loop()
        self.assertEqual([1.0, 2.0, 5.0, 15.0], waits)
        self.assertEqual(4, bridge._ensure_broker.call_count)

    def test_sse_eof_and_connection_failures_back_off(self):
        bridge = self.make_bridge()
        waits = []
        response = mock.Mock()
        response.readline.return_value = b""
        with (
            mock.patch.object(
                bridge._stop_event,
                "wait",
                side_effect=lambda delay: waits.append(delay) or len(waits) == 4,
            ),
            mock.patch.object(
                bridge_module.urllib.request,
                "urlopen",
                side_effect=[
                    response,
                    OSError("offline"),
                    response,
                    OSError("offline"),
                ],
            ),
        ):
            bridge._sse_loop()
        self.assertEqual([1.0, 2.0, 5.0, 15.0], waits)
        self.assertFalse(bridge._sse_connected)

    def test_sse_revocation_event_and_http_410_halt_until_start(self):
        for source in ("event", "http"):
            with self.subTest(source=source):
                bridge = self.make_bridge()
                bridge._queue_injection("old", 1)
                response = mock.Mock()
                response.readline.return_value = b"event: revoked\n"
                error = urllib.error.HTTPError(
                    "http://localhost/events", 410, "gone", {}, io.BytesIO()
                )
                with mock.patch.object(
                    bridge_module.urllib.request,
                    "urlopen",
                    **(
                        {"return_value": response}
                        if source == "event"
                        else {"side_effect": error}
                    ),
                ):
                    bridge._sse_loop()
                self.assertTrue(bridge._stop_event.is_set())
                self.assertIsNone(bridge.state.peer_id)
                self.assertFalse(bridge._queued_message_ids)
                self.assertEqual("peer revoked by broker", bridge.state.last_error)
                with self.assertRaisesRegex(RuntimeError, "peer revoked"):
                    bridge._ensure_registered()
                bridge._post_json.return_value = {"id": "manual-restart"}
                with (
                    mock.patch.object(bridge, "_run_loop"),
                    mock.patch.object(bridge, "_sse_loop"),
                ):
                    bridge.start()
                    bridge._ensure_registered(verify_heartbeat=False)
                self.assertEqual("manual-restart", bridge.state.peer_id)

    def test_heartbeat_http_410_never_reregisters_or_sends(self):
        bridge = self.make_bridge()
        bridge._queue_injection("old", 1)
        bridge._post_json = ClaudePeersBridge._post_json.__get__(bridge)
        error = urllib.error.HTTPError(
            "http://localhost/heartbeat", 410, "gone", {}, io.BytesIO()
        )
        with mock.patch.object(
            bridge_module.urllib.request, "urlopen", side_effect=error
        ) as urlopen:
            with self.assertRaisesRegex(RuntimeError, "peer revoked"):
                bridge.send_message("other", "hello")
        self.assertEqual(1, urlopen.call_count)
        self.assertTrue(bridge._stop_event.is_set())
        self.assertEqual("peer revoked by broker", bridge.state.last_error)
        self.assertFalse(bridge._queued_message_ids)
        self.assertIsNone(bridge.state.peer_id)

    def test_status_and_poll_transport_do_not_start_sse(self):
        bridge = self.make_bridge(CLAUDE_PEERS_BRIDGE_TRANSPORT="poll")
        with mock.patch.object(bridge, "_run_loop"):
            bridge.start()
        status = bridge.status_payload()
        self.assertEqual("poll", status["transport"])
        self.assertFalse(status["sse_connected"])
        self.assertIsNone(bridge._sse_thread)


class StreamingFixture:
    """Real loopback HTTP/SSE sockets; never connects to a live broker."""

    def __init__(self):
        self.lines = queue.Queue()
        self.connections = queue.Queue()
        self.event_statuses = queue.Queue()
        self.lock = threading.Lock()
        self.polls = 0
        self.heartbeats = 0
        self.messages = []
        self.closed = threading.Event()
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_GET(self):
                try:
                    status = fixture.event_statuses.get_nowait()
                except queue.Empty:
                    status = 200
                if status != 200:
                    self.send_response(status)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                self.wfile.flush()
                fixture.connections.put((self.path, dict(self.headers)))
                while not fixture.closed.is_set():
                    try:
                        line = fixture.lines.get(timeout=0.05)
                    except queue.Empty:
                        continue
                    if line is None:
                        self.close_connection = True
                        return
                    try:
                        self.wfile.write(line)
                        self.wfile.flush()
                    except OSError:
                        return

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                with fixture.lock:
                    if self.path == "/poll-messages":
                        fixture.polls += 1
                        payload = {"messages": list(fixture.messages)}
                    elif self.path == "/register":
                        payload = {"id": "peer-new"}
                    elif self.path == "/heartbeat":
                        fixture.heartbeats += 1
                        payload = {"ok": True}
                    elif self.path == "/mark-message-delivered":
                        fixture.messages = [
                            m for m in fixture.messages if m["id"] != body["id"]
                        ]
                        payload = {"ok": True}
                    else:
                        payload = {"ok": True}
                data = json.dumps(payload).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.closed.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)


class BridgeStreamingTests(BridgeTestCase):
    def make_streaming_bridge(self, **env):
        fixture = StreamingFixture()
        self.addCleanup(fixture.close)
        bridge = self.make_bridge(**env)
        bridge._state.broker_url = fixture.url
        bridge._state.broker_auth = "Bearer test-token"
        bridge._post_json = ClaudePeersBridge._post_json.__get__(bridge)
        resolve = mock.patch.object(
            bridge_module,
            "_resolve_broker",
            return_value=(fixture.url, False, "Bearer test-token"),
        )
        resolve.start()
        self.addCleanup(resolve.stop)
        # Stop the bridge before the server or its resolver is removed.
        self.addCleanup(bridge.stop)
        return bridge, fixture

    def wait_until(self, predicate, timeout=2):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            threading.Event().wait(0.01)
        self.fail("background operation did not finish")

    def test_real_sse_wake_injects_immediately_and_idle_does_not_poll(self):
        bridge, fixture = self.make_streaming_bridge()
        bridge.start()
        path, headers = fixture.connections.get(timeout=2)
        self.assertEqual("/events?id=peer-one", path)
        self.assertEqual("Bearer test-token", headers["Authorization"])
        self.assertEqual("text/event-stream", headers["Accept"])
        self.assertEqual("peers-bridge/1.0", headers["User-Agent"])
        self.wait_until(lambda: fixture.polls >= 2)
        # A connected bridge must sleep past the old one-second polling tick.
        with fixture.lock:
            polls = fixture.polls
        threading.Event().wait(1.1)
        self.assertEqual(polls, fixture.polls)
        self.assertEqual(1, fixture.heartbeats)
        with fixture.lock:
            fixture.messages = [{"id": 7, "text": "push received"}]
        started = time.monotonic()
        fixture.lines.put(b"event: wake\ndata: {}\n\n")
        self.assertTrue(bridge._ctx.received.wait(0.75))
        self.assertLess(time.monotonic() - started, 0.75)
        self.wait_until(lambda: not fixture.messages)
        self.assertIn("push received", bridge._ctx.injected[0][1])
        bridge.stop()
        self.assertFalse(bridge._thread.is_alive())
        self.assertFalse(bridge._sse_thread.is_alive())

    def test_real_data_messages_wakes_and_peer_change_reconnects(self):
        bridge, fixture = self.make_streaming_bridge()
        bridge.start()
        fixture.connections.get(timeout=2)
        self.wait_until(lambda: fixture.polls >= 2)
        with fixture.lock:
            fixture.messages = [{"id": 9, "text": "data notification"}]
        fixture.lines.put(b'data: {"messages": 1}\n\n')
        self.assertTrue(bridge._ctx.received.wait(0.75))
        self.wait_until(lambda: not fixture.messages)
        bridge._set_peer_id("peer-two")
        # Let the fixture's old server handler terminate as the client reconnects.
        fixture.lines.put(None)
        path, _ = fixture.connections.get(timeout=2.5)
        self.assertEqual("/events?id=peer-two", path)
        self.assertTrue(bridge._wake_event.is_set() or fixture.polls >= 3)

    def test_disconnected_sse_falls_back_to_polling(self):
        bridge, fixture = self.make_streaming_bridge(
            CLAUDE_PEERS_BRIDGE_TRANSPORT="poll"
        )
        with mock.patch.object(bridge_module, "DEFAULT_POLL_INTERVAL", 0.05):
            bridge.start()
            self.wait_until(lambda: fixture.polls >= 3)
            self.assertEqual(1, fixture.heartbeats)
            bridge.stop()
        self.assertTrue(fixture.connections.empty())

    def test_real_sse_404_reregisters_and_reconnects(self):
        bridge, fixture = self.make_streaming_bridge()
        fixture.event_statuses.put(404)
        bridge.start()
        path, _ = fixture.connections.get(timeout=2.5)
        self.assertEqual("/events?id=peer-new", path)
        self.assertEqual("peer-new", bridge.status_payload()["peer_id"])
        self.wait_until(lambda: bridge.status_payload()["sse_connected"])

    def test_real_revocation_stops_both_background_threads(self):
        bridge, fixture = self.make_streaming_bridge()
        bridge.start()
        fixture.connections.get(timeout=2)
        fixture.lines.put(b"event: revoked\ndata: {}\n\n")
        self.wait_until(
            lambda: not bridge._thread.is_alive() and not bridge._sse_thread.is_alive()
        )
        self.assertFalse(bridge.status_payload()["running"])
        self.assertIsNone(bridge.status_payload()["peer_id"])
        self.assertEqual(
            "peer revoked by broker", bridge.status_payload()["last_error"]
        )
        with self.assertRaisesRegex(RuntimeError, "peer revoked"):
            bridge.send_message("other", "blocked")

    def test_sse_outage_polls_at_legacy_interval(self):
        bridge, fixture = self.make_streaming_bridge()
        fixture.event_statuses.put(503)
        with mock.patch.object(bridge_module, "DEFAULT_POLL_INTERVAL", 0.05):
            bridge.start()
            self.wait_until(lambda: fixture.polls >= 3)
            self.assertFalse(bridge.status_payload()["sse_connected"])
            bridge.stop()

    def test_connected_heartbeat_deadline_does_not_trigger_extra_polls(self):
        bridge, fixture = self.make_streaming_bridge()
        with mock.patch.object(bridge_module, "DEFAULT_HEARTBEAT_INTERVAL", 0.1):
            bridge.start()
            fixture.connections.get(timeout=2)
            self.wait_until(lambda: fixture.polls >= 2)
            initial_polls = fixture.polls
            self.wait_until(lambda: fixture.heartbeats >= 3)
            self.assertEqual(initial_polls, fixture.polls)
            bridge.stop()

    def test_safety_poll_recovers_silent_missing_wake(self):
        bridge, fixture = self.make_streaming_bridge(
            CLAUDE_PEERS_BRIDGE_SAFETY_POLL_SEC="0.1"
        )
        bridge.start()
        fixture.connections.get(timeout=2)
        self.wait_until(lambda: fixture.polls >= 2)
        with fixture.lock:
            fixture.messages = [{"id": 10, "text": "safety recovery"}]
        self.assertTrue(bridge._ctx.received.wait(0.75))
        self.wait_until(lambda: not fixture.messages)
        self.assertEqual(1, len(bridge._ctx.injected))


if __name__ == "__main__":
    unittest.main()
