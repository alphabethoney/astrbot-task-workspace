# -*- coding: utf-8 -*-
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import helpers  # noqa: F401,E402  (adds the plugin package to sys.path)

from astrbot_plugin_task_workspace.exec_adapter import (  # noqa: E402
    BridgeDispatcherAdapter,
    ExecutorError,
    FakeRemoteExecutor,
    SshRemoteExecutor,
    _PostDispatchError,
    _PreDispatchError,
    extract_result,
    unpack_watch,
)


class UnpackTests(unittest.TestCase):
    def test_tuple_shape(self):
        events, state = unpack_watch(([{"seq": 1}], {"status": "running"}))
        self.assertEqual(events, [{"seq": 1}])
        self.assertEqual(state["status"], "running")

    def test_dict_shape(self):
        events, state = unpack_watch({"events": [{"seq": 2}], "state": {"status": "ok"}})
        self.assertEqual(len(events), 1)
        self.assertEqual(state["status"], "ok")

    def test_records_shape_with_state_sentinel(self):
        records = [{"seq": 1, "type": "text"}, {"type": "_state", "state": {"status": "succeeded"}}]
        events, state = unpack_watch(records)
        self.assertEqual(events, [{"seq": 1, "type": "text"}])
        self.assertEqual(state["status"], "succeeded")

    def test_garbage(self):
        events, state = unpack_watch("nonsense")
        self.assertEqual(events, [])
        self.assertEqual(state, {})


class ExtractTests(unittest.TestCase):
    def test_final_and_artifacts(self):
        state = {"status": "succeeded", "final": "hello",
                 "artifacts": [{"path": "out/report.txt"}, "raw.txt"]}
        text, arts = extract_result(state)
        self.assertEqual(text, "hello")
        self.assertEqual(arts, ["out/report.txt", "raw.txt"])

    def test_result_alias(self):
        text, arts = extract_result({"result": "body"})
        self.assertEqual(text, "body")
        self.assertEqual(arts, [])


class FakeExecutorDedupeTests(unittest.IsolatedAsyncioTestCase):
    async def test_duplicate_request_id_is_idempotent(self):
        ex = FakeRemoteExecutor()
        t1 = await ex.submit("req_1", "p", "D:\\ws", "read-only", 60, "dsh")
        t2 = await ex.submit("req_1", "p", "D:\\ws", "read-only", 60, "dsh")
        self.assertEqual(t1, t2)
        self.assertEqual(len(ex.submitted), 1)


class BridgeAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_wraps_dispatcher_and_forwards(self):
        calls = {}

        class D:
            async def submit(self, req):
                calls["req"] = req
                return "t1"

            async def watch(self, task_id, after=0):
                return ([{"seq": after + 1}], {"status": "running"})

            async def cancel(self, task_id):
                calls["cancel"] = task_id

        adapter = BridgeDispatcherAdapter(D())
        tid = await adapter.submit("req_9", "p", "D:\\ws", "read-only", 30, "dsh")
        self.assertEqual(tid, "t1")
        self.assertEqual(calls["req"]["request_id"], "req_9")
        events, state = await adapter.watch("t1", 0)
        self.assertEqual(events[0]["seq"], 1)
        await adapter.cancel("t1")
        self.assertEqual(calls["cancel"], "t1")

    async def test_bridge_without_reconcile_returns_unknown_not_absent(self):
        class D:
            async def submit(self, req):
                return "t1"

            async def watch(self, task_id, after=0):
                return ([], {})

            async def cancel(self, task_id):
                pass

        adapter = BridgeDispatcherAdapter(D())
        out = await adapter.reconcile(request_id="req_9")
        self.assertFalse(out["ok"])
        self.assertEqual(out.get("error"), "unsupported")

    async def test_bridge_submit_error_is_ambiguous(self):
        class D:
            async def submit(self, req):
                raise RuntimeError("connection reset")

            async def watch(self, task_id, after=0):
                return ([], {})

            async def cancel(self, task_id):
                pass

        adapter = BridgeDispatcherAdapter(D())
        with self.assertRaises(ExecutorError) as cm:
            await adapter.submit("r", "p", "D:\\ws", "read-only", 30, "dsh")
        self.assertTrue(cm.exception.ambiguous)
        self.assertFalse(cm.exception.connectivity)


class _FakeStream:
    def __init__(self, data=b"", channel=None):
        self._data = data
        self.channel = channel

    def write(self, payload):
        pass

    def flush(self):
        pass

    def read(self):
        return self._data


class _FakeChannel:
    def __init__(self, *, exec_error=None, stdout_data=b"", stderr_data=b"",
                 exit_code=0):
        self.exec_error = exec_error
        self.stdout_data = stdout_data
        self.stderr_data = stderr_data
        self.exit_code = exit_code
        self.exec_calls = 0
        self.timeout = None

    def settimeout(self, timeout):
        self.timeout = timeout

    def exec_command(self, command):
        self.exec_calls += 1
        if self.exec_error is not None:
            raise self.exec_error

    def makefile_stdin(self, mode, bufsize):
        return _FakeStream(channel=self)

    def makefile(self, mode, bufsize):
        return _FakeStream(data=self.stdout_data, channel=self)

    def makefile_stderr(self, mode, bufsize):
        return _FakeStream(data=self.stderr_data, channel=self)

    def recv_exit_status(self):
        return self.exit_code

    def shutdown_write(self):
        pass


class _FakeTransport:
    def __init__(self, *, channel=None, open_error=None):
        self.channel = channel
        self.open_error = open_error

    def open_session(self, timeout=None):
        if self.open_error is not None:
            raise self.open_error
        return self.channel


class _FakeClient:
    def __init__(self, transport):
        self._transport = transport
        self.closed = False

    def get_transport(self):
        return self._transport

    def close(self):
        self.closed = True


class ExecDispatchBoundaryTests(unittest.IsolatedAsyncioTestCase):
    """Regression tests for the exec-request acknowledgement boundary.

    ``client.exec_command`` sends the exec request *before* it waits for the
    acknowledgement, so a failure there is ambiguous. Only the channel-open
    request (and connect/auth) can be proven to have not dispatched the
    command.
    """

    def _executor(self):
        return SshRemoteExecutor(host="h", port=22, user="u", work_dir="D:\\x")

    async def test_connect_auth_failure_is_definite_predispatch(self):
        ex = self._executor()

        def bad_client():
            raise RuntimeError("authentication failed")

        ex._client = bad_client
        with self.assertRaises(ExecutorError) as cm:
            await ex.submit("r", "p", "D:\\ws", "read-only", 60, "dsh")
        self.assertTrue(cm.exception.connectivity)
        self.assertFalse(cm.exception.ambiguous)

    async def test_channel_open_failure_is_proven_predispatch(self):
        ex = self._executor()
        channel = _FakeChannel()
        transport = _FakeTransport(channel=channel,
                                   open_error=RuntimeError("channel open failed"))
        ex._client = lambda: _FakeClient(transport)
        with self.assertRaises(ExecutorError) as cm:
            await ex.submit("r", "p", "D:\\ws", "read-only", 60, "dsh")
        self.assertTrue(cm.exception.connectivity)
        self.assertFalse(cm.exception.ambiguous)
        self.assertEqual(channel.exec_calls, 0)

    async def test_exec_request_ack_loss_is_ambiguous_postdispatch(self):
        ex = self._executor()
        channel = _FakeChannel(exec_error=RuntimeError("Channel closed."))
        ex._client = lambda: _FakeClient(_FakeTransport(channel=channel))
        with self.assertRaises(ExecutorError) as cm:
            await ex.submit("r", "p", "D:\\ws", "read-only", 60, "dsh")
        self.assertTrue(cm.exception.ambiguous)
        self.assertFalse(cm.exception.connectivity)
        # The exec request was attempted exactly once: no blind resubmit.
        self.assertEqual(channel.exec_calls, 1)

    async def test_ambiguous_ack_loss_does_not_resubmit_inside_adapter(self):
        ex = self._executor()
        channel = _FakeChannel(exec_error=RuntimeError("response never arrived"))
        ex._client = lambda: _FakeClient(_FakeTransport(channel=channel))
        for _ in range(2):
            with self.assertRaises(ExecutorError) as cm:
                await ex.submit("r", "p", "D:\\ws", "read-only", 60, "dsh")
            self.assertTrue(cm.exception.ambiguous)
        # Each explicit submit is one attempt; the adapter never silently
        # retries an ambiguous acknowledgement loss on its own.
        self.assertEqual(channel.exec_calls, 2)

    async def test_successful_exec_path_still_works(self):
        ex = self._executor()
        payload = b'{"ok": true, "task_id": "t123"}\n'
        channel = _FakeChannel(stdout_data=payload)
        client = _FakeClient(_FakeTransport(channel=channel))
        ex._client = lambda: client
        task_id = await ex.submit("r", "p", "D:\\ws", "read-only", 60, "dsh")
        self.assertEqual(task_id, "t123")
        self.assertEqual(channel.exec_calls, 1)
        self.assertTrue(client.closed)


class SubmitTransportTests(unittest.IsolatedAsyncioTestCase):
    def _executor(self):
        return SshRemoteExecutor(host="h", port=22, user="u", work_dir="D:\\x")

    async def test_predispatch_connect_failure_is_definite(self):
        ex = self._executor()

        async def fake(command, stdin_obj=None):
            raise _PreDispatchError("auth failed")

        ex._exec = fake
        with self.assertRaises(ExecutorError) as cm:
            await ex.submit("r", "p", "D:\\ws", "read-only", 60, "dsh")
        self.assertTrue(cm.exception.connectivity)
        self.assertFalse(cm.exception.ambiguous)

    async def test_postdispatch_read_error_is_ambiguous(self):
        ex = self._executor()

        async def fake(command, stdin_obj=None):
            raise _PostDispatchError("read timed out")

        ex._exec = fake
        with self.assertRaises(ExecutorError) as cm:
            await ex.submit("r", "p", "D:\\ws", "read-only", 60, "dsh")
        self.assertTrue(cm.exception.ambiguous)
        self.assertFalse(cm.exception.connectivity)

    async def test_scalar_response_is_ambiguous(self):
        ex = self._executor()

        async def fake_run(command, stdin_obj=None):
            return ["not-a-dict"]

        ex._run_and_parse = fake_run
        with self.assertRaises(ExecutorError) as cm:
            await ex.submit("r", "p", "D:\\ws", "read-only", 60, "dsh")
        self.assertTrue(cm.exception.ambiguous)

    async def test_empty_task_id_is_ambiguous(self):
        ex = self._executor()

        async def fake_run(command, stdin_obj=None):
            return [{"ok": True, "task_id": ""}]

        ex._run_and_parse = fake_run
        with self.assertRaises(ExecutorError) as cm:
            await ex.submit("r", "p", "D:\\ws", "read-only", 60, "dsh")
        self.assertTrue(cm.exception.ambiguous)


if __name__ == "__main__":
    unittest.main()
