# -*- coding: utf-8 -*-
"""Regression tests for defects raised in REVIEW_PASS1.md / INDEPENDENT_AUDIT.md."""
import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from helpers import FakeEvent, FakeSp, make_config, make_gate  # noqa: E402
from test_manager import build_manager  # noqa: E402

from astrbot_plugin_task_workspace.context_builder import build_snapshot  # noqa: E402
from astrbot_plugin_task_workspace.exec_adapter import (  # noqa: E402
    ExecutorError,
    FakeRemoteExecutor,
    SshRemoteExecutor,
    unpack_watch,
)
from astrbot_plugin_task_workspace.manager import (  # noqa: E402
    WorkspaceError,
    WorkspaceManager,
)
from astrbot_plugin_task_workspace.memory_gate import MemoryGate  # noqa: E402
from astrbot_plugin_task_workspace.models import RunStatus  # noqa: E402
from astrbot_plugin_task_workspace.notify import Notifier  # noqa: E402
from astrbot_plugin_task_workspace.store import Store  # noqa: E402


async def _wait(cond, timeout=3.0):
    for _ in range(int(timeout / 0.01)):
        if cond():
            return True
        await asyncio.sleep(0.01)
    return cond()


class RawNdjsonWatchTests(unittest.IsolatedAsyncioTestCase):
    async def _watch(self, records):
        ex = SshRemoteExecutor(host="h", port=22, user="u", work_dir="D:\\x")
        async def fake(command, stdin_obj=None):
            return records
        ex._run_and_parse = fake
        return await ex.watch("t1", 0)

    async def test_zero_events(self):
        events, state = await self._watch([{"type": "_state", "state": {"status": "running"}}])
        self.assertEqual(events, [])
        self.assertEqual(state["status"], "running")

    async def test_one_and_two_and_eleven_events(self):
        for n in (1, 2, 11):
            records = [{"seq": i + 1, "type": "text", "text": "x"} for i in range(n)]
            records.append({"seq": 10**9, "type": "_state", "state": {"status": "succeeded"}})
            events, state = await self._watch(records)
            self.assertEqual(len(events), n)
            self.assertEqual(state["status"], "succeeded")
            self.assertTrue(all(e.get("type") != "_state" for e in events))

    async def test_aggregate_envelope(self):
        records = [{"events": [{"seq": 1, "type": "text"}],
                    "state": {"status": "succeeded"}}]
        events, state = await self._watch(records)
        self.assertEqual(len(events), 1)
        self.assertEqual(state["status"], "succeeded")

    async def test_malformed_records_skipped(self):
        records = ["not a dict", 42, {"seq": 1, "type": "text"},
                   {"type": "_state", "state": {"status": "running"}}]
        events, state = await self._watch(records)
        self.assertEqual(len(events), 1)
        self.assertEqual(state["status"], "running")

    def test_cursor_excludes_state_sentinel(self):
        records = [{"seq": 3, "type": "text"},
                   {"seq": 10**9, "type": "_state", "state": {"status": "running"}}]
        events, _ = unpack_watch(records)
        self.assertEqual(max(int(e["seq"]) for e in events), 3)


class RecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def _manager(self, executor=None):
        mgr, store, ctx, sp = build_manager(
            self.tmp.name, executor=executor or FakeRemoteExecutor())
        await store.migrate()
        return mgr, store, ctx, sp

    async def test_reconcile_running_does_not_resubmit(self):
        from astrbot_plugin_task_workspace.models import Run
        mgr, store, ctx, sp = await self._manager()
        ev = FakeEvent()
        ws = await mgr.create_workspace(ev, "rec")
        run = Run(id="run_bbbbbbbb", workspace_id=ws.id, executor="dsh",
                  request_id="req_resume", input_snapshot="{}",
                  status=RunStatus.RUNNING.value, remote_task_id="t999")
        await store.insert_run(run)
        mgr.executor.reconcile_result = {"ok": True, "found": True,
                                          "task_id": "t999", "status": "running"}
        mgr.executor.default_watch = ([], {"status": "succeeded", "final": "ok"})
        await mgr._recover_reconcile(run.id)
        await mgr.wait_background(timeout=3)
        self.assertEqual(mgr.executor.submitted, [])
        final = await store.get_run(run.id)
        self.assertEqual(final.status, RunStatus.SUCCEEDED.value)
        store.close()

    async def test_startup_recovers_queued_without_remote(self):
        from astrbot_plugin_task_workspace.models import Run
        mgr, store, ctx, sp = await self._manager()
        ev = FakeEvent()
        ws = await mgr.create_workspace(ev, "q")
        run = Run(id="run_aaaaaaaa", workspace_id=ws.id, executor="dsh",
                  request_id="req_orphan", input_snapshot="{}",
                  status=RunStatus.QUEUED.value)
        await store.insert_run(run)
        mgr.executor.default_watch = ([], {"status": "succeeded", "final": "ok"})
        await mgr.resume_active_runs()
        await mgr.wait_background(timeout=3)
        # A known-unsubmitted queued run must actually execute once, not be
        # marked failed. It reaches a terminal state via the normal poll path.
        self.assertEqual(len(mgr.executor.submitted), 1)
        self.assertEqual((await store.get_run(run.id)).status,
                         RunStatus.SUCCEEDED.value)
        store.close()

    async def test_startup_submitting_without_remote_reconciles(self):
        from astrbot_plugin_task_workspace.models import Run
        mgr, store, ctx, sp = await self._manager()
        ev = FakeEvent()
        ws = await mgr.create_workspace(ev, "s")
        run = Run(id="run_cccccccc", workspace_id=ws.id, executor="dsh",
                  request_id="req_ambiguous", input_snapshot="{}",
                  status=RunStatus.SUBMITTING.value, submit_attempts=1)
        await store.insert_run(run)
        mgr.executor.reconcile_result = {"ok": True, "found": False}
        await mgr.resume_active_runs()
        await mgr.wait_background(timeout=3)
        # Uncertain submit is never re-driven; authoritative absence fails it.
        self.assertEqual(mgr.executor.submitted, [])
        self.assertEqual((await store.get_run(run.id)).status,
                         RunStatus.FAILED.value)
        store.close()

    async def test_reconcile_unsupported_stays_unknown(self):
        mgr, store, ctx, sp = await self._manager()
        ev = FakeEvent()
        ws = await mgr.create_workspace(ev, "u")
        run = await mgr.start_run(ev, "dsh")
        await store.update_run(run.id, status=RunStatus.NEEDS_RECONCILIATION.value)
        mgr.executor.reconcile_result = {"ok": False, "error": "unsupported"}
        out = await mgr.reconcile(ev, run.id)
        self.assertEqual(out.status, RunStatus.NEEDS_RECONCILIATION.value)
        store.close()

    async def test_terminal_not_revived(self):
        mgr, store, ctx, sp = await self._manager()
        ev = FakeEvent()
        ws = await mgr.create_workspace(ev, "t")
        run = await mgr.start_run(ev, "dsh")
        await store.update_run(run.id, status=RunStatus.SUCCEEDED.value)
        # stale update must not revive a terminal run
        changed = await store.update_run(run.id, status=RunStatus.RUNNING.value)
        self.assertFalse(changed)
        self.assertEqual((await store.get_run(run.id)).status, RunStatus.SUCCEEDED.value)
        store.close()

    async def test_shutdown_cancels_blocking_workers_and_closes(self):
        class BlockingExecutor(FakeRemoteExecutor):
            async def watch(self, task_id, after=0):
                await asyncio.Event().wait()
        ex = BlockingExecutor()
        mgr, store, ctx, sp = await self._manager(executor=ex)
        ev = FakeEvent()
        ws = await mgr.create_workspace(ev, "shut")
        await mgr.start_run(ev, "dsh")
        await _wait(lambda: bool(ex.submitted))
        await asyncio.wait_for(mgr.shutdown(), timeout=3)
        store.close()
        with self.assertRaises(Exception):
            store._connect()

    async def test_two_stores_same_db_reserve_one_active(self):
        path = os.path.join(self.tmp.name, "shared.db")
        s1 = Store(path)
        s2 = Store(path)
        await s1.migrate()
        await s2.migrate()
        from astrbot_plugin_task_workspace.models import Run, Workspace
        ws = Workspace(id="ws_aaaa1111", owner_user_id="1", umo="u:1:1",
                       conversation_id="c1", title="t")
        await s1.insert_workspace(ws)
        r1 = Run(id="run_11111111", workspace_id=ws.id, executor="dsh",
                 request_id="r1", input_snapshot="{}", status=RunStatus.RUNNING.value)
        await s2.reserve_run(r1, [])
        r2 = Run(id="run_22222222", workspace_id=ws.id, executor="dsh",
                 request_id="r2", input_snapshot="{}", status=RunStatus.QUEUED.value)
        with self.assertRaises(ValueError):
            await s1.reserve_run(r2, [])
        s1.close()
        s2.close()

    async def test_workspace_cwd_distinct(self):
        mgr, store, ctx, sp = await self._manager()
        ev = FakeEvent()
        ws1 = await mgr.create_workspace(ev, "one")
        ws2 = await mgr.create_workspace(ev, "two")
        mgr.executor.default_watch = ([], {"status": "succeeded", "final": "ok"})
        await mgr.switch(ev, ws1.id)
        await mgr.start_run(ev, "dsh")
        await mgr.switch(ev, ws2.id)
        await mgr.start_run(ev, "dsh")
        await mgr.wait_background(timeout=3)
        a = (await store.get_workspace(ws1.id)).cwd
        b = (await store.get_workspace(ws2.id)).cwd
        self.assertTrue(a and b)
        self.assertNotEqual(a, b)
        store.close()


class OutboxStarvationTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_item_does_not_starve_newer(self):
        tmp = tempfile.TemporaryDirectory()
        try:
            store = Store(os.path.join(tmp.name, "t.db"))
            await store.migrate()
            notifier = Notifier(store, None, interval=999)
            await notifier.enqueue("run_1", "k1", "u:1:1", "first")
            async def bad(session, content):
                raise RuntimeError("down")
            await notifier.flush_once(sender=bad)
            await notifier.enqueue("run_2", "k2", "u:1:1", "second")
            sent = []
            async def good(session, content):
                sent.append(content)
            await notifier.flush_once(sender=good)
            self.assertEqual(sent, ["second"])
            store.close()
        finally:
            tmp.cleanup()


class LongConversationTests(unittest.TestCase):
    def test_long_conversation_keeps_requirement(self):
        from helpers import FakeConversation
        conv = FakeConversation("c", "u", history=[
            {"role": "user", "content": "ORIGINAL GOAL " + "x" * 100},
            {"role": "assistant", "content": "y" * 4000},
            {"role": "user", "content": "latest ask " + "z" * 100},
        ])
        snap, truncated = build_snapshot(
            conv=conv, title="t", user_requirement="do the thing",
            pending_amendments=["and test"], prior_result="r" * 5000, max_chars=800)
        self.assertIn("do the thing", snap["prompt"])
        self.assertIn("and test", snap["prompt"])
        self.assertIn("ORIGINAL GOAL", snap["prompt"])
        self.assertIn("latest ask", snap["prompt"])


class MemoryCapabilityRevocationTests(unittest.TestCase):
    def test_invalidate_rechecks_live(self):
        from astrbot_plugin_task_workspace.memory_capability import (
            CAPABILITY_VERSION, MemoryCapabilityRegistry)
        from helpers import FakeProvider
        reg = MemoryCapabilityRegistry()
        gate = MemoryGate(registry=reg)
        self.assertFalse(gate.detect().available)
        provider = FakeProvider(True)
        reg.publish(provider, CAPABILITY_VERSION)
        # The live registry is consulted every probe: no stale negative window.
        self.assertTrue(gate.detect().available)
        reg.revoke(provider)
        # Revocation is honoured immediately without waiting for invalidate().
        self.assertFalse(gate.detect().available)


if __name__ == "__main__":
    unittest.main()
