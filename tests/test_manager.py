# -*- coding: utf-8 -*-
import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from helpers import (  # noqa: E402
    FakeContext,
    FakeEvent,
    FakeSp,
    make_config,
    make_gate,
)

from astrbot_plugin_task_workspace.exec_adapter import (  # noqa: E402
    ExecutorError,
    FakeRemoteExecutor,
)
from astrbot_plugin_task_workspace.manager import (  # noqa: E402
    AccessDeniedError,
    AmbiguousIdError,
    WorkspaceError,
    WorkspaceManager,
)
from astrbot_plugin_task_workspace.models import RunStatus  # noqa: E402
from astrbot_plugin_task_workspace.notify import Notifier  # noqa: E402
from astrbot_plugin_task_workspace.store import Store  # noqa: E402


async def _wait_for(cond, timeout=3.0, interval=0.01):
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if cond():
            return True
        await asyncio.sleep(interval)
    return cond()


def build_manager(tmp, *, config=None, executor=None, cm=None, gate=None, sp=None):
    config = config or make_config()
    store = Store(os.path.join(tmp, "t.db"))
    ctx = FakeContext(cm)
    notifier = Notifier(store, ctx, interval=999)
    sp = sp if sp is not None else FakeSp()
    manager = WorkspaceManager(
        config=config,
        store=store,
        executor=executor or FakeRemoteExecutor(),
        notifier=notifier,
        cm=ctx.conversation_manager,
        memory_gate=gate or make_gate(sp=sp),
        results_dir=os.path.join(tmp, "results"),
        sp=sp,
        sleep_fn=lambda s: asyncio.sleep(0.01),
    )
    return manager, store, ctx, sp


class ManagerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.manager, self.store, self.ctx, self.sp = build_manager(self.tmpdir.name)
        await self.store.migrate()

    async def asyncTearDown(self):
        await self.manager.wait_background(timeout=3)
        self.store.close()
        self.tmpdir.cleanup()

    async def _new_ws(self, title="alpha"):
        ev = FakeEvent()
        return await self.manager.create_workspace(ev, title), ev

    # ------------------------------------------------------------ lifecycle
    async def test_create_workspace_and_current(self):
        ws, ev = await self._new_ws("My Workspace")
        self.assertTrue(ws.conversation_id)
        current = await self.manager.current_workspace(ev)
        self.assertEqual(current.id, ws.id)
        self.assertEqual(current.title, "My Workspace")
        # Persona is bound on the conversation only; the UMO-wide session
        # service config must never be mutated (daily personality stays intact).
        self.assertNotIn(
            ("umo", ev.unified_msg_origin, "session_service_config"), self.sp._umo)
        conv = await self.ctx.conversation_manager.get_conversation(
            ev.unified_msg_origin, ws.conversation_id)
        self.assertEqual(conv.persona_id, "运维")

    async def test_unknown_cid_has_no_workspace(self):
        ev = FakeEvent()
        await self.ctx.conversation_manager.new_conversation(ev.unified_msg_origin)
        self.assertIsNone(await self.manager.current_workspace(ev))

    async def test_force_persona_does_not_mutate_global_session(self):
        ev = FakeEvent()
        umo = ev.unified_msg_origin
        # A pre-existing daily session persona must survive workspace creation.
        await self.sp.put_async("umo", umo, "session_service_config",
                                {"persona_id": "日常人格"})
        await self.manager.create_workspace(ev, "ws")
        cfg = await self.sp.get_async("umo", umo, "session_service_config", {})
        self.assertEqual(cfg["persona_id"], "日常人格")
        self.assertTrue(await self.manager.persona_conflict(umo, "运维"))
        # No conflict when the global persona matches the workspace persona.
        self.assertFalse(await self.manager.persona_conflict(umo, "日常人格"))

    async def test_result_ack_targets_exact_run(self):
        from astrbot_plugin_task_workspace.models import Run
        ws, ev = await self._new_ws("ack")
        for i, rid in enumerate(("run_11111111", "run_22222222")):
            await self.store.insert_run(Run(
                id=rid, workspace_id=ws.id, executor="dsh", request_id=f"req{i}",
                input_snapshot="{}", status=RunStatus.SUCCEEDED.value,
                result_text=f"body{i}"))
        run_id, surface = await self.manager.surface_pending_result(ev)
        self.assertTrue(run_id)
        self.assertIn("body", surface)
        # Acknowledge only that exact run.
        await self.manager.mark_result_surfaced(ev, run_id=run_id)
        remaining = [r for r in await self.store.list_runs(ws.id)
                     if r.status == RunStatus.SUCCEEDED.value and not r.result_surfaced]
        self.assertEqual(len(remaining), 1)
        self.assertNotEqual(remaining[0].id, run_id)

    async def test_switch_between_workspaces(self):
        ws1, ev = await self._new_ws("one")
        ws2, _ = await self._new_ws("two")
        self.assertEqual((await self.manager.current_workspace(ev)).id, ws2.id)
        await self.manager.switch(ev, ws1.id)
        self.assertEqual((await self.manager.current_workspace(ev)).id, ws1.id)
        await self.manager.switch(ev, "two")
        self.assertEqual((await self.manager.current_workspace(ev)).id, ws2.id)

    async def test_archive_refused_when_active(self):
        ws, ev = await self._new_ws()
        run = await self.manager.start_run(ev, "dsh")
        with self.assertRaises(WorkspaceError):
            await self.manager.archive(ev, ws.id)
        await self.manager.cancel(ev, run.id)

    async def test_rename_updates_both_stores(self):
        ws, ev = await self._new_ws("old")
        await self.manager.rename(ev, ws.id, "new title")
        got = await self.store.get_workspace(ws.id)
        self.assertEqual(got.title, "new title")
        conv = await self.ctx.conversation_manager.get_conversation(ev.unified_msg_origin,
                                                                    ws.conversation_id)
        self.assertEqual(conv.title, "new title")

    # ------------------------------------------------------------- resolve
    async def test_resolve_exact_prefix_title_ambiguous(self):
        ws1, ev = await self._new_ws("deploy pipeline")
        ws2, _ = await self._new_ws("deploy database")
        self.assertEqual((await self.manager.resolve(ev, ws1.id)).id, ws1.id)
        self.assertEqual((await self.manager.resolve(ev, ws1.id[:6])).id, ws1.id)
        with self.assertRaises(AmbiguousIdError):
            await self.manager.resolve(ev, "deploy")
        with self.assertRaises(WorkspaceError):
            await self.manager.resolve(ev, "zzzzzz")

    async def test_exact_id_lookup_is_authorization_checked(self):
        ws, _ = await self._new_ws("secret")
        other = FakeEvent(sender="99999")
        # Owner filtering hides the workspace entirely rather than revealing it.
        with self.assertRaises(WorkspaceError):
            await self.manager.resolve(other, ws.id)

    # --------------------------------------------------------------- access
    async def test_disabled_is_fail_closed(self):
        self.manager.config = make_config(enabled=False)
        ev = FakeEvent()
        with self.assertRaises(AccessDeniedError) as cm:
            await self.manager.create_workspace(ev, "x")
        self.assertIn("未启用", str(cm.exception))
        with self.assertRaises(AccessDeniedError):
            await self.manager.start_run(ev, "dsh")

    async def test_empty_allowed_sessions_denies(self):
        self.manager.config = make_config(allowed_sessions=[])
        with self.assertRaises(AccessDeniedError):
            await self.manager.create_workspace(FakeEvent(), "x")

    async def test_non_admin_denied(self):
        with self.assertRaises(AccessDeniedError):
            await self.manager.create_workspace(FakeEvent(admin=False), "x")

    # ------------------------------------------------------------ snapshot
    async def test_snapshot_includes_amendments_and_consumes(self):
        ws, ev = await self._new_ws("ctx")
        await self.ctx.conversation_manager.append_history(ws.conversation_id, "user",
                                                           "please add tests")
        await self.manager.amend(ev, "also update docs")
        run = await self.manager.start_run(ev, "dsh")
        await self.manager.wait_background(timeout=3)
        snap = json.loads(run.input_snapshot)
        self.assertIn("also update docs", snap["pending_amendments"][0])
        self.assertIn("please add tests", snap["prompt"])
        self.assertEqual(await self.store.pending_unconsumed(ws.id), [])

    # ----------------------------------------------------------- execution
    async def test_success_notifies_and_surfaces_once(self):
        ws, ev = await self._new_ws("run")
        self.manager.executor.default_watch = ([], {"status": "succeeded", "final": "RESULT BODY",
                                                     "artifacts": ["a.txt"]})
        run = await self.manager.start_run(ev, "dsh")
        await self.manager.wait_background(timeout=3)
        got = await self.store.get_run(run.id)
        self.assertEqual(got.status, RunStatus.SUCCEEDED.value)
        self.assertEqual((await self.manager.read_result(got)), "RESULT BODY")
        surface = await self.manager.surface_pending_result(ev)
        self.assertIn("RESULT BODY", surface[1])
        self.assertIn("untrusted", surface[1].lower())
        await self.manager.mark_result_surfaced(ev, run_id=surface[0])
        self.assertEqual((await self.manager.surface_pending_result(ev))[1], "")

    async def test_same_workspace_exclusion(self):
        ws, ev = await self._new_ws("excl")
        self.manager.executor.default_watch = ([], {"status": "running"})
        await self.manager.start_run(ev, "dsh")
        await _wait_for(lambda: bool(self.manager.executor.submitted))
        with self.assertRaises(WorkspaceError):
            await self.manager.start_run(ev, "codex")

    async def test_ambiguous_submit_needs_reconciliation(self):
        ws, ev = await self._new_ws("amb")
        self.manager.executor.fail_submit = ExecutorError("timeout", ambiguous=True)
        run = await self.manager.start_run(ev, "dsh")
        await self.manager.wait_background(timeout=3)
        got = await self.store.get_run(run.id)
        self.assertEqual(got.status, RunStatus.NEEDS_RECONCILIATION.value)

    async def test_reconcile_missing_marks_failed(self):
        ws, ev = await self._new_ws("rec")
        self.manager.executor.fail_submit = ExecutorError("timeout", ambiguous=True)
        self.manager.executor.reconcile_result = {"ok": True, "found": False}
        run = await self.manager.start_run(ev, "dsh")
        await self.manager.wait_background(timeout=3)
        got = await self.manager.reconcile(ev, run.id)
        self.assertEqual(got.status, RunStatus.FAILED.value)

    async def test_connectivity_error_is_separate(self):
        ws, ev = await self._new_ws("conn")
        self.manager.executor.fail_submit = ExecutorError("ssh down", connectivity=True)
        run = await self.manager.start_run(ev, "dsh")
        await self.manager.wait_background(timeout=3)
        got = await self.store.get_run(run.id)
        self.assertEqual(got.status, RunStatus.FAILED.value)
        self.assertTrue(got.connectivity_error)

    async def test_cancel_requires_remote_confirmation(self):
        ws, ev = await self._new_ws("cancel")
        self.manager.executor.default_watch = ([], {"status": "running"})
        run = await self.manager.start_run(ev, "dsh")
        for _ in range(300):
            if (await self.store.get_run(run.id)).remote_task_id:
                break
            await asyncio.sleep(0.01)
        got = await self.manager.cancel(ev, run.id)
        self.assertEqual(got.status, RunStatus.CANCEL_REQUESTED.value)
        for _ in range(200):
            if self.manager.executor.cancelled:
                break
            await asyncio.sleep(0.01)
        self.assertTrue(self.manager.executor.cancelled)
        self.manager.executor.default_watch = ([], {"status": "cancelled"})
        await self.manager.wait_background(timeout=3)
        final = await self.store.get_run(run.id)
        self.assertEqual(final.status, RunStatus.CANCELLED.value)

    async def test_memory_gate_blocks_execution_when_capability_absent(self):
        tmp = tempfile.TemporaryDirectory()
        try:
            mgr, store, ctx, sp = build_manager(tmp.name, gate=make_gate(capable=False, sp=FakeSp()))
            await store.migrate()
            ev = FakeEvent()
            await mgr.create_workspace(ev, "blocked")
            with self.assertRaises(WorkspaceError) as cm:
                await mgr.start_run(ev, "dsh")
            self.assertIn("隔离能力", str(cm.exception))
            store.close()
        finally:
            tmp.cleanup()


class GatedExecutor(FakeRemoteExecutor):
    def __init__(self, gate):
        super().__init__()
        self.gate = gate
        self.entered = 0
        self.active = 0
        self.max_active = 0

    async def submit(self, request_id, prompt, cwd, permission, timeout_s, executor):
        self.entered += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await self.gate.wait()
            return await super().submit(request_id, prompt, cwd, permission,
                                        timeout_s, executor)
        finally:
            self.active -= 1


class ConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_global_limit_and_no_cross_workspace_surface(self):
        tmp = tempfile.TemporaryDirectory()
        try:
            gate = asyncio.Event()
            ex = GatedExecutor(gate)
            mgr, store, ctx, sp = build_manager(
                tmp.name, config=make_config(max_global_runs=2), executor=ex
            )
            await store.migrate()
            ex.default_watch = ([], {"status": "succeeded", "final": "ok"})
            ev = FakeEvent()
            wss = []
            for i in range(3):
                wss.append(await mgr.create_workspace(ev, f"ws{i}"))

            runs = []
            for ws in wss:
                await mgr.switch(ev, ws.id)
                runs.append(await mgr.start_run(ev, "dsh"))
            for _ in range(200):
                if ex.entered >= 2:
                    break
                await asyncio.sleep(0.01)
            self.assertEqual(ex.entered, 2)
            self.assertLessEqual(ex.max_active, 2)
            gate.set()
            await mgr.wait_background(timeout=5)
            self.assertEqual(ex.max_active, 2)
            self.assertEqual(len(ex.submitted), 3)

            # result surfacing is scoped to the current workspace only
            await mgr.switch(ev, wss[0].id)
            surface = (await mgr.surface_pending_result(ev))[1]
            self.assertIn("ws", surface)
            await mgr.switch(ev, wss[1].id)
            other = (await mgr.surface_pending_result(ev))[1]
            self.assertNotEqual(surface, other)
            store.close()
        finally:
            tmp.cleanup()


class NotificationTests(unittest.IsolatedAsyncioTestCase):
    async def test_outbox_retry_until_success(self):
        tmp = tempfile.TemporaryDirectory()
        try:
            store = Store(os.path.join(tmp.name, "t.db"))
            await store.migrate()
            ctx = FakeContext()
            notifier = Notifier(store, ctx, interval=999)
            await notifier.enqueue("run_1", "terminal:succeeded", "u:1:1", "done")
            fails = {"n": 0}

            async def bad_sender(session, content):
                fails["n"] += 1
                raise RuntimeError("platform offline")

            self.assertEqual(await notifier.flush_once(sender=bad_sender), 0)
            self.assertEqual(fails["n"], 1)
            self.assertEqual(await store.due_outbox(), [])
            conn = store._connect()
            row = conn.execute("SELECT attempts FROM outbox").fetchone()
            self.assertEqual(row["attempts"], 1)
            conn.execute("UPDATE outbox SET next_attempt_at=0")

            sent = []
            async def good_sender(session, content):
                sent.append(content)

            self.assertEqual(await notifier.flush_once(sender=good_sender), 1)
            self.assertEqual(sent, ["done"])
            self.assertEqual(await store.due_outbox(), [])
            store.close()
        finally:
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
