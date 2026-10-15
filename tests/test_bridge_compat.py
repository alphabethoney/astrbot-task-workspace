# -*- coding: utf-8 -*-
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parent))
from helpers import FakeEvent, make_config  # noqa: E402

sys.path.insert(0, str(HERE.parents[1] / "legacy"))
from test_manager import build_manager  # noqa: E402

from astrbot_plugin_task_workspace.bridge_compat import TaskWorkspaceService  # noqa: E402
from astrbot_plugin_bridge_server.config import BridgeConfig  # noqa: E402
from astrbot_plugin_bridge_server.main import BridgeServer  # noqa: E402
from astrbot_plugin_bridge_server.task_table import (  # noqa: E402
    POLLER_LEGACY,
    POLLER_TASK_WORKSPACE,
    TaskTable,
)


class BridgeCompatTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cfg = make_config()
        self.mgr, self.store, self.ctx, self.sp = build_manager(self.tmp.name, config=self.cfg)
        await self.store.migrate()
        self.mgr.executor.default_watch = ([], {"status": "succeeded", "final": "ok"})
        self.service = TaskWorkspaceService(self.mgr, self.cfg)
        self.ev = FakeEvent()
        self.bridge = BridgeServer.__new__(BridgeServer)

    async def asyncTearDown(self):
        await self.mgr.wait_background(timeout=3)
        self.store.close()
        self.tmp.cleanup()

    def _bridge_context(self, service):
        ctx = types.SimpleNamespace()
        if service is not None:
            ctx._task_workspace_service = service
        self.bridge.context = ctx

    async def test_flag_off_returns_none(self):
        self.bridge._cfg = BridgeConfig({"use_task_workspace": False})
        self._bridge_context(None)
        self.assertIsNone(await self.bridge._compat(self.ev, "wg", "x"))

    async def test_flag_on_without_service_fails_closed(self):
        self.bridge._cfg = BridgeConfig({"use_task_workspace": True})
        self._bridge_context(None)
        result = await self.bridge._compat(self.ev, "wg", "x")
        self.assertIn("服务不可用", result)

    async def test_flag_on_routes_to_service(self):
        self.bridge._cfg = BridgeConfig({"use_task_workspace": True})
        self._bridge_context(self.service)
        await self.service.route("nt", self.ev, "do the thing")
        self.assertIn("工作区", await self.service.route("tasks", self.ev, ""))
        result = await self.bridge._compat(self.ev, "ww", "")
        self.assertIsInstance(result, str)

    async def test_disabled_service_reported_unavailable(self):
        disabled = TaskWorkspaceService(self.mgr, make_config(enabled=False))
        self.bridge._cfg = BridgeConfig({"use_task_workspace": True})
        self._bridge_context(disabled)
        self.assertIn("服务不可用", await self.bridge._compat(self.ev, "wg", "x"))


class NoDuplicateCommandsTest(unittest.TestCase):
    def test_task_and_legacy_commands_coexist(self):
        from astrbot.core.star.star_handler import star_handlers_registry as reg
        import astrbot_plugin_task_workspace.main  # noqa: F401
        import astrbot_plugin_bridge_server.main  # noqa: F401

        command_names = []
        for h in reg._handlers:
            for f in h.event_filters:
                name = getattr(f, "command_name", None)
                if name:
                    command_names.append(name)
        self.assertIn("task", command_names)
        self.assertIn("wg", command_names)
        self.assertEqual(command_names.count("task"), 1)
        self.assertEqual(command_names.count("wg"), 1)


class PollOwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "task_table.json")

    async def asyncTearDown(self):
        self.tmp.cleanup()

    def _bridge(self, service, table_path):
        import types
        bridge = BridgeServer.__new__(BridgeServer)
        bridge._cfg = BridgeConfig({"use_task_workspace": True})
        bridge.table = TaskTable(path=table_path)
        bridge._poll_task = None
        bridge._managed = bridge.table.managed()
        ctx = types.SimpleNamespace()
        if service is not None:
            ctx._task_workspace_service = service
        bridge.context = ctx
        return bridge

    async def test_managed_mode_disables_legacy_poller(self):
        tmp = tempfile.TemporaryDirectory()
        try:
            svc = types.SimpleNamespace(available=True)
            bridge = self._bridge(svc, self.path)
            await bridge.initialize()
            self.assertIsNone(bridge._poll_task)
            self.assertTrue(bridge.table.managed())
            self.assertEqual(bridge.table.poller_owner(), POLLER_TASK_WORKSPACE)
            # Handoff survives a reload.
            reloaded = TaskTable(path=self.path)
            self.assertTrue(reloaded.managed())
        finally:
            tmp.cleanup()

    async def test_managed_mode_without_service_fails_closed(self):
        bridge = self._bridge(None, self.path)
        await bridge.initialize()
        self.assertIsNone(bridge._poll_task)

    async def test_managed_activation_refused_when_active_legacy_jobs(self):
        table = TaskTable(path=self.path)
        table.add("t1", "dsh", "p", "u:1:1")
        bridge = self._bridge(types.SimpleNamespace(available=True), self.path)
        bridge.table = TaskTable(path=self.path)
        await bridge.initialize()
        self.assertIsNone(bridge._poll_task)
        self.assertFalse(bridge.table.managed())
        self.assertEqual(bridge.table.poller_owner(), POLLER_LEGACY)

    async def test_quarantined_rows_not_polled(self):
        table = TaskTable(path=self.path)
        table.add("t1", "dsh", "p", "u:1:1")
        table.quarantine("t1", "imported_elsewhere")
        self.assertEqual(table.active(), [])
        reloaded = TaskTable(path=self.path)
        rec = reloaded.get("t1")
        self.assertTrue(rec.quarantined)
        self.assertEqual(reloaded.active(), [])


class HandoffTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "task_table.json")

    async def asyncTearDown(self):
        self.tmp.cleanup()

    def test_persisted_handoff_write_failure_is_not_silent(self):
        from astrbot_plugin_bridge_server.task_table import HandoffError, POLLER_LEGACY
        table = TaskTable(path=self.path)

        def boom():
            raise OSError("disk full")
        table._write = boom  # type: ignore[assignment]
        with self.assertRaises(HandoffError):
            table.record_handoff({"source": "test"})
        # Rolled back to the previous owner, never a false success.
        self.assertEqual(table.poller_owner(), POLLER_LEGACY)
        self.assertFalse(table.handoff_ok())

    def test_offline_helper_is_idempotent_and_quarantine_preserved(self):
        from astrbot_plugin_bridge_server.handoff import (
            build_handoff_plan, read_handoff, write_handoff)
        from astrbot_plugin_bridge_server.task_table import POLLER_TASK_WORKSPACE

        table = TaskTable(path=self.path)
        table.add("t-active", "dsh", "p", "u:1:1")
        table.add("t-stale", "dsh", "p", "u:1:1")
        # Stale row is quarantined with evidence before the drain check.
        write_handoff(self.path, build_handoff_plan(
            [{"task_id": "t-active"}], {"t-active": {"owner": "1"}}),
            quarantine={"t-stale": {"reason": "stale_running_needs_review",
                                    "evidence": {"pid": 42508}}})
        first = read_handoff(self.path)
        self.assertTrue(first["ready"])
        self.assertEqual(first["poller_owner"], POLLER_TASK_WORKSPACE)
        # Idempotent second run with a now-empty active set.
        write_handoff(self.path, {"approved": [], "rejected": []}, drained=True)
        second = read_handoff(self.path)
        self.assertTrue(second["ready"])
        # Quarantine evidence + original record survive and are never deleted.
        reloaded = TaskTable(path=self.path)
        rec = reloaded.get("t-stale")
        self.assertTrue(rec.quarantined)
        self.assertEqual(rec.quarantine_reason, "stale_running_needs_review")
        self.assertEqual(rec.quarantine_original.get("status"), "queued")
        self.assertEqual(rec.quarantine_evidence.get("pid"), 42508)

    def test_active_rows_block_handoff_without_explicit_quarantine(self):
        from astrbot_plugin_bridge_server.handoff import write_handoff
        from astrbot_plugin_bridge_server.task_table import HandoffError
        table = TaskTable(path=self.path)
        table.add("t-active", "dsh", "p", "u:1:1")
        with self.assertRaises(HandoffError):
            write_handoff(self.path, {"approved": [], "rejected": []})

    def test_handoff_state_gates_execution(self):
        from astrbot_plugin_task_workspace.bridge_compat import (
            HandoffState, attach_handoff, handoff_allows_execution)
        ctx = types.SimpleNamespace()
        self.assertEqual(handoff_allows_execution(ctx), (True, ""))
        attach_handoff(ctx, HandoffState(required=False, ready=False))
        self.assertEqual(handoff_allows_execution(ctx), (True, ""))
        attach_handoff(ctx, HandoffState(required=True, ready=False,
                                         reason="not drained"))
        allowed, reason = handoff_allows_execution(ctx)
        self.assertFalse(allowed)
        self.assertIn("not drained", reason)
        attach_handoff(ctx, HandoffState(required=True, ready=True, verified=True))
        self.assertEqual(handoff_allows_execution(ctx), (True, ""))

    async def test_compat_refuses_before_verified_handoff(self):
        bridge = BridgeServer.__new__(BridgeServer)
        bridge._cfg = BridgeConfig({"use_task_workspace": True})
        bridge.table = TaskTable(path=self.path)
        bridge.table.add("t1", "dsh", "p", "u:1:1")
        ctx = types.SimpleNamespace()
        ctx._task_workspace_service = types.SimpleNamespace(available=True)
        bridge.context = ctx
        result = await bridge._compat(FakeEvent(), "wg", "x")
        self.assertIn("拒绝", result)

    async def test_manager_execution_gate_refuses_when_handoff_unready(self):
        tmp = tempfile.TemporaryDirectory()
        try:
            mgr, store, ctx, sp = build_manager(tmp.name, config=make_config())
            await store.migrate()
            ev = FakeEvent()
            await mgr.create_workspace(ev, "x")
            mgr.handoff_gate = lambda: (False, "交接未完成")
            with self.assertRaises(Exception) as cm:
                await mgr.start_run(ev, "dsh")
            self.assertIn("交接", str(cm.exception))
            store.close()
        finally:
            tmp.cleanup()


class HandoffLoadOrderTests(unittest.IsolatedAsyncioTestCase):
    """Item 2: handoff readiness must not depend on workspace-service load order."""

    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "task_table.json")

    async def asyncTearDown(self):
        self.tmp.cleanup()

    def _bridge(self, service, table_path, ctx=None):
        bridge = BridgeServer.__new__(BridgeServer)
        bridge._cfg = BridgeConfig({"use_task_workspace": True})
        bridge.table = TaskTable(path=table_path)
        bridge._poll_task = None
        bridge._managed = bridge.table.managed()
        if ctx is None:
            ctx = types.SimpleNamespace()
        if service is not None:
            ctx._task_workspace_service = service
        bridge.context = ctx
        return bridge

    async def test_bridge_first_publishes_persisted_ready_without_service(self):
        from astrbot_plugin_task_workspace.bridge_compat import get_handoff

        # Persisted, readback-verified handoff with no active rows already
        # exists: readiness must be published even though no service is loaded.
        TaskTable(path=self.path).record_handoff(
            {"source": "legacy_bridge", "reason": "drained"})
        bridge = self._bridge(None, self.path)
        await bridge.initialize()
        state = get_handoff(bridge.context)
        self.assertIsNotNone(state)
        self.assertTrue(state.ready)
        self.assertTrue(state.verified)
        self.assertIsNone(bridge._poll_task)

    async def test_bridge_first_active_rows_blocks_readiness(self):
        from astrbot_plugin_task_workspace.bridge_compat import get_handoff

        table = TaskTable(path=self.path)
        table.add("t1", "dsh", "p", "u:1:1")
        table.record_handoff({"source": "legacy_bridge"})
        self.assertTrue(table.handoff_ok())
        bridge = self._bridge(types.SimpleNamespace(available=True), self.path)
        await bridge.initialize()
        state = get_handoff(bridge.context)
        self.assertFalse(state.ready)
        self.assertIsNone(bridge._poll_task)

    async def test_workspace_first_pending_then_late_bridge_ready(self):
        from astrbot_plugin_task_workspace.bridge_compat import (
            ensure_pending_handoff, handoff_allows_execution)

        ctx = types.SimpleNamespace()
        ensure_pending_handoff(ctx, required=True)
        self.assertFalse(handoff_allows_execution(ctx, require=True)[0])
        # Bridge initializes later with the service available: it records the
        # verified handoff and updates the same state to ready.
        bridge = self._bridge(types.SimpleNamespace(available=True), self.path, ctx=ctx)
        await bridge.initialize()
        self.assertTrue(handoff_allows_execution(ctx, require=True)[0])

    async def test_absent_bridge_with_required_true_denies(self):
        from astrbot_plugin_task_workspace.bridge_compat import (
            ensure_pending_handoff, handoff_allows_execution)

        ctx = types.SimpleNamespace()
        # No state at all: required => deny, standalone default => allow.
        self.assertFalse(handoff_allows_execution(ctx, require=True)[0])
        self.assertTrue(handoff_allows_execution(ctx, require=False)[0])
        ensure_pending_handoff(ctx, required=True)
        allowed, reason = handoff_allows_execution(ctx, require=True)
        self.assertFalse(allowed)
        self.assertTrue(reason)

    async def test_pending_preserves_existing_bridge_readiness(self):
        from astrbot_plugin_task_workspace.bridge_compat import (
            HandoffState, attach_handoff, ensure_pending_handoff, get_handoff)

        ctx = types.SimpleNamespace()
        attach_handoff(ctx, HandoffState(required=True, ready=True, verified=True))
        ensure_pending_handoff(ctx, required=True)
        state = get_handoff(ctx)
        self.assertTrue(state.ready)
        self.assertTrue(state.verified)

    async def test_plugin_initialize_wires_required_handoff_gate(self):
        from astrbot_plugin_task_workspace.config import TaskWorkspaceConfig
        from astrbot_plugin_task_workspace.main import TaskWorkspace

        cfg = TaskWorkspaceConfig({"enabled": True, "require_legacy_handoff": True})

        class FakeMgr:
            handoff_gate = None

        plugin = TaskWorkspace.__new__(TaskWorkspace)
        plugin._cfg = cfg
        plugin._manager = FakeMgr()
        plugin.context = types.SimpleNamespace()
        plugin._publish_handoff_gate()
        # Workspace-first: the facade has not exposed execution yet and the gate
        # denies because no bridge has published readiness.
        allowed, _ = plugin._manager.handoff_gate()
        self.assertFalse(allowed)

    async def test_plugin_config_require_legacy_handoff_default_off(self):
        from astrbot_plugin_task_workspace.config import TaskWorkspaceConfig

        self.assertFalse(TaskWorkspaceConfig({}).require_legacy_handoff)
        self.assertTrue(TaskWorkspaceConfig(
            {"require_legacy_handoff": True}).require_legacy_handoff)


if __name__ == "__main__":
    unittest.main()
