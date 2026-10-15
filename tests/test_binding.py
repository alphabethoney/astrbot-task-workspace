# -*- coding: utf-8 -*-
"""Classification vs authorization tests for managed workspace requests."""
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE.parents[1] / "legacy" / "astrbot_core_patch"))

from helpers import FakeEvent, make_config, native_guard  # noqa: E402
from test_manager import build_manager  # noqa: E402

from follow_up_guard import binding_guard_allows  # noqa: E402
from astrbot_plugin_task_workspace.binding import BINDING_EXTRA_KEY  # noqa: E402
from astrbot_plugin_task_workspace.main import TaskWorkspace  # noqa: E402


class ClassificationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cfg = make_config()
        self.mgr, self.store, self.ctx, self.sp = build_manager(
            self.tmp.name, config=self.cfg)
        await self.store.migrate()
        self.plugin = TaskWorkspace.__new__(TaskWorkspace)
        self.plugin._manager = self.mgr
        self.plugin._cfg = self.cfg
        self.ev = FakeEvent()
        self.umo = self.ev.unified_msg_origin

    async def asyncTearDown(self):
        await self.mgr.shutdown()
        self.store.close()
        self.tmp.cleanup()

    def _binding(self, event):
        return event.get_extra(BINDING_EXTRA_KEY)

    async def test_managed_missing_cache_fails_closed(self):
        await self.mgr.create_workspace(self.ev, "ws")
        await self.mgr.refresh_policy()
        # Simulate a known-managed UMO whose current conversation is unknown:
        # the live selection is lost (e.g. deleted) rather than switched.
        self.mgr.policy.forget_current(self.umo)
        self.ctx.conversation_manager.session_conversations.pop(self.umo, None)
        ev = FakeEvent()
        self.plugin.on_adapter_message(ev)
        self.assertTrue(ev.is_stopped())

    async def test_managed_authorized_stamps_and_isolates(self):
        await self.mgr.create_workspace(self.ev, "ws")
        await self.mgr.refresh_policy()
        ev = FakeEvent()
        with native_guard(1):
            self.plugin.on_adapter_message(ev)
        self.assertFalse(ev.is_stopped())
        b = self._binding(ev)
        self.assertTrue(b["managed"])
        self.assertTrue(ev.get_extra("task_workspace_memory_isolated"))

    async def test_managed_wrong_owner_fails_closed(self):
        await self.mgr.create_workspace(self.ev, "ws")
        await self.mgr.refresh_policy()
        ev = FakeEvent(sender="99999")
        self.plugin.on_adapter_message(ev)
        self.assertTrue(ev.is_stopped())

    async def test_ordinary_session_stamps_ordinary(self):
        ev = FakeEvent(umo="aiocqhttp:private:55555", sender="55555")
        self.plugin.on_adapter_message(ev)
        b = self._binding(ev)
        self.assertIsNotNone(b)
        self.assertFalse(b["managed"])

    async def test_daily_to_managed_race_denied_by_guard(self):
        # Daily message stamped ordinary, then a managed run is active for the
        # same UMO in a different conversation: native capture must be denied.
        daily = FakeEvent(umo="aiocqhttp:private:55555", sender="55555")
        daily.set_extra(BINDING_EXTRA_KEY, {
            "umo": daily.unified_msg_origin, "cid": "c_daily",
            "ws_id": "", "owner": "", "managed": False})
        managed_runner = FakeEvent(umo="aiocqhttp:private:55555", sender="55555")
        managed_runner.set_extra(BINDING_EXTRA_KEY, {
            "umo": managed_runner.unified_msg_origin, "cid": "c_managed",
            "ws_id": "ws_x", "owner": "55555", "managed": True})
        self.assertFalse(binding_guard_allows(daily, managed_runner))
        self.assertFalse(binding_guard_allows(managed_runner, daily))

    async def test_absent_binding_on_managed_llm_request_stops(self):
        ws = await self.mgr.create_workspace(self.ev, "ws")
        await self.mgr.refresh_policy()
        ev = FakeEvent()  # no binding stamped
        req = type("Req", (), {"conversation": type("C", (), {"cid": ws.conversation_id})()})()
        await self.plugin._surface_result(ev, req)
        self.assertTrue(ev.is_stopped())


class TerminateTests(unittest.IsolatedAsyncioTestCase):
    async def test_terminate_calls_plugin_shutdown_not_wait_background(self):
        calls = {"shutdown": 0, "wait": 0}

        class FakeManager:
            async def shutdown(self):
                calls["shutdown"] += 1

            async def wait_background(self, timeout=30):
                calls["wait"] += 1

        class FakeNotifier:
            async def stop(self):
                calls["notifier"] = True

        class FakeStore:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        plugin = TaskWorkspace.__new__(TaskWorkspace)
        plugin._manager = FakeManager()
        plugin._notifier = FakeNotifier()
        plugin._store = FakeStore()
        plugin.context = type("Ctx", (), {})()
        await plugin.terminate()
        self.assertEqual(calls["shutdown"], 1)
        self.assertEqual(calls["wait"], 0)
        self.assertTrue(calls.get("notifier"))
        self.assertTrue(plugin._store.closed)


if __name__ == "__main__":
    unittest.main()
