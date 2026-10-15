# -*- coding: utf-8 -*-
"""Authoritative conversation-selection tests (item 3).

The synchronous early filter follows ``cm.session_conversations``: native UI,
``/new`` and ``/reset`` switch conversations without notifying the plugin. A
nonempty CID that is not in the managed index is ordinary daily chat even when
the same UMO owns other workspaces; only a *missing* selection for a
known-managed UMO is uncertain (fail closed).
"""
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parent))
from helpers import FakeEvent, make_config, native_guard  # noqa: E402
from test_manager import build_manager  # noqa: E402

from astrbot_plugin_task_workspace.binding import BINDING_EXTRA_KEY  # noqa: E402
from astrbot_plugin_task_workspace.main import TaskWorkspace  # noqa: E402


class SelectionTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_native_switch_managed_to_ordinary_is_daily(self):
        ws = await self.mgr.create_workspace(self.ev, "ws")
        await self.mgr.refresh_policy()
        self.assertEqual(self.mgr.classify(self.ev), "managed")
        # Native UI/cm switches the live selection to an unrelated conversation.
        cm = self.ctx.conversation_manager
        ordinary = await cm.new_conversation(self.umo, title="daily")
        await cm.switch_conversation(self.umo, ordinary)
        self.assertEqual(self.mgr.classify(self.ev), "ordinary")
        ev = FakeEvent()
        self.plugin.on_adapter_message(ev)
        self.assertFalse(ev.is_stopped())
        b = self._binding(ev)
        self.assertIsNotNone(b)
        self.assertFalse(b["managed"])
        self.assertEqual(b["cid"], ordinary)
        self.assertFalse(ev.get_extra("task_workspace_memory_isolated"))

    async def test_new_conversation_switches_to_ordinary(self):
        await self.mgr.create_workspace(self.ev, "ws")
        await self.mgr.refresh_policy()
        cm = self.ctx.conversation_manager
        cid = await cm.new_conversation(self.umo, title="fresh")
        self.assertEqual(self.mgr.classify(self.ev), "ordinary")
        ev = FakeEvent()
        self.plugin.on_adapter_message(ev)
        self.assertFalse(ev.is_stopped())
        self.assertEqual(self._binding(ev)["cid"], cid)

    async def test_switch_ordinary_to_managed(self):
        await self.mgr.create_workspace(self.ev, "ws")
        cm = self.ctx.conversation_manager
        ordinary = await cm.new_conversation(self.umo, title="daily")
        await self.mgr.refresh_policy()
        self.assertEqual(self.mgr.classify(self.ev), "ordinary")
        ws = (await self.store.list_workspaces(self.umo))[0]
        await cm.switch_conversation(self.umo, ws.conversation_id)
        self.assertEqual(self.mgr.classify(self.ev), "managed")
        ev = FakeEvent()
        with native_guard(1):
            self.plugin.on_adapter_message(ev)
        self.assertFalse(ev.is_stopped())
        self.assertTrue(self._binding(ev)["managed"])

    async def test_missing_selection_known_managed_denies(self):
        await self.mgr.create_workspace(self.ev, "ws")
        await self.mgr.refresh_policy()
        # Live selection disappears (deleted) rather than switching.
        self.mgr.policy.forget_current(self.umo)
        self.ctx.conversation_manager.session_conversations.pop(self.umo, None)
        self.assertEqual(self.mgr.classify(self.ev), "unknown_managed")
        ev = FakeEvent()
        self.plugin.on_adapter_message(ev)
        self.assertTrue(ev.is_stopped())

    async def test_managed_cid_wins_over_umo_owner(self):
        ws = await self.mgr.create_workspace(self.ev, "ws")
        await self.mgr.refresh_policy()
        # A managed CID is managed even if classification is asked with it
        # explicitly while the live selection says otherwise.
        self.assertEqual(
            self.mgr.classify(self.ev, ws.conversation_id), "managed")


if __name__ == "__main__":
    unittest.main()
