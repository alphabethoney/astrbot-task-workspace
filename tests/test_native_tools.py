# -*- coding: utf-8 -*-
"""REPAIR_A5: managed native chat must not expose host execution tools.

A valid managed workspace *native* discussion request has its ``ToolSet``
default-denied before the runner reset coroutine is awaited and the model is
invoked. The real execution path is an explicit ``/task run``. Ordinary daily
requests are never touched.
"""
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parent))

from helpers import FakeEvent, make_config, native_guard  # noqa: E402
from test_manager import build_manager  # noqa: E402

from astrbot.core.agent.tool import FunctionTool, ToolSet  # noqa: E402
from astrbot_plugin_task_workspace.binding import BINDING_EXTRA_KEY  # noqa: E402
from astrbot_plugin_task_workspace.context_builder import (  # noqa: E402
    strip_managed_chat_tools,
)
from astrbot_plugin_task_workspace.main import TaskWorkspace  # noqa: E402
from astrbot_plugin_task_workspace.models import Run, RunStatus  # noqa: E402

NATIVE_NAMES = (
    "astrbot_execute_shell",
    "astrbot_shell_session",
    "astrbot_execute_python",
    "astrbot_file_write_tool",
    "astrbot_file_edit_tool",
)


def _tool(name):
    return FunctionTool(
        name=name, description="native tool",
        parameters={"type": "object", "properties": {}})


def _tool_set(names=NATIVE_NAMES):
    ts = ToolSet()
    for name in names:
        ts.add_tool(_tool(name))
    return ts


def _req(cid, tool_set):
    return SimpleNamespace(conversation=SimpleNamespace(cid=cid),
                           func_tool=tool_set, contexts=[])


class HelperTests(unittest.IsolatedAsyncioTestCase):
    def test_strips_in_place_and_preserves_object_identity(self):
        ts = _tool_set()
        req = _req("c", ts)
        removed = strip_managed_chat_tools(req, allowed=[])
        self.assertEqual(set(removed), set(NATIVE_NAMES))
        self.assertTrue(ts.empty())
        # Identity must be preserved: the runner reset coroutine captures the
        # same ProviderRequest and reads this exact object when awaited.
        self.assertIs(req.func_tool, ts)

    def test_missing_toolset_is_noop(self):
        req = SimpleNamespace(func_tool=None, contexts=[])
        self.assertEqual(strip_managed_chat_tools(req), [])

    def test_allowlisted_tool_survives(self):
        req = _req("c", _tool_set())
        strip_managed_chat_tools(req, allowed=["astrbot_file_write_tool"])
        self.assertEqual(req.func_tool.names(), ["astrbot_file_write_tool"])

    async def test_strip_before_awaited_reset_coro_is_observed(self):
        # Mirror build_main_agent(apply_reset=False): reset() returns a coroutine
        # that reads req.func_tool when awaited. The on_llm_request hook runs
        # between creation and await, so the in-place strip must be visible.
        seen = {}

        class FakeRunner:
            async def reset(self, request):
                seen["names"] = list(request.func_tool.names())

        req = _req("c", _tool_set())
        coro = FakeRunner().reset(req)  # not awaited yet
        strip_managed_chat_tools(req, allowed=[])
        self.assertTrue(req.func_tool.empty())  # identity preserved in place
        await coro
        self.assertEqual(seen["names"], [])


class ManagedToolDenyTests(unittest.IsolatedAsyncioTestCase):
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
        self.ws = await self.mgr.create_workspace(self.ev, "native")
        await self.mgr.refresh_policy()

    async def asyncTearDown(self):
        await self.mgr.shutdown()
        self.store.close()
        self.tmp.cleanup()

    def _managed_event(self):
        ev = FakeEvent()
        ev.set_extra(BINDING_EXTRA_KEY, {
            "umo": ev.unified_msg_origin, "cid": self.ws.conversation_id,
            "ws_id": self.ws.id, "owner": "10001", "managed": True,
        })
        return ev

    async def test_managed_request_toolset_emptied_and_result_surfaced(self):
        await self.store.insert_run(Run(
            id="run_aaaaaaaa", workspace_id=self.ws.id, executor="dsh",
            request_id="req_a", input_snapshot="{}",
            status=RunStatus.SUCCEEDED.value, result_text="executor output"))
        ev = self._managed_event()
        ts = _tool_set()
        req = _req(self.ws.conversation_id, ts)
        with native_guard(1):
            await self.plugin._surface_result(ev, req)
        # Actual ToolSet is empty, same object, event not stopped.
        self.assertTrue(ts.empty())
        self.assertIs(req.func_tool, ts)
        self.assertFalse(ev.is_stopped())
        # Result injection and the frozen binding still work.
        self.assertTrue(any("TASK WORKSPACE RESULT" in c.get("content", "")
                            for c in req.contexts))
        self.assertIsNotNone(ev.get_extra(BINDING_EXTRA_KEY))
        run = await self.store.get_run("run_aaaaaaaa")
        self.assertTrue(run.result_surfaced)
        # No persona mutation: the workspace persona is bound only on the
        # conversation, and the per-request ToolSet strip never touches config.
        conv = await self.ctx.conversation_manager.get_conversation(
            self.ev.unified_msg_origin, self.ws.conversation_id)
        self.assertEqual(conv.persona_id, "运维")
        self.assertNotIn(
            ("umo", self.ev.unified_msg_origin, "session_service_config"),
            self.sp._umo)

    async def test_managed_request_emptied_even_without_pending_result(self):
        ev = self._managed_event()
        ts = _tool_set()
        req = _req(self.ws.conversation_id, ts)
        with native_guard(1):
            await self.plugin._surface_result(ev, req)
        self.assertTrue(ts.empty())
        self.assertEqual(req.contexts, [])
        self.assertFalse(ev.is_stopped())

    async def test_ordinary_request_tools_unchanged(self):
        ev = FakeEvent(umo="aiocqhttp:private:55555", sender="55555")
        ts = _tool_set()
        req = _req("cid_daily", ts)
        await self.plugin._surface_result(ev, req)
        self.assertEqual(ts.names(), list(NATIVE_NAMES))
        self.assertFalse(ev.is_stopped())

    async def test_config_allowlist_keeps_allowed_tool(self):
        cfg = make_config(managed_chat_allowed_tools=["astrbot_file_write_tool"])
        mgr, store, ctx, sp = build_manager(
            tempfile.mkdtemp(), config=cfg)
        await store.migrate()
        try:
            plugin = TaskWorkspace.__new__(TaskWorkspace)
            plugin._manager = mgr
            plugin._cfg = cfg
            ev = FakeEvent()
            ws = await mgr.create_workspace(ev, "allow")
            await mgr.refresh_policy()
            ev.set_extra(BINDING_EXTRA_KEY, {
                "umo": ev.unified_msg_origin, "cid": ws.conversation_id,
                "ws_id": ws.id, "owner": "10001", "managed": True,
            })
            ts = _tool_set()
            req = _req(ws.conversation_id, ts)
            with native_guard(1):
                await plugin._surface_result(ev, req)
            self.assertEqual(ts.names(), ["astrbot_file_write_tool"])
        finally:
            await mgr.shutdown()
            store.close()


class HelpAndConfigTests(unittest.TestCase):
    def test_task_help_notes_execution_uses_task_run(self):
        self.assertIn("/task run", TaskWorkspace.task.__doc__)
        self.assertIn("/task run", TaskWorkspace._HELP_TEXT)

    def test_config_default_denies_all_managed_tools(self):
        self.assertEqual(make_config().managed_chat_allowed_tools, [])


if __name__ == "__main__":
    unittest.main()
