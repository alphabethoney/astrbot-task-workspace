# -*- coding: utf-8 -*-
import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from helpers import FakeEvent, make_config  # noqa: E402

from astrbot.core.star.filter.command import CommandFilter, GreedyStr  # noqa: E402

from test_manager import build_manager  # noqa: E402
from astrbot_plugin_task_workspace.main import TaskWorkspace  # noqa: E402
from astrbot_plugin_task_workspace.models import RunStatus  # noqa: E402


class ParserTests(unittest.TestCase):
    def test_greedy_rest_preserves_spaces(self):
        cf = CommandFilter("task")
        cf.handler_params = {"sub": "", "rest": GreedyStr}
        out = cf.validate_and_convert_params(["new", "My", "Big", "Title"], cf.handler_params)
        self.assertEqual(out["sub"], "new")
        self.assertEqual(out["rest"], "My Big Title")

    def test_greedy_rest_optional(self):
        cf = CommandFilter("task")
        cf.handler_params = {"sub": "", "rest": GreedyStr}
        out = cf.validate_and_convert_params(["status"], cf.handler_params)
        self.assertEqual(out["sub"], "status")
        self.assertEqual(out["rest"], "")


class DispatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cfg = make_config()
        self.mgr, self.store, self.ctx, self.sp = build_manager(
            self.tmp.name, config=self.cfg
        )
        await self.store.migrate()
        self.plugin = TaskWorkspace.__new__(TaskWorkspace)
        self.plugin._manager = self.mgr
        self.plugin._cfg = self.cfg
        self.ev = FakeEvent()
        self.mgr.executor.default_watch = ([], {"status": "succeeded", "final": "done!"})

    async def asyncTearDown(self):
        await self.mgr.wait_background(timeout=3)
        self.store.close()
        self.tmp.cleanup()

    async def test_disabled_returns_explicit_error(self):
        self.mgr.config = make_config(enabled=False)
        text = await self.plugin._dispatch(self.ev, "new", "x")
        self.assertIn("未启用", text)

    async def test_new_then_run_then_status(self):
        text = await self.plugin._dispatch(self.ev, "new", "Release 1.0")
        self.assertIn("已创建工作区", text)
        text = await self.plugin._dispatch(self.ev, "run", "dsh")
        self.assertIn("排队", text)
        await self.mgr.wait_background(timeout=3)
        status = await self.plugin._dispatch(self.ev, "status", "")
        self.assertIn("succeeded", status)

    async def test_result_and_amend_flow(self):
        await self.plugin._dispatch(self.ev, "new", "Release 2.0")
        self.assertIn("补充要求", await self.plugin._dispatch(self.ev, "amend", "go faster"))
        await self.plugin._dispatch(self.ev, "run", "codex")
        await self.mgr.wait_background(timeout=3)
        result = await self.plugin._dispatch(self.ev, "result", "")
        self.assertIn("done!", result)

    async def test_unknown_subcommand_shows_usage(self):
        text = await self.plugin._dispatch(self.ev, "bogus", "")
        self.assertIn("用法", text)


if __name__ == "__main__":
    unittest.main()
