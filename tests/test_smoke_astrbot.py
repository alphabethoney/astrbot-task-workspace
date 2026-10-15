# -*- coding: utf-8 -*-
"""Smoke tests against the real installed AstrBot interfaces."""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from helpers import PLUGINS  # noqa: E402

import astrbot  # noqa: E402
from astrbot.api.event import filter, AstrMessageEvent  # noqa: E402
from astrbot.api.star import Context, Star  # noqa: E402
from astrbot.core.star.star_handler import star_handlers_registry, EventType  # noqa: E402

import astrbot_plugin_task_workspace.main as tw_main  # noqa: E402


class SmokeTests(unittest.TestCase):
    def test_astrbot_version_present(self):
        self.assertTrue(getattr(astrbot, "__version__", ""))

    def test_plugin_is_star_subclass(self):
        self.assertTrue(issubclass(tw_main.TaskWorkspace, Star))

    def test_command_and_hooks_registered(self):
        names = {}
        for h in star_handlers_registry._handlers:
            names.setdefault(h.handler_name, h)
        self.assertIn("task", names)
        self.assertIn("_surface_result", names)
        self.assertIn("_bind_request", names)

        task_handler = names["task"]
        command_names = []
        for f in task_handler.event_filters:
            command_names.append(getattr(f, "command_name", None))
        self.assertIn("task", command_names)

    def test_stamp_hook_is_waiting_llm_event(self):
        for h in star_handlers_registry._handlers:
            if h.handler_name == "_bind_request":
                self.assertEqual(h.event_type, EventType.OnWaitingLLMRequestEvent)
            if h.handler_name == "_surface_result":
                self.assertEqual(h.event_type, EventType.OnLLMRequestEvent)

    def test_config_schema_defaults_fail_closed(self):
        schema = json.loads((Path(tw_main.__file__).parent / "_conf_schema.json").read_text("utf-8"))
        self.assertFalse(schema["enabled"]["default"])
        self.assertEqual(schema["allowed_sessions"]["default"], [])

    def test_command_parser_real_contract(self):
        from astrbot.core.star.filter.command import GreedyStr
        handler = next(h for h in star_handlers_registry._handlers if h.handler_name == "task")
        cf = handler.event_filters[0]
        self.assertIn("rest", cf.handler_params)
        self.assertIs(cf.handler_params["rest"], GreedyStr)


if __name__ == "__main__":
    unittest.main()
