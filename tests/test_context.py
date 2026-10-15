# -*- coding: utf-8 -*-
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from helpers import FakeConversation  # noqa: E402

from astrbot_plugin_task_workspace.context_builder import (  # noqa: E402
    TRUNCATION_NOTICE,
    UNTRUSTED_LABEL,
    build_result_surface,
    build_snapshot,
    parse_snapshot,
    snapshot_to_prompt,
)
from astrbot_plugin_task_workspace.models import Run, Workspace  # noqa: E402


class ContextBuilderTests(unittest.TestCase):
    def test_snapshot_labels_prior_result_untrusted(self):
        conv = FakeConversation("c1", "u", history=[{"role": "user", "content": "do x"}])
        snap, truncated = build_snapshot(
            conv=conv, title="t", user_requirement="build it",
            pending_amendments=["also test"], prior_result="old output",
        )
        self.assertFalse(truncated)
        self.assertIn(UNTRUSTED_LABEL, snap["prompt"])
        self.assertIn("also test", snap["prompt"])
        self.assertIn("do x", snap["prompt"])
        self.assertTrue(snap["has_prior_result"])

    def test_snapshot_truncates_and_discloses(self):
        conv = FakeConversation("c1", "u", history=[{"role": "user", "content": "y" * 5000}])
        snap, truncated = build_snapshot(
            conv=conv, title="t", user_requirement="z" * 5000,
            pending_amendments=[], prior_result="", max_chars=200,
        )
        self.assertTrue(truncated)
        self.assertTrue(snap["truncated"])
        self.assertIn(TRUNCATION_NOTICE, snap["prompt"])
        self.assertIn(TRUNCATION_NOTICE, snapshot_to_prompt(snap))

    def test_parse_snapshot_roundtrip(self):
        snap = {"schema": 1, "prompt": "hi"}
        self.assertEqual(parse_snapshot('{"schema":1,"prompt":"hi"}' ), snap)
        self.assertEqual(parse_snapshot("not json"), {})

    def test_result_surface_source_labeled_and_bounded(self):
        ws = Workspace(id="ws_aaaaaaaa", owner_user_id="1", umo="u", conversation_id="c",
                       title="t")
        run = Run(id="run_aaaaaaaa", workspace_id=ws.id, executor="dsh",
                  request_id="r", input_snapshot="{}")
        text = build_result_surface(ws, run, "x" * 10000, max_chars=100)
        self.assertIn("TASK WORKSPACE RESULT", text)
        self.assertIn("untrusted", text.lower())
        self.assertIn("truncated", text)


if __name__ == "__main__":
    unittest.main()
