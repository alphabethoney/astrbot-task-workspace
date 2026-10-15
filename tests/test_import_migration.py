# -*- coding: utf-8 -*-
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from helpers import FakeConversationManager  # noqa: E402

from astrbot_plugin_task_workspace.import_migration import LegacyImporter  # noqa: E402
from astrbot_plugin_task_workspace.store import Store  # noqa: E402


class ImportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(os.path.join(self.tmp.name, "t.db"))
        await self.store.migrate()
        self.cm = FakeConversationManager()
        self.umo = "aiocqhttp:private:10001"
        self.cid = await self.cm.new_conversation(self.umo, title="legacy")

    async def asyncTearDown(self):
        self.store.close()
        self.tmp.cleanup()

    async def test_unknown_cid_quarantined(self):
        table = {
            "tasks": {
                "t-good": {"agent": "dsh", "prompt": "good", "session": self.umo,
                           "conversation_id": self.cid, "status": "succeeded",
                           "last_seq": 3, "created": 1.0},
                "t-bad": {"agent": "dsh", "prompt": "bad", "session": self.umo,
                          "conversation_id": "does-not-exist", "status": "running"},
                "t-empty": {"agent": "dsh", "prompt": "empty", "session": self.umo,
                            "conversation_id": ""},
            }
        }
        path = os.path.join(self.tmp.name, "task_table.json")
        Path(path).write_text(json.dumps(table), encoding="utf-8")

        importer = LegacyImporter(self.store, self.cm)
        report = await importer.dry_run(path)
        self.assertEqual(report.total, 3)
        self.assertEqual(len(report.importable), 1)
        self.assertEqual(len(report.quarantined), 2)
        await importer.apply(report)
        await importer.quarantine_report(report)
        self.assertEqual(report.imported, 1)
        run = await self.store.get_run_by_request("legacy:t-good")
        self.assertIsNotNone(run)
        self.assertEqual(run.status, "succeeded")
        quarantined = await self.store.list_quarantine()
        self.assertEqual(len(quarantined), 2)

    async def test_owner_mismatch_quarantined(self):
        other = "aiocqhttp:private:99999"
        cid = await self.cm.new_conversation(other, title="not yours")
        table = {"tasks": {"t1": {"agent": "dsh", "prompt": "p", "session": self.umo,
                                  "conversation_id": cid, "status": "running"}}}
        path = os.path.join(self.tmp.name, "tt.json")
        Path(path).write_text(json.dumps(table), encoding="utf-8")
        importer = LegacyImporter(self.store, self.cm)
        report = await importer.dry_run(path)
        self.assertEqual(len(report.quarantined), 1)
        self.assertIn("owner_mismatch", report.quarantined[0]["reason"])

    async def test_group_umo_quarantined_without_explicit_owner(self):
        group_umo = "aiocqhttp:group:12345"
        cid = await self.cm.new_conversation(group_umo, title="grp")
        table = {"tasks": {"tg": {"agent": "dsh", "prompt": "p",
                                   "session": group_umo,
                                   "conversation_id": cid, "status": "running"}}}
        path = os.path.join(self.tmp.name, "tg.json")
        Path(path).write_text(json.dumps(table), encoding="utf-8")
        importer = LegacyImporter(self.store, self.cm)
        report = await importer.dry_run(path)
        self.assertEqual(len(report.quarantined), 1)
        self.assertIn("unsupported_umo_shape", report.quarantined[0]["reason"])

    async def test_group_umo_importable_with_explicit_owner(self):
        group_umo = "aiocqhttp:group:12345"
        cid = await self.cm.new_conversation(group_umo, title="grp")
        table = {"tasks": {"tg": {"agent": "dsh", "prompt": "p",
                                   "session": group_umo,
                                   "conversation_id": cid, "status": "running"}}}
        path = os.path.join(self.tmp.name, "tgo.json")
        Path(path).write_text(json.dumps(table), encoding="utf-8")
        importer = LegacyImporter(self.store, self.cm,
                                  owner_map={group_umo: "12345"})
        report = await importer.dry_run(path)
        self.assertEqual(len(report.importable), 1)
        self.assertEqual(len(report.quarantined), 0)

    async def test_friendmessage_umo_verified_private(self):
        umo = "example:FriendMessage:100000001"
        cid = await self.cm.new_conversation(umo, title="real")
        table = {"tasks": {"t-real": {
            "agent": "dsh", "prompt": "p", "session": umo,
            "conversation_id": cid, "status": "succeeded", "last_seq": 7}}}
        path = os.path.join(self.tmp.name, "real.json")
        Path(path).write_text(json.dumps(table), encoding="utf-8")
        importer = LegacyImporter(self.store, self.cm)
        report = await importer.dry_run(path)
        self.assertEqual(len(report.importable), 1, report.quarantined)
        self.assertEqual(importer.owner_for(umo), "100000001")
        await importer.apply(report)
        run = await self.store.get_run_by_request("legacy:t-real")
        self.assertIsNotNone(run)
        ws = await self.store.get_workspace(run.workspace_id)
        self.assertEqual(ws.owner_user_id, "100000001")
        self.assertEqual(ws.umo, umo)

    async def test_groupmessage_shape_rejected(self):
        umo = "example:GroupMessage:999"
        cid = await self.cm.new_conversation(umo, title="g")
        table = {"tasks": {"tg": {"agent": "dsh", "prompt": "p", "session": umo,
                                  "conversation_id": cid, "status": "running"}}}
        path = os.path.join(self.tmp.name, "gm.json")
        Path(path).write_text(json.dumps(table), encoding="utf-8")
        importer = LegacyImporter(self.store, self.cm)
        report = await importer.dry_run(path)
        self.assertEqual(len(report.quarantined), 1)
        self.assertIn("unsupported_umo_shape", report.quarantined[0]["reason"])

    async def test_selected_stale_row_quarantined_with_original_record(self):
        # Production-shaped stale running row: no terminal result, must become an
        # explicit needs-review quarantine, never a fake success.
        table = {"tasks": {"t00000000-000000-abcdef": {
            "agent": "dsh", "prompt": "stale", "session": self.umo,
            "conversation_id": self.cid, "status": "running",
            "last_seq": 12, "created": 1.0,
            "worker_log": "^C", "pid": 4242}}}
        path = os.path.join(self.tmp.name, "stale.json")
        Path(path).write_text(json.dumps(table), encoding="utf-8")
        importer = LegacyImporter(self.store, self.cm, selected_quarantine={
            "t00000000-000000-abcdef": {
                "reason": "stale_running_needs_review",
                "evidence": {"pid": 4242, "pid_present": False,
                             "worker_log_tail": "^C", "terminal_result": None},
            }})
        report = await importer.dry_run(path)
        self.assertEqual(len(report.importable), 0)
        self.assertEqual(len(report.quarantined), 1)
        item = report.quarantined[0]
        self.assertEqual(item["reason"], "stale_running_needs_review")
        self.assertEqual(item["original"]["status"], "running")
        self.assertEqual(item["evidence"]["pid"], 4242)
        await importer.apply(report)
        await importer.quarantine_report(report)
        # Not imported as any run (no fake success, no auto-resubmit).
        self.assertIsNone(
            await self.store.get_run_by_request("legacy:t00000000-000000-abcdef"))
        q = await self.store.list_quarantine()
        self.assertEqual(len(q), 1)

    async def test_verify_session_empty_cid_quarantined(self):
        table = {"tasks": {"synthetic-empty-conversation": {
            "agent": "dsh", "prompt": "v", "session": self.umo,
            "conversation_id": "", "status": "running"}}}
        path = os.path.join(self.tmp.name, "v1.json")
        Path(path).write_text(json.dumps(table), encoding="utf-8")
        importer = LegacyImporter(self.store, self.cm)
        report = await importer.dry_run(path)
        self.assertEqual(len(report.quarantined), 1)
        self.assertIn("empty_conversation_id", report.quarantined[0]["reason"])

    async def test_terminal_import_idempotent_and_skips_quarantined(self):
        table = {"tasks": {
            "t-term": {"agent": "dsh", "prompt": "ok", "session": self.umo,
                       "conversation_id": self.cid, "status": "succeeded",
                       "result": "DONE"},
            "t-q": {"agent": "dsh", "prompt": "q", "session": self.umo,
                    "conversation_id": self.cid, "status": "running",
                    "quarantined": True, "quarantine_reason": "needs_review"},
        }}
        path = os.path.join(self.tmp.name, "idem.json")
        Path(path).write_text(json.dumps(table), encoding="utf-8")
        importer = LegacyImporter(self.store, self.cm)
        for _ in range(2):
            report = await importer.dry_run(path)
            await importer.apply(report)
            await importer.quarantine_report(report)
        # Terminal row imported exactly once; quarantined active row never
        # imported (no double worker, no silent resubmit).
        self.assertIsNotNone(await self.store.get_run_by_request("legacy:t-term"))
        self.assertIsNone(await self.store.get_run_by_request("legacy:t-q"))
        runs = await self.store.list_runs((await self.store.get_run_by_request(
            "legacy:t-term")).workspace_id, limit=100)
        self.assertEqual(sum(1 for r in runs if r.request_id == "legacy:t-term"), 1)


if __name__ == "__main__":
    unittest.main()
