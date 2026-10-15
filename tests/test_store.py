# -*- coding: utf-8 -*-
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from helpers import PLUGINS  # noqa: E402

sys.path.insert(0, str(PLUGINS))
from astrbot_plugin_task_workspace.models import (  # noqa: E402
    OutboxItem,
    Run,
    RunStatus,
    Workspace,
)
from astrbot_plugin_task_workspace.store import Store  # noqa: E402


class StoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(os.path.join(self.tmp.name, "t.db"))
        await self.store.migrate()

    async def asyncTearDown(self):
        self.store.close()
        self.tmp.cleanup()

    async def _ws(self, cid, owner="1", umo="u:1:1", ws_id="ws_aaaaaaaa"):
        ws = Workspace(id=ws_id, owner_user_id=owner, umo=umo,
                       conversation_id=cid, title="t")
        await self.store.insert_workspace(ws)
        return ws

    async def test_migrate_idempotent(self):
        again = await self.store.migrate()
        self.assertEqual(again, 2)

    async def test_workspace_roundtrip_and_unknown_cid(self):
        await self._ws("c1")
        got = await self.store.get_workspace_by_cid("c1")
        self.assertIsNotNone(got)
        self.assertEqual(got.title, "t")
        self.assertIsNone(await self.store.get_workspace_by_cid("nope"))
        self.assertIsNone(await self.store.get_workspace_by_cid(""))

    async def test_prefix_free(self):
        ws = await self._ws("c1", ws_id="ws_aaaaaaaa")
        self.assertFalse(await self.store.prefix_free("workspaces", ws.id[:9]))
        self.assertTrue(await self.store.prefix_free("workspaces", "ws_bbbbbb"))

    async def test_run_request_id_unique_and_capacity(self):
        ws = await self._ws("c1")
        run = Run(id="run_aaaaaaaa", workspace_id=ws.id, executor="dsh",
                  request_id="req_1", input_snapshot="{}",
                  status=RunStatus.RUNNING.value)
        await self.store.insert_run(run)
        self.assertFalse(await self.store.workspace_has_capacity(ws.id))
        self.assertIsNotNone(await self.store.get_run_by_request("req_1"))
        dup = Run(id="run_bbbbbbbb", workspace_id=ws.id, executor="dsh",
                  request_id="req_1", input_snapshot="{}")
        with self.assertRaises(Exception):
            await self.store.insert_run(dup)

    async def test_capacity_frees_on_terminal(self):
        ws = await self._ws("c1")
        run = Run(id="run_aaaaaaaa", workspace_id=ws.id, executor="dsh",
                  request_id="req_1", input_snapshot="{}",
                  status=RunStatus.RUNNING.value)
        await self.store.insert_run(run)
        await self.store.update_run(run.id, status=RunStatus.SUCCEEDED.value)
        self.assertTrue(await self.store.workspace_has_capacity(ws.id))

    async def test_outbox_dedupe(self):
        item = OutboxItem(id=None, run_id="run_1", event_key="terminal:succeeded",
                          session="s", content="done")
        self.assertTrue(await self.store.enqueue_outbox(item))
        self.assertFalse(await self.store.enqueue_outbox(item))
        due = await self.store.due_outbox()
        self.assertEqual(len(due), 1)
        await self.store.mark_outbox_failed(due[0].id, "boom", 1)
        self.assertEqual(await self.store.due_outbox(), [])
        conn = self.store._connect()
        row = conn.execute("SELECT attempts, next_attempt_at FROM outbox WHERE id=?",
                           (due[0].id,)).fetchone()
        self.assertEqual(row["attempts"], 1)
        self.assertGreater(row["next_attempt_at"], 0)
        conn.execute("UPDATE outbox SET next_attempt_at=0 WHERE id=?", (due[0].id,))
        claimed = await self.store.claim_outbox()
        self.assertEqual(len(claimed), 1)
        self.assertEqual(claimed[0].attempts, 1)
        await self.store.mark_outbox_sent(due[0].id)
        self.assertEqual(await self.store.due_outbox(), [])

    async def test_pending_consumed(self):
        ws = await self._ws("c1")
        pid = await self.store.add_pending(ws.id, "add tests")
        pending = await self.store.pending_unconsumed(ws.id)
        self.assertEqual(len(pending), 1)
        await self.store.consume_pending([pid], "run_1")
        self.assertEqual(await self.store.pending_unconsumed(ws.id), [])

    async def test_quarantine(self):
        await self.store.quarantine("t1", {"task_id": "t1"}, "unknown_cid")
        rows = await self.store.list_quarantine()
        self.assertEqual(rows[0]["reason"], "unknown_cid")


if __name__ == "__main__":
    unittest.main()
