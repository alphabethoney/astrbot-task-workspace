# -*- coding: utf-8 -*-
"""Runner patch compatibility tests (updated to the SQLite ledger protocol).

Historical test names are kept; behaviour now goes through the durable store.
"""
import unittest

from test_runner_protocol import RunnerBase


class RunnerPatchTests(RunnerBase):
    def test_index_reserve_is_idempotent(self):
        r1 = self.r.index_reserve("req_x", "t0001", "fp1")
        self.assertEqual(r1["task_id"], "t0001")
        r2 = self.r.index_reserve("req_x", "t0002", "fp1")
        self.assertEqual(r2["task_id"], "t0001")
        r3 = self.r.index_reserve("req_x", "t0002", "fpDIFFERENT")
        self.assertTrue(r3.get("conflict"))

    def test_duplicate_submit_is_deduplicated(self):
        code, out = self.submit(self.make_req(request_id="req_dup", task_id="t0001"))
        self.assertEqual(code, 0)
        code2, out2 = self.submit(self.make_req(request_id="req_dup", task_id="t0002"))
        self.assertEqual(code2, 0)
        self.assertTrue(out2.get("deduplicated"))
        self.assertEqual(out2["task_id"], "t0001")
        self.assertEqual(len(list(self.tasks.iterdir())), 1)

    def test_duplicate_submit_fingerprint_conflict(self):
        self.submit(self.make_req(request_id="req_dup", task_id="t0001", prompt="one"))
        code, out = self.submit(self.make_req(request_id="req_dup", prompt="different"))
        self.assertEqual(code, 1)
        self.assertIn("冲突", out["error"])

    def test_reconcile_finds_task_by_request(self):
        code, out = self.submit(self.make_req(request_id="req_rec", task_id="t0002"))
        self.assertEqual(code, 0)
        code, rec = self.do_reconcile(request_id="req_rec")
        self.assertTrue(rec["found"])
        self.assertEqual(rec["task_id"], "t0002")

    def test_reconcile_missing(self):
        code, rec = self.do_reconcile(request_id="req_none")
        self.assertFalse(rec["found"])

    def test_cancel_is_not_terminal_until_worker_dead(self):
        code, out = self.submit(self.make_req(request_id="req_c", task_id="t0003"))
        self.assertEqual(code, 0)
        code, out = self.do_cancel("t0003")
        self.assertEqual(code, 0)
        self.assertTrue(out["accepted"])
        d = self.tasks / "t0003"
        self.assertTrue((d / "cancel.intent").exists())

    def test_prepare_creates_isolated_dir(self):
        code, out = self.do_prepare("ws_abcd1234")
        self.assertEqual(code, 0)
        self.assertTrue(out["ok"])
        self.assertIn("ws_abcd1234", out["cwd"])

    def test_prepare_rejects_bad_workspace_id(self):
        code, out = self.do_prepare("../evil")
        self.assertEqual(code, 1)
        self.assertFalse(out["ok"])


if __name__ == "__main__":
    unittest.main()
