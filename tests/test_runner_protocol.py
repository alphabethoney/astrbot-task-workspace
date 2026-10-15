# -*- coding: utf-8 -*-
"""Focused Windows-runner protocol tests (run on Linux with injected fakes).

Covers: cross-process SQLite reservation, payload fingerprints, launch phases,
worker claims, durable cancellation, timeout validation, permission flags,
workspace isolation, and adapter result extraction.
"""
import contextlib
import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path

RUNNER = Path(__file__).resolve().parents[1] / "legacy" / "agent-bridge-runner" / "runner.py"
RUNNER_DIR = RUNNER.parent


def verified_patch_factory():
    """A supervisor-verified three-entry DSH patch factory (per DSH_CONTRACT)."""
    def body(mode, preset):
        return [
            {"id": "sandbox-policy",
             "config": {"mode": mode, "workspaceRoot": "{workspace}"}},
            {"id": "approval", "config": {"policy": "never"}},
            {"id": "permission",
             "config": {"defaultPreset": preset,
                        "presets": {preset: {"sandbox": mode,
                                             "approval": "never"}}}},
        ]
    return {"verified": True, "arg": "--patch",
            "patches": {"read-only": body("read-only", "runner-read-only"),
                        "workspace-write": body("workspace-write",
                                                "runner-workspace-write")}}


def load_runner(name="bridge_runner_under_test"):
    spec = importlib.util.spec_from_file_location(name, RUNNER)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class FakeScheduler:
    def __init__(self):
        self.created = []
        self.runs = []
        self.deleted = []
        self.fail_create = False
        self.fail_run = False
        self.raise_run = False
        self.on_run = None

    def create(self, name, runcmd):
        self.created.append((name, runcmd))
        return 1 if self.fail_create else 0

    def run(self, name):
        self.runs.append(name)
        if self.raise_run:
            raise OSError("scheduler run boom")
        if self.on_run is not None:
            self.on_run(name)
        return 1 if self.fail_run else 0

    def delete(self, name):
        self.deleted.append(name)
        return 0


class FakeStdin:
    def __init__(self):
        self.data = ""
        self.closed = False

    def write(self, s):
        self.data += s

    def close(self):
        self.closed = True


class BlockingStdin:
    """A stdin pipe whose write blocks until the process is killed."""

    def __init__(self, release):
        self.release = release
        self.data = ""
        self.closed = False
        self.entered = threading.Event()

    def write(self, s):
        self.entered.set()
        self.release.wait(10)
        self.data += s

    def close(self):
        self.closed = True


class FakeProc:
    def __init__(self, lines=None, pid=4242, code=0, block=False,
                 block_stdin=False):
        self._lines = list(lines or [])
        self.pid = pid
        self._code = code
        self._block = block
        self._killed = threading.Event()
        if block_stdin:
            self.stdin = BlockingStdin(self._killed)
        else:
            self.stdin = FakeStdin()
        self.stdout = self._gen()
        self.killed = False
        self.waited = False

    def _gen(self):
        for ln in self._lines:
            yield ln
        if self._block:
            self._killed.wait(10)

    def wait(self):
        self.waited = True
        return self._code

    def kill_signal(self):
        self.killed = True
        self._killed.set()


class RunnerBase(unittest.TestCase):
    def setUp(self):
        self.r = load_runner()
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.tasks = self.root / "tasks"
        self.tasks.mkdir()
        self.r.TASKS = self.tasks
        self.r.INDEX_DIR = self.root / "index"
        self.r.DB_FILE = self.r.INDEX_DIR / "runner.db"
        self.r.CANCEL_POLL_S = 0.01
        self.r.SCHEDULE_ACK_TIMEOUT_S = 0.05
        self.wsroot = self.root / "wsroot"
        self.wsroot.mkdir()
        self.cwd = self.wsroot / "work"
        self.cwd.mkdir()
        self.pol = {
            "default_timeout_s": 60,
            "max_timeout_s": 3600,
            "allowed_roots": [str(self.wsroot)],
            "allowed_permissions": ["read-only", "workspace-write"],
            "permission_ceiling": "workspace-write",
            "agents": {
                "dsh": {"exe": "dsh", "args": ["--profile", "headless", "--json", "-"],
                        "kind": "dsh", "default_permission": "read-only",
                        "permission_patch": verified_patch_factory()},
                "codex": {"exe": "codex", "args": ["exec", "--skip-git-repo-check", "-"],
                          "kind": "codex", "default_permission": "read-only"},
                "dsh_verified": {
                    "exe": "dsh", "args": ["--profile", "headless", "--json", "-"],
                    "kind": "dsh", "default_permission": "read-only",
                    "permission_env": {"env": "DSH_PERMISSION_MODE",
                                       "verified": True,
                                       "supported": ["read-only", "workspace-write"]}},
                "dsh_unverified": {
                    "exe": "dsh", "args": ["--profile", "headless", "--json", "-"],
                    "kind": "dsh", "default_permission": "read-only"},
                "dsh_patched": {
                    "exe": "dsh", "args": ["--profile", "headless", "--json", "-"],
                    "kind": "dsh", "default_permission": "read-only",
                    "permission_patch": verified_patch_factory(),
                },
            },
        }
        self.r.policy = lambda: self.pol
        self.sched = FakeScheduler()
        self.r.SCHEDULER = self.sched
        self.alive = set()
        self.r.PID_ALIVE = lambda pid: pid is not None and str(pid) in self.alive
        self.killed_pids = []
        self.procs = {}

        def _kill(pid):
            self.killed_pids.append(pid)
            p = self.procs.get(pid)
            if p is not None:
                p.kill_signal()

        self.r.KILL_TREE = _kill

    def tearDown(self):
        self.tmp.cleanup()

    # ---------------------------------------------------------------- helpers
    def submit(self, req):
        buf = io.StringIO()
        old = sys.stdin
        sys.stdin = types.SimpleNamespace(
            buffer=io.BytesIO(json.dumps(req).encode("utf-8")))
        try:
            with contextlib.redirect_stdout(buf):
                code = self.r.cmd_submit(None)
        finally:
            sys.stdin = old
        lines = [l for l in buf.getvalue().strip().splitlines() if l.strip()]
        out = json.loads(lines[0]) if lines else {}
        return code, out

    def run_worker(self, tid):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = self.r.cmd_run(types.SimpleNamespace(task_id=tid))
        return code, buf.getvalue()

    def do_cancel(self, tid):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = self.r.cmd_cancel(types.SimpleNamespace(task_id=tid))
        lines = [l for l in buf.getvalue().strip().splitlines() if l.strip()]
        return code, (json.loads(lines[0]) if lines else {})

    def do_reconcile(self, request_id="", task_id=""):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = self.r.cmd_reconcile(
                types.SimpleNamespace(request_id=request_id, task_id=task_id))
        lines = [l for l in buf.getvalue().strip().splitlines() if l.strip()]
        return code, (json.loads(lines[0]) if lines else {})

    def do_watch(self, tid, after=0):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = self.r.cmd_watch(
                types.SimpleNamespace(task_id=tid, after=after))
        lines = [json.loads(l) for l in buf.getvalue().strip().splitlines() if l.strip()]
        return code, lines

    def do_prepare(self, workspace_id, root=None):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = self.r.cmd_prepare(types.SimpleNamespace(
                workspace_id=workspace_id, root=str(root or self.wsroot)))
        lines = [l for l in buf.getvalue().strip().splitlines() if l.strip()]
        return code, (json.loads(lines[0]) if lines else {})

    def make_req(self, **kw):
        req = {"request_id": "req1", "agent": "dsh", "cwd": str(self.cwd),
               "prompt": "do the thing", "permission": "read-only",
               "timeout_s": 30}
        req.update(kw)
        return req

    def install_proc(self, proc, record=None):
        def fake_popen(cmd, **kwargs):
            if record is not None:
                record["cmd"] = cmd
                record["cwd"] = kwargs.get("cwd")
                record["env"] = kwargs.get("env")
            self.procs[proc.pid] = proc
            return proc
        self.r.POPEN = fake_popen

    def state_of(self, tid):
        return json.loads((self.tasks / tid / "state.json").read_text(encoding="utf-8"))


class ReservationTests(RunnerBase):
    def test_index_reserve_idempotent_and_conflict(self):
        r1 = self.r.index_reserve("req_x", "t0001", "fp1")
        self.assertEqual(r1["task_id"], "t0001")
        self.assertTrue(r1.get("reserved"))
        r2 = self.r.index_reserve("req_x", "t0002", "fp1")
        self.assertEqual(r2["task_id"], "t0001")
        self.assertFalse(r2.get("reserved"))
        r3 = self.r.index_reserve("req_x", "t0002", "fpDIFFERENT")
        self.assertTrue(r3.get("conflict"))

    def test_submit_winner_then_dedupe_no_truncation(self):
        code, out = self.submit(self.make_req(request_id="req_dup", task_id="t0001"))
        self.assertEqual(code, 0)
        self.assertEqual(out["task_id"], "t0001")
        events = self.tasks / "t0001" / "events.jsonl"
        events.write_text('{"seq":1,"type":"marker"}\n', encoding="utf-8")
        code2, out2 = self.submit(self.make_req(request_id="req_dup", task_id="t0009"))
        self.assertEqual(code2, 0)
        self.assertTrue(out2.get("deduplicated"))
        self.assertEqual(out2["task_id"], "t0001")
        self.assertIn("marker", events.read_text(encoding="utf-8"))
        self.assertEqual(len(list(self.tasks.iterdir())), 1)

    def test_changed_prompt_conflict(self):
        self.submit(self.make_req(request_id="req_a", task_id="t0001", prompt="one"))
        code, out = self.submit(self.make_req(request_id="req_a", prompt="two"))
        self.assertEqual(code, 1)
        self.assertIn("冲突", out["error"])

    def test_external_session_id_rejected(self):
        # Persisted sessions carry higher-priority sandbox/approval settings.
        code, out = self.submit(self.make_req(request_id="req_s", task_id="t0001",
                                              session_id="sess-a"))
        self.assertEqual(code, 1)
        self.assertFalse(out["ok"])
        self.assertEqual(self.sched.created, [])

    def test_changed_defaults_affect_fingerprint(self):
        # same request with different cwd must conflict (cwd is part of fingerprint)
        code1, out1 = self.submit(self.make_req(request_id="req_c", task_id="t0001"))
        self.assertEqual(code1, 0)
        other = self.wsroot / "other"
        other.mkdir()
        code2, out2 = self.submit(self.make_req(request_id="req_c", cwd=str(other)))
        self.assertEqual(code2, 1)

    def test_task_id_collision_with_legacy_dir(self):
        legacy = self.tasks / "tlegacy"
        legacy.mkdir()
        (legacy / "request.json").write_text("{}", encoding="utf-8")
        code, out = self.submit(self.make_req(request_id="req_l", task_id="tlegacy"))
        self.assertEqual(code, 1)
        self.assertIn("task_id", out)

    def test_case_alias_rejected(self):
        (self.tasks / "tAbc").mkdir()
        (self.tasks / "tAbc" / "request.json").write_text("{}", encoding="utf-8")
        code, out = self.submit(self.make_req(request_id="req_case", task_id="tabc"))
        self.assertEqual(code, 1)

    def test_scheduler_failure_not_success_and_recovery(self):
        self.sched.fail_create = True
        code, out = self.submit(self.make_req(request_id="req_r", task_id="t0001"))
        self.assertEqual(code, 1)
        self.assertFalse(out["ok"])
        rec = self.r._store().get(request_id="req_r")
        self.assertEqual(rec["launch_phase"], "reserved")
        # known-unlaunched -> a later submit may recover the SAME id
        self.sched.fail_create = False
        code2, out2 = self.submit(self.make_req(request_id="req_r", task_id="t0001"))
        self.assertEqual(code2, 0)
        self.assertEqual(out2["task_id"], "t0001")
        self.assertEqual(len(self.sched.runs), 1)

    def test_scheduler_run_exception_is_unknown_no_rerun(self):
        self.sched.raise_run = True
        code, out = self.submit(self.make_req(request_id="req_u", task_id="t0001"))
        self.assertEqual(code, 1)
        self.assertTrue(out.get("unknown"))
        rec = self.r._store().get(task_id="t0001")
        self.assertEqual(rec["launch_phase"], "unknown")
        # worker must not claim an uncertain launch
        self.assertFalse(self.r._store().claim("t0001", "pid:1", "tok"))
        code2, _ = self.submit(self.make_req(request_id="req_u", task_id="t0001"))
        self.assertEqual(code2, 0)  # deduplicated, not re-scheduled
        self.assertEqual(len(self.sched.runs), 1)

    def test_cross_process_reservation_same_request(self):
        script = (
            "import importlib.util,pathlib,sys,json\n"
            "spec=importlib.util.spec_from_file_location('runner_mp',sys.argv[1])\n"
            "m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)\n"
            "m.TASKS=pathlib.Path(sys.argv[3]);m.DB_FILE=pathlib.Path(sys.argv[2])\n"
            "try:\n"
            "  print(json.dumps({'res':m.index_reserve(sys.argv[4],sys.argv[5],sys.argv[6])}))\n"
            "except Exception as e:\n"
            "  print(json.dumps({'err':repr(e)}))\n"
        )
        procs = []
        for tid in ("t0001", "t0002", "t0003", "t0004"):
            p = subprocess.Popen(
                [sys.executable, "-c", script, str(RUNNER), str(self.r.DB_FILE),
                 str(self.tasks), "req_cross", tid, "fp"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            procs.append(p)
        results = []
        for p in procs:
            out, err = p.communicate(timeout=60)
            self.assertEqual(p.returncode, 0, err)
            results.append(json.loads(out.strip().splitlines()[-1]))
        winners = [x["res"] for x in results
                   if "res" in x and x["res"].get("reserved")]
        tids = {x["res"]["task_id"] for x in results if "res" in x}
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(tids), 1)

    def test_cross_process_conflicting_payload(self):
        script = (
            "import importlib.util,pathlib,sys,json\n"
            "spec=importlib.util.spec_from_file_location('runner_mp',sys.argv[1])\n"
            "m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)\n"
            "m.TASKS=pathlib.Path(sys.argv[3]);m.DB_FILE=pathlib.Path(sys.argv[2])\n"
            "print(json.dumps({'res':m.index_reserve(sys.argv[4],sys.argv[5],sys.argv[6])}))\n"
        )
        procs = []
        for fp in ("fpA", "fpB"):
            p = subprocess.Popen(
                [sys.executable, "-c", script, str(RUNNER), str(self.r.DB_FILE),
                 str(self.tasks), "req_conf", "t" + fp[-1].lower() * 3, fp],
                stdout=subprocess.PIPE, text=True)
            procs.append(p)
        outs = [json.loads(p.communicate(timeout=60)[0].strip()) for p in procs]
        conflicts = [o["res"] for o in outs if o["res"].get("conflict")]
        winners = [o["res"] for o in outs if o["res"].get("reserved")]
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(len(winners), 1)

    def test_db_unreadable_reconcile_is_unknown(self):
        blocker = self.root / "blocker"
        blocker.write_text("x", encoding="utf-8")
        self.r.DB_FILE = blocker / "nested.db"
        code, out = self.do_reconcile(request_id="req_x")
        self.assertEqual(code, 1)
        self.assertFalse(out["ok"])
        self.assertTrue(out.get("unknown"))

    def test_no_age_lock_eviction_helper(self):
        self.assertFalse(hasattr(self.r, "_with_lock"))
        self.assertFalse(hasattr(self.r, "LOCK_FILE"))


class WorkerClaimTests(RunnerBase):
    def test_double_worker_only_one_spawns(self):
        code, out = self.submit(self.make_req(request_id="req_w", task_id="t0001"))
        self.assertEqual(code, 0)
        rec = {"cmd": None}
        proc = FakeProc(lines=['{"type":"final","text":"ok"}\n'], pid=1001)
        self.install_proc(proc, rec)
        self.run_worker("t0001")
        self.run_worker("t0001")
        self.assertEqual(rec["cmd"] is not None, True)
        # second worker skipped: only one spawn total
        self.assertEqual(len(self.procs), 1)

    def test_worker_revalidates_cwd(self):
        code, out = self.submit(self.make_req(request_id="req_wc", task_id="t0001"))
        self.assertEqual(code, 0)
        req_path = self.tasks / "t0001" / "request.json"
        req = json.loads(req_path.read_text(encoding="utf-8"))
        req["cwd"] = str(self.root / "outside")
        req_path.write_text(json.dumps(req), encoding="utf-8")
        self.install_proc(FakeProc(lines=[]))
        self.run_worker("t0001")
        self.assertEqual(self.state_of("t0001")["status"], "failed")


class CancellationTests(RunnerBase):
    def _new_task(self, tid="t0001"):
        code, out = self.submit(self.make_req(request_id="req_" + tid, task_id=tid))
        self.assertEqual(code, 0)
        return tid

    def test_cancel_queued_pre_spawn(self):
        tid = self._new_task()
        self.r._store().set_cancel(tid)
        called = {"n": 0}

        def fake_popen(*a, **k):
            called["n"] += 1
            raise AssertionError("must not spawn after cancel")
        self.r.POPEN = fake_popen
        self.run_worker(tid)
        self.assertEqual(called["n"], 0)
        self.assertEqual(self.state_of(tid)["status"], "cancelled")

    def test_cancel_command_does_not_kill_pid(self):
        tid = self._new_task()
        d = self.tasks / tid
        (d / "state.json").write_text(json.dumps(
            {"status": "running", "worker_pid": 111, "child_pid": 222}), encoding="utf-8")
        code, out = self.do_cancel(tid)
        self.assertEqual(code, 0)
        self.assertTrue(out["accepted"])
        self.assertEqual(self.killed_pids, [])
        self.assertTrue((d / "cancel.intent").exists())
        self.assertTrue(self.r._store().cancel_requested(tid))

    def test_cancel_nonexistent_no_phantom_dir(self):
        code, out = self.do_cancel("tnope")
        self.assertEqual(code, 1)
        self.assertFalse(out["found"])
        self.assertFalse((self.tasks / "tnope").exists())

    def test_double_cancel_terminal_idempotent(self):
        tid = self._new_task()
        d = self.tasks / tid
        (d / "state.json").write_text(json.dumps(
            {"status": "succeeded", "final": "done"}), encoding="utf-8")
        self.r._store().update(tid, phase="terminal", status="succeeded")
        before = (d / "state.json").read_text(encoding="utf-8")
        code, out = self.do_cancel(tid)
        self.assertEqual(code, 0)
        self.assertTrue(out["already_terminal"])
        self.assertEqual((d / "state.json").read_text(encoding="utf-8"), before)

    def test_cancel_during_run_worker_kills_and_finalizes(self):
        tid = self._new_task()
        proc = FakeProc(lines=['{"type":"text","text":"working"}\n'], pid=7777, block=True)
        self.install_proc(proc)
        calls = {"n": 0}

        def fake_cancel(*a, **k):
            calls["n"] += 1
            return calls["n"] >= 3
        self.r.cancel_pending = fake_cancel
        self.run_worker(tid)
        self.assertEqual(self.state_of(tid)["status"], "cancelled")
        self.assertIn(7777, self.killed_pids)
        self.assertTrue(proc.killed)

    def test_reconcile_liveness_uncertain_stays_requested(self):
        tid = self._new_task()
        d = self.tasks / tid
        (d / "state.json").write_text(json.dumps(
            {"status": "running", "worker_pid": 111, "child_pid": 222}), encoding="utf-8")
        self.r._store().set_cancel(tid)
        self.alive = {"222"}
        code, out = self.do_reconcile(task_id=tid)
        self.assertEqual(out["status"], "cancel_requested")
        self.alive = set()
        code, out = self.do_reconcile(task_id=tid)
        self.assertEqual(out["status"], "cancelled")

    def test_watch_finalizes_cancel_after_death(self):
        tid = self._new_task()
        d = self.tasks / tid
        (d / "state.json").write_text(json.dumps(
            {"status": "running", "worker_pid": 111, "child_pid": 222}), encoding="utf-8")
        self.r._store().set_cancel(tid)
        self.alive = set()
        code, records = self.do_watch(tid)
        state = [r for r in records if r.get("type") == "_state"][0]["state"]
        self.assertEqual(state["status"], "cancelled")


class TimeoutTests(RunnerBase):
    def test_invalid_timeout_rejected_before_subprocess(self):
        for bad in (0, -5, "abc"):
            code, out = self.submit(self.make_req(request_id="req_t" + str(bad),
                                                  task_id="t" + str(abs(hash(str(bad))))[:6],
                                                  timeout_s=bad))
            self.assertEqual(code, 1, bad)
        self.assertEqual(self.sched.created, [])

    def test_timeout_validation_in_worker_no_spawn(self):
        code, out = self.submit(self.make_req(request_id="req_tw", task_id="t0001"))
        self.assertEqual(code, 0)
        req_path = self.tasks / "t0001" / "request.json"
        req = json.loads(req_path.read_text(encoding="utf-8"))
        req["timeout_s"] = -1
        req_path.write_text(json.dumps(req), encoding="utf-8")
        called = {"n": 0}

        def fake_popen(*a, **k):
            called["n"] += 1
            return FakeProc()
        self.r.POPEN = fake_popen
        self.run_worker("t0001")
        self.assertEqual(called["n"], 0)
        self.assertEqual(self.state_of("t0001")["status"], "failed")

    def test_timeout_kills_process(self):
        tid = "t0001"
        code, out = self.submit(self.make_req(request_id="req_to", task_id=tid,
                                              timeout_s=1))
        self.assertEqual(code, 0)
        proc = FakeProc(lines=[], pid=8888, block=True)
        self.install_proc(proc)
        self.run_worker(tid)
        self.assertIn(8888, self.killed_pids)
        self.assertEqual(self.state_of(tid)["status"], "timed_out")


class PermissionTests(RunnerBase):
    def test_invalid_mode_rejected(self):
        code, out = self.submit(self.make_req(request_id="req_p1", task_id="t0001",
                                              permission="bogus"))
        self.assertEqual(code, 1)

    def test_danger_rejected_by_default(self):
        code, out = self.submit(self.make_req(request_id="req_p2", task_id="t0001",
                                              permission="danger-full-access"))
        self.assertEqual(code, 1)

    def test_invalid_adapter_kind_rejected(self):
        self.pol["agents"]["weird"] = {"exe": "x", "args": [], "kind": "nope"}
        code, out = self.submit(self.make_req(request_id="req_p3", task_id="t0001",
                                              agent="weird"))
        self.assertEqual(code, 1)

    def test_codex_flags_stripped_and_single_sandbox(self):
        spec = {"exe": "codex", "kind": "codex",
                "args": ["exec", "--sandbox=danger-full-access", "--full-auto",
                         "-s", "workspace-write", "-c", "model=evil",
                         "--config", "x=y", "--dangerously-bypass-approvals-and-sandbox",
                         "-"]}
        cmd = self.r.build_agent_command(spec, "codex", {}, "read-only")
        self.assertEqual(cmd.count("--sandbox"), 1)
        self.assertNotIn("--full-auto", cmd)
        self.assertNotIn("--dangerously-bypass-approvals-and-sandbox", cmd)
        self.assertNotIn("workspace-write", cmd)
        self.assertEqual(cmd[cmd.index("--sandbox") + 1], "read-only")
        self.assertIn("-", cmd)

    def test_dsh_unverified_all_modes_failclosed(self):
        # read-only must NOT be granted a silent fallback: env-only/absent
        # patches are rejected for every mode (installed default is
        # workspace-write/ask, so env alone cannot promise approval=never).
        spec = self.pol["agents"]["dsh_unverified"]
        d = self.tasks / "tperm"
        d.mkdir(exist_ok=True)
        for mode in ("read-only", "workspace-write"):
            with self.assertRaises(ValueError):
                self.r.prepare_dsh_permission_args(spec, mode, str(self.cwd), d)
        env_spec = self.pol["agents"]["dsh_verified"]
        with self.assertRaises(ValueError):
            self.r.prepare_dsh_permission_args(env_spec, "read-only",
                                               str(self.cwd), d)

    def test_dsh_unverified_rejected_before_scheduling(self):
        code, out = self.submit(self.make_req(request_id="req_uv", task_id="t0001",
                                              agent="dsh_unverified"))
        self.assertEqual(code, 1)
        self.assertFalse(out["ok"])
        self.assertEqual(self.sched.created, [])
        self.assertEqual(self.r._store().get(request_id="req_uv"), None)

    def test_dsh_session_resume_flags_rejected(self):
        self.pol["agents"]["dsh_bad"] = {
            "exe": "dsh", "kind": "dsh", "default_permission": "read-only",
            "args": ["--profile", "headless", "--resume", "sess-1", "--json", "-"],
            "permission_patch": verified_patch_factory()}
        code, out = self.submit(self.make_req(request_id="req_res", task_id="t0001",
                                              agent="dsh_bad"))
        self.assertEqual(code, 1)

    def test_dsh_verified_patch_generates_overlay_and_arg(self):
        spec = self.pol["agents"]["dsh_patched"]
        d = self.tasks / "tperm2"
        d.mkdir(exist_ok=True)
        args = self.r.prepare_dsh_permission_args(spec, "workspace-write",
                                                  str(self.cwd), d)
        self.assertEqual(args[0], "--patch")
        overlay = Path(args[1])
        self.assertTrue(overlay.exists())
        text = overlay.read_text(encoding="utf-8")
        self.assertIn("mode: 'workspace-write'", text)
        self.assertIn(str(self.cwd), text)
        self.assertIn("policy: 'never'", text)
        cmd = self.r.build_agent_command(spec, "dsh", {}, "workspace-write",
                                         extra_args=args)
        # Canonical installed order: --profile headless --patch <file> --json -
        self.assertEqual(cmd[1:3], ["--profile", "headless"])
        self.assertEqual(cmd[3], "--patch")
        self.assertLess(cmd.index("--patch"), cmd.index("--json"))
        self.assertEqual(cmd[-1], "-")
        self.assertEqual(cmd[cmd.index("--json") + 1], "-")

    def test_dsh_patch_rejects_workspace_root_mismatch(self):
        spec = {"exe": "dsh", "kind": "dsh", "default_permission": "read-only",
                "args": ["--profile", "headless", "--json", "-"],
                "permission_patch": {
                    "verified": True, "arg": "--patch",
                    "patches": {"read-only": [
                        {"id": "sandbox-policy",
                         "config": {"mode": "read-only",
                                    "workspaceRoot": str(self.root / "elsewhere")}},
                        {"id": "approval", "config": {"policy": "never"}},
                        {"id": "permission",
                         "config": {"defaultPreset": "p",
                                    "presets": {"p": {"sandbox": "read-only",
                                                      "approval": "never"}}}}]}}}
        d = self.tasks / "tperm3"
        d.mkdir(exist_ok=True)
        with self.assertRaises(ValueError):
            self.r.prepare_dsh_permission_args(spec, "read-only", str(self.cwd), d)

    def test_dsh_verified_env_injected(self):
        spec = self.pol["agents"]["dsh_verified"]
        env = self.r.build_agent_env(spec, "dsh", "workspace-write")
        self.assertEqual(env.get("DSH_PERMISSION_MODE"), "workspace-write")

    def test_stdin_transport_used(self):
        tid = "t0001"
        code, out = self.submit(self.make_req(request_id="req_stdin", task_id=tid,
                                              prompt="hello 世界"))
        self.assertEqual(code, 0)
        rec = {}
        proc = FakeProc(lines=['{"type":"final","text":"ok"}\n'], pid=1002)
        self.install_proc(proc, rec)
        self.run_worker(tid)
        self.assertEqual(proc.stdin.data, "hello 世界")
        self.assertTrue(proc.stdin.closed)
        self.assertIn("-", rec["cmd"])


class IsolationTests(RunnerBase):
    def test_task_id_validation(self):
        for bad in ("CON", "nul.txt", "a/b", "..\\x", "name.", " name", "a:b", ""):
            with self.assertRaises(ValueError, msg=bad):
                self.r.td(bad)

    def test_cwd_missing_and_file(self):
        code, out = self.submit(self.make_req(request_id="req_i1",
                                              cwd=str(self.wsroot / "nope")))
        self.assertEqual(code, 1)
        afile = self.wsroot / "afile"
        afile.write_text("x", encoding="utf-8")
        code, out = self.submit(self.make_req(request_id="req_i2", cwd=str(afile)))
        self.assertEqual(code, 1)

    def test_sibling_prefix_rejected(self):
        sibling = self.root / "wsroot-evil"
        sibling.mkdir()
        code, out = self.submit(self.make_req(request_id="req_i3", cwd=str(sibling)))
        self.assertEqual(code, 1)

    def test_prepare_distinct_and_bounded(self):
        code1, out1 = self.do_prepare("ws_aaaa1111")
        code2, out2 = self.do_prepare("ws_bbbb2222")
        self.assertEqual(code1, 0)
        self.assertEqual(code2, 0)
        self.assertNotEqual(out1["cwd"], out2["cwd"])
        self.assertTrue(Path(out1["cwd"]).is_dir())

    def test_prepare_rejects_traversal_and_reserved(self):
        for wid in ("../evil", "CON", "a/b", "abc."):
            code, out = self.do_prepare(wid)
            self.assertEqual(code, 1, wid)
            self.assertFalse(out["ok"])
        self.assertFalse((self.wsroot / "evil").exists())

    def test_prepare_refuses_symlink_target(self):
        outside = self.root / "outside"
        outside.mkdir()
        link = self.wsroot / "ws_link1"
        try:
            os.symlink(outside, link)
        except (OSError, NotImplementedError):
            self.skipTest("symlink not supported")
        code, out = self.do_prepare("ws_link1")
        self.assertEqual(code, 1)


class AdapterResultTests(RunnerBase):
    def test_dsh_final_and_session(self):
        ev = self.r.ad_dsh.parse('{"type":"session","sessionId":"s-1"}')
        self.assertEqual(ev[0], "session")
        self.assertEqual(ev[1], "s-1")
        ev = self.r.ad_dsh.parse('{"type":"final","text":"  answer  \\n"}')
        self.assertEqual(ev[0], "final")
        self.assertEqual(ev[1], "  answer  \n")

    def test_codex_nested_agent_message(self):
        line = json.dumps({"type": "item.completed",
                           "item": {"id": "item_1", "type": "agent_message",
                                    "text": "最终回答"}})
        ev = self.r.ad_codex.parse(line)
        self.assertEqual(ev[0], "final")
        self.assertEqual(ev[1], "最终回答")

    def test_codex_text_error_not_failure(self):
        ev = self.r.ad_codex.parse("this is an error but not fatal")
        self.assertEqual(ev[0], "text")
        ev = self.r.ad_codex.parse('{"type":"error","message":"boom"}')
        self.assertEqual(ev[0], "error")
        self.assertTrue(ev[2].get("terminal"))

    def test_long_unicode_result_fully_retrievable(self):
        text = "答案ABC" * 1500
        tid = "t0001"
        code, out = self.submit(self.make_req(request_id="req_long", task_id=tid))
        self.assertEqual(code, 0)
        line = json.dumps({"type": "final", "text": text}, ensure_ascii=False) + "\n"
        proc = FakeProc(lines=[line], pid=1003)
        self.install_proc(proc)
        self.run_worker(tid)
        state = self.state_of(tid)
        self.assertEqual(state["status"], "succeeded")
        self.assertEqual(state["final"], text)
        self.assertGreater(len(state["final"]), 2000)
        code, rec = self.do_reconcile(task_id=tid)
        self.assertTrue(rec["found"])
        self.assertEqual(rec["final"], text)
        self.assertEqual(rec["status"], "succeeded")

    def test_streaming_chunks_concatenated(self):
        tid = "t0001"
        code, out = self.submit(self.make_req(request_id="req_stream", task_id=tid,
                                              agent="codex"))
        self.assertEqual(code, 0)
        lines = [
            json.dumps({"type": "item.delta", "delta": "Hel"}) + "\n",
            json.dumps({"type": "item.delta", "delta": "lo "}) + "\n",
            json.dumps({"type": "item.delta", "delta": "世界"}) + "\n",
        ]
        proc = FakeProc(lines=lines, pid=1004)
        self.install_proc(proc)
        self.run_worker(tid)
        state = self.state_of(tid)
        self.assertEqual(state["final"], "Hello 世界")

    def test_watch_state_skips_sentinel_as_event(self):
        tid = "t0001"
        code, out = self.submit(self.make_req(request_id="req_watch", task_id=tid))
        self.assertEqual(code, 0)
        proc = FakeProc(lines=['{"type":"text","text":"hi"}\n'], pid=1005)
        self.install_proc(proc)
        self.run_worker(tid)
        code, records = self.do_watch(tid)
        types_ = [r.get("type") for r in records]
        self.assertIn("_state", types_)
        # _state is the terminal sentinel only, never mixed into seq events
        self.assertEqual(types_.count("_state"), 1)
        self.assertEqual(records[-1]["type"], "_state")
        self.assertEqual(records[-1]["state"]["status"], "succeeded")

    def test_terminal_error_with_zero_exit_is_failed(self):
        tid = "t0001"
        code, out = self.submit(self.make_req(request_id="req_terr", task_id=tid))
        self.assertEqual(code, 0)
        proc = FakeProc(lines=['{"type":"error","message":"boom"}\n'], pid=1006, code=0)
        self.install_proc(proc)
        self.run_worker(tid)
        state = self.state_of(tid)
        self.assertEqual(state["status"], "failed")
        self.assertEqual(state.get("error"), "boom")

    def test_dsh_session_preserved(self):
        tid = "t0001"
        code, out = self.submit(self.make_req(request_id="req_sess", task_id=tid))
        self.assertEqual(code, 0)
        lines = ['{"type":"session","sessionId":"sess-42"}\n',
                 '{"type":"final","text":"ok"}\n']
        proc = FakeProc(lines=lines, pid=1007)
        self.install_proc(proc)
        self.run_worker(tid)
        self.assertEqual(self.state_of(tid)["session_id"], "sess-42")

    def test_capabilities_reports_version(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            self.r.cmd_capabilities(None)
        out = json.loads(buf.getvalue().strip())
        self.assertTrue(out["ok"])
        self.assertEqual(out["version"], self.r.RUNNER_VERSION)
        self.assertTrue(out["full_result"])


class ReconcileLegacyTests(RunnerBase):
    def test_legacy_task_found_without_rewrite(self):
        d = self.tasks / "told"
        d.mkdir()
        (d / "request.json").write_text(json.dumps(
            {"task_id": "told", "request_id": "req_old", "agent": "dsh",
             "cwd": str(self.cwd), "prompt": "p"}), encoding="utf-8")
        (d / "state.json").write_text(json.dumps(
            {"status": "succeeded", "final": "legacy result"}), encoding="utf-8")
        (d / "result.txt").write_text("legacy result", encoding="utf-8")
        before = (d / "request.json").read_text(encoding="utf-8")
        code, out = self.do_reconcile(request_id="req_old")
        self.assertEqual(code, 0)
        self.assertTrue(out["found"])
        self.assertEqual(out["task_id"], "told")
        self.assertEqual(out["status"], "succeeded")
        self.assertEqual(out["final"], "legacy result")
        self.assertEqual((d / "request.json").read_text(encoding="utf-8"), before)

    def test_authoritative_absence(self):
        code, out = self.do_reconcile(request_id="req_missing")
        self.assertEqual(code, 0)
        self.assertTrue(out["ok"])
        self.assertFalse(out["found"])

    def test_reconcile_requires_key(self):
        code, out = self.do_reconcile()
        self.assertEqual(code, 1)
        self.assertTrue(out.get("unknown"))


class SchedulerAckTests(RunnerBase):
    def test_definition_kept_until_worker_ack(self):
        self.r.SCHEDULE_ACK_TIMEOUT_S = 1.0

        def claim_soon(_name):
            def _c():
                time.sleep(0.02)
                self.r._store().claim("t0001", "pid:worker", "tok",
                                      worker_pid=999)
            threading.Thread(target=_c, daemon=True).start()

        self.sched.on_run = claim_soon
        code, out = self.submit(self.make_req(request_id="req_ack",
                                              task_id="t0001"))
        self.assertEqual(code, 0)
        self.assertTrue(out["worker_started"])
        self.assertTrue(out["worker_acknowledged"])
        self.assertFalse(out["pending_ack"])
        # deletion happens only AFTER the durable claim was observed
        self.assertEqual(self.sched.deleted, ["qqagent-t0001"])

    def test_ack_timeout_keeps_definition_and_never_implies_running(self):
        self.r.SCHEDULE_ACK_TIMEOUT_S = 0.05
        code, out = self.submit(self.make_req(request_id="req_noack",
                                              task_id="t0001"))
        self.assertEqual(code, 0)
        self.assertFalse(out["worker_started"])
        self.assertTrue(out["pending_ack"])
        self.assertEqual(self.sched.deleted, [])
        rec = self.r._store().get(task_id="t0001")
        # never reset to unlaunched just because the ack was slow
        self.assertEqual(rec["launch_phase"], "scheduling")

    def test_later_claim_observed_after_scheduler_timeout(self):
        self.r.SCHEDULE_ACK_TIMEOUT_S = 0.05
        self.submit(self.make_req(request_id="req_late", task_id="t0001"))
        self.assertEqual(self.sched.deleted, [])
        # a later worker claim is visible to reconcile
        self.assertTrue(self.r._store().claim("t0001", "pid:late", "tok",
                                              worker_pid=1000))
        code, rec = self.do_reconcile(request_id="req_late")
        self.assertTrue(rec["found"])
        self.assertEqual(rec["launch_phase"], "claimed")


class CrossProcessInitRaceTests(RunnerBase):
    SCRIPT = (
        "import importlib.util,io,json,pathlib,sys,time,types\n"
        "spec=importlib.util.spec_from_file_location('runner_race',sys.argv[1])\n"
        "m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)\n"
        "m.TASKS=pathlib.Path(sys.argv[2]);m.DB_FILE=pathlib.Path(sys.argv[3])\n"
        "pol=json.loads(pathlib.Path(sys.argv[4]).read_text())\n"
        "m.policy=lambda: pol\n"
        "m.SCHEDULE_ACK_TIMEOUT_S=0.05\n"
        "class S:\n"
        "    def create(self,n,r): return 0\n"
        "    def run(self,n): return 0\n"
        "    def delete(self,n): return 0\n"
        "m.SCHEDULER=S()\n"
        "role=sys.argv[5];marker=pathlib.Path(sys.argv[6])\n"
        "count=marker/'ensure_count';a_in=marker/'a_in';b_done=marker/'b_done'\n"
        "orig=m._ensure_task_files\n"
        "def wrapper(d,tid,req,payload):\n"
        "    with open(count,'a') as fh: fh.write(role+'\\n')\n"
        "    if role=='a':\n"
        "        a_in.write_text('1')\n"
        "        for _ in range(500):\n"
        "            if b_done.exists(): break\n"
        "            time.sleep(0.01)\n"
        "    return orig(d,tid,req,payload)\n"
        "m._ensure_task_files=wrapper\n"
        "if role=='b':\n"
        "    for _ in range(500):\n"
        "        if a_in.exists(): break\n"
        "        time.sleep(0.01)\n"
        "req=json.loads(pathlib.Path(sys.argv[7]).read_text())\n"
        "sys.stdin=types.SimpleNamespace(buffer=io.BytesIO(json.dumps(req).encode()))\n"
        "m.cmd_submit(None)\n"
        "if role=='b': b_done.write_text('1')\n"
    )

    def test_two_submitters_paused_before_files_single_writer(self):
        marker = self.root / "markers"
        marker.mkdir()
        polfile = self.root / "pol.json"
        polfile.write_text(json.dumps(self.pol), encoding="utf-8")
        reqfile = self.root / "req.json"
        reqfile.write_text(json.dumps(
            self.make_req(request_id="req_race", task_id="t0001")),
            encoding="utf-8")

        def spawn(role):
            return subprocess.Popen(
                [sys.executable, "-c", self.SCRIPT, str(RUNNER),
                 str(self.tasks), str(self.r.DB_FILE), str(polfile), role,
                 str(marker), str(reqfile)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        a = spawn("a")
        try:
            deadline = time.time() + 10
            while not (marker / "a_in").exists() and time.time() < deadline:
                time.sleep(0.01)
            self.assertTrue((marker / "a_in").exists(),
                            "winner never reached initialization")
            b = spawn("b")
            outb, errb = b.communicate(timeout=60)
            outa, erra = a.communicate(timeout=60)
        finally:
            for p in (a,):
                if p.poll() is None:
                    p.kill()
        self.assertEqual(a.returncode, 0, erra)
        self.assertEqual(b.returncode, 0, errb)
        writers = (marker / "ensure_count").read_text().split()
        # exactly one process ever wrote task files: no concurrent .tmp/run.cmd
        self.assertEqual(writers, ["a"])
        rec = self.r._store().get(request_id="req_race")
        self.assertIsNotNone(rec)
        self.assertEqual(rec["task_id"], "t0001")
        events = self.tasks / "t0001" / "events.jsonl"
        self.assertTrue(events.exists())


class SettlementGuardTests(RunnerBase):
    def _new_task(self, tid="t0001", req="req_settle"):
        code, out = self.submit(self.make_req(request_id=req, task_id=tid))
        self.assertEqual(code, 0)
        return tid

    def _running(self, tid, wp=111, cp=222):
        d = self.tasks / tid
        (d / "state.json").write_text(json.dumps(
            {"status": "running", "worker_pid": wp, "child_pid": cp}),
            encoding="utf-8")
        self.r._store().update(tid, phase="claimed", worker_pid=wp, child_pid=cp)

    def test_observer_never_rewrites_live_worker_state(self):
        tid = self._new_task()
        self._running(tid)
        self.r._store().set_cancel(tid)
        self.alive = {"111"}
        code, out = self.do_reconcile(task_id=tid)
        self.assertEqual(out["status"], "cancel_requested")
        # derived status only; the live worker's on-disk state is untouched
        self.assertEqual(self.state_of(tid)["status"], "running")

    def test_cancel_does_not_rewrite_live_worker_state(self):
        tid = self._new_task()
        self._running(tid)
        self.alive = {"111"}
        d = self.tasks / tid
        before = (d / "state.json").read_text(encoding="utf-8")
        code, out = self.do_cancel(tid)
        self.assertEqual(code, 0)
        self.assertTrue(out["accepted"])
        self.assertEqual((d / "state.json").read_text(encoding="utf-8"), before)

    def test_terminal_full_result_immutable(self):
        tid = self._new_task()
        d = self.tasks / tid
        (d / "state.json").write_text(json.dumps(
            {"status": "succeeded", "final": "RESULT",
             "worker_pid": 111}), encoding="utf-8")
        self.r._store().update(tid, phase="terminal", status="succeeded")
        self.r._store().set_cancel(tid)
        self.alive = set()
        code, out = self.do_reconcile(task_id=tid)
        self.assertEqual(out["status"], "succeeded")
        self.assertEqual(out["final"], "RESULT")
        self.assertEqual(self.state_of(tid)["final"], "RESULT")

    def test_observer_losing_to_worker_finalize_does_not_clobber(self):
        tid = self._new_task()
        d = self.tasks / tid
        # observer snapshot taken while the worker was still alive
        (d / "state.json").write_text(json.dumps(
            {"status": "running", "worker_pid": 111, "child_pid": 222}),
            encoding="utf-8")
        self.r._store().set_cancel(tid)
        self.alive = {"111"}
        # ...the worker then publishes its terminal result before the observer
        # resumes; the guarded finalize must lose and preserve the result.
        self.r._finalize(d, tid, "succeeded", self.r._store(),
                         exit_code=0, final="FINAL")
        self.r._store().update(tid, phase="terminal", status="succeeded")
        self.alive = set()
        code, out = self.do_reconcile(task_id=tid)
        self.assertEqual(out["status"], "succeeded")
        self.assertEqual(out["final"], "FINAL")
        self.assertEqual(self.state_of(tid)["final"], "FINAL")

    def test_claim_publishes_worker_identity_atomically(self):
        tid = self._new_task()
        # begin_init already published an owner pid before any task file
        rec = self.r._store().get(task_id=tid)
        self.assertIsNotNone(rec["worker_pid"])
        self.assertTrue(self.r._store().claim(tid, "pid:x", "tok",
                                              worker_pid=4321))
        rec = self.r._store().get(task_id=tid)
        self.assertEqual(rec["launch_phase"], "claimed")
        self.assertEqual(rec["worker_pid"], 4321)

    def test_stale_observer_cancel_loses_to_later_claim(self):
        # Deterministic B3 barrier: the observer snapshots a scheduling task
        # whose recorded initializer is dead; before the observer can commit
        # terminal, a real worker claims and publishes a NEW identity. The
        # stale observer must lose and never mark the task terminal.
        tid = "t0101"
        code, out = self.submit(self.make_req(request_id="req_barrier",
                                              task_id=tid))
        self.assertEqual(code, 0)
        store = self.r._store()
        d = self.tasks / tid
        store.update(tid, worker_pid=111, claim_token="tok1")
        (d / "state.json").write_text(json.dumps(
            {"status": "running", "worker_pid": 111, "child_pid": 222}),
            encoding="utf-8")
        store.set_cancel(tid)
        stale = store.get(task_id=tid)
        self.assertEqual(stale["launch_phase"], "scheduling")
        self.assertEqual(stale["claim_token"], "tok1")
        self.assertEqual(stale["worker_pid"], 111)
        self.alive = set()  # the observed initializer is provably dead

        real_finalize = store.finalize_cancel

        def barrier(task_id, **kw):
            self.assertTrue(store.claim(tid, "pid:worker", "tok2",
                                        worker_pid=222))
            return real_finalize(task_id, **kw)

        store.finalize_cancel = barrier
        st = self.r._settle_cancel(d, self.state_of(tid), store, tid,
                                   rec=stale)
        store.finalize_cancel = real_finalize
        # stale settlement lost: pending, and the new owner is untouched
        self.assertEqual(st["status"], "cancel_requested")
        rec = store.get(task_id=tid)
        self.assertEqual(rec["launch_phase"], "claimed")
        self.assertEqual(rec["claim_token"], "tok2")
        self.assertEqual(rec["worker_pid"], 222)
        self.assertEqual(self.state_of(tid)["status"], "running")

        # owning worker alive -> reconcile stays pending, never terminal
        self.alive = {"222"}
        code, out = self.do_reconcile(task_id=tid)
        self.assertEqual(out["status"], "cancel_requested")
        self.assertEqual(self.state_of(tid)["status"], "running")

        # ...the owning worker exits and publishes its own terminal result
        self.alive = set()
        self.r._finalize(d, tid, "succeeded", store, exit_code=0,
                         final="OWNER")
        code, out = self.do_reconcile(task_id=tid)
        self.assertEqual(out["status"], "succeeded")
        self.assertEqual(out["final"], "OWNER")
        self.assertEqual(store.get(task_id=tid)["launch_phase"], "terminal")

    def test_stale_observer_unknown_loses_to_changed_claim(self):
        # The analogous settle_unknown guard must also bind to the observed
        # claim identity, not just the phase, so a stale observer cannot move
        # a re-published claim to `unknown`.
        tid = "t0102"
        code, out = self.submit(self.make_req(request_id="req_unknown_guard",
                                              task_id=tid))
        self.assertEqual(code, 0)
        store = self.r._store()
        d = self.tasks / tid
        store.update(tid, phase="claimed", worker_pid=111, child_pid=222,
                     claim_token="tok1")
        (d / "state.json").write_text(json.dumps(
            {"status": "running", "worker_pid": 111, "child_pid": 222}),
            encoding="utf-8")
        rec = store.get(task_id=tid)
        self.assertEqual(rec["launch_phase"], "claimed")
        self.alive = set()

        real_unknown = store.finalize_unknown

        def barrier(task_id, **kw):
            store.update(tid, claim_token="tok2", worker_pid=222)
            return real_unknown(task_id, **kw)

        store.finalize_unknown = barrier
        st = self.r._settle_running(d, self.state_of(tid), store, tid,
                                    rec=rec)
        store.finalize_unknown = real_unknown
        self.assertEqual(st["status"], "running")
        self.assertEqual(store.get(task_id=tid)["launch_phase"], "claimed")
        self.assertEqual(self.state_of(tid)["status"], "running")


class PidLivenessTests(RunnerBase):
    def test_tasklist_nonzero_exit_is_unknown(self):
        self.assertIsNone(self.r._classify_tasklist("", 1, "42"))
        self.assertIsNone(self.r._classify_tasklist(
            '"dsh.exe","42","Console","1","x"', 1, "42"))

    def test_tasklist_parse_error_is_unknown(self):
        self.assertIsNone(self.r._classify_tasklist("garbage", 0, "42"))
        self.assertIsNone(self.r._classify_tasklist('"only-one-col"', 0, "42"))

    def test_tasklist_present_and_absent(self):
        self.assertFalse(self.r._classify_tasklist("", 0, "42"))
        self.assertTrue(self.r._classify_tasklist(
            '"dsh.exe","42","Console","1","10 K"', 0, "42"))
        self.assertFalse(self.r._classify_tasklist(
            '"dsh.exe","43","Console","1","10 K"', 0, "42"))

    def test_default_pid_alive_dead_and_self(self):
        self.assertIs(self.r._default_pid_alive(999999), False)
        self.assertIs(self.r._default_pid_alive(os.getpid()), True)

    def test_settle_cancel_unknown_liveness_is_not_terminal(self):
        tid = "t0001"
        code, out = self.submit(self.make_req(request_id="req_unk",
                                              task_id=tid))
        self.assertEqual(code, 0)
        d = self.tasks / tid
        (d / "state.json").write_text(json.dumps(
            {"status": "running", "worker_pid": 111, "child_pid": 222}),
            encoding="utf-8")
        self.r._store().set_cancel(tid)
        self.r.PID_ALIVE = lambda pid: None
        code, out = self.do_reconcile(task_id=tid)
        self.assertEqual(out["status"], "cancel_requested")
        self.assertEqual(self.state_of(tid)["status"], "running")


class StdinTransferTests(RunnerBase):
    def test_timeout_unblocks_blocking_stdin(self):
        tid = "t0001"
        code, out = self.submit(self.make_req(request_id="req_bs1", task_id=tid,
                                              timeout_s=1))
        self.assertEqual(code, 0)
        proc = FakeProc(lines=[], pid=9101, block=True, block_stdin=True)
        self.install_proc(proc)
        self.run_worker(tid)
        self.assertTrue(proc.stdin.entered.wait(2))
        self.assertEqual(self.state_of(tid)["status"], "timed_out")
        self.assertIn(9101, self.killed_pids)

    def test_cancel_unblocks_blocking_stdin(self):
        tid = "t0001"
        code, out = self.submit(self.make_req(request_id="req_bs2", task_id=tid))
        self.assertEqual(code, 0)
        proc = FakeProc(lines=[], pid=9102, block=True, block_stdin=True)
        self.install_proc(proc)
        calls = {"n": 0}

        def fake_cancel(*a, **k):
            calls["n"] += 1
            return calls["n"] >= 3

        self.r.cancel_pending = fake_cancel
        self.run_worker(tid)
        self.assertEqual(self.state_of(tid)["status"], "cancelled")
        self.assertIn(9102, self.killed_pids)


class CodexCumulativeTests(RunnerBase):
    def test_cumulative_updated_then_completed_final_once(self):
        tid = "t0001"
        code, out = self.submit(self.make_req(request_id="req_cum", task_id=tid,
                                              agent="codex"))
        self.assertEqual(code, 0)

        def upd(text):
            return json.dumps({"type": "item.updated",
                               "item": {"id": "i1", "type": "agent_message",
                                        "text": text}}) + "\n"

        lines = [upd("Hel"), upd("Hello"),
                 json.dumps({"type": "item.completed",
                             "item": {"id": "i1", "type": "agent_message",
                                      "text": "Hello world"}}) + "\n"]
        proc = FakeProc(lines=lines, pid=2001)
        self.install_proc(proc)
        self.run_worker(tid)
        self.assertEqual(self.state_of(tid)["final"], "Hello world")


if __name__ == "__main__":
    unittest.main()
