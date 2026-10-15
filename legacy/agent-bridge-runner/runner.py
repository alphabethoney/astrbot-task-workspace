#!/usr/bin/env python3
"""agent-bridge-runner — 在 Windows 上接收任务、驱动 CLI agent、把事件落盘。

设计要点：
1. 任务不挂在 SSH 会话下：Windows 的 sshd 在会话结束时会终结整棵子进程树，
   所以这里为每个任务写一个 run.cmd，交给计划任务启动，进程便独立存活。
2. 所有事件按 seq 追加到 tasks/<id>/events.jsonl，watch 只按 seq 增量取，可重入。
3. 任务文本通过标准输入以 UTF-8 JSON 传入，绝不拼进命令行。
4. 跨进程去重与工作线程认领放在 SQLite 事务里：request_id 唯一、payload 指纹
   归一化、launch phase 持久化。赢家初始化任务文件并调度，输家只返回既有状态。
5. 取消意图持久化在同一 DB 中，由 worker 自己终止子进程并落终态；cancel 命令
   绝不直接 kill 可能被复用的 PID。
"""
import argparse
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TASKS = ROOT / "tasks"
INDEX_DIR = ROOT / "index"
DB_FILE = INDEX_DIR / "runner.db"

sys.path.insert(0, str(ROOT))
from adapters import codex as ad_codex  # noqa: E402
from adapters import dsh as ad_dsh  # noqa: E402

ADAPTERS = {"dsh": ad_dsh, "codex": ad_codex}
NEW_GROUP = 0x00000200
RUNNER_VERSION = 3
TERMINAL = ("succeeded", "failed", "timed_out", "cancelled")
PHASES = ("reserved", "prepared", "scheduling", "claimed", "terminal", "unknown")
CANCEL_POLL_S = 0.1
SCHEDULE_ACK_TIMEOUT_S = 3.0
DEFAULT_MAX_TIMEOUT_S = 86400

CAPABILITIES = {
    "version": RUNNER_VERSION,
    "protocol": ["submit", "watch", "cancel", "reconcile", "prepare",
                 "result", "list", "capabilities"],
    "dedupe": "sqlite-atomic",
    "launch_phases": list(PHASES),
    "full_result": True,
    "stdin_transport": True,
    "cancel": "durable-worker-owned",
    "scheduler_ack": "definition-kept-until-claim",
    "dsh_permission": "verified-patch-required",
}

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def now():
    return time.time()


def policy():
    return json.loads((ROOT / "policy.json").read_text(encoding="utf-8"))


def read_json(p, default=None):
    try:
        return json.loads(Path(p).read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json(p, obj):
    p = Path(p)
    p.parent.mkdir(parents=True, exist_ok=True)
    # unique tmp name: concurrent writers must never share a temp path
    tmp = p.parent / f".{p.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    try:
        tmp.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, p)
    except Exception:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise


def say(obj):
    sys.stdout.write(json.dumps(obj, ensure_ascii=False) + "\n")
    sys.stdout.flush()


# ---------------------------------------------------------------- id validation
_TID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_WS_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_WIN_RESERVED = {"CON", "PRN", "AUX", "NUL"}
_WIN_RESERVED |= {f"COM{i}" for i in range(1, 10)}
_WIN_RESERVED |= {f"LPT{i}" for i in range(1, 10)}


def _reserved_name(name):
    base = str(name).split(".")[0].upper()
    return base in _WIN_RESERVED


def validate_tid(tid):
    tid = str(tid)
    if not _TID_RE.fullmatch(tid):
        raise ValueError(f"非法的 task_id：{tid!r}")
    if tid.endswith((".", " ")):
        raise ValueError(f"非法的 task_id（尾随点/空格）：{tid!r}")
    if _reserved_name(tid):
        raise ValueError(f"非法的 task_id（Windows 保留名）：{tid!r}")
    return tid


def td(tid):
    tid = validate_tid(tid)
    root = TASKS.resolve()
    d = TASKS / tid
    real = d.resolve()
    if os.path.normcase(str(real)) != os.path.normcase(str(root / tid)):
        raise ValueError(f"task 目录越界：{tid!r}")
    return d


def _existing_alias(tid):
    """Return an existing task directory matching tid, case-insensitively."""
    if not TASKS.exists():
        return ""
    low = str(tid).lower()
    for child in TASKS.iterdir():
        if child.is_dir() and child.name.lower() == low:
            return child.name
    return ""


# ---------------------------------------------------------------- sqlite ledger
class StoreError(Exception):
    pass


_COLUMNS = (
    "request_id", "task_id", "fingerprint", "agent", "cwd", "prompt",
    "permission", "timeout_s", "session_id", "launch_phase", "status",
    "worker_pid", "child_pid", "claimed_by", "claim_token",
    "cancel_requested", "created", "updated", "last_error", "result_path",
)

# Distinguishes "no identity expectation" from an observed SQL NULL when
# guarding a phase transition by launch identity.
_NULL = object()


class Store:
    """Cross-process request ledger backed by sqlite3 (stdlib only)."""

    def __init__(self, path=None):
        self.path = Path(path or DB_FILE)
        self._ready = False

    def _connect(self):
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(self.path), timeout=30, isolation_level=None)
        except (sqlite3.Error, OSError) as e:
            raise StoreError(str(e))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.Error:
            pass
        conn.execute(
            """CREATE TABLE IF NOT EXISTS requests(
                request_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL COLLATE NOCASE,
                fingerprint TEXT NOT NULL DEFAULT '',
                agent TEXT, cwd TEXT, prompt TEXT, permission TEXT,
                timeout_s INTEGER, session_id TEXT,
                launch_phase TEXT NOT NULL DEFAULT 'reserved',
                status TEXT,
                worker_pid INTEGER, child_pid INTEGER,
                claimed_by TEXT, claim_token TEXT,
                cancel_requested REAL,
                created REAL, updated REAL, last_error TEXT, result_path TEXT
            )"""
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uniq_task_id "
            "ON requests(task_id COLLATE NOCASE)"
        )
        return conn

    @staticmethod
    def _row(row):
        if row is None:
            return None
        return {k: row[k] for k in row.keys()}

    def get(self, request_id=None, task_id=None):
        conn = self._connect()
        try:
            if request_id:
                row = conn.execute(
                    "SELECT * FROM requests WHERE request_id=?", (request_id,)
                ).fetchone()
            elif task_id:
                row = conn.execute(
                    "SELECT * FROM requests WHERE task_id=? COLLATE NOCASE", (task_id,)
                ).fetchone()
            else:
                return None
            return self._row(row)
        except sqlite3.Error as e:
            raise StoreError(str(e))
        finally:
            conn.close()

    def reserve(self, request_id, task_id, fingerprint, meta=None):
        """Atomically reserve a request_id -> task_id mapping.

        Returns {"status": winner|existing|conflict|collision, ...}.
        """
        meta = meta or {}
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is not None:
                rec = self._row(row)
                if rec["fingerprint"] != fingerprint:
                    conn.execute("ROLLBACK")
                    return {"status": "conflict", "task_id": rec["task_id"],
                            "phase": rec["launch_phase"]}
                conn.execute("COMMIT")
                return {"status": "existing", "task_id": rec["task_id"],
                        "phase": rec["launch_phase"], "record": rec}
            dup = conn.execute(
                "SELECT request_id FROM requests WHERE task_id=? COLLATE NOCASE",
                (task_id,),
            ).fetchone()
            if dup is not None:
                conn.execute("ROLLBACK")
                return {"status": "collision", "task_id": task_id}
            ts = now()
            conn.execute(
                """INSERT INTO requests(
                    request_id, task_id, fingerprint, agent, cwd, prompt,
                    permission, timeout_s, session_id, launch_phase,
                    created, updated)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (request_id, task_id, fingerprint, meta.get("agent"),
                 meta.get("cwd"), meta.get("prompt"), meta.get("permission"),
                 meta.get("timeout_s"), meta.get("session_id"), "reserved",
                 ts, ts),
            )
            conn.execute("COMMIT")
            return {"status": "winner", "task_id": task_id, "phase": "reserved"}
        except sqlite3.Error as e:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise StoreError(str(e))
        finally:
            conn.close()

    def update(self, task_id, phase=None, expect=None, **fields):
        sets = ["updated=?"]
        params = [now()]
        if phase is not None:
            sets.append("launch_phase=?")
            params.append(phase)
        for k in ("status", "worker_pid", "child_pid", "claimed_by",
                  "claim_token", "cancel_requested", "last_error", "result_path"):
            if k in fields:
                sets.append(f"{k}=?")
                params.append(fields[k])
        where = "task_id=? COLLATE NOCASE"
        params.append(task_id)
        if expect is not None:
            expect = tuple(expect) if not isinstance(expect, str) else (expect,)
            where += " AND launch_phase IN (" + ",".join("?" * len(expect)) + ")"
            params.extend(expect)
        conn = self._connect()
        try:
            cur = conn.execute(
                f"UPDATE requests SET {','.join(sets)} WHERE {where}", params)
            return cur.rowcount
        except sqlite3.Error as e:
            raise StoreError(str(e))
        finally:
            conn.close()

    def claim_scheduling(self, task_id, expect=("reserved", "prepared")):
        return self.update(task_id, phase="scheduling", expect=expect) == 1

    def begin_init(self, task_id, owner, token, worker_pid=None,
                   expect=("reserved", "prepared")):
        """Atomically become the single initializer for a task.

        The phase transition, owner identity and pid are written in one
        transaction BEFORE any task file is created, so two concurrent
        submitters can never both initialise/schedule the same task.
        """
        expect = tuple(expect)
        placeholders = ",".join("?" * len(expect))
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                f"""UPDATE requests SET launch_phase='scheduling', claimed_by=?,
                    claim_token=?, worker_pid=?, updated=?
                    WHERE task_id=? COLLATE NOCASE
                      AND launch_phase IN ({placeholders})""",
                (owner, token, worker_pid, now(), task_id, *expect),
            )
            conn.execute("COMMIT")
            return cur.rowcount == 1
        except sqlite3.Error as e:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise StoreError(str(e))
        finally:
            conn.close()

    def claim(self, task_id, owner, token, request_id="", worker_pid=None):
        """Atomically claim a task for execution. Returns True if this worker won.

        The worker identity (owner + pid + token) is published in the same
        transaction as the phase transition so a cancellation observer can
        never conclude "no worker" before the worker is visible, and can never
        see a worker without its pid. Uncertain launches (phase 'unknown') and
        already claimed/terminal tasks are never re-run.
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM requests WHERE task_id=? COLLATE NOCASE", (task_id,)
            ).fetchone()
            if row is None:
                if not (TASKS / task_id / "request.json").exists():
                    conn.execute("ROLLBACK")
                    return False
                req = read_json(TASKS / task_id / "request.json", {}) or {}
                ts = now()
                conn.execute(
                    """INSERT INTO requests(
                        request_id, task_id, fingerprint, agent, cwd, prompt,
                        permission, timeout_s, session_id, launch_phase,
                        claimed_by, claim_token, worker_pid, created, updated)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (req.get("request_id") or request_id or ("legacy:" + task_id),
                     task_id, req.get("fingerprint", ""), req.get("agent"),
                     req.get("cwd"), req.get("prompt"), req.get("permission"),
                     req.get("timeout_s"), req.get("session_id"), "claimed",
                     owner, token, worker_pid, ts, ts),
                )
                conn.execute("COMMIT")
                return True
            rec = self._row(row)
            if rec["launch_phase"] in ("claimed", "terminal", "unknown"):
                conn.execute("ROLLBACK")
                return False
            cur = conn.execute(
                """UPDATE requests SET launch_phase='claimed', claimed_by=?,
                   claim_token=?, worker_pid=?, updated=?
                   WHERE task_id=? COLLATE NOCASE
                     AND launch_phase IN ('reserved','prepared','scheduling')""",
                (owner, token, worker_pid, now(), task_id),
            )
            conn.execute("COMMIT")
            return cur.rowcount == 1
        except sqlite3.Error as e:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise StoreError(str(e))
        finally:
            conn.close()

    def recover_scheduling(self, task_id, owner, token, worker_pid=None):
        """Re-own a 'scheduling' task only if its init owner is provably gone.

        Recovery is never age-based. A live owner (or unverifiable liveness)
        keeps the task untouched, so an in-flight initializer/scheduler is
        never raced or overwritten.
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM requests WHERE task_id=? COLLATE NOCASE", (task_id,)
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                return False
            rec = self._row(row)
            if rec["launch_phase"] != "scheduling":
                conn.execute("ROLLBACK")
                return False
            if rec.get("cancel_requested"):
                conn.execute("ROLLBACK")
                return False
            old_pid = rec.get("worker_pid")
            if not old_pid:
                # owner unknown (legacy row): never steal, remain unknown
                conn.execute("ROLLBACK")
                return False
            if str(old_pid) == str(os.getpid()):
                # we are the recorded init owner: definitely still alive
                conn.execute("ROLLBACK")
                return False
            live = PID_ALIVE(old_pid)
            if live is not False:
                conn.execute("ROLLBACK")
                return False
            cur = conn.execute(
                """UPDATE requests SET claimed_by=?, claim_token=?, worker_pid=?,
                   updated=? WHERE task_id=? COLLATE NOCASE
                     AND launch_phase='scheduling'""",
                (owner, token, worker_pid, now(), task_id),
            )
            conn.execute("COMMIT")
            return cur.rowcount == 1
        except sqlite3.Error as e:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise StoreError(str(e))
        finally:
            conn.close()

    def _cas_transition(self, task_id, new_phase, new_status, allowed,
                        expect_phase=None, expect_token=_NULL,
                        expect_worker_pid=_NULL, require_cancel=False):
        """One guarded phase transition bound to an observed launch identity.

        The UPDATE runs in a single ``BEGIN IMMEDIATE`` transaction that
        requires all of: a currently cancellable phase, optionally a durable
        cancel intent, AND the exact identity (phase + claim token + worker
        pid, compared NULL-safely) the caller based its decision on. A
        concurrent claim/worker changes that identity, so a stale observer
        can never overwrite the newer owner. Returns True only for the winner.
        """
        where = [
            "task_id=? COLLATE NOCASE",
            "launch_phase IN (" + ",".join("?" * len(allowed)) + ")",
        ]
        params = [new_phase, new_status, now(), task_id, *allowed]
        if require_cancel:
            where.append("cancel_requested IS NOT NULL")
        if expect_phase is not None:
            where.append("launch_phase=?")
            params.append(expect_phase)
        if expect_token is not _NULL:
            where.append("claim_token IS ?")
            params.append(expect_token)
        if expect_worker_pid is not _NULL:
            where.append("worker_pid IS ?")
            params.append(expect_worker_pid)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                f"""UPDATE requests SET launch_phase=?, status=?, updated=?
                    WHERE {' AND '.join(where)}""",
                params,
            )
            winner = cur.rowcount == 1
            conn.execute("COMMIT")
            return winner
        except sqlite3.Error as e:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise StoreError(str(e))
        finally:
            conn.close()

    def finalize_cancel(self, task_id, expect_phase=None, expect_token=_NULL,
                        expect_worker_pid=_NULL):
        """Guarded worker->terminal transition for a cancelled task.

        Only succeeds when the ledger still shows a cancellable (non-terminal)
        phase, a durable cancel intent exists, AND the exact launch identity
        the observer based its "worker is dead" decision on is still current.
        A worker that claimed after the observer's snapshot changes that
        identity, so the stale observer loses and must never mark the task
        terminal. Returns True for the single winner, so state.json is only
        ever rewritten from a snapshot that was still true at commit time.
        """
        return self._cas_transition(
            task_id, "terminal", "cancelled",
            ("reserved", "prepared", "scheduling", "claimed"),
            expect_phase=expect_phase, expect_token=expect_token,
            expect_worker_pid=expect_worker_pid, require_cancel=True)

    def finalize_unknown(self, task_id, expect_phase=None, expect_token=_NULL,
                         expect_worker_pid=_NULL):
        """Guarded claimed->unknown transition bound to the observed claim.

        Same identity guard as :meth:`finalize_cancel`: a stale observer may
        only move the row to ``unknown`` while the worker it judged dead is
        still the recorded owner.
        """
        return self._cas_transition(
            task_id, "unknown", "unknown", ("claimed",),
            expect_phase=expect_phase, expect_token=expect_token,
            expect_worker_pid=expect_worker_pid)

    def set_cancel(self, task_id):
        return self.update(task_id, cancel_requested=now())

    def cancel_requested(self, task_id):
        rec = self.get(task_id=task_id)
        return bool(rec and rec.get("cancel_requested"))

    def legacy_find(self, request_id):
        if not TASKS.exists():
            return ""
        for d in TASKS.iterdir():
            if not d.is_dir():
                continue
            req = read_json(d / "request.json", {}) or {}
            if req.get("request_id") == request_id:
                return d.name
        return ""


def _store():
    return Store(DB_FILE)


def _normalized_payload(req, agent, cwd, permission, timeout_s):
    return {
        "agent": agent,
        "cwd": cwd,
        "prompt": req.get("prompt") or "",
        "permission": permission,
        "timeout_s": timeout_s,
        "session_id": req.get("session_id") or "",
    }


def fingerprint(payload):
    data = {k: payload.get(k) for k in
            ("agent", "cwd", "prompt", "permission", "timeout_s", "session_id")}
    return hashlib.sha256(
        json.dumps(data, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


# compatibility shims for the previous JSON index API
def index_lookup(request_id):
    try:
        rec = _store().get(request_id=request_id)
    except StoreError:
        return None
    if not rec:
        return None
    return {"task_id": rec["task_id"], "fingerprint": rec["fingerprint"],
            "state": rec["launch_phase"]}


def index_reserve(request_id, task_id, fp):
    try:
        res = _store().reserve(request_id, task_id, fp, {})
    except StoreError:
        return {"conflict": True, "task_id": task_id}
    if res["status"] == "conflict":
        return {"conflict": True, "task_id": res["task_id"]}
    if res["status"] == "collision":
        return {"conflict": True, "task_id": task_id}
    if res["status"] == "existing":
        return {"task_id": res["task_id"], "fingerprint": fp}
    return {"task_id": task_id, "fingerprint": fp, "reserved": True}


# ---------------------------------------------------------------- policy / permissions
def allowed_permissions(pol):
    raw = pol.get("allowed_permissions")
    order = ("read-only", "workspace-write", "danger-full-access")
    if raw is None:
        return ["read-only", "workspace-write"]
    out = [m for m in order if m in raw]
    return out or ["read-only", "workspace-write"]


def permission_ceiling(pol):
    order = ("read-only", "workspace-write", "danger-full-access")
    allowed = allowed_permissions(pol)
    ceiling = pol.get("permission_ceiling")
    if ceiling in order:
        limit = order.index(ceiling)
        return [m for m in allowed if order.index(m) <= limit]
    return allowed


def resolve_permission(pol, requested, spec):
    order = ("read-only", "workspace-write", "danger-full-access")
    mode = requested if requested not in (None, "") else (
        spec.get("default_permission") or "read-only")
    if mode not in order:
        raise ValueError(f"非法 permission：{mode!r}")
    if mode not in permission_ceiling(pol):
        raise ValueError(f"permission 不被允许：{mode}")
    return mode


def validate_timeout(req, pol):
    raw = req.get("timeout_s")
    if raw in (None, ""):
        raw = pol.get("default_timeout_s", 600)
    if isinstance(raw, bool):
        raise ValueError("timeout_s 必须是正整数")
    try:
        t = int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"非法 timeout_s：{raw!r}")
    maximum = int(pol.get("max_timeout_s", DEFAULT_MAX_TIMEOUT_S))
    if t <= 0 or t > maximum:
        raise ValueError(f"timeout_s 超出范围（1-{maximum}）：{t}")
    return t


_CODEX_TAKE_VALUE = ("-s", "--sandbox", "-a", "--ask-for-approval", "-c", "--config")
_CODEX_BYPASS = ("--full-auto", "--dangerously-bypass-approvals-and-sandbox",
                 "--yolo", "--dangerously-bypass-approvals")
_CODEX_DROP_PREFIX = ("--sandbox=", "--config=", "--ask-for-approval=")


def strip_conflicting_flags(cmd):
    out = []
    i = 0
    while i < len(cmd):
        a = str(cmd[i])
        if a in _CODEX_TAKE_VALUE:
            i += 2
            continue
        if any(a.startswith(p) for p in _CODEX_DROP_PREFIX):
            i += 1
            continue
        if a in _CODEX_BYPASS:
            i += 1
            continue
        out.append(cmd[i])
        i += 1
    return out


_DSH_SESSION_FLAGS = ("--session-id", "--session", "--resume", "--continue")


def _normalize_dsh_args(args):
    """Return (profile, extras) for a DSH invocation, rejecting bypass flags.

    The runner owns ``--profile``/``--patch``/``--json``/stdin so an operator
    spec can never smuggle in a conflicting profile/patch or a session-resume
    that would carry persisted (higher-priority) sandbox/approval settings.
    """
    profile = None
    extras = []
    i = 0
    while i < len(args):
        a = str(args[i])
        if a == "--profile":
            if i + 1 >= len(args):
                raise ValueError("DSH spec --profile 缺少值")
            profile = str(args[i + 1])
            i += 2
            continue
        if a.startswith("--profile="):
            profile = a.split("=", 1)[1]
            i += 1
            continue
        if a in ("--json", "-"):
            i += 1
            continue
        if a == "--patch" or a.startswith("--patch="):
            raise ValueError("DSH spec 不得自带 --patch（由 runner 逐次生成）")
        if any(a == f or a.startswith(f + "=") for f in _DSH_SESSION_FLAGS):
            raise ValueError(f"DSH spec 不得自带会话/恢复参数：{a}")
        extras.append(args[i])
        i += 1
    return (profile or "headless"), extras


def build_agent_command(spec, kind, req, permission, extra_args=None):
    exe = os.path.expandvars(str(spec["exe"]))
    if kind == "dsh":
        profile, extras = _normalize_dsh_args(list(spec.get("args", [])))
        patch_args = list(extra_args or [])
        # Canonical installed overlay order: --profile <p> --patch <file> ... --json -
        return [exe, "--profile", profile, *patch_args, *extras, "--json", "-"]
    args = strip_conflicting_flags(list(spec.get("args", [])))
    additions = []
    if kind == "codex":
        additions += ["--sandbox", permission]
    if extra_args:
        additions += list(extra_args)
    stdin_idx = None
    for i, a in enumerate(args):
        if a == "-":
            stdin_idx = i
    if stdin_idx is None:
        args = args + additions
    else:
        # keep the trailing stdin marker last so flags are never read as the task
        args = args[:stdin_idx] + additions + args[stdin_idx:]
    return [exe] + args


def _validate_dsh_patch(body, mode, cwd):
    """Enforce the verified three-entry DSH patch contract."""
    if not isinstance(body, list):
        raise ValueError("DSH 权限补丁必须是条目列表")
    entries = {}
    for e in body:
        if isinstance(e, dict) and e.get("id"):
            entries[str(e["id"])] = e.get("config") or {}
    for req_id in ("sandbox-policy", "approval", "permission"):
        if req_id not in entries:
            raise ValueError(f"DSH 权限补丁缺少必需条目：{req_id}")
    sp = entries["sandbox-policy"]
    if sp.get("mode") != mode:
        raise ValueError(f"DSH 补丁 sandbox mode 与请求不一致：{sp.get('mode')!r}")
    wr = sp.get("workspaceRoot")
    if wr is not None:
        if (os.path.normcase(str(_resolved(wr)))
                != os.path.normcase(str(_resolved(cwd)))):
            raise ValueError("DSH 补丁 workspaceRoot 与 cwd 不一致")
    if entries["approval"].get("policy") != "never":
        raise ValueError("DSH 补丁 approval policy 必须为 never")
    perm = entries["permission"]
    preset_name = perm.get("defaultPreset")
    preset = (perm.get("presets") or {}).get(preset_name)
    if not isinstance(preset, dict):
        raise ValueError("DSH 补丁缺少匹配的 permission preset")
    if preset.get("sandbox") != mode:
        raise ValueError("DSH 权限 preset sandbox 与请求不一致")
    if preset.get("approval") != "never":
        raise ValueError("DSH 权限 preset approval 必须为 never")


def build_dsh_patch_body(spec, mode, cwd):
    """Return (patch_meta, substituted_body) or raise if unverifiable.

    A missing/unverified patch (including env-only or the old read-only
    fallback) is REJECTED for every mode: the installed default is
    workspace-write/ask, so env alone cannot promise approval=never.
    """
    patch = spec.get("permission_patch")
    if not isinstance(patch, dict) or not patch.get("verified"):
        raise ValueError("DSH 权限不可验证：缺少受验证的 --patch 覆盖，拒绝该模式")
    patches = patch.get("patches") or {}
    body = patches.get(mode)
    if body is None:
        raise ValueError(f"DSH 权限补丁未覆盖模式：{mode}")
    body = _subst(body, mode, cwd)
    _validate_dsh_patch(body, mode, cwd)
    return patch, body


def validate_launch_spec(spec, kind, mode, cwd):
    """Reject a spec that cannot be safely launched, before scheduling."""
    args = list(spec.get("args", []))
    if kind == "dsh":
        _normalize_dsh_args(args)
        build_dsh_patch_body(spec, mode, cwd)
        return
    for a in args:
        s = str(a)
        if s in ("resume", "--session-id", "--session", "--resume", "--continue"):
            raise ValueError(f"spec 中不允许的会话/恢复参数：{s}")
        if s.startswith(("--session-id=", "--session=", "--resume=", "--continue=")):
            raise ValueError(f"spec 中不允许的会话/恢复参数：{s}")


def _yaml_scalar(v):
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _yaml_lines(obj, indent=0):
    pad = "  " * indent
    out = []
    if isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                items = list(item.items())
                first = True
                for k, v in items:
                    lead = f"{pad}- {k}:" if first else f"{pad}  {k}:"
                    first = False
                    if isinstance(v, (dict, list)):
                        out.append(lead)
                        out.extend(_yaml_lines(v, indent + 2))
                    else:
                        out.append(f"{lead} {_yaml_scalar(v)}")
            else:
                out.append(f"{pad}- {_yaml_scalar(item)}")
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                out.append(f"{pad}{k}:")
                out.extend(_yaml_lines(v, indent + 1))
            else:
                out.append(f"{pad}{k}: {_yaml_scalar(v)}")
        return out
    return [f"{pad}{_yaml_scalar(obj)}"]


def _subst(value, mode, cwd):
    if isinstance(value, str):
        return value.replace("{workspace}", str(cwd)).replace("{mode}", mode)
    if isinstance(value, dict):
        return {k: _subst(v, mode, cwd) for k, v in value.items()}
    if isinstance(value, list):
        return [_subst(v, mode, cwd) for v in value]
    return value


def prepare_dsh_permission_args(spec, mode, cwd, d):
    """Build the verified per-execution DSH ``--patch`` overlay args.

    Every mode (including read-only) must have a verified three-entry patch;
    there is no env-only or read-only fallback. See DSH_CONTRACT.md.
    """
    patch, body = build_dsh_patch_body(spec, mode, cwd)
    d.mkdir(parents=True, exist_ok=True)
    p = d / "permission-patch.yaml"
    p.write_text("\n".join(_yaml_lines(body)) + "\n", encoding="utf-8")
    return [str(patch.get("arg") or "--patch"), str(p)]


def build_agent_env(spec, kind, permission):
    """Supplementary env only; the verified --patch overlay is authoritative."""
    env = dict(os.environ)
    if kind != "dsh":
        return env
    pe = spec.get("permission_env")
    if not isinstance(pe, dict) or not pe.get("verified"):
        return env
    if permission not in (pe.get("supported") or []):
        return env
    env[str(pe.get("env") or "DSH_PERMISSION_MODE")] = permission
    return env


# ---------------------------------------------------------------- scheduler / process
class SubprocessScheduler:
    """Real scheduler: Windows Task Scheduler via schtasks."""

    def create(self, name, runcmd):
        return subprocess.run(
            ["schtasks", "/create", "/f", "/tn", name, "/tr", runcmd,
             "/sc", "once", "/st", "00:00"], capture_output=True).returncode

    def run(self, name):
        return subprocess.run(["schtasks", "/run", "/tn", name],
                              capture_output=True).returncode

    def delete(self, name):
        return subprocess.run(["schtasks", "/delete", "/f", "/tn", name],
                              capture_output=True).returncode


SCHEDULER = SubprocessScheduler()
POPEN = subprocess.Popen


def _spawn(cmd, cwd, env, errf):
    kwargs = dict(cwd=cwd, stdout=subprocess.PIPE, stderr=errf,
                  stdin=subprocess.PIPE, text=True, encoding="utf-8",
                  errors="replace", bufsize=1, env=env)
    if os.name == "nt":
        kwargs["creationflags"] = NEW_GROUP
    else:
        kwargs["start_new_session"] = True
    return POPEN(cmd, **kwargs)


def _default_kill_tree(pid):
    if not pid:
        return
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)],
                       capture_output=True)
        return
    try:
        os.killpg(os.getpgid(pid), 9)
    except Exception:
        try:
            os.kill(pid, 9)
        except Exception:
            pass


def _classify_tasklist(stdout, returncode, pid):
    """Classify a Windows ``tasklist /FO CSV /NH`` result into tri-state.

    Nonzero exit, unparsable rows or missing columns => None (unknown), never
    False, so an inconclusive probe can never be mistaken for "worker dead".
    """
    if returncode != 0:
        return None
    pid = str(pid)
    found = False
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line or line.lower().startswith("info:"):
            continue
        cols = [c.strip().strip('"') for c in line.split('","')]
        if len(cols) < 2:
            return None
        if cols[1] == pid:
            found = True
    return found


def _default_pid_alive(pid):
    """Tri-state liveness: True alive, False definitely dead, None unknown.

    A tasklist/OS error, nonzero exit or unparsable output must never be
    reported as "dead" — the caller then treats the worker as unverifiable and
    keeps the task nonterminal instead of finalising from a stale snapshot.
    """
    if not pid:
        return False
    pid = str(pid)
    if os.name == "nt":
        try:
            out = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                capture_output=True, text=True)
        except Exception:
            return None
        return _classify_tasklist(getattr(out, "stdout", "") or "",
                                  getattr(out, "returncode", 0), pid)
    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return None


KILL_TREE = _default_kill_tree
PID_ALIVE = _default_pid_alive


# ---------------------------------------------------------------- cwd / workspace
def _resolved(p):
    try:
        return Path(p).resolve()
    except Exception:
        return Path(os.path.abspath(str(p)))


def _is_within(child, parent):
    c = os.path.normcase(str(_resolved(child)))
    p = os.path.normcase(str(_resolved(parent))).rstrip("\\/")
    if not p:
        return False
    return c == p or c.startswith(p + os.sep)


def _is_linklike(p):
    try:
        if os.path.islink(p):
            return True
    except OSError:
        pass
    try:
        return bool(getattr(os.path, "isjunction", lambda _x: False)(p))
    except OSError:
        return False


def _allowed_roots(pol):
    roots = []
    for r in pol.get("allowed_roots", []):
        if not r:
            continue
        roots.append(os.path.expandvars(str(r)))
    if not roots:
        raise ValueError("未配置 allowed_roots")
    return roots


def check_cwd(cwd, pol, must_exist=False, must_dir=False):
    cwd = os.path.expandvars(str(cwd or ""))
    if not cwd:
        raise ValueError("缺少 cwd")
    if not os.path.isabs(cwd):
        raise ValueError(f"cwd 必须是绝对路径：{cwd}")
    p = Path(cwd)
    if must_exist and not p.exists():
        raise ValueError(f"目录不存在：{p}")
    if must_dir and p.exists() and not p.is_dir():
        raise ValueError(f"cwd 不是目录：{p}")
    roots = _allowed_roots(pol)
    if not any(_is_within(p, r) for r in roots):
        raise ValueError(f"目录不在白名单内：{p}")
    real = _resolved(p)
    if not any(_is_within(real, r) for r in roots):
        raise ValueError(f"目录经符号链接/联接逃逸白名单：{p}")
    return str(real)


def prepare_cwd(workspace_id, root, pol):
    workspace_id = str(workspace_id or "")
    if not _WS_RE.fullmatch(workspace_id) or _reserved_name(workspace_id):
        raise ValueError(f"非法 workspace_id：{workspace_id!r}")
    if not root:
        raise ValueError("缺少 root")
    base = check_cwd(root, pol, must_exist=True, must_dir=True)
    target = Path(base) / workspace_id
    if not _is_within(target, base):
        raise ValueError("目标目录越出 root")
    if target.exists():
        if _is_linklike(target):
            raise ValueError("拒绝符号链接/联接目标")
        if not target.is_dir():
            raise ValueError(f"目标已存在且不是目录：{target}")
    else:
        target.mkdir(parents=True, exist_ok=False)
    if _is_linklike(target):
        raise ValueError("拒绝符号链接/联接目标")
    real = _resolved(target)
    if not _is_within(real, _resolved(base)):
        raise ValueError("目标目录逃出 root")
    if os.path.normcase(str(real)) != os.path.normcase(str(target)):
        # case-alias / junction resolving elsewhere is rejected
        raise ValueError("目标目录解析后与请求名不一致")
    return str(real)


def _ensure_task_files(d, tid, req, payload):
    d.mkdir(parents=True, exist_ok=True)
    rp = d / "request.json"
    if not rp.exists():
        body = dict(req)
        body.update({"task_id": tid, "fingerprint": fingerprint(payload)})
        write_json(rp, body)
    sp = d / "state.json"
    if not sp.exists():
        write_json(sp, {"status": "queued", "created": now()})
    ep = d / "events.jsonl"
    if not ep.exists():
        ep.write_text("", encoding="utf-8")
    rc = d / "run.cmd"
    if not rc.exists():
        lines = [
            "@echo off",
            "chcp 65001 >nul",
            "set PYTHONUTF8=1",
            f'cd /d "{ROOT}"',
            f'"{sys.executable}" "{ROOT / "runner.py"}" _run {tid} '
            f'>> "{d / "worker.log"}" 2>&1',
        ]
        rc.write_text("\r\n".join(lines) + "\r\n", encoding="utf-8")


def _schedule(store, d, tid, ack_timeout=None):
    """Create+run the scheduled task; keep its definition until the worker acks.

    ``schtasks /run`` only *queues* the launch, so deleting the definition
    immediately would make a delayed-but-real worker unobservable. Instead we
    wait (bounded) for the worker to claim (= durable acknowledgement). On
    timeout the definition is kept and the phase stays 'scheduling' so a later
    reconcile can still observe the claim; scheduling is never reset to
    unlaunched merely because the ack was slow.
    """
    tn = f"qqagent-{tid}"
    runcmd = str(d / "run.cmd")
    try:
        cr = SCHEDULER.create(tn, runcmd)
    except Exception as e:
        store.update(tid, phase="reserved", last_error=str(e))
        return {"ok": False, "error": f"计划任务创建异常：{e}"}
    if cr != 0:
        store.update(tid, phase="reserved", last_error=f"create rc={cr}")
        return {"ok": False, "error": f"计划任务创建失败：rc={cr}"}
    try:
        rr = SCHEDULER.run(tn)
    except Exception as e:
        try:
            SCHEDULER.delete(tn)
        except Exception:
            pass
        store.update(tid, phase="unknown", last_error=str(e))
        return {"ok": False, "error": f"计划任务启动异常：{e}", "unknown": True}
    if rr != 0:
        try:
            SCHEDULER.delete(tn)
        except Exception:
            pass
        store.update(tid, phase="reserved", last_error=f"run rc={rr}")
        return {"ok": False, "error": f"计划任务启动失败：rc={rr}"}

    if ack_timeout is None:
        ack_timeout = SCHEDULE_ACK_TIMEOUT_S
    deadline = now() + float(ack_timeout)
    acknowledged = False
    while now() < deadline:
        try:
            rec = store.get(task_id=tid)
        except StoreError:
            rec = None
        if rec and rec.get("launch_phase") in ("claimed", "terminal"):
            acknowledged = True
            break
        time.sleep(min(0.05, max(0.005, float(ack_timeout) / 20.0)))
    if acknowledged:
        try:
            SCHEDULER.delete(tn)
        except Exception:
            pass
        return {"ok": True, "acknowledged": True}
    # No durable acknowledgement within the bound: keep the task definition and
    # report 'scheduling' (not running), never reset to unlaunched.
    return {"ok": True, "acknowledged": False, "pending": True,
            "launch_phase": "scheduling"}


def _schedule_response(tid, sch):
    """Translate a scheduler result into an honest submit response."""
    if not sch.get("ok"):
        return {"ok": False, "task_id": tid, "error": sch.get("error"),
                "unknown": sch.get("unknown", False)}
    acked = bool(sch.get("acknowledged"))
    return {"ok": True, "task_id": tid, "launch_phase": "scheduling",
            "worker_started": acked,
            "worker_acknowledged": acked,
            "pending_ack": not acked}


def _initialize_and_schedule(store, d, tid, req, payload):
    _ensure_task_files(d, tid, req, payload)
    return _schedule(store, d, tid)


# ---------------------------------------------------------------- submit
def cmd_submit(_args):
    try:
        raw = sys.stdin.buffer.read().decode("utf-8", "replace")
        req = json.loads(raw or "{}")
    except Exception as e:
        say({"ok": False, "error": f"请求不是合法 JSON：{e}"})
        return 1
    if not isinstance(req, dict):
        say({"ok": False, "error": "请求必须是 JSON 对象"})
        return 1
    pol = policy()
    agent = str(req.get("agent") or "dsh")
    if agent not in pol.get("agents", {}):
        say({"ok": False, "error": f"未知的 agent：{agent}"})
        return 1
    spec = pol["agents"][agent]
    kind = spec.get("kind") or agent
    if kind not in ADAPTERS:
        say({"ok": False, "error": f"未知的 adapter kind：{kind}"})
        return 1
    if str(req.get("session_id") or "").strip():
        say({"ok": False, "error": "第一版拒绝外部 session_id"
             "（持久化会话的 sandbox/approval 会覆盖逐次配置）"})
        return 1
    try:
        cwd = check_cwd(req.get("cwd"), pol, must_exist=True, must_dir=True)
        permission = resolve_permission(pol, req.get("permission"), spec)
        timeout_s = validate_timeout(req, pol)
        validate_launch_spec(spec, kind, permission, cwd)
    except ValueError as e:
        say({"ok": False, "error": str(e)})
        return 1

    request_id = str(req.get("request_id") or "").strip() or ("r" + uuid.uuid4().hex)
    tid = req.get("task_id") or ("t" + time.strftime("%Y%m%d-%H%M%S") + "-"
                                 + uuid.uuid4().hex[:6])
    try:
        tid = validate_tid(tid)
    except ValueError as e:
        say({"ok": False, "error": str(e)})
        return 1

    try:
        store = _store()
    except StoreError as e:
        say({"ok": False, "error": f"索引不可用：{e}", "unknown": True})
        return 1
    alias = _existing_alias(tid)
    if alias and alias != tid:
        say({"ok": False, "error": f"task_id 大小写别名冲突：{alias}",
             "task_id": tid})
        return 1
    if alias == tid:
        try:
            mapped = store.get(task_id=tid)
        except StoreError as e:
            say({"ok": False, "error": f"索引不可用：{e}", "unknown": True})
            return 1
        if mapped is None:
            say({"ok": False, "error": f"task_id 已存在且无归属（拒绝覆盖遗留目录）：{tid}",
                 "task_id": tid})
            return 1

    payload = _normalized_payload(req, agent, cwd, permission, timeout_s)
    fp = fingerprint(payload)
    meta = {"agent": agent, "cwd": cwd, "prompt": payload["prompt"],
            "permission": permission, "timeout_s": timeout_s,
            "session_id": payload["session_id"]}
    try:
        res = store.reserve(request_id, tid, fp, meta)
    except StoreError as e:
        say({"ok": False, "error": f"索引不可用：{e}", "unknown": True})
        return 1

    if res["status"] == "conflict":
        say({"ok": False, "error": "request_id 冲突：payload 指纹不一致",
             "task_id": res["task_id"]})
        return 1
    if res["status"] == "collision":
        say({"ok": False, "error": f"task_id 已被占用：{tid}",
             "task_id": tid})
        return 1

    tid = res["task_id"]
    d = td(tid)
    owner = f"pid:{os.getpid()}"
    token = uuid.uuid4().hex

    try:
        if res["status"] == "existing":
            phase = res["phase"]
            claimed_init = False
            if phase in ("reserved", "prepared"):
                # Become the single initializer BEFORE touching any task file.
                claimed_init = store.begin_init(
                    tid, owner, token, worker_pid=os.getpid(),
                    expect=("reserved", "prepared"))
            elif phase == "scheduling":
                # A previous initializer may have died; recover only when its
                # recorded owner process is provably gone (never age-based).
                claimed_init = store.recover_scheduling(
                    tid, owner, token, worker_pid=os.getpid())
            if not claimed_init:
                # Either another caller owns initialization, or the launch is
                # uncertain; report existing state truthfully, never reschedule.
                say({"ok": True, "task_id": tid, "deduplicated": True,
                     "launch_phase": phase, "status": "existing",
                     "worker_started": False})
                return 0
        else:
            # Winner: claim init ownership atomically, then create files.
            if not store.begin_init(tid, owner, token, worker_pid=os.getpid(),
                                    expect=("reserved",)):
                say({"ok": True, "task_id": tid, "deduplicated": True,
                     "launch_phase": "scheduling", "worker_started": False})
                return 0

        sch = _initialize_and_schedule(store, d, tid, req, payload)
    except StoreError as e:
        say({"ok": False, "error": f"索引不可用：{e}", "unknown": True})
        return 1
    resp = _schedule_response(tid, sch)
    say(resp)
    return 0 if resp.get("ok") else 1


# ---------------------------------------------------------------- worker
def cancel_pending(tid, d=None, store=None):
    if d is not None and (d / "cancel.intent").exists():
        return True
    try:
        return (store or _store()).cancel_requested(tid)
    except StoreError:
        return False


def _write_state(d, patch):
    st = read_json(d / "state.json", {}) or {}
    st.update(patch)
    write_json(d / "state.json", st)
    return st


def _finalize(d, tid, status, store, exit_code=None, final=None,
              session_id=None, error=None, phase="terminal"):
    st = read_json(d / "state.json", {}) or {}
    if st.get("status") in TERMINAL:
        return st
    st = dict(st)
    st["status"] = status
    st["ended"] = now()
    if exit_code is not None:
        st["exit_code"] = exit_code
    if final is not None:
        st["final"] = final
        st["result"] = final
        st["final_preview"] = final[:2000]
    if session_id:
        st["session_id"] = session_id
    if error is not None:
        st["error"] = error
    write_json(d / "state.json", st)
    try:
        (d / "result.txt").write_text(final or "", encoding="utf-8")
    except Exception:
        pass
    if store is not None:
        try:
            store.update(tid, phase=phase, status=status,
                         result_path=str(d / "result.txt") if final is not None else None)
        except StoreError:
            pass
    return st


class _TextAccum:
    def __init__(self):
        self.parts = []
        self.buf = ""

    def add(self, text, meta):
        if not text:
            return
        if meta.get("delta"):
            self.buf += text
        else:
            if self.buf:
                self.parts.append(self.buf)
                self.buf = ""
            self.parts.append(text)

    def flush(self):
        if self.buf:
            self.parts.append(self.buf)
            self.buf = ""
        return "\n".join(self.parts)


class _RunControl:
    def __init__(self, proc, kill_tree):
        self.proc = proc
        self.kill_tree = kill_tree
        self.lock = threading.Lock()
        self.done = False
        self.cancelled = False
        self.timed_out = False

    def _kill(self, flag):
        with self.lock:
            if self.done:
                return False
            setattr(self, flag, True)
        self.kill_tree(self.proc.pid)
        return True

    def cancel(self):
        return self._kill("cancelled")

    def timeout(self):
        return self._kill("timed_out")

    def finish(self):
        with self.lock:
            self.done = True


def _feed_stdin(proc, data, done):
    try:
        if getattr(proc, "stdin", None) is not None:
            proc.stdin.write(data)
            proc.stdin.close()
    except Exception:
        # broken pipe / killed child: the watchdog owns termination
        pass
    finally:
        done.set()


def _run_worker(d, tid, req, pol, spec, kind, store):
    st = read_json(d / "state.json", {}) or {}
    if st.get("status") in TERMINAL:
        return
    if cancel_pending(tid, d, store):
        _finalize(d, tid, "cancelled", store, final="", session_id=req.get("session_id"),
                  error="取消于启动前")
        return

    try:
        permission = resolve_permission(pol, req.get("permission"), spec)
        timeout_s = validate_timeout(req, pol)
        cwd = check_cwd(req.get("cwd"), pol, must_exist=True, must_dir=True)
        extra = (prepare_dsh_permission_args(spec, permission, cwd, d)
                 if kind == "dsh" else [])
        cmd = build_agent_command(spec, kind, req, permission, extra_args=extra)
        env = build_agent_env(spec, kind, permission)
    except ValueError as e:
        _finalize(d, tid, "failed", store, error=str(e))
        return

    emit = make_emitter(d)
    started = now()
    emit("permission", permission, kind=kind)
    emit("started", f"{req.get('agent')} 开始执行", cwd=cwd, timeout_s=timeout_s)
    _write_state(d, {"status": "running", "started": started,
                     "worker_pid": os.getpid(), "permission": permission,
                     "timeout_s": timeout_s})

    # Re-check the durable cancel intent immediately before spawn: a cancel
    # that arrived during validation must not start a child.
    if cancel_pending(tid, d, store):
        _finalize(d, tid, "cancelled", store, final="",
                  session_id=req.get("session_id"), error="取消于启动前")
        try:
            emit.close()
        except Exception:
            pass
        return

    errf = open(d / "stderr.log", "a", encoding="utf-8", errors="replace")
    try:
        proc = _spawn(cmd, cwd, env, errf)
    except Exception as e:
        errf.close()
        emit("error", f"启动失败：{e}")
        _finalize(d, tid, "failed", store, error=str(e))
        try:
            emit.close()
        except Exception:
            pass
        return

    child_pid = getattr(proc, "pid", None)
    _write_state(d, {"status": "running", "child_pid": child_pid,
                     "worker_pid": os.getpid()})
    try:
        store.update(tid, child_pid=child_pid,
                     worker_pid=os.getpid(), phase="claimed")
    except StoreError:
        pass

    # Start the control/watchdog BEFORE transferring the prompt so a stalled
    # stdin pipe cannot block cancellation/timeout indefinitely.
    control = _RunControl(proc, KILL_TREE)
    stop = threading.Event()

    def _watch_cancel():
        while not stop.wait(CANCEL_POLL_S):
            if control.done:
                return
            if cancel_pending(tid, d, store):
                control.cancel()
                return

    watcher = threading.Thread(target=_watch_cancel, daemon=True)
    watcher.start()
    timer = threading.Timer(timeout_s, control.timeout)
    timer.daemon = True
    timer.start()

    # A cancel that arrived between publication and the first check.
    if cancel_pending(tid, d, store):
        control.cancel()

    stdin_done = threading.Event()
    feeder = threading.Thread(
        target=_feed_stdin,
        args=(proc, str(req.get("prompt") or ""), stdin_done),
        daemon=True)
    feeder.start()

    accum = _TextAccum()
    finals = {}
    final_order = []

    def add_final(text, meta):
        iid = meta.get("item_id")
        key = iid if iid else ("anon", len(final_order))
        if key not in finals:
            final_order.append(key)
        finals[key] = text

    last_error = ""
    terminal_error = False
    adapter = ADAPTERS[kind]
    code = None
    try:
        for line in proc.stdout:
            ev = adapter.parse(line)
            if not ev:
                continue
            etype, text, meta = ev
            if etype == "text":
                accum.add(text, meta)
            elif etype == "final":
                # Cumulative snapshots (codex item.updated) replace the same
                # item; only the last value per item is emitted, once.
                add_final(text, meta)
            elif etype == "session":
                if text:
                    _write_state(d, {"session_id": text})
            elif etype == "error":
                last_error = text
                if meta.get("terminal"):
                    terminal_error = True
            emit(etype, text, **meta)
        code = proc.wait()
    except Exception as e:
        control.finish()
        stop.set()
        timer.cancel()
        try:
            errf.close()
        except Exception:
            pass
        try:
            KILL_TREE(child_pid)
        except Exception:
            pass
        try:
            proc.wait()
        except Exception:
            pass
        emit("error", f"worker 读取失败：{e}")
        _finalize(d, tid, "unknown", store, error=str(e), phase="unknown")
        try:
            emit.close()
        except Exception:
            pass
        return
    finally:
        control.finish()
        stop.set()
        timer.cancel()
        try:
            errf.close()
        except Exception:
            pass

    if finals:
        final_text = "".join(finals[k] for k in final_order)
    else:
        final_text = accum.flush()
    session_id = (read_json(d / "state.json", {}) or {}).get("session_id") \
        or req.get("session_id")

    if control.cancelled:
        status, error = "cancelled", None
    elif control.timed_out:
        status, error = "timed_out", f"超时（{timeout_s} 秒），进程已终止"
        emit("error", error)
    elif code == 0 and terminal_error:
        status, error = "failed", last_error
    else:
        status = "succeeded" if code == 0 else "failed"
        error = None if code == 0 else (last_error or f"退出码 {code}")
    emit("exit", f"进程退出码 {code}", code=code)
    _finalize(d, tid, status, store, exit_code=code, final=final_text,
              session_id=session_id, error=error)
    try:
        emit.close()
    except Exception:
        pass


def cmd_run(args):
    try:
        tid = validate_tid(args.task_id)
    except ValueError as e:
        say({"ok": False, "error": str(e)})
        return 1
    d = TASKS / tid
    req = read_json(d / "request.json")
    if not req:
        say({"ok": False, "error": "找不到 request.json"})
        return 1
    pol = policy()
    agent = str(req.get("agent") or "dsh")
    spec = (pol.get("agents") or {}).get(agent)
    if not spec:
        say({"ok": False, "error": f"未知的 agent：{agent}"})
        return 1
    kind = spec.get("kind") or agent
    if kind not in ADAPTERS:
        say({"ok": False, "error": f"未知的 adapter kind：{kind}"})
        return 1
    store = _store()
    owner = f"pid:{os.getpid()}"
    token = uuid.uuid4().hex
    try:
        claimed = store.claim(tid, owner, token, req.get("request_id", ""),
                              worker_pid=os.getpid())
    except StoreError as e:
        say({"ok": False, "error": f"索引不可用：{e}"})
        return 1
    if not claimed:
        say({"ok": True, "task_id": tid, "worker": "skipped",
             "reason": "already claimed/terminal/unknown"})
        return 0
    # The worker is the durable acknowledgement the scheduler waited for; the
    # task definition can now be removed (idempotent if already gone).
    try:
        SCHEDULER.delete(f"qqagent-{tid}")
    except Exception:
        pass
    try:
        _run_worker(d, tid, req, pol, spec, kind, store)
    except Exception as e:  # last-resort truthfulness
        st = read_json(d / "state.json", {}) or {}
        cp = st.get("child_pid")
        if cp:
            try:
                KILL_TREE(cp)
            except Exception:
                pass
        _finalize(d, tid, "unknown", store, error=f"worker 异常：{e}",
                  phase="unknown")
    return 0


# ---------------------------------------------------------------- emitter
def make_emitter(d):
    d.mkdir(parents=True, exist_ok=True)
    f = open(d / "events.jsonl", "a", encoding="utf-8")
    seq = 0
    lock = threading.Lock()

    def emit(etype, text="", **meta):
        nonlocal seq
        with lock:
            seq += 1
            rec = {"seq": seq, "ts": now(), "type": etype, "text": text,
                   "meta": meta}
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            f.flush()
            return rec

    emit.close = f.close
    return emit


# ---------------------------------------------------------------- cancellation settlement
def _pid_liveness(pids):
    """Combine several pids into one tri-state: True, False or None (unknown).

    Any live pid wins; if none is provably live but at least one is
    unverifiable the result is None so the caller keeps the task nonterminal.
    """
    unknown = False
    for p in pids:
        if not p:
            continue
        state = PID_ALIVE(p)
        if state is True:
            return True
        if state is None:
            unknown = True
    return None if unknown else False


def _settle_cancel(d, st, store, tid, rec=None):
    """Derive a cancellation status without ever overwriting a live worker.

    While the worker/child is alive *or unverifiable* an in-memory
    ``cancel_requested`` view is returned and nothing is written to disk.
    Finalisation to ``cancelled`` happens only through the guarded ledger
    transition, which also blocks any future claim/start.
    """
    if st.get("status") in TERMINAL:
        return st
    if rec is None:
        try:
            rec = store.get(task_id=tid) if store is not None else None
        except StoreError:
            rec = None
    pending = cancel_pending(tid, d, store) or bool(rec and rec.get("cancel_requested"))
    if not pending:
        return st
    wp = (rec or {}).get("worker_pid") or st.get("worker_pid")
    cp = st.get("child_pid")
    if _pid_liveness([wp, cp]) is not False:
        derived = dict(st)
        derived["status"] = "cancel_requested"
        return derived
    if store is None:
        return st
    try:
        won = store.finalize_cancel(
            tid,
            expect_phase=(rec or {}).get("launch_phase"),
            expect_token=(rec or {}).get("claim_token"),
            expect_worker_pid=(rec or {}).get("worker_pid"),
        )
    except StoreError:
        derived = dict(st)
        derived["status"] = "cancel_requested"
        return derived
    if not won:
        # Another observer/worker already won, or the launch identity changed
        # under our snapshot (a real worker claimed after we observed a dead
        # initializer). Never publish terminal from the stale view: re-read
        # and report the still-pending cancellation while a new worker owns it.
        try:
            fresh = store.get(task_id=tid)
        except StoreError:
            fresh = None
        if (fresh is not None and fresh.get("cancel_requested")
                and fresh.get("launch_phase") not in ("terminal", "unknown")):
            derived = dict(st)
            derived["status"] = "cancel_requested"
            return derived
        return read_json(d / "state.json", {}) or st
    fresh = dict(read_json(d / "state.json", {}) or st)
    fresh["status"] = "cancelled"
    fresh["ended"] = fresh.get("ended") or now()
    write_json(d / "state.json", fresh)
    return fresh


def _settle_running(d, st, store, tid, rec=None):
    st = _settle_cancel(d, st, store, tid, rec)
    if st.get("status") in TERMINAL:
        return st
    if st.get("status") != "running":
        return st
    if rec is None:
        try:
            rec = store.get(task_id=tid) if store is not None else None
        except StoreError:
            rec = None
    if not (rec and rec.get("launch_phase") == "claimed"):
        return st
    wp = (rec or {}).get("worker_pid") or st.get("worker_pid")
    cp = st.get("child_pid")
    if _pid_liveness([wp, cp]) is not False:
        return st
    if store is None:
        return st
    try:
        won = store.finalize_unknown(
            tid,
            expect_phase=(rec or {}).get("launch_phase"),
            expect_token=(rec or {}).get("claim_token"),
            expect_worker_pid=(rec or {}).get("worker_pid"),
        )
    except StoreError:
        won = False
    if not won:
        return st
    fresh = dict(st)
    fresh["status"] = "unknown"
    fresh["ended"] = fresh.get("ended") or now()
    write_json(d / "state.json", fresh)
    return fresh


# ---------------------------------------------------------------- watch / cancel / prepare / reconcile / result / list
def cmd_watch(args):
    try:
        d = td(args.task_id)
    except ValueError as e:
        say({"ok": False, "error": str(e)})
        return 1
    f = d / "events.jsonl"
    if not f.exists() and not (d / "request.json").exists():
        say({"ok": False, "error": "任务不存在"})
        return 1
    if f.exists():
        for line in f.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if int(rec.get("seq", 0)) > args.after:
                say(rec)
    st = read_json(d / "state.json", {}) or {}
    try:
        st = _settle_running(d, st, _store(), args.task_id)
    except StoreError:
        pass
    say({"type": "_state", "seq": 10 ** 9, "state": st})
    return 0


def cmd_cancel(args):
    try:
        d = td(args.task_id)
    except ValueError as e:
        say({"ok": False, "error": str(e)})
        return 1
    store = _store()
    st = read_json(d / "state.json", {}) or {}
    try:
        rec = store.get(task_id=args.task_id)
    except StoreError as e:
        say({"ok": False, "error": f"索引不可用：{e}", "unknown": True})
        return 1
    exists = rec is not None or (d / "request.json").exists()
    if not exists:
        say({"ok": False, "error": "任务不存在", "found": False})
        return 1
    if st.get("status") in TERMINAL or (rec and rec.get("launch_phase") == "terminal"):
        say({"ok": True, "task_id": args.task_id, "status": st.get("status"),
             "already_terminal": True})
        return 0
    try:
        store.set_cancel(args.task_id)
    except StoreError as e:
        say({"ok": False, "error": f"索引不可用：{e}", "unknown": True})
        return 1
    if d.exists():
        try:
            (d / "cancel.intent").write_text(str(now()), encoding="utf-8")
        except Exception:
            pass
    say({"ok": True, "task_id": args.task_id, "requested": True,
         "accepted": True})
    return 0


def cmd_prepare(args):
    try:
        cwd = prepare_cwd(args.workspace_id, args.root, policy())
    except Exception as e:
        say({"ok": False, "error": str(e)})
        return 1
    say({"ok": True, "cwd": cwd})
    return 0


def _authoritative_status(d, st):
    return st.get("status")


def cmd_reconcile(args):
    request_id = getattr(args, "request_id", "") or ""
    task_id = getattr(args, "task_id", "") or ""
    store = _store()
    try:
        rec = None
        if request_id:
            rec = store.get(request_id=request_id)
        if rec is None and task_id:
            rec = store.get(task_id=task_id)
        legacy_checked = bool(request_id or task_id)
        if rec is None and request_id:
            task_id = store.legacy_find(request_id) or task_id
    except StoreError as e:
        say({"ok": False, "error": f"索引不可用：{e}", "unknown": True})
        return 1

    if rec is not None:
        task_id = rec["task_id"]
    if not task_id:
        if not legacy_checked:
            say({"ok": False, "error": "reconcile 需要 request_id 或 task_id",
                 "unknown": True})
            return 1
        say({"ok": True, "found": False})
        return 0
    try:
        d = td(task_id)
    except ValueError as e:
        say({"ok": False, "error": str(e)})
        return 1
    if not (d / "request.json").exists():
        if rec is None:
            say({"ok": True, "found": False, "task_id": task_id})
            return 0
        # ledger knows it but task files are missing -> explicit uncertainty
        say({"ok": True, "found": True, "task_id": task_id,
             "request_id": request_id or rec.get("request_id", ""),
             "status": "unknown", "launch_phase": rec.get("launch_phase"),
             "unknown": True, "final": ""})
        return 0

    st = read_json(d / "state.json", {}) or {}
    try:
        st = _settle_running(d, st, store, task_id)
    except StoreError:
        pass
    req = read_json(d / "request.json", {}) or {}
    try:
        res = (d / "result.txt").read_text(encoding="utf-8", errors="replace")
    except Exception:
        res = ""
    final = st.get("final") or st.get("result") or res or ""
    phase = rec.get("launch_phase") if rec else "legacy"
    launched = bool(rec and rec.get("claimed_by")) if rec else True
    say({"ok": True, "found": True, "task_id": task_id,
         "request_id": request_id or req.get("request_id", ""),
         "status": _authoritative_status(d, st),
         "launch_phase": phase, "launched": launched,
         "exit_code": st.get("exit_code"),
         "session_id": st.get("session_id"),
         "final": final, "result": final, "artifacts": st.get("artifacts") or []})
    return 0


def cmd_result(args):
    try:
        d = td(args.task_id)
    except ValueError as e:
        say({"ok": False, "error": str(e)})
        return 1
    if not (d / "request.json").exists():
        say({"ok": False, "found": False, "error": "任务不存在"})
        return 1
    st = read_json(d / "state.json", {}) or {}
    try:
        res = (d / "result.txt").read_text(encoding="utf-8", errors="replace")
    except Exception:
        res = ""
    final = st.get("final") or st.get("result") or res or ""
    say({"ok": True, "task_id": args.task_id, "status": st.get("status"),
         "final": final, "result": final,
         "session_id": st.get("session_id")})
    return 0


def cmd_list(_args):
    out = []
    if TASKS.exists():
        for d in sorted(TASKS.iterdir(), reverse=True):
            if not d.is_dir():
                continue
            st = read_json(d / "state.json", {}) or {}
            req = read_json(d / "request.json", {}) or {}
            out.append({"task_id": d.name, "agent": req.get("agent"),
                        "cwd": req.get("cwd"), "status": st.get("status"),
                        "prompt": (req.get("prompt") or "")[:60]})
            if len(out) >= 50:
                break
    say({"ok": True, "tasks": out})
    return 0


def cmd_capabilities(_args):
    say({"ok": True, **CAPABILITIES})
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", action="version", version=f"runner {RUNNER_VERSION}")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("submit").set_defaults(fn=cmd_submit)
    p_run = sub.add_parser("_run")
    p_run.add_argument("task_id")
    p_run.set_defaults(fn=cmd_run)
    p_w = sub.add_parser("watch")
    p_w.add_argument("task_id")
    p_w.add_argument("--after", type=int, default=0)
    p_w.set_defaults(fn=cmd_watch)
    p_c = sub.add_parser("cancel")
    p_c.add_argument("task_id")
    p_c.set_defaults(fn=cmd_cancel)
    p_p = sub.add_parser("prepare")
    p_p.add_argument("--workspace-id", dest="workspace_id", required=True)
    p_p.add_argument("--root", required=True)
    p_p.set_defaults(fn=cmd_prepare)
    p_r = sub.add_parser("reconcile")
    p_r.add_argument("--request-id", dest="request_id", default="")
    p_r.add_argument("--task-id", dest="task_id", default="")
    p_r.set_defaults(fn=cmd_reconcile)
    p_res = sub.add_parser("result")
    p_res.add_argument("task_id")
    p_res.set_defaults(fn=cmd_result)
    sub.add_parser("list").set_defaults(fn=cmd_list)
    sub.add_parser("capabilities").set_defaults(fn=cmd_capabilities)
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
