"""Native execution ledger and per-window lifecycle, separate from remote runs."""
from __future__ import annotations

import asyncio
import time
import uuid

from astrbot.api import logger

from .binding import get_binding
from .window_policy import LINUX_TOOLS, sanitize_dsml, capability_prompt

ACTIVE = ("running", "background", "cancel_requested")


class NativeLedger:
    def __init__(self, store):
        self.store = store

    async def query(self, sql, params=()):
        def run():
            with self.store._lock:
                return [dict(r) for r in self.store._connect().execute(sql, params).fetchall()]
        return await asyncio.to_thread(run)

    async def reserve(self, ws, scope, maximum):
        def run():
            with self.store._lock:
                c = self.store._connect()
                c.execute("BEGIN IMMEDIATE")
                try:
                    current = c.execute("SELECT archived,environment FROM workspaces WHERE id=?", (ws.id,)).fetchone()
                    if current is None or current["archived"] or current["environment"] != "linux_local":
                        raise RuntimeError("Linux 工作区已归档或不存在")
                    count = c.execute("SELECT count(*) FROM native_executions WHERE status IN ('running','background','cancel_requested')").fetchone()[0]
                    if count >= maximum:
                        raise RuntimeError("Linux 全局活跃任务已达上限，请先查看 /task status")
                    if c.execute("SELECT 1 FROM native_executions WHERE workspace_id=? AND status IN ('running','background','cancel_requested')", (ws.id,)).fetchone():
                        raise RuntimeError("该 Linux 窗口仍有任务或后台进程，请先等待或 /task cancel")
                    rid = "local_" + uuid.uuid4().hex
                    c.execute("INSERT INTO native_executions(id,workspace_id,owner,scope,status,created_ts) VALUES(?,?,?,?,?,?)", (rid, ws.id, ws.owner_user_id, scope + ":" + rid, "running", time.time()))
                    c.execute("COMMIT")
                    return rid
                except BaseException:
                    c.execute("ROLLBACK")
                    raise
        return await asyncio.to_thread(run)

    async def finish(self, rid, status, text="", error=""):
        await self.query("UPDATE native_executions SET status=?,result_text=?,error=?,runner_done=1,ended_ts=? WHERE id=?", (status, text, error, 0 if status in ACTIVE else time.time(), rid))


class NativeService:
    def __init__(self, manager, context):
        self.manager = manager
        self.context = context
        self.ledger = NativeLedger(manager.store)
        self.runners = {}
        self.events = {}
        self._watcher = None
        self._lifecycle = asyncio.Lock()

    async def initialize(self):
        # No command is replayed after a process restart. Child processes may
        # survive a crash, so report uncertainty rather than claiming failure.
        await self.ledger.query("UPDATE native_executions SET status='interrupted',error='进程重启：执行状态待核对，未自动重放',ended_ts=? WHERE status IN ('running','background','cancel_requested')", (time.time(),))
        self._watcher = asyncio.create_task(self._watch_background())

    async def validate(self, event, *, tool=None):
        b = get_binding(event)
        if not b or not b.get("managed"):
            raise RuntimeError("缺少工作区绑定")
        m = self.manager
        if err := m.check_access(event):
            raise RuntimeError(err)
        ws = await m.store.get_workspace(b.get("ws_id", ""))
        if (not ws or ws.archived or ws.conversation_id != b.get("cid")
                or ws.umo != b.get("umo") or ws.umo != event.unified_msg_origin
                or ws.owner_user_id != b.get("owner") or ws.owner_user_id != m._sender(event)
                or ws.environment != b.get("environment") or ws.cwd != b.get("cwd")):
            raise RuntimeError("工作区冻结绑定校验失败，拒绝执行")
        from .binding import dual_runtime_ready
        if not dual_runtime_ready() or not m.memory_gate.detect().available:
            raise RuntimeError("工作区核心或记忆隔离能力不可用")
        if ws.environment == "linux_local":
            config = self.context.get_config(umo=ws.umo)
            if config.get("provider_settings", {}).get("computer_use_runtime", "local") != "local":
                raise RuntimeError("Linux 窗口要求 local 运行时，不能回退到其他执行环境")
        if tool is not None:
            if ws.environment != "linux_local" or tool not in LINUX_TOOLS:
                raise RuntimeError("当前窗口不允许调用该工具")
            if tool not in b.get("allowed_tools", ()):
                raise RuntimeError("工具不属于本次请求的能力快照")
            rid = event.get_extra("_task_workspace_native_id")
            rows = await self.ledger.query("SELECT status FROM native_executions WHERE id=? AND workspace_id=?", (rid, ws.id))
            if not rows or rows[0]["status"] not in ("running", "background"):
                raise RuntimeError("本机任务已取消或结束")
        return ws

    async def restrict_request(self, event, req):
        """Last capability check after all plugin hooks and before model reset."""
        ws = await self.validate(event)
        b = get_binding(event)
        if getattr(getattr(req, "conversation", None), "cid", "") != b["cid"]:
            raise RuntimeError("模型请求与冻结窗口不一致")
        from .context_builder import strip_managed_chat_tools
        strip_managed_chat_tools(req, LINUX_TOOLS if ws.environment == "linux_local" else [])
        prompt = capability_prompt(b)
        if prompt not in (req.system_prompt or ""):
            req.system_prompt = (req.system_prompt or "") + "\n\n" + prompt

    async def begin(self, event, scope):
        ws = await self.validate(event)
        if ws.environment != "linux_local":
            return
        rid = await self.ledger.reserve(ws, scope, self.manager.config.linux_max_runs)
        event.set_extra("_task_workspace_native_id", rid)
        self.events[rid] = event

    def register(self, event, runner):
        rid = event.get_extra("_task_workspace_native_id")
        if rid:
            self.runners[rid] = runner

    @staticmethod
    async def sessions(scope, owner):
        from astrbot.core.computer.computer_client import get_local_booter
        return await get_local_booter().shell.list_sessions(owner_id=scope, requester_id=owner, requester_is_admin=True)

    async def finish(self, event, response=None, error=""):
        async with self._lifecycle:
            await self._finish(event, response, error)

    async def _finish(self, event, response=None, error=""):
        rid = event.get_extra("_task_workspace_native_id")
        if not rid:
            return
        rows = await self.ledger.query("SELECT * FROM native_executions WHERE id=?", (rid,))
        if not rows:
            return
        row = rows[0]
        self.runners.pop(rid, None)
        text = getattr(response, "completion_text", "") or ""
        try:
            sessions = (await self.sessions(row["scope"], row["owner"]))["sessions"]
        except Exception:
            await self.ledger.finish(rid, "interrupted", text, "无法核对后台进程，需人工检查")
            self.events.pop(rid, None)
            return
        cancelled = row["status"] == "cancel_requested" or bool(event.get_extra("agent_user_aborted"))
        if cancelled or error:
            await self._stop_sessions(row)
        active = any(s["status"] == "running" for s in sessions) and not (cancelled or error)
        status = "cancelled" if cancelled else "failed" if error else "background" if active else "succeeded"
        await self.ledger.finish(rid, status, text, error)
        if not active:
            self.events.pop(rid, None)

    async def _stop_sessions(self, row):
        from astrbot.core.computer.computer_client import get_local_booter
        shell = get_local_booter().shell
        for s in (await self.sessions(row["scope"], row["owner"]))["sessions"]:
            if s["status"] == "running":
                await shell.terminate_session(owner_id=row["scope"], requester_id=row["owner"], requester_is_admin=True, session_id=s["session_id"])

    async def _session_output(self, row):
        from astrbot.core.computer.computer_client import get_local_booter
        shell = get_local_booter().shell
        parts = []
        for item in (await self.sessions(row["scope"], row["owner"]))["sessions"]:
            result = await shell.snapshot_session(
                owner_id=row["scope"], requester_id=row["owner"], requester_is_admin=True,
                session_id=item["session_id"],
                max_output_chars=self.manager.config.result_max_chars,
            )
            parts.append(f"Shell {item['session_id']} [{item['status']}]\n{result.get('stdout', '')}")
        return "\n".join(parts)

    async def _watch_background(self):
        while True:
            await asyncio.sleep(2)
            try:
                rows = await self.ledger.query("SELECT * FROM native_executions WHERE status='background'")
                for row in rows:
                    async with self._lifecycle:
                        await self._finish_background(row["id"])
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("[task-workspace] 本机后台状态核对失败", exc_info=True)

    async def _finish_background(self, rid):
        row = (await self.ledger.query("SELECT * FROM native_executions WHERE id=?", (rid,)))[0]
        if row["status"] != "background":
            return
        sessions = (await self.sessions(row["scope"], row["owner"]))["sessions"]
        if any(s["status"] == "running" for s in sessions):
            return
        status = "failed" if any(s["status"] in ("failed", "timed_out") for s in sessions) else "succeeded"
        await self.ledger.finish(rid, status, row["result_text"] + "\n" + await self._session_output(row))
        self.events.pop(rid, None)
        ws = await self.manager.store.get_workspace(row["workspace_id"])
        if ws:
            await self.manager.notifier.enqueue(rid, "background_done", ws.umo, f"[Linux · {ws.title} · {rid[:14]}] 后台进程已结束：{status}。使用 /task result 查看结果。")

    async def busy(self):
        return bool(await self.ledger.query("SELECT 1 FROM native_executions WHERE status IN ('running','background','cancel_requested') LIMIT 1"))

    async def ensure_archivable(self, event, key):
        ws = await self.manager.resolve(event, key)
        rows = await self.ledger.query("SELECT 1 FROM native_executions WHERE workspace_id=? AND status IN ('running','background','cancel_requested')", (ws.id,))
        if rows:
            raise RuntimeError("Linux 窗口仍有活跃执行，不能归档")

    async def command(self, event, sub, key):
        if err := self.manager.check_access(event):
            return err
        ws = await self.manager.current_workspace(event)
        if key.startswith("run_"):
            return None  # Manager resolves explicit remote IDs by owner, independent of selection.
        if key.startswith("local_"):
            candidates = await self.ledger.query("SELECT * FROM native_executions WHERE substr(id,1,?)=?", (len(key), key))
            owned = []
            for row in candidates:
                target = await self.manager.store.get_workspace(row["workspace_id"])
                if target and self.manager._authorize_workspace(target, event):
                    owned.append((row, target))
            if len(owned) != 1:
                return "本机执行 ID 不存在、无权访问或前缀不唯一"
            rows, ws = [owned[0][0]], owned[0][1]
        elif ws and ws.environment == "linux_local":
            if key:
                return "当前是 Linux 窗口，请提供 local_ 执行 ID；远端操作请切换 Windows 窗口"
            rows = await self.ledger.query("SELECT * FROM native_executions WHERE workspace_id=? ORDER BY created_ts DESC LIMIT 20", (ws.id,))
        else:
            return None
        if sub in ("amend", "reconcile"):
            return "Linux 窗口请直接描述需求；中断执行需核对后重新发起，不自动重放"
        if not rows:
            return "当前 Linux 窗口暂无执行记录"
        if sub == "status":
            return "\n".join(f"[Linux · {ws.title} · {r['id'][:14]}] {r['status']} {r['error']}" for r in rows)
        row = rows[0]
        if sub == "result":
            extra = await self._session_output(row) if row["status"] in ACTIVE else ""
            return f"[Linux · {ws.title} · {row['id'][:14]}]\n" + (row["result_text"] or row["error"] or row["status"]) + ("\n" + extra if extra else "")
        if sub == "cancel":
            async with self._lifecycle:
                return await self._cancel(row["id"], ws)
        return None

    async def _cancel(self, rid, ws):
        row = (await self.ledger.query("SELECT * FROM native_executions WHERE id=?", (rid,)))[0]
        if row["status"] not in ACTIVE:
            return f"执行已结束：{row['status']}"
        await self.ledger.query("UPDATE native_executions SET status='cancel_requested' WHERE id=?", (rid,))
        ev = self.events.get(rid)
        if ev:
            ev.set_extra("agent_stop_requested", True)
        runner = self.runners.get(rid)
        if runner:
            runner.request_stop()
        await self._stop_sessions(row)
        if runner is None and row["runner_done"]:
            await self.ledger.finish(rid, "cancelled", row["result_text"])
            self.events.pop(rid, None)
        return f"[Linux · {ws.title} · {rid[:14]}] 已请求停止执行及其登记的后台进程"

    async def close(self):
        if self._watcher:
            self._watcher.cancel()
            await asyncio.gather(self._watcher, return_exceptions=True)

    def sanitize(self, event, response, messages):
        b = get_binding(event)
        if not b or not b.get("managed") or response is None:
            return
        user = event.get_message_str() or ""
        text, blocked = sanitize_dsml(response.completion_text or "", user, b["environment"])
        if blocked:
            logger.warning("[task-workspace] blocked textual tool call ws=%s environment=%s", b["ws_id"], b["environment"])
            response.result_chain = None
        label = "Linux" if b["environment"] == "linux_local" else "Windows"
        rid = event.get_extra("_task_workspace_native_id") or "chat"
        prefix = f"[{label} · {b['title']} · {rid[:14]}]\n"
        response.completion_text = prefix + text
        response.reasoning_content = None
        event.set_extra("_llm_reasoning_content", "")
        # Agent messages are also persisted; sanitize assistant content there,
        # never user quotations or real tool responses.
        for message in messages or []:
            if getattr(message, "role", None) == "assistant":
                content = getattr(message, "content", None)
                if isinstance(content, str):
                    message.content = sanitize_dsml(content, user, b["environment"])[0]
                elif isinstance(content, list):
                    for part in content:
                        if getattr(part, "type", "") == "text":
                            part.text = sanitize_dsml(part.text, user, b["environment"])[0]
