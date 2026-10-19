# -*- coding: utf-8 -*-
"""Workspace and run service.

One workspace == one persistent conversation == many runs. The current
workspace is derived from AstrBot's current conversation id; the plugin keeps no
competing pointer. Every access centralises UMO + non-empty owner enforcement so
a conversation id cannot be used across owners.
"""
from __future__ import annotations

import asyncio
import json
import secrets
from pathlib import Path

from astrbot.api import logger

from .binding import BindingPolicy
from .context_builder import build_result_surface, build_snapshot, parse_snapshot
from .exec_adapter import ExecutorError
from .models import (
    ACTIVE,
    EXECUTORS,
    OutboxItem,
    Run,
    RunStatus,
    Workspace,
    now,
)


class WorkspaceError(Exception):
    pass


class AmbiguousIdError(WorkspaceError):
    pass


class AccessDeniedError(WorkspaceError):
    pass


def _new_id(prefix: str) -> str:
    return f"{prefix}{secrets.token_hex(4)}"


class WorkspaceManager:
    def __init__(self, *, config, store, executor, notifier, cm, memory_gate,
                 results_dir: str | Path, sp=None, sleep_fn=None):
        self.config = config
        self.store = store
        self.executor = executor
        self.notifier = notifier
        self.cm = cm
        self.memory_gate = memory_gate
        self.sp = sp
        self.results_dir = Path(results_dir)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self._locks: dict[str, asyncio.Lock] = {}
        self._global_sem = asyncio.Semaphore(config.max_global_runs)
        self._background: set[asyncio.Task] = set()
        self._sleep = sleep_fn or asyncio.sleep
        self._shutting_down = False
        self.policy = BindingPolicy()
        # Optional readiness probe for the staged native follow-up guard. When
        # configured (``require_native_guard``), execution is refused unless the
        # running core actually exposes the guard marker.
        self.native_guard = None
        # Optional legacy-handoff probe: returns ``(allowed, reason)``.
        self.handoff_gate = None
        self.policy.bind_conversation_manager(cm)

    # ----------------------------------------------------------- access
    def check_access(self, event, *, need_admin: bool = True) -> str:
        if self._shutting_down:
            return "任务工作区正在关闭。"
        if not self.config.enabled:
            return "任务工作区插件未启用（fail-closed），请先在配置中启用。"
        umo = self._umo(event)
        if not umo:
            return "无法识别会话来源。"
        if not self.config.session_allowed(umo):
            return "该会话未被授权使用任务工作区（allowed_sessions 为空即全部拒绝）。"
        if need_admin and self.config.require_admin_private:
            try:
                if not event.is_admin():
                    return "仅限管理员使用。"
                if not event.is_private_chat():
                    return "仅限私聊使用。"
            except Exception:
                return "无法校验管理员/私聊身份。"
        return ""

    @staticmethod
    def _umo(event) -> str:
        return str(getattr(event, "unified_msg_origin", "") or "")

    @staticmethod
    def _sender(event) -> str:
        try:
            return str(event.get_sender_id() or "")
        except Exception:
            return ""

    def is_managed(self, event) -> bool:
        return self.check_access(event) == ""

    def _lock(self, ws_id: str) -> asyncio.Lock:
        lock = self._locks.get(ws_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[ws_id] = lock
        return lock

    def _authorize_workspace(self, ws: Workspace | None, event) -> Workspace | None:
        """Central owner + UMO gate for every workspace access."""
        if ws is None:
            return None
        umo = self._umo(event)
        sender = self._sender(event)
        if not sender:
            return None
        if ws.umo != umo or not ws.owner_user_id or ws.owner_user_id != sender:
            return None
        return ws

    async def refresh_policy(self) -> None:
        """Rehydrate the synchronous managed-conversation index."""
        self.policy.bind_conversation_manager(self.cm)
        workspaces = await self.store.all_workspaces()
        self.policy.load(workspaces)
        if self.cm is None:
            return
        umos = {ws.umo for ws in workspaces if ws.umo}
        # Warm the authoritative current-selection cache for every UMO the live
        # ConversationManager already knows about, not just workspace owners.
        live = getattr(self.cm, "session_conversations", None)
        if isinstance(live, dict):
            umos.update(k for k in live.keys() if k)
        for umo in umos:
            cid = ""
            if isinstance(live, dict):
                cid = live.get(umo) or ""
            if not cid:
                try:
                    cid = await self.cm.get_curr_conversation_id(umo)
                except Exception:
                    cid = None
            if cid:
                self.policy.set_current(umo, cid)
            else:
                self.policy.forget_current(umo)

    def managed_cid(self, event) -> str:
        """Best-effort current CID for the event's UMO (authoritative if bound)."""
        umo = self._umo(event)
        return self.policy.current_cid(umo)

    def classify(self, event, cid: str = "") -> str:
        """Return ``managed``, ``ordinary`` or ``unknown_managed``.

        A nonempty CID that is not in the managed index is an *ordinary*
        conversation, even when the same UMO owns other workspaces: the user
        switched to daily chat. ``unknown_managed`` is reserved for a
        known-managed UMO with no determinable current selection (fail closed).
        """
        umo = self._umo(event)
        if not umo:
            return "ordinary"
        if not self.config.session_allowed(umo):
            return "ordinary"
        effective = cid or self.managed_cid(event)
        if effective:
            if self.policy.is_managed_cid(effective):
                return "managed"
            # Nonempty, non-managed CID => ordinary daily conversation.
            return "ordinary"
        if self.policy.is_managed_umo(umo):
            return "unknown_managed"
        return "ordinary"

    # --------------------------------------------------------- workspaces
    async def _alloc_workspace_id(self) -> str:
        for _ in range(20):
            candidate = _new_id("ws_")
            if await self.store.prefix_free("workspaces", candidate[:9]):
                return candidate
        raise WorkspaceError("无法分配唯一的工作区 ID 前缀")

    async def create_workspace(self, event, title: str, environment: str = "windows_remote") -> Workspace:
        err = self.check_access(event)
        if err:
            raise AccessDeniedError(err)
        if environment not in ("windows_remote", "linux_local"):
            raise WorkspaceError("未知执行环境")
        if environment == "linux_local":
            from .binding import dual_runtime_ready
            if not dual_runtime_ready():
                raise WorkspaceError("Linux 窗口需要双窗口核心支持 v2，请先部署核心补丁")
        title = (title or "").strip()
        if not title:
            raise WorkspaceError("用法：/task new <标题>")
        umo = self._umo(event)
        sender = self._sender(event)
        if not sender:
            raise WorkspaceError("无法识别操作者。")
        if self.cm is None:
            raise WorkspaceError("conversation 管理器不可用。")
        platform_id = ""
        try:
            platform_id = event.get_platform_id()
        except Exception:
            pass

        cid = await self.cm.new_conversation(
            umo, platform_id, title=title, persona_id=self.config.workspace_persona
        )
        try:
            await self.cm.update_conversation(umo, conversation_id=cid, title=title)
        except Exception:
            pass

        ws_id = await self._alloc_workspace_id()
        ws = Workspace(
            id=ws_id, owner_user_id=sender, umo=umo, conversation_id=cid,
            title=title, persona=self.config.workspace_persona,
            memory_policy="isolated",
            cwd=self.config.linux_cwd if environment == "linux_local" else "",
            environment=environment,
        )
        try:
            await self.store.insert_workspace(ws)
        except Exception as e:
            logger.error("[task-workspace] 工作区落库失败，保留对话不删除：%s", e)
            raise WorkspaceError(
                f"工作区写入本地状态失败（对话 {cid[:8]} 已保留，未删除历史）：{e}"
            ) from e
        await self._force_persona(umo, self.config.workspace_persona, cid)
        self.policy.set_current(umo, cid)
        await self.refresh_policy()
        return ws

    async def _force_persona(self, umo: str, persona: str, cid: str = "") -> bool:
        """Bind the workspace persona on the *conversation*, never UMO-wide.

        The previous implementation wrote ``session_service_config`` at the UMO
        scope, which permanently overrode the daily personality for that session
        (including ordinary conversations). We only set ``conversation.persona_id``
        now; the global session override is never mutated. If a UMO-wide session
        persona is configured and conflicts with the workspace persona, callers
        refuse the operation via :meth:`persona_conflict` rather than overriding.
        """
        if self.cm is None or not cid:
            return False
        try:
            await self.cm.update_conversation(umo, cid, persona_id=persona)
        except Exception:
            logger.warning("[task-workspace] 设置会话人格失败", exc_info=True)
            return False
        if self.sp is None:
            return True
        try:
            cfg = await self.sp.get_async("umo", umo, "session_service_config", {})
            forced = (cfg or {}).get("persona_id") if isinstance(cfg, dict) else None
            return not forced or forced == persona
        except Exception:
            return True

    async def persona_conflict(self, umo: str, persona: str) -> bool:
        """True when a UMO-wide forced persona conflicts with the workspace one."""
        if self.sp is None:
            return False
        try:
            cfg = await self.sp.get_async("umo", umo, "session_service_config", {})
        except Exception:
            return False
        forced = (cfg or {}).get("persona_id") if isinstance(cfg, dict) else None
        return bool(forced) and forced != persona

    async def list_workspaces(self, event, include_archived: bool = True) -> list[Workspace]:
        owner = self._sender(event)
        ws = await self.store.list_workspaces(self._umo(event), owner)
        if include_archived:
            return ws
        return [w for w in ws if not w.archived]

    async def resolve(self, event, key: str) -> Workspace:
        owner = self._sender(event)
        key = (key or "").strip()
        if not key:
            raise WorkspaceError("请提供工作区 ID 或标题。")
        workspaces = await self.store.list_workspaces(self._umo(event), owner)
        exact = [w for w in workspaces if w.id == key]
        if exact:
            return exact[0]
        low = key.lower()
        by_prefix = [w for w in workspaces if w.id.lower().startswith(low)]
        by_title = [w for w in workspaces if low in (w.title or "").lower()]
        candidates = by_prefix or by_title
        if len(candidates) > 1:
            ids = ", ".join(w.short_id for w in candidates[:5])
            raise AmbiguousIdError(f"「{key}」匹配到多个工作区：{ids}，请用更精确的 ID 前缀。")
        if not candidates:
            raise WorkspaceError(f"找不到工作区「{key}」。")
        return candidates[0]

    async def current_workspace(self, event) -> Workspace | None:
        umo = self._umo(event)
        if self.cm is None:
            return None
        try:
            cid = await self.cm.get_curr_conversation_id(umo)
        except Exception:
            return None
        if not cid:
            return None
        ws = await self.store.get_workspace_by_cid(cid)
        return self._authorize_workspace(ws, event)

    async def switch(self, event, key: str) -> Workspace:
        err = self.check_access(event)
        if err:
            raise AccessDeniedError(err)
        ws = await self.resolve(event, key)
        if ws.archived:
            raise WorkspaceError("该工作区已归档，请先 /task restore。")
        if self.cm is None:
            raise WorkspaceError("conversation 管理器不可用。")
        current = await self.cm.get_conversation(ws.umo, ws.conversation_id)
        if current is None or str(getattr(current, "user_id", "") or "") not in ("", ws.umo):
            raise AccessDeniedError("对话归属校验失败，拒绝切换。")
        await self.cm.switch_conversation(ws.umo, ws.conversation_id)
        self.policy.set_current(ws.umo, ws.conversation_id)
        return ws

    async def rename(self, event, key: str, title: str) -> Workspace:
        err = self.check_access(event)
        if err:
            raise AccessDeniedError(err)
        title = (title or "").strip()
        if not title:
            raise WorkspaceError("用法：/task rename <标题>")
        ws = await self.resolve(event, key)
        await self.cm.update_conversation(ws.umo, conversation_id=ws.conversation_id, title=title)
        await self.store.update_workspace(ws.id, title=title)
        ws.title = title
        return ws

    async def archive(self, event, key: str) -> Workspace:
        err = self.check_access(event)
        if err:
            raise AccessDeniedError(err)
        ws = await self.resolve(event, key)
        async with self._lock(ws.id):
            if await self.store.active_runs_for_workspace(ws.id):
                raise WorkspaceError("该工作区仍有进行中的任务，不能归档。")
            await self.store.update_workspace(ws.id, archived=1)
        ws.archived = 1
        await self.refresh_policy()
        return ws

    async def restore(self, event, key: str) -> Workspace:
        err = self.check_access(event)
        if err:
            raise AccessDeniedError(err)
        ws = await self.resolve(event, key)
        await self.store.update_workspace(ws.id, archived=0)
        ws.archived = 0
        await self.refresh_policy()
        return ws

    # ------------------------------------------------------------ amendments
    async def amend(self, event, text: str) -> int:
        err = self.check_access(event)
        if err:
            raise AccessDeniedError(err)
        text = (text or "").strip()
        if not text:
            raise WorkspaceError("用法：/task amend <补充要求>")
        ws = await self.current_workspace(event)
        if ws is None:
            raise WorkspaceError("当前会话不是任务工作区。")
        return await self.store.add_pending(ws.id, text)

    # ---------------------------------------------------------------- runs
    def _execution_gate(self) -> str:
        ok, reason = self.memory_gate.execution_allowed()
        if not ok:
            return reason
        probe = self.handoff_gate
        if probe is not None:
            try:
                allowed, hreason = probe()
            except Exception:
                allowed, hreason = False, "旧桥交接状态不可信，拒绝执行。"
            if not allowed:
                return hreason or "旧桥轮询权尚未成功交接，拒绝执行。"
        if getattr(self.config, "require_native_guard", False):
            guard = self.native_guard
            if guard is not None:
                try:
                    installed, version = guard()
                except Exception:
                    installed, version = False, 0
                if not installed:
                    return (
                        "原生 follow-up 绑定守卫未加载"
                        f"（TASK_WORKSPACE_BINDING_GUARD_VERSION={version}），"
                        "按保守策略拒绝执行；需部署 staged core patch。"
                    )
        return ""

    async def _prepare_cwd(self, ws: Workspace) -> str:
        if ws.cwd:
            return ws.cwd
        prepare = getattr(self.executor, "prepare", None)
        if prepare is None:
            raise WorkspaceError("执行适配器不支持工作目录准备，拒绝使用共享目录。")
        try:
            cwd = await prepare(ws.id, self.config.workspace_root)
        except Exception as e:
            raise WorkspaceError(f"工作目录准备失败：{e}") from e
        if not cwd:
            raise WorkspaceError("工作目录准备失败：空路径。")
        await self.store.update_workspace(ws.id, cwd=cwd)
        ws.cwd = cwd
        return cwd

    async def start_run(self, event, executor: str, requirement: str = "",
                        workspace: Workspace | None = None) -> Run:
        err = self.check_access(event)
        if err:
            raise AccessDeniedError(err)
        if executor not in EXECUTORS:
            executor = self.config.default_executor
        if workspace is not None:
            ws = self._authorize_workspace(workspace, event)
            if ws is None:
                raise AccessDeniedError("工作区归属校验失败，拒绝执行。")
        else:
            ws = await self.current_workspace(event)
        if ws is None:
            raise WorkspaceError("当前会话不是任务工作区，请先 /task new 或 /task switch。")
        if ws.environment != "windows_remote":
            raise WorkspaceError("当前是 Linux 窗口，请直接描述本机操作；远端任务请切换 Windows 窗口。")
        if ws.archived:
            raise WorkspaceError("该工作区已归档，请先 /task restore。")
        if await self.persona_conflict(ws.umo, self.config.workspace_persona):
            raise WorkspaceError(
                "该会话存在冲突的全局人格覆盖设置，拒绝执行以避免污染日常人格；"
                "请移除 session_service_config.persona_id 后重试。")
        gate = self._execution_gate()
        if gate:
            raise WorkspaceError(gate)
        if not self.config.ssh_host and not getattr(self.executor, "is_fake", False):
            raise WorkspaceError("执行适配器未配置（缺少 SSH 目标），拒绝执行。")

        async with self._lock(ws.id):
            pending = await self.store.pending_unconsumed(ws.id)
            prior = ""
            for r in await self.store.list_runs(ws.id, limit=20):
                if r.status in (RunStatus.SUCCEEDED.value, RunStatus.FAILED.value,
                                RunStatus.TIMED_OUT.value):
                    prior = await self._result_text(r)
                    break
            conv = None
            try:
                conv = await self.cm.get_conversation(ws.umo, ws.conversation_id)
            except Exception:
                conv = None
            snapshot, truncated = build_snapshot(
                conv=conv, title=ws.title, user_requirement=requirement,
                pending_amendments=[p.text for p in pending], prior_result=prior,
                max_chars=self.config.context_max_chars,
            )
            snapshot["permission"] = self.config.default_permission
            snapshot["environment"] = ws.environment
            request_id = _new_id("req_")
            run = Run(
                id=_new_id("run_"), workspace_id=ws.id, executor=executor,
                request_id=request_id,
                input_snapshot=json.dumps(snapshot, ensure_ascii=False),
                status=RunStatus.QUEUED.value,
            )
            try:
                await self.store.reserve_run(run, [p.id for p in pending])
            except ValueError as e:
                raise WorkspaceError(
                    "该工作区已有进行中或排队中的任务，请等待完成或取消。"
                ) from e
        self._spawn(self._drive_run(run.id))
        if truncated:
            logger.info("[task-workspace] run %s 输入上下文已截断", run.id)
        return run

    def _spawn(self, coro) -> None:
        if self._shutting_down:
            return
        task = asyncio.create_task(coro)
        self._background.add(task)

        def _done(t):
            self._background.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc:
                logger.warning("[task-workspace] 后台任务异常", exc_info=exc)

        task.add_done_callback(_done)

    async def wait_background(self, timeout: float = 30.0) -> None:
        pending = [t for t in self._background if not t.done()]
        if pending:
            await asyncio.wait(pending, timeout=timeout)

    async def shutdown(self) -> None:
        self._shutting_down = True
        tasks = [t for t in self._background if not t.done()]
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._background.clear()

    # ------------------------------------------------------------- driving
    async def _drive_run(self, run_id: str) -> None:
        if self._shutting_down:
            return
        async with self._global_sem:
            run = await self.store.get_run(run_id)
            if run is None or run.status in (
                RunStatus.SUCCEEDED.value, RunStatus.FAILED.value,
                RunStatus.CANCELLED.value, RunStatus.TIMED_OUT.value,
            ):
                return
            ws = await self.store.get_workspace(run.workspace_id)
            if ws is None:
                await self.store.finalize_run(
                    run_id, RunStatus.FAILED.value, "", "[]", [], error="工作区丢失")
                return
            if run.cancel_intent or run.status == RunStatus.CANCEL_REQUESTED.value:
                await self._attempt_cancel(run, ws)
                return
            if run.status != RunStatus.QUEUED.value:
                return
            claimed = await self.store.claim_for_submit(run_id, now())
            if not claimed:
                return
            try:
                cwd = await self._prepare_cwd(ws)
            except WorkspaceError as e:
                await self._finalize(run, ws, RunStatus.FAILED.value, state={}, error=str(e))
                return
            prompt = parse_snapshot(run.input_snapshot).get("prompt", "")
            try:
                task_id = await self.executor.submit(
                    run.request_id, prompt, cwd, parse_snapshot(run.input_snapshot).get("permission", "read-only"),
                    self.config.default_timeout_s, run.executor,
                )
            except ExecutorError as e:
                await self._on_submit_error(run, ws, e)
                return
            await self.store.attach_remote_task(run_id, str(task_id))
            await self._notify(run, "submitted", ws.umo,
                               f"任务 {run.short_id} [{run.executor}] 已提交：{task_id}")
            await self._poll_until_terminal(await self.store.get_run(run_id), ws)

    async def _on_submit_error(self, run: Run, ws: Workspace, e: ExecutorError) -> None:
        if e.ambiguous:
            await self.store.update_run(
                run.id, status=RunStatus.NEEDS_RECONCILIATION.value, error=str(e))
            await self._notify(run, "reconcile", ws.umo,
                               f"任务 {run.short_id} 提交结果不确定，需核对：{e}")
        elif e.connectivity:
            await self._finalize(run, ws, RunStatus.FAILED.value, state={},
                                 error="远端不可达", connectivity_error=str(e))
        else:
            await self._finalize(run, ws, RunStatus.FAILED.value, state={}, error=str(e))

    async def _attempt_cancel(self, run: Run, ws: Workspace) -> None:
        await self.store.update_run(
            run.id, status=RunStatus.CANCEL_REQUESTED.value, cancel_intent=1)
        if not run.remote_task_id:
            if run.status == RunStatus.QUEUED.value:
                await self._finalize(run, ws, RunStatus.CANCELLED.value, state={})
            else:
                await self.store.update_run(
                    run.id, status=RunStatus.NEEDS_RECONCILIATION.value,
                    error="提交中取消，远端状态未知", cancel_intent=1)
            return
        try:
            await self.executor.cancel(run.remote_task_id)
        except Exception as e:
            logger.warning("[task-workspace] 取消失败，保留意图稍后重试：%s", e)
            return
        await self._poll_until_terminal(await self.store.get_run(run.id), ws)

    async def _poll_until_terminal(self, run: Run, ws: Workspace) -> None:
        while not self._shutting_down:
            await self._sleep(self.config.poll_interval)
            current = await self.store.get_run(run.id)
            if current is None or current.status in (
                RunStatus.SUCCEEDED.value, RunStatus.FAILED.value,
                RunStatus.CANCELLED.value, RunStatus.TIMED_OUT.value,
                RunStatus.NEEDS_RECONCILIATION.value,
            ):
                return
            try:
                events, state = await self.executor.watch(
                    current.remote_task_id, current.last_seq)
            except ExecutorError as e:
                await self.store.update_run(current.id, connectivity_error=str(e),
                                            error="watch 失败（连接层）")
                continue
            except Exception as e:
                await self.store.update_run(current.id, connectivity_error=str(e))
                continue

            last_seq = current.last_seq
            for ev in events or []:
                try:
                    last_seq = max(last_seq, int(ev.get("seq", 0)))
                except Exception:
                    pass
            status = (state or {}).get("status")
            if status in ("succeeded", "failed", "timed_out", "cancelled"):
                mapped = {"succeeded": RunStatus.SUCCEEDED.value,
                          "failed": RunStatus.FAILED.value,
                          "timed_out": RunStatus.TIMED_OUT.value,
                          "cancelled": RunStatus.CANCELLED.value}[status]
                await self._finalize(current, ws, mapped, state, last_seq)
                return
            if current.cancel_intent and current.remote_task_id:
                try:
                    await self.executor.cancel(current.remote_task_id)
                except Exception:
                    pass
            await self.store.update_run(current.id, last_seq=last_seq)

    async def _finalize(self, run: Run, ws: Workspace, status: str, state: dict,
                        last_seq: int | None = None, error: str = "",
                        connectivity_error: str | None = None) -> bool:
        from .exec_adapter import extract_result

        result_text, artifacts = extract_result(state or {})
        outbox = []
        if status in (RunStatus.SUCCEEDED.value, RunStatus.FAILED.value,
                      RunStatus.TIMED_OUT.value):
            summary = build_result_surface(
                ws, Run(id=run.id, workspace_id=ws.id, executor=run.executor,
                        request_id=run.request_id, input_snapshot=run.input_snapshot,
                        status=status),
                result_text, self.config.result_max_chars)
            outbox.append(self._outbox(run, f"terminal:{status}", ws.umo, summary, title=ws.title))
        elif status == RunStatus.CANCELLED.value:
            outbox.append(self._outbox(run, "cancelled", ws.umo,
                                       f"任务 {run.short_id} 已取消。", title=ws.title))
        return await self.store.finalize_run(
            run.id, status, result_text, json.dumps(artifacts, ensure_ascii=False),
            outbox, error=error, connectivity_error=connectivity_error,
        )

    def _outbox(self, run: Run, key: str, session: str, content: str, *, title: str = "") -> OutboxItem:
        return OutboxItem(id=None, run_id=run.id, event_key=key, session=session,
                          content=f"[Windows · {title or run.workspace_id[:9]} · {run.short_id}]\n" + content, created_ts=now())

    async def _result_text(self, run: Run) -> str:
        if run.result_text:
            return run.result_text
        if run.result_ref:
            try:
                return Path(run.result_ref).read_text(encoding="utf-8")
            except Exception:
                return ""
        return ""

    async def read_result(self, run: Run) -> str:
        return await self._result_text(run)

    async def _notify(self, run: Run, event_key: str, session: str, content: str) -> None:
        if not session:
            return
        ws = await self.store.get_workspace(run.workspace_id)
        label = f"[Windows · {ws.title if ws else run.workspace_id} · {run.short_id}]\n"
        await self.notifier.enqueue(run.id, event_key, session, label + content)

    # -------------------------------------------------------------- recovery
    async def resume_active_runs(self) -> None:
        for run in await self.store.active_runs():
            if run.cancel_intent:
                self._spawn(self._recover_cancel(run.id))
            elif run.status == RunStatus.NEEDS_RECONCILIATION.value:
                self._spawn(self._recover_reconcile(run.id))
            elif run.remote_task_id:
                self._spawn(self._resume_run(run.id))
            elif run.status in (RunStatus.QUEUED.value,) and int(run.submit_attempts or 0) == 0:
                # Known not-yet-submitted: execute once rather than marking it
                # failed. (Only a run that provably never dispatched may be
                # re-driven; any uncertain submit attempt reconciles instead.)
                self._spawn(self._drive_run(run.id))
            else:
                # submitting/cancel-requested with no remote id: unknown.
                self._spawn(self._recover_reconcile(run.id))

    async def _recover_cancel(self, run_id: str) -> None:
        run = await self.store.get_run(run_id)
        if run is None:
            return
        ws = await self.store.get_workspace(run.workspace_id)
        if ws is None:
            return
        if run.remote_task_id:
            try:
                await self.executor.cancel(run.remote_task_id)
            except Exception:
                pass
            await self._resume_run(run_id)
        else:
            await self._attempt_cancel(run, ws)

    async def _recover_reconcile(self, run_id: str) -> None:
        run = await self.store.get_run(run_id)
        if run is None:
            return
        ws = await self.store.get_workspace(run.workspace_id)
        if ws is None:
            return
        try:
            result = await self.executor.reconcile(request_id=run.request_id)
        except Exception as e:
            logger.warning("[task-workspace] 启动对账失败 %s: %s", run_id, e)
            return
        await self._apply_reconcile(run, ws, result)

    async def _apply_reconcile(self, run: Run, ws: Workspace, result: dict) -> Run:
        if not isinstance(result, dict) or result.get("ok") is not True:
            await self.store.update_run(
                run.id, status=RunStatus.NEEDS_RECONCILIATION.value,
                error="对账结果不可信（未确认）")
            return await self.store.get_run(run.id)
        if result.get("found") is False:
            await self._finalize(run, ws, RunStatus.FAILED.value, state={},
                                 error="对账确认远端无此任务")
            return await self.store.get_run(run.id)
        remote = str(result.get("task_id") or run.remote_task_id)
        status = result.get("status")
        if status in (RunStatus.SUCCEEDED.value, RunStatus.FAILED.value,
                      RunStatus.TIMED_OUT.value, RunStatus.CANCELLED.value):
            await self._finalize(run, ws, status, state=result)
            if remote:
                await self.store.update_run(run.id, remote_task_id=remote)
            return await self.store.get_run(run.id)
        if remote:
            await self.store.update_run(run.id, remote_task_id=remote,
                                        status=RunStatus.RUNNING.value)
            self._spawn(self._resume_run(run.id))
        return await self.store.get_run(run.id)

    async def _resume_run(self, run_id: str) -> None:
        if self._shutting_down:
            return
        async with self._global_sem:
            run = await self.store.get_run(run_id)
            if run is None or not run.remote_task_id:
                return
            if run.status in (RunStatus.SUCCEEDED.value, RunStatus.FAILED.value,
                              RunStatus.CANCELLED.value, RunStatus.TIMED_OUT.value):
                return
            ws = await self.store.get_workspace(run.workspace_id)
            if ws is None:
                return
            if run.cancel_intent or run.status == RunStatus.CANCEL_REQUESTED.value:
                try:
                    await self.executor.cancel(run.remote_task_id)
                except Exception:
                    pass
            else:
                await self.store.update_run(run_id, status=RunStatus.RUNNING.value)
            await self._poll_until_terminal(await self.store.get_run(run_id), ws)

    # -------------------------------------------------------------- queries
    async def _owned_remote_target(self, event, key):
        if not key or not key.startswith("run_"):
            return None
        matches = await self.store.owned_runs_by_prefix(self._umo(event), self._sender(event), key)
        exact = [r for r in matches if r.id == key]
        matches = exact or matches
        if len(matches) != 1:
            raise WorkspaceError("远端任务 ID 不存在、无权访问或前缀不唯一")
        run = matches[0]
        ws = await self.store.get_workspace(run.workspace_id)
        if not self._authorize_workspace(ws, event):
            raise AccessDeniedError("无权管理此任务")
        return ws, run

    async def cancel(self, event, run_key: str = "") -> Run:
        err = self.check_access(event)
        if err:
            raise AccessDeniedError(err)
        target = await self._owned_remote_target(event, run_key)
        ws = target[0] if target else await self.current_workspace(event)
        if ws is None:
            raise WorkspaceError("当前会话不是任务工作区。")
        run = target[1] if target else await self._resolve_run(event, ws, run_key)
        if run.status not in ACTIVE:
            raise WorkspaceError(f"任务 {run.short_id} 已是终态（{run.status}），无需取消。")
        await self.store.update_run(run.id, status=RunStatus.CANCEL_REQUESTED.value,
                                    cancel_intent=1)
        self._spawn(self._attempt_cancel(await self.store.get_run(run.id), ws))
        return await self.store.get_run(run.id)

    async def _resolve_run(self, event, ws: Workspace, key: str) -> Run:
        runs = await self.store.list_runs(ws.id, limit=200)
        if not key:
            for r in runs:
                if r.status in ACTIVE:
                    return r
            if runs:
                return runs[0]
            raise WorkspaceError("该工作区还没有任务。")
        key = key.strip()
        exact = [r for r in runs if r.id == key]
        if exact:
            return exact[0]
        low = key.lower()
        matches = [r for r in runs if r.id.lower().startswith(low)]
        if len(matches) > 1:
            raise AmbiguousIdError("任务 ID 前缀不唯一，请提供更精确的 ID。")
        if not matches:
            raise WorkspaceError(f"找不到任务「{key}」。")
        return matches[0]

    async def status(self, event, run_key: str = "") -> list[Run] | Run:
        err = self.check_access(event)
        if err:
            raise AccessDeniedError(err)
        target = await self._owned_remote_target(event, run_key)
        ws = target[0] if target else await self.current_workspace(event)
        if ws is None:
            raise WorkspaceError("当前会话不是任务工作区。")
        if run_key:
            return target[1] if target else await self._resolve_run(event, ws, run_key)
        return await self.store.list_runs(ws.id, limit=20)

    async def reconcile(self, event, run_key: str = "") -> Run:
        err = self.check_access(event)
        if err:
            raise AccessDeniedError(err)
        target = await self._owned_remote_target(event, run_key)
        ws = target[0] if target else await self.current_workspace(event)
        if ws is None:
            raise WorkspaceError("当前会话不是任务工作区。")
        run = target[1] if target else await self._resolve_run(event, ws, run_key)
        if run.status != RunStatus.NEEDS_RECONCILIATION.value:
            return run
        try:
            result = await self.executor.reconcile(request_id=run.request_id)
        except Exception as e:
            raise WorkspaceError(f"对账失败：{e}") from e
        return await self._apply_reconcile(run, ws, result)

    async def surface_pending_result(self, event, workspace: Workspace | None = None,
                                     expected_cid: str | None = None) -> tuple[str, str]:
        """Return ``(run_id, text)`` for the newest unsurfaced result.

        The exact run id is returned so the caller can acknowledge *that* run
        after injection, instead of a separate "newest unsurfaced" lookup that a
        concurrent completion could race.
        """
        ws = workspace or await self.current_workspace(event)
        if ws is None:
            return "", ""
        if expected_cid is not None:
            if not expected_cid or expected_cid != ws.conversation_id:
                return "", ""
        for r in await self.store.list_runs(ws.id, limit=20):
            if r.status == RunStatus.SUCCEEDED.value and not r.result_surfaced:
                text = await self._result_text(r)
                if not text:
                    await self.store.update_run(r.id, allow_terminal=True, result_surfaced=1)
                    return "", ""
                return r.id, build_result_surface(
                    ws, r, text, self.config.result_max_chars)
        return "", ""

    async def mark_result_surfaced(self, event, workspace: Workspace | None = None,
                                   expected_cid: str | None = None,
                                   run_id: str | None = None) -> None:
        ws = workspace or await self.current_workspace(event)
        if ws is None:
            return
        if expected_cid is not None and expected_cid != ws.conversation_id:
            return
        for r in await self.store.list_runs(ws.id, limit=20):
            if run_id is not None and r.id != run_id:
                continue
            if r.status == RunStatus.SUCCEEDED.value and not r.result_surfaced:
                if not await self._result_text(r):
                    continue
                await self.store.update_run(r.id, allow_terminal=True, result_surfaced=1)
                return
