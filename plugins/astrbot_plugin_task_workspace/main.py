# -*- coding: utf-8 -*-
"""astrbot_plugin_task_workspace — user-approved task workspace architecture.

A workspace is a persistent AstrBot conversation; runs are executor jobs bound
to that workspace. State is SQLite in the plugin data directory. Long-term
memory is isolated per workspace policy (gated on a livingmemory capability
handshake). The plugin registers only ``/task`` to avoid duplicate commands;
legacy bridge commands are routed through the service facade in
``bridge_compat``.
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path

from astrbot.api import logger, sp as shared_prefs
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star
from astrbot.core.star.filter.command import GreedyStr

from .bridge_compat import (
    TaskWorkspaceService,
    attach_service,
    detach_service,
    ensure_pending_handoff,
    handoff_allows_execution,
)
from .binding import (
    BINDING_EXTRA_KEY,
    BINDING_GUARD_VERSION,
    POLICY_CONTEXT_ATTR,
    TaskWorkspaceBindingFilter,
    active_runner_binding,
    get_binding,
    looks_like_command,
    native_guard_status,
    set_active_plugin,
)
from .config import TaskWorkspaceConfig
from .context_builder import strip_managed_chat_tools
from .exec_adapter import SshRemoteExecutor
from .import_migration import LegacyImporter
from .manager import AccessDeniedError, AmbiguousIdError, WorkspaceError, WorkspaceManager
from .memory_gate import MemoryGate
from .models import RunStatus
from .notify import Notifier
from .store import Store

PLUGIN_NAME = "astrbot_plugin_task_workspace"


def _resolve_data_dir() -> Path:
    override = os.environ.get("TASK_WORKSPACE_DATA_DIR")
    if override:
        p = Path(override)
        p.mkdir(parents=True, exist_ok=True)
        return p
    try:
        from astrbot.core.star.star_tools import StarTools

        return Path(StarTools.get_data_dir(PLUGIN_NAME))
    except Exception:
        p = Path.cwd() / "task_workspace_data"
        p.mkdir(parents=True, exist_ok=True)
        return p


class TaskWorkspace(Star):
    _HELP_TEXT = (
        "用法：/task new|list|switch|rename|archive|restore|run|status|"
        "result|amend|cancel|reconcile ...\n"
        "说明：工作区会话只做讨论与结果消费，不暴露本机原生执行工具，"
        "不会直接执行命令；真正执行必须显式发送 /task run。"
    )

    def __init__(self, context: Context, config=None):
        super().__init__(context)
        self._cfg = TaskWorkspaceConfig(config)
        self._store: Store | None = None
        self._manager: WorkspaceManager | None = None
        self._notifier: Notifier | None = None

    async def initialize(self):
        data_dir = _resolve_data_dir()
        self._store = Store(data_dir / "task_workspace.db")
        await self._store.migrate()

        executor = SshRemoteExecutor(
            host=self._cfg.ssh_host,
            port=self._cfg.ssh_port,
            user=self._cfg.ssh_user,
            work_dir=self._cfg.ssh_work_dir,
            python=self._cfg.ssh_python,
            timeout=self._cfg.ssh_timeout_s,
            known_hosts=self._cfg.known_hosts,
            host_key_policy=self._cfg.host_key_policy,
        )
        self._notifier = Notifier(self._store, self.context, self._cfg.notify_interval)
        gate = MemoryGate(sp=shared_prefs)
        gate.attach_context(self.context)
        self._manager = WorkspaceManager(
            config=self._cfg,
            store=self._store,
            executor=executor,
            notifier=self._notifier,
            cm=getattr(self.context, "conversation_manager", None),
            memory_gate=gate,
            results_dir=data_dir / "results",
            sp=shared_prefs,
        )
        self._manager.native_guard = native_guard_status
        self._publish_handoff_gate()
        attach_service(self.context, TaskWorkspaceService(self._manager, self._cfg))
        self._publish_policy()
        set_active_plugin(self)
        self._notifier.start()
        if self._cfg.enabled:
            await self._manager.refresh_policy()
            await self._manager.resume_active_runs()
        logger.info("[task-workspace] 已初始化，enabled=%s", self._cfg.enabled)

    def _publish_handoff_gate(self) -> None:
        """Wire and publish the legacy-handoff gate before the facade exists.

        Bridge-first: an existing bridge-published state is preserved (already
        ready or pending). Workspace-first: when ``require_legacy_handoff`` is
        set, publish pending-required so a managed run cannot start before the
        bridge hands off; a late bridge updates the same Context attribute to
        ready. The gate reads readiness at execution time.
        """
        self._manager.handoff_gate = lambda: handoff_allows_execution(
            self.context, require=self._cfg.require_legacy_handoff)
        ensure_pending_handoff(
            self.context, required=self._cfg.require_legacy_handoff)

    def _publish_policy(self) -> None:
        """Expose the live binding policy on the shared Context.

        The staged LivingMemory passive-capture patch consults the same
        idempotent policy before scheduling capture, independent of filter
        priority order.
        """
        if self._manager is None:
            return
        try:
            setattr(self.context, POLICY_CONTEXT_ATTR, self._manager.policy)
        except Exception:
            pass

    async def terminate(self):
        # Cancel and await local drivers (never remote jobs) before closing the
        # notifier/store. The store must not transparently reopen after close.
        if self._manager is not None:
            await self._manager.shutdown()
        if self._notifier is not None:
            await self._notifier.stop()
        set_active_plugin(None)
        detach_service(self.context)
        if self._store is not None:
            self._store.close()
        logger.info("[task-workspace] 已停止")

    # ------------------------------------------------------------- commands
    @filter.command("task")
    async def task(self, event: AstrMessageEvent, sub: str = "", *, rest: GreedyStr):
        """任务工作区管理入口。

        工作区会话只用于「讨论需求 + 查看/消费结果」，其中不向模型暴露任何
        本机原生执行工具（shell/python/文件写入等），因此聊天不会绕过运行
        账本与单次权限去直接执行。真正的执行必须由用户显式发送
        ``/task run`` 触发：它走远端执行器、写入运行账本并受单次权限约束。

        ``rest`` is keyword-only and has no default so AstrBot's parser uses the
        ``GreedyStr`` annotation and preserves spaces in titles/amendments.
        """
        text = await self._dispatch(event, (sub or "").strip().lower(), str(rest or ""))
        # Yielding the result makes the respond stage send it and set
        # ``_has_send_oper``, which causes ProcessStage to skip the default LLM.
        yield event.plain_result(text)

    async def _dispatch(self, event, sub: str, rest: str) -> str:
        m = self._manager
        if m is None:
            return "任务工作区未初始化。"
        try:
            if sub == "new":
                ws = await m.create_workspace(event, rest)
                return (
                    f"已创建工作区「{ws.title}」{ws.short_id}。\n"
                    f"先在此会话描述需求，再 /task run 执行。"
                )
            if sub == "list":
                err = m.check_access(event)
                if err:
                    return err
                workspaces = await m.list_workspaces(event)
                if not workspaces:
                    return "暂无工作区，用 /task new <标题> 创建。"
                lines = []
                for w in workspaces[:30]:
                    mark = "（已归档）" if w.archived else ""
                    lines.append(f"{w.short_id} {w.title}{mark}")
                return "工作区：\n" + "\n".join(lines)
            if sub == "switch":
                ws = await m.switch(event, rest)
                return f"已切换到工作区 {ws.short_id}「{ws.title}」"
            if sub == "rename":
                title = rest.strip()
                if not title:
                    return "用法：/task rename <标题>"
                ws = await m.current_workspace(event)
                if ws is None:
                    return "当前会话不是任务工作区。"
                ws = await m.rename(event, ws.id, title)
                return f"已改名为「{ws.title}」"
            if sub in ("archive", "close"):
                key = rest.strip()
                if not key:
                    ws = await m.current_workspace(event)
                    if ws is None:
                        return "当前会话不是任务工作区，请提供工作区 ID。"
                    key = ws.id
                ws = await m.archive(event, key)
                return f"已归档工作区 {ws.short_id}（历史保留）。"
            if sub == "restore":
                ws = await m.restore(event, rest)
                return f"已恢复工作区 {ws.short_id}「{ws.title}」"
            if sub == "run":
                executor = rest.strip().lower() or self._cfg.default_executor
                if executor not in ("dsh", "codex"):
                    return "用法：/task run [dsh|codex]"
                run = await m.start_run(event, executor)
                return (
                    f"已排队 [{run.executor}] {run.short_id}，后台执行中，"
                    f"完成后会播报。可用 /task status 查看。"
                )
            if sub == "status":
                result = await m.status(event, rest.strip())
                if isinstance(result, list):
                    if not result:
                        return "当前工作区暂无任务。"
                    return "任务：\n" + "\n".join(self._fmt_run(r) for r in result[:20])
                return self._fmt_run(result)
            if sub == "result":
                run = await m.status(event, rest.strip())
                if isinstance(run, list):
                    if not run:
                        return "当前工作区暂无任务。"
                    run = run[0]
                return await self._render_result(m, run)
            if sub == "amend":
                await m.amend(event, rest)
                return "已记录补充要求，将在下次 /task run 生效（不会注入正在执行的任务）。"
            if sub == "cancel":
                run = await m.cancel(event, rest.strip())
                return f"已请求取消 {run.short_id}（收到远端确认后才算取消）。"
            if sub == "reconcile":
                run = await m.reconcile(event, rest.strip())
                return self._fmt_run(run)
            # Unknown / ``help`` / bare ``/task``: show usage plus the note that
            # managed chat only discusses and real execution uses /task run.
            return self._HELP_TEXT
        except AmbiguousIdError as e:
            return str(e)
        except AccessDeniedError as e:
            return str(e)
        except WorkspaceError as e:
            return str(e)
        except Exception as e:
            logger.warning("[task-workspace] 命令失败", exc_info=True)
            return f"操作失败：{e}"

    @staticmethod
    def _fmt_run(run) -> str:
        line = f"{run.short_id} [{run.executor}] {run.status}"
        if run.remote_task_id:
            line += f" remote={run.remote_task_id}"
        if run.connectivity_error:
            line += f" 连接异常={run.connectivity_error[:60]}"
        elif run.error:
            line += f" 错误={run.error[:80]}"
        return line

    async def _render_result(self, m, run) -> str:
        text = await m.read_result(run)
        if run.status != RunStatus.SUCCEEDED.value:
            head = f"任务 {run.short_id} 状态={run.status}"
            return head + (f"：{text[:500]}" if text else "")
        if not text:
            return f"任务 {run.short_id} 成功，但结果为空。"
        return f"[{run.short_id} 结果]\n{text}"

    # --------------------------------------------------------------- hooks
    BINDING_KEY = BINDING_EXTRA_KEY

    def _stamp_binding(self, event, *, umo, cid, ws_id, owner, managed) -> bool:
        """Stamp the immutable binding exactly once."""
        if get_binding(event) is not None:
            return False
        try:
            event.set_extra(self.BINDING_KEY, {
                "umo": umo, "cid": cid, "ws_id": ws_id,
                "owner": owner, "managed": bool(managed),
            })
            return True
        except Exception:
            return False

    def _native_guard_allows(self) -> bool:
        """Readiness of the loaded pre-capture guard (native chat gate).

        This is deliberately separate from ``require_native_guard`` (the remote
        execution gate): every managed *native* LLM request needs the guard
        loaded before it is allowed or stamped, whether or not an active native
        runner exists. A plugin-local constant or a staged-but-unapplied file
        is not evidence.
        """
        installed, version = native_guard_status()
        if not installed:
            logger.warning(
                "[task-workspace] 原生 follow-up 守卫未加载（版本=%s），"
                "保守拒绝本管理请求", version)
        return installed

    def _deny_managed_native_tools(self, req) -> list[str]:
        """Default-deny native tools on a valid managed chat request.

        Managed workspace chat is a discussion/consumption surface: it must not
        expose host shell / python / file-write tools that would bypass the run
        ledger and per-run permission. The real execution path is an explicit
        ``/task run``. The ``ToolSet`` object is mutated in place (identity
        preserved) so the runner reset coroutine -- which captures the same
        ``req`` and reads ``req.func_tool`` when awaited -- sees no native
        tools. The global persona/config is never touched.
        """
        removed = strip_managed_chat_tools(
            req, allowed=self._cfg.managed_chat_allowed_tools)
        if removed:
            logger.info(
                "[task-workspace] 管理会话默认拒绝原生工具：%s",
                ", ".join(removed))
        return removed

    def on_adapter_message(self, event) -> None:
        """Synchronous early guard run during AstrBot's WakingCheckStage.

        This executes before ``ProcessStage`` and therefore before native
        ``try_capture_follow_up``. It never wakes an ordinary message; the
        filter always returns ``False``. It (a) rejects text that a native
        runner bound to a *different* conversation would capture, and (b)
        stamps an immutable managed binding for authorized managed requests.
        Management/bridge commands are always allowed through.
        """
        m = self._manager
        if m is None or not self._cfg.enabled:
            return
        umo = str(getattr(event, "unified_msg_origin", "") or "")
        if not umo:
            return
        sender = m._sender(event)
        cur_cid = m.policy.current_cid(umo)
        is_command = looks_like_command(event)

        # (a) Cross-conversation native runner guard.
        runner_binding = active_runner_binding(umo)
        if runner_binding and runner_binding.get("managed") and not is_command:
            # The synchronous pre-capture guard must actually be loaded in the
            # running core; without it the TOCTOU window after async
            # preprocessing is open, so fail closed for managed native ingress.
            if not self._native_guard_allows():
                event.stop_event()
                return
            incoming_same = (
                runner_binding.get("cid") == cur_cid
                and runner_binding.get("owner") == sender
                and m.policy.is_managed_cid(cur_cid)
            )
            if not incoming_same:
                # Let native UMO lock / request-CID guard handle normal flow;
                # we stop this capture instead of forwarding to the wrong run.
                event.stop_event()
                return

        kind = m.classify(event, cur_cid)
        # (b) EVERY managed native request needs the loaded guard before it may
        # be allowed or stamped for the LLM -- not only requests that happen to
        # have an active native runner. Otherwise the first managed chat with an
        # empty active-runner registry bypasses the guard. Management/bridge
        # commands stay available without it.
        if kind in ("managed", "unknown_managed") and not is_command:
            if not self._native_guard_allows():
                event.stop_event()
                return
        if kind == "ordinary":
            self._stamp_binding(event, umo=umo, cid=cur_cid, ws_id="",
                                owner="", managed=False)
            return
        if kind == "unknown_managed":
            # Known managed UMO but no hydrated binding for this conversation.
            if not is_command:
                event.stop_event()
            return

        ws = m.policy.workspace_for_cid(cur_cid) or {}
        if (not sender or not ws.get("owner") or ws.get("owner") != sender):
            if not is_command:
                event.stop_event()
            return
        if not m.memory_gate.detect().available:
            if not is_command:
                event.stop_event()
            return
        if not MemoryGate.stamp(event):
            if not is_command:
                event.stop_event()
            return
        self._stamp_binding(event, umo=umo, cid=cur_cid,
                            ws_id=ws.get("ws_id", ""), owner=sender, managed=True)

    @filter.custom_filter(TaskWorkspaceBindingFilter, False, priority=100)
    async def _workspace_binding_filter(self, event):
        # The filter performs the side effects synchronously and always returns
        # False, so this handler is inert (mirrors LivingMemory's passive filter).
        return

    @filter.on_waiting_llm_request(priority=100)
    async def _bind_request(self, event):
        """Fail closed for managed requests after native follow-up capture.

        Native ``try_capture_follow_up`` already ran; cross-conversation capture
        is prevented by the early filter plus the staged core guard. Here we
        ensure a managed request either carries a valid isolated binding or is
        stopped before any memory hook or result injection can run.
        """
        if not self._cfg.enabled or self._manager is None:
            return
        umo = str(getattr(event, "unified_msg_origin", "") or "")
        if not self._cfg.session_allowed(umo):
            return
        kind = self._manager.classify(event, self._manager.managed_cid(event))
        if kind == "ordinary":
            return
        # Defense in depth: a managed native request must carry the loaded
        # pre-capture guard, exactly like the synchronous early filter.
        if not looks_like_command(event) and not self._native_guard_allows():
            event.stop_event()
            return
        binding = get_binding(event)
        if binding and binding.get("managed"):
            if not self._manager.memory_gate.detect().available:
                logger.warning("[task-workspace] 管理会话缺少记忆隔离能力，封锁本次 LLM 请求")
                event.stop_event()
                return
            if not MemoryGate.stamp(event):
                logger.warning("[task-workspace] 记忆隔离标记失败，封锁本次 LLM 请求")
                event.stop_event()
                return
            return
        # Managed/unknown but no valid binding: fail closed.
        if not self._manager.memory_gate.detect().available:
            logger.warning("[task-workspace] 管理会话缺少记忆隔离能力，封锁本次 LLM 请求")
            event.stop_event()
            return
        ws = await self._manager.current_workspace(event)
        sender = self._manager._sender(event)
        if (ws is None or not sender or not ws.owner_user_id
                or ws.owner_user_id != sender
                or ws.umo != umo):
            logger.warning("[task-workspace] 管理请求缺少有效归属绑定，封锁本次 LLM 请求")
            event.stop_event()
            return
        conv = None
        try:
            conv = await self._manager.cm.get_conversation(umo, ws.conversation_id)
        except Exception:
            conv = None
        if conv is None or str(getattr(conv, "user_id", "") or "") not in ("", umo):
            logger.warning("[task-workspace] 对话归属校验失败，封锁本次 LLM 请求")
            event.stop_event()
            return
        if not MemoryGate.stamp(event):
            event.stop_event()
            return
        self._stamp_binding(event, umo=umo, cid=ws.conversation_id,
                            ws_id=ws.id, owner=sender, managed=True)

    @filter.on_llm_request(priority=100)
    async def _surface_result(self, event, req):
        """Default-deny native tools and inject results for a managed request.

        For a valid managed native discussion request this runs after
        ``build_main_agent`` assembled ``req.func_tool`` but before the stage
        awaits the runner reset coroutine, so it is the last point at which the
        model's tool set can be emptied without touching the host core.
        """
        if not self._cfg.enabled or self._manager is None:
            return
        binding = get_binding(event)
        req_cid = ""
        try:
            conv = getattr(req, "conversation", None)
            req_cid = str(getattr(conv, "cid", "") or "")
        except Exception:
            req_cid = ""
        if binding is None or not binding.get("managed"):
            # Classification vs authorization: a managed request without a
            # valid binding must fail closed, never fall through as ordinary.
            if self._manager.classify(event, req_cid) in ("managed", "unknown_managed"):
                logger.warning("[task-workspace] 管理请求缺少冻结绑定，封锁本次 LLM 请求")
                event.stop_event()
            return
        # Defense in depth: never inject a managed result without the loaded
        # pre-capture guard in the running core.
        if not self._native_guard_allows():
            event.stop_event()
            return
        if not self._manager.memory_gate.detect().available:
            event.stop_event()
            return
        if not req_cid or req_cid != binding.get("cid"):
            logger.warning("[task-workspace] 请求会话与冻结绑定不一致，封锁结果注入")
            event.stop_event()
            return
        ws = await self._manager.store.get_workspace(binding.get("ws_id", ""))
        sender = self._manager._sender(event)
        if (ws is None or ws.conversation_id != req_cid
                or not ws.owner_user_id or ws.owner_user_id != sender
                or ws.umo != str(event.unified_msg_origin)):
            event.stop_event()
            return
        # This is a valid managed native discussion request. Strip host
        # execution tools before the stage awaits the runner reset coroutine
        # (build_main_agent was called with apply_reset=False, so the model has
        # not been invoked yet) and before any result injection. Ordinary daily
        # requests never reach this branch.
        self._deny_managed_native_tools(req)
        try:
            run_id, surface = await self._manager.surface_pending_result(
                event, workspace=ws, expected_cid=req_cid)
        except Exception:
            logger.warning("[task-workspace] 结果注入失败", exc_info=True)
            return
        if not surface:
            return
        try:
            contexts = getattr(req, "contexts", None)
            if contexts is None:
                contexts = []
                req.contexts = contexts
            contexts.append({"role": "user", "content": surface})
        except Exception:
            logger.warning("[task-workspace] 无法写入 req.contexts", exc_info=True)
            return
        # Acknowledge the exact injected run so a concurrent completion cannot be
        # marked surfaced by a separate lookup.
        await self._manager.mark_result_surfaced(
            event, workspace=ws, expected_cid=req_cid, run_id=run_id)

    @filter.on_plugin_loaded()
    async def _on_plugin_loaded(self, metadata):
        name = getattr(metadata, "name", "") or ""
        if "livingmemory" in name.lower() or name == "LivingMemory":
            if self._manager is not None:
                self._manager.memory_gate.invalidate()

    @filter.on_plugin_unloaded()
    async def _on_plugin_unloaded(self, metadata):
        name = getattr(metadata, "name", "") or ""
        if "livingmemory" in name.lower() or name == "LivingMemory":
            if self._manager is not None:
                self._manager.memory_gate.invalidate()

    async def import_legacy(self, path, apply: bool = False, owner_map: dict | None = None,
                            selected_quarantine: dict | None = None):
        """Import the legacy bridge task table.

        ``owner_map`` maps a group/unknown UMO to a verified owner and is
        required for any non-private UMO shape; it is never guessed from a
        group id. ``selected_quarantine`` maps an explicit task id to
        ``{"reason": ..., "evidence": ...}`` for admin-selected historical rows
        that must be excluded from active polling without deleting history.
        """
        importer = LegacyImporter(
            self._store, getattr(self.context, "conversation_manager", None),
            owner_map=owner_map, selected_quarantine=selected_quarantine)
        report = await importer.dry_run(path)
        if apply:
            await importer.apply(report)
            await importer.quarantine_report(report)
            if self._manager is not None:
                await self._manager.refresh_policy()
        return report


def register(context: Context, config=None) -> TaskWorkspace:
    return TaskWorkspace(context, config)
