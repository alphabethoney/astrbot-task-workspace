# -*- coding: utf-8 -*-
"""Compatibility surface for the existing bridge plugin.

The task-workspace plugin attaches a service facade to the shared AstrBot
``Context`` object. The staged bridge plugin consults it when its
``use_task_workspace`` flag is enabled. If the flag is on but the service is
absent, the bridge must return an explicit error rather than silently falling
back to the legacy unsafe path.
"""
from __future__ import annotations

SERVICE_ATTR = "_task_workspace_service"
FLAG_NAME = "use_task_workspace"
HANDOFF_ATTR = "_task_workspace_handoff"


class HandoffState:
    """Shared handoff readiness published by the legacy bridge.

    The workspace plugin gates *new* execution on this when a legacy source is
    configured, so a load-order race can never expose ``/task run`` before the
    old poller handed off (or while it still owns active jobs).
    """

    def __init__(self, *, required=False, ready=False, verified=False,
                 reason="", source=""):
        self.required = bool(required)
        self.ready = bool(ready)
        self.verified = bool(verified)
        self.reason = reason
        self.source = source

    def __repr__(self):
        return (f"HandoffState(required={self.required}, ready={self.ready}, "
                f"verified={self.verified}, reason={self.reason!r})")


def attach_handoff(context, state: HandoffState) -> None:
    try:
        setattr(context, HANDOFF_ATTR, state)
    except Exception:
        pass


def get_handoff(context):
    return getattr(context, HANDOFF_ATTR, None)


def handoff_allows_execution(context, *, require=False) -> tuple[bool, str]:
    """Return ``(allowed, reason)`` for new managed execution.

    ``require`` comes from the workspace's ``require_legacy_handoff`` config
    (survives a workspace-first boot where the bridge has not published any
    state yet): when set, a *missing* state denies instead of allowing. The
    presence of a ready + verified state is what permits execution; a
    required-but-unready/unverified handoff always denies.
    """
    state = get_handoff(context)
    if state is None:
        if require:
            return False, "旧桥交接状态缺失，拒绝新执行（require_legacy_handoff）。"
        return True, ""
    if not (require or getattr(state, "required", False)):
        return True, ""
    if getattr(state, "ready", False) and getattr(state, "verified", False):
        return True, ""
    reason = getattr(state, "reason", "") or "旧桥轮询权尚未成功交接，拒绝新执行。"
    return False, reason


def ensure_pending_handoff(context, *, required=False, reason="") -> None:
    """Publish a pending legacy-handoff gate before the facade is exposed.

    Bridge-first: an existing bridge-published state (ready or pending) is
    preserved. Workspace-first: when ``required`` is set, publish pending-
    required so a late bridge can update readiness but execution cannot start
    before it does.
    """
    if not required:
        return
    if get_handoff(context) is not None:
        return
    attach_handoff(context, HandoffState(
        required=True, ready=False, verified=False,
        reason=reason or "等待旧桥插件初始化并完成轮询权交接。",
        source="task_workspace"))


class TaskWorkspaceService:
    def __init__(self, manager, config):
        self.manager = manager
        self.config = config

    @property
    def available(self) -> bool:
        return self.config.enabled

    async def route(self, command: str, event, args: str = "") -> str:
        m = self.manager
        try:
            if command in ("wg", "wc"):
                run = await m.start_run(event, "dsh" if command == "wg" else "codex", args)
                return f"已提交 [{run.executor}] {run.short_id}，稍后播报进度"
            if command == "nt":
                prompt = (args or "").strip()
                if not prompt:
                    return "用法：/nt <任务描述>"
                ws = await m.create_workspace(event, prompt[:20])
                run = await m.start_run(event, self.config.default_executor, prompt,
                                        workspace=ws)
                return f"已建工作区「{ws.title}」{ws.short_id}，任务 {run.short_id} 已排队"
            if command == "ww":
                result = await m.status(event, args)
                if isinstance(result, list):
                    return _format_status_list(result)
                return _format_status(result)
            if command == "wl":
                return await self._list_runs(event)
            if command == "wx":
                run = await m.cancel(event, args)
                return f"已请求取消 {run.short_id}"
            if command == "switch":
                if not (args or "").strip():
                    return await self._list_workspaces(event)
                ws = await m.switch(event, args)
                return f"已切换到工作区 {ws.short_id}「{ws.title}」"
            if command == "tasks":
                return await self._list_workspaces(event)
            if command == "close":
                key = (args or "").strip()
                if not key:
                    ws = await m.current_workspace(event)
                    if ws is None:
                        return "当前会话不是任务工作区，请提供工作区 ID。"
                    key = ws.id
                ws = await m.archive(event, key)
                return f"已归档工作区 {ws.short_id}"
            if command == "rename":
                ws = await m.current_workspace(event)
                if ws is None:
                    return "当前会话不是任务工作区。"
                ws = await m.rename(event, ws.id, args)
                return f"已改名为「{ws.title}」"
            if command == "wd":
                return await self._dashboard(event)
        except Exception as e:
            return f"任务工作区操作失败：{e}"
        return f"未知的兼容命令：{command}"

    async def _list_workspaces(self, event) -> str:
        err = self.manager.check_access(event)
        if err:
            return err
        workspaces = await self.manager.list_workspaces(event)
        if not workspaces:
            return "暂无任务工作区，用 /task new 建一个"
        lines = []
        for w in workspaces[:20]:
            flag = "（已归档）" if w.archived else ""
            lines.append(f"{w.short_id} {w.title}{flag}")
        return "任务工作区：\n" + "\n".join(lines)

    async def _list_runs(self, event) -> str:
        err = self.manager.check_access(event)
        if err:
            return err
        ws = await self.manager.current_workspace(event)
        if ws is None:
            return "当前会话不是任务工作区。"
        runs = await self.manager.status(event)
        if not runs:
            return "当前工作区暂无任务。"
        return "任务列表（当前工作区）：\n" + "\n".join(_format_status(r) for r in runs[:20])

    async def _dashboard(self, event) -> str:
        err = self.manager.check_access(event)
        if err:
            return err
        active = await self.manager.store.active_runs()
        return f"进行中任务：{len(active)}（全局上限 {self.config.max_global_runs}）"


def manager_umo(event) -> str:
    return str(getattr(event, "unified_msg_origin", "") or "")


def _format_status(run) -> str:
    tail = run.status
    if run.connectivity_error:
        tail += f" (连接异常: {run.connectivity_error[:60]})"
    return f"{run.short_id} [{run.executor}] {tail} {run.remote_task_id}"


def _format_status_list(runs) -> str:
    if not runs:
        return "当前工作区暂无任务。"
    return "\n".join(_format_status(r) for r in runs[:20])


def attach_service(context, service: TaskWorkspaceService) -> None:
    try:
        setattr(context, SERVICE_ATTR, service)
    except Exception:
        pass


def detach_service(context) -> None:
    try:
        delattr(context, SERVICE_ATTR)
    except Exception:
        pass


def get_service(context):
    return getattr(context, SERVICE_ATTR, None)
