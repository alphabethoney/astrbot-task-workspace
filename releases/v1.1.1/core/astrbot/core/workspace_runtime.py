"""Versioned window binding contract; message delivery UMO is never rewritten."""
from __future__ import annotations

import hashlib
import json

RUNTIME_VERSION = 2
BINDING_KEY = "_task_workspace_binding"


def binding(event):
    raw = event.get_extra(BINDING_KEY)
    if not isinstance(raw, dict) or not raw.get("managed"):
        return None
    if (any(not raw.get(k) for k in ("umo", "owner", "cid", "ws_id", "environment"))
            or raw["environment"] not in ("linux_local", "windows_remote")
            or raw["umo"] != event.unified_msg_origin
            or raw["owner"] != event.get_sender_id()):
        raise RuntimeError("无效的工作区绑定")
    return raw


def execution_key(event):
    b = binding(event)
    if not b:
        return event.unified_msg_origin
    identity = [b[k] for k in ("umo", "owner", "cid", "ws_id", "environment")]
    digest = hashlib.sha256(json.dumps(identity, ensure_ascii=False).encode()).hexdigest()
    return "task-window:" + digest


def process_key(event):
    key = execution_key(event)
    if binding(event):
        rid = event.get_extra("_task_workspace_native_id")
        if not rid:
            raise RuntimeError("本机执行记录缺失")
        key += ":" + rid
    return key


def native_service(context):
    facade = getattr(context, "_task_workspace_service", None)
    service = getattr(facade, "native", None)
    if service is None or not facade.available:
        raise RuntimeError("任务工作区服务未就绪，拒绝执行")
    return service


async def authorize_tool(context, event, name):
    b = binding(event)
    if b:
        await native_service(context).validate(event, tool=name)
        return
    # A managed conversation missing its stamp must never fall back to the
    # ordinary permissive route (e.g. hot unload or inconsistent middleware).
    policy = getattr(context, "_task_workspace_binding_policy", None)
    if policy is not None:
        cid = policy.current_cid(event.unified_msg_origin)
        if policy.is_managed_cid(cid) or (not cid and policy.is_managed_umo(event.unified_msg_origin)):
            raise RuntimeError("管理会话缺少执行绑定，拒绝调用工具")


async def refuse_busy_reload(context, plugin_name=None):
    if plugin_name not in (None, "astrbot_plugin_task_workspace", "astrbot_plugin_livingmemory", "LivingMemory"):
        return
    facade = getattr(context, "_task_workspace_service", None)
    service = getattr(facade, "native", None)
    from astrbot.core.pipeline.process_stage.follow_up import _ACTIVE_AGENT_RUNNERS
    active_native = False
    for runner in list(_ACTIVE_AGENT_RUNNERS.values()):
        ev = getattr(getattr(runner.run_context, "context", None), "event", None)
        b = binding(ev) if ev is not None else None
        if b and b["environment"] == "linux_local":
            active_native = True
            break
    if active_native or (service is not None and await service.busy()):
        raise RuntimeError("Linux 窗口仍有活跃任务或后台进程，请先结束任务再重载相关插件")


def display_prefix(event):
    b = binding(event)
    if not b:
        return ""
    label = "Linux" if b["environment"] == "linux_local" else "Windows"
    rid = event.get_extra("_task_workspace_native_id") or "chat"
    return f"[{label} · {b.get('title', b['ws_id'])} · {rid[:14]}] "
