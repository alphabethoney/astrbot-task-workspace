# -*- coding: utf-8 -*-
"""桥服务器端插件主入口。

把聊天指令接到远端 Windows 桥（runner.py）：
  /wg <描述>    提交给 dsh
  /wc <描述>    提交给 codex
  /wl           列出远端任务
  /ww [task_id] 查看进度（缺省取本会话最近一个任务）
  /wx <task_id> 取消任务

指令会做会话白名单校验；任务提交后由后台轮询远端事件，翻译成人话并按限频播报。
"""
import asyncio
import time
import types
import uuid

from astrbot.api import logger
from astrbot.api.event import MessageChain
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star

from .announcer import Announcer
from .config import BridgeConfig
from .dashboard import Dashboard
from .ssh_dispatch import SshDispatcher, SshError
from .task_table import (
    HANDOFF_VERSION,
    HandoffError,
    TaskTable,
    TERMINAL,
)

HANDOFF_ATTR = "_task_workspace_handoff"


class BridgeServer(Star):
    def __init__(self, context: Context, config=None):
        super().__init__(context)
        self._cfg = BridgeConfig(config)
        self.dispatch = SshDispatcher(
            host=self._cfg.ssh_host,
            port=self._cfg.ssh_port,
            user=self._cfg.ssh_user,
            work_dir=self._cfg.work_dir,
            python=self._cfg.python,
            timeout=self._cfg.ssh_timeout_s,
        )
        self.table = TaskTable()
        self.dashboard = Dashboard(self._cfg.snapshot_path, self._cfg.stale_seconds)
        self.announcer = Announcer(
            interval=self._cfg.announce_interval,
            max_len=self._cfg.max_text_len,
            verbose=self._cfg.verbose,
        )
        self._poll_task = None
        self._managed = self.table.managed()

    # ---------------------------------------------------------------- 生命周期
    async def initialize(self):
        if not self._cfg.ssh_host:
            logger.warning("[桥服务] 未配置目标主机，轮询不会工作")

        # Managed mode: exactly one poll owner. The legacy JSON poller must be
        # disabled before the task-workspace service adopts any run.
        if self._cfg.use_task_workspace:
            # Evaluate the *persisted* handoff INDEPENDENTLY of whether the
            # workspace service has loaded yet. A durable, readback-verified
            # handoff with no active unquarantined rows is evidence the old
            # poller already handed off, so bridge-first boot publishes
            # readiness instead of wedging on a not-yet-loaded service.
            persisted_ready = (
                self.table.managed()
                and self.table.handoff_ok()
                and not self.table.active()
            )
            if persisted_ready:
                self._managed = True
                self._publish_handoff(
                    required=True, ready=True, verified=True, reason="")
                logger.info(
                    "[桥服务] 检测到已持久化且回读验证的交接，托管模式生效（无需服务门面）。")
                return

            service = self._bridge_service()
            if service is None or not getattr(service, "available", False):
                logger.warning(
                    "[桥服务] 托管模式已开启但任务工作区服务不可用；"
                    "旧轮询保持关闭（fail-closed），旧指令一律拒绝。")
                self._managed = True
                self._publish_handoff(
                    required=True, ready=False, verified=False,
                    reason="任务工作区服务不可用，旧路径拒绝且新执行被阻断。")
                return
            active = self.table.active()
            if active:
                logger.error(
                    "[桥服务] 托管模式检测到 %d 个未排空的旧任务；"
                    "拒绝启用托管轮询，需先排空或执行显式交接。", len(active))
                self._managed = False
                self._publish_handoff(
                    required=True, ready=False, verified=False,
                    reason="存在未排空的旧任务，交接未完成，拒绝新执行。")
                return
            # Handoff recorded *before* the old poller would ever start, and the
            # readback is verified: a swallowed write can no longer "succeed".
            try:
                self.table.record_handoff({
                    "source": "legacy_bridge",
                    "service": "task_workspace",
                    "reason": "no_active_legacy_jobs",
                })
            except HandoffError as e:
                logger.error("[桥服务] 交接标记持久化/回读失败：%s", e)
                self._managed = False
                self._publish_handoff(
                    required=True, ready=False, verified=False,
                    reason=f"交接标记无法持久化：{e}")
                return
            self._managed = True
            self._publish_handoff(required=True, ready=True, verified=True,
                                  reason="")
            logger.info("[桥服务] 托管模式：已禁用旧 JSON 轮询，轮询权移交任务工作区。")
            return

        # A previous handoff must not be silently reverted.
        if self.table.managed():
            self._managed = True
            self._publish_handoff(
                required=True, ready=self.table.handoff_ok(),
                verified=self.table.handoff_ok(),
                reason="" if self.table.handoff_ok() else "交接标记未经验证。")
            logger.warning("[桥服务] 轮询权已移交任务工作区，旧轮询保持关闭。")
            return

        self._managed = False
        self._publish_handoff(required=False, ready=True, verified=True, reason="")
        self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info("[桥服务] 已启动后台轮询，间隔 %d 秒", self._cfg.poll_interval)

    def _publish_handoff(self, *, required, ready, verified, reason=""):
        """Publish the handoff readiness on the shared AstrBot Context."""
        try:
            setattr(self.context, HANDOFF_ATTR, types.SimpleNamespace(
                required=required, ready=ready, verified=verified,
                reason=reason, source="legacy_bridge",
                version=HANDOFF_VERSION))
        except Exception:
            logger.warning("[桥服务] 无法发布交接状态到 Context", exc_info=True)

    async def terminate(self):
        if self._poll_task:
            self._poll_task.cancel()
            self._poll_task = None

    # ---------------------------------------------------------------- 兼容路由
    def _bridge_service(self):
        """取任务工作区服务门面；不存在则 None。"""
        return getattr(self.context, "_task_workspace_service", None)

    async def _compat(self, event, command, message=""):
        """当 use_task_workspace 打开时，把旧指令路由到任务工作区服务。

        fail-closed：服务缺失时返回显式错误，绝不回退到旧的不安全路径。
        并且当配置了旧桥来源时，必须先有持久化且回读验证过的交接，否则拒绝。
        """
        if not (self._cfg.use_task_workspace
                or (getattr(self, "table", None) is not None and self.table.managed())):
            return None
        table = getattr(self, "table", None)
        if table is not None and self._cfg.use_task_workspace:
            # Both legacy routing and new execution stay blocked until the old
            # rows are drained and the handoff marker is durably verified.
            if table.active() or not table.handoff_ok():
                return "旧任务尚未排空或交接未完成，已拒绝旧路径与新执行（fail-closed）。"
        service = self._bridge_service()
        if service is None or not getattr(service, "available", False):
            return "任务工作区服务不可用（插件未启用或未加载），已拒绝旧路径回退。"
        return await service.route(command, event, message or "")

    # ---------------------------------------------------------------- 指令
    @filter.command("wg")
    async def wg(self, event: AstrMessageEvent, message: str = ""):
        """提交任务给 dsh。"""
        if (r := await self._compat(event, "wg", message)) is not None:
            yield event.plain_result(r)
            return
        yield event.plain_result(await self._submit(event, "dsh", message))

    @filter.command("wc")
    async def wc(self, event: AstrMessageEvent, message: str = ""):
        """提交任务给 codex。"""
        if (r := await self._compat(event, "wc", message)) is not None:
            yield event.plain_result(r)
            return
        yield event.plain_result(await self._submit(event, "codex", message))

    @filter.command("wl")
    async def wl(self, event: AstrMessageEvent):
        """列出远端任务。"""
        if (r := await self._compat(event, "wl", "")) is not None:
            yield event.plain_result(r)
            return
        if denied := self._session_denied(event):
            yield event.plain_result(denied)
            return
        yield event.plain_result(await self._list_tasks(event))

    @filter.command("ww")
    async def ww(self, event: AstrMessageEvent, message: str = ""):
        """查看任务进度。"""
        if (r := await self._compat(event, "ww", message)) is not None:
            yield event.plain_result(r)
            return
        if denied := self._session_denied(event):
            yield event.plain_result(denied)
            return
        yield event.plain_result(await self._watch_progress(event, message))

    @filter.command("wx")
    async def wx(self, event: AstrMessageEvent, message: str = ""):
        """取消任务。"""
        if (r := await self._compat(event, "wx", message)) is not None:
            yield event.plain_result(r)
            return
        if denied := self._session_denied(event):
            yield event.plain_result(denied)
            return
        yield event.plain_result(await self._cancel(event, message))

    @filter.command("wd")
    async def wd(self, event: AstrMessageEvent):
        """任务仪表盘（纯本地读快照，不发起 SSH）。"""
        if (r := await self._compat(event, "wd", "")) is not None:
            yield event.plain_result(r)
            return
        if denied := self._session_denied(event):
            yield event.plain_result(denied)
            return
        yield event.plain_result(self.dashboard.render())

    @filter.command("nt")
    async def nt(self, event: AstrMessageEvent, message: str = ""):
        """新建任务会话并提交桥任务。"""
        if (r := await self._compat(event, "nt", message)) is not None:
            yield event.plain_result(r)
            return
        if denied := self._session_denied(event):
            yield event.plain_result(denied)
            return
        prompt = (message or "").strip()
        if not prompt:
            yield event.plain_result("用法：/nt <任务描述>")
            return
        umo = self._session(event)
        cm = getattr(self.context, "conversation_manager", None)
        if cm is None:
            yield event.plain_result("conversation 管理器不可用")
            return
        try:
            cid = await cm.new_conversation(umo, event.get_platform_id(), persona_id="运维")
            title = prompt[:12]
            await cm.update_conversation_title(umo, title, conversation_id=cid)
        except Exception as e:
            yield event.plain_result(f"建任务会话失败：{e}")
            return
        request_id = "bridge:" + uuid.uuid4().hex
        req = {
            "request_id": request_id,
            "agent": "dsh",
            "cwd": self._cfg.work_dir,
            "prompt": prompt,
            "permission": self._cfg.default_permission,
            "timeout_s": self._cfg.default_timeout_s,
        }
        try:
            task_id = await self.dispatch.submit(req)
        except SshError as e:
            yield event.plain_result(f"提交失败：{e}")
            return
        except Exception as e:
            yield event.plain_result(f"提交异常：{e}")
            return
        if not task_id:
            yield event.plain_result("提交失败：未拿到 task_id")
            return
        self.table.add(task_id, "dsh", prompt, umo, conversation_id=cid,
                       request_id=request_id)
        yield event.plain_result(f"已建任务「{title}」#{cid[:4]}，task {task_id} 已提交")

    @filter.command("switch")
    async def switch(self, event: AstrMessageEvent, message: str = ""):
        """切换任务会话。"""
        if (r := await self._compat(event, "switch", message)) is not None:
            yield event.plain_result(r)
            return
        if denied := self._session_denied(event):
            yield event.plain_result(denied)
            return
        umo = self._session(event)
        key = (message or "").strip()
        if not key:
            yield event.plain_result(await self._list_conversations(umo))
            return
        cid = await self._resolve_cid(umo, key)
        if not cid:
            yield event.plain_result(f"找不到任务「{key}」，用 /tasks 看看")
            return
        cm = getattr(self.context, "conversation_manager", None)
        if cm is None:
            yield event.plain_result("conversation 管理器不可用")
            return
        try:
            await cm.switch_conversation(umo, cid)
        except Exception as e:
            yield event.plain_result(f"切换失败：{e}")
            return
        yield event.plain_result(f"已切换到任务 #{cid[:4]}")

    @filter.command("tasks")
    async def tasks(self, event: AstrMessageEvent):
        """列出本会话的任务会话。"""
        if (r := await self._compat(event, "tasks", "")) is not None:
            yield event.plain_result(r)
            return
        if denied := self._session_denied(event):
            yield event.plain_result(denied)
            return
        yield event.plain_result(await self._list_conversations(self._session(event)))

    @filter.command("close")
    async def close(self, event: AstrMessageEvent, message: str = ""):
        """关闭任务会话。"""
        if (r := await self._compat(event, "close", message)) is not None:
            yield event.plain_result(r)
            return
        if denied := self._session_denied(event):
            yield event.plain_result(denied)
            return
        umo = self._session(event)
        key = (message or "").strip()
        cm = getattr(self.context, "conversation_manager", None)
        if cm is None:
            yield event.plain_result("conversation 管理器不可用")
            return
        cid = await self._resolve_cid(umo, key) if key else await self._cur_cid(event)
        if not cid:
            yield event.plain_result("找不到要关闭的任务会话")
            return
        try:
            await cm.delete_conversation(umo, cid)
        except Exception as e:
            yield event.plain_result(f"关闭失败：{e}")
            return
        yield event.plain_result(f"已关闭任务会话 #{cid[:4]}")

    @filter.command("rename")
    async def rename(self, event: AstrMessageEvent, message: str = ""):
        """改名当前任务会话。"""
        if (r := await self._compat(event, "rename", message)) is not None:
            yield event.plain_result(r)
            return
        if denied := self._session_denied(event):
            yield event.plain_result(denied)
            return
        title = (message or "").strip()
        if not title:
            yield event.plain_result("用法：/rename <新名>")
            return
        umo = self._session(event)
        cid = await self._cur_cid(event)
        if not cid:
            yield event.plain_result("当前没有任务会话")
            return
        cm = getattr(self.context, "conversation_manager", None)
        if cm is None:
            yield event.plain_result("conversation 管理器不可用")
            return
        try:
            await cm.update_conversation_title(umo, title, conversation_id=cid)
        except Exception as e:
            yield event.plain_result(f"改名失败：{e}")
            return
        yield event.plain_result(f"已改名为「{title}」")

    # ---------------------------------------------------------------- 实现
    @staticmethod
    def _session(event) -> str:
        return str(getattr(event, "unified_msg_origin", "") or "")

    def _session_denied(self, event) -> str:
        """会话白名单校验，未授权返回提示串，否则返回空串。"""
        if not self._cfg.session_allowed(self._session(event)):
            return "该会话未被授权使用桥"
        return ""

    async def _cur_cid(self, event) -> str:
        """取当前对话（conversation）ID，任务按对话隔离。"""
        try:
            cm = getattr(self.context, "conversation_manager", None)
            if cm is None:
                return ""
            return (await cm.get_curr_conversation_id(self._session(event))) or ""
        except Exception:
            return ""

    async def _resolve_cid(self, umo: str, key: str) -> str:
        """按对话 id 前缀或标题匹配一个 conversation_id。"""
        try:
            cm = getattr(self.context, "conversation_manager", None)
            if cm is None:
                return ""
            convs = await cm.get_conversations(umo)
        except Exception:
            return ""
        key = key.strip().lower()
        for c in convs:
            cid = getattr(c, "cid", "") or ""
            title = (getattr(c, "title", "") or "").lower()
            if cid.startswith(key) or key in title:
                return cid
        return ""

    async def _list_conversations(self, umo: str) -> str:
        """列出本会话的任务对话。"""
        try:
            cm = getattr(self.context, "conversation_manager", None)
            if cm is None:
                return "conversation 管理器不可用"
            convs = await cm.get_conversations(umo)
        except Exception as e:
            return f"获取任务列表失败：{e}"
        if not convs:
            return "暂无任务会话，用 /nt 建一个"
        lines = []
        for c in convs[:20]:
            cid = getattr(c, "cid", "") or ""
            title = getattr(c, "title", "") or "(无标题)"
            lines.append(f"#{cid[:4]} {title}")
        return "任务会话：\n" + "\n".join(lines)

    async def _submit(self, event, agent, message):
        session = self._session(event)
        if not self._cfg.session_allowed(session):
            return "该会话未被授权使用桥"
        prompt = (message or "").strip()
        if not prompt:
            return f"用法：/{'wg' if agent == 'dsh' else 'wc'} <任务描述>"
        request_id = "bridge:" + uuid.uuid4().hex
        req = {
            "request_id": request_id,
            "agent": agent,
            "cwd": self._cfg.work_dir,
            "prompt": prompt,
            "permission": self._cfg.default_permission,
            "timeout_s": self._cfg.default_timeout_s,
        }
        try:
            task_id = await self.dispatch.submit(req)
        except SshError as e:
            logger.warning("[桥服务] submit 失败：%s", e)
            return f"提交失败：{e}"
        except Exception as e:
            logger.warning("[桥服务] submit 异常", exc_info=True)
            return f"提交异常：{e}"
        if not task_id:
            return "提交失败：未拿到 task_id"
        cid = await self._cur_cid(event)
        self.table.add(task_id, agent, prompt, session, conversation_id=cid,
                       request_id=request_id)
        return f"已提交 [{agent}] {task_id}，稍后播报进度"

    async def _list_tasks(self, event):
        cid = await self._cur_cid(event)
        recs = self.table.conversation_tasks(cid)
        if not recs:
            return "当前任务会话暂无任务，用 /nt 建一个"
        lines = []
        for r in recs[:20]:
            lines.append(f"{r.task_id} [{r.agent}] {r.status} {(r.prompt or '')[:40]}")
        return "任务列表（当前会话）：\n" + "\n".join(lines)

    async def _watch_progress(self, event, message):
        cid = await self._cur_cid(event)
        tid = (message or "").strip()
        if tid:
            rec = self.table.get(tid)
            if rec is None:
                return "找不到该任务"
            if rec.conversation_id and rec.conversation_id != cid:
                return "该任务不属于当前任务会话"
        else:
            rec = self.table.recent_for_conversation(cid)
            if rec is None:
                return "当前任务会话没有任务，用 /nt 建一个"
        await self._poll_one(rec)
        return (
            f"任务 {rec.task_id} [{rec.agent}] 状态：{rec.status}，"
            f"已读到 seq {rec.last_seq}。新的进度事件会直接发到本会话。"
        )

    async def _cancel(self, event, message):
        tid = (message or "").strip()
        if not tid:
            return "用法：/wx <task_id>"
        cid = await self._cur_cid(event)
        rec = self.table.get(tid)
        if rec is None:
            return "找不到该任务"
        if rec.conversation_id and rec.conversation_id != cid:
            return "该任务不属于当前任务会话"
        try:
            await self.dispatch.cancel(tid)
        except Exception as e:
            logger.warning("[桥服务] cancel 失败：%s", e)
            return f"取消失败：{e}"
        self.table.set_status(tid, "cancelled")
        return f"已请求取消 {tid}"

    # ---------------------------------------------------------------- 轮询与播报
    async def _poll_loop(self):
        if self._managed or self.table.managed():
            logger.info("[桥服务] 托管模式：旧 JSON 轮询不启动。")
            return
        while True:
            try:
                await asyncio.sleep(self._cfg.poll_interval)
                await self._poll_active()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("[桥服务] 轮询出错", exc_info=True)

    async def _poll_active(self):
        if self._managed or self.table.managed():
            return
        for rec in list(self.table.active()):
            try:
                await self._poll_one(rec)
            except Exception:
                logger.warning("[桥服务] 轮询任务 %s 出错", rec.task_id, exc_info=True)

    @staticmethod
    def _unpack_watch(result):
        """把 watch 的返回值统一解包成 (events, state)。

        远端 watch 通道当前返回 (events, state) 二元组；这里同时兼容日后
        改成 {"events": [...], "state": {...}} 这类字典的情况，形状不认识
        时退回空结果，避免调用方对二元组按字典取键抛 AttributeError。
        """
        if isinstance(result, dict):
            events = result.get("events")
            state = result.get("state")
        elif isinstance(result, (tuple, list)) and len(result) == 2:
            events, state = result
        else:
            events, state = None, None
        if not isinstance(events, (list, tuple)):
            events = []
        if not isinstance(state, dict):
            state = {}
        return list(events), state

    async def _poll_one(self, rec):
        """对单个任务做一次增量 watch，翻译并限频播报，最后应用 _state。"""
        try:
            result = await self.dispatch.watch(rec.task_id, rec.last_seq)
        except SshError as e:
            logger.warning("[桥服务] watch %s 失败：%s", rec.task_id, e)
            return
        except Exception as e:
            logger.warning("[桥服务] watch %s 异常：%s", rec.task_id, e)
            return

        events, state = self._unpack_watch(result)
        rec.last_poll = time.time()
        for ev in events:
            seq = ev.get("seq", 0)
            if isinstance(seq, (int, float)):
                rec.last_seq = max(rec.last_seq, int(seq))
            text = self.announcer.translate(ev)
            if text and self.announcer.should_announce(rec, ev):
                rec.last_announce = time.time()
                await self._say(rec, text)

        if state:
            summary = self._apply_state(rec, state)
            if summary:
                await self._say(rec, summary)

        self.table.save()

    def _apply_state(self, rec, state):
        """把 _state 应用到任务记录，状态切到终态时返回一句总结。"""
        status = state.get("status")
        if not status:
            return None
        old = rec.status
        rec.status = status
        if status in TERMINAL and old != status:
            return self.announcer.translate_state(state)
        return None

    async def _say(self, rec, text):
        """把播报内容发回触发任务的那个会话。"""
        if not rec.session:
            logger.info("[桥服务] 任务 %s 无会话信息，内容：%s", rec.task_id, text)
            return
        try:
            await self.context.send_message(rec.session, MessageChain().message(text))
        except Exception:
            logger.warning(
                "[桥服务] 播报失败（目标 %s）: %s", rec.session, text, exc_info=True
            )
