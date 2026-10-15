# -*- coding: utf-8 -*-
"""Execution adapters for the task workspace.

The protocol is the existing bridge remote protocol:

    submit(request_id, prompt, cwd, permission, timeout) -> task_id
    watch(task_id, after) -> (events, state)
    cancel(task_id)
    reconcile(request_id=None, task_id=None) -> status dict

``SshRemoteExecutor`` speaks the protocol directly over SSH (same shape as
``astrbot_plugin_bridge_server.ssh_dispatch``). ``BridgeDispatcherAdapter`` wraps
an existing bridge ``SshDispatcher`` instance without importing it, so the
workspace plugin remains independent. ``FakeRemoteExecutor`` is used by tests.
"""
from __future__ import annotations

import asyncio
import json
import logging

from astrbot.api import logger

try:
    import paramiko
except ImportError:  # pragma: no cover
    paramiko = None

logging.getLogger("paramiko").setLevel(logging.WARNING)


class ExecutorError(Exception):
    def __init__(self, message: str, *, ambiguous: bool = False, connectivity: bool = False):
        super().__init__(message)
        self.ambiguous = ambiguous
        self.connectivity = connectivity


class _PreDispatchError(Exception):
    """Raised when a failure provably happened before the command was sent."""


class _PostDispatchError(Exception):
    """Raised when the command may have been transmitted before failing."""


class RemoteExecutor:
    async def submit(self, request_id: str, prompt: str, cwd: str, permission: str,
                     timeout_s: int, executor: str) -> str:
        raise NotImplementedError

    async def watch(self, task_id: str, after: int = 0) -> tuple[list, dict]:
        raise NotImplementedError

    async def cancel(self, task_id: str) -> None:
        raise NotImplementedError

    async def reconcile(self, request_id: str = "", task_id: str = "") -> dict:
        raise NotImplementedError

    async def prepare(self, workspace_id: str, root: str) -> str:
        """Ensure and return a canonical per-workspace directory."""
        return ""


def _quote(s: str) -> str:
    return '"' + str(s).replace('"', "") + '"'


class SshRemoteExecutor(RemoteExecutor):
    def __init__(self, host, port, user, work_dir, python="python", timeout=30,
                 known_hosts="", host_key_policy="reject"):
        self.host = host
        self.port = int(port)
        self.user = user
        self.work_dir = work_dir
        self.python = python
        self.timeout = timeout
        self.known_hosts = known_hosts
        self.host_key_policy = (host_key_policy or "reject").lower()

    def _client(self):
        if paramiko is None:
            raise ExecutorError("paramiko 未安装", connectivity=True)
        client = paramiko.SSHClient()
        if self.known_hosts:
            client.load_host_keys(self.known_hosts)
        else:
            client.load_system_host_keys()
        if self.host_key_policy == "auto":
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        else:
            client.set_missing_host_key_policy(paramiko.RejectPolicy())
        client.connect(
            hostname=self.host,
            port=self.port,
            username=self.user,
            look_for_keys=True,
            allow_agent=True,
            timeout=self.timeout,
            banner_timeout=self.timeout,
            auth_timeout=self.timeout,
        )
        return client

    def _build_command(self, subcommand, *args):
        parts = []
        if self.work_dir:
            parts.append("cd /d " + _quote(self.work_dir) + " &&")
        parts.append(_quote(self.python))
        parts.append("runner.py")
        parts.append(subcommand)
        for a in args:
            parts.append(_quote(str(a)))
        return " ".join(parts)

    async def _exec(self, command, stdin_data=None):
        def _run():
            try:
                client = self._client()
            except Exception as e:
                # Connect/auth failure: the command was never transmitted.
                raise _PreDispatchError(str(e)) from e
            try:
                # Deliberately split the channel open from the exec request.
                # ``client.exec_command`` bundles both: it opens a session
                # channel and then transmits the exec request before waiting
                # for the server acknowledgement. If that single call fails we
                # cannot tell whether the remote command was accepted, so it
                # must not be reported as a definite pre-dispatch failure.
                try:
                    channel = client.get_transport().open_session(
                        timeout=self.timeout)
                except Exception as e:
                    # Only the channel-open request was sent; the exec request
                    # was never transmitted, so absence of dispatch is proven.
                    raise _PreDispatchError(str(e)) from e
                try:
                    channel.settimeout(self.timeout)
                    channel.exec_command(command)
                except Exception as e:
                    # The exec request is transmitted before the server
                    # acknowledgement is awaited. A failure here (lost
                    # acknowledgement, closed channel) cannot prove the command
                    # was never accepted, so the outcome is ambiguous.
                    raise _PostDispatchError(str(e)) from e
                try:
                    stdin = channel.makefile_stdin("wb", -1)
                    stdout = channel.makefile("r", -1)
                    stderr = channel.makefile_stderr("r", -1)
                    if stdin_data is not None:
                        stdin.write(stdin_data)
                        stdin.flush()
                        stdin.channel.shutdown_write()
                    out = stdout.read().decode("utf-8", "replace")
                    err = stderr.read().decode("utf-8", "replace")
                    code = stdout.channel.recv_exit_status()
                    return code, out, err
                except Exception as e:
                    # The command may already be running remotely: unknown.
                    raise _PostDispatchError(str(e)) from e
            finally:
                client.close()

        return await asyncio.to_thread(_run)

    @staticmethod
    def _parse_lines(out_text):
        records = []
        for line in out_text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except Exception:
                logger.warning("[task-workspace] 忽略无法解析的输出行：%s", line[:200])
        return records

    async def _run_and_parse(self, command, stdin_obj=None):
        stdin_data = None
        if stdin_obj is not None:
            stdin_data = (json.dumps(stdin_obj, ensure_ascii=False) + "\n").encode("utf-8")
        try:
            code, out, err = await self._exec(command, stdin_data)
        except _PreDispatchError as e:
            raise ExecutorError(f"SSH 连接失败（未发送）：{e}", connectivity=True) from e
        except _PostDispatchError as e:
            raise ExecutorError(f"SSH 执行中断（可能已发送）：{e}", ambiguous=True) from e
        except ExecutorError:
            raise
        except Exception as e:
            raise ExecutorError(f"SSH 连接/执行失败：{e}", connectivity=True) from e
        records = self._parse_lines(out)
        if code != 0:
            raise ExecutorError(f"远端命令退出码 {code}：{(err or out).strip()[:300]}")
        return records

    async def submit(self, request_id, prompt, cwd, permission, timeout_s, executor) -> str:
        req = {
            "request_id": request_id,
            "agent": executor,
            "cwd": cwd,
            "prompt": prompt,
            "permission": permission,
            "timeout_s": timeout_s,
        }
        try:
            records = await self._run_and_parse(self._build_command("submit"), stdin_obj=req)
        except ExecutorError as e:
            if e.connectivity:
                raise
            # Any non-connectivity error after dispatch is ambiguous.
            raise ExecutorError(str(e), ambiguous=True) from e
        if not records:
            raise ExecutorError("submit 未返回任何结果", ambiguous=True)
        resp = records[0]
        if not isinstance(resp, dict):
            raise ExecutorError(f"submit 响应形状无法识别：{resp!r}", ambiguous=True)
        if not resp.get("ok"):
            raise ExecutorError(f"submit 失败：{resp.get('error', resp)}")
        task_id = resp.get("task_id")
        if task_id is None or str(task_id).strip() == "":
            # The request may have been accepted; we just cannot identify it.
            raise ExecutorError("submit 未返回有效远端 ID", ambiguous=True)
        return str(task_id)

    async def watch(self, task_id, after=0):
        command = self._build_command("watch", task_id, "--after", str(int(after)))
        records = await self._run_and_parse(command)
        return unpack_watch(records)

    async def cancel(self, task_id) -> None:
        records = await self._run_and_parse(self._build_command("cancel", task_id))
        if not records or not records[0].get("ok"):
            raise ExecutorError(f"cancel 失败：{records}")

    async def reconcile(self, request_id="", task_id="") -> dict:
        if request_id:
            command = self._build_command("reconcile", "--request-id", request_id)
        elif task_id:
            command = self._build_command("reconcile", "--task-id", task_id)
        else:
            raise ExecutorError("reconcile 需要 request_id 或 task_id")
        records = await self._run_and_parse(command)
        return records[0] if records else {"ok": False, "found": False}

    async def prepare(self, workspace_id: str, root: str) -> str:
        if not root:
            raise ExecutorError("未配置 workspace_root，无法准备隔离工作目录")
        command = self._build_command("prepare", "--workspace-id", workspace_id,
                                      "--root", root)
        records = await self._run_and_parse(command)
        if not records or not records[0].get("ok"):
            raise ExecutorError(f"prepare 失败：{records}")
        return str(records[0].get("cwd") or "")


class BridgeDispatcherAdapter(RemoteExecutor):
    """Adapt an existing bridge ``SshDispatcher``-like object.

    The wrapped object must expose ``submit(req: dict)``, ``watch(task_id, after)``
    and ``cancel(task_id)``. ``reconcile`` is used when present; otherwise a
    best-effort local reconciliation is performed from an empty ledger.
    """

    def __init__(self, dispatcher):
        self._d = dispatcher

    async def submit(self, request_id, prompt, cwd, permission, timeout_s, executor) -> str:
        req = {
            "request_id": request_id,
            "agent": executor,
            "cwd": cwd,
            "prompt": prompt,
            "permission": permission,
            "timeout_s": timeout_s,
        }
        try:
            task_id = await self._d.submit(req)
        except ExecutorError:
            raise
        except Exception as e:
            # We cannot prove the request was never transmitted, so treat any
            # transport error as ambiguous (unknown) rather than a definite
            # failure. A reconciliation path must resolve it.
            raise ExecutorError(f"submit 失败（结果不确定）：{e}", ambiguous=True) from e
        if task_id is None or str(task_id).strip() == "":
            raise ExecutorError("submit 未拿到有效 task_id", ambiguous=True)
        return str(task_id)

    async def watch(self, task_id, after=0):
        result = await self._d.watch(task_id, after)
        return unpack_watch(result)

    async def cancel(self, task_id) -> None:
        await self._d.cancel(task_id)

    async def reconcile(self, request_id="", task_id="") -> dict:
        fn = getattr(self._d, "reconcile", None)
        if fn is None:
            # No reconciliation channel: the result is unknown, never a
            # confident "absent" that would mark a live task failed.
            return {"ok": False, "error": "unsupported",
                    "reason": "adapter_has_no_reconcile"}
        if request_id:
            return await fn(request_id=request_id)
        return await fn(task_id=task_id)

    async def prepare(self, workspace_id: str, root: str) -> str:
        fn = getattr(self._d, "prepare", None)
        if fn is None:
            return ""
        return await fn(workspace_id, root)


def unpack_watch(result):
    """Normalise a watch result into ``(events, state)``."""
    if isinstance(result, dict):
        events = result.get("events")
        state = result.get("state")
        if events is None and state is None:
            events, state = _from_records(result.get("records"))
        if not isinstance(events, (list, tuple)):
            events = []
        if not isinstance(state, dict):
            state = {}
        return list(events), state
    if isinstance(result, (list, tuple)):
        if (
            len(result) == 2
            and isinstance(result[0], (list, tuple))
            and isinstance(result[1], dict)
        ):
            return list(result[0]), dict(result[1])
        return _from_records(result)
    return [], {}


def _from_records(records):
    events, state = [], {}
    for rec in records or []:
        if not isinstance(rec, dict):
            continue
        if rec.get("type") == "_state":
            state = rec.get("state") or {}
        elif "type" not in rec and ("events" in rec or "state" in rec):
            agg_events = rec.get("events")
            if isinstance(agg_events, (list, tuple)):
                events.extend(e for e in agg_events if isinstance(e, dict)
                              and e.get("type") != "_state")
            agg_state = rec.get("state")
            if isinstance(agg_state, dict) and agg_state:
                state = agg_state
        else:
            events.append(rec)
    return events, state


def extract_result(state: dict) -> tuple[str, list[str]]:
    """Extract the result text and artifact references from a remote state."""
    if not isinstance(state, dict):
        return "", []
    final = state.get("final") or state.get("result") or ""
    if not isinstance(final, str):
        final = str(final)
    artifacts = []
    raw = state.get("artifacts") or state.get("files") or []
    if isinstance(raw, dict):
        raw = list(raw.values())
    if isinstance(raw, (list, tuple)):
        for item in raw:
            if isinstance(item, str) and item:
                artifacts.append(item)
            elif isinstance(item, dict):
                ref = item.get("path") or item.get("ref") or item.get("url")
                if ref:
                    artifacts.append(str(ref))
    return final, artifacts


class FakeRemoteExecutor(RemoteExecutor):
    """Deterministic in-memory executor for tests."""

    is_fake = True

    def __init__(self):
        self.submitted: list[dict] = []
        self.cancelled: list[str] = []
        self.watch_results: dict[str, tuple[list, dict]] = {}
        self.default_watch: tuple[list, dict] = ([], {"status": "succeeded", "final": ""})
        self.reconcile_result: dict = {}
        self.fail_submit: Exception | None = None
        self.fail_watch: Exception | None = None
        self.fail_cancel: Exception | None = None
        self.request_index: dict[str, str] = {}
        self.prepared: dict[str, str] = {}
        self._seq = 0

    async def prepare(self, workspace_id: str, root: str) -> str:
        if not root:
            raise ExecutorError("未配置 workspace_root")
        cwd = root.rstrip("\\/") + "\\" + workspace_id
        self.prepared[workspace_id] = cwd
        return cwd

    async def submit(self, request_id, prompt, cwd, permission, timeout_s, executor) -> str:
        if self.fail_submit:
            raise self.fail_submit
        if request_id in self.request_index:
            return self.request_index[request_id]
        self._seq += 1
        task_id = f"t{self._seq:04d}"
        self.request_index[request_id] = task_id
        self.submitted.append(
            {
                "request_id": request_id,
                "task_id": task_id,
                "prompt": prompt,
                "cwd": cwd,
                "permission": permission,
                "timeout_s": timeout_s,
                "executor": executor,
            }
        )
        return task_id

    async def watch(self, task_id, after=0):
        if self.fail_watch:
            raise self.fail_watch
        events, state = self.watch_results.get(task_id, self.default_watch)
        return list(events), dict(state)

    async def cancel(self, task_id) -> None:
        if self.fail_cancel:
            raise self.fail_cancel
        self.cancelled.append(task_id)

    async def reconcile(self, request_id="", task_id="") -> dict:
        if self.reconcile_result:
            return dict(self.reconcile_result)
        if request_id and request_id in self.request_index:
            return {"ok": True, "found": True, "task_id": self.request_index[request_id]}
        return {"ok": True, "found": False}
