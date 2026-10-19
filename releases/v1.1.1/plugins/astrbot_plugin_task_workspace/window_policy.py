"""Window capabilities and presentation. No execution from generated text."""
from __future__ import annotations

import re

LINUX_TOOLS = frozenset({
    "astrbot_execute_shell", "astrbot_execute_python", "astrbot_shell_session",
    "astrbot_file_read_tool", "astrbot_file_write_tool", "astrbot_file_edit_tool",
    "astrbot_grep_tool", "linux_download_file", "linux_send_file",
})


def window_info(ws, config) -> str:
    linux = ws.environment == "linux_local"
    return (
        f"[{'Linux' if linux else 'Windows'} · {ws.title} · {ws.short_id}]\n"
        f"执行：{'直接描述需求，调用本机工具' if linux else '/task run dsh 或 /task run codex'}\n"
        f"目录：{ws.cwd or config.workspace_root + ' / ' + ws.id}\n"
        f"权限：{'AstrBot 服务账户权限（不自动提权）' if linux else config.default_permission}"
    )


def capability_prompt(binding: dict) -> str:
    if binding["environment"] == "linux_local":
        return (
            "当前窗口是 Linux 本机维护窗口。真实工具运行在 AstrBot 所在 Linux 主机，"
            f"默认目录为 {binding['cwd']}，权限为服务账户权限。"
            "通过本次请求提供的结构化工具接口执行；收到真实工具结果后才能报告执行成功。"
            "不要输出 DSML/XML 来冒充调用，不要自行切换到 Windows 或调用远端桥。"
        )
    return (
        "当前窗口是 Windows 远端任务讨论窗口，本次聊天没有任何本机执行工具。"
        "你只能讨论需求及解释结果；执行需要用户发送 /task run dsh 或 /task run codex。"
        "不要输出 DSML/XML 工具调用，不要声称已经执行命令，不要将 Linux 路径当成远端目录。"
    )


_DSML = re.compile(r"<\s*[｜|]{2}DSML[｜|]{2}\s+(?:calls|invoke)\b", re.I)
_EXPLAIN = re.compile(r"解释|说明|示例|举例|格式|语法|什么|为何|为什么|含义|分析|展示|引用|explain|example|syntax|quote|what", re.I)


def sanitize_dsml(text: str, user_text: str, environment: str) -> tuple[str, bool]:
    if not _DSML.search(text or ""):
        return text, False
    # Only an explicit documentation request is exempt. Merely pasting an old
    # call must not persuade the model to "execute" it again as plain text.
    if ("DSML" in user_text.upper() or _DSML.search(user_text)) and _EXPLAIN.search(user_text):
        # A model claiming execution despite a documentation request is not an explanation.
        if not re.search(r"我(?:来|实际|现在|先)?执行|正在执行|已经执行|已执行|执行一下|I(?:'ll| will) (?:run|execute)", text, re.I):
            return text, False
    entry = "请使用 /task run dsh 或 /task run codex 提交 Windows 任务。" if environment == "windows_remote" else "请重新描述本机需求；执行必须通过真实工具接口。"
    return "该调用未执行：模型输出了工具调用标记文本，系统没有将它作为命令执行。" + entry, True
