"""Linux transfer tools (the stock upload/download tools target a sandbox)."""
from dataclasses import dataclass, field
from pathlib import Path

import aiohttp
from astrbot.api import FunctionTool
from astrbot.api.event import MessageChain
from astrbot.core.message.components import File


async def local_path(context, name, tool):
    from astrbot.core.workspace_runtime import native_service, binding
    event = context.context.event
    await native_service(context.context.context).validate(event, tool=tool)
    path = Path(name).expanduser()
    if not path.is_absolute():
        path = Path(binding(event)["cwd"]) / path
    return path.resolve()


@dataclass
class LinuxDownloadTool(FunctionTool):
    name: str = "linux_download_file"
    description: str = "Download an HTTP(S) URL to a Linux host file. Existing files are not overwritten."
    parameters: dict = field(default_factory=lambda: {
        "type": "object", "properties": {
            "url": {"type": "string", "description": "HTTP(S) file URL"},
            "path": {"type": "string", "description": "Destination Linux path"},
        }, "required": ["url", "path"],
    })

    async def call(self, context, url: str, path: str):
        if not url.startswith(("http://", "https://")):
            return "只支持 HTTP(S) 下载"
        target = await local_path(context, path, self.name)
        if target.exists():
            return "目标已存在，请选择新文件名"
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as client:
            async with client.get(url) as response:
                response.raise_for_status()
                # Exclusive creation and cleanup prevent an incomplete file
                # from being mistaken for a completed transfer after cancel.
                with target.open("xb") as out:
                    try:
                        async for chunk in response.content.iter_chunked(65536):
                            out.write(chunk)
                    except BaseException:
                        target.unlink(missing_ok=True)
                        raise
        return f"已下载到 Linux：{target}"


@dataclass
class LinuxSendFileTool(FunctionTool):
    name: str = "linux_send_file"
    description: str = "Send a Linux host file to the owner of this private workspace conversation."
    parameters: dict = field(default_factory=lambda: {
        "type": "object", "properties": {"path": {"type": "string"}},
        "required": ["path"],
    })

    async def call(self, context, path: str):
        target = await local_path(context, path, self.name)
        if not target.is_file():
            return "本机文件不存在"
        from astrbot.core.workspace_runtime import binding
        b = binding(context.context.event)
        await context.context.event.send(MessageChain().message(f"[Linux · {b['title']}] 文件：{target.name}"))
        await context.context.event.send(MessageChain(chain=[File(name=target.name, file=str(target))]))
        return f"已发送 Linux 文件：{target.name}"
