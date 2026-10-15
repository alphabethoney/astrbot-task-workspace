"""codex 适配器：解析 codex exec --json 的 NDJSON 事件。

关键形状（fixture 文档）：

    {"type":"item.completed","item":{"id":"item_1","type":"agent_message",
                                    "text":"最终回答"}}
    {"type":"item.updated","item":{"type":"reasoning","text":"..."}}
    {"type":"item.started","item":{"type":"command_execution","command":"..."}}
    {"type":"item.delta","delta":"流式片段"}
    {"type":"turn.completed","usage":{...}}
    {"type":"error","message":"..."}

最终 agent_message 作为 final 事件保留原始文本；流式 delta 作为 text 增量。
绝不再把普通文本里出现 "error" 当失败。
"""
import json


def _text_of(obj):
    for k in ("text", "message", "content", "delta"):
        v = obj.get(k)
        if isinstance(v, str) and v != "":
            return v
    v = obj.get("content")
    if isinstance(v, list):
        parts = []
        for item in v:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                t = _text_of(item)
                if t:
                    parts.append(t)
        return "".join(parts)
    return ""


def parse(line):
    if line is None:
        return None
    s = line.rstrip("\r\n")
    if not s.strip():
        return None
    st = s.strip()
    if st.startswith("{"):
        try:
            obj = json.loads(st)
        except Exception:
            return ("text", s, {})
        if isinstance(obj, dict):
            return _parse_obj(obj, s)
    low = st.lower()
    if st.startswith("ERROR") or low.startswith("error:"):
        return ("error", st, {"terminal": True})
    return ("text", st, {})


def _parse_obj(obj, raw):
    t = str(obj.get("type") or obj.get("event") or "")
    if t in ("item.completed", "item.updated", "item", "item.message"):
        item = obj.get("item") if isinstance(obj.get("item"), dict) else obj
        it = str(item.get("type") or "")
        if it in ("agent_message", "assistant_message", "assistant", "message"):
            return ("final", _text_of(item),
                    {"item_id": item.get("id"), "item_type": it})
        if it == "reasoning":
            return ("thinking", _text_of(item), {"item_id": item.get("id")})
        if it in ("command_execution", "function_call", "tool_call", "tool"):
            return ("tool", str(item.get("command") or item.get("name") or ""),
                    {"call_id": item.get("id")})
        if it == "error":
            return ("error", _text_of(item) or raw, {"terminal": True})
        return ("text", _text_of(item), {"item_type": it})
    if t in ("item.delta", "response.output_text.delta", "agent_message.delta",
             "response.delta"):
        delta = obj.get("delta") or obj.get("text")
        if isinstance(delta, str):
            return ("text", delta, {"delta": True})
    if t in ("turn.completed", "session.completed", "response.completed"):
        return ("usage", "", {"usage": obj.get("usage")})
    if t in ("error", "turn.failed", "response.failed", "fatal"):
        return ("error", _text_of(obj) or raw, {"terminal": True})
    if t in ("session.created", "session"):
        return ("session",
                str(obj.get("session_id") or obj.get("sessionId") or ""), {})
    txt = _text_of(obj)
    if txt:
        return ("text", txt, {"json": True})
    return ("raw", json.dumps(obj, ensure_ascii=False), {"json": True})
