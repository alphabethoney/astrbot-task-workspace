"""dsh 适配器：把 dsh --json 吐出的 NDJSON 事件翻成统一事件。

统一事件类型：session / status / thinking / text / tool / tool_result /
usage / final / error / raw。

final 事件保留原始空白，worker 用它作为权威结果；text/delta 事件用于流式增量。
"""
import json

_TEXT_KEYS = ("text", "message", "content", "delta", "output")
_CMD_KEYS = ("command", "cmd", "path", "file_path")
_FINAL_TYPES = ("final", "result", "answer", "complete", "completed", "done")


def _first_str(obj, keys):
    for k in keys:
        v = obj.get(k)
        if isinstance(v, str) and v != "":
            return v
    return ""


def _text_of(obj):
    return _first_str(obj, _TEXT_KEYS)


def _content_text(obj):
    v = obj.get("content")
    if isinstance(v, list):
        parts = []
        for item in v:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                t = _first_str(item, ("text", "content"))
                if t:
                    parts.append(t)
        return "".join(parts)
    return ""


def _raw_text(obj):
    txt = _text_of(obj)
    if txt:
        return txt
    return _content_text(obj)


def _raw_final(obj):
    txt = _raw_text(obj)
    if txt:
        return txt
    result = obj.get("result")
    if isinstance(result, str):
        return result
    if isinstance(result, dict):
        inner = _raw_text(result) or _raw_final(result)
        if inner:
            return inner
    for k in ("answer", "message", "text"):
        v = obj.get(k)
        if isinstance(v, str):
            return v
    return ""


def _brief(v, limit=200):
    if v is None:
        return ""
    if not isinstance(v, str):
        v = json.dumps(v, ensure_ascii=False)
    v = v.replace("\r", " ").replace("\n", " ").strip()
    return v if len(v) <= limit else v[:limit] + "…"


def parse(line):
    """返回 (类型, 文本, 附加字段) 或 None。"""
    if line is None:
        return None
    s = line.rstrip("\r\n")
    if not s.strip():
        return None
    try:
        obj = json.loads(s)
    except Exception:
        return ("raw", s, {})
    if not isinstance(obj, dict):
        return ("raw", s, {})

    t = str(obj.get("type") or obj.get("event") or "")
    if t in ("session", "session_start", "session.created"):
        sid = obj.get("sessionId") or obj.get("session_id") or obj.get("id") or ""
        return ("session", str(sid), {"cwd": obj.get("cwd")})
    if t == "status":
        usage = obj.get("usage")
        if usage:
            return ("usage", obj.get("phase") or "", {"usage": usage})
        return ("status", obj.get("phase") or "", {})
    if t == "thinking":
        return ("thinking", _raw_text(obj), {})
    if t in ("text", "delta", "message", "assistant"):
        return ("text", _raw_text(obj),
                {"delta": bool(obj.get("delta")) or t == "delta"})
    if t in _FINAL_TYPES:
        return ("final", _raw_final(obj), {"session_id": obj.get("sessionId")})
    if t in ("tool_call", "tool", "tool_use"):
        name = obj.get("tool") or obj.get("name") or ""
        inp = obj.get("input") or {}
        detail = ""
        if isinstance(inp, dict):
            for k in _CMD_KEYS:
                if inp.get(k):
                    detail = _brief(inp.get(k))
                    break
            if not detail:
                detail = _brief(inp, 120)
        return ("tool", str(name), {"command": detail, "call_id": obj.get("callId")})
    if t in ("tool_result",):
        return ("tool_result", _brief(obj.get("result"), 300),
                {"status": obj.get("status"), "call_id": obj.get("callId")})
    if t in ("error", "failure", "fatal"):
        return ("error", _raw_text(obj) or _brief(obj), {"terminal": True})
    return ("raw", _brief(obj, 200), {"raw_type": t})
