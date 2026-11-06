"""Định tuyến chat (bước 5): broadcast cho mọi người hoặc chuyển riêng cho một người.

Trạng thái: pump_stream HOÀN THIỆN; handle_chat là TODO cho người B (ngày 2).

Message chat (JSON, đi trên stream 1 hoặc 2 của phiên UDP):
  Client -> Server: {"to": "*" | "<username>", "text": "...", "client_msg_id": "..."}
  Server -> Client: {"type": "MSG", "msg_id": <int>, "from": "...", "to": "...", "text": "...", "ts": <float>}
"""
import asyncio
import itertools
import json
import time

from common.config import STREAM_DIRECT_CHAT, STREAM_GLOBAL_CHAT
from common.log import get_logger
from common.messages import T, make
from server.session_manager import ClientSession, SessionManager

log = get_logger("router")
MAX_TEXT = 4000

def parse_chat(raw: bytes) -> tuple[str, str]:
    """Kiểm tra một tin chat client gửi lên. Hợp lệ -> (to, text); không hợp lệ -> ValueError / KeyError."""
    data = json.loads(raw)                                        # bước 1
    if not isinstance(data, dict):                                # bước 2
        raise ValueError("tin nhắn phải là object JSON")
    to = data["to"]                                               # bước 3
    if not isinstance(to, str) or not to.strip():
        raise ValueError("người nhận phải là chuỗi, không được rỗng")
    text = data.get("text", "")                                   # bước 4
    if not isinstance(text, str):
        raise ValueError("nội dung phải là chuỗi")
    text = text.strip()                                           # bước 5
    if not text:
        raise ValueError("nội dung rỗng")
    if len(text) > MAX_TEXT:                                      # bước 6
        raise ValueError(f"nội dung dài quá {MAX_TEXT} ký tự")
    return to.strip(), text                                       # bước 7




class Router:
    def __init__(self, sessions: SessionManager) -> None:
        self.sessions = sessions
        self._ids = itertools.count(1)

    async def pump_stream(self, client: ClientSession, stream_id: int) -> None:
        """Task nền: đọc liên tục các message chat mà client gửi lên trên một stream."""
        stream = client.udp.stream(stream_id)
        try:
            while True:
                raw = await stream.read_message()
                try:
                    await self.handle_chat(client, raw)
                except (ValueError, KeyError) as exc:
                    log.warning("message chat lỗi từ %s: %s", client.username, exc)
                    await client.control.send(make(T.ERROR, reason=f"tin nhắn không hợp lệ: {exc}"))
        except asyncio.CancelledError:
            pass

    async def handle_chat(self, sender: ClientSession, raw: bytes) -> None:
                to, text = parse_chat(raw)
                # (2) Chọn người nhận
                if to == "*":
                    stream_id = STREAM_GLOBAL_CHAT
                    targets = [c for c in self.sessions.all() if c.username != sender.username]
                else:
                    stream_id = STREAM_DIRECT_CHAT
                    receiver = self.sessions.get(to)
                    if receiver is None:
                        await sender.control.send(make(T.ERROR, reason=f"{to} không online"))
                        return
                    targets = [] if receiver is sender else [receiver]

                # (3) Tạo bản chính thức (chỉ khi chắc chắn có tin được gửi đi)
                out = {
                    "type": "MSG",
                    "msg_id": next(self._ids),
                    "from": sender.username,
                    "to": to,
                    "text": text,
                    "ts": time.time(),
                }
                payload = json.dumps(out, ensure_ascii=False).encode()

                # (4) Gửi song song cho mọi người nhận
                results = await asyncio.gather(
                    *(c.udp.stream(stream_id).send(payload) for c in targets),
                    return_exceptions=True,
                )
                for c, res in zip(targets, results):
                    if isinstance(res, Exception):
                        log.warning("không gửi được tin #%d cho %s: %r", out["msg_id"], c.username, res)

                # (5) Bản xác nhận cho chính người gửi
                await sender.udp.stream(stream_id).send(payload)
                log.info("MSG #%d %s -> %s: %d người nhận", out["msg_id"], sender.username, to, len(targets))