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
        """TODO[B-D2]:
          1. data = json.loads(raw); lấy "to", "text"; kiểm tra text không rỗng và <= MAX_TEXT.
          2. out = {"type": "MSG", "msg_id": next(self._ids), "from": sender.username,
                    "to": data["to"], "text": text, "ts": time.time()}
          3. payload = json.dumps(out).encode()
          4. Nếu to == "*": gửi payload trên STREAM_GLOBAL_CHAT tới MỌI client khác
             (dùng asyncio.gather(..., return_exceptions=True) để client chậm không chặn người khác).
          5. Nếu là tên người: tìm self.sessions.get(to);
             - không online -> gửi T.ERROR qua sender.control
             - có -> gửi trên STREAM_DIRECT_CHAT tới người nhận
          6. Luôn gửi lại bản `out` cho chính người gửi (trên cùng stream) làm xác nhận
             "đã được server nhận" -> UI hiển thị tin của mình kèm msg_id chính thức.
          7. log.info để demo thấy luồng định tuyến.
        """
        raise NotImplementedError("TODO[B-D2] Router.handle_chat")
