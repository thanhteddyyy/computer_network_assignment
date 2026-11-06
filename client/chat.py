"""Chat phía client (bước 5): gửi lên server, nhận tin từ stream 1 (chung) và 2 (riêng).

Trạng thái: vòng nhận HOÀN THIỆN; send là TODO cho người B (ngày 2).
"""
import asyncio
import json

from common.config import STREAM_DIRECT_CHAT, STREAM_GLOBAL_CHAT
from common.log import get_logger

log = get_logger("chat")
MAX_TEXT = 4000


class ChatService:
    def __init__(self, client) -> None:
        self.client = client          # CollabClient
        self._tasks: list[asyncio.Task] = []

    def start(self) -> None:
        for sid in (STREAM_GLOBAL_CHAT, STREAM_DIRECT_CHAT):
            self._tasks.append(asyncio.create_task(self._receive_loop(sid)))

    def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()

    async def _receive_loop(self, stream_id: int) -> None:
        stream = self.client.udp.stream(stream_id)
        try:
            while True:
                raw = await stream.read_message()
                try:
                    msg = json.loads(raw)
                except ValueError:
                    log.warning("bỏ tin nhắn không phải JSON trên stream %s", stream_id)
                    continue
                self.client.events.add("message", message=msg, channel="global" if stream_id == STREAM_GLOBAL_CHAT else "direct")
        except asyncio.CancelledError:
            pass

    async def send(self, to: str, text: str) -> None:
        """Gửi tin chat lên server: '*' đi stream 1 (chat chung), tên người đi stream 2 (chat riêng)."""
        if self.client.udp is None:                                            # (1)
            self.client.events.add("error", reason="chưa đăng nhập, hoặc UDP chưa bật (ENABLE_UDP)")
            return
        text = text.strip()                                                    # (2)
        if not text:
            return
        if len(text) > MAX_TEXT:                                               # (3)
            self.client.events.add("error", reason=f"tin nhắn dài quá {MAX_TEXT} ký tự")
            return
        stream_id = STREAM_GLOBAL_CHAT if to == "*" else STREAM_DIRECT_CHAT    # (4)
        payload = json.dumps({"to": to, "text": text}, ensure_ascii=False).encode()
        await self.client.udp.stream(stream_id).send(payload)                  # (5)
