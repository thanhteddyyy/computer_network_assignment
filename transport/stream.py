"""Stream: dòng byte có thứ tự, độc lập với nhau trong cùng một phiên (chống HoL blocking).

Trạng thái: SendStream HOÀN THIỆN; RecvStream.on_data là TODO cho người A (ngày 2).

Ranh giới message: cờ FIN đánh dấu byte cuối của MỘT message.
Offset liên tục qua các message trong cùng stream:
    message "hi" (2 byte) rồi "abc" (3 byte):  (offset 0, "hi", FIN) (offset 2, "abc", FIN)
"""
import asyncio

from common.config import CHUNK_SIZE


class SendStream:
    def __init__(self, stream_id: int) -> None:
        self.stream_id = stream_id
        self.next_offset = 0

    def segment(self, data: bytes, fin: bool = True, chunk: int = CHUNK_SIZE) -> list[tuple[int, bytes, bool]]:
        """Cắt data thành các đoạn (offset, bytes, fin). Chỉ đoạn cuối mang FIN."""
        if not data:
            seg = [(self.next_offset, b"", fin)]
            return seg
        segments = []
        for i in range(0, len(data), chunk):
            part = data[i:i + chunk]
            is_last = i + chunk >= len(data)
            segments.append((self.next_offset, part, fin and is_last))
            self.next_offset += len(part)
        return segments


class RecvStream:
    def __init__(self, stream_id: int) -> None:
        self.stream_id = stream_id
        self.next_offset = 0                                   # byte tiếp theo đang chờ
        self._pending: dict[int, tuple[bytes, bool]] = {}      # đoạn đến sớm (đảo thứ tự)
        self._current = bytearray()                            # message đang ghép dở
        self._messages: asyncio.Queue[bytes] = asyncio.Queue() # message hoàn chỉnh
        self.bytes_received = 0

    def on_data(self, offset: int, data: bytes, fin: bool) -> None:
        if offset < self.next_offset or offset in self._pending:      # (1) đoạn cũ hoặc trùng
            return
        self._pending[offset] = (data, fin)                            # (2) cất vào chỗ chờ
        while self.next_offset in self._pending:                       # (3) ghép liên tục
            chunk, is_fin = self._pending.pop(self.next_offset)
            self._current += chunk
            self.next_offset += len(chunk)
            self.bytes_received += len(chunk)
            if is_fin:                                                 # (4) đủ một message
                self._messages.put_nowait(bytes(self._current))
                self._current.clear()

    async def read_message(self) -> bytes:
        """Chờ (không chặn event loop) đến khi có một message hoàn chỉnh."""
        return await self._messages.get()

    def has_message(self) -> bool:
        return not self._messages.empty()
