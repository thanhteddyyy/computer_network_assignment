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
        """Nhận một đoạn của stream (có thể đến sai thứ tự hoặc trùng).

        TODO[A-D2]:
          1. Nếu offset < self.next_offset: đoạn cũ/trùng -> bỏ qua.
             (Đơn giản hóa: giả định đoạn không bị cắt chồng lấn vì mỗi đoạn luôn được gửi lại nguyên vẹn.)
          2. Lưu vào self._pending[offset] = (data, fin).
          3. Lặp: while self.next_offset in self._pending:
                 lấy đoạn ra, nối vào self._current, next_offset += len(data), bytes_received += len(data)
                 nếu fin: self._messages.put_nowait(bytes(self._current)); xóa _current
        Đây chính là chỗ chống HoL: mỗi stream có _pending riêng, stream này chờ không làm stream khác chờ.
        """
        raise NotImplementedError("TODO[A-D2] RecvStream.on_data")

    async def read_message(self) -> bytes:
        """Chờ (không chặn event loop) đến khi có một message hoàn chỉnh."""
        return await self._messages.get()

    def has_message(self) -> bool:
        return not self._messages.empty()
