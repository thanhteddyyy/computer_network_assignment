"""Kênh điều khiển TCP: gửi/nhận message JSON có đóng khung, tùy chọn mã hóa AES-GCM.

Trạng thái: ĐÃ HOÀN THIỆN (hạ tầng). Đọc hiểu kỹ trước khi dùng.
"""
import asyncio
import json


from common.log import get_logger
from common.crypto import CryptoError, open_random_nonce, seal_random_nonce
from common.framing import FrameError, read_frame, write_frame
from common.messages import ProtocolError
log = get_logger("tcp")


class ChannelClosed(Exception):
    """Peer đã đóng kết nối TCP (bình thường hoặc đột ngột)."""


class ControlChannel:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader = reader
        self.writer = writer
        self._key: bytes | None = None
        self._send_lock = asyncio.Lock()  # nhiều task cùng gửi -> tránh xen kẽ byte của 2 frame
        self.peername = writer.get_extra_info("peername")

    @classmethod
    async def connect(cls, host: str, port: int, timeout: float = 5.0) -> "ControlChannel":
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
        return cls(reader, writer)

    @property
    def encrypted(self) -> bool:
        return self._key is not None

    def enable_encryption(self, key: bytes) -> None:
        """Gọi ngay sau KEY_EXCHANGE. Từ đây mọi frame đều được mã hóa."""
        self._key = key

    async def send(self, msg: dict) -> None:
        if self.writer.is_closing():
            raise ChannelClosed("kết nối đã đóng")
        raw = json.dumps(msg, ensure_ascii=False).encode()
        if self._key:
            raw = seal_random_nonce(self._key, raw)
        try:
            async with self._send_lock:
                await write_frame(self.writer, raw)
        except (ConnectionError, OSError) as exc:
            raise ChannelClosed(str(exc)) from exc
        log.debug("-> %s %s", self.peername, msg.get("type"))

    async def recv(self, timeout: float | None = None) -> dict:
        # (1) Chờ đọc một frame đầy đủ
        try:
            raw = await asyncio.wait_for(read_frame(self.reader), timeout)
        except asyncio.TimeoutError:
            raise          # hết giờ chờ: để người gọi xử lý, KHÔNG coi là mất kết nối
        except FrameError as exc:
            raise ProtocolError(str(exc)) from exc            # frame quá lớn = vi phạm giao thức
        except (asyncio.IncompleteReadError, ConnectionError, OSError) as exc:
            raise ChannelClosed(str(exc)) from exc            # người kia đi rồi

        # (2) Giải mã và đọc JSON
        try:
            if self._key:
                raw = open_random_nonce(self._key, raw)
            msg = json.loads(raw)
        except (CryptoError, ValueError) as exc:
            raise ProtocolError(f"frame không hợp lệ: {exc}") from exc   # giải mã hỏng / JSON hỏng

        # (3) JSON đúng cú pháp nhưng phải là một message thật
        if not isinstance(msg, dict) or "type" not in msg:
            raise ProtocolError("message phải là object JSON có trường type")

        log.debug("<- %s %s", self.peername, msg.get("type"))
        return msg

    async def close(self) -> None:
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except (ConnectionError, OSError):
            pass
