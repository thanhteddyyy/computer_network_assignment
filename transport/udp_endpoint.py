"""Một socket UDP dùng chung cho nhiều phiên; chuyển gói đến đúng UdpSession theo session_id.

Trạng thái: ĐÃ HOÀN THIỆN (plumbing).
Server: 1 endpoint ở cổng 9001, nhiều phiên. Client: 1 endpoint cổng ngẫu nhiên, 1 phiên.
"""
import asyncio
import sys
from typing import Protocol

from common.log import get_logger
from transport.loss_sim import LossSimulator
from transport.packet import PacketError, peek_session_id

log = get_logger("udp")


class DatagramHandler(Protocol):
    def on_datagram(self, data: bytes, addr) -> None: ...


class UdpEndpoint(asyncio.DatagramProtocol):
    def __init__(self, loss: LossSimulator | None = None) -> None:
        self.transport: asyncio.DatagramTransport | None = None
        self.loss = loss or LossSimulator()
        self._sessions: dict[int, DatagramHandler] = {}
        self.unknown_packets = 0

    # ----------------------------------------------------------- vòng đời
    @classmethod
    async def open(cls, host: str, port: int, loss: LossSimulator | None = None) -> "UdpEndpoint":
        loop = asyncio.get_running_loop()
        _, endpoint = await loop.create_datagram_endpoint(lambda: cls(loss), local_addr=(host, port))
        return endpoint

    def connection_made(self, transport) -> None:
        self.transport = transport
        _disable_windows_connreset(transport)
        log.info("UDP lắng nghe tại %s", transport.get_extra_info("sockname"))

    @property
    def local_port(self) -> int:
        return self.transport.get_extra_info("sockname")[1]

    def close(self) -> None:
        if self.transport:
            self.transport.close()

    # ----------------------------------------------------------- phiên
    def register(self, session_id: int, handler: DatagramHandler) -> None:
        self._sessions[session_id] = handler

    def unregister(self, session_id: int) -> None:
        self._sessions.pop(session_id, None)

    # ----------------------------------------------------------- gửi / nhận
    def sendto(self, data: bytes, addr) -> None:
        if self.transport and not self.transport.is_closing():
            self.loss.send(self.transport, data, addr)

    def datagram_received(self, data: bytes, addr) -> None:
        try:
            handler = self._sessions.get(peek_session_id(data))
        except PacketError:
            self.unknown_packets += 1
            return
        if handler is None:
            self.unknown_packets += 1
            log.debug("bỏ gói từ %s: session_id không tồn tại", addr)
            return
        try:
            handler.on_datagram(data, addr)
        except Exception:  # một gói lỗi không được phép làm chết cả endpoint (bước 7)
            log.exception("lỗi xử lý gói từ %s", addr)

    def error_received(self, exc: Exception) -> None:
        # Trên Windows: ICMP "port unreachable" -> WinError 10054. Chỉ cần log rồi bỏ qua.
        log.debug("UDP error_received: %s", exc)


def _disable_windows_connreset(transport) -> None:
    """Tắt hành vi Windows báo ConnectionResetError (10054) trên socket UDP khi peer đã tắt."""
    if sys.platform != "win32":
        return
    sock = transport.get_extra_info("socket")
    try:
        import socket
        SIO_UDP_CONNRESET = 0x9800000C
        sock.ioctl(SIO_UDP_CONNRESET, False)  # type: ignore[attr-defined]
    except (AttributeError, OSError, ValueError):
        pass
