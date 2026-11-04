"""Quản lý các client đang online (bước 3): username, IP, cổng, phiên UDP, thời điểm vào.

Trạng thái: KHUNG + TODO cho người B (ngày 1).
"""
import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Any

from common.log import get_logger
from transport.tcp_channel import ChannelClosed, ControlChannel

log = get_logger("sessions")


@dataclass
class ClientSession:
    username: str
    session_id: int
    control: ControlChannel             # kênh TCP
    ip: str
    udp_port: int
    udp: Any = None                     # UdpSession (hoặc MockSession khi test)
    joined_at: float = field(default_factory=time.time)
    tasks: list = field(default_factory=list)   # các task nền gắn với client này

    def public_info(self) -> dict:
        """Thông tin được phép gửi cho client khác (không bao giờ gửi khóa/mật khẩu)."""
        return {"username": self.username, "ip": self.ip, "port": self.udp_port, "since": int(self.joined_at)}


class SessionManager:
    def __init__(self) -> None:
        self._by_name: dict[str, ClientSession] = {}
        self._by_id: dict[int, ClientSession] = {}

    def new_session_id(self) -> int:
        """session_id ngẫu nhiên 32 bit, khác 0 và chưa dùng (khó đoán hơn số tăng dần)."""
        while True:
            sid = random.getrandbits(32)
            if sid and sid not in self._by_id:
                return sid

    def add(self, client: ClientSession) -> None:
        if client.username in self._by_name:
            raise ValueError("đã đăng nhập ở nơi khác")
        self._by_name[client.username] = client       # ghi vào sổ theo tên
        self._by_id[client.session_id] = client       # ghi vào sổ theo session_id

    def remove(self, username: str) -> ClientSession | None:
        client = self._by_name.pop(username, None)    # xóa khỏi sổ theo tên; không có thì trả None
        if client is not None:
            self._by_id.pop(client.session_id, None)  # xóa luôn khỏi sổ theo id
        return client

    def get(self, username: str) -> ClientSession | None:
        return self._by_name.get(username)

    def by_session_id(self, session_id: int) -> ClientSession | None:
        return self._by_id.get(session_id)

    def all(self) -> list[ClientSession]:
        return list(self._by_name.values())

    def online_users(self) -> list[dict]:
        return [c.public_info() for c in sorted(self.all(), key=lambda c: c.username)]

    async def broadcast(self, msg: dict, exclude: str | None = None) -> None:
        targets = [c for c in self.all() if c.username != exclude]                     # (1)
        results = await asyncio.gather(*(c.control.send(msg) for c in targets),         # (2)
                                       return_exceptions=True)
        for c, r in zip(targets, results):                                              # (3)
            if isinstance(r, Exception):
                log.warning("không gửi được %s cho %s: %r", msg.get("type"), c.username, r)

    def print_table(self) -> None:
        """In bảng user cho log server (bằng chứng cho bước 3 khi demo)."""
        rows = [f"  {c.username:<15} {c.ip:<15} udp:{c.udp_port:<6} sid:{c.session_id}" for c in self.all()]
        log.info("Online (%d):\n%s", len(rows), "\n".join(rows) or "  (trống)")
