"""Giả lập mạng xấu ngay trong code: mất gói, trễ, jitter, đảo thứ tự.

Trạng thái: ĐÃ HOÀN THIỆN. Bật bằng tham số dòng lệnh, ví dụ:
    python -m server.server --loss 0.1 --delay 30 --jitter 20
Dùng để chứng minh bước 5, 6, 7 hoạt động đúng khi mạng không hoàn hảo.
"""
import asyncio
import random
from dataclasses import dataclass


@dataclass
class LossSimulator:
    loss_rate: float = 0.0     # 0.1 = rớt 10% gói
    delay_ms: float = 0.0      # trễ cố định
    jitter_ms: float = 0.0     # trễ ngẫu nhiên thêm 0..jitter (tạo ra đảo thứ tự)
    seed: int | None = None

    def __post_init__(self) -> None:
        self._rng = random.Random(self.seed)
        self.dropped = 0
        self.sent = 0

    @property
    def enabled(self) -> bool:
        return self.loss_rate > 0 or self.delay_ms > 0 or self.jitter_ms > 0

    def send(self, transport: asyncio.DatagramTransport, data: bytes, addr) -> None:
        self.sent += 1
        if self._rng.random() < self.loss_rate:
            self.dropped += 1
            return
        delay = (self.delay_ms + self._rng.random() * self.jitter_ms) / 1000
        if delay <= 0:
            transport.sendto(data, addr)
        else:
            asyncio.get_running_loop().call_later(delay, self._late_send, transport, data, addr)

    @staticmethod
    def _late_send(transport, data, addr) -> None:
        if not transport.is_closing():
            transport.sendto(data, addr)
