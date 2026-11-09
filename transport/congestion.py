"""Điều khiển tắc nghẽn. cwnd tính bằng SỐ GÓI cho đơn giản.

Trạng thái: FixedWindow HOÀN THIỆN (dùng tạm); Cubic là TODO cho người A (ngày 2).
Session dùng FixedWindow mặc định; khi Cubic xong thì đổi trong transport/session.py.
"""
import csv
import time

from common.config import CUBIC_BETA, CUBIC_C, INITIAL_CWND, MIN_CWND


class CongestionController:
    name = "base"

    def __init__(self) -> None:
        self.cwnd: float = INITIAL_CWND
        self._log_writer = None
        self._log_t0 = time.monotonic()

    def can_send(self, in_flight: int) -> bool:
        return in_flight < int(self.cwnd)

    def on_ack(self, acked_count: int, rtt: float, now: float) -> None: ...
    def on_loss(self, now: float) -> None: ...
    def on_timeout(self, now: float) -> None: ...

    # Ghi cwnd ra CSV để vẽ biểu đồ cho báo cáo
    def enable_csv_log(self, path: str) -> None:
        f = open(path, "w", newline="")
        self._log_writer = csv.writer(f)
        self._log_writer.writerow(["t", "cwnd", "event"])

    def log(self, event: str) -> None:
        if self._log_writer:
            self._log_writer.writerow([f"{time.monotonic() - self._log_t0:.3f}", f"{self.cwnd:.2f}", event])


class FixedWindow(CongestionController):
    """Cửa sổ cố định – KHÔNG phải congestion control thật, chỉ để hệ thống chạy được trước."""
    name = "fixed"

    def __init__(self, window: int = 32) -> None:
        super().__init__()
        self.cwnd = window


class Cubic(CongestionController):
    """CUBIC đơn giản hóa (RFC 8312)."""
    name = "cubic"

    def __init__(self) -> None:
        super().__init__()
        self.ssthresh: float = float("inf")   # Ngưỡng Slow Start ban đầu vô hạn
        self.w_max: float = INITIAL_CWND
        self.epoch_start: float = 0.0         # Mốc thời gian bắt đầu chu kỳ tránh tắc nghẽn
        self.K: float = 0.0                   # Thời gian để đạt lại w_max
        self.last_loss_at: float = 0.0        # Chống giảm nhiều lần trong cùng 1 RTT

    def on_ack(self, acked_count: int, rtt: float, now: float) -> None:
        """Xử lý khi nhận được ACK."""
        if self.cwnd < self.ssthresh:
            # 1. Slow Start: tăng theo số gói được ACK (tăng gấp đôi mỗi RTT)
            self.cwnd += acked_count
        else:
            # 2. Congestion Avoidance: hàm bậc 3 CUBIC
            t = max(0.0, now - self.epoch_start)
            target_w = CUBIC_C * ((t - self.K) ** 3) + self.w_max
            if target_w > self.cwnd:
                self.cwnd += (target_w - self.cwnd) / self.cwnd * acked_count
            else:
                self.cwnd += 0.01 * acked_count   # Tăng tối thiểu để không bị đứng yên
        self.log("ack")

    def on_loss(self, now: float) -> None:
        """Xử lý khi phát hiện mất gói (qua SACK hoặc 3 duplicate ACK)."""
        # Chỉ giảm 1 lần cho mỗi đợt mất gói (cách nhau tối thiểu 0.1s để tránh giảm liên tục cùng 1 RTT)
        if now - self.last_loss_at < 0.1:
            return
        self.last_loss_at = now
        self.w_max = self.cwnd
        self.ssthresh = max(float(MIN_CWND), self.cwnd * CUBIC_BETA)
        self.cwnd = self.ssthresh
        self.epoch_start = now

        # Tính K = (w_max * (1 - BETA) / C) ** (1/3)
        diff = max(0.0, self.w_max * (1.0 - CUBIC_BETA))
        self.K = (diff / CUBIC_C) ** (1.0 / 3.0)
        self.log("loss")

    def on_timeout(self, now: float) -> None:
        """Xử lý khi xảy ra RTO timeout (nghẽn nghiêm trọng)."""
        self.ssthresh = max(float(MIN_CWND), self.cwnd / 2.0)
        self.cwnd = float(MIN_CWND)
        self.w_max = self.cwnd
        self.epoch_start = now
        self.K = 0.0
        self.last_loss_at = now
        self.log("timeout")