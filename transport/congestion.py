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
    """CUBIC đơn giản hóa (RFC 8312).

    Slow start:   cwnd < ssthresh  ->  cwnd += acked_count     (tăng gấp đôi mỗi RTT)
    Khi mất gói:  w_max = cwnd ; cwnd = max(MIN_CWND, cwnd * BETA) ; ssthresh = cwnd
                  epoch_start = now ; K = cbrt(w_max * (1 - BETA) / C)
    Tránh tắc nghẽn (cwnd >= ssthresh):
                  t = now - epoch_start
                  W(t) = C * (t - K)^3 + w_max
                  nếu W(t) > cwnd: cwnd += (W(t) - cwnd) / cwnd * acked_count
                  ngược lại:       cwnd += 0.01 * acked_count   (tăng rất chậm)
    Timeout:      ssthresh = max(MIN_CWND, cwnd/2) ; cwnd = MIN_CWND
    Chỉ giảm cwnd MỘT lần cho mỗi đợt mất gói (nhiều gói mất cùng RTT là một sự kiện).
    Nhớ gọi self.log("ack"/"loss"/"timeout") để có dữ liệu vẽ biểu đồ.
    """
    name = "cubic"

    def __init__(self) -> None:
        super().__init__()
        self.ssthresh: float = float("inf")
        self.w_max: float = 0.0
        self.k: float = 0.0
        self.epoch_start: float | None = None

    def on_ack(self, acked_count: int, rtt: float, now: float) -> None:
        raise NotImplementedError("TODO[A-D2] Cubic.on_ack")

    def on_loss(self, now: float) -> None:
        raise NotImplementedError("TODO[A-D2] Cubic.on_loss")

    def on_timeout(self, now: float) -> None:
        raise NotImplementedError("TODO[A-D2] Cubic.on_timeout")
