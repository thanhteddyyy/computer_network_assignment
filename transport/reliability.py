"""Độ tin cậy: theo dõi gói đã gửi/đã nhận, ACK có chọn lọc (SACK), phát hiện mất gói, RTO.

Trạng thái: KHUNG + TODO cho người A (ngày 2). Test: tests/test_reliability.py

Mô hình (giống QUIC, đơn giản hóa):
  - Mỗi gói DATA mang một packet_number duy nhất, tăng dần.
  - Bên nhận ghi lại các packet_number đã nhận, định kỳ gửi AckFrame (largest + ranges).
  - Bên gửi nhận AckFrame -> biết gói nào đã tới, gói nào bị mất:
        * Mất theo NGƯỠNG GÓI: gói p chưa được ACK mà đã có gói >= p + 3 được ACK.
        * Mất theo THỜI GIAN: gói chưa được ACK quá RTO giây.
  - Gói mất được gửi lại với packet_number MỚI (dữ liệu stream_id/offset giữ nguyên).
"""
import time
from dataclasses import dataclass

from common.config import DEFAULT_RECV_WINDOW, INITIAL_RTO, MAX_RTO, MIN_RTO
from transport.packet import AckFrame

PACKET_THRESHOLD = 3


@dataclass
class SentPacket:
    packet_number: int
    stream_id: int
    offset: int
    data: bytes          # plaintext, để gửi lại nếu mất
    fin: bool
    sent_time: float
    retransmit_count: int = 0


# =================================================================== RTT
class RttEstimator:
    def __init__(self) -> None:
        self.srtt: float | None = None
        self.rttvar: float | None = None
        self.latest: float | None = None
        self._rto: float = INITIAL_RTO
    def update(self, sample: float) -> None:
        self.latest = sample
        if self.srtt is None:
            self.srtt = sample
            self.rttvar = sample / 2
        else:
            self.rttvar = 0.75 * self.rttvar + 0.25 * abs(self.srtt - sample)
            self.srtt = 0.875 * self.srtt + 0.125 * sample
        self._rto = max(MIN_RTO, min(self.srtt + 4 * self.rttvar, MAX_RTO))       # ─────────────────────────────────────────────────────────────────

    def backoff(self) -> None:                       # CÓ SẴN: gói hết giờ → RTO nhân đôi, tối đa 8 s
        self._rto = min(self._rto * 2, MAX_RTO)

    @property
    def rto(self) -> float:                          # CÓ SẴN: nơi bên ngoài đọc RTO (kẹp thêm một lần cho chắc)
        return max(MIN_RTO, min(self._rto, MAX_RTO))
    """Ước lượng RTT và tính RTO theo RFC 6298.

    Lần đo đầu tiên R:     SRTT = R ;  RTTVAR = R / 2
    Các lần sau:           RTTVAR = (1 - 1/4) * RTTVAR + 1/4 * |SRTT - R|
                           SRTT   = (1 - 1/8) * SRTT   + 1/8 * R
    RTO = SRTT + 4 * RTTVAR, kẹp trong [MIN_RTO, MAX_RTO]
    Khi timeout: RTO nhân đôi (exponential backoff) -> gọi backoff().
    """

    def __init__(self) -> None:
        self.srtt: float | None = None
        self.rttvar: float = 0.0
        self.latest: float = 0.0
        self._rto: float = INITIAL_RTO

    def update(self, sample: float) -> None:
        # TODO[A-D2]: cập nhật srtt, rttvar, latest và self._rto theo công thức ở docstring.
        self.latest = sample
        if self.srtt is None:
            self.srtt = sample
            self.rttvar = sample / 2
        else:
            self.rttvar = 0.75 * self.rttvar + 0.25 * abs(self.srtt - sample)
            self.srtt = 0.875 * self.srtt + 0.125 * sample
        self._rto = max(MIN_RTO, min(self.srtt + 4 * self.rttvar, MAX_RTO))

    def backoff(self) -> None:
        self._rto = min(self._rto * 2, MAX_RTO)

    @property
    def rto(self) -> float:
        return max(MIN_RTO, min(self._rto, MAX_RTO))


# =================================================================== Bên gửi
class SendTracker:
    def __init__(self) -> None:
        self._next_pn = 0
        self.unacked: dict[int, SentPacket] = {}
        self.largest_acked = -1
        self.peer_recv_window = DEFAULT_RECV_WINDOW
        self.total_sent = 0
        self.total_lost = 0

    def next_packet_number(self) -> int:
        pn = self._next_pn
        self._next_pn += 1
        return pn

    def on_sent(self, pkt: SentPacket) -> None:
        self.unacked[pkt.packet_number] = pkt
        self.total_sent += 1

    @property
    def in_flight(self) -> int:
        return len(self.unacked)

    def on_ack(self, frame: AckFrame) -> tuple[list[SentPacket], list[SentPacket]]:
        self.peer_recv_window = frame.recv_window                              # (1)

        newly_acked: list[SentPacket] = []                                     # (2)
        for pn in list(self.unacked):
            if frame.contains(pn):
                newly_acked.append(self.unacked.pop(pn))

        self.largest_acked = max(self.largest_acked, frame.largest_acked)      # (3)

        lost: list[SentPacket] = []                                            # (4)
        for pn in list(self.unacked):
            if pn + PACKET_THRESHOLD <= self.largest_acked:
                lost.append(self.unacked.pop(pn))
        self.total_lost += len(lost)

        return newly_acked, lost                                               # (5)

    def pop_timed_out(self, now: float, rto: float) -> list[SentPacket]:
        """TODO[A-D2]: lấy ra (xóa khỏi unacked) mọi gói có now - sent_time > rto. Tăng total_lost."""
        timed_out = [p for p in self.unacked.values() if now - p.sent_time > rto]   # (1)
        for p in timed_out:                                                         # (2)
            del self.unacked[p.packet_number]
        self.total_lost += len(timed_out)                                           # (3)
        return timed_out
    def loss_rate(self) -> float:
        return self.total_lost / self.total_sent if self.total_sent else 0.0


# =================================================================== Bên nhận
class ReceiveTracker:
    def __init__(self, recv_window: int = DEFAULT_RECV_WINDOW) -> None:
        self.received: set[int] = set()   # có thể thay bằng cấu trúc khoảng để tiết kiệm bộ nhớ
        self.largest = -1
        self.recv_window = recv_window
        self.ack_pending = False
        self.first_unacked_time: float | None = None

    def on_packet(self, pn: int, ack_eliciting: bool) -> bool:
        if pn in self.received:                       # (1) gói TRÙNG
            self.ack_pending = True
            return True
        self.received.add(pn)                         # (2) gói MỚI
        self.largest = max(self.largest, pn)
        if ack_eliciting:                             # (3) gói cần được ACK
            self.ack_pending = True
            if self.first_unacked_time is None:
                self.first_unacked_time = time.monotonic()
        return False

    def build_ack(self, now: float | None = None, max_ranges: int = 32) -> AckFrame:
        t = now if now is not None else time.monotonic()                 # (1)

        ranges: list[tuple[int, int]] = []                               # (2)
        for pn in sorted(self.received, reverse=True):
            if ranges and pn == ranges[-1][0] - 1:
                ranges[-1] = (pn, ranges[-1][1])
            elif len(ranges) < max_ranges:
                ranges.append((pn, pn))
            else:
                break

        delay_ms = 0                                                     # (3)
        if self.first_unacked_time is not None:
            delay_ms = min(int((t - self.first_unacked_time) * 1000), 65535)
        self.ack_pending = False                                         # (4)
        self.first_unacked_time = None

        if len(self.received) > 20000:                                   # (5)
            cutoff = self.largest - 10000
            self.received = {pn for pn in self.received if pn >= cutoff}

        return AckFrame(self.largest, self.recv_window, max(delay_ms, 0), ranges)   # (6)

def now() -> float:
    return time.monotonic()
