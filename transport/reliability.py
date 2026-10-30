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
        raise NotImplementedError("TODO[A-D2] RttEstimator.update")

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
        """Xử lý một AckFrame. Trả về (các gói vừa được ACK, các gói bị xác định là mất).

        TODO[A-D2]:
          1. Cập nhật self.peer_recv_window = frame.recv_window.
          2. Với mỗi pn trong self.unacked mà frame.contains(pn): lấy ra khỏi unacked -> newly_acked.
             (Lưu ý: frame có thể chứa pn của gói ACK/PING không có trong unacked -> bỏ qua.)
          3. Cập nhật self.largest_acked = max(self.largest_acked, frame.largest_acked).
          4. Ngưỡng gói: pn còn trong unacked và pn + PACKET_THRESHOLD <= largest_acked
             -> lấy ra khỏi unacked -> lost. Tăng self.total_lost.
          5. Trả về (newly_acked, lost).
        """
        raise NotImplementedError("TODO[A-D2] SendTracker.on_ack")

    def pop_timed_out(self, now: float, rto: float) -> list[SentPacket]:
        """TODO[A-D2]: lấy ra (xóa khỏi unacked) mọi gói có now - sent_time > rto. Tăng total_lost."""
        raise NotImplementedError("TODO[A-D2] SendTracker.pop_timed_out")

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
        """Ghi nhận gói pn. Trả về True nếu là gói TRÙNG (đã nhận trước đó) -> bên gọi bỏ dữ liệu.

        TODO[A-D2]:
          - Nếu pn đã có trong self.received -> vẫn đặt ack_pending = True (để bên gửi biết)
            rồi return True.
          - Ngược lại: thêm vào received, cập nhật largest.
          - Nếu ack_eliciting (gói DATA/PING): ack_pending = True, ghi first_unacked_time nếu chưa có.
          - return False
        """
        raise NotImplementedError("TODO[A-D2] ReceiveTracker.on_packet")

    def build_ack(self, now: float | None = None, max_ranges: int = 32) -> AckFrame:
        """Tạo AckFrame từ self.received.

        TODO[A-D2]:
          - Gom các pn đã nhận thành các khoảng liên tiếp, sắp xếp GIẢM dần,
            chỉ giữ tối đa max_ranges khoảng mới nhất. Ví dụ {0,1,2,4,5,9} -> [(9,9),(4,5),(0,2)]
          - ack_delay_ms = (now - first_unacked_time) * 1000 (nếu có)
          - Reset ack_pending = False, first_unacked_time = None
        Gợi ý: để tránh set phình to, có thể xóa các pn quá cũ (< largest - 10000).
        """
        raise NotImplementedError("TODO[A-D2] ReceiveTracker.build_ack")


def now() -> float:
    return time.monotonic()
