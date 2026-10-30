"""UdpSession: ghép mọi thành phần của giao thức UDP tin cậy thành một phiên hoàn chỉnh.

Trạng thái: ĐÃ VIẾT PHẦN ĐIỀU PHỐI. Phiên chỉ chạy được khi người A hoàn thành các TODO trong
reliability.py, stream.py (và congestion.py nếu bật Cubic). Đọc kỹ file này trước khi làm TODO.

Đường đi của dữ liệu:
  GỬI:  stream.send(data) -> SendStream.segment -> hàng đợi -> _pump (kiểm cwnd & cửa sổ peer)
        -> Packet.seal (AES-GCM) -> endpoint.sendto (qua LossSimulator)
  NHẬN: endpoint -> on_datagram -> Packet.open (giải mã + xác thực) -> ReceiveTracker (chống trùng)
        -> DATA: RecvStream.on_data (ghép theo offset) -> read_message()
        -> ACK : SendTracker.on_ack -> cập nhật RTT, cwnd, gửi lại gói mất
  TIMER (mỗi 20 ms): gửi ACK trễ, gửi lại gói quá RTO, heartbeat PING, phát hiện peer chết.
"""
import asyncio
from collections import deque
from dataclasses import dataclass
from typing import Callable

from common.config import (ACK_DELAY, HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, MAX_RETRANSMIT)
from common.crypto import CryptoError, SessionKeys
from common.log import get_logger
from transport.congestion import CongestionController, FixedWindow
from transport.packet import AckFrame, Flags, Packet, PacketError, PacketType
from transport.reliability import ReceiveTracker, RttEstimator, SendTracker, SentPacket, now
from transport.stream import RecvStream, SendStream
from transport.udp_endpoint import UdpEndpoint

log = get_logger("session")

MAX_QUEUE = 512   # số đoạn tối đa chờ gửi; vượt quá thì send() phải chờ (backpressure)
TICK = 0.02


class SessionClosed(Exception):
    pass


@dataclass
class _Segment:
    stream_id: int
    offset: int
    data: bytes
    fin: bool
    retransmit_count: int = 0


class Stream:
    """Đối tượng mà tầng ứng dụng dùng. Interface giống hệt MockStream."""

    def __init__(self, session: "UdpSession", stream_id: int) -> None:
        self.session = session
        self.stream_id = stream_id
        self.send_side = SendStream(stream_id)
        self.recv_side = RecvStream(stream_id)

    async def send(self, data: bytes, fin: bool = True) -> None:
        await self.session._enqueue(self, data, fin)

    async def read_message(self) -> bytes:
        return await self.recv_side.read_message()


class UdpSession:
    def __init__(self, session_id: int, endpoint: UdpEndpoint, peer_addr, keys: SessionKeys,
                 is_server: bool, congestion: CongestionController | None = None) -> None:
        self.session_id = session_id
        self.endpoint = endpoint
        self.peer_addr = peer_addr
        self.is_server = is_server
        self.send_keys, self.recv_keys = keys.for_role(is_server)

        self.sender = SendTracker()
        self.receiver = ReceiveTracker()
        self.rtt = RttEstimator()
        self.cc = congestion or FixedWindow()   # TODO[A-D2]: đổi thành Cubic() khi Cubic chạy

        self.streams: dict[int, Stream] = {}
        self._queue: deque[_Segment] = deque()
        self._queue_space = asyncio.Event()
        self._queue_space.set()

        self.last_seen = now()
        self._last_ping = 0.0
        self.closed = False
        self.close_reason = ""
        self.bad_packets = 0
        self._timer_task: asyncio.Task | None = None

        # Callback cho tầng ứng dụng
        self.on_stream_created: Callable[[Stream], None] | None = None
        self.on_close: Callable[[str], None] | None = None

    # ================================================================ vòng đời
    def start(self) -> None:
        self.endpoint.register(self.session_id, self)
        self._timer_task = asyncio.create_task(self._timer_loop(), name=f"timer-{self.session_id}")

    async def close(self, reason: str = "local close") -> None:
        if self.closed:
            return
        try:
            self._send_control(PacketType.CLOSE)
        except Exception:
            pass
        self._close_local(reason)

    def _close_local(self, reason: str) -> None:
        if self.closed:
            return
        self.closed = True
        self.close_reason = reason
        self.endpoint.unregister(self.session_id)
        if self._timer_task and self._timer_task is not asyncio.current_task():
            self._timer_task.cancel()
        self._queue_space.set()   # đánh thức các send() đang chờ để chúng ném SessionClosed
        log.info("phiên %s đóng: %s", self.session_id, reason)
        if self.on_close:
            self.on_close(reason)

    # ================================================================ stream
    def stream(self, stream_id: int) -> Stream:
        st = self.streams.get(stream_id)
        if st is None:
            st = self.streams[stream_id] = Stream(self, stream_id)
            if self.on_stream_created:
                self.on_stream_created(st)
        return st

    # ================================================================ gửi
    async def _enqueue(self, stream: Stream, data: bytes, fin: bool) -> None:
        for offset, part, seg_fin in stream.send_side.segment(data, fin):
            while len(self._queue) >= MAX_QUEUE and not self.closed:
                self._queue_space.clear()
                await self._queue_space.wait()
            if self.closed:
                raise SessionClosed(self.close_reason)
            self._queue.append(_Segment(stream.stream_id, offset, part, seg_fin))
            self._pump()

    def _pump(self) -> None:
        """Gửi các đoạn đang chờ chừng nào cwnd và cửa sổ nhận của peer còn cho phép."""
        window = min(int(self.cc.cwnd), self.sender.peer_recv_window)
        while self._queue and self.sender.in_flight < window and not self.closed:
            self._send_data(self._queue.popleft())
        if len(self._queue) < MAX_QUEUE:
            self._queue_space.set()

    def _send_data(self, seg: _Segment) -> None:
        pn = self.sender.next_packet_number()
        flags = Flags.FIN if seg.fin else Flags.NONE
        self._transmit(Packet(PacketType.DATA, self.session_id, pn, seg.stream_id, seg.offset, flags, seg.data))
        self.sender.on_sent(SentPacket(pn, seg.stream_id, seg.offset, seg.data, seg.fin, now(), seg.retransmit_count))

    def _send_control(self, ptype: PacketType, payload: bytes = b"") -> None:
        """Gói không cần độ tin cậy (ACK/PING/PONG/CLOSE): vẫn cần packet_number riêng vì nonce."""
        pn = self.sender.next_packet_number()
        self._transmit(Packet(ptype, self.session_id, pn, payload=payload))

    def _transmit(self, pkt: Packet) -> None:
        self.endpoint.sendto(pkt.seal(self.send_keys), self.peer_addr)

    def _requeue(self, packets: list[SentPacket]) -> None:
        for p in sorted(packets, key=lambda x: x.packet_number, reverse=True):
            if p.retransmit_count + 1 > MAX_RETRANSMIT:
                self._close_local(f"gói stream {p.stream_id} offset {p.offset} gửi lại quá {MAX_RETRANSMIT} lần")
                return
            self._queue.appendleft(_Segment(p.stream_id, p.offset, p.data, p.fin, p.retransmit_count + 1))

    # ================================================================ nhận
    def on_datagram(self, data: bytes, addr) -> None:
        if self.closed:
            return
        try:
            pkt = Packet.open(data, self.recv_keys)
        except (PacketError, CryptoError):
            self.bad_packets += 1   # gói rác/giả mạo: bỏ qua, không crash (bước 7, 8)
            return
        self.peer_addr = addr       # chấp nhận đổi địa chỉ khi gói đã xác thực đúng
        self.last_seen = now()

        ack_eliciting = pkt.type in (PacketType.DATA, PacketType.PING)
        duplicate = self.receiver.on_packet(pkt.packet_number, ack_eliciting)
        if duplicate:
            return

        if pkt.type == PacketType.DATA:
            self.stream(pkt.stream_id).recv_side.on_data(pkt.offset, pkt.payload, bool(pkt.flags & Flags.FIN))
        elif pkt.type == PacketType.ACK:
            self._on_ack(AckFrame.decode(pkt.payload))
        elif pkt.type == PacketType.PING:
            self._send_control(PacketType.PONG)
        elif pkt.type == PacketType.CLOSE:
            self._close_local("peer đóng phiên")

    def _on_ack(self, frame: AckFrame) -> None:
        t = now()
        acked, lost = self.sender.on_ack(frame)
        if acked:
            newest = max(acked, key=lambda p: p.packet_number)
            if newest.packet_number == frame.largest_acked:
                sample = t - newest.sent_time - frame.ack_delay_ms / 1000
                if sample > 0:
                    self.rtt.update(sample)
            self.cc.on_ack(len(acked), self.rtt.latest, t)
        if lost:
            self.cc.on_loss(t)
            self._requeue(lost)
        self._pump()

    # ================================================================ timer
    async def _timer_loop(self) -> None:
        try:
            while not self.closed:
                await asyncio.sleep(TICK)
                t = now()
                # 1) ACK trễ: gom ACK để không gửi một ACK cho mỗi gói
                r = self.receiver
                if r.ack_pending and (r.first_unacked_time is None or t - r.first_unacked_time >= ACK_DELAY):
                    self._send_control(PacketType.ACK, r.build_ack(t).encode())
                # 2) Gửi lại gói quá RTO
                timed_out = self.sender.pop_timed_out(t, self.rtt.rto)
                if timed_out:
                    self.rtt.backoff()
                    self.cc.on_timeout(t)
                    self._requeue(timed_out)
                self._pump()
                # 3) Heartbeat (client chủ động PING, server trả PONG)
                if not self.is_server and t - self._last_ping >= HEARTBEAT_INTERVAL:
                    self._last_ping = t
                    self._send_control(PacketType.PING)
                # 4) Peer im lặng quá lâu -> coi như đã chết
                if t - self.last_seen > HEARTBEAT_TIMEOUT:
                    self._close_local("heartbeat timeout")
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("timer loop của phiên %s lỗi", self.session_id)
            self._close_local("lỗi nội bộ timer")

    # ================================================================ thống kê
    def stats(self) -> dict:
        return {
            "rtt_ms": round((self.rtt.srtt or 0) * 1000, 1),
            "rto_ms": round(self.rtt.rto * 1000, 1),
            "cwnd": round(self.cc.cwnd, 1),
            "cc": self.cc.name,
            "in_flight": self.sender.in_flight,
            "queued": len(self._queue),
            "loss": round(self.sender.loss_rate(), 4),
            "sent": self.sender.total_sent,
            "bad_packets": self.bad_packets,
            "encrypted": True,
        }
