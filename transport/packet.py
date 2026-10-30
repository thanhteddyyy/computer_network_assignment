"""Định dạng gói UDP (21 byte header, network byte order) và frame ACK.

Trạng thái: ĐÃ HOÀN THIỆN (codec). Xem tests/test_packet.py.

 0               1               2               3
+---------------+---------------+---------------+---------------+
|            Magic (2)          |  Version (1)  |   Type (1)    |
+---------------+---------------+---------------+---------------+
|   Flags (1)   |                Session ID (4)                 |
+---------------+---------------+---------------+---------------+
|               |              Packet Number (4)                |
+---------------+---------------+---------------+---------------+
|               |        Stream ID (2)          |  Offset (4) ...
+---------------+---------------+---------------+---------------+
     ... Offset |       Payload Length (2)      |  Payload ...
+---------------+-------------------------------+---------------+

- Packet Number: tăng đơn điệu, KHÔNG tái sử dụng khi gửi lại (gửi lại = số mới).
  -> đo RTT không nhập nhằng, và nonce AES-GCM không bao giờ lặp.
- Stream ID + Offset: vị trí dữ liệu trong từng stream -> ghép lại theo từng stream,
  mất gói ở stream file không chặn stream chat (chống Head-of-Line blocking).
- Payload trên dây là ciphertext (AES-GCM), header được dùng làm AAD (xác thực nhưng không mã hóa).
"""
import struct
from dataclasses import dataclass, field
from enum import IntEnum, IntFlag

from common.config import AEAD_TAG_SIZE, HEADER_SIZE, PROTOCOL_MAGIC, PROTOCOL_VERSION
from common.crypto import DirectionKeys, open_packet_payload, seal_packet_payload

_HEADER = struct.Struct("!HBBBIIHIH")
assert _HEADER.size == HEADER_SIZE


class PacketType(IntEnum):
    DATA = 1    # mang dữ liệu stream, cần ACK
    ACK = 2     # xác nhận, KHÔNG cần ACK lại
    PING = 3    # heartbeat
    PONG = 4
    CLOSE = 5   # đóng phiên có kiểm soát


class Flags(IntFlag):
    NONE = 0
    FIN = 0x01  # End-of-Message: byte cuối của một message trên stream


class PacketError(Exception):
    """Gói rác / sai định dạng. Bên nhận chỉ cần bỏ qua, không được làm sập chương trình."""


@dataclass
class Packet:
    type: PacketType
    session_id: int
    packet_number: int
    stream_id: int = 0
    offset: int = 0
    flags: Flags = Flags.NONE
    payload: bytes = b""

    # ---------------------------------------------------------- dạng rõ (dùng cho test / debug)
    def header_bytes(self, payload_len: int | None = None) -> bytes:
        length = len(self.payload) if payload_len is None else payload_len
        return _HEADER.pack(
            PROTOCOL_MAGIC, PROTOCOL_VERSION, int(self.type), int(self.flags),
            self.session_id, self.packet_number, self.stream_id, self.offset, length,
        )

    def encode(self) -> bytes:
        return self.header_bytes() + self.payload

    @classmethod
    def decode(cls, data: bytes) -> "Packet":
        if len(data) < HEADER_SIZE:
            raise PacketError("gói ngắn hơn header")
        magic, ver, ptype, flags, sid, pn, stream, off, length = _HEADER.unpack_from(data)
        if magic != PROTOCOL_MAGIC:
            raise PacketError(f"sai magic 0x{magic:04x}")
        if ver != PROTOCOL_VERSION:
            raise PacketError(f"không hỗ trợ version {ver}")
        if len(data) - HEADER_SIZE != length:
            raise PacketError("độ dài payload không khớp")
        try:
            ptype = PacketType(ptype)
        except ValueError as exc:
            raise PacketError(f"type lạ {ptype}") from exc
        return cls(ptype, sid, pn, stream, off, Flags(flags), data[HEADER_SIZE:])

    # ---------------------------------------------------------- dạng mã hóa (dùng thật)
    def seal(self, keys: DirectionKeys) -> bytes:
        header = self.header_bytes(len(self.payload) + AEAD_TAG_SIZE)
        return header + seal_packet_payload(keys, self.packet_number, self.payload, header)

    @classmethod
    def open(cls, data: bytes, keys: DirectionKeys) -> "Packet":
        pkt = cls.decode(data)  # kiểm tra header trước
        pkt.payload = open_packet_payload(keys, pkt.packet_number, pkt.payload, data[:HEADER_SIZE])
        return pkt


def peek_session_id(data: bytes) -> int:
    """Đọc session_id mà chưa giải mã, để endpoint biết chuyển gói cho phiên nào."""
    if len(data) < HEADER_SIZE:
        raise PacketError("gói ngắn hơn header")
    return _HEADER.unpack_from(data)[4]


# ================================================================== ACK frame
_ACK_HEAD = struct.Struct("!IHHH")   # largest_acked, recv_window, ack_delay_ms, n_ranges
_ACK_RANGE = struct.Struct("!II")    # (start, end) bao gồm cả 2 đầu


@dataclass
class AckFrame:
    """Payload của gói ACK.

    ranges: các khoảng packet number ĐÃ NHẬN, ví dụ đã nhận 0..4 và 6..9 (mất 5)
            -> ranges = [(6, 9), (0, 4)]   (sắp xếp giảm dần, như QUIC)
    Bên gửi thấy 5 nằm giữa các khoảng -> biết chính xác gói 5 bị mất (Selective ACK).
    """
    largest_acked: int
    recv_window: int
    ack_delay_ms: int = 0
    ranges: list[tuple[int, int]] = field(default_factory=list)

    def encode(self) -> bytes:
        out = _ACK_HEAD.pack(self.largest_acked, self.recv_window, self.ack_delay_ms, len(self.ranges))
        return out + b"".join(_ACK_RANGE.pack(a, b) for a, b in self.ranges)

    @classmethod
    def decode(cls, data: bytes) -> "AckFrame":
        if len(data) < _ACK_HEAD.size:
            raise PacketError("ACK frame quá ngắn")
        largest, rwnd, delay, n = _ACK_HEAD.unpack_from(data)
        if len(data) != _ACK_HEAD.size + n * _ACK_RANGE.size:
            raise PacketError("ACK frame sai độ dài")
        ranges = [_ACK_RANGE.unpack_from(data, _ACK_HEAD.size + i * _ACK_RANGE.size) for i in range(n)]
        return cls(largest, rwnd, delay, ranges)

    def contains(self, pn: int) -> bool:
        return any(a <= pn <= b for a, b in self.ranges)
