"""Đóng khung message trên TCP: [4 byte độ dài, big-endian][payload].

Vì sao cần? TCP là dòng byte (byte stream), không có ranh giới message.
Một lần recv() có thể nhận nửa message hoặc 2 message dính nhau.
Tiền tố độ dài cho bên nhận biết phải đọc chính xác bao nhiêu byte.
"""
import asyncio
import struct

from common.config import MAX_FRAME_SIZE

_LEN = struct.Struct("!I")


class FrameError(Exception):
    """Frame không hợp lệ (quá lớn hoặc kết nối đóng giữa chừng)."""


def encode_frame(payload: bytes) -> bytes:
    if len(payload) > MAX_FRAME_SIZE:
        raise FrameError(f"frame quá lớn: {len(payload)} byte")
    return _LEN.pack(len(payload)) + payload


async def read_frame(reader: asyncio.StreamReader) -> bytes:
    """Đọc đúng một frame. Ném asyncio.IncompleteReadError nếu peer đóng kết nối."""
    header = await reader.readexactly(_LEN.size)
    (length,) = _LEN.unpack(header)
    if length > MAX_FRAME_SIZE:
        raise FrameError(f"peer gửi frame {length} byte, vượt giới hạn")
    return await reader.readexactly(length)


async def write_frame(writer: asyncio.StreamWriter, payload: bytes) -> None:
    writer.write(encode_frame(payload))
    await writer.drain()
