"""Server phát ANNOUNCE qua UDP broadcast để client trong LAN tự tìm thấy (không cần nhập IP).

Trạng thái: TODO cho người B (ngày 1).
Gói ANNOUNCE là JSON rõ (không bí mật):
    {"type": "ANNOUNCE", "name": "collab-server", "tcp_port": 9000, "udp_port": 9001, "version": 1}
"""
import asyncio
import json
import socket

from common.config import BROADCAST_ADDR, DISCOVERY_INTERVAL, DISCOVERY_PORT, PROTOCOL_VERSION
from common.log import get_logger

log = get_logger("discovery")


def build_announce(tcp_port: int, udp_port: int) -> bytes:
    return json.dumps({"type": "ANNOUNCE", "name": "collab-server", "tcp_port": tcp_port,
                       "udp_port": udp_port, "version": PROTOCOL_VERSION}).encode()


async def run_announcer(tcp_port: int, udp_port: int, stop: asyncio.Event) -> None:
    """TODO[B-D1]:
      1. sock = socket.socket(AF_INET, SOCK_DGRAM); sock.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
      2. Lặp tới khi stop.is_set(): sock.sendto(build_announce(...), (BROADCAST_ADDR, DISCOVERY_PORT))
         rồi chờ DISCOVERY_INTERVAL giây (dùng asyncio.wait_for(stop.wait(), ...) để thoát nhanh).
      3. Bắt OSError (máy không có mạng/broadcast bị chặn) -> log cảnh báo, không làm sập server.
      4. finally: sock.close()
    """
    log.warning("discovery chưa được cài đặt (TODO[B-D1]) – client cần chỉ định --server")
