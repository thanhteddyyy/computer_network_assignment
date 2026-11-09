"""Server phát ANNOUNCE qua UDP broadcast để client trong LAN tự tìm thấy (không cần nhập IP).

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
    """Phát định kỳ gói ANNOUNCE qua UDP broadcast mỗi DISCOVERY_INTERVAL giây."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    payload = build_announce(tcp_port, udp_port)
    try:
        while not stop.is_set():
            try:
                sock.sendto(payload, (BROADCAST_ADDR, DISCOVERY_PORT))
            except OSError as exc:
                log.debug("không gửi được UDP broadcast discovery: %s", exc)
            try:
                await asyncio.wait_for(stop.wait(), timeout=DISCOVERY_INTERVAL)
            except asyncio.TimeoutError:
                pass
    finally:
        sock.close()
