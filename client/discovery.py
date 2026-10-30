"""Client nghe gói ANNOUNCE do server broadcast để tự tìm địa chỉ server trong LAN.

Trạng thái: TODO cho người B (ngày 1).
"""
import asyncio
import json
import socket

from common.config import DISCOVERY_PORT, DISCOVERY_TIMEOUT
from common.log import get_logger

log = get_logger("discovery")


async def discover_server(timeout: float = DISCOVERY_TIMEOUT) -> tuple[str, int] | None:
    """Trả về (ip_server, tcp_port) hoặc None nếu hết thời gian.

    TODO[B-D1]:
      1. sock = socket.socket(AF_INET, SOCK_DGRAM); setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
         (trên Linux/macOS thêm SO_REUSEPORT nếu có, để nhiều client trên cùng máy cùng nghe được)
      2. sock.bind(("", DISCOVERY_PORT)); sock.setblocking(False)
      3. loop = asyncio.get_running_loop(); data, addr = await asyncio.wait_for(loop.sock_recvfrom(sock, 2048), timeout)
      4. msg = json.loads(data); nếu msg["type"] == "ANNOUNCE" -> return (addr[0], msg["tcp_port"])
         (gói lạ -> bỏ qua và nghe tiếp cho tới hết timeout)
      5. Bắt asyncio.TimeoutError -> None; finally: sock.close()
    """
    log.warning("discovery chưa được cài đặt (TODO[B-D1]) – dùng --server <ip>")
    return None
