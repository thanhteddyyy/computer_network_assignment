"""Client nghe gói ANNOUNCE do server broadcast để tự tìm địa chỉ server trong LAN."""
import asyncio
import json
import socket

from common.config import DISCOVERY_PORT, DISCOVERY_TIMEOUT
from common.log import get_logger

log = get_logger("discovery")


async def discover_server(timeout: float = DISCOVERY_TIMEOUT) -> tuple[str, int] | None:
    """Trả về (ip_server, tcp_port) hoặc None nếu hết thời gian chờ."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except OSError:
            pass
    try:
        sock.bind(("", DISCOVERY_PORT))
        sock.setblocking(False)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while (remaining := deadline - loop.time()) > 0:
            data, addr = await asyncio.wait_for(loop.sock_recvfrom(sock, 2048), timeout=remaining)
            try:
                msg = json.loads(data.decode("utf-8"))
                if isinstance(msg, dict) and msg.get("type") == "ANNOUNCE" and "tcp_port" in msg:
                    log.info("tìm thấy server tại %s:%s", addr[0], msg["tcp_port"])
                    return addr[0], int(msg["tcp_port"])
            except (ValueError, KeyError, TypeError):
                continue
    except (asyncio.TimeoutError, OSError):
        return None
    finally:
        sock.close()
    return None
