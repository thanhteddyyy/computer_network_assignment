"""Logic client (không phụ thuộc giao diện). Được dùng bởi http_server.py (UI web) và cli.py (test).

Chạy client có giao diện web:
    python -m client.client --http-port 8081 [--server 127.0.0.1]
    rồi mở http://localhost:8081

Máy trạng thái (vẽ vào báo cáo):
    DISCONNECTED -connect()-> CONNECTED -HELLO_ACK-> KEY_EXCHANGED -AUTH_OK-> AUTHENTICATED
    AUTHENTICATED -logout()/mất kết nối-> DISCONNECTED

Trạng thái: connect/HELLO và vòng điều khiển HOÀN THIỆN; trao khóa (A-D1), đăng ký/đăng nhập (B-D1) là TODO.
"""
import argparse
import asyncio

from common.config import ENABLE_UDP, HTTP_BASE_PORT, PROTOCOL_VERSION, TCP_PORT
from common.crypto import KeyExchange, SessionKeys
from common.log import get_logger, setup_logging
from common.messages import ProtocolError, T, expect, make
from client.chat import ChatService
from client.discovery import discover_server
from client.events import EventLog
from client.file_transfer import FileService
from transport.loss_sim import LossSimulator
from transport.session import UdpSession
from transport.tcp_channel import ChannelClosed, ControlChannel
from transport.udp_endpoint import UdpEndpoint

log = get_logger("client")


class CollabClient:
    def __init__(self, loss: LossSimulator | None = None) -> None:
        self.loss = loss or LossSimulator()
        self.events = EventLog()
        self.chat = ChatService(self)
        self.files = FileService(self)

        self.state = "DISCONNECTED"
        self.server_host: str | None = None
        self.ch: ControlChannel | None = None
        self.keys: SessionKeys | None = None
        self.endpoint: UdpEndpoint | None = None
        self.udp: UdpSession | None = None
        self.username: str | None = None
        self.session_id: int | None = None
        self.users: dict[str, dict] = {}
        self._control_task: asyncio.Task | None = None

    def _set_state(self, state: str, detail: str = "") -> None:
        self.state = state
        log.info("trạng thái -> %s %s", state, detail)
        self.events.add("status", state=state, detail=detail)

    # ================================================================ kết nối
    async def connect(self, host: str | None = None, port: int = TCP_PORT) -> None:
        if self.state != "DISCONNECTED":
            return
        if host is None:
            found = await discover_server()
            if not found:
                raise ConnectionError("không tìm thấy server trong LAN, hãy nhập địa chỉ server")
            host, port = found
        self.server_host = host
        self.ch = await ControlChannel.connect(host, port)
        self._set_state("CONNECTED", f"{host}:{port}")
        try:
            await self.ch.send(make(T.HELLO, version=PROTOCOL_VERSION, client_name="collab-client"))
            expect(await self.ch.recv(timeout=10), T.HELLO_ACK)
            self.keys = await self._key_exchange()
            self._set_state("KEY_EXCHANGED", "kênh TCP đã mã hóa")
        except BaseException as exc:
            await self._on_disconnect(f"handshake thất bại: {exc!r}")
            raise

    async def _key_exchange(self) -> SessionKeys:
        kx = KeyExchange()
        await self.ch.send(make(T.KEY_EXCHANGE, public_key=kx.public_bytes().hex()))  # GỬI trước
        msg = expect(await self.ch.recv(timeout=10), T.KEY_EXCHANGE)                  # rồi NHẬN
        try:
            peer_public = bytes.fromhex(msg["public_key"])
        except (KeyError, ValueError) as exc:
            raise ProtocolError("server gửi public_key không hợp lệ") from exc
        if len(peer_public) != 32:
            raise ProtocolError("server gửi public_key sai độ dài")
        keys = kx.derive(peer_public)
        self.ch.enable_encryption(keys.tcp_key)
        return keys

    async def register(self, username: str, password: str) -> tuple[bool, str]:
        await self.ch.send(make(T.REGISTER, username=username, password=password))
        resp = await self.ch.recv(timeout=10)
        if resp.get("type") == T.REGISTER and resp.get("ok"):
            return True, "đăng ký thành công"
        return False, resp.get("reason", "đăng ký thất bại")
    
    async def login(self, username: str, password: str) -> tuple[bool, str]:
        self.endpoint = await UdpEndpoint.open("0.0.0.0", 0, self.loss)             # (1)
        await self.ch.send(make(T.AUTH_REQ, username=username, password=password,
                                udp_port=self.endpoint.local_port))                 # (2)
        resp = await self.ch.recv(timeout=10)
        if resp.get("type") != T.AUTH_OK:                                           # (3)
            self.endpoint.close()
            self.endpoint = None
            return False, resp.get("reason", "đăng nhập thất bại")

        self.username = username                                                    # (4)
        self.session_id = resp["session_id"]
        self.users = {u["username"]: u for u in resp["users"]}
        if ENABLE_UDP:
            self._start_udp_session((self.server_host, resp["udp_port"]))
        self._control_task = asyncio.create_task(self._control_loop())              # (5)
        if ENABLE_UDP:
            self.chat.start()
        self._set_state("AUTHENTICATED", username)                                  # (6)
        self.events.add("users", users=list(self.users.values()))
        return True, "ok"

    def _start_udp_session(self, server_udp_addr) -> None:
        """HOÀN THIỆN: tạo phiên UDP tới server sau khi AUTH_OK."""
        self.udp = UdpSession(self.session_id, self.endpoint, server_udp_addr, self.keys, is_server=False)
        self.udp.on_close = lambda reason: asyncio.ensure_future(self._on_disconnect(f"UDP: {reason}"))
        self.udp.start()

    # ================================================================ vòng điều khiển
    async def _control_loop(self) -> None:
        handlers = {
            T.USER_LIST: self._on_user_list,
            T.USER_JOIN: self._on_user_join,
            T.USER_LEAVE: self._on_user_leave,
            T.FILE_OFFER: self.files.on_offer,
            T.FILE_OFFER_ACK: self.files.on_offer_ack,
            T.FILE_ACCEPT: self.files.on_accept,
            T.FILE_REJECT: self.files.on_reject,
            T.FILE_DONE: self.files.on_done,
            T.ERROR: lambda m: self.events.add("error", reason=m.get("reason", "")),
        }
        try:
            while True:
                msg = await self.ch.recv()
                handler = handlers.get(msg.get("type"))
                if handler is None:
                    log.warning("bỏ qua message lạ: %s", msg.get("type"))
                    continue
                try:
                    handler(msg)
                except NotImplementedError as exc:
                    log.error("chưa cài đặt: %s", exc)
        except ChannelClosed:
            await self._on_disconnect("server đóng kết nối TCP")
        except asyncio.CancelledError:
            pass

    def _on_user_list(self, msg: dict) -> None:
        self.users = {u["username"]: u for u in msg["users"]}
        self.events.add("users", users=list(self.users.values()))

    def _on_user_join(self, msg: dict) -> None:
        u = msg["user"]
        self.users[u["username"]] = u
        self.events.add("user_join", user=u)

    def _on_user_leave(self, msg: dict) -> None:
        self.users.pop(msg["username"], None)
        self.events.add("user_leave", username=msg["username"], reason=msg.get("reason", ""))

    # ================================================================ thoát
    async def logout(self) -> None:
        if self.ch and self.state == "AUTHENTICATED":
            try:
                await self.ch.send(make(T.LOGOUT))
            except ChannelClosed:
                pass
        await self._on_disconnect("đăng xuất")

    async def _on_disconnect(self, reason: str) -> None:
        """Dọn dẹp phía client. TODO[A-D3]: đảm bảo gọi nhiều lần không lỗi; các file đang truyền -> failed."""
        if self.state == "DISCONNECTED":
            return
        self.chat.stop()
        if self._control_task and self._control_task is not asyncio.current_task():
            self._control_task.cancel()
        if self.udp:
            await self.udp.close(reason)
        if self.endpoint:
            self.endpoint.close()
        if self.ch:
            await self.ch.close()
        self.udp = self.endpoint = self.ch = None
        self.users.clear()
        self._set_state("DISCONNECTED", reason)

    # ================================================================ cho UI
    def snapshot(self) -> dict:
        return {
            "state": self.state,
            "me": self.username,
            "server": self.server_host,
            "users": sorted(self.users.values(), key=lambda u: u["username"]),
            "stats": self.udp.stats() if self.udp else None,
            "files": self.files.list(),
            "last_event_id": self.events.last_id,
        }


def main() -> None:
    from client.http_server import HttpServer

    ap = argparse.ArgumentParser(description="Collab client (giao diện web)")
    ap.add_argument("--server", default=None, help="IP server; bỏ trống để tự tìm bằng discovery")
    ap.add_argument("--tcp-port", type=int, default=TCP_PORT)
    ap.add_argument("--http-port", type=int, default=HTTP_BASE_PORT)
    ap.add_argument("--loss", type=float, default=0.0)
    ap.add_argument("--delay", type=float, default=0.0)
    ap.add_argument("--jitter", type=float, default=0.0)
    ap.add_argument("--log", default=None)
    args = ap.parse_args()
    setup_logging(args.log)

    async def runner() -> None:
        client = CollabClient(LossSimulator(args.loss, args.delay, args.jitter))
        http = HttpServer(client, "127.0.0.1", args.http_port, default_server=args.server, tcp_port=args.tcp_port)
        await http.start()
        log.info("Mở trình duyệt tại http://localhost:%s", args.http_port)
        try:
            await asyncio.Event().wait()
        finally:
            await client.logout()

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        log.info("đã thoát")


if __name__ == "__main__":
    main()
