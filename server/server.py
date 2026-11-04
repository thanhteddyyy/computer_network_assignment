"""Điểm vào của server.   Chạy:  python -m server.server [--loss 0.1 --delay 30]

Trạng thái: vòng accept + HELLO/HELLO_ACK HOÀN THIỆN (bước 2, 4).
TODO: trao khóa (A-D1), xác thực (B-D1), dọn dẹp nâng cao (A-D3).

Mỗi client = 1 coroutine handle_client chạy trong event loop asyncio -> nhiều client đồng thời
mà không cần thread (bước 4). Không được có lời gọi blocking (time.sleep, input, đọc file lớn) ở đây.
"""
import argparse
import asyncio
import signal

from common.config import (ENABLE_UDP, FILE_STREAM_BASE, PROTOCOL_VERSION, STREAM_DIRECT_CHAT,
                           STREAM_GLOBAL_CHAT, TCP_PORT, UDP_PORT)
from common.crypto import KeyExchange, SessionKeys
from common.log import get_logger, setup_logging
from common.messages import ProtocolError, T, expect, make
from server.auth import UserStore
from server.discovery import run_announcer
from server.file_relay import FileRelay
from server.router import Router
from server.session_manager import ClientSession, SessionManager
from transport.loss_sim import LossSimulator
from transport.session import UdpSession
from transport.tcp_channel import ChannelClosed, ControlChannel
from transport.udp_endpoint import UdpEndpoint

log = get_logger("server")

HANDSHAKE_TIMEOUT = 10
AUTH_TIMEOUT = 120


class CollabServer:
    def __init__(self, host: str, tcp_port: int, udp_port: int, loss: LossSimulator) -> None:
        self.host, self.tcp_port, self.udp_port = host, tcp_port, udp_port
        self.loss = loss
        self.users = UserStore()
        self.sessions = SessionManager()
        self.router = Router(self.sessions)
        self.files = FileRelay(self.sessions)
        self.endpoint: UdpEndpoint | None = None
        self._stop = asyncio.Event()

    # ================================================================ khởi động
    async def run(self) -> None:
        self.endpoint = await UdpEndpoint.open(self.host, self.udp_port, self.loss)
        tcp = await asyncio.start_server(self.handle_client, self.host, self.tcp_port)
        log.info("TCP lắng nghe tại %s:%s", self.host, self.tcp_port)
        if self.loss.enabled:
            log.warning("ĐANG GIẢ LẬP MẠNG XẤU: %s", self.loss)
        announcer = asyncio.create_task(run_announcer(self.tcp_port, self.udp_port, self._stop))
        async with tcp:
            await self._stop.wait()
        announcer.cancel()
        for c in self.sessions.all():
            await c.control.close()
        self.endpoint.close()
        log.info("server đã dừng")

    def stop(self) -> None:
        self._stop.set()

    # ================================================================ một client
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        ch = ControlChannel(reader, writer)
        log.info("kết nối TCP mới từ %s", ch.peername)
        client: ClientSession | None = None
        try:
            # ---- Bước 2: handshake
            hello = expect(await ch.recv(timeout=HANDSHAKE_TIMEOUT), T.HELLO)
            if hello.get("version") != PROTOCOL_VERSION:
                raise ProtocolError(f"client dùng version {hello.get('version')}")
            await ch.send(make(T.HELLO_ACK, version=PROTOCOL_VERSION, server_name="collab-server"))
            log.info("HELLO/HELLO_ACK xong với %s", ch.peername)

            # ---- Bước 8: trao khóa
            keys = await self._key_exchange(ch)

            # ---- Bước 3: xác thực
            client = await self._authenticate(ch, keys)

            # ---- Phiên đã thiết lập: xử lý message điều khiển
            await self._control_loop(client)
        except ChannelClosed:
            log.info("%s đóng kết nối TCP", client.username if client else ch.peername)
        except (ProtocolError, asyncio.TimeoutError) as exc:
            log.warning("vi phạm giao thức từ %s: %r", ch.peername, exc)
            await self._safe_send(ch, make(T.ERROR, reason=str(exc) or "timeout"))
        except NotImplementedError as exc:
            log.error("chưa cài đặt: %s", exc)
        except Exception:
            log.exception("lỗi không lường trước với %s", ch.peername)   # không bao giờ làm sập server
        finally:
            await self._cleanup(client, ch)

    async def _key_exchange(self, ch: ControlChannel) -> SessionKeys:
            kx = KeyExchange()                                                        # (1)
            msg = expect(await ch.recv(timeout=HANDSHAKE_TIMEOUT), T.KEY_EXCHANGE)    # (2)
            try:
                peer_public = bytes.fromhex(msg["public_key"])                        # (3)
            except (KeyError, ValueError) as exc:
                raise ProtocolError("public_key không hợp lệ") from exc
            if len(peer_public) != 32:                                                # (4)
                raise ProtocolError("public_key phải dài 32 byte")
            await ch.send(make(T.KEY_EXCHANGE, public_key=kx.public_bytes().hex()))   # (5)
            keys = kx.derive(peer_public)                                             # (6)
            ch.enable_encryption(keys.tcp_key)                                        # (7)
            log.info("trao khóa xong với %s, kênh TCP đã mã hóa", ch.peername)
            return keys                                                               # (8)

    async def _authenticate(self, ch: ControlChannel, keys: SessionKeys) -> ClientSession:
        failures = 0
        while True:
            msg = await ch.recv(timeout=AUTH_TIMEOUT)                          # (1)
            mtype = msg.get("type")
            username = str(msg.get("username", ""))
            password = str(msg.get("password", ""))

            if mtype == T.REGISTER:                                            # (2)
                ok, reason = await asyncio.to_thread(self.users.register, username, password)
                if ok:
                    log.info("đăng ký tài khoản mới: %s", username)
                    await ch.send(make(T.REGISTER, ok=True))
                else:
                    await ch.send(make(T.AUTH_FAIL, reason=reason))
                continue

            if mtype != T.AUTH_REQ:                                            # (3)
                raise ProtocolError(f"mong đợi REGISTER hoặc AUTH_REQ, nhận {mtype}")

            if not await asyncio.to_thread(self.users.verify, username, password):                   # (4)
                failures += 1
                log.warning("đăng nhập sai từ %s (user=%r, lần %d)", ch.peername, username, failures)
                await ch.send(make(T.AUTH_FAIL, reason="sai tên đăng nhập hoặc mật khẩu"))
                if failures >= 5:
                    raise ProtocolError("sai mật khẩu quá 5 lần")
                continue

            try:                                                               # (5)
                udp_port = int(msg["udp_port"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ProtocolError("thiếu hoặc sai udp_port") from exc

            client = ClientSession(username, self.sessions.new_session_id(), ch,   # (6)
                                   ip=ch.peername[0], udp_port=udp_port)
            try:
                self.sessions.add(client)                                      # (7)
            except ValueError as exc:
                await ch.send(make(T.AUTH_FAIL, reason=str(exc)))
                continue

            if ENABLE_UDP:                                                     # (8)
                self._start_udp_session(client, keys)
            await ch.send(make(T.AUTH_OK, session_id=client.session_id, udp_port=self.udp_port,
                               users=self.sessions.online_users()))            # (9)
            await self.sessions.broadcast(make(T.USER_JOIN, user=client.public_info()),
                                          exclude=client.username)             # (10)
            log.info("%s đăng nhập: IP %s, cổng TCP %d, cổng UDP %d",
                     username, client.ip, ch.peername[1], client.udp_port)
            self.sessions.print_table()                                        # (11)
            return client

    def _start_udp_session(self, client: ClientSession, keys: SessionKeys) -> None:
        """Tạo phiên UDP cho client đã xác thực và gắn các task định tuyến. (HOÀN THIỆN)"""
        udp = UdpSession(client.session_id, self.endpoint, (client.ip, client.udp_port), keys, is_server=True)
        client.udp = udp

        def on_stream(stream) -> None:
            if stream.stream_id >= FILE_STREAM_BASE:
                self.files.attach_stream(client, stream)

        def on_close(reason: str) -> None:
            # Phiên UDP chết (vd. heartbeat timeout) -> đóng luôn TCP để handle_client dọn dẹp
            log.info("phiên UDP của %s đóng (%s) -> đóng TCP", client.username, reason)
            asyncio.ensure_future(client.control.close())

        udp.on_stream_created = on_stream
        udp.on_close = on_close
        udp.start()
        for sid in (STREAM_GLOBAL_CHAT, STREAM_DIRECT_CHAT):
            client.tasks.append(asyncio.create_task(self.router.pump_stream(client, sid)))

    async def _control_loop(self, client: ClientSession) -> None:
        handlers = {
            T.FILE_OFFER: self.files.on_offer,
            T.FILE_ACCEPT: self.files.on_accept,
            T.FILE_REJECT: self.files.on_reject,
            T.FILE_DONE: self.files.on_done,
        }
        while True:
            msg = await client.control.recv()
            mtype = msg.get("type")
            if mtype == T.LOGOUT:
                log.info("%s đăng xuất", client.username)
                return
            handler = handlers.get(mtype)
            if handler is None:
                await client.control.send(make(T.ERROR, reason=f"message không hỗ trợ: {mtype}"))
                continue
            await handler(client, msg)

    async def _cleanup(self, client: ClientSession | None, ch: ControlChannel) -> None:
        """Dọn dẹp khi client rời đi vì BẤT KỲ lý do gì (bước 7).
        TODO[A-D3]: kiểm tra kỹ mọi đường thoát: LOGOUT, kill client, rút mạng (heartbeat timeout),
        gói rác, và việc gọi _cleanup hai lần không gây lỗi."""
        if client is not None:
            try:
                self.sessions.remove(client.username)
            except NotImplementedError:
                pass
            for task in client.tasks:
                task.cancel()
            if client.udp is not None:
                await client.udp.close("client rời đi")
            await self.files.abort_for(client.username)
            try:
                await self.sessions.broadcast(make(T.USER_LEAVE, username=client.username, reason="left"))
                self.sessions.print_table()
            except NotImplementedError:
                pass
        await ch.close()

    @staticmethod
    async def _safe_send(ch: ControlChannel, msg: dict) -> None:
        try:
            await ch.send(msg)
        except ChannelClosed:
            pass


def main() -> None:
    ap = argparse.ArgumentParser(description="Collab server")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--tcp-port", type=int, default=TCP_PORT)
    ap.add_argument("--udp-port", type=int, default=UDP_PORT)
    ap.add_argument("--loss", type=float, default=0.0, help="tỉ lệ rớt gói UDP giả lập, vd 0.1")
    ap.add_argument("--delay", type=float, default=0.0, help="trễ giả lập (ms)")
    ap.add_argument("--jitter", type=float, default=0.0, help="trễ ngẫu nhiên thêm (ms)")
    ap.add_argument("--log", default=None, help="DEBUG/INFO/WARNING")
    args = ap.parse_args()
    setup_logging(args.log)

    async def runner() -> None:
        server = CollabServer(args.host, args.tcp_port, args.udp_port,
                              LossSimulator(args.loss, args.delay, args.jitter))
        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, server.stop)
            loop.add_signal_handler(signal.SIGTERM, server.stop)
        except NotImplementedError:
            pass  # Windows: Ctrl+C sẽ ném KeyboardInterrupt, xử lý ở dưới
        await server.run()

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        log.info("dừng bởi Ctrl+C")


if __name__ == "__main__":
    main()
