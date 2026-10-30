"""Điểm vào của server.   Chạy:  python -m server.server [--loss 0.1 --delay 30]

Trạng thái: vòng accept + HELLO/HELLO_ACK HOÀN THIỆN (bước 2, 4).
TODO: trao khóa (A-D1), xác thực (B-D1), dọn dẹp nâng cao (A-D3).

Mỗi client = 1 coroutine handle_client chạy trong event loop asyncio -> nhiều client đồng thời
mà không cần thread (bước 4). Không được có lời gọi blocking (time.sleep, input, đọc file lớn) ở đây.
"""
import argparse
import asyncio
import signal

from common.config import (FILE_STREAM_BASE, PROTOCOL_VERSION, STREAM_DIRECT_CHAT,
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
        """TODO[A-D1]:
          1. kx = KeyExchange()
          2. msg = expect(await ch.recv(timeout=HANDSHAKE_TIMEOUT), T.KEY_EXCHANGE)  # khóa công khai của client
          3. await ch.send(make(T.KEY_EXCHANGE, public_key=kx.public_bytes().hex()))   # gửi RÕ, trước khi bật mã hóa
          4. keys = kx.derive(bytes.fromhex(msg["public_key"]))
          5. ch.enable_encryption(keys.tcp_key); return keys
        Ghi vào báo cáo: chưa xác thực khóa công khai -> vẫn có thể bị MITM (giới hạn đã biết).
        """
        raise NotImplementedError("TODO[A-D1] CollabServer._key_exchange")

    async def _authenticate(self, ch: ControlChannel, keys: SessionKeys) -> ClientSession:
        """TODO[B-D1]: lặp nhận message với timeout AUTH_TIMEOUT:
          - REGISTER {username, password}: self.users.register(...)
                -> trả AUTH_FAIL{reason} nếu lỗi, hoặc make(T.REGISTER, ok=True) nếu thành công; tiếp tục lặp
          - AUTH_REQ {username, password, udp_port}:
                * sai mật khẩu -> AUTH_FAIL {reason}; tiếp tục lặp (có thể giới hạn 5 lần)
                * đã online   -> AUTH_FAIL {reason: "đã đăng nhập ở nơi khác"}
                * đúng: client = ClientSession(username, self.sessions.new_session_id(), ch,
                                               ip=ch.peername[0], udp_port=msg["udp_port"])
                        self.sessions.add(client)
                        self._start_udp_session(client, keys)
                        await ch.send(make(T.AUTH_OK, session_id=..., udp_port=self.udp_port,
                                           users=self.sessions.online_users()))
                        await self.sessions.broadcast(make(T.USER_JOIN, user=client.public_info()),
                                                      exclude=client.username)
                        self.sessions.print_table(); return client
          - message khác -> raise ProtocolError
        """
        raise NotImplementedError("TODO[B-D1] CollabServer._authenticate")

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
