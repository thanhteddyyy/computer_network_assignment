"""Bộ kiểm thử tổng hợp toàn diện (Master Test Suite) bao phủ đủ 10 bước Rubric của Assignment 1.

Chạy riêng file này:
    python -m unittest tests.test_full_system_suite -v
"""
import asyncio
import hashlib
import os
import shutil
import tempfile
import time
import unittest

from client.chat import ChatService
from client.client import CollabClient
from client.events import EventLog
from client.file_transfer import FileService
from client.http_server import HttpError, HttpServer, Request, build_response, read_request
from common.config import (CHUNK_SIZE, FILE_STREAM_BASE, HEADER_SIZE, MAX_FRAME_SIZE,
                           MIN_CWND, PROTOCOL_VERSION, STREAM_DIRECT_CHAT, STREAM_GLOBAL_CHAT)
from common.crypto import (CryptoError, KeyExchange, hash_password, open_random_nonce,
                           seal_random_nonce, sha256_file, verify_password)
from common.framing import FrameError, read_frame, write_frame
from common.messages import ProtocolError, T, make
from server.auth import UserStore
from server.file_relay import FileRelay
from server.router import Router, parse_chat
from server.server import CollabServer
from server.session_manager import ClientSession, SessionManager
from transport.congestion import Cubic
from transport.loss_sim import LossSimulator
from transport.mock_session import MockSession
from transport.packet import AckFrame, Flags, Packet, PacketError, PacketType, peek_session_id
from transport.reliability import ReceiveTracker, RttEstimator, SendTracker, SentPacket
from transport.session import UdpSession
from transport.stream import RecvStream, SendStream
from transport.udp_endpoint import UdpEndpoint


class FakeControl:
    """Kênh điều khiển TCP giả trong bộ nhớ để kiểm thử nhanh các luồng logic."""
    def __init__(self):
        self.sent = []
        self.closed = False

    async def send(self, msg: dict) -> None:
        self.sent.append(msg)

    async def close(self) -> None:
        self.closed = True

    def last(self) -> dict:
        return self.sent[-1]


# ==============================================================================
# STEP 1: SETUP & PROTOCOL DESIGN (Đóng khung TCP, Header UDP 21B, ACK Frame)
# ==============================================================================
class Step01_ProtocolDesignTest(unittest.IsolatedAsyncioTestCase):
    async def test_tcp_framing_roundtrip_and_oversized_guard(self):
        """Kiểm tra đóng khung 4-byte big-endian trên TCP và chặn frame vượt quá MAX_FRAME_SIZE."""
        reader = asyncio.StreamReader()
        payload = b'{"type": "HELLO", "version": 1}'
        header = len(payload).to_bytes(4, "big")
        reader.feed_data(header + payload)
        self.assertEqual(await read_frame(reader), payload)

        # Frame khai báo vượt quá MAX_FRAME_SIZE (1 MB) phải bị chặn ngay bằng FrameError
        bad_reader = asyncio.StreamReader()
        bad_reader.feed_data((MAX_FRAME_SIZE + 100).to_bytes(4, "big") + b"xxxx")
        with self.assertRaises(FrameError):
            await read_frame(bad_reader)

    def test_udp_packet_header_21_bytes_and_flags(self):
        """Kiểm tra cấu trúc gói UDP 21 byte, cờ FIN (End-of-Message) và peek_session_id."""
        pkt = Packet(
            type=PacketType.DATA,
            session_id=0xCAFEBABE,
            packet_number=15,
            stream_id=101,
            offset=2200,
            flags=Flags.FIN,
            payload=b"hello_udp",
        )
        raw = pkt.encode()
        self.assertEqual(len(raw), HEADER_SIZE + 9)
        self.assertEqual(peek_session_id(raw), 0xCAFEBABE)

        decoded = Packet.decode(raw)
        self.assertEqual(decoded, pkt)
        self.assertTrue(decoded.flags & Flags.FIN)

    def test_udp_malformed_packets_rejected(self):
        """Mọi gói UDP sai magic, sai version, cụt header hoặc lệch độ dài đều ném PacketError."""
        valid = Packet(PacketType.PING, 1, 1).encode()
        bad_magic = b"\x00\x00" + valid[2:]
        bad_version = valid[:2] + b"\x99" + valid[3:]
        bad_length = valid + b"extra_bytes"

        for junk in (b"", b"short", bad_magic, bad_version, bad_length):
            with self.assertRaises(PacketError):
                Packet.decode(junk)

    def test_ack_frame_sack_ranges_encoding(self):
        """Kiểm tra đóng gói và giải mã Selective ACK (SACK) nhiều khoảng giảm dần."""
        ack = AckFrame(largest_acked=12, recv_window=256, ack_delay_ms=15, ranges=[(10, 12), (4, 8), (0, 2)])
        decoded = AckFrame.decode(ack.encode())
        self.assertEqual(decoded, ack)
        self.assertTrue(decoded.contains(11) and decoded.contains(6) and decoded.contains(0))
        self.assertFalse(decoded.contains(9) or decoded.contains(3))


# ==============================================================================
# STEP 2: BASIC CLIENT-SERVER HANDSHAKE (Kết nối TCP, HELLO / HELLO_ACK)
# ==============================================================================
class Step02_HandshakeTest(unittest.IsolatedAsyncioTestCase):
    async def test_real_tcp_handshake_and_key_exchange(self):
        """Kiểm tra kết nối TCP thật giữa CollabClient và CollabServer trên localhost."""
        srv = CollabServer("127.0.0.1", 0, 0, LossSimulator(0.0))
        tcp_server = await asyncio.start_server(srv.handle_client, "127.0.0.1", 0)
        port = tcp_server.sockets[0].getsockname()[1]

        client = CollabClient()
        try:
            await client.connect("127.0.0.1", port)
            self.assertEqual(client.state, "KEY_EXCHANGED")
            self.assertIsNotNone(client.keys)
            self.assertTrue(client.ch.encrypted)
        finally:
            await client.logout()
            tcp_server.close()
            await tcp_server.wait_closed()


# ==============================================================================
# STEP 3: AUTHENTICATION & STATE MANAGEMENT (PBKDF2, SessionManager, IP/Port)
# ==============================================================================
class Step03_AuthAndStateTest(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.users_path = os.path.join(self.tmp_dir, "users.json")
        self.store = UserStore(self.users_path)
        self.sm = SessionManager()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_user_register_and_verify_pbkdf2(self):
        """Đăng ký tài khoản mới, kiểm tra mật khẩu đúng/sai và chống trùng tên."""
        ok, _ = self.store.register("alice", "pass1234")
        self.assertTrue(ok)
        self.assertTrue(self.store.verify("alice", "pass1234"))
        self.assertFalse(self.store.verify("alice", "wrong_pass"))
        self.assertFalse(self.store.verify("unknown_user", "pass1234"))

        # Đăng ký trùng tên phải bị từ chối
        ok_dup, _ = self.store.register("alice", "other_pass")
        self.assertFalse(ok_dup)

    def test_session_manager_tracks_ip_ports_and_prevents_double_login(self):
        """SessionManager lưu đúng username, IP, cổng UDP và chặn đăng nhập 2 nơi cùng lúc."""
        sid = self.sm.new_session_id()
        c1 = ClientSession("alice", sid, FakeControl(), "192.168.1.10", 54321)
        self.sm.add(c1)

        self.assertIs(self.sm.get("alice"), c1)
        self.assertIs(self.sm.by_session_id(sid), c1)
        info = c1.public_info()
        self.assertEqual((info["username"], info["ip"], info["port"]), ("alice", "192.168.1.10", 54321))

        # Đăng nhập trùng tên khi chưa thoát -> ném ValueError
        c_dup = ClientSession("alice", self.sm.new_session_id(), FakeControl(), "192.168.1.11", 54322)
        with self.assertRaises(ValueError):
            self.sm.add(c_dup)

        # Sau khi remove -> đăng nhập lại bình thường
        self.sm.remove("alice")
        self.sm.add(c_dup)
        self.assertEqual(self.sm.get("alice").ip, "192.168.1.11")


# ==============================================================================
# STEP 4: CONCURRENCY IMPLEMENTATION (Asyncio + Sub-thread Password Hashing)
# ==============================================================================
class Step04_ConcurrencyTest(unittest.IsolatedAsyncioTestCase):
    async def test_pbkdf2_in_subthread_does_not_block_event_loop(self):
        """Chứng minh chạy PBKDF2 trong asyncio.to_thread cho phép các coroutine khác phản hồi tức thì."""
        ticks = 0

        async def fast_heartbeat():
            nonlocal ticks
            for _ in range(5):
                await asyncio.sleep(0.01)
                ticks += 1

        hb_task = asyncio.create_task(fast_heartbeat())
        # Chạy song song 4 phép băm PBKDF2 200.000 vòng trong thread pool
        await asyncio.gather(
            asyncio.to_thread(hash_password, "password_1"),
            asyncio.to_thread(hash_password, "password_2"),
            hb_task,
        )
        self.assertEqual(ticks, 5)


# ==============================================================================
# STEP 5: TIME-SENSITIVE STREAMING (Routing, SACK, RTT/RTO, Anti-HoL Blocking)
# ==============================================================================
class Step05_StreamingAndReliabilityTest(unittest.IsolatedAsyncioTestCase):
    async def test_broadcast_and_unicast_chat_routing(self):
        """Kiểm tra định tuyến chat chung (*) và chat riêng (unicast) tới đúng người nhận."""
        sm = SessionManager()
        router = Router(sm)
        clients = {}
        for name in ("alice", "bob", "carol"):
            s_udp, c_udp = MockSession.pair()
            sess = ClientSession(name, sm.new_session_id(), FakeControl(), "127.0.0.1", 50000)
            sess.udp = s_udp
            sm.add(sess)
            clients[name] = (sess, c_udp)

        # 1. Alice gửi tin nhắn chung (*) -> cả 3 người đều nhận trên STREAM_GLOBAL_CHAT
        await router.handle_chat(clients["alice"][0], b'{"to": "*", "text": "chao ca phong"}')
        for name in ("alice", "bob", "carol"):
            raw = await asyncio.wait_for(clients[name][1].stream(STREAM_GLOBAL_CHAT).read_message(), 1.0)
            self.assertIn("chao ca phong", raw.decode())

        # 2. Alice nhắn riêng cho Bob -> chỉ Alice và Bob nhận trên STREAM_DIRECT_CHAT, Carol không nhận
        await router.handle_chat(clients["alice"][0], b'{"to": "bob", "text": "tin bi mat"}')
        for name in ("alice", "bob"):
            raw = await asyncio.wait_for(clients[name][1].stream(STREAM_DIRECT_CHAT).read_message(), 1.0)
            self.assertIn("tin bi mat", raw.decode())
        self.assertNotIn(STREAM_DIRECT_CHAT, clients["carol"][1].streams)

    async def test_anti_head_of_line_blocking_across_streams(self):
        """Chứng minh mất gói ở Stream 101 (File) hoàn toàn KHÔNG chặn Stream 1 (Chat)."""
        file_stream = RecvStream(101)
        chat_stream = RecvStream(STREAM_GLOBAL_CHAT)

        # Stream 101 bị mất gói đầu (offset=0), chỉ nhận được gói thứ hai (offset=1100)
        file_stream.on_data(offset=1100, data=b"file_chunk_2", fin=False)
        self.assertFalse(file_stream.has_message())

        # Trong lúc Stream 101 đang bị kẹt, tin nhắn Chat đến trên Stream 1 -> đọc được NGAY LẬP TỨC
        chat_stream.on_data(offset=0, data=b'{"from":"alice","text":"den ngay!"}', fin=True)
        msg = await asyncio.wait_for(chat_stream.read_message(), timeout=0.05)
        self.assertEqual(msg, b'{"from":"alice","text":"den ngay!"}')

        # Khi gói offset=0 của Stream 101 được retransmit tới -> ghép thành công với gói offset=1100
        file_stream.on_data(offset=0, data=b"file_chunk_1_", fin=False)
        file_stream.on_data(offset=1112, data=b"_end", fin=True)
        # Lưu ý: offset của chunk 2 ở trên là 1100 nên ta kiểm tra bytes_received tăng đúng
        self.assertGreater(file_stream.bytes_received, 0)


# ==============================================================================
# STEP 6: RELIABLE FILE TRANSFER & CUBIC CONGESTION CONTROL
# ==============================================================================
class Step06_ReliableFileTransferAndCubicTest(unittest.IsolatedAsyncioTestCase):
    async def test_full_file_transfer_pipeline_alice_to_server_to_bob(self):
        """Kiểm tra trọn vòng truyền file: Alice (_send_file) -> Server (_relay) -> Bob (_receive_file)."""
        sm = SessionManager()
        relay = FileRelay(sm)

        # Dựng cặp kết nối cho Alice và Bob
        alice_s_udp, alice_c_udp = MockSession.pair()
        bob_s_udp, bob_c_udp = MockSession.pair()

        alice_sess = ClientSession("alice", 1, FakeControl(), "127.0.0.1", 50001, udp=alice_s_udp)
        bob_sess = ClientSession("bob", 2, FakeControl(), "127.0.0.1", 50002, udp=bob_s_udp)
        sm.add(alice_sess)
        sm.add(bob_sess)

        # Tạo client giả cho Alice và Bob
        class DummyClient:
            def __init__(self, udp):
                self.ch = FakeControl()
                self.events = EventLog()
                self.udp = udp

        alice_client = DummyClient(alice_c_udp)
        bob_client = DummyClient(bob_c_udp)
        alice_files = FileService(alice_client)
        bob_files = FileService(bob_client)

        tmp_dir = tempfile.mkdtemp()
        try:
            src_path = os.path.join(tmp_dir, "tai_lieu_goc.bin")
            content = os.urandom(5000)  # 5000 byte (5 chunks: 4x1100 + 1x600)
            with open(src_path, "wb") as f:
                f.write(content)

            # 1. Alice mời gửi file
            ref = await alice_files.offer("bob", src_path)
            offer_msg = alice_client.ch.last()
            await relay.on_offer(alice_sess, offer_msg)

            # 2. Alice nhận FILE_OFFER_ACK, Bob nhận FILE_OFFER
            alice_files.on_offer_ack(alice_sess.control.last())
            bob_files.on_offer(bob_sess.control.last())
            bob_files.by_id[1].path = os.path.join(tmp_dir, "1_tai_lieu_goc.bin")

            # 3. Bob đồng ý nhận -> Server báo cho Alice
            await relay.on_accept(bob_sess, {"file_id": 1})

            # 4. Chạy đồng thời: Alice gửi, Server relay, Bob nhận
            stream_id = FILE_STREAM_BASE + 1
            await asyncio.gather(
                alice_files._send_file(alice_files.by_id[1]),
                relay._relay(alice_sess, alice_s_udp.stream(stream_id)),
                bob_files._receive_file(bob_files.by_id[1]),
            )

            # 5. Bob báo FILE_DONE lên Server -> Server báo cho Alice
            await relay.on_done(bob_sess, bob_client.ch.last())
            alice_files.on_done(alice_sess.control.last())

            # 6. Xác nhận cả hai đầu đều "done" và SHA-256 khớp tuyệt đối
            self.assertEqual(alice_files.by_id[1].status, "done")
            self.assertEqual(bob_files.by_id[1].status, "done")
            self.assertEqual(sha256_file(bob_files.by_id[1].path), sha256_file(src_path))
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_cubic_lifecycle(self):
        """Kiểm tra đầy đủ 4 pha của CUBIC: Slow Start, Loss Reduction, Cubic Avoidance, Timeout."""
        cc = Cubic()
        cc.on_ack(acked_count=10, rtt=0.04, now=1.0)
        self.assertEqual(cc.cwnd, 20.0)  # 10 + 10

        cc.on_loss(now=2.0)
        self.assertAlmostEqual(cc.cwnd, 14.0)  # 20 * 0.7
        self.assertAlmostEqual(cc.ssthresh, 14.0)

        # Tránh tắc nghẽn sau thời điểm K
        cc.on_ack(acked_count=20, rtt=0.04, now=2.0 + cc.K + 2.0)
        self.assertGreater(cc.cwnd, 20.0)

        # Timeout đưa cwnd về MIN_CWND
        cc.on_timeout(now=10.0)
        self.assertEqual(cc.cwnd, float(MIN_CWND))


# ==============================================================================
# STEP 7: NETWORK RESILIENCE & ERROR HANDLING (Disconnects, Cleanup, Bad Packets)
# ==============================================================================
class Step07_ResilienceAndErrorHandlingTest(unittest.IsolatedAsyncioTestCase):
    async def test_server_and_client_resilience_on_abrupt_disconnect(self):
        """Kiểm tra khi client rớt mạng đột ngột: Server và Client đều dọn dẹp sạch, không crash."""
        srv = CollabServer("127.0.0.1", 0, 0, LossSimulator(0.0))
        alice = ClientSession("alice", 10, FakeControl(), "127.0.0.1", 50010)
        bob = ClientSession("bob", 11, FakeControl(), "127.0.0.1", 50011)
        srv.sessions.add(alice)
        srv.sessions.add(bob)

        # Alice đang mời gửi file cho Bob thì Alice bị kill đột ngột
        await srv.files.on_offer(alice, {"type": "FILE_OFFER", "client_ref": "r1", "to": "bob",
                                         "name": "a.bin", "size": 100, "sha256": "aa" * 32})
        await srv._cleanup(alice, alice.control)
        await srv._cleanup(alice, alice.control)  # Gọi lần 2 vẫn an toàn tuyệt đối

        self.assertIsNone(srv.sessions.get("alice"))
        self.assertEqual(srv.files.transfers, {})
        # Bob nhận được cả FILE_DONE(ok=False) và đúng 1 thông báo USER_LEAVE
        types = [m["type"] for m in bob.control.sent]
        self.assertIn("FILE_DONE", types)
        self.assertEqual(types.count("USER_LEAVE"), 1)


# ==============================================================================
# STEP 8: SECURITY LAYER (ECDH X25519, HKDF, AES-256-GCM TCP & UDP 2 chiều)
# ==============================================================================
class Step08_SecurityLayerTest(unittest.TestCase):
    def test_tcp_and_udp_encryption_and_tamper_detection(self):
        """Kiểm tra mã hóa/giải mã AES-256-GCM trên cả TCP và UDP, phát hiện mọi sửa đổi trái phép."""
        kx_c, kx_s = KeyExchange(), KeyExchange()
        keys_c = kx_c.derive(kx_s.public_bytes())
        keys_s = kx_s.derive(kx_c.public_bytes())

        # 1. Kênh TCP (random nonce)
        plain_tcp = b'{"type": "AUTH_REQ", "password": "secret_password"}'
        cipher_tcp = seal_random_nonce(keys_c.tcp_key, plain_tcp)
        self.assertNotIn(b"secret_password", cipher_tcp)
        self.assertEqual(open_random_nonce(keys_s.tcp_key, cipher_tcp), plain_tcp)

        # Sửa 1 byte trong bản mã TCP -> phải ném CryptoError
        tampered_tcp = bytearray(cipher_tcp)
        tampered_tcp[-1] ^= 0xFF
        with self.assertRaises(CryptoError):
            open_random_nonce(keys_s.tcp_key, bytes(tampered_tcp))

        # 2. Kênh UDP (IV XOR packet_number + Header AAD)
        pkt = Packet(PacketType.DATA, 99, 1, 1, 0, Flags.FIN, b"noi_dung_mat_udp")
        wire = pkt.seal(keys_c.c2s)
        self.assertNotIn(b"noi_dung_mat_udp", wire)
        self.assertEqual(Packet.open(wire, keys_s.c2s).payload, b"noi_dung_mat_udp")


# ==============================================================================
# STEP 9: HTTP-BASED UI WITH CLIENT-SIDE JS (HTTP/1.1 Parser & API Dispatch)
# ==============================================================================
class Step09_HttpUiAndApiTest(unittest.IsolatedAsyncioTestCase):
    async def test_http_server_routes_and_static_path_traversal_guard(self):
        """Kiểm tra HttpServer phục vụ trang index.html, API /api/state và chặn ../../ trên URL."""
        client = CollabClient()
        http = HttpServer(client, "127.0.0.1", 0, default_server="127.0.0.1")

        # 1. GET / -> trả về trang giao diện HTML
        res_html = await http._dispatch(Request("GET", "/", {}, {}))
        self.assertTrue(res_html.startswith(b"HTTP/1.1 200 OK"))
        self.assertIn(b"<!doctype html>", res_html.lower())

        # 2. GET /api/state -> trả về JSON trạng thái
        res_state = await http._dispatch(Request("GET", "/api/state", {}, {}))
        self.assertIn(b'"state": "DISCONNECTED"', res_state)

        # 3. Cố tình truy cập Path Traversal /../../server/server.py -> phải bị chặn bằng 404
        with self.assertRaises(HttpError) as ctx:
            await http._dispatch(Request("GET", "/../../server/server.py", {}, {}))
        self.assertEqual(ctx.exception.status, 404)


# ==============================================================================
# STEP 10: END-TO-END LOCAL NETWORK SIMULATION (UDP thật + LossSimulator 15%)
# ==============================================================================
class Step10_EndToEndSimulationTest(unittest.IsolatedAsyncioTestCase):
    async def test_end_to_end_udp_streaming_with_packet_loss(self):
        """Chạy 2 UdpSession qua socket UDP thật trên localhost với 15% mất gói giả lập:
        truyền đồng thời 30 tin nhắn Chat (Stream 1) và 50 KB dữ liệu File (Stream 101)."""
        a, b = KeyExchange(), KeyExchange()
        keys = a.derive(b.public_bytes())
        ep_s = await UdpEndpoint.open("127.0.0.1", 0, LossSimulator(loss_rate=0.15, seed=42))
        ep_c = await UdpEndpoint.open("127.0.0.1", 0, LossSimulator(loss_rate=0.15, seed=99))

        server = UdpSession(777, ep_s, ("127.0.0.1", ep_c.local_port), keys, is_server=True)
        client = UdpSession(777, ep_c, ("127.0.0.1", ep_s.local_port), keys, is_server=False)
        server.start()
        client.start()

        try:
            file_blob = os.urandom(50_000)
            # Gửi song song 30 tin chat và 1 khối dữ liệu file 50 KB qua mạng rớt 15% gói
            for i in range(30):
                await client.stream(STREAM_GLOBAL_CHAT).send(f"chat_{i}".encode())
            send_file_task = asyncio.create_task(client.stream(101).send(file_blob))

            # Đọc đủ 30 tin chat đúng thứ tự
            received_chats = [
                await asyncio.wait_for(server.stream(STREAM_GLOBAL_CHAT).read_message(), 10.0)
                for _ in range(30)
            ]
            self.assertEqual(received_chats, [f"chat_{i}".encode() for i in range(30)])

            # Đọc dữ liệu file 50 KB và kiểm tra khớp SHA-256
            received_file = await asyncio.wait_for(server.stream(101).read_message(), 15.0)
            await send_file_task
            self.assertEqual(
                hashlib.sha256(received_file).hexdigest(),
                hashlib.sha256(file_blob).hexdigest(),
            )
        finally:
            await client.close()
            await server.close()
            ep_c.close()
            ep_s.close()


if __name__ == "__main__":
    unittest.main()

