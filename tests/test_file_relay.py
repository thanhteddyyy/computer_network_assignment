"""Step 6a (phía server): luồng thỏa thuận gửi file qua TCP, dùng kênh TCP giả."""
import unittest
import asyncio
from transport.mock_session import MockSession
from common.messages import ProtocolError
from server.file_relay import FileRelay
from server.session_manager import ClientSession, SessionManager


class FakeControl:
    """Kênh TCP giả: ghi lại mọi message server gửi cho người này."""
    def __init__(self):
        self.sent = []

    async def send(self, msg):
        self.sent.append(msg)

    async def close(self):
        pass

    def last(self):
        return self.sent[-1]


def offer(to="bob", ref="r1", name="bao_cao.pdf", size=5000, sha="ab" * 32):
    return {"type": "FILE_OFFER", "client_ref": ref, "to": to, "name": name, "size": size, "sha256": sha}


class FileRelayTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.sm = SessionManager()
        self.relay = FileRelay(self.sm)
        self.alice, self.bob, self.carol = (self._user(n) for n in ("alice", "bob", "carol"))

    def _user(self, name):
        c = ClientSession(name, self.sm.new_session_id(), FakeControl(), "127.0.0.1", 50000)
        self.sm.add(c)
        return c

    # ---------- FILE_OFFER
    async def test_moi_gui_thanh_cong(self):
        await self.relay.on_offer(self.alice, offer())
        ack = self.alice.control.last()
        self.assertEqual((ack["type"], ack["client_ref"], ack["file_id"]), ("FILE_OFFER_ACK", "r1", 1))
        moi = self.bob.control.last()
        self.assertEqual((moi["type"], moi["file_id"], moi["from"], moi["name"], moi["size"]),
                         ("FILE_OFFER", 1, "alice", "bao_cao.pdf", 5000))
        self.assertEqual(self.relay.transfers[1].status, "offered")
        self.assertEqual(self.carol.control.sent, [])                  # carol không liên quan

    async def test_moi_nguoi_khong_online(self):
        await self.relay.on_offer(self.alice, offer(to="dave"))
        self.assertIn("error", self.alice.control.last())
        self.assertEqual(self.relay.transfers, {})

    async def test_moi_chinh_minh(self):
        await self.relay.on_offer(self.alice, offer(to="alice"))
        self.assertIn("error", self.alice.control.last())

    async def test_moi_thieu_truong(self):
        with self.assertRaises(ProtocolError):
            await self.relay.on_offer(self.alice, {"type": "FILE_OFFER", "to": "bob"})

    # ---------- FILE_ACCEPT / FILE_REJECT
    async def test_dong_y(self):
        await self.relay.on_offer(self.alice, offer())
        await self.relay.on_accept(self.bob, {"file_id": 1})
        self.assertEqual(self.relay.transfers[1].status, "accepted")
        self.assertEqual(self.alice.control.last(), {"type": "FILE_ACCEPT", "file_id": 1})

    async def test_nguoi_khac_khong_duoc_dong_y_thay(self):
        await self.relay.on_offer(self.alice, offer())
        await self.relay.on_accept(self.carol, {"file_id": 1})
        self.assertEqual(self.carol.control.last()["type"], "ERROR")
        self.assertEqual(self.relay.transfers[1].status, "offered")

    async def test_dong_y_hai_lan(self):
        await self.relay.on_offer(self.alice, offer())
        await self.relay.on_accept(self.bob, {"file_id": 1})
        await self.relay.on_accept(self.bob, {"file_id": 1})
        self.assertEqual(self.bob.control.last()["type"], "ERROR")

    async def test_tu_choi(self):
        await self.relay.on_offer(self.alice, offer())
        await self.relay.on_reject(self.bob, {"file_id": 1, "reason": "không cần"})
        self.assertEqual(self.alice.control.last()["type"], "FILE_REJECT")
        self.assertNotIn(1, self.relay.transfers)

    # ---------- FILE_DONE
    async def test_ket_thuc(self):
        await self.relay.on_offer(self.alice, offer())
        await self.relay.on_accept(self.bob, {"file_id": 1})
        await self.relay.on_done(self.bob, {"file_id": 1, "ok": True})
        done = self.alice.control.last()
        self.assertEqual((done["type"], done["file_id"], done["ok"]), ("FILE_DONE", 1, True))
        self.assertNotIn(1, self.relay.transfers)

    async def test_file_id_tang_dan(self):
        await self.relay.on_offer(self.alice, offer(ref="r1"))
        await self.relay.on_offer(self.alice, offer(ref="r2", to="carol"))
        self.assertEqual(self.alice.control.last()["file_id"], 2)
    # ---------- RELAY DATA QUA UDP (Step 6b-1)
    async def test_relay_file_thanh_cong(self):
        # Thiết lập MockSession UDP cho alice và bob
        alice_server_udp, alice_client_udp = MockSession.pair()
        bob_server_udp, bob_client_udp = MockSession.pair()
        self.alice.udp = alice_server_udp
        self.bob.udp = bob_server_udp

        # 1. Thỏa thuận file 6 byte qua TCP
        await self.relay.on_offer(self.alice, offer(name="test.bin", size=6))
        await self.relay.on_accept(self.bob, {"file_id": 1})

        # 2. Server bắt đầu relay khi stream 101 xuất hiện
        stream_sender = self.alice.udp.stream(101)
        relay_task = asyncio.create_task(self.relay._relay(self.alice, stream_sender))

        # 3. Alice client đẩy 2 chunk (4 byte + 2 byte) lên stream 101
        await alice_client_udp.stream(101).send(b"1234")
        await alice_client_udp.stream(101).send(b"56")

        # 4. Bob client đọc từ stream 101 của mình
        chunk1 = await bob_client_udp.stream(101).read_message()
        chunk2 = await bob_client_udp.stream(101).read_message()
        self.assertEqual(chunk1 + chunk2, b"123456")

        await relay_task
        t = self.relay.transfers[1]
        self.assertEqual(t.relayed, 6)
        self.assertEqual(t.status, "relaying")

    async def test_relay_nguoi_nhan_mat_ket_noi(self):
        alice_server_udp, alice_client_udp = MockSession.pair()
        self.alice.udp = alice_server_udp
        self.bob.udp = None  # Giả sử bob chưa hoặc mất kết nối UDP

        await self.relay.on_offer(self.alice, offer(size=10))
        await self.relay.on_accept(self.bob, {"file_id": 1})

        stream_sender = self.alice.udp.stream(101)
        await self.relay._relay(self.alice, stream_sender)

        # Alice phải nhận được FILE_DONE ok=False
        done_msg = self.alice.control.last()
        self.assertEqual((done_msg["type"], done_msg["ok"]), ("FILE_DONE", False))
    # ---------- DỌN DẸP KHI RỚT MẠNG (Step 7a: abort_for)
    async def test_abort_for_khi_nguoi_gui_roi_di(self):
        # Alice gửi cho Bob (file #1), đồng thời Bob gửi cho Carol (file #2)
        await self.relay.on_offer(self.alice, offer(to="bob", ref="r1"))
        await self.relay.on_accept(self.bob, {"file_id": 1})
        await self.relay.on_offer(self.bob, offer(to="carol", ref="r2"))

        # Alice đột ngột rời mạng
        self.sm.remove("alice")
        await self.relay.abort_for("alice")

        # File #1 (của Alice) phải bị xóa và Bob nhận được FILE_DONE ok=False
        self.assertNotIn(1, self.relay.transfers)
        done = self.bob.control.last()
        self.assertEqual((done["type"], done["file_id"], done["ok"]), ("FILE_DONE", 1, False))

        # File #2 (giữa Bob và Carol) không liên quan tới Alice nên vẫn còn nguyên
        self.assertIn(2, self.relay.transfers)

    async def test_abort_for_khi_nguoi_nhan_roi_di(self):
        await self.relay.on_offer(self.alice, offer(to="bob", ref="r1"))

        # Bob đột ngột rời mạng khi lời mời đang chờ
        self.sm.remove("bob")
        await self.relay.abort_for("bob")

        # File #1 bị xóa và Alice nhận được FILE_DONE ok=False
        self.assertNotIn(1, self.relay.transfers)
        done = self.alice.control.last()
        self.assertEqual((done["type"], done["file_id"], done["ok"]), ("FILE_DONE", 1, False))


    async def test_server_cleanup_idempotent(self):
        from server.server import CollabServer
        from transport.loss_sim import LossSimulator

        srv = CollabServer("127.0.0.1", 0, 0, LossSimulator(0.0))
        srv.sessions = self.sm
        srv.files = self.relay

        # Gắn phiên UDP giả và 1 background task đang chạy cho alice
        alice_s_udp, _ = MockSession.pair()
        self.alice.udp = alice_s_udp
        bg_task = asyncio.create_task(asyncio.sleep(3600))
        self.alice.tasks.append(bg_task)

        # Alice đang gửi file dở cho Bob
        await self.relay.on_offer(self.alice, offer(to="bob", ref="r1"))

        # Gọi _cleanup LẦN 1 và LẦN 2 liên tiếp
        await srv._cleanup(self.alice, self.alice.control)
        await srv._cleanup(self.alice, self.alice.control)

        # 1. Alice đã bị xóa khỏi SessionManager, task nền đã bị cancel, UDP đã đóng
        self.assertIsNone(self.sm.get("alice"))
        self.assertTrue(bg_task.cancelled())
        self.assertEqual(self.alice.tasks, [])
        self.assertIsNone(self.alice.udp)
        self.assertTrue(alice_s_udp.closed)

        # 2. Bob chỉ nhận ĐÚNG 1 bản tin USER_LEAVE (dù _cleanup được gọi 2 lần)
        leave_msgs = [m for m in self.bob.control.sent if m.get("type") == "USER_LEAVE"]
        self.assertEqual(len(leave_msgs), 1)
        self.assertEqual(leave_msgs[0]["username"], "alice")



if __name__ == "__main__":
    unittest.main()