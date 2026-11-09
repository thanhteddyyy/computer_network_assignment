"""Step 6a-2 (phía client): mời gửi file, nhận mã vận đơn, đồng ý/từ chối nhận."""
import os
import tempfile
import unittest

from client.events import EventLog
from client.file_transfer import FileService
from common.config import CHUNK_SIZE
from common.crypto import sha256_file
from transport.mock_session import MockSession
import asyncio

class FakeControl:
    def __init__(self):
        self.sent = []

    async def send(self, msg):
        self.sent.append(msg)


class FakeClient:
    """Client giả: chỉ có kênh TCP giả (ch) và sổ sự kiện (events)."""
    def __init__(self):
        self.ch = FakeControl()
        self.events = EventLog()


def loai_su_kien(client, kind):
    return [e for e in client.events.since(0) if e["kind"] == kind]


class FileClientTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = FakeClient()
        self.files = FileService(self.client)
        fd, self.path = tempfile.mkstemp(suffix=".txt")
        with os.fdopen(fd, "wb") as f:
            f.write(b"xin chao " * 1000)                  # file thử 9000 byte

    async def asyncTearDown(self):
        os.remove(self.path)

    # ---------- phía GỬI
    async def test_offer_gui_dung_thong_tin(self):
        ref = await self.files.offer("bob", self.path)
        msg = self.client.ch.sent[-1]
        self.assertEqual(msg["type"], "FILE_OFFER")
        self.assertEqual((msg["client_ref"], msg["to"], msg["size"]), (ref, "bob", 9000))
        self.assertEqual(msg["name"], os.path.basename(self.path))
        self.assertEqual(msg["sha256"], sha256_file(self.path))

    async def test_offer_file_khong_ton_tai(self):
        ref = await self.files.offer("bob", "khong_co_file_nay.bin")
        self.assertEqual(ref, "")
        self.assertEqual(self.client.ch.sent, [])          # không gửi gì
        self.assertEqual(len(loai_su_kien(self.client, "error")), 1)

    async def test_offer_ack_gan_file_id(self):
        ref = await self.files.offer("bob", self.path)
        self.files.on_offer_ack({"client_ref": ref, "file_id": 7})
        self.assertEqual(self.files.by_id[7].client_ref, ref)
        self.assertEqual(self.files.by_ref[ref].file_id, 7)

    async def test_offer_ack_bao_loi(self):
        ref = await self.files.offer("dave", self.path)
        self.files.on_offer_ack({"client_ref": ref, "error": "không gửi được cho dave"})
        self.assertEqual(self.files.by_ref[ref].status, "failed")
        self.assertEqual(len(loai_su_kien(self.client, "error")), 1)

    # ---------- phía NHẬN
    async def _co_loi_moi(self, file_id=3):
        self.files.on_offer({"file_id": file_id, "from": "alice", "name": "../../bi_mat.txt",
                             "size": 9000, "sha256": "ab" * 32})

    async def test_loi_moi_chong_path_traversal(self):
        await self._co_loi_moi()
        t = self.files.by_id[3]
        self.assertEqual(t.name, "bi_mat.txt")                # bỏ phần ../../
        self.assertNotIn("..", t.path)

    async def test_accept_gui_file_accept(self):
        await self._co_loi_moi()

        async def khong_lam_gi(t):                            # 6b mới viết _receive_file
            pass
        self.files._receive_file = khong_lam_gi
        await self.files.accept(3)
        self.assertEqual(self.client.ch.sent[-1], {"type": "FILE_ACCEPT", "file_id": 3})
        self.assertEqual(self.files.by_id[3].status, "receiving")

    async def test_accept_file_id_la(self):
        await self.files.accept(99)
        self.assertEqual(self.client.ch.sent, [])
        self.assertEqual(len(loai_su_kien(self.client, "error")), 1)

    async def test_reject(self):
        await self._co_loi_moi()
        await self.files.reject(3)
        self.assertEqual(self.client.ch.sent[-1]["type"], "FILE_REJECT")
        self.assertEqual(self.files.by_id[3].status, "rejected")
    # ---------- GỬI DỮ LIỆU QUA UDP (Step 6b-2)
    async def test_send_file_cat_khuc_va_cap_nhat_tien_do(self):
        phia_server, phia_client = MockSession.pair()
        self.client.udp = phia_client

        # 1. Mời gửi và nhận file_id = 5
        ref = await self.files.offer("bob", self.path)
        self.files.on_offer_ack({"client_ref": ref, "file_id": 5})
        t = self.files.by_id[5]

        # 2. Chạy _send_file
        t.status = "sending"
        await self.files._send_file(t)

        cac_khuc = []
        stream_s = phia_server.stream(105)
        while sum(len(c) for c in cac_khuc) < 9000:
            cac_khuc.append(await stream_s.read_message())

        self.assertEqual(len(cac_khuc), 9)
        self.assertTrue(all(len(c) <= CHUNK_SIZE for c in cac_khuc))
        with open(self.path, "rb") as f:
            self.assertEqual(b"".join(cac_khuc), f.read())

        # 4. Đã gửi hết byte nhưng status vẫn là "sending" chờ FILE_DONE
        self.assertEqual(t.done_bytes, 9000)
        self.assertEqual(t.status, "sending")

        # 5. Khi nhận FILE_DONE từ người nhận -> mới đổi sang "done"
        self.files.on_done({"file_id": 5, "ok": True})
        self.assertEqual(t.status, "done")

    async def test_send_file_gap_loi_os(self):
        _, phia_client = MockSession.pair()
        self.client.udp = phia_client

        ref = await self.files.offer("bob", self.path)
        self.files.on_offer_ack({"client_ref": ref, "file_id": 6})
        t = self.files.by_id[6]

        # Giả lập file bị xóa mất trước khi kịp gửi
        t.path = "file_da_bi_xoa_mat.bin"
        await self.files._send_file(t)
        self.assertEqual(t.status, "failed")
        self.assertTrue(len(t.error) > 0)
    # ---------- NHẬN DỮ LIỆU QUA UDP (Step 6b-3)
    async def test_receive_file_dung_sha256(self):
        phia_server, phia_client = MockSession.pair()
        self.client.udp = phia_client

        # 1. Tính SHA-256 thật của file mẫu self.path (9000 byte) bằng sha256_file
        sha_goc = sha256_file(self.path)
        self.files.on_offer({"file_id": 8, "from": "alice", "name": "nhan_ve.txt",
                             "size": 9000, "sha256": sha_goc})
        t = self.files.by_id[8]
        t.path = self.path + ".received"

        try:
            # 2. Đẩy 2 mảnh (5000B + 4000B = 9000B) của file mẫu vào stream 108
            with open(self.path, "rb") as f:
                data = f.read()
            await phia_server.stream(108).send(data[:5000])
            await phia_server.stream(108).send(data[5000:])

            # 3. Chạy _receive_file
            await self.files._receive_file(t)

            # 4. Kiểm tra: status = "done", file đích tồn tại, khớp sha256_file, không còn file .part
            self.assertEqual(t.status, "done")
            self.assertTrue(os.path.isfile(t.path))
            self.assertFalse(os.path.exists(t.path + ".part"))
            self.assertEqual(sha256_file(t.path), sha_goc)

            # 5. Đã gửi FILE_DONE ok=True lên server
            done_msg = self.client.ch.sent[-1]
            self.assertEqual((done_msg["type"], done_msg["file_id"], done_msg["ok"]), ("FILE_DONE", 8, True))
        finally:
            if os.path.exists(t.path):
                os.remove(t.path)

    async def test_receive_file_sai_sha256_xoa_file_tam(self):
        phia_server, phia_client = MockSession.pair()
        self.client.udp = phia_client

        # 1. Lời mời có mã băm "00"*32 (cố tình khác với dữ liệu thực tế)
        self.files.on_offer({"file_id": 9, "from": "alice", "name": "bi_sua.txt",
                             "size": 10, "sha256": "00" * 32})
        t = self.files.by_id[9]
        t.path = self.path + ".corrupted"

        # 2. Server gửi 10 byte dữ liệu không khớp mã băm
        await phia_server.stream(109).send(b"0123456789")

        # 3. Chạy _receive_file
        await self.files._receive_file(t)

        # 4. Kiểm tra: status = "failed", lỗi "sai hash", đã xóa sạch file .part và không tạo file đích
        self.assertEqual(t.status, "failed")
        self.assertEqual(t.error, "sai hash")
        self.assertFalse(os.path.exists(t.path))
        self.assertFalse(os.path.exists(t.path + ".part"))

        # 5. Đã gửi FILE_DONE ok=False lên server
        done_msg = self.client.ch.sent[-1]
        self.assertEqual((done_msg["type"], done_msg["file_id"], done_msg["ok"]), ("FILE_DONE", 9, False))




    # ---------- DỌN DẸP KHI MẤT KẾT NỐI (Step 7c: _on_disconnect)
    async def test_on_disconnect_danh_dau_file_dang_truyen_failed(self):
        from client.client import CollabClient

        c = CollabClient()
        c.state = "AUTHENTICATED"

        # Tạo 3 file ở 3 trạng thái: đang gửi (sending), mới mời (offered), và đã xong từ trước (done)
        c.files.on_offer({"file_id": 1, "from": "bob", "name": "dang_nhan.bin", "size": 100, "sha256": "aa" * 32})
        c.files.by_id[1].status = "receiving"

        c.files.on_offer({"file_id": 2, "from": "bob", "name": "da_xong.bin", "size": 100, "sha256": "bb" * 32})
        c.files.by_id[2].status = "done"

        # Gọi _on_disconnect 2 lần liên tiếp (kiểm tra idempotent)
        await c._on_disconnect("rớt mạng đột ngột")
        await c._on_disconnect("gọi lần 2 không được ghi đè")

        # File đang nhận dở -> chuyển thành failed với đúng lý do của lần 1
        self.assertEqual(c.files.by_id[1].status, "failed")
        self.assertEqual(c.files.by_id[1].error, "rớt mạng đột ngột")

        # File đã xong từ trước -> vẫn giữ nguyên là done
        self.assertEqual(c.files.by_id[2].status, "done")
        self.assertEqual(c.state, "DISCONNECTED")

if __name__ == "__main__":
    unittest.main()