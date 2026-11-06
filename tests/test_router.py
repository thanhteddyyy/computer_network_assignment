"""Step 5a: kiểm tra logic định tuyến chat bằng MockSession (chưa cần UDP thật)."""
import asyncio
import json
import unittest

from common.config import STREAM_DIRECT_CHAT, STREAM_GLOBAL_CHAT
from server.router import MAX_TEXT, Router, parse_chat
from server.session_manager import ClientSession, SessionManager
from transport.mock_session import MockSession


class FakeControl:
    """Kênh TCP giả: chỉ ghi lại những gì server gửi qua TCP."""
    def __init__(self):
        self.sent = []

    async def send(self, msg):
        self.sent.append(msg)



def tao_user(sm: SessionManager, name: str):
    phia_server, phia_client = MockSession.pair()          # một "sợi dây" UDP giả nối server với client
    c = ClientSession(name, sm.new_session_id(), FakeControl(), "127.0.0.1", 50000)
    c.udp = phia_server
    sm.add(c)
    return c, phia_client


async def doc(phia_client, stream_id):
    raw = await asyncio.wait_for(phia_client.stream(stream_id).read_message(), 1)
    return json.loads(raw)


def chat(to, text):
    return json.dumps({"to": to, "text": text}).encode()


class RouterTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.sm = SessionManager()
        self.router = Router(self.sm)
        self.alice, self.alice_c = tao_user(self.sm, "alice")
        self.bob, self.bob_c = tao_user(self.sm, "bob")
        self.carol, self.carol_c = tao_user(self.sm, "carol")

    async def test_gui_tat_ca(self):
        await self.router.handle_chat(self.alice, chat("*", "chào cả nhóm"))
        for phia in (self.bob_c, self.carol_c, self.alice_c):     # alice nhận bản xác nhận
            m = await doc(phia, STREAM_GLOBAL_CHAT)
            self.assertEqual((m["type"], m["from"], m["to"], m["text"]), ("MSG", "alice", "*", "chào cả nhóm"))

    async def test_gui_rieng(self):
        await self.router.handle_chat(self.alice, chat("bob", "bí mật"))
        m = await doc(self.bob_c, STREAM_DIRECT_CHAT)
        self.assertEqual((m["from"], m["to"], m["text"]), ("alice", "bob", "bí mật"))
        await doc(self.alice_c, STREAM_DIRECT_CHAT)                # bản xác nhận cho alice
        await asyncio.sleep(0.05)
        self.assertTrue(self.carol_c.stream(STREAM_DIRECT_CHAT)._inbox.empty())   # carol không thấy gì

    async def test_gui_cho_nguoi_khong_online(self):
        await self.router.handle_chat(self.alice, chat("dave", "alo"))
        self.assertEqual(self.alice.control.sent[0]["type"], "ERROR")

    async def test_msg_id_tang_dan(self):
        await self.router.handle_chat(self.alice, chat("*", "một"))
        await self.router.handle_chat(self.bob, chat("*", "hai"))
        a = await doc(self.carol_c, STREAM_GLOBAL_CHAT)
        b = await doc(self.carol_c, STREAM_GLOBAL_CHAT)
        self.assertLess(a["msg_id"], b["msg_id"])

    async def test_tin_hong(self):
        for raw in (b"khong phai json", chat("*", "   "), chat("*", "x" * 5000), b'{"text": "thieu to"}'):
            with self.assertRaises((ValueError, KeyError)):
                await self.router.handle_chat(self.alice, raw)
def j(obj) -> bytes:
    """Đổi một dict thành bytes JSON, giống client gửi lên."""
    return json.dumps(obj, ensure_ascii=False).encode()


class ParseChatTest(unittest.TestCase):
    """Kiểm tra riêng bước 'đọc và kiểm tra tin', không cần server hay mạng."""

    HOP_LE = [
        # (mô tả, bytes gửi lên, kết quả mong đợi (to, text))
        ("gửi tất cả",              j({"to": "*", "text": "hi"}),                        ("*", "hi")),
        ("gửi riêng",               j({"to": "bob", "text": "chào bob"}),                ("bob", "chào bob")),
        ("bỏ dấu cách hai đầu",     j({"to": " bob ", "text": "   chào   "}),            ("bob", "chào")),
        ("tiếng Việt + emoji",      j({"to": "*", "text": "Xin chào 👋"}),               ("*", "Xin chào 👋")),
        ("đúng giới hạn 4000",      j({"to": "*", "text": "x" * MAX_TEXT}),              ("*", "x" * MAX_TEXT)),
        ("có thêm trường lạ",       j({"to": "*", "text": "hi", "client_msg_id": "a1"}), ("*", "hi")),
    ]

    KHONG_HOP_LE = [
        # (mô tả, bytes gửi lên)
        ("không phải JSON",          b"khong phai json"),
        ("rỗng hoàn toàn",           b""),
        ("byte không phải UTF-8",    b"\xc3\x28"),
        ("JSON là danh sách",        b"[1, 2]"),
        ("JSON là số",               b"123"),
        ("JSON là null",             b"null"),
        ("thiếu người nhận",         j({"text": "thiếu to"})),
        ("người nhận rỗng",          j({"to": "", "text": "hi"})),
        ("người nhận là null",       j({"to": None, "text": "hi"})),
        ("người nhận là danh sách",  j({"to": ["bob"], "text": "hi"})),
        ("thiếu nội dung",           j({"to": "*"})),
        ("nội dung toàn dấu cách",   j({"to": "*", "text": "   "})),
        ("nội dung là số",           j({"to": "*", "text": 123})),
        ("vượt giới hạn 1 ký tự",    j({"to": "*", "text": "x" * (MAX_TEXT + 1)})),
    ]

    def test_tin_hop_le(self):
        for mo_ta, raw, mong_doi in self.HOP_LE:
            with self.subTest(mo_ta):
                self.assertEqual(parse_chat(raw), mong_doi)

    def test_tin_khong_hop_le(self):
        for mo_ta, raw in self.KHONG_HOP_LE:
            with self.subTest(mo_ta):
                with self.assertRaises((ValueError, KeyError)):
                    parse_chat(raw)

if __name__ == "__main__":
    unittest.main()