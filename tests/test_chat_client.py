"""Step 5a (phía client): ChatService.send, và một vòng đầy đủ client -> router -> client bằng MockSession."""
import asyncio
import json
import unittest

from client.chat import MAX_TEXT, ChatService
from client.events import EventLog
from common.config import STREAM_DIRECT_CHAT, STREAM_GLOBAL_CHAT
from server.router import Router
from server.session_manager import ClientSession, SessionManager
from transport.mock_session import MockSession


class FakeClient:
    """Client giả: chỉ có đúng hai thứ ChatService cần là udp và events."""
    def __init__(self, udp=None):
        self.udp = udp
        self.events = EventLog()


class FakeControl:
    def __init__(self):
        self.sent = []

    async def send(self, msg):
        self.sent.append(msg)


async def doc(phia, stream_id):
    raw = await asyncio.wait_for(phia.stream(stream_id).read_message(), 1)
    return json.loads(raw)


class ChatSendTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.phia_server, phia_client = MockSession.pair()
        self.client = FakeClient(phia_client)
        self.chat = ChatService(self.client)

    async def test_gui_tat_ca_di_stream_1(self):
        await self.chat.send("*", "chào cả nhóm")
        self.assertEqual(await doc(self.phia_server, STREAM_GLOBAL_CHAT), {"to": "*", "text": "chào cả nhóm"})

    async def test_gui_rieng_di_stream_2(self):
        await self.chat.send("bob", "bí mật")
        self.assertEqual(await doc(self.phia_server, STREAM_DIRECT_CHAT), {"to": "bob", "text": "bí mật"})

    async def test_khong_tu_hien_tin_cua_minh(self):
        await self.chat.send("*", "hi")
        self.assertEqual([e for e in self.client.events.since(0) if e["kind"] == "message"], [])

    async def test_tin_rong_khong_gui(self):
        await self.chat.send("*", "   ")
        await asyncio.sleep(0.05)
        self.assertTrue(self.phia_server.stream(STREAM_GLOBAL_CHAT)._inbox.empty())

    async def test_tin_qua_dai_bao_loi(self):
        await self.chat.send("*", "x" * (MAX_TEXT + 1))
        await asyncio.sleep(0.05)
        self.assertTrue(self.phia_server.stream(STREAM_GLOBAL_CHAT)._inbox.empty())
        self.assertEqual(self.client.events.since(0)[-1]["kind"], "error")

    async def test_chua_dang_nhap_bao_loi(self):
        client = FakeClient(udp=None)
        await ChatService(client).send("*", "hi")
        self.assertEqual(client.events.since(0)[-1]["kind"], "error")


class ChatEndToEndTest(unittest.IsolatedAsyncioTestCase):
    """alice gõ tin -> ChatService.send -> router.pump_stream/handle_chat -> vòng nhận của bob -> events của bob."""

    async def test_alice_gui_bob_nhan(self):
        sm = SessionManager()
        router = Router(sm)
        nguoi = {}
        for ten in ("alice", "bob"):
            phia_server, phia_client = MockSession.pair()
            s = ClientSession(ten, sm.new_session_id(), FakeControl(), "127.0.0.1", 50000)
            s.udp = phia_server
            sm.add(s)
            client = FakeClient(phia_client)
            chat = ChatService(client)
            chat.start()                                                    # vòng nhận phía client
            tasks = [asyncio.create_task(router.pump_stream(s, sid))        # vòng nhận phía server
                     for sid in (STREAM_GLOBAL_CHAT, STREAM_DIRECT_CHAT)]
            nguoi[ten] = (client, chat, tasks)

        alice, bob = nguoi["alice"], nguoi["bob"]
        await alice[1].send("*", "chào bob")
        await asyncio.sleep(0.1)

        tin_bob = [e for e in bob[0].events.since(0) if e["kind"] == "message"]
        self.assertEqual(len(tin_bob), 1)
        self.assertEqual((tin_bob[0]["message"]["from"], tin_bob[0]["message"]["text"]), ("alice", "chào bob"))
        self.assertEqual(tin_bob[0]["channel"], "global")
        tin_alice = [e for e in alice[0].events.since(0) if e["kind"] == "message"]
        self.assertEqual(len(tin_alice), 1)                                  # bản xác nhận

        for client, chat, tasks in nguoi.values():
            chat.stop()
            for t in tasks:
                t.cancel()


if __name__ == "__main__":
    unittest.main()