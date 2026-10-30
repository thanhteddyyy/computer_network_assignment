"""Phiên giả trong bộ nhớ, cùng interface với UdpSession.

Trạng thái: ĐÃ HOÀN THIỆN. Người B dùng để viết router/chat/file khi transport thật chưa xong:
    a, b = MockSession.pair()
    await a.stream(1).send(b"hello")
    assert await b.stream(1).read_message() == b"hello"
"""
import asyncio
from typing import Callable


class MockStream:
    def __init__(self, session: "MockSession", stream_id: int) -> None:
        self.session = session
        self.stream_id = stream_id
        self._inbox: asyncio.Queue[bytes] = asyncio.Queue()

    async def send(self, data: bytes, fin: bool = True) -> None:
        peer = self.session.peer
        if self.session.closed or peer is None:
            raise ConnectionError("mock session đã đóng")
        await asyncio.sleep(0)  # nhường event loop, giống hành vi thật
        peer.stream(self.stream_id)._inbox.put_nowait(bytes(data))

    async def read_message(self) -> bytes:
        return await self._inbox.get()


class MockSession:
    def __init__(self, session_id: int = 1) -> None:
        self.session_id = session_id
        self.peer: MockSession | None = None
        self.streams: dict[int, MockStream] = {}
        self.closed = False
        self.on_stream_created: Callable[[MockStream], None] | None = None
        self.on_close: Callable[[str], None] | None = None

    @classmethod
    def pair(cls, session_id: int = 1) -> tuple["MockSession", "MockSession"]:
        a, b = cls(session_id), cls(session_id)
        a.peer, b.peer = b, a
        return a, b

    def start(self) -> None:
        pass

    def stream(self, stream_id: int) -> MockStream:
        st = self.streams.get(stream_id)
        if st is None:
            st = self.streams[stream_id] = MockStream(self, stream_id)
            if self.on_stream_created:
                self.on_stream_created(st)
        return st

    async def close(self, reason: str = "local close") -> None:
        self.closed = True
        if self.on_close:
            self.on_close(reason)

    def stats(self) -> dict:
        return {"rtt_ms": 0, "rto_ms": 0, "cwnd": 0, "cc": "mock", "in_flight": 0,
                "queued": 0, "loss": 0, "sent": 0, "bad_packets": 0, "encrypted": False}
