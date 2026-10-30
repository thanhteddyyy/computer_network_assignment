"""Test tích hợp tầng transport: 2 UdpSession nói chuyện qua UDP thật trên localhost với 20% mất gói.
Chạy được khi người A xong reliability.py + stream.py (ngày 2). Đây là bằng chứng tốt cho báo cáo."""
import asyncio
import hashlib
import os
import unittest

from common.crypto import KeyExchange
from transport.loss_sim import LossSimulator
from transport.session import UdpSession
from transport.udp_endpoint import UdpEndpoint
from tests._helpers import todo
from transport.packet import AckFrame
from transport.reliability import ReceiveTracker, RttEstimator, SendTracker
from transport.stream import RecvStream


def require_transport_done() -> None:
    """Gọi thử các hàm TODO; nếu chưa làm sẽ ném NotImplementedError -> test được SKIP."""
    r = ReceiveTracker()
    r.on_packet(0, True)
    r.build_ack(0.0)
    SendTracker().on_ack(AckFrame(0, 10, 0, [(0, 0)]))
    SendTracker().pop_timed_out(0.0, 1.0)
    RttEstimator().update(0.01)
    RecvStream(1).on_data(0, b"", True)


async def make_pair(loss: float):
    a, b = KeyExchange(), KeyExchange()
    keys = a.derive(b.public_bytes())
    ep_s = await UdpEndpoint.open("127.0.0.1", 0, LossSimulator(loss, seed=1))
    ep_c = await UdpEndpoint.open("127.0.0.1", 0, LossSimulator(loss, seed=2))
    server = UdpSession(77, ep_s, ("127.0.0.1", ep_c.local_port), keys, is_server=True)
    client = UdpSession(77, ep_c, ("127.0.0.1", ep_s.local_port), keys, is_server=False)
    server.start()
    client.start()
    return client, server, (ep_s, ep_c)


class LoopbackTest(unittest.TestCase):
    @todo
    async def test_messages_and_file_with_loss(self):
        require_transport_done()
        client, server, eps = await make_pair(loss=0.2)
        try:
            for i in range(100):
                await client.stream(1).send(f"tin {i}".encode())
            blob = os.urandom(300_000)
            send_task = asyncio.create_task(client.stream(101).send(blob))

            got = [await asyncio.wait_for(server.stream(1).read_message(), 20) for _ in range(100)]
            self.assertEqual(got, [f"tin {i}".encode() for i in range(100)])   # đủ, đúng thứ tự

            received = await asyncio.wait_for(server.stream(101).read_message(), 60)
            await send_task
            self.assertEqual(hashlib.sha256(received).digest(), hashlib.sha256(blob).digest())
            print("\n  stats client:", client.stats())
        finally:
            await client.close()
            await server.close()
            for ep in eps:
                ep.close()


if __name__ == "__main__":
    unittest.main()
