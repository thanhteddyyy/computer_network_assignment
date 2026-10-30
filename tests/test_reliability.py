"""Test cho các TODO của người A (ngày 2). Chưa làm -> SKIP. Làm xong -> phải PASS."""
import asyncio
import unittest

from transport.congestion import Cubic
from transport.packet import AckFrame
from transport.reliability import ReceiveTracker, RttEstimator, SendTracker, SentPacket
from transport.stream import RecvStream, SendStream
from tests._helpers import todo


def sent(pn, t=0.0):
    return SentPacket(pn, stream_id=1, offset=pn * 10, data=b"x" * 10, fin=False, sent_time=t)


class RttTest(unittest.TestCase):
    @todo
    def test_first_and_next_sample(self):
        r = RttEstimator()
        r.update(0.100)
        self.assertAlmostEqual(r.srtt, 0.100)
        self.assertAlmostEqual(r.rttvar, 0.050)
        self.assertAlmostEqual(r.rto, 0.300)
        r.update(0.200)
        self.assertAlmostEqual(r.srtt, 0.1125)
        self.assertAlmostEqual(r.rttvar, 0.0625)


class ReceiveTrackerTest(unittest.TestCase):
    @todo
    def test_ranges_and_duplicates(self):
        r = ReceiveTracker()
        for pn in (0, 1, 2, 4, 5, 9):
            self.assertFalse(r.on_packet(pn, True))
        self.assertTrue(r.on_packet(4, True))        # gói trùng
        ack = r.build_ack(now=0.0)
        self.assertEqual(ack.largest_acked, 9)
        self.assertEqual(ack.ranges, [(9, 9), (4, 5), (0, 2)])
        self.assertFalse(r.ack_pending)


class SendTrackerTest(unittest.TestCase):
    @todo
    def test_sack_and_packet_threshold_loss(self):
        s = SendTracker()
        for pn in range(8):
            s.on_sent(sent(pn))
        acked, lost = s.on_ack(AckFrame(7, 100, 0, [(5, 7), (0, 3)]))   # thiếu gói 4
        self.assertEqual(sorted(p.packet_number for p in acked), [0, 1, 2, 3, 5, 6, 7])
        self.assertEqual([p.packet_number for p in lost], [4])          # 4 + 3 <= 7
        self.assertEqual(s.in_flight, 0)
        self.assertEqual(s.peer_recv_window, 100)

    @todo
    def test_timeout(self):
        s = SendTracker()
        s.on_sent(sent(0, t=0.0))
        s.on_sent(sent(1, t=0.9))
        out = s.pop_timed_out(now=1.2, rto=1.0)
        self.assertEqual([p.packet_number for p in out], [0])
        self.assertEqual(list(s.unacked), [1])


class StreamTest(unittest.TestCase):
    @todo
    async def test_reorder_and_message_boundaries(self):
        tx, rx = SendStream(1), RecvStream(1)
        segs = tx.segment(b"hello world", fin=True, chunk=4) + tx.segment(b"bye", fin=True, chunk=4)
        for off, data, fin in reversed(segs):     # đến ngược thứ tự hoàn toàn
            rx.on_data(off, data, fin)
        rx.on_data(*segs[0])                       # gói trùng
        self.assertEqual(await rx.read_message(), b"hello world")
        self.assertEqual(await rx.read_message(), b"bye")
        self.assertFalse(rx.has_message())

    @todo
    async def test_no_head_of_line_blocking_between_streams(self):
        file_rx, chat_rx = RecvStream(100), RecvStream(1)
        file_rx.on_data(1100, b"chunk2", True)      # chunk 1 của file bị mất, chưa tới
        chat_rx.on_data(0, b"chat van den", True)
        msg = await asyncio.wait_for(chat_rx.read_message(), 0.1)   # chat không bị chặn
        self.assertEqual(msg, b"chat van den")
        self.assertFalse(file_rx.has_message())


class CubicTest(unittest.TestCase):
    @todo
    def test_slow_start_then_loss(self):
        c = Cubic()
        start = c.cwnd
        c.on_ack(acked_count=10, rtt=0.05, now=0.0)
        self.assertGreater(c.cwnd, start)           # slow start tăng
        before = c.cwnd
        c.on_loss(now=0.1)
        self.assertAlmostEqual(c.cwnd, before * 0.7, places=3)
        c.on_timeout(now=0.2)
        self.assertLessEqual(c.cwnd, 2)


if __name__ == "__main__":
    unittest.main()
