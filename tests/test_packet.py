import unittest

from common.config import HEADER_SIZE
from common.crypto import CryptoError, KeyExchange
from transport.packet import AckFrame, Flags, Packet, PacketError, PacketType, peek_session_id


class PacketTest(unittest.TestCase):
    def test_roundtrip(self):
        p = Packet(PacketType.DATA, 0xDEADBEEF, 42, stream_id=101, offset=1100, flags=Flags.FIN, payload=b"abc")
        raw = p.encode()
        self.assertEqual(len(raw), HEADER_SIZE + 3)
        self.assertEqual(Packet.decode(raw), p)
        self.assertEqual(peek_session_id(raw), 0xDEADBEEF)

    def test_garbage_rejected(self):
        for junk in (b"", b"xx", b"\x00" * 40, Packet(PacketType.PING, 1, 1).encode() + b"extra"):
            with self.assertRaises(PacketError):
                Packet.decode(junk)

    def test_seal_open(self):
        a, b = KeyExchange(), KeyExchange()
        keys = a.derive(b.public_bytes())
        p = Packet(PacketType.DATA, 5, 9, 1, 0, Flags.FIN, b"xin chao")
        wire = p.seal(keys.c2s)
        self.assertNotIn(b"xin chao", wire)                        # payload không đọc được trên dây
        self.assertEqual(Packet.open(wire, keys.c2s).payload, b"xin chao")
        bad = bytearray(wire)
        bad[12] ^= 0xFF                                            # sửa packet number
        with self.assertRaises((CryptoError, PacketError)):
            Packet.open(bytes(bad), keys.c2s)

    def test_ack_frame(self):
        f = AckFrame(largest_acked=9, recv_window=256, ack_delay_ms=5, ranges=[(6, 9), (0, 4)])
        g = AckFrame.decode(f.encode())
        self.assertEqual(f, g)
        self.assertTrue(g.contains(3) and g.contains(7))
        self.assertFalse(g.contains(5))


if __name__ == "__main__":
    unittest.main()
