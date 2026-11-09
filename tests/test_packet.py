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


    def test_step8_two_directions_and_header_aad(self):
        client_kx, server_kx = KeyExchange(), KeyExchange()
        c_keys = client_kx.derive(server_kx.public_bytes())
        s_keys = server_kx.derive(client_kx.public_bytes())

        secret_msg = b'{"to": "*", "text": "mat_khau_kho_bau_123456"}'
        pkt = Packet(PacketType.DATA, 0x11223344, packet_number=0, stream_id=1, offset=0,
                     flags=Flags.FIN, payload=secret_msg)

        # 1. Client mã hóa bằng khóa gửi của Client (c2s)
        c_send, _ = c_keys.for_role(is_server=False)
        _, s_recv = s_keys.for_role(is_server=True)
        wire = pkt.seal(c_send)

        # Chứng minh: Trên dây KHÔNG HỀ chứa nội dung tin nhắn rõ
        self.assertNotIn(b"mat_khau_kho_bau", wire)

        # 2. Server giải mã bằng khóa nhận của Server (c2s) -> khớp 100%
        opened = Packet.open(wire, s_recv)
        self.assertEqual(opened.payload, secret_msg)

        # 3. Dùng nhầm khóa chiều ngược lại (s2c) -> giải mã thất bại ngay (CryptoError)
        s_send, _ = s_keys.for_role(is_server=True)
        with self.assertRaises(CryptoError):
            Packet.open(wire, s_send)

        # 4. Kẻ tấn công sửa 1 byte ở Header (ví dụ đổi stream_id ở byte thứ 14) -> AAD phát hiện ngay!
        tampered_header = bytearray(wire)
        tampered_header[14] ^= 0x01
        with self.assertRaises(CryptoError):
            Packet.open(bytes(tampered_header), s_recv)


if __name__ == "__main__":
    unittest.main()
