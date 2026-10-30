import unittest

from common.crypto import (CryptoError, KeyExchange, hash_password, open_packet_payload,
                           open_random_nonce, packet_nonce, seal_packet_payload, seal_random_nonce,
                           verify_password)


class CryptoTest(unittest.TestCase):
    def setUp(self):
        a, b = KeyExchange(), KeyExchange()
        self.ka = a.derive(b.public_bytes())
        self.kb = b.derive(a.public_bytes())

    def test_both_sides_derive_same_keys(self):
        self.assertEqual(self.ka, self.kb)

    def test_directions_use_different_keys(self):
        self.assertNotEqual(self.ka.c2s.key, self.ka.s2c.key)
        client_send, client_recv = self.ka.for_role(is_server=False)
        server_send, server_recv = self.kb.for_role(is_server=True)
        self.assertEqual(client_send, server_recv)
        self.assertEqual(server_send, client_recv)

    def test_tcp_roundtrip_and_tamper(self):
        sealed = seal_random_nonce(self.ka.tcp_key, b"mat khau 123")
        self.assertEqual(open_random_nonce(self.kb.tcp_key, sealed), b"mat khau 123")
        tampered = sealed[:-1] + bytes([sealed[-1] ^ 1])
        with self.assertRaises(CryptoError):
            open_random_nonce(self.kb.tcp_key, tampered)

    def test_packet_nonce_unique(self):
        iv = self.ka.c2s.iv
        self.assertEqual(len({packet_nonce(iv, pn) for pn in range(1000)}), 1000)

    def test_packet_aad_protects_header(self):
        ct = seal_packet_payload(self.ka.c2s, 7, b"data", b"header-v1")
        self.assertEqual(open_packet_payload(self.kb.c2s, 7, ct, b"header-v1"), b"data")
        with self.assertRaises(CryptoError):
            open_packet_payload(self.kb.c2s, 7, ct, b"header-v2")   # header bị sửa
        with self.assertRaises(CryptoError):
            open_packet_payload(self.kb.c2s, 8, ct, b"header-v1")   # packet number bị sửa

    def test_password_hash(self):
        salt, h = hash_password("bi mat")
        self.assertTrue(verify_password("bi mat", salt, h))
        self.assertFalse(verify_password("sai", salt, h))
        self.assertNotEqual(hash_password("bi mat")[1], h)   # salt khác -> hash khác


if __name__ == "__main__":
    unittest.main()
