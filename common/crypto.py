"""Các hàm mật mã. Dùng thư viện chuẩn `cryptography` (đề cho phép ở bước 8).

Luồng tổng quát:
  1. Client và server mỗi bên sinh cặp khóa X25519, gửi public key cho nhau qua TCP (KEY_EXCHANGE).
  2. Hai bên tính cùng một shared secret (ECDH), rồi dùng HKDF sinh ra các khóa phiên.
  3. Kênh TCP: AES-GCM với nonce ngẫu nhiên 12 byte gắn trước ciphertext.
  4. Kênh UDP: AES-GCM với nonce = IV XOR packet_number, header làm AAD.

QUAN TRỌNG – không bao giờ dùng lại cặp (khóa, nonce):
  Client và server đều đánh packet_number từ 0. Nếu dùng chung một khóa UDP thì
  gói số 5 của client và gói số 5 của server sẽ trùng nonce -> phá vỡ AES-GCM.
  Vì vậy ta sinh KHÓA RIÊNG CHO MỖI CHIỀU (client->server, server->client).
"""
import hashlib
import hmac
import os
from dataclasses import dataclass

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from common.config import HKDF_INFO, PBKDF2_ITERATIONS

NONCE_SIZE = 12


class CryptoError(Exception):
    """Giải mã thất bại: sai khóa, dữ liệu bị sửa, hoặc gói giả mạo."""


# ============================================================ Khóa phiên
@dataclass(frozen=True)
class DirectionKeys:
    key: bytes   # 32 byte AES-256
    iv: bytes    # 12 byte, XOR với packet_number để ra nonce


@dataclass(frozen=True)
class SessionKeys:
    tcp_key: bytes          # Khóa cho kênh TCP (nonce ngẫu nhiên nên dùng chung 2 chiều được)
    c2s: DirectionKeys      # UDP client -> server
    s2c: DirectionKeys      # UDP server -> client

    def for_role(self, is_server: bool) -> tuple[DirectionKeys, DirectionKeys]:
        """Trả về (khóa để GỬI, khóa để NHẬN) theo vai trò."""
        return (self.s2c, self.c2s) if is_server else (self.c2s, self.s2c)


class KeyExchange:
    """Một bên của phiên trao khóa ECDH X25519."""

    def __init__(self) -> None:
        self._private = X25519PrivateKey.generate()

    def public_bytes(self) -> bytes:
        return self._private.public_key().public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw
        )

    def derive(self, peer_public: bytes) -> SessionKeys:
        shared = self._private.exchange(X25519PublicKey.from_public_bytes(peer_public))
        material = HKDF(
            algorithm=hashes.SHA256(), length=32 + 32 + 12 + 32 + 12, salt=None, info=HKDF_INFO
        ).derive(shared)
        return SessionKeys(
            tcp_key=material[0:32],
            c2s=DirectionKeys(material[32:64], material[64:76]),
            s2c=DirectionKeys(material[76:108], material[108:120]),
        )


# ============================================================ AES-GCM
def seal_random_nonce(key: bytes, plaintext: bytes, aad: bytes = b"") -> bytes:
    """Mã hóa với nonce ngẫu nhiên. Đầu ra: nonce(12) || ciphertext+tag. Dùng cho kênh TCP."""
    nonce = os.urandom(NONCE_SIZE)
    return nonce + AESGCM(key).encrypt(nonce, plaintext, aad or None)


def open_random_nonce(key: bytes, data: bytes, aad: bytes = b"") -> bytes:
    if len(data) < NONCE_SIZE + 16:
        raise CryptoError("dữ liệu mã hóa quá ngắn")
    try:
        return AESGCM(key).decrypt(data[:NONCE_SIZE], data[NONCE_SIZE:], aad or None)
    except Exception as exc:  # InvalidTag
        raise CryptoError("giải mã TCP thất bại") from exc


def packet_nonce(iv: bytes, packet_number: int) -> bytes:
    """nonce = IV XOR packet_number (đặt ở 8 byte cuối). Giống cách QUIC làm.
    packet_number tăng đơn điệu và KHÔNG tái sử dụng khi retransmit -> nonce không bao giờ lặp."""
    pn = packet_number.to_bytes(NONCE_SIZE, "big")
    return bytes(a ^ b for a, b in zip(iv, pn))


def seal_packet_payload(keys: DirectionKeys, packet_number: int, plaintext: bytes, aad: bytes) -> bytes:
    return AESGCM(keys.key).encrypt(packet_nonce(keys.iv, packet_number), plaintext, aad)


def open_packet_payload(keys: DirectionKeys, packet_number: int, ciphertext: bytes, aad: bytes) -> bytes:
    try:
        return AESGCM(keys.key).decrypt(packet_nonce(keys.iv, packet_number), ciphertext, aad)
    except Exception as exc:
        raise CryptoError(f"giải mã gói #{packet_number} thất bại") from exc


# ============================================================ Mật khẩu
def hash_password(password: str, salt: bytes | None = None) -> tuple[str, str]:
    """Trả về (salt_hex, hash_hex). Không bao giờ lưu mật khẩu dạng rõ."""
    salt = salt or os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, PBKDF2_ITERATIONS)
    return salt.hex(), digest.hex()


def verify_password(password: str, salt_hex: str, hash_hex: str) -> bool:
    _, candidate = hash_password(password, bytes.fromhex(salt_hex))
    return hmac.compare_digest(candidate, hash_hex)  # so sánh thời gian hằng, chống timing attack


def sha256_file(path: str, block: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(block):
            h.update(chunk)
    return h.hexdigest()
