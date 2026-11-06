"""Hằng số cấu hình dùng chung. Mọi con số "ma thuật" đều phải nằm ở đây, không rải trong code.

Quy ước TODO trong toàn dự án:  TODO[<người>-D<ngày>]
    TODO[A-D1]  -> người A làm trong ngày 1
    TODO[B-D2]  -> người B làm trong ngày 2
Tìm việc của mình:  grep -rn "TODO\\[A" .   (hoặc "TODO\\[B")
"""

# ---------------------------------------------------------------- Cổng mạng
TCP_PORT = 9000          # Kênh điều khiển (HELLO, AUTH, USER_LIST, FILE_OFFER...)
UDP_PORT = 9001          # Data plane: chat + file qua giao thức UDP tin cậy tự viết
DISCOVERY_PORT = 9002    # Server broadcast ANNOUNCE để client tự tìm thấy
HTTP_BASE_PORT = 8081    # Mỗi client chạy mini HTTP server ở 8081, 8082, ...

BROADCAST_ADDR = "255.255.255.255"

# ---------------------------------------------------------------- Giao thức UDP
PROTOCOL_MAGIC = 0xC0DE
PROTOCOL_VERSION = 1
HEADER_SIZE = 21         # Xem transport/packet.py
AEAD_TAG_SIZE = 16       # AES-GCM thêm 16 byte tag vào ciphertext
MAX_DATAGRAM = 1200      # Giữ dưới MTU để tránh phân mảnh IP
CHUNK_SIZE = 1100        # 21 (header) + 1100 (payload) + 16 (tag) = 1137 <= 1200

# Ánh xạ stream ID
STREAM_CONTROL = 0       # Dự phòng (ACK/PING không thuộc stream nào)
STREAM_GLOBAL_CHAT = 1
STREAM_DIRECT_CHAT = 2
FILE_STREAM_BASE = 100   # file_id = k  ->  stream_id = 100 + k

# ---------------------------------------------------------------- Độ tin cậy
INITIAL_RTO = 1.0        # giây, theo RFC 6298
MIN_RTO = 0.2
MAX_RTO = 8.0
MAX_RETRANSMIT = 10      # Quá số lần này coi như kết nối chết
ACK_DELAY = 0.02         # Gom ACK tối đa 20 ms rồi mới gửi
DEFAULT_RECV_WINDOW = 256  # Số gói tối đa bên nhận chịu được (flow control)

# ---------------------------------------------------------------- Congestion control
INITIAL_CWND = 10        # gói
MIN_CWND = 2
CUBIC_C = 0.4
CUBIC_BETA = 0.7
ENABLE_UDP = True
# ---------------------------------------------------------------- Heartbeat / discovery
HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_TIMEOUT = 6.0  # Mất 3 heartbeat liên tiếp -> offline
DISCOVERY_INTERVAL = 2.0
DISCOVERY_TIMEOUT = 5.0

# ---------------------------------------------------------------- TCP framing
MAX_FRAME_SIZE = 1 * 1024 * 1024   # 1 MB, chặn frame rác làm tràn bộ nhớ

# ---------------------------------------------------------------- Bảo mật
PBKDF2_ITERATIONS = 200_000
HKDF_INFO = b"collab-app v1 session keys"

# ---------------------------------------------------------------- Đường dẫn
USERS_FILE = "data/users.json"
DOWNLOAD_DIR = "downloads"
STATIC_DIR = "client/static"
