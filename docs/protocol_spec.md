# Protocol Specification (v1.0 — Hoàn chỉnh)

> Tài liệu này là nguồn sự thật duy nhất của giao thức. Code phải khớp với nó; đổi giao thức thì sửa file này trước.

## 1. Tổng quan kiến trúc

| Kênh | Giao thức | Cổng | Mã hóa | Dùng cho |
|---|---|---|---|---|
| Điều khiển | TCP | 9000 | AES-256-GCM (sau KEY_EXCHANGE) | Handshake, trao khóa, đăng nhập, presence, điều khiển file |
| Dữ liệu | UDP + giao thức tin cậy riêng | 9001 | AES-256-GCM, header 21B làm AAD | Chat (stream 1, 2), dữ liệu file (stream 100+id) |
| Discovery | UDP broadcast | 9002 | Không (thông tin công khai) | Server thông báo địa chỉ cho client trong LAN |
| Giao diện | HTTP/1.1 tự viết | 8081+ (chỉ 127.0.0.1) | Không (loopback nội bộ máy) | Trình duyệt ↔ tiến trình client |

Hai kênh TCP và UDP được gắn kết với nhau bằng `session_id` 32-bit ngẫu nhiên do server cấp trong bản tin `AUTH_OK`.

## 2. Kênh TCP

**Đóng khung (Length-prefixed framing):** `[độ dài 4 byte, big-endian][payload]`, payload tối đa `MAX_FRAME_SIZE = 1 MB`.
- Trước `KEY_EXCHANGE`: payload là chuỗi UTF-8 JSON rõ (`HELLO`, `HELLO_ACK`, `KEY_EXCHANGE`).
- Sau `KEY_EXCHANGE`: payload = `nonce (12 byte) || AES-256-GCM(JSON) || Tag (16 byte)`.

| Message | Chiều | Trường | Ghi chú |
|---|---|---|---|
| HELLO | C→S | version, client_name | Message đầu tiên, timeout 10 s |
| HELLO_ACK | S→C | version, server_name | Bước 2 rubric |
| KEY_EXCHANGE | C→S rồi S→C | public_key (hex, X25519 32 byte) | Gửi rõ; sau đó cả hai bật mã hóa AES-256-GCM |
| REGISTER | C→S | username, password | Phản hồi REGISTER{ok} hoặc AUTH_FAIL{reason} |
| AUTH_REQ | C→S | username, password, udp_port | Đã được bảo vệ bởi AES-GCM trên kênh TCP |
| AUTH_OK | S→C | session_id, udp_port, users[] | Cấp `session_id` 32-bit ngẫu nhiên |
| AUTH_FAIL | S→C | reason | Client được thử lại (tối đa 5 lần sai liên tiếp) |
| USER_LIST | S→C | users[] | users: {username, ip, port, since} |
| USER_JOIN / USER_LEAVE | S→C | user / username, reason | Broadcast cho mọi client đang AUTHENTICATED |
| FILE_OFFER | C→S | client_ref, to, name, size, sha256 | Server chuyển cho người nhận kèm file_id, from |
| FILE_OFFER_ACK | S→C | client_ref, file_id \| error | Trả mã vận đơn `file_id` hoặc báo lỗi |
| FILE_ACCEPT / FILE_REJECT | C→S→C | file_id (, reason) | Kích hoạt hoặc từ chối luồng truyền UDP |
| FILE_DONE | C→S→C | file_id, ok, reason? | Người nhận gửi sau khi kiểm tra SHA-256 (hoặc Server gửi khi hủy phiên) |
| LOGOUT | C→S | | Thoát có kiểm soát |
| ERROR | S→C | reason | Thông báo lỗi giao thức / yêu cầu không hợp lệ |

## 3. Gói UDP

Header 21 byte, network byte order (`struct` format `!HBBBIIHIH`):

| Trường | Byte | Ý nghĩa |
|---|---|---|
| Magic | 2 | `0xC0DE` (nhận diện gói của giao thức) |
| Version | 1 | `1` |
| Type | 1 | `1` DATA, `2` ACK, `3` PING, `4` PONG, `5` CLOSE |
| Flags | 1 | bit 0 = `FIN` (End-of-Message) |
| Session ID | 4 | Cấp trong `AUTH_OK` (dùng để demux phiên tại `UdpEndpoint`) |
| Packet Number | 4 | Tăng đơn điệu theo từng chiều (`0, 1, 2...`), **không tái sử dụng** khi gửi lại |
| Stream ID | 2 | `1` chat chung, `2` chat riêng, `100 + file_id` dữ liệu file |
| Offset | 4 | Vị trí byte của đoạn dữ liệu trong stream |
| Payload Length | 2 | Độ dài ciphertext (đã bao gồm 16 byte AEAD authentication tag) |

**Kích thước tối đa:** `21 (Header) + 1100 (Plaintext chunk) + 16 (GCM Tag) = 1137 byte ≤ 1200 byte` → tránh hoàn toàn phân mảnh ở tầng IP (IP fragmentation).

**ACK payload:** `largest_acked(4) recv_window(2) ack_delay_ms(2) n_ranges(2)` + $n \times$ `(start(4), end(4))`, các khoảng sắp xếp giảm dần theo chuẩn QUIC.

## 4. Độ tin cậy, thứ tự, điều khiển luồng và tắc nghẽn

- **Phát hiện mất gói:**
  1. *Packet threshold (Selective ACK):* Khi gói $p$ chưa được ACK nhưng đã có gói $q \ge p + 3$ được ACK trong `AckFrame.ranges`.
  2. *RTO Timeout:* Quá thời gian chờ `rto` mà chưa nhận được ACK.
- **Ước lượng RTT/RTO:** Theo chuẩn RFC 6298 (`srtt`, `rttvar`), kẹp trong khoảng `[200 ms, 8.0 s]`, nhân đôi (`exponential backoff`) khi xảy ra timeout.
- **Gửi lại (Retransmission):** Chỉ gửi lại **đúng đoạn dữ liệu (`_Segment`) bị mất**, đóng vào gói UDP mới với `packet_number` mới (tránh nhập nhằng RTT và không bao giờ lặp nonce AES-GCM).
- **Thứ tự & Chống Head-of-Line (HoL) Blocking:** Ghép mảnh theo `offset` **độc lập trên từng `RecvStream`**. Gói bị mất ở stream truyền file (`100 + file_id`) chỉ làm chờ stream đó, tin nhắn chat ở stream `1` và `2` vẫn được đẩy lên ứng dụng ngay lập tức.
- **Flow control:** Số gói đang bay trên mạng `in_flight ≤ min(int(cwnd), peer_recv_window)`.
- **Congestion control (CUBIC — RFC 8312):**
  - *Slow Start:* khi `cwnd < ssthresh`, tăng `cwnd += acked_count`.
  - *Khi mất gói (`on_loss`):* đặt `w_max = cwnd`, giảm `cwnd = ssthresh = max(MIN_CWND, cwnd * 0.7)`, tính $K = \sqrt[3]{\frac{w_{\max}(1 - 0.7)}{0.4}}$ (chỉ giảm tối đa 1 lần trong mỗi cửa sổ 100ms).
  - *Congestion Avoidance:* tăng theo hàm bậc ba $W(t) = 0.4(t - K)^3 + w_{\max}$.
  - *Timeout (`on_timeout`):* đặt `ssthresh = max(MIN_CWND, cwnd / 2)` và đưa `cwnd = MIN_CWND = 2`.

## 5. Heartbeat và phát hiện mất kết nối

Client gửi gói `PING` mỗi `2.0 s`, server trả lời `PONG`. Nếu một bên im lặng quá `HEARTBEAT_TIMEOUT = 6.0 s` (mất 3 nhịp liên tiếp) hoặc một đoạn dữ liệu phải gửi lại quá `MAX_RETRANSMIT = 10` lần, phiên UDP tự động đóng và kích hoạt đóng kênh TCP để thu hồi toàn bộ tài nguyên.

## 6. Máy trạng thái (State Diagrams)

### 6.1. Máy trạng thái phía Client (`CollabClient`)
- `DISCONNECTED` $\xrightarrow{\text{connect()}}$ `CONNECTED` $\xrightarrow{\text{HELLO / HELLO\_ACK + KEY\_EXCHANGE}}$ `KEY_EXCHANGED` $\xrightarrow{\text{AUTH\_REQ / AUTH\_OK}}$ `AUTHENTICATED`.
- Từ bất kỳ trạng thái nào, khi gọi `logout()` hoặc gặp lỗi mạng (`ChannelClosed`, UDP timeout) $\rightarrow$ chạy `_on_disconnect()` và quay về `DISCONNECTED`.

### 6.2. Máy trạng thái phiên phía Server (`handle_client`)
- ` TCP_ACCEPTED` $\xrightarrow{\text{nhận HELLO hợp lệ, gửi HELLO\_ACK}}$ `HANDSHAKEN` $\xrightarrow{\text{nhận/gửi KEY\_EXCHANGE, bật AES-GCM}}$ `ENCRYPTED` $\xrightarrow{\text{nhận AUTH\_REQ hợp lệ, tạo UdpSession, gửi AUTH\_OK}}$ `AUTHENTICATED` $\xrightarrow{\text{LOGOUT / ChannelClosed / Timeout}}$ `CLEANED_UP` (qua `_cleanup`).

### 6.3. Máy trạng thái một lượt truyền File (`Transfer`)
- `offered` $\xrightarrow{\text{FILE\_ACCEPT}}$ `accepted` $\xrightarrow{\text{bắt đầu đọc/ghi UDP stream 100+id}}$ `sending / receiving / relaying` $\xrightarrow{\text{SHA-256 khớp, FILE\_DONE(ok=True)}}$ `done`.
- Các nhánh kết thúc sớm:
  - Từ `offered` $\xrightarrow{\text{FILE\_REJECT}}$ `rejected`.
  - Từ bất kỳ trạng thái đang chạy $\xrightarrow{\text{sai SHA-256 / lỗi I/O / đối tác rớt mạng (abort\_for)}}$ `failed / aborted`.

## 7. Xử lý lỗi & Chịu lỗi (Network Resilience — Step 7)

| Tình huống lỗi | Cơ chế phát hiện | Cách xử lý tại Server & Client |
|---|---|---|
| Client bị tắt đột ngột (`kill -9` / đóng terminal) | `IncompleteReadError`, `ConnectionResetError`, `BrokenPipeError` $\rightarrow$ quy về `ChannelClosed` | `finally` gọi `_cleanup`: xóa khỏi `SessionManager`, hủy các task nền, đóng `UdpSession`, gọi `abort_for` báo `FILE_DONE(ok=False)` cho đối tác, phát `USER_LEAVE`. |
| Đứt dây mạng / mất Wi-Fi âm thầm (Half-open TCP) | Quá `HEARTBEAT_TIMEOUT = 6s` không nhận gói UDP hoặc gửi lại quá `MAX_RETRANSMIT = 10` lần | `UdpSession` tự đóng $\rightarrow$ callback `on_close` đóng kênh TCP $\rightarrow$ kích hoạt `_cleanup` trên Server và `_on_disconnect` trên Client. |
| Client kết nối nhưng treo không gửi `HELLO` / `AUTH_REQ` | `asyncio.wait_for(..., timeout)` ném `asyncio.TimeoutError` (`10s` cho handshake, `120s` cho auth) | Server gửi `ERROR{reason="timeout"}` qua `_safe_send` rồi đóng kết nối trong `_cleanup`. |
| Gói UDP rác, bị sửa đổi trên đường truyền hoặc giả mạo | Sai `magic`, sai `version`, sai độ dài hoặc giải mã AES-GCM thất bại (`PacketError`, `CryptoError`) | Bỏ qua gói tin, tăng bộ đếm `bad_packets += 1`, tuyệt đối không làm sập tiến trình. |
| Gọi dọn dẹp nhiều lần cho cùng một sự cố | Kiểm tra biến trạng thái (`removed is not None` ở Server, `self.state == "DISCONNECTED"` ở Client) | Đảm bảo tính **Idempotent**: không phát `USER_LEAVE` trùng lặp, không đóng lại tài nguyên đã giải phóng. |

## 8. Tầng Bảo Mật (Security Layer — Step 8)

### 8.1. Thư viện & Thuật toán sử dụng
Hệ thống sử dụng thư viện chuẩn công nghiệp **`cryptography`** (module `hazmat.primitives`) với bộ giao thức mật mã hiện đại tương đương TLS 1.3 / QUIC:
1. **Trao đổi khóa bất đối xứng:** **ECDH trên đường cong Elliptic Curve25519 (`X25519`)**. Mỗi phiên kết nối sinh một cặp khóa tạm thời (ephemeral keypair) mới tinh $\rightarrow$ đảm bảo tính chất **Forward Secrecy (Bảo mật chuyển tiếp)**: lộ khóa của phiên này không giải mã được các phiên trước đó.
2. **Dẫn xuất khóa (Key Derivation):** **HKDF-SHA256** (`info = b"collab-app v1 session keys"`) mở rộng `shared_secret` (32 byte) thành **120 byte** nguyên liệu khóa, chia thành 3 bộ độc lập:
   - `tcp_key` (`32 byte`): Khóa AES-256 cho kênh điều khiển TCP.
   - `c2s` (`key = 32 byte`, `iv = 12 byte`): Khóa và IV riêng cho chiều UDP **Client $\rightarrow$ Server**.
   - `s2c` (`key = 32 byte`, `iv = 12 byte`): Khóa và IV riêng cho chiều UDP **Server $\rightarrow$ Client**.
3. **Mã hóa xác thực (AEAD):** **AES-256-GCM** (khóa 256-bit, nonce 96-bit, authentication tag 128-bit) đảm bảo đồng thời **Tính bí mật (Confidentiality)** và **Tính toàn vẹn / Xác thực nguồn gốc (Integrity & Authenticity)**.
4. **Bảo vệ mật khẩu lưu trữ:** **PBKDF2-HMAC-SHA256** với `200.000` vòng lặp và `salt` ngẫu nhiên 16 byte cho mỗi tài khoản; kiểm tra bằng `hmac.compare_digest` (thời gian hằng) chống tấn công đo thời gian (Timing Attack).

### 8.2. Cơ chế Nonce và chống tái sử dụng Nonce (Nonce Reuse Prevention)
Trong AES-GCM, việc lặp lại cặp `(Key, Nonce)` dù chỉ một lần sẽ làm lộ khóa xác thực GHASH. Hệ thống giải quyết triệt để như sau:
- **Trên kênh TCP:** Mỗi frame gọi `os.urandom(12)` sinh 12 byte nonce ngẫu nhiên mới và ghép vào đầu bản mã: `nonce(12B) || ciphertext || tag(16B)`.
- **Trên kênh UDP (theo chuẩn QUIC — RFC 9001):**
  - Không gửi kèm nonce để tiết kiệm 12 byte mỗi gói UDP.
  - Tính `nonce = IV XOR packet_number` (trong đó `packet_number` được mở rộng thành 12 byte big-endian rồi XOR với `iv` 12 byte của chiều tương ứng).
  - Vì `packet_number` tăng đơn điệu (`0, 1, 2, ...`) và **không bao giờ dùng lại ngay cả khi gửi lại gói mất (retransmit được cấp `packet_number` mới)**, đồng thời chiều Client$\rightarrow$Server (`c2s`) và Server$\rightarrow$Client (`s2c`) dùng hai bộ `(key, iv)` khác hẳn nhau, nên cặp `(Key, Nonce)` **không bao giờ trùng lặp**.
  - **Bảo vệ Header bằng AAD:** Toàn bộ 21 byte Header UDP (chứa `session_id`, `packet_number`, `stream_id`, `offset`, `flags`) được đưa vào trường **AAD (Associated Authenticated Data)** của `AESGCM.encrypt/decrypt`. Kẻ tấn công không thể sửa đổi `offset` hay `stream_id` trên đường truyền vì phép kiểm tra Tag 16 byte sẽ lập tức phát hiện và ném `CryptoError`.

## 9. Giới hạn đã biết & Hướng phát triển (Known Limitations)

1. **Lỗ hổng Man-in-the-Middle (MITM) khi trao khóa unauthenticated ECDH:**
   - *Phân tích:* Ở bước `KEY_EXCHANGE`, Client và Server trao đổi khóa công khai `X25519` dạng rõ mà chưa có cơ chế xác thực danh tính của Server (không có chứng chỉ số X.509/CA hay chữ ký số Ed25519). Do đó, một kẻ tấn công nghe lén thụ động (passive sniffer như Wireshark) hoàn toàn **không thể đọc hay sửa dữ liệu**, nhưng một kẻ tấn công chủ động (active attacker đứng giữa ARP spoofing) có thể chặn `KEY_EXCHANGE` và thực hiện tấn công Man-in-the-Middle.
   - *Hướng khắc phục:* Gắn cố định khóa công khai định danh của Server (Public Key Pinning bằng Ed25519) vào cấu hình Client để Server ký lên khóa tạm X25519 trong bản tin `KEY_EXCHANGE`, hoặc sử dụng Pre-Shared Key (PSK) của phòng làm việc đưa vào tham số `salt` của HKDF.
2. **Kiến trúc Server-Relayed File Transfer:**
   - Toàn bộ các mảnh dữ liệu file đi qua bộ nhớ RAM của Server (`FileRelay._relay`). Dù không lưu đĩa, băng thông mạng của Server vẫn là nút cổ chai khi có nhiều cặp client truyền file dung lượng lớn đồng thời.
