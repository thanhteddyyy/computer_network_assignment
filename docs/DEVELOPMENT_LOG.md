# NHẬT KÝ PHÁT TRIỂN & NÂNG CẤP HỆ THỐNG (STEP 1 → STEP 8)

Tài liệu này tổng hợp toàn bộ các phương thức đã được cài đặt, sửa đổi và nâng cấp theo từng bước của Rubric để phục vụ ôn tập vấn đáp và viết báo cáo.

---

## Tổng quan tiến trình nâng cấp theo thời gian

| Giai đoạn | Mục tiêu nâng cấp | File tác động chính | Giá trị mang lại cho hệ thống |
|---|---|---|---|
| **Step 1 & 2** | Đóng khung TCP & Handshake `HELLO` / `HELLO_ACK` | `transport/tcp_channel.py` | Phân tách dòng byte TCP thành các message JSON có kiểm soát kích thước (`MAX_FRAME_SIZE = 1MB`) và timeout rõ ràng. |
| **Step 8 (P1)** | Trao khóa ECDH X25519 + HKDF-SHA256 | `server/server.py`, `client/client.py`, `common/crypto.py` | Thiết lập khóa phiên động (Forward Secrecy) ngay sau `HELLO_ACK`, bảo vệ cả mật khẩu đăng nhập ở bước sau. |
| **Step 3** | Đăng ký / Đăng nhập (PBKDF2) & Quản lý phiên | `server/auth.py`, `server/session_manager.py`, `server/server.py`, `client/client.py` | Lưu mật khẩu an toàn bằng `PBKDF2-HMAC-SHA256` (200.000 vòng) + `salt`, chống timing attack bằng `hmac.compare_digest`. |
| **Step 4** | Xử lý đồng thời 20+ clients & Offload CPU-bound | `server/server.py` | Chuyển `users.register` và `users.verify` sang `asyncio.to_thread`, giảm thời gian chờ `HELLO_ACK` từ **2256ms xuống 22ms**. |
| **Step 5a** | Định tuyến Chat Broadcast & Unicast | `server/router.py`, `client/chat.py` | Kiểm tra nghiêm ngặt kiểu dữ liệu bằng `isinstance` (chặn lỗi `str(None)`), định tuyến tin nhắn chung (`*`) và riêng (`username`). |
| **Step 5b** | Tầng giao vận UDP tin cậy (QUIC-like) | `transport/reliability.py`, `transport/stream.py`, `common/config.py` | Ước lượng RTT/RTO (RFC 6298), Selective ACK (SACK), phát hiện mất gói sớm (packet threshold = 3), ghép mảnh theo `offset` độc lập từng stream. |
| **Step 6a** | Thỏa thuận gửi/nhận file qua kênh điều khiển TCP | `server/file_relay.py`, `client/file_transfer.py`, `client/cli.py` | Luồng `FILE_OFFER` → `FILE_OFFER_ACK` → `FILE_ACCEPT` / `FILE_REJECT` → `FILE_DONE`; chống Path Traversal bằng `os.path.basename`. |
| **Step 6b** | Truyền dữ liệu file thật qua UDP & Chống HoL Blocking | `server/file_relay.py`, `client/file_transfer.py` | Server relay luồng trong RAM (không lưu đĩa); Client đọc/ghi từng khúc `CHUNK_SIZE = 1100B` vào file `.part`, kiểm tra `sha256_file` bằng `to_thread`, đổi tên nguyên tử `os.replace`. |
| **Step 6c** | Điều khiển tắc nghẽn CUBIC (RFC 8312) | `transport/congestion.py`, `transport/session.py` | Thay thế cửa sổ cố định `FixedWindow` bằng `Cubic` (Slow Start, hàm bậc 3 tránh tắc nghẽn, giảm $\beta = 0.7$ khi loss, co về `MIN_CWND` khi timeout). |
| **Step 7** | Chịu lỗi mạng & Dọn dẹp tài nguyên (Resilience) | `server/file_relay.py`, `server/server.py`, `client/client.py` | Hủy file mồ côi (`abort_for`), dọn dẹp lũy đẳng (`_cleanup` và `_on_disconnect` gọi nhiều lần không lỗi), chuyển file đang truyền dở sang `failed`. |
| **Step 8 (P2)** | Mã hóa & Giải mã toàn bộ Payload (TCP + UDP) | `common/crypto.py`, `transport/tcp_channel.py`, `transport/packet.py`, `transport/session.py` | AES-256-GCM cho cả TCP (random nonce) và UDP (`IV XOR packet_number`, Header làm AAD, tách khóa 2 chiều `c2s`/`s2c`). |

---

## Chi tiết từng phương thức đã thay đổi & nâng cấp

### 1. Tầng Giao Vận & Độ Tin Cậy (`transport/`)

| File | Phương thức / Class | Thay đổi đã thực hiện | Tại sao nâng cấp này quan trọng? |
|---|---|---|---|
| `transport/tcp_channel.py` | `ControlChannel.recv` | Tách riêng `asyncio.TimeoutError` (không gộp vào `ChannelClosed`), chuyển `FrameError` và lỗi JSON/giải mã thành `ProtocolError`. | Giúp phân biệt rõ client chậm trả lời (timeout), client gửi dữ liệu sai giao thức, và client đã đóng kết nối. |
| `transport/reliability.py` | `RttEstimator.update` | Cài đặt công thức Jacobson/Karels (RFC 6298): tính `srtt`, `rttvar` và `rto = clamp(srtt + 4*rttvar, MIN_RTO, MAX_RTO)`. | Giúp thời gian chờ gửi lại (RTO) tự co giãn linh hoạt theo độ trễ thực tế của mạng LAN/WAN. |
| `transport/reliability.py` | `ReceiveTracker.on_packet` & `build_ack` | Chống gói trùng lặp, gom các khoảng `packet_number` đã nhận thành danh sách `ranges` giảm dần cho `AckFrame`. | Tạo khung Selective ACK (SACK) kiểu QUIC để báo chính xác những gói nào đã tới, gói nào bị khuyết. |
| `transport/reliability.py` | `SendTracker.on_ack` & `pop_timed_out` | Xác định gói đã được ACK và gói bị mất khi có gói mới hơn vượt quá ngưỡng `PACKET_THRESHOLD = 3` hoặc quá hạn `rto`. | Chỉ gửi lại **đúng gói bị mất**, không phải gửi lại toàn bộ các gói phía sau. |
| `transport/stream.py` | `RecvStream.on_data` | Lưu tạm gói đến sớm vào `_pending[offset]`, ghép liên tục khi đủ `next_offset`, đẩy message lên hàng đợi khi gặp cờ `FIN`. | Đảm bảo dữ liệu đúng thứ tự trong từng stream nhưng **không chặn giữa các stream khác nhau** (chống Head-of-Line blocking). |
| `transport/congestion.py` | `Cubic.on_ack`, `on_loss`, `on_timeout` | Cài đặt thuật toán CUBIC: Slow Start tăng theo `acked_count`, tránh tắc nghẽn theo hàm bậc 3 $W(t) = C(t-K)^3 + W_{\max}$, chống giảm `cwnd` nhiều lần trong cùng 1 RTT (`0.1s`). | Tận dụng tối đa băng thông đường truyền và tự động giảm tốc khi mạng bị nghẽn/mất gói. |
| `transport/session.py` | `UdpSession.__init__` | Nâng cấp bộ điều khiển tắc nghẽn mặc định từ `FixedWindow()` sang `Cubic()`. | Đưa thuật toán CUBIC vào vận hành thực tế cho mọi luồng chat và truyền file. |

---

### 2. Phía Server (`server/`)

| File | Phương thức / Class | Thay đổi đã thực hiện | Tại sao nâng cấp này quan trọng? |
|---|---|---|---|
| `server/server.py` | `CollabServer._key_exchange` | Nhận public key X25519 của client, gửi public key của server, dẫn xuất `SessionKeys` và bật `ch.enable_encryption(keys.tcp_key)`. | Mã hóa toàn bộ kênh điều khiển TCP trước khi client gửi mật khẩu. |
| `server/server.py` | `CollabServer._authenticate` | Bọc `self.users.register` và `self.users.verify` trong `await asyncio.to_thread(...)`. | Không để phép băm PBKDF2 (200.000 vòng) làm đơ Event Loop khi 20 client đăng nhập cùng lúc. |
| `server/router.py` | `parse_chat` & `Router.handle_chat` | Kiểm tra `isinstance(to, str)` và `isinstance(text, str)`, giới hạn `MAX_TEXT`, định tuyến broadcast (`*`) và unicast. | Chặn đứng gói tin chat sai định dạng, định tuyến nhanh qua các stream UDP của người nhận. |
| `server/file_relay.py` | `on_offer`, `on_accept`, `on_reject`, `on_done`, `_find` | Quản lý vòng đời truyền file qua TCP, kiểm tra đúng vai trò (`sender`/`receiver`) và trạng thái hợp lệ. | Chống giả mạo người nhận đồng ý thay hoặc đồng ý 2 lần. |
| `server/file_relay.py` | `FileRelay._relay` | Đọc từng chunk từ stream `100 + file_id` của người gửi và đẩy thẳng sang stream của người nhận trong RAM; báo lỗi nếu người nhận rớt mạng. | Chuyển tiếp tốc độ cao không chạm ổ đĩa cứng của Server (Zero-disk footprint), bảo mật file riêng tư. |
| `server/file_relay.py` | `FileRelay.abort_for` | Duyệt bản sao `list(self.transfers.values())`, xóa các transfer của user rời mạng và gửi `FILE_DONE(ok=False)` cho đối tác. | Giải phóng tài nguyên trên Server và giúp đối tác không bị treo chờ vô hạn khi người kia rớt mạng. |
| `server/server.py` | `CollabServer._cleanup` | Kiểm tra `removed = self.sessions.remove(...)`, cancel và `gather` mọi task nền, gỡ `udp.on_close`, chỉ broadcast `USER_LEAVE` khi `removed is not None`. | Đảm bảo tính **Idempotent** (gọi dọn dẹp 2 lần liên tiếp không gây lỗi và không bắn thông báo `USER_LEAVE` trùng lặp). |

---

### 3. Phía Client (`client/`)

| File | Phương thức / Class | Thay đổi đã thực hiện | Tại sao nâng cấp này quan trọng? |
|---|---|---|---|
| `client/client.py` | `_key_exchange`, `register`, `login` | Trao khóa X25519, mở cổng UDP cục bộ, gửi `AUTH_REQ`, khởi chạy `UdpSession` và `_control_loop`. | Hoàn thiện luồng kết nối bảo mật và đồng bộ danh sách người dùng online. |
| `client/chat.py` | `ChatService.send` | Chọn đúng `STREAM_GLOBAL_CHAT` (`1`) hoặc `STREAM_DIRECT_CHAT` (`2`) và gửi payload JSON qua UDP. | Tách luồng chat độc lập hoàn toàn với luồng truyền file (`100+`). |
| `client/file_transfer.py` | `offer`, `on_offer_ack`, `accept`, `reject` | Tính `sha256_file` trong `to_thread`, khởi tạo task `_receive_file` trước khi gửi `FILE_ACCEPT`. | Tránh đơ giao diện khi chọn file 50MB và tránh mất gói UDP đầu tiên do Race Condition. |
| `client/file_transfer.py` | `FileService._send_file` | Đọc file từng khúc `CHUNK_SIZE` (`1100B`), gửi qua UDP, cập nhật tiến độ mỗi 2%, bắt lỗi `(SessionClosed, OSError)`. | Truyền được file dung lượng bất kỳ mà chỉ tốn ~1.1KB RAM cho mỗi bước đọc. |
| `client/file_transfer.py` | `FileService._receive_file` | Ghi vào file tạm `.part`, đóng file rồi kiểm tra `sha256_file` bằng `to_thread`; khớp thì `os.replace`, lệch thì `os.remove`. | Đảm bảo file xuất hiện trong `downloads/` luôn toàn vẹn 100%, không bao giờ để lại file hỏng/thiếu. |
| `client/client.py` | `CollabClient._on_disconnect` | Chốt trạng thái `DISCONNECTED` ngay đầu hàm, đóng UDP/TCP, chuyển mọi file đang truyền dở (`offered`, `accepted`, `sending`, `receiving`) sang `failed`. | Giúp giao diện phía Client phản ánh đúng trạng thái lỗi ngay khi mất kết nối tới Server. |

