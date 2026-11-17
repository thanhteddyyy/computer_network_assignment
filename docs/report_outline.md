# DÀN Ý CHI TIẾT BÁO CÁO KỸ THUẬT (TECHNICAL EXPLANATION REPORT OUTLINE)
**Môn học:** Network Application Development (CO3093) — HCMUT  
**Đề tài:** Distributed Streaming-based Client-Server Collaboration Application  
**Thời lượng dự kiến:** 15 – 20 trang (Xuất file PDF nộp bài kèm mã nguồn)

---

## TRANG BÌA (COVER PAGE)
- Tên trường / Khoa: ĐẠI HỌC BÁCH KHOA - ĐHQG-HCM / KHOA KHOA HỌC & KỸ THUẬT MÁY TÍNH
- Tên môn học & Mã môn: Network Application Development (CO3093)
- Tên đề tài: **DISTRIBUTED STREAMING-BASED CLIENT-SERVER COLLABORATION APPLICATION**
- Giảng viên hướng dẫn: [Tên Giảng viên]
- Sinh viên thực hiện: Họ tên, MSSV, Email, Nhóm / Vai trò (Role A: Transport & Security / Role B: Application & UI)
- Thời gian nộp: Tháng 11/2026

---

## MỤC LỤC & DANH MỤC (TABLE OF CONTENTS & LISTS)
- Mục lục nội dung (Table of Contents)
- Danh mục hình ảnh & sơ đồ (List of Figures)
- Danh mục bảng biểu (List of Tables)
- Danh mục thuật ngữ viết tắt (Glossary: HoL, RTT, RTO, SACK, AEAD, MTU, PBKDF2, ECDH...)

---

## CHƯƠNG 1: GIỚI THIỆU & MỤC TIÊU ĐỀ TÀI (INTRODUCTION)
### 1.1. Bối cảnh & Sự cần thiết (Current Practices & Motivation)
- Xu hướng làm việc cộng tác trong mạng cục bộ (LAN), mạng nội bộ doanh nghiệp biệt lập (air-gapped networks) không phụ thuộc Internet hoặc bên thứ ba.
- Vấn đề bảo mật dữ liệu, độ trễ và sự phụ thuộc khi dùng các ứng dụng SaaS thương mại (Slack, Teams).
### 1.2. Mục tiêu của ứng dụng (Goals & Key Features)
- Xây dựng ứng dụng cộng tác Client–Server hoàn chỉnh từ tầng socket nguyên thủy (**Raw TCP & UDP sockets** kết hợp **Python `asyncio`**), cấm framework cấp cao (Flask/Django/WebSocket).
- Tính năng: Xác thực an toàn, tự động tìm kiếm server (LAN Discovery), chat thời gian thực (Broadcast & Unicast), truyền file dung lượng lớn tin cậy qua UDP, chịu lỗi mạng, mã hóa đầu-cuối, và giao diện Web (HTML/JS thuần).
### 1.3. Phân công công việc (Team Responsibilities)
- Bảng phân chia công việc theo vai trò: Người A (Giao vận, Độ tin cậy & Bảo mật) và Người B (Ứng dụng, Định tuyến, Quản lý trạng thái & Giao diện).

---

## CHƯƠNG 2: KIẾN TRÚC HỆ THỐNG (SYSTEM ARCHITECTURE)
*(Trích xuất từ `docs/SYSTEM_ARCHITECTURE.md` và `docs/diagrams/architecture.mmd`)*
### 2.1. Sơ đồ Kiến trúc Tổng thể (High-Level Architecture Diagram)
- Chèn sơ đồ Mermaid / Draw.io thể hiện 3 khối lớn: Client A, Collab Server, Client B.
### 2.2. Phân tầng Chức năng (Modular Layering Principle)
- **Tầng 1: Giao diện (User Interface Layer):** Web UI (HTML/CSS/Vanilla JS) giao tiếp qua HTTP/1.1 Short Polling (500ms) trên loopback `127.0.0.1:8081` + CLI Terminal (`client/cli.py`).
- **Tầng 2: Nghiệp vụ & Dữ liệu (Application Layer):** `HttpServer` (tự parse HTTP/1.1 `read_request`), `ChatService`, `FileService` (chunking 1100B, `.part`, băm `SHA-256` trên `asyncio.to_thread`).
- **Tầng 3: Giao vận & Bảo mật (Transport & Security Layer):** `ControlChannel` (TCP :9000, framing 4B, AES-256-GCM) và `UdpSession` (UDP :9001 tin cậy, QUIC-like, SACK, CUBIC, Stream Multiplexing).
### 2.3. Ba Kênh Tương tác Mạng trong LAN (Interaction Planes)
- **Control Plane (TCP :9000):** Kết nối bền vững, phục vụ Handshake, Auth, Presence, và thỏa thuận truyền file 3 bước.
- **Data Plane (UDP :9001):** Truyền stream chat và truyền dữ liệu file dạng **Streaming Relay trong RAM (Zero-Disk Footprint)**.
- **Discovery Plane (UDP Broadcast :9002):** Server phát gói `ANNOUNCE` định kỳ mỗi 2s, client tự bắt IP không cần gõ tay.

---

## CHƯƠNG 3: ĐẶC TẢ GIAO THỨC TẦNG ỨNG DỤNG (PROTOCOL SPECIFICATION)
*(Trích xuất từ `docs/protocol_spec.md`)*
### 3.1. Đóng khung trên Kênh Điều khiển TCP (Length-Prefixed Framing)
- Sơ đồ byte khung bản tin TCP: Tiền tố 4-byte độ dài Big-Endian + Giới hạn `MAX_FRAME_SIZE = 1 MB`.
- Bảng đặc tả 16 bản tin JSON điều khiển (`HELLO`, `KEY_EXCHANGE`, `REGISTER`, `AUTH_REQ`, `FILE_OFFER`, `FILE_DONE`...).
### 3.2. Cấu trúc Gói tin Nhị phân UDP 21-Byte Header
- Sơ đồ cấu trúc lưới 32-bit chuẩn RFC (`struct !HBBBIIHIH` = 21 Bytes).
- Bảng giải thích chi tiết 9 trường: `Magic (0xC0DE)`, `Version`, `Type` (DATA, ACK, PING, PONG, CLOSE), `Flags (FIN)`, `Session ID`, `Packet Number`, `Stream ID`, `Offset`, `Payload Length`.
- Phân tích ngân sách MTU: $21\text{B} + 1100\text{B} + 16\text{B} = 1137\text{B} \le 1200\text{B}$ chống phân mảnh IP.
### 3.3. Khung Xác nhận Chọn lọc (Selective ACK Frame — SACK)
- Cấu trúc nhị phân `AckFrame` (`largest_acked`, `recv_window`, `ack_delay`, danh sách dải `ranges`).

---

## CHƯƠNG 4: ĐỘ TIN CẬY & ĐIỀU KHIỂN TẮC NGHẼN (RELIABILITY & CONGESTION CONTROL)
### 4.1. Cơ chế Truyền dữ liệu Tin cậy trên UDP (QUIC-like Reliability)
- **Đánh số gói đơn điệu (Monotonic Packet Numbering):** Không tái sử dụng `packet_number` khi retransmit $\rightarrow$ giải quyết triệt để bài toán *RTT Ambiguity* và chống lặp Nonce AES-GCM.
- **Ước lượng RTT & Tính RTO (RFC 6298):** Công thức tính `SampleRTT`, `EstimatedRTT (srtt)`, `DevRTT (rttvar)` và kẹp RTO trong `[200ms, 8s]`.
- **Phát hiện mất gói:** Phối hợp giữa ngưỡng số gói `PACKET_THRESHOLD = 3` (Fast Retransmit qua SACK) và `RTO Timer`.
- **Ghép mảnh theo Stream & Triệt tiêu Head-of-Line (HoL) Blocking:** Ghép theo `offset` độc lập trên từng `RecvStream` (Stream 1 Chat vs Stream 101 File).
### 4.2. Thuật toán Điều khiển Tắc nghẽn CUBIC (RFC 8312)
- Các pha: Slow Start (tăng gấp đôi mỗi RTT), Giảm cửa sổ khi mất gói ($\beta = 0.7$, tính $K$), Tránh tắc nghẽn theo hàm bậc ba $W(t) = C(t-K)^3 + W_{\max}$, Timeout (co về `MIN_CWND = 2`).
- Biểu đồ biến thiên `cwnd` theo thời gian (vẽ từ log CSV của `transport/congestion.py`).
### 4.3. Cơ chế Flow Control & Backpressure
- Điều hòa tốc độ gửi theo `peer_recv_window` và hàng đợi `MAX_QUEUE = 512`.

---

## CHƯƠNG 5: SƠ ĐỒ TUẦN TỰ & MÁY TRẠNG THÁI (SEQUENCE & STATE DIAGRAMS)
*(Trích xuất từ thư mục `docs/diagrams/`)*
### 5.1. Sơ đồ Tuần tự (Sequence Diagrams)
- **Vòng đời tin nhắn Chat (`sequence_message.mmd`):** Trình duyệt $\rightarrow$ HTTP Polling $\rightarrow$ UDP Stream 1/2 $\rightarrow$ Router $\rightarrow$ Người nhận.
- **Vòng đời truyền File tin cậy (`sequence_file_transfer.mmd`):** Thỏa thuận TCP $\rightarrow$ Truyền chunk UDP có mất gói và retransmit bằng SACK $\rightarrow$ Băm SHA-256 $\rightarrow$ Đổi tên nguyên tử `.part`.
- **Sơ đồ tổng quan (`sequence_lifecycle.mmd`):** Bắt tay, đăng nhập và truyền file/chat đồng thời.
### 5.2. Máy Trạng thái (Finite State Machines — FSM)
- Máy trạng thái Client: `state_client.mmd` (`DISCONNECTED` $\rightarrow$ `AUTHENTICATED`).
- Máy trạng thái Server: `state_server.mmd` (`TCP_ACCEPTED` $\rightarrow$ `CLEANED_UP`).
- Máy trạng thái File: `state_file.mmd` (`offered` $\rightarrow$ `relaying` $\rightarrow$ `done` / `failed` / `aborted`).

---

## CHƯƠNG 6: TẦNG BẢO MẬT & MẬT MÃ ỨNG DỤNG (SECURITY LAYER)
*(Trích xuất từ `docs/protocol_spec.md` - Mục 8 & 9)*
### 6.1. Trao đổi khóa & Dẫn xuất khóa phiên (ECDH & HKDF)
- Trao khóa bất đối xứng `ECDH X25519` (Forward Secrecy).
- Hàm dẫn xuất `HKDF-SHA256` sinh 3 bộ khóa: `tcp_key`, `c2s` (client $\rightarrow$ server), `s2c` (server $\rightarrow$ client).
### 6.2. Mã hóa Xác thực Đầu-cuối (AES-256-GCM)
- Kênh TCP: Nonce ngẫu nhiên 12 byte kẹp đầu frame.
- Kênh UDP: Cơ chế `nonce = IV XOR packet_number` không tốn băng thông truyền nonce; kẹp 21 byte Header làm AAD chống giả mạo `offset`/`stream_id`.
- Tách khóa 2 chiều chống trùng lặp Nonce khi cả 2 bên cùng phát gói `pn = 0`.
### 6.3. Bảo vệ Mật khẩu Lưu trữ
- Thuật toán `PBKDF2-HMAC-SHA256` với `200.000` vòng lặp và `salt` 16B; xác thực thời gian hằng bằng `hmac.compare_digest`.
### 6.4. Phân tích An toàn & Giới hạn Mật mã
- Đánh giá khả năng chống Passive Eavesdropping (nghe lén Wireshark).
- Giới hạn: Lỗ hổng Man-in-the-Middle (MITM) khi trao khóa unauthenticated X25519 và hướng khắc phục (Public Key Pinning / PSK).

---

## CHƯƠNG 7: KHẢ NĂNG CHỊU LỖI MẠNG (NETWORK RESILIENCE & ERROR HANDLING)
### 7.1. Bắt lỗi Socket & Đứt kết nối đột ngột
- Xử lý `ChannelClosed`, `ConnectionResetError`, `BrokenPipeError` không làm sập server.
### 7.2. Cơ chế Giám sát Sống chết (Heartbeat PING/PONG & Timeout)
- Gửi `PING` mỗi 2s, quá 6s im lặng hoặc gửi lại quá 10 lần tự động đóng phiên.
### 7.3. Thu hồi Tài nguyên Lũy đẳng (Idempotent Cleanup)
- Cơ chế `_cleanup` (Server), `_on_disconnect` (Client) và `abort_for` (hủy file mồ côi khi 1 bên rớt mạng).

---

## CHƯƠNG 8: KIỂM THỬ & ĐÁNH GIÁ KẾT QUẢ (TESTING & EVALUATION)
*(Trích xuất từ `docs/TEST_CASES.md`)*
### 8.1. Ma trận Kiểm thử 10 Bước Rubric (Master Test Suite)
- Kết quả chạy `python -m unittest tests.test_full_system_suite -v` (toàn bộ 10/10 test classes PASS).
### 8.2. Kiểm thử Tích hợp qua Mạng Giả lập Mất gói (`test_session_loopback`)
- Truyền đồng thời chat và file qua socket UDP thật với 20% packet loss.
### 8.3. Thử nghiệm Tải cao (Stress Test 20 Clients — `tests/spawn_clients.py`)
- Kết quả chạy 20 clients gửi đồng thời 200 tin nhắn qua mạng mất gói 5%, kill đột ngột 3 client (ảnh chụp terminal, bảng thống kê thời gian và tỉ lệ nhận tin 100%).
### 8.4. Thực nghiệm Truyền File lớn & Minh chứng Chống HoL Blocking
- Demo gửi file 10–50 MB: so sánh mã băm SHA-256 gốc và nhận (khớp 100%).
- Minh chứng tin nhắn chat hiển thị ngay trong lúc thanh tiến độ file đang chạy dở.
### 8.5. Bằng chứng Bắt gói Mạng (Wireshark / Hex Dump)
- Ảnh chụp Wireshark bắt cổng 9001: payload hoàn toàn là bản mã AES-256-GCM vô nghĩa, không lộ dữ liệu rõ.

---

## CHƯƠNG 9: THÁCH THỨC KỸ THUẬT & BÀI HỌC KINH NGHIỆM (CHALLENGES & SOLUTIONS)
*(Trích xuất từ `docs/learning_log.md` — Chuẩn 1 trang Reflection)*
- Thách thức 1: Phân mảnh gói IP vs Dính gói TCP.
- Thách thức 2: Đơ Event Loop 2.2s do băm mật khẩu PBKDF2 và giải pháp `asyncio.to_thread`.
- Thách thức 3: Triệt tiêu Head-of-Line Blocking bằng Stream Multiplexing.
- Thách thức 4: Nhập nhằng RTT và tái sử dụng Nonce AES-GCM khi retransmit.
- Thách thức 5: Khóa file trên Windows (`WinError 32`) và Staging File `.part`.
- Thách thức 6: Thu hồi tài nguyên lũy đẳng khi cả TCP và UDP cùng báo mất kết nối.

---

## CHƯƠNG 10: KẾT LUẬN & HƯỚNG PHÁT TRIỂN (CONCLUSION & FUTURE WORK)
### 10.1. Đánh giá mức độ hoàn thành
- Tự chấm điểm đối chiếu 10/10 tiêu chí của đề bài (Đạt 10/10 điểm Rubric).
### 10.2. Hướng phát triển trong tương lai
- Chuyển từ kiến trúc Server-Relayed sang Direct Peer-to-Peer (NAT Traversal / STUN / TURN).
- Bổ sung xác thực danh tính Server bằng chứng chỉ Ed25519 chống tấn công MITM.
- Hỗ trợ gọi thoại thời gian thực (Audio Streaming) trên một Stream ID mới.

---

## TÀI LIỆU THAM KHẢO (REFERENCES)
1. RFC 6298: *Computing TCP's Retransmission Timer*.
2. RFC 8312: *CUBIC for Fast and Long-Distance Networks*.
3. RFC 9000 & 9001: *QUIC: A UDP-Based Multiplexed and Secure Transport* & *Using TLS to Secure QUIC*.
4. RFC 8439: *ChaCha20 and Poly1305 / AES-GCM for Authenticated Encryption*.
5. Python Official Documentation: *`asyncio` — Asynchronous I/O* & *`socket` — Low-level networking interface*.
6. Slide bài giảng môn Phát triển Ứng dụng Mạng (Network Application Development) — HCMUT.
