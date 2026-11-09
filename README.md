# Collab LAN — Distributed Streaming-based Client-Server Collaboration Application

Ứng dụng cộng tác thời gian thực trong mạng LAN (Client–Server) được xây dựng từ tầng socket nguyên thủy (**Raw TCP/UDP Sockets** kết hợp **Python `asyncio`**), không sử dụng bất kỳ HTTP framework (Flask/Django) hay thư viện WebSocket nào.

Hệ thống tự thiết kế giao thức tầng ứng dụng gồm hai kênh tách biệt:
- **Control Plane (TCP — Port `9000`):** Đóng khung 4-byte Big-Endian, thực hiện Handshake (`HELLO` / `HELLO_ACK`), trao đổi khóa bất đối xứng **ECDH X25519**, dẫn xuất khóa **HKDF-SHA256**, mã hóa **AES-256-GCM**, xác thực người dùng (`PBKDF2-HMAC-SHA256` + Salt), đồng bộ danh sách người dùng online và điều phối truyền file (`FILE_OFFER` / `ACCEPT` / `REJECT` / `DONE`).
- **Data Plane (UDP — Port `9001`):** Giao thức truyền luồng tin cậy tự thiết kế lấy cảm hứng từ **Google QUIC**: Header nhị phân 21 byte, đánh số gói đơn điệu (`packet_number`), ước lượng `RTT/RTO` (RFC 6298), xác nhận chọn lọc **Selective ACK (SACK)** chỉ gửi lại đúng gói bị mất, điều khiển tắc nghẽn **CUBIC (RFC 8312)**, đa hợp luồng (**Stream Multiplexing**) chống hoàn toàn **Head-of-Line (HoL) Blocking** giữa luồng Chat (`Stream 1, 2`) và luồng File (`Stream 100 + file_id`), bảo mật từng gói bằng **AES-256-GCM** (`nonce = IV XOR packet_number`, Header 21B làm `AAD`).
- **Service Discovery (UDP Broadcast — Port `9002`):** Server phát bản tin `ANNOUNCE` định kỳ giúp Client tự động tìm thấy IP của Server trong mạng LAN.
- **Web UI (HTTP/1.1 tự viết — Port `8081+`):** Mỗi tiến trình Client tích hợp một bộ phân tích cú pháp HTTP/1.1 tự viết trên socket TCP (`127.0.0.1`) phục vụ giao diện HTML/CSS/JavaScript thuần.

---

## 1. Yêu cầu Hệ thống & Cài đặt (Step-by-Step Installation)

### 1.1. Yêu cầu môi trường
- **Hệ điều hành:** Windows 10/11, macOS, hoặc Linux.
- **Python:** Phiên bản **Python 3.11** trở lên (khuyến nghị Python 3.11 hoặc 3.12).
- **Thư viện ngoài duy nhất:** `cryptography >= 42.0.0` (phục vụ các thuật toán mật mã chuẩn `X25519`, `HKDF`, `AESGCM`).

---

### 1.2. Các bước cài đặt chi tiết

> **Quy tắc bắt buộc:** Mọi câu lệnh bên dưới đều phải được chạy khi đang đứng tại **thư mục gốc của dự án (`collab-app`)**.

#### Bước 1: Mở Terminal và di chuyển vào thư mục gốc của dự án
```bash
cd collab-app
```

#### Bước 2: Tạo môi trường ảo Python (`.venv`)
```bash
python -m venv .venv
```

#### Bước 3: Kích hoạt môi trường ảo
- **Trên Windows (PowerShell):**
  ```powershell
  .\.venv\Scripts\Activate.ps1
  ```
  *(Nếu PowerShell báo lỗi ExecutionPolicy, chạy lệnh: `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` rồi chạy lại lệnh kích hoạt trên).*
- **Trên Windows (Command Prompt - CMD):**
  ```cmd
  .venv\Scripts\activate.bat
  ```
- **Trên macOS / Linux:**
  ```bash
  source .venv/bin/activate
  ```
*(Sau khi kích hoạt thành công, đầu dòng nhắc lệnh sẽ xuất hiện chữ `(.venv)`).*

#### Bước 4: Cài đặt thư viện phụ thuộc (`dependencies`)
```bash
pip install -r requirements.txt
```

#### Bước 5: Xác nhận cài đặt thành công bằng cách chạy bộ kiểm thử tự động
```bash
python -m unittest discover -s tests -t . -v
```
Nếu màn hình in ra `OK` ở dòng cuối cùng, môi trường của bạn đã sẵn sàng 100%.

---

## 2. Hướng dẫn Khởi chạy Server (How to Run the Server)

Mở **Terminal 1** tại thư mục gốc `collab-app` (đã kích hoạt `(.venv)`):

### Chế độ 1: Chạy Server ở điều kiện mạng bình thường
```bash
python -m server.server --host 0.0.0.0 --tcp-port 9000 --udp-port 9001
```
*(Có thể viết gọn là `python -m server.server` vì các giá trị trên là mặc định).*

### Chế độ 2: Chạy Server kèm bộ giả lập mạng xấu (Packet Loss, Delay, Jitter)
Để kiểm chứng độ tin cậy của tầng giao vận UDP (SACK, Retransmission, CUBIC, chống HoL Blocking), khởi chạy Server với tham số `--loss`, `--delay`, `--jitter`:
```bash
# Giả lập mạng rớt 10% gói UDP, trễ 30ms, biến thiên trễ 20ms:
python -m server.server --loss 0.1 --delay 30 --jitter 20
```

**Dấu hiệu Server đã sẵn sàng:**
```text
INFO  [server] TCP lắng nghe tại 0.0.0.0:9000
```
*(Lưu ý: Khi muốn dừng Server, nhấn `Ctrl + C` tại cửa sổ Terminal của Server).*

---

## 3. Hướng dẫn Kết nối Clients (How to Connect the Clients)

Bạn có thể kết nối nhiều Client đồng thời vào Server bằng **Giao diện Web (Web UI)** hoặc **Giao diện Dòng lệnh (CLI)**.

---

### Cách A: Kết nối bằng Giao diện Web trên Trình duyệt (Khuyến nghị)

Mỗi Client chạy một mini HTTP Server cục bộ trên một cổng riêng (`8081`, `8082`, `8083`, ...).

#### Bước 1: Mở Terminal 2 — Khởi chạy Client thứ nhất (ví dụ cho `alice` ở cổng `8081`)
```bash
python -m client.client --http-port 8081 --server 127.0.0.1
```

#### Bước 2: Mở Terminal 3 — Khởi chạy Client thứ hai (ví dụ cho `bob` ở cổng `8082`)
```bash
python -m client.client --http-port 8082 --server 127.0.0.1
```
*(Nếu muốn giả lập mất gói ở cả chiều gửi của Client, thêm `--loss 0.05` vào cuối lệnh).*

#### Bước 3: Mở Trình duyệt Web và Sử dụng
1. Mở tab trình duyệt thứ nhất tại địa chỉ: **`http://localhost:8081`**
   - Nhập địa chỉ Server: `127.0.0.1`
   - Nhập Tên đăng nhập: `alice`, Mật khẩu: `1234`
   - Nhấn **Đăng ký & vào** (lần đầu) hoặc **Đăng nhập** (các lần sau).
2. Mở tab trình duyệt thứ hai tại địa chỉ: **`http://localhost:8082`**
   - Đăng ký / Đăng nhập tài khoản: `bob`, Mật khẩu: `1234`.
3. **Các thao tác trên Giao diện Web:**
   - **Chat chung (Broadcast):** Bấm vào mục `# Phòng chung` ở cột trái, nhập tin nhắn và nhấn `Gửi`.
   - **Chat riêng (Unicast):** Bấm vào tên người dùng (ví dụ `bob`) ở danh sách **Đang online** cột trái, nhập tin nhắn riêng.
   - **Gửi & Nhận File tin cậy qua UDP:** Khi đang ở khung chat riêng với một người, bấm nút **Gửi file** và chọn tệp bất kỳ. Bên nhận sẽ hiện thông báo kèm nút **Nhận / Từ chối** và thanh tiến độ `%` theo thời gian thực. Sau khi kiểm tra khớp `SHA-256`, bấm **Tải về** (hoặc lấy file trực tiếp trong thư mục `downloads/`).
   - **Theo dõi thông số mạng trực tiếp:** Quan sát cột phải (`RTT`, `RTO`, `cwnd (cubic)`, `Đang bay`, `Mất gói`, `Gói lỗi`).

---

### Cách B: Kết nối bằng Giao diện Dòng lệnh (Command Line Interface — CLI)

Mở một Terminal mới cho mỗi Client tại thư mục gốc `collab-app`:
```bash
python -m client.cli --server 127.0.0.1 --loss 0.05
```
*(Nếu bỏ trống `--server`, Client sẽ tự động lắng nghe gói UDP Broadcast `ANNOUNCE` ở cổng `9002` trong 5 giây để tự tìm địa chỉ IP của Server trong LAN).*

#### Bảng các lệnh điều khiển trong CLI:
| Lệnh trong CLI | Chức năng | Ví dụ sử dụng |
|---|---|---|
| `/register <user> <pass>` | Đăng ký tài khoản mới | `/register alice 1234` |
| `/login <user> <pass>` | Đăng nhập vào hệ thống | `/login alice 1234` |
| `<nội dung bất kỳ>` | Gửi tin nhắn vào phòng chat chung (Broadcast) | `Xin chao ca nha!` |
| `/dm <user> <nội dung>` | Gửi tin nhắn riêng cho một người (Unicast) | `/dm bob Nhan rieng cho Bob` |
| `/users` | Xem danh sách người dùng đang online kèm `IP:Port` UDP | `/users` |
| `/send <user> <đường_dẫn>` | Mời gửi file cho người dùng khác | `/send bob bao_cao.pdf` |
| `/accept <file_id>` | Đồng ý nhận file có mã `<file_id>` | `/accept 1` |
| `/reject <file_id>` | Từ chối lời mời nhận file `<file_id>` | `/reject 1` |
| `/files` | Xem danh sách và tiến độ `%` các file đang truyền | `/files` |
| `/stats` | Xem thông số phiên UDP (`rtt_ms`, `rto_ms`, `cwnd`, `loss`...) | `/stats` |
| `/quit` | Đăng xuất có kiểm soát (`LOGOUT`) và thoát | `/quit` |

---

### Cách C: Kết nối giữa nhiều máy tính thật trong cùng mạng LAN / Wi-Fi
1. **Trên máy chạy Server:**
   - Tìm địa chỉ IPv4 LAN của máy Server (dùng lệnh `ipconfig` trên Windows hoặc `ip a` / `ifconfig` trên Linux/macOS, ví dụ `192.168.1.50`).
   - Mở Firewall cho cổng `9000/TCP` và `9001/UDP`, `9002/UDP`.
   - Chạy: `python -m server.server --host 0.0.0.0`
2. **Trên các máy chạy Client:**
   - Chạy Web UI: `python -m client.client --http-port 8081 --server 192.168.1.50`
   - Hoặc để tự động tìm Server qua UDP Broadcast: `python -m client.client --http-port 8081`

---

## 4. Hướng dẫn Kiểm thử & Mô phỏng Mạng (Testing & Simulation)

```bash
# 1. Chạy toàn bộ các bộ Unit Test & Integration Test trong dự án:
python -m unittest discover -s tests -t . -v

# 2. Chạy riêng bộ Master Test Suite kiểm tra từ Step 1 đến Step 10 của Rubric:
python -m unittest tests.test_full_system_suite -v

# 3. Chạy kiểm thử tích hợp tầng Transport qua socket UDP thật với 20% mất gói:
python -m unittest tests.test_session_loopback -v

# 4. Chạy Stress Test 20 Client đồng thời (mỗi client gửi 10 tin, mất gói 5%, kill đột ngột 3 client):
#    (Lưu ý: Cần bật `python -m server.server` ở một Terminal khác trước khi chạy lệnh này)
python -m tests.spawn_clients --n 20 --m 10 --loss 0.05 --kill 3
```

---

## 5. Cấu trúc Mã nguồn & Tài liệu (Project Structure)

| Đường dẫn | Vai trò trong hệ thống | Bước Rubric |
|---|---|---|
| `common/config.py` | Tập trung toàn bộ hằng số cấu hình (Cổng, MTU, `CHUNK_SIZE`, `RTO`, `CUBIC`) | Step 1 – 9 |
| `common/framing.py` | Đóng khung bản tin TCP 4-byte Big-Endian, chặn gói quá `MAX_FRAME_SIZE` | Step 1, 2 |
| `common/crypto.py` | Trao khóa `X25519`, dẫn xuất `HKDF-SHA256`, mã hóa `AES-256-GCM`, băm `PBKDF2` | Step 3, 8 |
| `common/messages.py` | Định nghĩa các loại bản tin điều khiển JSON và kiểm tra hợp lệ | Step 1, 2 |
| `transport/tcp_channel.py` | Kênh điều khiển TCP bất đồng bộ có khóa gửi và mã hóa AES-GCM | Step 2, 7, 8 |
| `transport/packet.py` | Codec gói nhị phân UDP 21-byte Header, `AckFrame` (SACK), `seal`/`open` AEAD | Step 1, 5, 8 |
| `transport/udp_endpoint.py` | Socket UDP không chặn, phân luồng datagram theo `session_id` | Step 4, 5 |
| `transport/loss_sim.py` | Bộ giả lập mất gói, độ trễ và biến thiên trễ (Loss/Delay/Jitter Simulator) | Step 5, 6, 10 |
| `transport/reliability.py` | Ước lượng `RTT/RTO` (RFC 6298), `ReceiveTracker`, `SendTracker` (SACK) | Step 5, 6 |
| `transport/stream.py` | `SendStream` (cắt mảnh) và `RecvStream` (ghép theo offset, chống HoL Blocking) | Step 5, 6 |
| `transport/congestion.py` | Bộ điều khiển tắc nghẽn **CUBIC (RFC 8312)** và xuất log CSV | Step 6 |
| `transport/session.py` | Điều phối phiên UDP hoàn chỉnh (hàng đợi gửi, timer 20ms, heartbeat `PING/PONG`) | Step 5, 6, 7, 8 |
| `server/server.py` | Điểm vào của Server: Handshake, Trao khóa, Xác thực (`to_thread`), `_cleanup` | Step 2, 3, 4, 7, 8 |
| `server/auth.py` | Quản lý lưu trữ tài khoản người dùng (`data/users.json`) | Step 3 |
| `server/session_manager.py` | Quản lý danh sách Client đang online (Username, IP, Port, Session ID) | Step 3, 4 |
| `server/router.py` | Định tuyến tin nhắn Chat Broadcast (`*`) và Unicast (`dm`) | Step 5 |
| `server/file_relay.py` | Điều phối truyền file và chuyển tiếp luồng UDP trong RAM (`_relay`, `abort_for`) | Step 6, 7 |
| `server/discovery.py` & `client/discovery.py` | Phát và lắng nghe quảng bá UDP `ANNOUNCE` tìm Server trong LAN | Step 3 |
| `client/client.py` | Máy trạng thái Client (`DISCONNECTED` $\rightarrow$ `AUTHENTICATED`), `_on_disconnect` | Step 2, 3, 7, 8 |
| `client/chat.py` & `client/file_transfer.py` | Dịch vụ gửi/nhận Chat và File (`CHUNK_SIZE`, `.part`, kiểm tra `SHA-256`) | Step 5, 6 |
| `client/http_server.py` | Bộ phân tích HTTP/1.1 tự viết (`read_request`) và các REST API cho trình duyệt | Step 9 |
| `client/static/` | Giao diện người dùng Web (`index.html`, `style.css`, `app.js` thuần) | Step 9 |
| `docs/protocol_spec.md` | Đặc tả giao thức chi tiết, máy trạng thái, xử lý lỗi và phân tích bảo mật | Step 1, 7, 8 |
| `docs/TEST_CASES.md` | Tài liệu mô tả toàn bộ các kịch bản kiểm thử ánh xạ theo 10 bước Rubric | Step 10 |
| `docs/learning_log.md` | Tổng hợp các thách thức kỹ thuật & giải pháp (*Challenges & Solutions*) | Báo cáo PDF |
| `docs/DEVELOPMENT_LOG.md` | Nhật ký tiến hóa của từng phương thức từ Step 1 đến Step 9 | Ôn tập vấn đáp |
| `docs/diagrams/` | Bộ sơ đồ kiến trúc, State Diagrams và Sequence Diagram (`.mmd`) | Step 1 & Báo cáo |
