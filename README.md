# Collab LAN: Distributed Streaming-based Client-Server Collaboration Application

Ứng dụng chat và gửi file trong mạng LAN. Giao thức tầng ứng dụng tự thiết kế gồm hai phần:

- **TCP** cho kênh điều khiển (handshake, trao khóa, đăng nhập, presence).
- **UDP với độ tin cậy tự viết** cho chat và file: SACK, sliding window, RTO, CUBIC, nhiều stream để không bị Head-of-Line blocking, mã hóa AES-GCM.

Giao diện là trang web JavaScript thuần, nói chuyện với tiến trình client qua một mini HTTP server tự viết bằng socket.

## 1. Cài đặt

Yêu cầu: Python 3.11 trở lên.

```bash
python -m venv .venv
# Windows:      .venv\Scripts\activate
# macOS/Linux:  source .venv/bin/activate
pip install -r requirements.txt
```

## 2. Chạy

Mọi lệnh chạy từ **thư mục gốc dự án**.

```bash
# Terminal 1: server
python -m server.server
python -m server.server --loss 0.1 --delay 30 --jitter 20    # giả lập mạng xấu

# Terminal 2, 3, ...: client có giao diện web (mỗi client một cổng HTTP)
python -m client.client --http-port 8081 --server 127.0.0.1
python -m client.client --http-port 8082 --server 127.0.0.1
# Mở http://localhost:8081 và http://localhost:8082 ở hai tab trình duyệt

# Client dòng lệnh (test nhanh khi chưa có UI)
python -m client.cli --server 127.0.0.1

# Stress test 20 client (bước 4, 7, 10)
python -m tests.spawn_clients --n 20 --m 10 --loss 0.05 --kill 3
```

Chạy trên nhiều máy trong LAN: server chạy với `--host 0.0.0.0` (mặc định). Client dùng `--server <IP máy server>`, hoặc bỏ trống để tự tìm khi discovery đã làm xong. Nhớ mở firewall cho cổng 9000/TCP và 9001, 9002/UDP.

## 3. Kiểm thử

```bash
python -m unittest discover -s tests -t . -v
```

Test của các phần **chưa làm sẽ được SKIP**. Làm xong TODO nào thì test tương ứng tự chạy thật và phải PASS.
`test_session_loopback` là bài kiểm tra quan trọng nhất của tầng transport: 100 tin nhắn và 300 KB dữ liệu qua UDP thật với 20% mất gói.

## 4. Cấu trúc và trạng thái

| Đường dẫn | Vai trò | Trạng thái |
|---|---|---|
| `common/config.py` | Mọi hằng số (cổng, MTU, RTO, …) | ✅ Hoàn thiện |
| `common/framing.py` | Đóng khung message trên TCP | ✅ |
| `common/crypto.py` | X25519, HKDF, AES-GCM, băm mật khẩu | ✅ |
| `common/messages.py` | Tên các message TCP | ✅ |
| `transport/tcp_channel.py` | Kênh TCP gửi/nhận JSON, có mã hóa | ✅ |
| `transport/packet.py` | Header UDP 21 byte, ACK frame, seal/open | ✅ |
| `transport/udp_endpoint.py` | Socket UDP, chia gói theo session_id | ✅ |
| `transport/loss_sim.py` | Giả lập mất gói, trễ, đảo thứ tự | ✅ |
| `transport/session.py` | Điều phối phiên UDP (gửi, nhận, timer, heartbeat) | ✅ phần điều phối |
| `transport/reliability.py` | RTT/RTO, SACK, phát hiện mất gói | 🔧 A ngày 2 |
| `transport/stream.py` | Ghép dữ liệu theo stream (chống HoL) | 🔧 A ngày 2 |
| `transport/congestion.py` | FixedWindow ✅, CUBIC | 🔧 A ngày 2 |
| `transport/mock_session.py` | Phiên giả để B làm trước khi transport xong | ✅ |
| `server/server.py` | Vòng accept, HELLO ✅, trao khóa, xác thực, dọn dẹp | 🔧 A ngày 1, B ngày 1, A ngày 3 |
| `server/session_manager.py` | Bảng client online | 🔧 B ngày 1 |
| `server/auth.py` | Tài khoản (`data/users.json`) | 🔧 B ngày 1 |
| `server/discovery.py`, `client/discovery.py` | UDP broadcast tìm server | 🔧 B ngày 1 |
| `server/router.py` | Định tuyến chat chung/riêng | 🔧 B ngày 2 |
| `server/file_relay.py` | Relay file qua server | 🔧 A ngày 2 |
| `client/client.py` | Logic client, máy trạng thái | 🔧 A ngày 1, B ngày 1 |
| `client/chat.py`, `client/file_transfer.py` | Chat và file phía client | 🔧 B ngày 2 |
| `client/http_server.py` | Mini HTTP server (route + API ✅, parse request) | 🔧 B ngày 2 |
| `client/static/` | Giao diện HTML/CSS/JS thuần | ✅ bản chạy được, B hoàn thiện ngày 3 |
| `tests/` | Unit test, test tích hợp, stress test | ✅ |
| `docs/` | Protocol spec, sơ đồ Mermaid, learning log, dàn ý báo cáo | 📝 cần hoàn thiện |

**Tìm việc của mình:** mỗi chỗ cần làm có nhãn `TODO[<người>-D<ngày>]`, kèm hướng dẫn từng bước ngay trong docstring.

```bash
grep -rn "TODO\[A-D1" .      # việc của A ngày 1
grep -rn "TODO\[B-D2" .      # việc của B ngày 2
```

## 5. Thứ tự làm và cách kiểm tra

| Ngày | Người | TODO | Xong khi |
|---|---|---|---|
| 1 | A | `_key_exchange` ở server và client | Client CLI kết nối được, log có "kênh TCP đã mã hóa" |
| 1 | B | `UserStore`, `SessionManager`, `_authenticate`, `register`/`login` client, discovery | 3 CLI client đăng nhập, thấy nhau, kill 1 thì 2 người kia nhận USER_LEAVE |
| 2 | A | `reliability.py`, `stream.py`, `Cubic`, `file_relay.py` | `test_reliability` và `test_session_loopback` PASS |
| 2 | B | `read_request`, `ChatService.send`, `Router.handle_chat`, `FileService` | `test_mock_and_http` PASS; chat và file chạy qua UI |
| 3 | A | Các `TODO[A-D3]`: dọn dẹp và chịu lỗi | `spawn_clients --n 20 --kill 3` không làm server lỗi |
| 3 | B | `TODO[B-D3]` trong `app.js`: hoàn thiện giao diện | Demo đủ: danh sách user, chat, tiến độ file, trạng thái mạng |

## 6. Quy ước làm việc

- **Không đổi giao thức trong code trước khi sửa `docs/protocol_spec.md`.**
- Mỗi người làm trên branch riêng (`a/transport`, `b/app`). Merge vào `main` mỗi tối sau khi test pass.
- Với LLM: hỏi giải thích trước, tự vẽ ra giấy, rồi mới nhờ sinh code theo docstring. Người còn lại review và hỏi "tại sao" trước khi merge.
- Ghi mọi lỗi gặp phải vào `docs/learning_log.md`.

## 7. Sự cố thường gặp

| Hiện tượng | Cách xử lý |
|---|---|
| `ModuleNotFoundError: common` | Chạy bằng `python -m ...` từ thư mục gốc, không chạy trực tiếp file |
| `Address already in use` | Một server hoặc client cũ còn chạy. Tắt nó, hoặc đổi `--tcp-port`/`--http-port` |
| Windows: `WinError 10054` trên UDP | Đã xử lý trong `udp_endpoint.py`. Nếu vẫn gặp, xem log `error_received` |
| Không tìm thấy server qua discovery | Router hoặc firewall chặn broadcast. Dùng `--server <IP>` |
| Trình duyệt báo lỗi 500 "chưa cài đặt" | API đó gọi tới một TODO chưa làm. Xem log terminal của client |
