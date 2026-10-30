# Protocol Specification (bản nháp v1)

> Tài liệu này là nguồn sự thật duy nhất của giao thức. Code phải khớp với nó; đổi giao thức thì sửa file này trước.
> Những mục đánh dấu **[cần hoàn thiện]** là phần nhóm tự viết ở buổi sáng ngày 1.

## 1. Tổng quan kiến trúc

| Kênh | Giao thức | Cổng | Mã hóa | Dùng cho |
|---|---|---|---|---|
| Điều khiển | TCP | 9000 | AES-GCM (sau KEY_EXCHANGE) | Handshake, trao khóa, đăng nhập, presence, điều khiển file |
| Dữ liệu | UDP + giao thức tin cậy riêng | 9001 | AES-GCM, header làm AAD | Chat (stream 1, 2), dữ liệu file (stream 100+id) |
| Discovery | UDP broadcast | 9002 | Không (thông tin công khai) | Server thông báo địa chỉ cho client trong LAN |
| Giao diện | HTTP/1.1 tự viết | 8081+ (chỉ 127.0.0.1) | Không (chỉ trong máy) | Trình duyệt ↔ tiến trình client |

Hai kênh được gắn với nhau bằng `session_id` 32 bit ngẫu nhiên do server cấp trong `AUTH_OK`.

## 2. Kênh TCP

**Đóng khung:** `[độ dài 4 byte, big-endian][payload]`, payload tối đa 1 MB.
Trước KEY_EXCHANGE payload là JSON rõ; sau đó payload = `nonce(12) || AES-GCM(JSON)`.

| Message | Chiều | Trường | Ghi chú |
|---|---|---|---|
| HELLO | C→S | version, client_name | Message đầu tiên, timeout 10 s |
| HELLO_ACK | S→C | version, server_name | Bước 2 rubric |
| KEY_EXCHANGE | C→S rồi S→C | public_key (hex, X25519 32 byte) | Gửi rõ; sau đó cả hai bật mã hóa |
| REGISTER | C→S | username, password | Phản hồi REGISTER{ok} hoặc AUTH_FAIL{reason} |
| AUTH_REQ | C→S | username, password, udp_port | |
| AUTH_OK | S→C | session_id, udp_port, users[] | |
| AUTH_FAIL | S→C | reason | Client được thử lại |
| USER_LIST | S→C | users[] | users: {username, ip, port, since} |
| USER_JOIN / USER_LEAVE | S→C | user / username, reason | Broadcast |
| FILE_OFFER | C→S | client_ref, to, name, size, sha256 | Server chuyển cho người nhận kèm file_id, from |
| FILE_OFFER_ACK | S→C | client_ref, file_id \| error | |
| FILE_ACCEPT / FILE_REJECT | C→S→C | file_id (, reason) | |
| FILE_DONE | C→S→C | file_id, ok, reason? | Người nhận gửi sau khi kiểm tra SHA-256 |
| LOGOUT | C→S | | Thoát có kiểm soát |
| ERROR | S→C | reason | |

## 3. Gói UDP

Header 21 byte, network byte order (`struct` format `!HBBBIIHIH`):

| Trường | Byte | Ý nghĩa |
|---|---|---|
| Magic | 2 | 0xC0DE |
| Version | 1 | 1 |
| Type | 1 | 1 DATA, 2 ACK, 3 PING, 4 PONG, 5 CLOSE |
| Flags | 1 | bit 0 = FIN (End-of-Message) |
| Session ID | 4 | cấp trong AUTH_OK |
| Packet Number | 4 | tăng đơn điệu theo từng chiều, không tái sử dụng khi gửi lại |
| Stream ID | 2 | 1 chat chung, 2 chat riêng, 100+file_id file |
| Offset | 4 | vị trí byte trong stream |
| Payload Length | 2 | độ dài ciphertext (gồm 16 byte tag) |

**Kích thước:** 21 + 1100 (dữ liệu) + 16 (tag) = 1137 byte ≤ 1200 → không phân mảnh IP.

**Mã hóa:** khóa riêng mỗi chiều (c2s, s2c) sinh bằng HKDF từ ECDH X25519; nonce = IV ⊕ packet_number; AAD = header.

**ACK payload:** `largest_acked(4) recv_window(2) ack_delay_ms(2) n_ranges(2)` + n × `(start(4), end(4))`, các khoảng giảm dần.

## 4. Độ tin cậy, thứ tự, điều khiển luồng và tắc nghẽn

- Phát hiện mất gói: (a) ngưỡng gói, khi gói p chưa ACK mà đã có gói ≥ p+3 được ACK; (b) quá RTO.
- RTO theo RFC 6298, kẹp trong [200 ms, 8 s], nhân đôi khi timeout.
- Gửi lại: chỉ đúng đoạn bị mất, với packet number mới.
- Thứ tự: ghép theo offset **riêng cho từng stream**, nên mất gói ở stream file không chặn stream chat (không có HoL blocking).
- Flow control: số gói đang bay ≤ min(cwnd, recv_window của peer).
- Congestion control: CUBIC đơn giản hóa (slow start, giảm 0.7 khi mất gói, hàm bậc ba khi tránh tắc nghẽn).
- Gom ACK: tối đa 20 ms.

## 5. Heartbeat và phát hiện mất kết nối

Client gửi PING mỗi 2 s, server trả PONG. Một bên im lặng quá 6 s thì bên kia đóng phiên. TCP đóng cũng kết thúc phiên.

## 6. Máy trạng thái **[cần hoàn thiện]**

- Client: DISCONNECTED → CONNECTED → KEY_EXCHANGED → AUTHENTICATED → DISCONNECTED. Xem `diagrams/state_client.mmd`.
- Phiên phía server: **[cần hoàn thiện]**: vẽ các trạng thái và sự kiện chuyển (HELLO, KEY_EXCHANGE, AUTH_OK, LOGOUT, timeout, lỗi).
- Một lượt truyền file: offered → accepted → sending/receiving → done | rejected | failed.

## 7. Xử lý lỗi **[cần hoàn thiện]**

| Tình huống | Phát hiện bằng | Xử lý |
|---|---|---|
| Client bị kill | TCP đóng / heartbeat timeout | Dọn phiên, broadcast USER_LEAVE, hủy file đang truyền |
| Gói UDP rác hoặc giả mạo | sai magic / giải mã thất bại | Bỏ qua, đếm `bad_packets` |
| Gói gửi lại quá 10 lần | MAX_RETRANSMIT | Đóng phiên |
| … | … | … |

## 8. Giới hạn đã biết

- Khóa công khai X25519 chưa được xác thực, nên về lý thuyết vẫn có thể bị tấn công man-in-the-middle khi trao khóa.
- File được relay qua server, nên băng thông server là nút cổ chai.
