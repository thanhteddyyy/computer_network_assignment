# ĐẶC TẢ TOÀN BỘ TEST CASES (FULL TEST SPECIFICATION — RUBRIC 1 → 10)

Tài liệu này liệt kê và giải thích toàn bộ các kịch bản kiểm thử (Unit Tests, Integration Tests, Resilience Tests, Security Tests, và Stress/Simulation Tests) đối chiếu trực tiếp với yêu cầu trong đề bài **Assignment 1 - Distributed Streaming-based Client-Server Collaboration Application**.

---

## 1. Ma trận bao phủ Rubric (Rubric Traceability Matrix)

| Step | Milestone trong Đề bài | File Test đảm nhiệm | Số lượng Test | Trạng thái |
|---|---|---|---|---|
| **Step 1** | **Setup & Protocol Design:** Cấu trúc gói UDP 21B, đóng khung TCP 4B big-endian, ACK frame. | `tests/test_packet.py`<br/>`tests/test_full_system_suite.py` | 8 tests | ✅ PASS |
| **Step 2** | **Basic Client-Server Handshake:** Kết nối TCP, trao đổi `HELLO` / `HELLO_ACK`, kiểm tra version, timeout. | `tests/test_full_system_suite.py` | 4 tests | ✅ PASS |
| **Step 3** | **Authentication & State Management:** Đăng ký/đăng nhập (`PBKDF2` + `salt`), quản lý `SessionManager` (IP, cổng TCP/UDP, `session_id`), chống đăng nhập trùng. | `tests/test_auth_and_sessions.py`<br/>`tests/test_full_system_suite.py` | 9 tests | ✅ PASS |
| **Step 4** | **Concurrency Implementation:** Xử lý đồng thời 10–20+ clients bằng `asyncio`, đẩy băm mật khẩu sang `asyncio.to_thread` không chặn Event Loop. | `tests/test_full_system_suite.py`<br/>`tests/spawn_clients.py` | 3 tests + Stress script | ✅ PASS |
| **Step 5** | **Time-sensitive Streaming:** Định tuyến chat Broadcast (`*`) & Unicast (`dm`), độ tin cậy UDP (RTT/RTO, SACK, fast retransmit, ghép theo offset), chống Head-of-Line (HoL) blocking. | `tests/test_router.py`<br/>`tests/test_chat_client.py`<br/>`tests/test_reliability.py`<br/>`tests/test_session_loopback.py` | 21 tests | ✅ PASS |
| **Step 6** | **Reliable File Transfer & Congestion Control:** Thỏa thuận qua TCP, cắt chunk `1100B`, relay qua RAM trên Server, ghi `.part`, kiểm tra `SHA-256`, đổi tên nguyên tử, điều khiển tắc nghẽn **CUBIC**. | `tests/test_file_relay.py`<br/>`tests/test_file_client.py`<br/>`tests/test_reliability.py` | 28 tests | ✅ PASS |
| **Step 7** | **Network Resilience & Error Handling:** Xử lý `TimeoutError`, `BrokenPipeError`/`ChannelClosed`, thoát đột ngột, hủy file mồ côi (`abort_for`), dọn dẹp lũy đẳng (`_cleanup`, `_on_disconnect`). | `tests/test_file_relay.py`<br/>`tests/test_file_client.py`<br/>`tests/test_full_system_suite.py` | 8 tests | ✅ PASS |
| **Step 8** | **Security Layer:** Trao khóa `ECDH X25519`, dẫn xuất `HKDF-SHA256`, mã hóa/giải mã `AES-256-GCM` trên TCP & UDP (khóa 2 chiều `c2s`/`s2c`, `nonce = IV XOR pn`, Header làm `AAD`). | `tests/test_packet.py`<br/>`tests/test_full_system_suite.py` | 6 tests | ✅ PASS |
| **Step 9** | **HTTP-based UI with Client-side JS:** Tự phân tích cú pháp HTTP/1.1 (`read_request`) trên raw TCP stream, xử lý phân mảnh TCP, bắt lỗi `400`/`413`, phục vụ API & static files. | `tests/test_mock_and_http.py`<br/>`tests/test_full_system_suite.py` | 8 tests | ✅ PASS |
| **Step 10** | **Testing & Code Quality:** Kiểm thử tích hợp toàn hệ thống (End-to-End) qua mạng mô phỏng mất gói, trễ, đảo thứ tự và nhiều tiến trình client. | `tests/test_full_system_suite.py`<br/>`tests/test_session_loopback.py`<br/>`tests/spawn_clients.py` | Toàn bộ Suite | ✅ PASS |

---

## 2. Chi tiết từng nhóm Test Case theo yêu cầu Đề bài

### Nhóm 1: Giao thức & Đóng khung (Step 1)
| Mã TC | Tên Test Case | Đầu vào (Input) | Kết quả kỳ vọng (Expected Output) | Ý nghĩa bảo vệ đồ án |
|---|---|---|---|---|
| **TC-1.1** | `test_tcp_framing_roundtrip_and_limit` | Chuỗi byte JSON hợp lệ & frame khai báo kích thước `> 1 MB` (`MAX_FRAME_SIZE`). | Frame hợp lệ đọc đúng 100%; frame `> 1 MB` bị chặn ngay bằng `FrameError` trước khi cấp phát RAM. | Chống dính gói TCP (sticky packets) và chống tấn công làm tràn bộ nhớ (OOM). |
| **TC-1.2** | `test_udp_packet_header_21_bytes` | Gói `Packet` với `session_id`, `packet_number`, `stream_id=101`, `offset=1100`, `Flags.FIN`. | Chuỗi nhị phân đúng 21 byte header (`0xC0DE`), `decode()` khôi phục chính xác mọi trường. | Xác nhận cấu trúc gói nhị phân tầng ứng dụng tự thiết kế. |
| **TC-1.3** | `test_udp_garbage_packets_rejected` | Các gói rỗng, ngắn hơn 21B, sai `magic`, sai `version`, hoặc độ dài payload lệch header. | Ném `PacketError` (không văng `struct.error` hay làm sập chương trình). | Đảm bảo đầu vào từ mạng không đáng tin luôn được lọc sạch. |
| **TC-1.4** | `test_ack_frame_sack_ranges` | `AckFrame` chứa nhiều dải SACK `[(6, 9), (0, 4)]` (khuyết gói 5). | Mã hóa/giải mã khớp tuyệt đối; `contains(5)` trả về `False`, `contains(7)` trả về `True`. | Nền tảng cho cơ chế chỉ gửi lại đúng gói bị mất (Selective Repeat). |

---

### Nhóm 2: Handshake TCP & Kiểm soát Phiên bản (Step 2)
| Mã TC | Tên Test Case | Đầu vào (Input) | Kết quả kỳ vọng (Expected Output) | Ý nghĩa bảo vệ đồ án |
|---|---|---|---|---|
| **TC-2.1** | `test_handshake_hello_ack_success` | Client kết nối TCP tới Server, gửi `HELLO {version: 1}`. | Server phản hồi `HELLO_ACK {version: 1, server_name: "collab-server"}`. | Xác nhận bước thiết lập kết nối cơ bản theo Rubric 2. |
| **TC-2.2** | `test_handshake_wrong_version_rejected` | Client gửi `HELLO {version: 99}`. | Server trả về `ERROR` và đóng kết nối sạch sẽ qua `_cleanup`. | Chặn client không tương thích phiên bản giao thức. |
| **TC-2.3** | `test_tcp_recv_timeout_vs_closed` | Kênh TCP im lặng quá `timeout` so với kênh TCP bị đóng (`EOF`). | Hết giờ ném `asyncio.TimeoutError`; đóng kết nối ném `ChannelClosed`. | Phân biệt rõ lỗi mạng chậm và sự kiện ngắt kết nối. |

---

### Nhóm 3: Xác thực, Quản lý Trạng thái & Đồng thời (Step 3 & Step 4)
| Mã TC | Tên Test Case | Đầu vào (Input) | Kết quả kỳ vọng (Expected Output) | Ý nghĩa bảo vệ đồ án |
|---|---|---|---|---|
| **TC-3.1** | `test_password_pbkdf2_and_salt` | Đăng ký `alice` với mật khẩu `"123456"`. | File `users.json` chỉ lưu `salt` và `hash` (không lưu `"123456"`), 2 user cùng mật khẩu có 2 `salt` khác nhau. | Đảm bảo an toàn mật khẩu, chống Rainbow Table. |
| **TC-3.2** | `test_session_manager_track_ip_port_and_duplicate` | Thêm `alice (127.0.0.1, udp:50001)`, sau đó thêm `alice` lần 2 khi chưa thoát. | Lần 1 lưu đúng IP, cổng UDP, `session_id`; lần 2 ném `ValueError("đã đăng nhập ở nơi khác")`. | Server theo dõi chính xác người dùng đang hoạt động và chặn đăng nhập chồng. |
| **TC-3.3** | `test_login_wrong_password_lockout_after_5_tries` | Client gửi `AUTH_REQ` sai mật khẩu 5 lần liên tiếp. | 4 lần đầu nhận `AUTH_FAIL` (được thử lại); lần thứ 5 bị coi là tấn công Brute-force và ngắt kết nối. | Chống dò mật khẩu tự động (Brute-force protection). |
| **TC-4.1** | `test_concurrent_clients_and_nonblocking_event_loop` | Trong lúc 1 luồng đang băm PBKDF2 nặng, các tác vụ `asyncio` khác vẫn chạy song song. | Tác vụ nhẹ hoàn thành trong vài ms mà không phải đợi PBKDF2 (~100ms) chạy xong. | Chứng minh `asyncio.to_thread` giải quyết triệt để nghẽn Event Loop ở Step 4. |

---

### Nhóm 4: Định tuyến Chat, Độ tin cậy UDP & Chống HoL Blocking (Step 5)
| Mã TC | Tên Test Case | Đầu vào (Input) | Kết quả kỳ vọng (Expected Output) | Ý nghĩa bảo vệ đồ án |
|---|---|---|---|---|
| **TC-5.1** | `test_chat_broadcast_and_unicast` | `alice` gửi tin chung `to="*"` và tin riêng `to="bob"` (trong khi `carol` cũng online). | Tin chung đến cả `alice, bob, carol` trên Stream 1; tin riêng chỉ đến `alice, bob` trên Stream 2 (`carol` không nhận được). | Đúng yêu cầu Broadcast & Unicast messaging. |
| **TC-5.2** | `test_parse_chat_strict_validation` | Payload thiếu trường, `to=None`, `text=""`, hoặc `text` dài quá `MAX_TEXT (4000)`. | Ném `ValueError`, tuyệt đối không ép kiểu `str(None)` thành tên người nhận `"None"`. | Chống dữ liệu rác từ mạng làm hỏng bộ định tuyến. |
| **TC-5.3** | `test_rtt_estimator_rfc6298` | Mẫu RTT đầu tiên `100ms`, mẫu tiếp theo `200ms`. | `srtt = 112.5ms`, `rttvar = 62.5ms`, `rto` được kẹp trong `[0.2s, 8.0s]`. | Tính toán chính xác thời gian chờ gửi lại (RTO). |
| **TC-5.4** | `test_sack_selective_retransmission_only_missing` | Gửi gói `0..7`, bên nhận báo ACK `[(5, 7), (0, 3)]` (mất gói 4). | `SendTracker.on_ack` chỉ đánh dấu **đúng gói số 4** là `lost` để gửi lại (`4 + 3 <= 7`). | Chứng minh chỉ gửi lại đúng gói mất, không gửi lại gói `5, 6, 7`. |
| **TC-5.5** | `test_stream_out_of_order_reassembly` | Các đoạn của message `"hello world"` đến theo thứ tự đảo ngược hoàn toàn + gói trùng. | `RecvStream` xếp lại đúng thứ tự theo `offset`, bỏ gói trùng, trả về đúng `b"hello world"`. | Đảm bảo thứ tự dữ liệu trên môi trường UDP không thứ tự. |
| **TC-5.6** | `test_no_head_of_line_blocking_between_file_and_chat` | Gói đầu (`offset=0`) của Stream 100 (File) bị mất, chỉ có gói `offset=1100` tới; sau đó gói `offset=0` của Stream 1 (Chat) tới. | `chat_rx.read_message()` trả về tin nhắn chat **ngay lập tức** (< 1ms) dù Stream File đang bị kẹt chờ gói đầu. | **Bằng chứng cốt lõi chống Head-of-Line (HoL) Blocking.** |

---

### Nhóm 5: Truyền File tin cậy qua UDP & Điều khiển tắc nghẽn CUBIC (Step 6)
| Mã TC | Tên Test Case | Đầu vào (Input) | Kết quả kỳ vọng (Expected Output) | Ý nghĩa bảo vệ đồ án |
|---|---|---|---|---|
| **TC-6.1** | `test_file_offer_accept_reject_security` | `alice` mời `bob`, `carol` cố tình gửi `FILE_ACCEPT` nhận thay `bob`; hoặc tên file là `"../../bi_mat.txt"`. | `carol` bị chặn bằng `ERROR`; tên file bị lọc thành `"bi_mat.txt"` (chống Path Traversal). | Đảm bảo an toàn phân quyền và đường dẫn khi truyền file. |
| **TC-6.2** | `test_server_streaming_relay_zero_disk` | `alice` đẩy các chunk qua Stream 101 lên Server. | Server chuyển tiếp ngay lập tức sang Stream 101 của `bob` trong RAM, không tạo file nào trên đĩa của Server. | Kiến trúc Cut-through Streaming Relay. |
| **TC-6.3** | `test_client_chunked_send_and_sha256_receive` | File 9.000 byte được gửi qua `FileService._send_file` và nhận bởi `_receive_file`. | Cắt đúng 9 chunk (`<= 1100B`), ghi vào `.part`, kiểm tra khớp `SHA-256`, đổi tên nguyên tử sang file đích, gửi `FILE_DONE(ok=True)`. | Đảm bảo toàn vẹn dữ liệu 100% khi truyền file lớn. |
| **TC-6.4** | `test_corrupted_file_deleted_on_hash_mismatch` | Dữ liệu nhận về có mã băm SHA-256 khác với lời mời `FILE_OFFER`. | Xóa ngay file tạm `.part`, không tạo file đích, đặt `status="failed"`, gửi `FILE_DONE(ok=False, reason="sai hash")`. | Không bao giờ để lọt file lỗi/hỏng tới tay người dùng. |
| **TC-6.5** | `test_cubic_congestion_control_full_lifecycle` | Chuỗi sự kiện: nhận ACK ở Slow Start $\rightarrow$ mất gói (`on_loss`) $\rightarrow$ tránh tắc nghẽn bậc 3 $\rightarrow$ `on_timeout`. | Slow start tăng nhanh; khi loss giảm còn `0.7 * w_max` (chỉ giảm 1 lần/RTT); sau mốc $K$ tăng vượt `w_max`; khi timeout về `MIN_CWND=2`. | Xác nhận thuật toán **CUBIC (RFC 8312)** hoạt động chuẩn xác. |

---

### Nhóm 6: Chịu lỗi mạng, Bảo mật & Giao diện HTTP (Step 7, 8, 9)
| Mã TC | Tên Test Case | Đầu vào (Input) | Kết quả kỳ vọng (Expected Output) | Ý nghĩa bảo vệ đồ án |
|---|---|---|---|---|
| **TC-7.1** | `test_resilience_abort_transfer_and_idempotent_cleanup` | Client đang truyền file bị ngắt kết nối đột ngột; gọi `_cleanup` và `_on_disconnect` 2 lần liên tiếp. | Hủy file đang truyền, báo `FILE_DONE(ok=False)` cho đối tác, chỉ phát đúng 1 bản tin `USER_LEAVE`, hủy sạch background tasks. | Server và Client không bao giờ sập hay rò rỉ tài nguyên khi rớt mạng. |
| **TC-8.1** | `test_security_ecdh_aes_gcm_c2s_s2c_and_aad` | Mã hóa gói UDP bằng khóa `c2s` và `s2c`; thử đọc trộm trên dây; thử sửa 1 bit ở Header (AAD) hoặc Ciphertext. | Trên dây không chứa plaintext; 2 chiều sinh bản mã khác nhau; sửa 1 bit ở Header hay Payload đều bị chặn bởi `CryptoError`. | Chứng minh bảo mật đầu-cuối trên đường truyền và chống giả mạo gói tin. |
| **TC-9.1** | `test_http11_parser_chunked_tcp_and_error_codes` | Gói HTTP/1.1 bị cắt nhỏ trên TCP, có URL query `%20`, tiếng Việt UTF-8; kèm các gói lỗi cú pháp và quá kích thước. | Đọc đúng `method`, `path`, `query`, `headers`, `json()`; trả mã `400 Bad Request` và `413 Payload Too Large` chính xác. | Tự hiện thực HTTP/1.1 server trên raw socket không dùng Flask/Django. |

---

## 3. Hướng dẫn chạy toàn bộ Test Suite

Đứng tại thư mục gốc `collab-app`, chạy lệnh:

```powershell
# 1. Chạy riêng file tổng hợp toàn bộ 10 bước Rubric:
python -m unittest tests.test_full_system_suite -v

# 2. Chạy toàn bộ tất cả các file test trong thư mục tests/:
python -m unittest discover -s tests -t . -v
```

