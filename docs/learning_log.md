# CHALLENGES & SOLUTIONS — PHÂN TÍCH THÁCH THỨC KỸ THUẬT & GIẢI PHÁP (1-PAGE REFLECTION)

> Nội dung dưới đây được trình bày theo đúng khuôn mẫu **1-Page Technical Reflection** trong mục **10.2 (Required Submission Content)** của đề bài. Bạn có thể chèn trực tiếp phần này làm chương **Challenges & Solutions** trong Báo cáo PDF.

---

## 1. Xử lý Phân mảnh Gói tin IP và Dính gói TCP (Packet Fragmentation & TCP Sticky Frames)

- **Thách thức (Challenge):**
  Trong quá trình truyền dữ liệu qua mạng, nhóm đối mặt với hai vấn đề trái ngược ở hai giao thức tầng giao vận:
  1. **Trên kênh TCP:** TCP là giao thức dòng byte (`byte-stream`) không bảo toàn ranh giới bản tin. Khi Server gửi liên tiếp `AUTH_OK` và `USER_LIST`, phía Client gọi `recv()` một lần có thể đọc dính cả hai bản tin JSON vào cùng một buffer (gây lỗi `JSONDecodeError`), hoặc một bản tin lớn bị chia thành nhiều mảnh nhỏ.
  2. **Trên kênh UDP:** Nếu gửi một khối dữ liệu file lớn (ví dụ `64 KB`) trong một datagram UDP duy nhất, tầng IP sẽ phải cắt gói tin thành hàng chục mảnh IP nhỏ (IP Fragmentation). Chỉ cần 1 mảnh IP bị rớt trên đường truyền, toàn bộ datagram `64 KB` sẽ bị hệ điều hành vứt bỏ hoàn toàn, làm suy giảm nghiêm trọng hiệu năng.
- **Giải pháp (Solution):**
  - **Với TCP:** Thiết kế cơ chế đóng khung tiền tố độ dài **Length-Prefixed Framing 4-byte Big-Endian** (`common/framing.py`) kết hợp hàm `reader.readexactly(n)` để đọc chính xác từng bản tin, đồng thời chốt chặn `MAX_FRAME_SIZE = 1 MB` nhằm chống tràn bộ nhớ.
  - **Với UDP:** Giới hạn kích thước datagram tối đa dưới ngưỡng MTU an toàn (`MAX_DATAGRAM = 1200 byte`). Dữ liệu file được cắt thành từng khúc `CHUNK_SIZE = 1100 byte`. Khi cộng thêm `21 byte Header` và `16 byte AES-GCM Tag`, tổng kích thước gói UDP trên dây là `1137 byte <= 1200 byte`, loại bỏ hoàn toàn hiện tượng phân mảnh IP.

---

## 2. Giải quyết Nghẽn Event Loop & Deadlock khi Xử lý Đồng thời (Event Loop Blocking & Deadlocks)

- **Thách thức (Challenge):**
  Hệ thống sử dụng mô hình bất đồng bộ đơn luồng `asyncio` để phục vụ đồng thời 20+ Clients. Tuy nhiên, ở thử nghiệm stress test ban đầu, khi 20 Clients kết nối và đăng nhập cùng lúc, các kết nối sau bị văng lỗi `TimeoutError` ở bước chờ `HELLO_ACK` (độ trễ vọt lên **2.256 ms**). Ngoài ra, khi nhiều coroutine cùng gọi `ControlChannel.send()` trên một kết nối TCP hoặc khi `FileService` vừa gửi xong file lớn vừa nhận tín hiệu đóng kết nối, hệ thống có nguy cơ xảy ra **Race Condition / Deadlock** làm xen kẽ byte của hai frame hoặc treo hàng đợi gửi.
- **Giải pháp (Solution):**
  1. **Offload tác vụ CPU-bound & Disk I/O sang Worker Thread:** Nguyên nhân làm đơ Event Loop là hàm băm mật khẩu `PBKDF2-HMAC-SHA256` (`200.000` vòng lặp, mất ~110ms CPU mỗi lần) và hàm băm file `sha256_file` (cho file 50 MB). Nhóm đã chuyển toàn bộ các lời gọi này sang `await asyncio.to_thread(...)`, giúp Event Loop luôn rảnh để xử lý mạng. Kết quả: thời gian phản hồi `HELLO_ACK` khi 20 Clients vào cùng lúc giảm từ **2.256 ms xuống chỉ còn 22 ms** (nhanh hơn 100 lần).
  2. **Đồng bộ hóa và Chống Deadlock:** Sử dụng `asyncio.Lock()` (`self._send_lock`) trong `ControlChannel.send()` để tuần tự hóa việc ghi frame TCP. Ở tầng UDP (`UdpSession`), khi hàng đợi `MAX_QUEUE = 512` đầy, hàm `_enqueue` chờ trên sự kiện `self._queue_space.wait()` (tạo **Backpressure** tự nhiên). Khi phiên đóng (`_close_local`), hệ thống chủ động gọi `self._queue_space.set()` để đánh thức mọi coroutine đang chờ, ngăn chặn hoàn toàn tình trạng Deadlock treo vĩnh viễn.

---

## 3. Triệt tiêu Nghẽn Đầu Hàng (Eliminating Head-of-Line Blocking)

- **Thách thức (Challenge):**
  Khi người dùng Alice vừa truyền một file lớn (50 MB — hơn 45.000 gói UDP) cho Bob trong môi trường mạng mất gói 10%, vừa gửi tin nhắn chat cho Bob: nếu dùng chung một chuỗi số thứ tự duy nhất như TCP, mỗi khi một gói của file bị rớt và chờ gửi lại, toàn bộ các tin nhắn chat đến sau đều bị chặn đứng trong bộ đệm nhận.
- **Giải pháp (Solution):**
  Áp dụng kiến trúc **Stream Multiplexing** giống Google QUIC: tách trường `packet_number` (cấp phiên kết nối) khỏi cặp `(stream_id, offset)` (cấp luồng dữ liệu). Mỗi luồng (`Stream 1` cho chat chung, `Stream 2` cho chat riêng, `Stream 100 + file_id` cho từng file) sở hữu một bộ ghép mảnh `RecvStream` độc lập. Khi gói của `Stream 101` (File) bị mất, chỉ có `RecvStream(101)` chờ gói bù; trong khi đó gói tin của `Stream 1` (Chat) đến nơi có `offset` hợp lệ của riêng nó nên được đẩy thẳng lên giao diện người dùng ngay lập tức.

---

## 4. Nhập nhằng RTT và Lặp Nonce AES-GCM khi Gửi lại Gói mất (RTT Ambiguity & Nonce Reuse)

- **Thách thức (Challenge):**
  Trong giao thức UDP tự thiết kế có mã hóa **AES-256-GCM**, `nonce` được tính bằng `IV XOR packet_number`. Nếu khi mất gói mà bên gửi phát lại gói tin với cùng `packet_number` cũ:
  1. Khi nhận được `ACK`, bên gửi không thể phân biệt `ACK` đó trả lời cho lần gửi đầu hay lần gửi lại (bài toán *Karn's Algorithm / RTT Ambiguity*), làm sai lệch công thức `RttEstimator`.
  2. Nguy hiểm hơn, nếu bất kỳ bit nào ở Header thay đổi nhưng dùng lại `packet_number` cũ, cặp `(Key, Nonce)` của AES-GCM bị tái sử dụng, làm lộ khóa xác thực mật mã.
- **Giải pháp (Solution):**
  Thiết kế `packet_number` **tăng đơn điệu tuyệt đối và không bao giờ tái sử dụng**. Khi `SendTracker` phát hiện một đoạn dữ liệu (`_Segment`) bị mất (qua **Selective ACK** với ngưỡng `PACKET_THRESHOLD = 3` hoặc quá hạn `RTO`), đoạn dữ liệu đó được giữ nguyên `(stream_id, offset)` nhưng khi đóng gói gửi lại sẽ được cấp một **`packet_number` mới hoàn toàn**. Kết hợp với việc tách khóa hai chiều độc lập (`c2s` và `s2c` sinh từ `HKDF-SHA256`), hệ thống đảm bảo không bao giờ trùng lặp Nonce và đo `SampleRTT` chính xác tuyệt đối.

---

## 5. Chịu lỗi Rớt mạng Đột ngột và Tính Toàn vẹn File Đa nền tảng (Abrupt Disconnects & Atomic File I/O)

- **Thách thức (Challenge):**
  Khi một Client đang truyền file bị tắt đột ngột (`kill -9`) hoặc rớt mạng, phía đối tác rất dễ bị treo ở trạng thái chờ vô tận, đồng thời trên ổ đĩa của người nhận còn sót lại file tải dở bị hỏng. Thêm vào đó, trên hệ điều hành Windows, nếu gọi đổi tên (`os.replace`) hoặc xóa (`os.remove`) khi file vẫn đang mở trong khối `with open(...)`, hệ điều hành sẽ khóa file và ném lỗi `PermissionError [WinError 32]`.
- **Giải pháp (Solution):**
  - **Phía Client nhận:** Ghi toàn bộ các mảnh nhận được vào file tạm có hậu tố `.part`, đóng hẳn file handle (thoát khỏi khối `with open`) trước khi kiểm tra `sha256_file` trên luồng phụ. Chỉ khi mã băm khớp 100% mới thực hiện đổi tên nguyên tử `os.replace(part_path, t.path)`; nếu lệch hash hoặc đứt mạng thì lập tức xóa sạch file `.part`.
  - **Phía Server & Client khi断 mạng:** Thiết kế cơ chế thu hồi tài nguyên lũy đẳng (**Idempotent Cleanup**) trong `_cleanup()`, `_on_disconnect()` và `FileRelay.abort_for()`, tự động hủy mọi phiên truyền file mồ côi và gửi bản tin `FILE_DONE(ok=False)` báo ngay cho đối tác còn lại.
