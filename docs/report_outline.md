# Dàn ý báo cáo kỹ thuật (xuất PDF)

1. **Giới thiệu**: mục tiêu, phạm vi, thành viên và phân công.
2. **System Architecture** (B): `diagrams/architecture.mmd`; giải thích vì sao tách TCP điều khiển và UDP dữ liệu; tách tầng UI, app, transport.
3. **Protocol Specification** (A): lấy từ `protocol_spec.md` (bảng message TCP, header UDP, ACK frame, mã hóa).
4. **Cơ chế tin cậy và congestion control** (A): SACK, RTO, chống HoL, CUBIC; biểu đồ cwnd từ CSV.
5. **Sequence diagrams** (B): vòng đời tin nhắn, vòng đời truyền file, state diagram.
6. **Kiểm thử** (cả hai): bảng 10 bước rubric kèm ảnh chụp: 20 client, mất gói 10%, file 50 MB khớp hash, Wireshark.
7. **Challenges & Solutions** (A, ~1 trang): từ `learning_log.md`.
8. **Giới hạn và hướng phát triển**.
