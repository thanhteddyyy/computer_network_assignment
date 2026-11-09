"""Gửi/nhận file phía client (bước 6). Xem luồng đầy đủ ở server/file_relay.py.

Trạng thái: khung + TODO cho người B (ngày 2).
"""
import asyncio
import itertools
import os
from dataclasses import asdict, dataclass

from common.config import CHUNK_SIZE, DOWNLOAD_DIR, FILE_STREAM_BASE
from common.crypto import sha256_file
from common.log import get_logger
from common.messages import T, make
from transport.session import SessionClosed
log = get_logger("files")


@dataclass
class Transfer:
    file_id: int | None
    direction: str          # "out" | "in"
    peer: str               # người nhận (out) hoặc người gửi (in)
    name: str
    size: int
    sha256: str
    path: str               # file nguồn (out) hoặc file đích (in)
    done_bytes: int = 0
    status: str = "offered" # offered -> accepted -> sending/receiving -> done | rejected | failed
    client_ref: str = ""
    error: str = ""

    def public(self) -> dict:
        d = asdict(self)
        d["progress"] = round(self.done_bytes / self.size, 4) if self.size else 1.0
        return d


class FileService:
    def __init__(self, client) -> None:
        self.client = client
        self.by_ref: dict[str, Transfer] = {}
        self.by_id: dict[int, Transfer] = {}
        self._refs = itertools.count(1)
        self._last_reported: dict[int, float] = {}

    def list(self) -> list[dict]:
        seen = {id(t): t for t in [*self.by_ref.values(), *self.by_id.values()]}
        return [t.public() for t in seen.values()]

    def _progress(self, t: Transfer, force: bool = False) -> None:
        """Phát sự kiện tiến độ, tối đa mỗi 2% để không làm ngập event log."""
        p = t.done_bytes / t.size if t.size else 1.0
        key = t.file_id or 0
        if force or p - self._last_reported.get(key, -1) >= 0.02:
            self._last_reported[key] = p
            self.client.events.add("file_progress", transfer=t.public())

    # ============================================================ phía GỬI
    async def offer(self, to: str, path: str, name: str | None = None) -> str:
        """TODO[B-D2]:
          - size = os.path.getsize(path); digest = await asyncio.to_thread(sha256_file, path)
            (tính hash trong thread để không chặn event loop với file lớn)
          - ref = f"r{next(self._refs)}"; tạo Transfer(direction="out", client_ref=ref, file_id=None, ...)
          - self.by_ref[ref] = t
          - await self.client.ch.send(make(T.FILE_OFFER, client_ref=ref, to=to, name=..., size=..., sha256=...))
          - phát sự kiện file_progress; return ref
        """
        if not os.path.isfile(path):                                             # (1)
            self.client.events.add("error", reason=f"không tìm thấy file: {path}")
            return ""
        size = os.path.getsize(path)                                             # (2)
        digest = await asyncio.to_thread(sha256_file, path)

        ref = f"r{next(self._refs)}"                                             # (3)
        t = Transfer(file_id=None, direction="out", peer=to, name=name or os.path.basename(path),
                     size=size, sha256=digest, path=path, client_ref=ref)
        self.by_ref[ref] = t

        await self.client.ch.send(make(T.FILE_OFFER, client_ref=ref, to=to,      # (4)
                                       name=t.name, size=size, sha256=digest))
        self._progress(t, force=True)
        return ref

    def on_offer_ack(self, msg: dict) -> None:
        """TODO[B-D2]: tìm transfer theo client_ref; có "error" -> status failed; không thì gán file_id,
        thêm vào self.by_id."""
        t = self.by_ref.get(msg.get("client_ref"))
        if t is None:
            return
        if msg.get("error"):
            t.status, t.error = "failed", msg["error"]
            self.client.events.add("error", reason=f"gửi file {t.name} thất bại: {t.error}")
        else:
            t.file_id = msg["file_id"]
            self.by_id[t.file_id] = t
        self._progress(t, force=True)

    def on_accept(self, msg: dict) -> None:
        """Người nhận đồng ý -> bắt đầu gửi (chạy nền)."""
        t = self.by_id.get(msg["file_id"])
        if t and t.direction == "out":
            t.status = "sending"
            self._progress(t, force = True)
            asyncio.create_task(self._send_file(t))

    async def _send_file(self, t: Transfer) -> None:
        """Đọc file từng khúc CHUNK_SIZE byte và đẩy lên stream UDP 100 + file_id."""
        stream = self.client.udp.stream(FILE_STREAM_BASE + t.file_id)
        try:
            with open(t.path, "rb") as f:
                while chunk := f.read(CHUNK_SIZE):
                    await stream.send(chunk)
                    t.done_bytes += len(chunk)
                    self._progress(t)
            self._progress(t, force=True)
            log.info("file #%d: đã gửi hết %d byte, chờ xác nhận FILE_DONE", t.file_id, t.done_bytes)
        except (SessionClosed, OSError) as exc:
            log.error("file #%d: lỗi khi gửi file: %s", t.file_id, exc)
            t.status, t.error = "failed", str(exc)
            self._progress(t, force=True)
            self.client.events.add("error", reason=f"gửi file {t.name} thất bại: {exc}")
    # ============================================================ phía NHẬN
    def on_offer(self, msg: dict) -> None:
        """Có người muốn gửi file cho mình -> lưu lại và báo UI (UI hiện nút Nhận/Từ chối)."""
        safe_name = os.path.basename(msg["name"]) or f"file_{msg['file_id']}"   # chống path traversal
        path = os.path.join(DOWNLOAD_DIR, f"{msg['file_id']}_{safe_name}")
        t = Transfer(msg["file_id"], "in", msg["from"], safe_name, int(msg["size"]), msg["sha256"], path)
        self.by_id[t.file_id] = t
        self.client.events.add("file_incoming", transfer=t.public())

    async def accept(self, file_id: int) -> None:
        """TODO[B-D2]: status = "receiving"; tạo task _receive_file(t) TRƯỚC, rồi gửi FILE_ACCEPT."""
        t = self.by_id.get(file_id)
        if t is None or t.direction != "in" or t.status != "offered":
            self.client.events.add("error", reason=f"không có lời mời file #{file_id}")
            return
        t.status = "receiving"
        asyncio.create_task(self._receive_file(t))          # sẵn sàng nhận TRƯỚC khi báo đồng ý
        await self.client.ch.send(make(T.FILE_ACCEPT, file_id=file_id))
        self._progress(t, force=True)

    async def reject(self, file_id: int) -> None:
        t = self.by_id.get(file_id)
        if t and t.direction == "in":
            t.status = "rejected"
            await self.client.ch.send(make(T.FILE_REJECT, file_id=file_id, reason="người nhận từ chối"))
            self._progress(t, force=True)

    async def _receive_file(self, t: Transfer) -> None:
        """Nhận các chunk từ stream UDP vào file .part, kiểm tra SHA-256 rồi đổi tên thành file hoàn chỉnh."""
        os.makedirs(os.path.dirname(t.path) or DOWNLOAD_DIR, exist_ok=True)
        part_path = t.path + ".part"
        stream = self.client.udp.stream(FILE_STREAM_BASE + t.file_id)

        try:
            with open(part_path, "wb") as f:
                while t.done_bytes < t.size:
                    chunk = await stream.read_message()
                    if not chunk:
                        break
                    f.write(chunk)
                    t.done_bytes += len(chunk)
                    self._progress(t)

            # Ra khỏi khối 'with' để đóng file hoàn toàn trước khi băm và đổi tên/xóa (bắt buộc trên Windows)
            digest = await asyncio.to_thread(sha256_file, part_path)
            if digest == t.sha256:
                os.replace(part_path, t.path)
                t.status = "done"
                await self.client.ch.send(make(T.FILE_DONE, file_id=t.file_id, ok=True))
                log.info("file #%d: nhận thành công -> %s", t.file_id, t.path)
            else:
                os.remove(part_path)
                t.status, t.error = "failed", "sai hash"
                await self.client.ch.send(make(T.FILE_DONE, file_id=t.file_id, ok=False, reason="sai hash"))
                log.warning("file #%d: sai SHA-256, đã xóa file tạm", t.file_id)
        except (SessionClosed, OSError) as exc:
            log.error("file #%d: lỗi khi nhận file: %s", t.file_id, exc)
            if os.path.exists(part_path):
                os.remove(part_path)
            t.status, t.error = "failed", str(exc)
            await self.client.ch.send(make(T.FILE_DONE, file_id=t.file_id, ok=False, reason=str(exc)))

        self._progress(t, force=True)
        self.client.events.add("file_done", transfer=t.public())
    def on_reject(self, msg: dict) -> None:
        t = self.by_id.get(msg["file_id"])
        if t:
            t.status, t.error = "rejected", msg.get("reason", "")
            self._progress(t, force=True)

    def on_done(self, msg: dict) -> None:
        t = self.by_id.get(msg["file_id"])
        if t:
            t.status = "done" if msg.get("ok") else "failed"
            t.error = msg.get("reason", "")
            self.client.events.add("file_done", transfer=t.public())
