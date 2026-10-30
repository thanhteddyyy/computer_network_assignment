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
        raise NotImplementedError("TODO[B-D2] FileService.offer")

    def on_offer_ack(self, msg: dict) -> None:
        """TODO[B-D2]: tìm transfer theo client_ref; có "error" -> status failed; không thì gán file_id,
        thêm vào self.by_id."""
        raise NotImplementedError("TODO[B-D2] FileService.on_offer_ack")

    def on_accept(self, msg: dict) -> None:
        """Người nhận đồng ý -> bắt đầu gửi (chạy nền)."""
        t = self.by_id.get(msg["file_id"])
        if t and t.direction == "out":
            t.status = "sending"
            asyncio.create_task(self._send_file(t))

    async def _send_file(self, t: Transfer) -> None:
        """TODO[B-D2]:
          - stream = self.client.udp.stream(FILE_STREAM_BASE + t.file_id)
          - Mở file 'rb', đọc từng CHUNK_SIZE byte, await stream.send(chunk)   # send() tự chờ khi hàng đợi đầy
          - t.done_bytes += len(chunk); self._progress(t)
          - Xong: status vẫn "sending" cho tới khi nhận FILE_DONE từ người nhận.
          - Bắt lỗi (SessionClosed, OSError) -> status failed, error, self._progress(t, force=True)
        """
        raise NotImplementedError("TODO[B-D2] FileService._send_file")

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
        raise NotImplementedError("TODO[B-D2] FileService.accept")

    async def reject(self, file_id: int) -> None:
        t = self.by_id.get(file_id)
        if t and t.direction == "in":
            t.status = "rejected"
            await self.client.ch.send(make(T.FILE_REJECT, file_id=file_id, reason="người nhận từ chối"))
            self._progress(t, force=True)

    async def _receive_file(self, t: Transfer) -> None:
        """TODO[B-D2]:
          - os.makedirs(DOWNLOAD_DIR, exist_ok=True); ghi vào t.path + ".part"
          - stream = self.client.udp.stream(FILE_STREAM_BASE + t.file_id)
          - while t.done_bytes < t.size: chunk = await stream.read_message(); f.write(chunk); cập nhật tiến độ
          - Kiểm tra sha256_file(.part) == t.sha256 (chạy bằng asyncio.to_thread)
              đúng -> os.replace(.part, t.path); status done; gửi FILE_DONE {file_id, ok: True}
              sai  -> xóa .part; status failed; gửi FILE_DONE {file_id, ok: False, reason: "sai hash"}
          - self._progress(t, force=True)
        """
        raise NotImplementedError("TODO[B-D2] FileService._receive_file")

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
