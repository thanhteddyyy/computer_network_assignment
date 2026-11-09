"""Mini HTTP/1.1 server TỰ VIẾT trên socket TCP – cầu nối giữa trình duyệt và CollabClient (bước 9).

Vì sao cần? JavaScript trong trình duyệt không mở được raw socket TCP/UDP, nên trình duyệt chỉ nói
chuyện với tiến trình client này qua HTTP. Đề cấm framework (Flask/Django) và WebSocket, nên:
  - tự parse request bằng tay (read_request)
  - trình duyệt lấy tin mới bằng polling GET /api/events?since=<id>
Chỉ lắng nghe 127.0.0.1 -> chỉ trình duyệt trên cùng máy truy cập được.

Ghi chú: asyncio.start_server chỉ là lớp bọc socket TCP của thư viện chuẩn (không hiểu HTTP).
Nếu giảng viên yêu cầu nghiêm hơn, có thể thay bằng socket.socket + accept() trong thread.

Trạng thái: định tuyến, phản hồi, file tĩnh, các API HOÀN THIỆN; read_request là TODO cho B (ngày 2).
"""
import asyncio
import json
import mimetypes
import os
import time
from dataclasses import dataclass, field
from urllib.parse import parse_qs, unquote, urlsplit

from common.config import STATIC_DIR
from common.log import get_logger
from transport.tcp_channel import ChannelClosed

log = get_logger("http")

MAX_HEADER_BYTES = 16 * 1024
MAX_BODY_BYTES = 512 * 1024 * 1024    # file upload tối đa 512 MB
OUTGOING_DIR = "data/outgoing"
REASONS = {200: "OK", 204: "No Content", 400: "Bad Request", 404: "Not Found",
           405: "Method Not Allowed", 409: "Conflict", 413: "Payload Too Large", 500: "Internal Server Error"}


class HttpError(Exception):
    def __init__(self, status: int, message: str = "") -> None:
        super().__init__(message)
        self.status, self.message = status, message


@dataclass
class Request:
    method: str
    path: str
    query: dict[str, str]
    headers: dict[str, str]          # tên header viết thường
    body: bytes = b""
    params: dict = field(default_factory=dict)

    def json(self) -> dict:
        try:
            return json.loads(self.body or b"{}")
        except ValueError as exc:
            raise HttpError(400, "body không phải JSON") from exc


# ===================================================================== parse request
async def read_request(reader: asyncio.StreamReader) -> Request | None:
    """Đọc và phân tích MỘT HTTP/1.1 request từ trình duyệt."""
    # 1. Đọc phần header cho tới dấu phân cách \r\n\r\n
    try:
        head = await reader.readuntil(b"\r\n\r\n")                                 # (1)
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            return None                                                            # (2) Trình duyệt đóng kết nối sạch
        raise HttpError(400, "request header bị cắt cụt") from exc
    except asyncio.LimitOverrunError as exc:
        raise HttpError(413, "header quá lớn") from exc

    if len(head) > MAX_HEADER_BYTES:                                               # (3)
        raise HttpError(413, "header vượt quá MAX_HEADER_BYTES")

    # 2. Tách dòng đầu (Request Line) và các dòng Header
    text = head[:-4].decode("latin-1")                                             # (4) Bỏ 4 byte \r\n\r\n ở cuối
    lines = text.split("\r\n")
    try:
        method, target, version = lines[0].split(" ", 2)                           # (5)
    except ValueError as exc:
        raise HttpError(400, "request line không hợp lệ") from exc
    if not version.startswith("HTTP/1."):
        raise HttpError(400, f"không hỗ trợ {version}")

    # 3. Phân tích các dòng Header: "Name: value"
    headers: dict[str, str] = {}
    for line in lines[1:]:                                                         # (6)
        if not line:
            continue
        if ":" not in line:
            raise HttpError(400, f"header thiếu dấu hai chấm: {line}")
        name, value = line.split(":", 1)
        headers[name.strip().lower()] = value.strip()

    # 4. Tách URL path và query string
    url = urlsplit(target)                                                         # (7)
    path = unquote(url.path)
    query = {k: v[0] for k, v in parse_qs(url.query, keep_blank_values=True).items()}

    # 5. Đọc phần Body theo Content-Length (nếu có)
    try:
        content_len = int(headers.get("content-length", "0"))                      # (8)
    except ValueError as exc:
        raise HttpError(400, "Content-Length không hợp lệ") from exc
    if content_len < 0:
        raise HttpError(400, "Content-Length âm")
    if content_len > MAX_BODY_BYTES:
        raise HttpError(413, "body vượt quá MAX_BODY_BYTES")

    try:
        body = await reader.readexactly(content_len) if content_len > 0 else b""   # (9)
    except asyncio.IncompleteReadError as exc:
        raise HttpError(400, "body bị ngắt giữa chừng") from exc

    return Request(method.upper(), path, query, headers, body)                     # (10)v


# ===================================================================== tạo response
def build_response(status: int, body: bytes = b"", content_type: str = "text/plain; charset=utf-8",
                   extra: dict | None = None) -> bytes:
    headers = {
        "Content-Type": content_type,
        "Content-Length": str(len(body)),
        "Connection": "close",
        "Cache-Control": "no-store",
        **(extra or {}),
    }
    head = f"HTTP/1.1 {status} {REASONS.get(status, 'Unknown')}\r\n"
    head += "".join(f"{k}: {v}\r\n" for k, v in headers.items()) + "\r\n"
    return head.encode("latin-1") + body


def json_response(obj, status: int = 200) -> bytes:
    return build_response(status, json.dumps(obj, ensure_ascii=False).encode(), "application/json; charset=utf-8")


# ===================================================================== server
class HttpServer:
    def __init__(self, client, host: str, port: int, default_server: str | None = None, tcp_port: int = 9000) -> None:
        self.client = client
        self.host, self.port = host, port
        self.default_server, self.tcp_port = default_server, tcp_port
        self.routes = {
            ("GET", "/api/state"): self.api_state,
            ("GET", "/api/events"): self.api_events,
            ("POST", "/api/connect"): self.api_connect,
            ("POST", "/api/register"): self.api_register,
            ("POST", "/api/login"): self.api_login,
            ("POST", "/api/logout"): self.api_logout,
            ("POST", "/api/send"): self.api_send,
            ("POST", "/api/upload"): self.api_upload,
            ("POST", "/api/files/accept"): self.api_file_accept,
            ("POST", "/api/files/reject"): self.api_file_reject,
            ("GET", "/api/files/download"): self.api_file_download,
        }

    async def start(self) -> None:
        await asyncio.start_server(self._handle, self.host, self.port, limit=MAX_HEADER_BYTES * 4)
        log.info("HTTP UI tại http://%s:%s", self.host, self.port)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        started = time.monotonic()
        req = None
        try:
            req = await read_request(reader)
            if req is None:
                return
            response = await self._dispatch(req)
        except HttpError as exc:
            response = json_response({"error": exc.message or REASONS.get(exc.status)}, exc.status)
        except NotImplementedError as exc:
            log.error("chưa cài đặt: %s", exc)
            response = json_response({"error": f"chưa cài đặt: {exc}"}, 500)
        except Exception as exc:
            log.exception("lỗi xử lý HTTP")
            response = json_response({"error": str(exc)}, 500)
        try:
            writer.write(response)
            await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()
            if req and req.path != "/api/events":
                log.debug("%s %s (%.0f ms)", req.method, req.path, (time.monotonic() - started) * 1000)

    async def _dispatch(self, req: Request) -> bytes:
        handler = self.routes.get((req.method, req.path))
        if handler:
            return await handler(req)
        if req.method == "GET":
            return self.serve_static(req.path)
        raise HttpError(404, "không có route này")

    # ---------------------------------------------------------------- file tĩnh
    def serve_static(self, path: str) -> bytes:
        rel = "index.html" if path in ("/", "") else path.lstrip("/")
        root = os.path.abspath(STATIC_DIR)
        full = os.path.abspath(os.path.join(root, rel))
        if not full.startswith(root + os.sep) or not os.path.isfile(full):   # chặn ../../etc/passwd
            raise HttpError(404, "không tìm thấy")
        ctype = mimetypes.guess_type(full)[0] or "application/octet-stream"
        if ctype.startswith("text/") or ctype in ("application/javascript", "text/javascript"):
            ctype += "; charset=utf-8"
        with open(full, "rb") as f:
            return build_response(200, f.read(), ctype)

    # ---------------------------------------------------------------- API
    async def api_state(self, req: Request) -> bytes:
        snap = self.client.snapshot()
        snap["default_server"] = self.default_server
        return json_response(snap)

    async def api_events(self, req: Request) -> bytes:
        since = int(req.query.get("since", 0))
        return json_response({"events": self.client.events.since(since), "state": self.client.snapshot()})

    async def api_connect(self, req: Request) -> bytes:
        data = req.json()
        host = data.get("server") or self.default_server or None
        try:
            await self.client.connect(host, self.tcp_port)
        except (OSError, asyncio.TimeoutError, ChannelClosed) as exc:
            await self.client._on_disconnect(f"kết nối thất bại: {exc}")
            raise HttpError(409, f"không kết nối được server: {exc}") from exc
        return json_response({"ok": True, "state": self.client.state})

    async def api_register(self, req: Request) -> bytes:
        d = req.json()
        ok, reason = await self.client.register(d.get("username", ""), d.get("password", ""))
        return json_response({"ok": ok, "reason": reason}, 200 if ok else 409)

    async def api_login(self, req: Request) -> bytes:
        d = req.json()
        ok, reason = await self.client.login(d.get("username", ""), d.get("password", ""))
        return json_response({"ok": ok, "reason": reason}, 200 if ok else 409)

    async def api_logout(self, req: Request) -> bytes:
        await self.client.logout()
        return json_response({"ok": True})

    async def api_send(self, req: Request) -> bytes:
        d = req.json()
        await self.client.chat.send(d.get("to", "*"), d.get("text", ""))
        return json_response({"ok": True})

    async def api_upload(self, req: Request) -> bytes:
        """Trình duyệt gửi byte thô của file trong body: POST /api/upload?to=bob&name=a.pdf"""
        to, name = req.query.get("to"), os.path.basename(req.query.get("name", ""))
        if not to or not name:
            raise HttpError(400, "thiếu to hoặc name")
        os.makedirs(OUTGOING_DIR, exist_ok=True)
        path = os.path.join(OUTGOING_DIR, f"{int(time.time() * 1000)}_{name}")
        await asyncio.to_thread(_write_file, path, req.body)
        ref = await self.client.files.offer(to, path, name)
        return json_response({"ok": True, "client_ref": ref})

    async def api_file_accept(self, req: Request) -> bytes:
        await self.client.files.accept(int(req.json()["file_id"]))
        return json_response({"ok": True})

    async def api_file_reject(self, req: Request) -> bytes:
        await self.client.files.reject(int(req.json()["file_id"]))
        return json_response({"ok": True})

    async def api_file_download(self, req: Request) -> bytes:
        t = self.client.files.by_id.get(int(req.query.get("id", -1)))
        if not t or t.direction != "in" or t.status != "done":
            raise HttpError(404, "file chưa sẵn sàng")
        data = await asyncio.to_thread(_read_file, t.path)
        return build_response(200, data, "application/octet-stream",
                              {"Content-Disposition": f'attachment; filename="{_ascii(t.name)}"'})


def _write_file(path: str, data: bytes) -> None:
    with open(path, "wb") as f:
        f.write(data)


def _read_file(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def _ascii(name: str) -> str:
    return name.encode("ascii", "replace").decode().replace('"', "_")
