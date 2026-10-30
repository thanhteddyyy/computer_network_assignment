"""Relay file qua server (bước 6). Điều khiển qua TCP, dữ liệu qua UDP stream 100 + file_id.

Trạng thái: khung + TODO cho người A (ngày 2 chiều, vì phần mã hóa UDP đã có sẵn).

Luồng đầy đủ:
  1. A --TCP FILE_OFFER {client_ref, to, name, size, sha256}--> Server
  2. Server cấp file_id, lưu Transfer; --FILE_OFFER_ACK {client_ref, file_id}--> A
                                        --FILE_OFFER {file_id, from, name, size, sha256}--> B
  3. B --FILE_ACCEPT {file_id}--> Server --FILE_ACCEPT--> A     (hoặc FILE_REJECT)
  4. A gửi các chunk trên stream 100+file_id của phiên UDP A<->Server
     Server đọc từng chunk, chuyển ngay sang stream 100+file_id của phiên Server<->B (không lưu đĩa)
  5. B ghép file, kiểm tra SHA-256 --FILE_DONE {file_id, ok}--> Server --FILE_DONE--> A
"""
import asyncio
import itertools
from dataclasses import dataclass

from common.config import FILE_STREAM_BASE
from common.log import get_logger
from common.messages import T, make
from server.session_manager import ClientSession, SessionManager

log = get_logger("file-relay")


@dataclass
class Transfer:
    file_id: int
    sender: str
    receiver: str
    name: str
    size: int
    sha256: str
    relayed: int = 0
    status: str = "offered"   # offered -> accepted -> relaying -> done | rejected | aborted

    @property
    def stream_id(self) -> int:
        return FILE_STREAM_BASE + self.file_id


class FileRelay:
    def __init__(self, sessions: SessionManager) -> None:
        self.sessions = sessions
        self.transfers: dict[int, Transfer] = {}
        self._ids = itertools.count(1)

    async def on_offer(self, sender: ClientSession, msg: dict) -> None:
        """TODO[A-D2]: bước 1-2 ở docstring đầu file. Người nhận không online -> FILE_OFFER_ACK kèm error."""
        raise NotImplementedError("TODO[A-D2] FileRelay.on_offer")

    async def on_accept(self, receiver: ClientSession, msg: dict) -> None:
        """TODO[A-D2]: kiểm tra transfer tồn tại và receiver đúng người; status = accepted; báo cho sender."""
        raise NotImplementedError("TODO[A-D2] FileRelay.on_accept")

    async def on_reject(self, receiver: ClientSession, msg: dict) -> None:
        """TODO[A-D2]: status = rejected; báo cho sender; xóa transfer."""
        raise NotImplementedError("TODO[A-D2] FileRelay.on_reject")

    async def on_done(self, receiver: ClientSession, msg: dict) -> None:
        """TODO[A-D2]: chuyển FILE_DONE cho sender; status = done/aborted; xóa transfer."""
        raise NotImplementedError("TODO[A-D2] FileRelay.on_done")

    def attach_stream(self, client: ClientSession, stream) -> None:
        """Gọi khi phiên UDP của client xuất hiện stream >= FILE_STREAM_BASE (dữ liệu file đến)."""
        task = asyncio.create_task(self._relay(client, stream), name=f"relay-{stream.stream_id}")
        client.tasks.append(task)

    async def _relay(self, sender: ClientSession, stream) -> None:
        """TODO[A-D2]:
          - file_id = stream.stream_id - FILE_STREAM_BASE; tìm transfer, kiểm tra sender đúng và
            status == "accepted" (nếu không -> log và return).
          - receiver = self.sessions.get(transfer.receiver); status = "relaying"
          - Lặp tới khi transfer.relayed >= transfer.size:
                chunk = await stream.read_message()
                await receiver.udp.stream(transfer.stream_id).send(chunk)
                transfer.relayed += len(chunk)
          - Nếu receiver rời đi giữa chừng -> gửi FILE_DONE {ok: False, reason} cho sender.
        """
        raise NotImplementedError("TODO[A-D2] FileRelay._relay")

    async def abort_for(self, username: str) -> None:
        """TODO[A-D3]: khi một user rời đi, hủy mọi transfer liên quan và báo FILE_DONE {ok: False}
        cho phía còn lại (nếu còn online)."""
        pass
