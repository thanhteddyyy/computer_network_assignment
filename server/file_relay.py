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
from common.messages import ProtocolError, T, make
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
        ref = msg.get("client_ref")
        try:                                                                   # (1)
            to = str(msg["to"])
            name = str(msg["name"])
            size = int(msg["size"])
            sha256 = str(msg["sha256"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ProtocolError(f"FILE_OFFER thiếu hoặc sai trường: {exc}") from exc
        if size < 0:
            raise ProtocolError("FILE_OFFER có size âm")

        receiver = self.sessions.get(to)                                       # (2)
        if receiver is None or receiver is sender:
            await sender.control.send(make(T.FILE_OFFER_ACK, client_ref=ref, error=f"không gửi được cho {to}"))
            return

        t = Transfer(next(self._ids), sender.username, receiver.username, name, size, sha256)   # (3)
        self.transfers[t.file_id] = t

        await sender.control.send(make(T.FILE_OFFER_ACK, client_ref=ref, file_id=t.file_id))   # (4)
        await receiver.control.send(make(T.FILE_OFFER, file_id=t.file_id, name=name, size=size,
                                         sha256=sha256, **{"from": sender.username}))
        log.info("file #%d: %s -> %s, %s (%d byte)", t.file_id, t.sender, t.receiver, name, size)

    def _find(self, msg: dict, user: ClientSession, role: str) -> Transfer | None:
        """Tìm transfer theo file_id, và chỉ trả về nếu user đúng là người gửi/nhận (role) của nó."""
        t = self.transfers.get(msg.get("file_id"))
        if t is None or getattr(t, role) != user.username:
            return None
        return t    




    async def on_accept(self, receiver: ClientSession, msg: dict) -> None:
        """TODO[A-D2]: kiểm tra transfer tồn tại và receiver đúng người; status = accepted; báo cho sender."""
        t = self._find(msg, receiver, "receiver")
        if t is None or t.status != "offered":
            await receiver.control.send(make(T.ERROR, reason="file_id không hợp lệ"))
            return
        sender = self.sessions.get(t.sender)
        if sender is None:
            del self.transfers[t.file_id]
            await receiver.control.send(make(T.FILE_DONE, file_id=t.file_id, ok=False, reason="người gửi đã rời đi"))
            return
        t.status = "accepted"
        await sender.control.send(make(T.FILE_ACCEPT, file_id=t.file_id))
        log.info("file #%d: %s đồng ý nhận", t.file_id, t.receiver)

    async def on_reject(self, receiver: ClientSession, msg: dict) -> None:
        """TODO[A-D2]: status = rejected; báo cho sender; xóa transfer."""
        t = self._find(msg, receiver, "receiver")
        if t is None or t.status != "offered":
            await receiver.control.send(make(T.ERROR, reason="file_id không hợp lệ"))
            return
        t.status = "rejected"
        del self.transfers[t.file_id]
        sender = self.sessions.get(t.sender)
        if sender is not None:
            await sender.control.send(make(T.FILE_REJECT, file_id=t.file_id,
                                           reason=msg.get("reason", "người nhận từ chối")))
        log.info("file #%d: %s từ chối", t.file_id, t.receiver)

    async def on_done(self, receiver: ClientSession, msg: dict) -> None:
        """TODO[A-D2]: chuyển FILE_DONE cho sender; status = done/aborted; xóa transfer."""
        t = self._find(msg, receiver, "receiver")
        if t is None:
            await receiver.control.send(make(T.ERROR, reason="file_id không hợp lệ"))
            return
        ok = bool(msg.get("ok"))
        t.status = "done" if ok else "aborted"
        del self.transfers[t.file_id]
        sender = self.sessions.get(t.sender)
        if sender is not None:
            await sender.control.send(make(T.FILE_DONE, file_id=t.file_id, ok=ok, reason=msg.get("reason", "")))
        log.info("file #%d: kết thúc, ok=%s", t.file_id, ok)

    def attach_stream(self, client: ClientSession, stream) -> None:
        """Gọi khi phiên UDP của client xuất hiện stream >= FILE_STREAM_BASE (dữ liệu file đến)."""
        task = asyncio.create_task(self._relay(client, stream), name=f"relay-{stream.stream_id}")
        client.tasks.append(task)

    async def _relay(self, sender: ClientSession, stream) -> None:
        """Đọc từng chunk từ sender và đẩy ngay sang receiver qua UDP (không lưu đĩa)."""
        file_id = stream.stream_id - FILE_STREAM_BASE
        transfer = self.transfers.get(file_id)

        # 1. Kiểm tra an toàn: đúng transfer, đúng người gửi, trạng thái đã accepted
        if transfer is None or transfer.sender != sender.username or transfer.status != "accepted":
            log.warning("relay từ chối: stream=%d, file_id=%d không hợp lệ hoặc chưa accepted", stream.stream_id, file_id)
            return

        receiver = self.sessions.get(transfer.receiver)
        if receiver is None or receiver.udp is None:
            log.warning("file #%d: người nhận %s không online", file_id, transfer.receiver)
            transfer.status = "aborted"
            await sender.control.send(make(T.FILE_DONE, file_id=file_id, ok=False, reason="người nhận không online"))
            return

        transfer.status = "relaying"
        log.info("file #%d: bắt đầu relay %s -> %s (%d byte)", file_id, transfer.sender, transfer.receiver, transfer.size)

        try:
            while transfer.relayed < transfer.size:
                chunk = await stream.read_message()
                if not chunk:
                    break

                # Kiểm tra người nhận có còn sống giữa chừng không
                receiver = self.sessions.get(transfer.receiver)
                if receiver is None or receiver.udp is None:
                    log.warning("file #%d: người nhận %s mất kết nối giữa chừng", file_id, transfer.receiver)
                    transfer.status = "aborted"
                    await sender.control.send(make(T.FILE_DONE, file_id=file_id, ok=False, reason="người nhận mất kết nối giữa chừng"))
                    return

                await receiver.udp.stream(transfer.stream_id).send(chunk)
                transfer.relayed += len(chunk)

            log.info("file #%d: relay xong %d/%d byte", file_id, transfer.relayed, transfer.size)
        except Exception as exc:
            log.error("file #%d: lỗi relay: %s", file_id, exc)
            transfer.status = "aborted"
            await sender.control.send(make(T.FILE_DONE, file_id=file_id, ok=False, reason=f"lỗi relay: {exc}"))



    async def abort_for(self, username: str) -> None:
        """Khi một user rời đi, hủy mọi transfer liên quan và báo FILE_DONE {ok: False} cho phía còn lại."""
        for t in list(self.transfers.values()):                                    # (1)
            if t.sender != username and t.receiver != username:                    # (2)
                continue
            self.transfers.pop(t.file_id, None)                                    # (3)
            t.status = "aborted"                                                   # (4)
            other_name = t.receiver if t.sender == username else t.sender          # (5)
            other = self.sessions.get(other_name)                                  # (6)
            if other is not None:                                                  # (7)
                try:
                    await other.control.send(make(                                 # (8)
                        T.FILE_DONE,
                        file_id=t.file_id,
                        ok=False,
                        reason=f"{username} đã rời đi",
                    ))
                except Exception:
                    pass
            log.info("file #%d: hủy do %s rời đi", t.file_id, username)            # (9)