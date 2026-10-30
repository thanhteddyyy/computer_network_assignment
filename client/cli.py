"""Client dòng lệnh để test nhanh khi chưa có giao diện web (ngày 1-2).

Chạy:  python -m client.cli --server 127.0.0.1
Lệnh:  /register <user> <pass>   /login <user> <pass>   /users   /stats
       /dm <user> <nội dung>     /quit       (gõ chữ thường = chat chung)
Trạng thái: HOÀN THIỆN (chỉ chạy được khi các TODO liên quan đã xong).
"""
import argparse
import asyncio
import time

from common.config import TCP_PORT
from common.log import get_logger, setup_logging
from client.client import CollabClient
from transport.loss_sim import LossSimulator

log = get_logger("cli")


async def print_events(client: CollabClient) -> None:
    last = 0
    while True:
        for ev in client.events.since(last):
            last = ev["id"]
            k = ev["kind"]
            if k == "message":
                m = ev["message"]
                tag = "" if m.get("to") == "*" else " (riêng)"
                print(f"[{time.strftime('%H:%M:%S', time.localtime(m['ts']))}] {m['from']}{tag}: {m['text']}")
            elif k in ("user_join", "user_leave", "status", "error", "file_incoming", "file_done"):
                print(f"* {k}: { {x: y for x, y in ev.items() if x not in ('id', 'kind', 'ts')}}")
        await asyncio.sleep(0.2)


async def repl(client: CollabClient, server: str | None, port: int) -> None:
    await client.connect(server, port)
    printer = asyncio.create_task(print_events(client))
    try:
        while True:
            line = (await asyncio.to_thread(input)).strip()
            if not line:
                continue
            parts = line.split(" ", 2)
            cmd = parts[0]
            try:
                if cmd == "/quit":
                    break
                elif cmd == "/register" and len(parts) == 3:
                    print(await client.register(parts[1], parts[2]))
                elif cmd == "/login" and len(parts) == 3:
                    print(await client.login(parts[1], parts[2]))
                elif cmd == "/users":
                    for u in client.users.values():
                        print(f"  {u['username']:<15} {u['ip']}:{u['port']}")
                elif cmd == "/stats":
                    print(client.udp.stats() if client.udp else "chưa đăng nhập")
                elif cmd == "/dm" and len(parts) == 3:
                    await client.chat.send(parts[1], parts[2])
                elif cmd.startswith("/"):
                    print("lệnh không hợp lệ")
                else:
                    await client.chat.send("*", line)
            except NotImplementedError as exc:
                print(f"!! chưa cài đặt: {exc}")
    finally:
        printer.cancel()
        await client.logout()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default=None)
    ap.add_argument("--tcp-port", type=int, default=TCP_PORT)
    ap.add_argument("--loss", type=float, default=0.0)
    ap.add_argument("--log", default="WARNING")
    args = ap.parse_args()
    setup_logging(args.log)
    try:
        asyncio.run(repl(CollabClient(LossSimulator(args.loss)), args.server, args.tcp_port))
    except (KeyboardInterrupt, EOFError):
        pass


if __name__ == "__main__":
    main()
