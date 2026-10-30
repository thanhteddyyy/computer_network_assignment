"""Stress test bước 4 & 10: bật N client cùng lúc trong một tiến trình, mỗi client gửi M tin vào phòng chung.

Chạy server trước, rồi:
    python -m tests.spawn_clients --n 20 --m 10 [--server 127.0.0.1] [--loss 0.05] [--kill 3]
--kill k : k client cuối sẽ "chết đột ngột" (đóng socket, không LOGOUT) để kiểm tra bước 7.
Kết quả in ra: số tin mỗi client nhận được (kỳ vọng N*M) và thời gian chạy -> chụp đưa vào báo cáo.
"""
import argparse
import asyncio
import time

from common.config import TCP_PORT
from common.log import setup_logging
from client.client import CollabClient
from transport.loss_sim import LossSimulator


async def bot(i: int, args, ready: asyncio.Barrier, results: dict) -> None:
    c = CollabClient(LossSimulator(args.loss))
    name, pw = f"bot{i:02d}", "bot-pass"
    await c.connect(args.server, args.tcp_port)
    await c.register(name, pw)           # lỗi "đã tồn tại" ở lần chạy sau là bình thường
    ok, reason = await c.login(name, pw)
    if not ok:
        raise RuntimeError(f"{name} đăng nhập thất bại: {reason}")
    await ready.wait()
    for k in range(args.m):
        await c.chat.send("*", f"{name} tin {k}")
    await asyncio.sleep(args.wait)
    got = sum(1 for e in c.events.since(0) if e["kind"] == "message")
    results[name] = (got, c.udp.stats() if c.udp else None)
    if i >= args.n - args.kill:
        c.ch.writer.transport.abort()    # chết đột ngột: không LOGOUT
        if c.endpoint:
            c.endpoint.close()
    else:
        await c.logout()


async def main(args) -> None:
    ready = asyncio.Barrier(args.n)
    results: dict = {}
    t0 = time.monotonic()
    outcomes = await asyncio.gather(*(bot(i, args, ready, results) for i in range(args.n)), return_exceptions=True)
    errors = [o for o in outcomes if isinstance(o, Exception)]
    expected = args.n * args.m
    print(f"\n{args.n} client x {args.m} tin, mất gói giả lập {args.loss:.0%}, {time.monotonic() - t0:.1f}s")
    for name in sorted(results):
        got, st = results[name]
        flag = "OK " if got == expected else "THIẾU"
        print(f"  {flag} {name}: nhận {got}/{expected}  rtt={st and st['rtt_ms']}ms loss={st and st['loss']}")
    for e in errors:
        print("  LỖI:", repr(e))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=20)
    ap.add_argument("--m", type=int, default=10)
    ap.add_argument("--server", default="127.0.0.1")
    ap.add_argument("--tcp-port", type=int, default=TCP_PORT)
    ap.add_argument("--loss", type=float, default=0.0)
    ap.add_argument("--wait", type=float, default=5.0, help="giây chờ sau khi gửi xong")
    ap.add_argument("--kill", type=int, default=0)
    ap.add_argument("--log", default="WARNING")
    a = ap.parse_args()
    setup_logging(a.log)
    asyncio.run(main(a))
