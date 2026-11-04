"""Stress test step 4: N client cùng kết nối, đăng nhập, rồi rời đi (một số chết đột ngột).

Chạy server trước, rồi:
    python -m tests.stress_login --n 20 --kill 5
In ra: thời gian đăng nhập, độ trễ của một client "đo thử" trong lúc server đang bận,
và số người online mà một client "quan sát" nhìn thấy ở từng giai đoạn.
"""
import argparse
import asyncio
import time

from client.client import CollabClient
from common.config import TCP_PORT
from common.log import setup_logging
from transport.loss_sim import LossSimulator
from transport.tcp_channel import ControlChannel


async def bot(i: int, args, start: asyncio.Event) -> CollabClient:
    c = CollabClient(LossSimulator())
    name = f"bot{i:02d}"
    await start.wait()                                   # mọi bot xuất phát cùng lúc
    await c.connect(args.server, args.tcp_port)
    await c.register(name, "bot-pass")                   # lần chạy sau báo "đã tồn tại" là bình thường
    ok, reason = await c.login(name, "bot-pass")
    if not ok:
        raise RuntimeError(f"{name}: {reason}")
    return c


async def do_tre(args) -> float:
    """Một client chỉ chào HELLO, đo server mất bao lâu mới trả lời (server có bị 'đơ' không)."""
    t = time.perf_counter()
    ch = await ControlChannel.connect(args.server, args.tcp_port)
    await ch.send({"type": "HELLO", "version": 1})
    await ch.recv(timeout=30)
    await ch.close()
    return (time.perf_counter() - t) * 1000


async def main(args) -> None:
    obs = CollabClient(LossSimulator())                  # "người quan sát" ngồi xem ai vào, ai ra
    await obs.connect(args.server, args.tcp_port)
    await obs.register("observer", "obs-pass")
    ok, reason = await obs.login("observer", "obs-pass")
    assert ok, reason

    start = asyncio.Event()
    tasks = [asyncio.create_task(bot(i, args, start)) for i in range(args.n)]
    t0 = time.perf_counter()
    start.set()
    await asyncio.sleep(0.05)
    tre = await do_tre(args)                             # đo ngay giữa lúc server đang bận đăng nhập cho N bot
    bots = await asyncio.gather(*tasks, return_exceptions=True)
    t_login = time.perf_counter() - t0
    loi = [b for b in bots if isinstance(b, Exception)]
    bots = [b for b in bots if not isinstance(b, Exception)]
    await asyncio.sleep(0.5)
    print(f"\n{len(bots)}/{args.n} bot đăng nhập thành công trong {t_login:.2f}s")
    print(f"Client đo thử nhận HELLO_ACK sau {tre:.0f} ms (server đang bận mà vẫn trả lời nhanh = tốt)")
    print(f"Người quan sát thấy online: {len(obs.users)} người (kỳ vọng {len(bots) + 1})")

    chet, ngoan = bots[:args.kill], bots[args.kill:]
    for c in chet:
        c.ch.writer.transport.abort()                    # chết đột ngột, không LOGOUT
        if c.endpoint:
            c.endpoint.close()
    await asyncio.gather(*(c.logout() for c in ngoan))
    await asyncio.sleep(1)
    print(f"Sau khi {len(ngoan)} bot đăng xuất + {len(chet)} bot chết đột ngột:"
          f" người quan sát thấy online {len(obs.users)} người (kỳ vọng 1)")
    for e in loi:
        print("  LỖI:", repr(e))
    await obs.logout()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=20)
    ap.add_argument("--kill", type=int, default=5)
    ap.add_argument("--server", default="127.0.0.1")
    ap.add_argument("--tcp-port", type=int, default=TCP_PORT)
    ap.add_argument("--log", default="WARNING")
    a = ap.parse_args()
    setup_logging(a.log)
    asyncio.run(main(a))