import asyncio
from transport.tcp_channel import ControlChannel

async def main():
    ch = await ControlChannel.connect("127.0.0.1", 9000)
    print("Đã kết nối TCP tới", ch.peername)
    await ch.send({"type": "HELLO", "version": 1, "client_name": "test"})
    print("Server trả lời:", await ch.recv(timeout=5))
    await ch.close()

asyncio.run(main())