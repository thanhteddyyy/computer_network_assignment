import asyncio, json, struct, time

async def gui_tho(raw: bytes, ten: str):
    """Mở kết nối TCP, gửi đúng các byte raw, in ra server trả lời gì."""
    reader, writer = await asyncio.open_connection("127.0.0.1", 9000)
    writer.write(raw)
    await writer.drain()
    t = time.time()
    data = await reader.read(1000)
    print(f"{ten:<28} → sau {time.time() - t:4.1f}s nhận: {data[4:].decode() or '(không có gì)'}")
    writer.close()

def frame(payload: bytes) -> bytes:
    return struct.pack("!I", len(payload)) + payload       # 4 byte độ dài + nội dung

async def main():
    await gui_tho(frame(json.dumps({"type": "HELLO", "version": 99}).encode()), "1. Sai version")
    await gui_tho(frame(json.dumps({"type": "AUTH_REQ"}).encode()),             "2. Sai thứ tự")
    await gui_tho(frame(b"not json"),                                           "3. Không phải JSON")
    await gui_tho(frame(b"[1, 2]"),                                             "4. JSON nhưng không có type")
    await gui_tho(struct.pack("!I", 0xFFFFFFFF),                                "5. Khai độ dài 4 GB")
    await gui_tho(b"",                                                          "6. Im lặng (chờ ~10s)")

asyncio.run(main())