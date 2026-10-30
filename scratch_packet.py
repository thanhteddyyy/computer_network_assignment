import json
from common.crypto import KeyExchange
from common.framing import encode_frame
from transport.packet import AckFrame, Flags, Packet, PacketError, PacketType

# 1) Một gói DATA chở tin chat
chat = json.dumps({"to": "*", "text": "hi"}).encode()
p = Packet(PacketType.DATA, session_id=0xABCD, packet_number=5,
           stream_id=1, offset=0, flags=Flags.FIN, payload=chat)
raw = p.encode()
print("Độ dài:", len(raw))
print("Header :", raw[:21].hex(" "))
print("Payload:", raw[21:])

# 2) Giải mã ngược lại
print(Packet.decode(raw))

# 3) Sửa 1 byte magic -> phải bị từ chối
bad = b"\x00" + raw[1:]
try:
    Packet.decode(bad)
except PacketError as e:
    print("Bị từ chối:", e)

# 4) Mã hóa gói: payload không còn đọc được
keys = KeyExchange().derive(KeyExchange().public_bytes())
wire = p.seal(keys.c2s)
print("Trên dây:", wire.hex(" "))
print("Có thấy chữ 'hi' không?", b"hi" in wire)

# 5) Frame TCP
print(encode_frame(json.dumps({"type": "HELLO", "version": 1}).encode()))

# 6) ACK frame
print(AckFrame(9, 256, 5, [(6, 9), (0, 4)]).encode().hex(" "))