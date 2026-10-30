"""Thử nghiệm tầng 2 phía UDP: udp_endpoint + loss_sim. Không cần bật server."""
import asyncio
import time

from transport.loss_sim import LossSimulator
from transport.packet import Packet, PacketType
from transport.udp_endpoint import UdpEndpoint


class SoGhi:
    """Một 'phòng' nhận hàng: ghi lại số thứ tự và thời điểm mỗi gói đến."""
    def __init__(self):
        self.da_nhan = []                     # danh sách (packet_number, thời điểm đến)

    def on_datagram(self, data, addr):
        pkt = Packet.decode(data)
        self.da_nhan.append((pkt.packet_number, time.perf_counter()))


async def gui_va_nhan(loss: LossSimulator, so_goi: int, nghi_moi: int | None):
    nhan = await UdpEndpoint.open("127.0.0.1", 0)
    gui = await UdpEndpoint.open("127.0.0.1", 0, loss)
    ghi = SoGhi()
    nhan.register(42, ghi)                    # phòng số 42
    dich = ("127.0.0.1", nhan.local_port)

    gio_gui = {}
    for pn in range(so_goi):
        gio_gui[pn] = time.perf_counter()
        gui.sendto(Packet(PacketType.DATA, 42, pn, payload=b"x" * 100).encode(), dich)
        if nghi_moi and pn % nghi_moi == 0:
            await asyncio.sleep(0.001)        # nghỉ một chút cho bên nhận kịp lấy hàng

    gui.sendto(Packet(PacketType.DATA, 99, 0).encode(), dich)   # gửi tới phòng 99 không tồn tại
    gui.sendto(b"rac rac rac", dich)                            # hộp rác, không có C0DE
    await asyncio.sleep(0.5)                                    # chờ các gói bị giữ lại đến nơi
    gui.close(); nhan.close()
    return ghi.da_nhan, gio_gui, nhan.unknown_packets, loss.dropped


def bao_cao(ten, so_goi, da_nhan, gio_gui, la, bi_vut):
    thu_tu = [pn for pn, _ in da_nhan]
    dao = sum(1 for a, b in zip(thu_tu, thu_tu[1:]) if b < a)  # số lần gói sau đến trước gói trước
    tre = [(t - gio_gui[pn]) * 1000 for pn, t in da_nhan]
    print(f"\n=== {ten} ===")
    print(f"Gửi {so_goi} gói | loss_sim vứt {bi_vut} | đến nơi {len(da_nhan)} | gói lạ bị lọc {la}")
    print(f"Số lần đảo thứ tự: {dao}")
    if tre:
        print(f"Độ trễ: nhỏ nhất {min(tre):.1f} ms, lớn nhất {max(tre):.1f} ms")
    print("20 gói đầu tiên đến theo thứ tự:", thu_tu[:20])


async def main():
    tests = [
        ("A. Mạng tốt",                       LossSimulator(),                                   20),
        ("B. Rơi 10%",                        LossSimulator(loss_rate=0.1, seed=1),              20),
        ("C. Trễ 30 ms + jitter 20 ms",       LossSimulator(delay_ms=30, jitter_ms=20, seed=1),  20),
        ("D. Cả ba",                          LossSimulator(0.1, 30, 20, seed=1),                20),
        ("E. Xả liền một mạch, không nghỉ",   LossSimulator(),                                   None),
    ]
    for ten, loss, nghi in tests:
        so_goi = 1000
        bao_cao(ten, so_goi, *await gui_va_nhan(loss, so_goi, nghi))

asyncio.run(main())