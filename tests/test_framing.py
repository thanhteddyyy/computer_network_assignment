import asyncio
import unittest

from common.framing import FrameError, encode_frame, read_frame
from tests._helpers import run_async


class FramingTest(unittest.TestCase):
    @run_async
    async def test_two_frames_glued_together(self):
        # TCP có thể giao 2 message dính nhau trong một lần đọc -> framing phải tách đúng
        reader = asyncio.StreamReader()
        reader.feed_data(encode_frame(b"hello") + encode_frame(b"world!"))
        self.assertEqual(await read_frame(reader), b"hello")
        self.assertEqual(await read_frame(reader), b"world!")

    @run_async
    async def test_frame_split_in_pieces(self):
        reader = asyncio.StreamReader()
        data = encode_frame(b"x" * 1000)
        reader.feed_data(data[:3])            # chỉ mới có 3 byte của header
        task = asyncio.create_task(read_frame(reader))
        await asyncio.sleep(0)
        reader.feed_data(data[3:500])
        await asyncio.sleep(0)
        reader.feed_data(data[500:])
        self.assertEqual(await task, b"x" * 1000)

    @run_async
    async def test_oversized_frame_rejected(self):
        reader = asyncio.StreamReader()
        reader.feed_data((50 * 1024 * 1024).to_bytes(4, "big"))
        with self.assertRaises(FrameError):
            await read_frame(reader)

    @run_async
    async def test_peer_closed(self):
        reader = asyncio.StreamReader()
        reader.feed_data(encode_frame(b"abc")[:5])
        reader.feed_eof()
        with self.assertRaises(asyncio.IncompleteReadError):
            await read_frame(reader)


if __name__ == "__main__":
    unittest.main()
