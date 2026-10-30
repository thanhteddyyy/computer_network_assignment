import asyncio
import unittest

from client.http_server import build_response, read_request
from transport.mock_session import MockSession
from tests._helpers import run_async, todo


class MockSessionTest(unittest.TestCase):
    @run_async
    async def test_pair(self):
        a, b = MockSession.pair()
        await a.stream(1).send(b"hello")
        await a.stream(2).send(b"dm")
        self.assertEqual(await b.stream(2).read_message(), b"dm")
        self.assertEqual(await b.stream(1).read_message(), b"hello")


class HttpTest(unittest.TestCase):
    def test_build_response(self):
        raw = build_response(200, b"hi")
        self.assertTrue(raw.startswith(b"HTTP/1.1 200 OK\r\n"))
        self.assertIn(b"Content-Length: 2\r\n", raw)
        self.assertTrue(raw.endswith(b"\r\n\r\nhi"))

    @todo
    async def test_parse_post_with_body_in_pieces(self):
        body = b'{"to": "*", "text": "xin ch\xc3\xa0o"}'
        raw = (b"POST /api/send?x=1&name=a%20b HTTP/1.1\r\nHost: localhost\r\n"
               b"Content-Type: application/json\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
        reader = asyncio.StreamReader()
        reader.feed_data(raw[:20])
        task = asyncio.create_task(read_request(reader))
        await asyncio.sleep(0)
        reader.feed_data(raw[20:])
        req = await task
        self.assertEqual((req.method, req.path), ("POST", "/api/send"))
        self.assertEqual(req.query, {"x": "1", "name": "a b"})
        self.assertEqual(req.headers["content-type"], "application/json")
        self.assertEqual(req.json()["text"], "xin chào")

    @todo
    async def test_closed_connection_returns_none(self):
        reader = asyncio.StreamReader()
        reader.feed_eof()
        self.assertIsNone(await read_request(reader))


if __name__ == "__main__":
    unittest.main()
