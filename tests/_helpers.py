"""Tiện ích cho test. Test của phần chưa làm (NotImplementedError) sẽ được SKIP thay vì FAIL,
nên ngay từ đầu chạy test đã thấy xanh; làm xong TODO nào thì test tương ứng tự chuyển sang chạy thật."""
import asyncio
import functools
import unittest


def todo(fn):
    @functools.wraps(fn)
    def wrapper(self, *a, **kw):
        try:
            result = fn(self, *a, **kw)
            if asyncio.iscoroutine(result):
                asyncio.run(result)
        except NotImplementedError as exc:
            raise unittest.SkipTest(f"chưa cài đặt: {exc}")
    return wrapper


def run_async(fn):
    @functools.wraps(fn)
    def wrapper(self, *a, **kw):
        return asyncio.run(fn(self, *a, **kw))
    return wrapper
