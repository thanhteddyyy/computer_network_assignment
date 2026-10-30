"""Logger dùng chung. Log rõ ràng là công cụ debug mạng quan trọng nhất (và là bằng chứng cho báo cáo)."""
import logging
import os
import sys

_FORMAT = "%(asctime)s.%(msecs)03d %(levelname)-5s [%(name)s] %(message)s"
_configured = False


def setup_logging(level: str | None = None) -> None:
    """Gọi một lần ở hàm main. Mức log lấy từ tham số hoặc biến môi trường LOG_LEVEL."""
    global _configured
    if _configured:
        return
    level = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(level=level, format=_FORMAT, datefmt="%H:%M:%S", stream=sys.stdout)
    _configured = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
