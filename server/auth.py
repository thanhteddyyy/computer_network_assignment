"""Lưu tài khoản người dùng trong data/users.json, mật khẩu băm PBKDF2 + salt.

Trạng thái: load/save HOÀN THIỆN; register/verify là TODO cho người B (ngày 1).
Định dạng file: {"alice": {"salt": "<hex>", "hash": "<hex>"}, ...}
"""
import json
import os
import re
import threading

from common.config import USERS_FILE
from common.crypto import hash_password, verify_password

USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{3,20}$")
MIN_PASSWORD = 4


class UserStore:
    def __init__(self, path: str = USERS_FILE) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._users: dict[str, dict] = self._load()

    def _load(self) -> dict:
        if not os.path.exists(self.path):
            return {}
        with open(self.path, encoding="utf-8") as f:
            return json.load(f)

    def _save(self) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._users, f, indent=2)
        os.replace(tmp, self.path)   # ghi nguyên tử: không bao giờ để file hỏng giữa chừng

    def register(self, username: str, password: str) -> tuple[bool, str]:
        """TODO[B-D1]:
          - Kiểm tra USERNAME_RE, độ dài mật khẩu >= MIN_PASSWORD, username chưa tồn tại.
          - salt, h = hash_password(password); lưu {"salt": salt, "hash": h}; self._save() (trong self._lock).
          - Trả về (True, "ok") hoặc (False, "<lý do>").
        """
        raise NotImplementedError("TODO[B-D1] UserStore.register")

    def verify(self, username: str, password: str) -> bool:
        """TODO[B-D1]: trả về True nếu user tồn tại và verify_password(...) đúng."""
        raise NotImplementedError("TODO[B-D1] UserStore.verify")
