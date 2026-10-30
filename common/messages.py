"""Định nghĩa các message trên kênh TCP điều khiển (JSON).

Mỗi message là một dict có khóa "type". Bảng đầy đủ các trường: docs/protocol_spec.md.
Dùng hằng số ở đây thay vì gõ chuỗi tay để tránh lỗi chính tả.
"""


class T:
    # Handshake (bước 2)
    HELLO = "HELLO"                  # C->S {version, client_name}
    HELLO_ACK = "HELLO_ACK"          # S->C {version, server_name}
    # Trao khóa (bước 8)
    KEY_EXCHANGE = "KEY_EXCHANGE"    # 2 chiều {public_key: hex}
    # Xác thực (bước 3)
    REGISTER = "REGISTER"            # C->S {username, password}
    AUTH_REQ = "AUTH_REQ"            # C->S {username, password, udp_port}
    AUTH_OK = "AUTH_OK"              # S->C {session_id, udp_port, users}
    AUTH_FAIL = "AUTH_FAIL"          # S->C {reason}
    # Presence
    USER_LIST = "USER_LIST"          # S->C {users: [{username, ip, port}]}
    USER_JOIN = "USER_JOIN"          # S->C {user}
    USER_LEAVE = "USER_LEAVE"        # S->C {username, reason}
    # File (điều khiển qua TCP, dữ liệu qua UDP)
    FILE_OFFER = "FILE_OFFER"        # C->S {client_ref, to, name, size, sha256}; S->C(nhận) thêm {file_id, from}
    FILE_OFFER_ACK = "FILE_OFFER_ACK"  # S->C(gửi) {client_ref, file_id} hoặc {client_ref, error}
    FILE_ACCEPT = "FILE_ACCEPT"      # C->S->C {file_id}
    FILE_REJECT = "FILE_REJECT"      # C->S->C {file_id, reason}
    FILE_DONE = "FILE_DONE"          # {file_id, ok, reason?}
    # Kết thúc / lỗi
    LOGOUT = "LOGOUT"
    ERROR = "ERROR"                  # {reason}


def make(msg_type: str, **fields) -> dict:
    return {"type": msg_type, **fields}


def expect(msg: dict, *types: str) -> dict:
    """Kiểm tra message thuộc một trong các type mong đợi, nếu không thì ném ProtocolError."""
    if msg.get("type") not in types:
        raise ProtocolError(f"mong đợi {types}, nhận {msg.get('type')}")
    return msg


class ProtocolError(Exception):
    """Peer gửi message sai thứ tự hoặc sai định dạng."""
