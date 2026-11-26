"""Minimal PostgreSQL frontend/backend wire-protocol primitives (protocol v3).

Only the pieces this pool needs are implemented: framing, startup packets,
common frontend/backend message builders, and the ErrorResponse parser.

Reference: https://www.postgresql.org/docs/current/protocol-message-formats.html
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field

PROTOCOL_VERSION = 196608  # 3.0
SSL_REQUEST_CODE = 80877103
GSSENC_REQUEST_CODE = 80877104
CANCEL_REQUEST_CODE = 80877102

# Frontend message tags
PASSWORD = b"p"
QUERY = b"Q"
FUNCTION_CALL = b"F"
COPY_DONE = b"c"
COPY_FAIL = b"f"
TERMINATE = b"X"
PARSE = b"P"
BIND = b"B"
EXECUTE = b"E"
DESCRIBE = b"D"
CLOSE = b"C"
SYNC = b"S"
FLUSH = b"H"

# Backend message tags
AUTHENTICATION = b"R"
BACKEND_KEY = b"K"
PARAMETER_STATUS = b"S"
READY_FOR_QUERY = b"Z"
ERROR_RESPONSE = b"E"
NOTICE_RESPONSE = b"N"
NOTIFICATION_RESPONSE = b"A"
PARSE_COMPLETE = b"1"
BIND_COMPLETE = b"2"
CLOSE_COMPLETE = b"3"
COMMAND_COMPLETE = b"C"
EMPTY_QUERY_RESPONSE = b"I"
ROW_DESCRIPTION = b"T"
DATA_ROW = b"D"
NO_DATA = b"n"
PORTAL_SUSPENDED = b"s"
PARAMETER_DESCRIPTION = b"t"
COPY_IN_RESPONSE = b"G"
COPY_OUT_RESPONSE = b"H"
COPY_BOTH_RESPONSE = b"W"
COPY_DATA = b"d"
NEGOTIATE_PROTOCOL_VERSION = b"v"

MAX_MESSAGE_LENGTH = 1 << 24  # 16 MiB guard for frontend messages


class ProtocolError(Exception):
    pass


class ConnectionClosed(ProtocolError):
    pass


@dataclass
class ErrorInfo:
    severity: str = ""
    code: str = ""
    message: str = ""
    detail: str = ""
    hint: str = ""
    fields: dict[str, str] = field(default_factory=dict)

    @property
    def is_query_canceled(self) -> bool:
        return self.code == "57014"

    @property
    def is_admin_shutdown(self) -> bool:
        # 57P01 admin_shutdown, 57P02 crash_shutdown, 57P03 cannot_connect_now
        return self.code in ("57P01", "57P02", "57P03")

    def __str__(self) -> str:
        return f"{self.severity} {self.code}: {self.message}"


def parse_error_fields(payload: bytes) -> ErrorInfo:
    info = ErrorInfo()
    pos = 0
    mapping = {
        "S": "severity",
        "C": "code",
        "M": "message",
        "D": "detail",
        "H": "hint",
    }
    while pos < len(payload):
        code = payload[pos : pos + 1]
        if code == b"\x00":
            break
        end = payload.find(b"\x00", pos + 1)
        if end < 0:
            break
        value = payload[pos + 1 : end].decode("utf-8", "replace")
        key = code.decode("latin-1")
        info.fields[key] = value
        attr = mapping.get(key)
        if attr:
            setattr(info, attr, value)
        pos = end + 1
    return info


def parse_parameter_status(payload: bytes) -> tuple[str, str]:
    name, _, value = payload.partition(b"\x00")
    value = value.split(b"\x00", 1)[0]
    return name.decode("utf-8", "replace"), value.decode("utf-8", "replace")


def startup_packet(params: dict[str, str]) -> bytes:
    body = struct.pack("!I", PROTOCOL_VERSION)
    for key, value in params.items():
        body += key.encode("utf-8") + b"\x00" + value.encode("utf-8") + b"\x00"
    body += b"\x00"
    return struct.pack("!I", len(body) + 4) + body


def cancel_request_packet(process_id: int, secret_key: int) -> bytes:
    return struct.pack("!IIII", 16, CANCEL_REQUEST_CODE, process_id, secret_key)


def message(tag: bytes, payload: bytes = b"") -> bytes:
    return tag + struct.pack("!I", len(payload) + 4) + payload


def query_message(sql: str) -> bytes:
    return message(QUERY, sql.encode("utf-8") + b"\x00")


def password_message(secret: str) -> bytes:
    return message(PASSWORD, secret.encode("utf-8") + b"\x00")


def cstr(value: str) -> bytes:
    return value.encode("utf-8") + b"\x00"


def parse_complete() -> bytes:
    return message(PARSE_COMPLETE)


def bind_complete() -> bytes:
    return message(BIND_COMPLETE)


def ready_for_query(status: bytes = b"I") -> bytes:
    return message(READY_FOR_QUERY, status)


def auth_ok() -> bytes:
    return message(AUTHENTICATION, struct.pack("!I", 0))


def backend_key(pid: int, secret: int) -> bytes:
    return message(BACKEND_KEY, struct.pack("!II", pid, secret))


def parameter_status(name: str, value: str) -> bytes:
    return message(PARAMETER_STATUS, cstr(name) + cstr(value))


def command_complete(tag: str) -> bytes:
    return message(COMMAND_COMPLETE, cstr(tag))


def error_response(
    severity: str,
    code: str,
    message_text: str,
    *,
    detail: str | None = None,
    hint: str | None = None,
) -> bytes:
    payload = b"S" + cstr(severity) + b"V" + cstr(severity)
    payload += b"C" + cstr(code)
    payload += b"M" + cstr(message_text)
    if detail:
        payload += b"D" + cstr(detail)
    if hint:
        payload += b"H" + cstr(hint)
    payload += b"\x00"
    return message(ERROR_RESPONSE, payload)


def terminate() -> bytes:
    return message(TERMINATE)


def read_exact(sock, n: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < n:
        chunk = sock.recv(n - len(chunks))
        if not chunk:
            raise ConnectionClosed("peer closed connection")
        chunks += chunk
    return bytes(chunks)


def read_message(sock, *, timeout: float | None = None) -> tuple[bytes, bytes] | tuple[None, None]:
    """Read one backend message. Returns (None, None) on clean EOF at boundary."""
    if timeout is not None:
        sock.settimeout(timeout)
    try:
        header = sock.recv(1)
    finally:
        if timeout is not None:
            sock.settimeout(None)
    if not header:
        return None, None
    length = struct.unpack("!I", read_exact(sock, 4))[0]
    if length < 4 or length > MAX_MESSAGE_LENGTH:
        raise ProtocolError(f"invalid message length {length}")
    payload = read_exact(sock, length - 4)
    return header, payload


class MessageBuffer:
    """Incremental parser for a byte stream of frontend messages."""

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, data: bytes) -> None:
        self._buf += data

    def pop(self) -> tuple[bytes, bytes] | None:
        if len(self._buf) < 5:
            return None
        length = struct.unpack("!I", self._buf[1:5])[0]
        if length < 4 or length > MAX_MESSAGE_LENGTH:
            raise ProtocolError(f"invalid frontend message length {length}")
        total = 1 + length
        if len(self._buf) < total:
            return None
        tag = bytes(self._buf[0:1])
        payload = bytes(self._buf[5:total])
        del self._buf[:total]
        return tag, payload

    def __len__(self) -> int:
        return len(self._buf)


def split_cstrs(payload: bytes, count: int) -> tuple[list[bytes], int]:
    """Split `count` NUL-terminated strings; return (strings, bytes_consumed)."""
    out: list[bytes] = []
    pos = 0
    for _ in range(count):
        end = payload.find(b"\x00", pos)
        if end < 0:
            raise ProtocolError("malformed message: truncated string")
        out.append(payload[pos:end])
        pos = end + 1
    return out, pos
