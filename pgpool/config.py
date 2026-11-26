"""Pool configuration and the server-side connection factory."""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os
import socket
import struct
import time
from dataclasses import dataclass, field
from urllib.parse import parse_qs, urlparse

from . import wire

log = logging.getLogger("pgpool")


class StartupError(Exception):
    """Raised when a backend connection cannot be established."""


@dataclass
class ServerTarget:
    """How to reach the real PostgreSQL server."""

    host: str = "127.0.0.1"
    port: int = 5432
    unix_socket_dir: str | None = None
    user: str = "postgres"
    password: str | None = None
    database: str = "postgres"
    extra_startup: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dsn(cls, dsn: str, **overrides) -> "ServerTarget":
        """Parse a libpq-style URL, e.g. postgresql://user:pw@host:5432/db."""
        parsed = urlparse(dsn)
        query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        host = parsed.hostname or query.get("host", "127.0.0.1")
        socket_dir = None
        if query.get("host", "").startswith("/") or (parsed.hostname is None and host.startswith("/")):
            socket_dir = query.get("host", host)
            host = "127.0.0.1"
        target = cls(
            host=host,
            port=int(parsed.port or query.get("port", 5432)),
            unix_socket_dir=socket_dir,
            user=parsed.username or query.get("user", "postgres"),
            password=parsed.password or query.get("password"),
            database=(parsed.path or "/").lstrip("/") or query.get("dbname", "postgres"),
        )
        for key, value in overrides.items():
            if value is not None:
                setattr(target, key, value)
        return target

    def connect_socket(self, timeout: float = 10.0) -> socket.socket:
        if self.unix_socket_dir:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect(os.path.join(self.unix_socket_dir, f".s.PGSQL.{self.port}"))
        else:
            sock = socket.create_connection((self.host, self.port), timeout=timeout)
        sock.settimeout(None)
        return sock

    def connect_cancel_socket(self, timeout: float = 3.0) -> socket.socket:
        return self.connect_socket(timeout)

    def describe(self) -> str:
        if self.unix_socket_dir:
            return f"unix:{self.unix_socket_dir}:{self.port}/{self.database}"
        return f"{self.host}:{self.port}/{self.database}"


@dataclass
class TestHooks:
    """Knobs used by the demo page and the automated race tests.

    These exist so cancellation can be made to land inside the cleanup window
    deterministically; production use leaves them empty.
    """

    # Run before the cleanup statement; use to widen the window a late cancel
    # can land in (e.g. "SELECT pg_sleep(0.4)").
    cleanup_prefix_sql: str = ""
    # Skip DISCARD ALL entirely. The verification probe must then catch the
    # leftovers and refuse to reuse the backend.
    cleanup_skip_discard: bool = False
    # Extra pause *inside* the cleanup window, after the prefix.
    cleanup_delay: float = 0.0
    # Add a pause between the timeout firing and the next client being served.
    checkout_delay: float = 0.0


@dataclass
class PoolConfig:
    max_backends: int = 4
    min_backends: int = 1
    checkout_timeout: float = 30.0
    reset_timeout: float = 10.0
    disconnect_grace: float = 2.0
    cancel_barrier_rounds: int = 3
    cancel_barrier_window: float = 3.0
    idle_backend_timeout: float = 600.0
    relay_poll_interval: float = 0.05
    max_open_failures: int = 5
    application_name: str = "pgpool"
    # Client-facing authentication. Backends always use ServerTarget credentials.
    client_auth: str = "trust"  # trust | cleartext | md5
    client_password: str | None = None
    server: ServerTarget = field(default_factory=ServerTarget)
    hooks: TestHooks = field(default_factory=TestHooks)
    # Record at most this many events for the dashboard.
    event_log_size: int = 400
    series_points: int = 90

    def __post_init__(self) -> None:
        if self.hooks is None:
            self.hooks = TestHooks()

    def client_secret(self) -> str | None:
        return self.client_password if self.client_password is not None else self.server.password


class ServerConnector:
    """Opens one raw server connection and performs the startup handshake."""

    def __init__(self, target: ServerTarget, application_name: str = "pgpool") -> None:
        self.target = target
        self.application_name = application_name

    def connect(self, timeout: float = 10.0) -> tuple[socket.socket, int, int, list[tuple[str, str]]]:
        sock = self.target.connect_socket(timeout)
        try:
            sock.settimeout(timeout)
            params = {
                "user": self.target.user,
                "database": self.target.database,
                "application_name": self.application_name,
                "client_encoding": "UTF8",
                **self.target.extra_startup,
            }
            sock.sendall(wire.startup_packet(params))
            pid, secret, parameter_status = self._authenticate(sock)
            sock.settimeout(None)
            return sock, pid, secret, parameter_status
        except Exception:
            try:
                sock.close()
            except Exception:
                pass
            raise

    def _authenticate(self, sock: socket.socket) -> tuple[int, int, list[tuple[str, str]]]:
        pid = secret = 0
        statuses: list[tuple[str, str]] = []
        while True:
            tag, payload = wire.read_message(sock, timeout=10.0)
            if tag is None:
                raise StartupError("server closed connection during startup")
            if tag == wire.ERROR_RESPONSE:
                info = wire.parse_error_fields(payload)
                raise StartupError(str(info))
            if tag == wire.AUTHENTICATION:
                code = struct.unpack("!I", payload[:4])[0]
                if code == 0:
                    continue
                if code == 3:  # cleartext
                    if not self.target.password:
                        raise StartupError("server requested a password but none configured")
                    sock.sendall(wire.password_message(self.target.password))
                elif code == 5:  # md5
                    self._send_md5(sock, payload)
                elif code == 10:  # SASL
                    self._send_sasl(sock, payload)
                elif code in (11, 12):  # SASL continue / final are handled inline
                    continue
                else:
                    raise StartupError(f"unsupported authentication method {code}")
            elif tag == wire.BACKEND_KEY:
                pid, secret = struct.unpack("!II", payload[:8])
            elif tag == wire.PARAMETER_STATUS:
                statuses.append(wire.parse_parameter_status(payload))
            elif tag == wire.NOTICE_RESPONSE:
                continue
            elif tag == wire.READY_FOR_QUERY:
                return pid, secret, statuses

    def _send_md5(self, sock: socket.socket, payload: bytes) -> None:
        if not self.target.password:
            raise StartupError("server requested md5 password but none configured")
        salt = payload[4:8]
        inner = hashlib.md5(self.target.password.encode() + self.target.user.encode()).hexdigest()
        outer = hashlib.md5(inner.encode() + salt).hexdigest()
        sock.sendall(wire.password_message("md5" + outer))

    def _send_sasl(self, sock: socket.socket, payload: bytes) -> None:
        if not self.target.password:
            raise StartupError("server requested SCRAM-SHA-256 but no password configured")
        mechanisms = [m for m in payload[4:].split(b"\x00") if m]
        if b"SCRAM-SHA-256" not in mechanisms:
            raise StartupError("server requires an unsupported SASL mechanism")

        client_nonce = base64.b64encode(os.urandom(18)).decode()
        client_first_bare = f"n=,r={client_nonce}"
        client_first = "n,," + client_first_bare
        body = b"SCRAM-SHA-256\x00" + struct.pack("!I", len(client_first)) + client_first.encode()
        sock.sendall(wire.message(wire.PASSWORD, body))

        tag, payload = wire.read_message(sock, timeout=10.0)
        if tag != wire.AUTHENTICATION:
            raise StartupError("unexpected response to SASL initial response")
        server_first = payload[4:].decode()
        attrs = dict(kv.split("=", 1) for kv in server_first.split(","))
        salt = base64.b64decode(attrs["s"])
        iterations = int(attrs["i"])
        server_nonce = attrs["r"]
        if not server_nonce.startswith(client_nonce):
            raise StartupError("server nonce does not extend client nonce")

        salted = hashlib.pbkdf2_hmac("sha256", self.target.password.encode(), salt, iterations)
        client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
        stored_key = hashlib.sha256(client_key).digest()
        without_proof = f"c=biws,r={server_nonce}"
        auth_message = f"{client_first_bare},{server_first},{without_proof}".encode()
        client_sig = hmac.new(stored_key, auth_message, hashlib.sha256).digest()
        proof = base64.b64encode(bytes(a ^ b for a, b in zip(client_key, client_sig))).decode()
        sock.sendall(wire.message(wire.PASSWORD, f"{without_proof},p={proof}".encode()))


def md5_password(password: str, user: str, salt: bytes = b"") -> str:
    inner = hashlib.md5(password.encode() + user.encode()).hexdigest()
    return "md5" + hashlib.md5(inner.encode() + salt).hexdigest()


def check_client_password(method: str, supplied: str, config: PoolConfig) -> bool:
    expected = config.client_secret()
    if method == "trust":
        return True
    if expected is None:
        return False
    if method == "cleartext":
        return hmac.compare_digest(supplied, expected)
    return False
