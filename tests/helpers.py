"""Test helpers: a raw-protocol client and a per-test proxy fixture."""

from __future__ import annotations

import socket
import struct
import threading
import time
from dataclasses import dataclass

from pgpool import wire
from pgpool.config import PoolConfig, ServerTarget, TestHooks
from pgpool.server import ProxyServer


@dataclass
class Reply:
    messages: list[tuple[bytes, bytes]]

    def errors(self) -> list[wire.ErrorInfo]:
        return [wire.parse_error_fields(pl) for t, pl in self.messages if t == wire.ERROR_RESPONSE]

    def error_codes(self) -> list[str]:
        return [e.code for e in self.errors()]

    def rows(self) -> list[list[str | None]]:
        out: list[list[str | None]] = []
        for tag, payload in self.messages:
            if tag != wire.DATA_ROW:
                continue
            count = struct.unpack("!H", payload[:2])[0]
            off = 2
            row: list[str | None] = []
            for _ in range(count):
                length = struct.unpack("!i", payload[off:off + 4])[0]
                off += 4
                if length < 0:
                    row.append(None)
                else:
                    row.append(payload[off:off + length].decode("utf-8", "replace"))
                    off += length
            out.append(row)
        return out

    def first(self) -> str | None:
        rows = self.rows()
        return rows[0][0] if rows else None

    @property
    def tx_status(self) -> str:
        payload = self.messages[-1][1]
        return payload[:1].decode() if payload else "I"


class RawClient:
    """Speaks the v3 protocol just far enough to drive the proxy."""

    def __init__(self, host: str, port: int, *, user: str = "postgres",
                 database: str = "postgres", timeout: float = 10.0) -> None:
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.port = port
        self.key: tuple[int, int] | None = None
        self.startup = self._recv_until_ready(
            wire.startup_packet({"user": user, "database": database})
        )

    # ---------------------------------------------------------------- plumbing

    def _recv_until_ready(self, initial: bytes = b"") -> list[tuple[bytes, bytes]]:
        if initial:
            self.sock.sendall(initial)
        messages: list[tuple[bytes, bytes]] = []
        while True:
            tag, payload = wire.read_message(self.sock)
            messages.append((tag, payload))
            if tag == wire.BACKEND_KEY:
                self.key = struct.unpack("!II", payload)
            if tag == wire.READY_FOR_QUERY:
                return messages

    def send(self, sql: str) -> None:
        self.sock.sendall(wire.query_message(sql))

    def collect(self) -> Reply:
        return Reply(self._recv_until_ready())

    def query(self, sql: str) -> Reply:
        self.send(sql)
        return self.collect()

    def _send_cancel_request(self) -> None:
        assert self.key is not None
        cancel = socket.create_connection(("127.0.0.1", self.port), timeout=5)
        try:
            cancel.sendall(wire.cancel_request_packet(*self.key))
        finally:
            cancel.close()

    @property
    def peer(self) -> str:
        """Our own address as the proxy sees it (it records it on the client)."""
        host, port = self.sock.getsockname()[:2]
        return f"{host}:{port}"

    # ------------------------------------------------------------------ sugar

    def scalar(self, sql: str) -> str | None:
        reply = self.query(sql)
        if reply.errors():
            raise AssertionError(f"{sql!r} failed: {reply.error_codes()}")
        return reply.first()

    def ok(self, sql: str) -> Reply:
        reply = self.query(sql)
        assert not reply.errors(), f"{sql!r} -> {reply.errors()[0].message}"
        return reply

    def cancel(self) -> None:
        """Cancel *after* the statement has finished (the race we care about)."""
        self._send_cancel_request()

    def abort(self) -> None:
        """Hard reset, so the proxy cannot mistake it for a graceful Terminate."""
        try:
            self.sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
            )
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass

    def close(self) -> None:
        try:
            self.sock.sendall(wire.terminate())
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


class PoolHarness:
    """A running proxy on a random port, plus convenience accessors."""

    def __init__(self, target: ServerTarget, **overrides) -> None:
        defaults = dict(max_backends=2, min_backends=1, checkout_timeout=6.0, server=target)
        defaults.update({k: v for k, v in overrides.items() if v is not None})
        self.config = PoolConfig(**defaults)
        self.server = ProxyServer(self.config)
        self.host, self.port = self.server.bind("127.0.0.1", 0)
        self.server.start()
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()

    def client(self, **kwargs) -> RawClient:
        return RawClient(self.host, self.port, **kwargs)

    def dsn(self) -> str:
        return f"postgresql://postgres@127.0.0.1:{self.port}/postgres"

    def stats(self) -> dict:
        return self.server.pool.snapshot()["stats"]

    def snapshot(self) -> dict:
        return self.server.pool.snapshot()

    def wait_for(self, predicate, timeout: float = 10.0, interval: float = 0.02):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            last = self.snapshot()
            if predicate(last):
                return last
            time.sleep(interval)
        raise AssertionError(f"condition not met in {timeout}s; last state: {last}")

    def wait_idle(self, timeout: float = 10.0) -> dict:
        """Wait until every backend is free and nobody is queued."""
        return self.wait_for(
            lambda s: bool(s["backends"]) and not s["queue"]
            and all(b["state"] == "free" for b in s["backends"]),
            timeout=timeout,
        )

    def close(self) -> None:
        self.server.shutdown()


def harness_for(target: ServerTarget, **overrides) -> PoolHarness:
    return PoolHarness(target, **overrides)


def residue_report(client: RawClient) -> dict[str, str]:
    """Everything a fresh client could have inherited from a previous session."""
    report: dict[str, str] = {}
    temp = client.scalar(
        "select count(*) from pg_class c join pg_namespace n on n.oid = c.relnamespace "
        "where n.nspname like 'pg_temp%'"
    )
    if temp != "0":
        report["temp_relations"] = str(temp)
    prepared = client.scalar("select count(*) from pg_prepared_statements")
    if prepared != "0":
        report["prepared"] = str(prepared)
    channels = client.scalar("select count(*) from pg_listening_channels()")
    if channels != "0":
        report["channels"] = str(channels)
    locks = client.scalar("select count(*) from pg_locks where locktype = 'advisory'")
    if locks != "0":
        report["advisory_locks"] = str(locks)
    app = client.scalar("show application_name")
    if app not in ("pgpool", ""):
        report["application_name"] = str(app)
    role = client.scalar("select current_setting('role')")
    if role not in ("none", None):
        report["role"] = str(role)
    user_pair = client.query("select current_user, session_user").rows()
    if user_pair and user_pair[0][0] != user_pair[0][1]:
        report["role_switch"] = f"{user_pair[0][0]} != {user_pair[0][1]}"
    return report


__all__ = [
    "RawClient",
    "Reply",
    "PoolHarness",
    "harness_for",
    "residue_report",
    "TestHooks",
]
