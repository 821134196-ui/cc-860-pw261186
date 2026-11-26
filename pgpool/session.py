"""Client sessions, backend routing and the cancel/cleanup races.

The proxy terminates the client startup handshake (so it can hand each client
its own BackendKeyData) and relays everything else. Simple queries are parsed
enough to know when a backend must stay pinned; extended-protocol traffic is
relayed verbatim, but Parse messages are still inspected.

Routing rules
-------------
* Pinning happens as soon as a client opens a transaction, creates a temp
  table, runs a session-level SET / RESET / LISTEN / advisory lock, declares a
  holdable cursor, or creates a *named* prepared statement. While pinned the
  client always talks to the same backend, and the backend is not returned to
  the pool.
* An unpinned client is transaction-pooled: after each ReadyForQuery that
  leaves the session idle, the backend goes back to the pool. The next
  statement may land on a different backend.
* Every return runs `DISCARD ALL` and then a verification probe. If the probe
  still sees residue (a cleanup that was interrupted), the backend is
  quarantined rather than reused.
* A disconnect while a statement is running sends a PostgreSQL cancel, drains
  the backend to ReadyForQuery, then runs the same cleanup path. A cancel that
  arrives late is absorbed by barrier statements before cleanup, and a cancel
  that lands *inside* cleanup causes a retry, not a dirty hand-off.
"""

from __future__ import annotations

import logging
import select
import socket
import struct
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

from . import wire
from .classify import analyze
from .config import PoolConfig, check_client_password
from .pool import Pool
from .types import Backend, next_client_id, now

log = logging.getLogger("pgpool.session")

PROBE_SQL = (
    "SELECT "
    "(SELECT count(*) FROM pg_catalog.pg_class c "
    " JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
    " WHERE n.nspname LIKE 'pg_temp\\_%' OR n.nspname LIKE 'pg_toast_temp\\_%')::text AS temp_relations, "
    "(SELECT count(*) FROM pg_catalog.pg_prepared_statements)::text AS prepared, "
    "(SELECT count(*) FROM pg_catalog.pg_listening_channels())::text AS channels, "
    "(SELECT count(*) FROM pg_catalog.pg_locks WHERE locktype = 'advisory' "
    " AND pid = pg_catalog.pg_backend_pid())::text AS advisory_locks, "
    "(SELECT count(*) FROM pg_catalog.pg_cursors WHERE is_holdable)::text AS holdable_cursors, "
    "(SELECT count(*) FROM pg_catalog.pg_settings "
    " WHERE source = 'session' AND name <> 'application_name')::text AS session_gucs, "
    "current_setting('role') AS role, "
    "current_user::text AS current_user, "
    "session_user::text AS session_user, "
    "current_setting('transaction_read_only') AS tx_read_only, "
    "current_setting('is_superuser') AS is_superuser, "
    "pg_catalog.pg_backend_pid()::text AS pid"
)

DIRTY_KEYS = (
    "temp_relations",
    "prepared",
    "channels",
    "advisory_locks",
    "holdable_cursors",
    "session_gucs",
)


class ClientGone(Exception):
    """The frontend socket closed while its statement was running."""


class Session:
    def __init__(self, sock: socket.socket, addr: Any, pool: Pool, config: PoolConfig) -> None:
        self.sock = sock
        self.addr = addr
        self.pool = pool
        self.config = config
        self.client_id = next_client_id()
        self.backend: Backend | None = None
        self.pins: set[str] = set()
        self.tx_status = "I"
        self.params: dict[str, str] = {}
        self.user = "?"
        self.database = "?"
        self.closed = False
        self.client_gone = False
        self.in_extended = False
        self.sent_lost = False
        self.cancel_key = (0, 0)
        self.statements = 0
        self.last_reason: str | None = None
        peer = f"{addr[0]}:{addr[1]}" if isinstance(addr, tuple) and len(addr) >= 2 else str(addr)
        self.pool.register_client(self.client_id, peer=peer, user="?", database="?")
        self.pool.event("connect", f"client {self.client_id}", f"accepted from {peer}")

    # ------------------------------------------------------------------ setup

    @property
    def pinned(self) -> bool:
        return bool(self.pins) or self.tx_status in ("T", "E")

    def run(self) -> None:
        try:
            if not self._handshake():
                return
            self._main_loop()
        except (wire.ConnectionClosed, ConnectionResetError):
            log.debug("client %s disconnected", self.client_id)
        except Exception as exc:  # noqa: BLE001 - never let one session kill the server
            log.exception("session %s failed", self.client_id)
            self.pool.event("error", f"client {self.client_id}", f"session error: {exc}", "error")
        finally:
            self.close()

    def _handshake(self) -> bool:
        while True:
            try:
                length = struct.unpack("!I", wire.read_exact(self.sock, 4))[0]
            except wire.ConnectionClosed:
                return False
            if length < 8 or length > wire.MAX_MESSAGE_LENGTH:
                return False
            body = wire.read_exact(self.sock, length - 4)
            code = struct.unpack("!I", body[:4])[0]
            if code in (wire.SSL_REQUEST_CODE, wire.GSSENC_REQUEST_CODE):
                self.sock.sendall(b"N")  # plaintext-only proxy
                continue
            if code == wire.CANCEL_REQUEST_CODE:
                pid, secret = struct.unpack("!II", body[4:12])
                self.pool.route_cancel(pid, secret, observer_client=self.client_id)
                return False
            if code != wire.PROTOCOL_VERSION:
                self._send_fatal("08P01", "unsupported frontend protocol")
                return False
            self.params = _parse_startup_params(body[4:])
            break

        self.user = self.params.get("user", "postgres")
        self.database = self.params.get("database", self.user)
        self.pool.update_client(self.client_id, user=self.user, database=self.database)

        if self.config.client_auth != "trust" and not self._client_auth():
            return False

        self.cancel_key = self.pool.allocate_cancel_key(self)
        self.pool.update_client(self.client_id, cancel_key=f"{self.cancel_key[0]}/{self.cancel_key[1]}")
        self.sock.sendall(wire.auth_ok())
        self.sock.sendall(wire.backend_key(*self.cancel_key))
        for name, value in self._startup_parameters():
            self.sock.sendall(wire.parameter_status(name, value))
        self.sock.sendall(wire.ready_for_query(b"I"))
        self.pool.stats["handshakes"] += 1
        self.pool.event(
            "auth", f"client {self.client_id}",
            f"handshake complete as {self.user}/{self.database}",
            "good",
        )
        return True

    def _startup_parameters(self) -> list[tuple[str, str]]:
        server_version = "16.0"
        return [
            ("server_version", server_version),
            ("server_encoding", "UTF8"),
            ("client_encoding", self.params.get("client_encoding", "UTF8")),
            ("DateStyle", "ISO, MDY"),
            ("integer_datetimes", "on"),
            ("standard_conforming_strings", "on"),
            ("TimeZone", "UTC"),
            ("is_superuser", "off"),
        ]

    def _client_auth(self) -> bool:
        if self.config.client_auth == "cleartext":
            self.sock.sendall(wire.message(wire.AUTHENTICATION, struct.pack("!I", 3)))
            tag, payload = wire.read_message(self.sock, timeout=10)
            if tag is None or tag != wire.PASSWORD:
                self._send_fatal("28P01", "password required")
                return False
            supplied = payload.split(b"\x00", 1)[0].decode("utf-8", "replace")
            if not check_client_password("cleartext", supplied, self.config):
                self._send_fatal("28P01", "password authentication failed", severity="FATAL")
                return False
        return True

    # ------------------------------------------------------------- main relay

    def _main_loop(self) -> None:
        while not self.closed:
            tag, payload = wire.read_message(self.sock)
            if tag is None:
                self.pool.event("disconnect", f"client {self.client_id}",
                                "peer closed the connection", "info")
                return
            if tag == wire.TERMINATE:
                self.pool.event("disconnect", f"client {self.client_id}", "sent Terminate", "info")
                return
            if tag == wire.QUERY:
                sql = payload.split(b"\x00", 1)[0].decode("utf-8", "replace")
                self._simple_query(sql)
                continue
            # Anything else belongs to the extended protocol and needs a backend
            # for the whole message stream (Parse .. Sync).
            if self.backend is None and not self._ensure_backend():
                return
            if tag == wire.PARSE:
                self._note_parse(payload)
            self._relay(tag, payload)
            if tag == wire.SYNC:
                self._after_sync()
            if self.client_gone:
                return

    # ------------------------------------------------------- state tracking

    def _note_parse(self, payload: bytes) -> None:
        """Inspect a Parse for SQL that needs a pinned backend."""
        name, pos = wire.split_cstrs(payload, 1)
        end = payload.find(b"\x00", pos)
        if end < 0:
            return
        sql = payload[pos:end].decode("utf-8", "replace")
        statement_name = name[0].decode("utf-8", "replace")
        result = analyze(sql)
        changed = self._apply_analysis(result)
        if statement_name:
            self._add_pin("named prepared statement")
            changed = True
        self.in_extended = True
        if changed:
            self.pool.event(
                "pin", f"client {self.client_id}",
                f"pinned by Parse: {', '.join(sorted(self.pins)) or 'transaction'}",
                "warn",
            )

    def _apply_analysis(self, result) -> bool:
        """Update the pin set from a classified statement.

        A transaction pins by itself (the backend's tx status does that); other
        session state gets a named pin. Additive only: a later statement in the
        same simple-query string can undo nothing already applied.
        """
        changed = False
        if result.opens_transaction:
            self.tx_status = "T"
        if result.touches_session and not result.opens_transaction and result.reason:
            self._add_pin(result.reason)
            changed = True
        return changed

    def _add_pin(self, reason: str) -> None:
        if reason not in self.pins:
            self.pins.add(reason)
            if self.backend is not None:
                self.pool.pin(self.backend, self.client_id, reason)

    def _simple_query(self, sql: str) -> None:
        result = analyze(sql)
        self.statements += 1
        self._apply_analysis(result)
        self.last_reason = result.reason
        self.pool.update_client(
            self.client_id,
            queries=self.statements,
            pinned=bool(self.pins),
            pinned_reason=", ".join(sorted(self.pins)) or None,
            state="pinned" if self.pins else ("in_txn" if self.tx_status == "T" else "active"),
        )
        if self.pins:
            self.pool.event(
                "pin", f"client {self.client_id}",
                f"pinned: {', '.join(sorted(self.pins))}",
                "warn",
            )
        if not self._ensure_backend():
            return
        try:
            self._relay(wire.QUERY, sql.encode("utf-8") + b"\x00")
            status = self._drain_ready(forward=True)
        except ClientGone:
            self._drain_after_client_gone()
            return
        self._after_ready(status)

    def _after_sync(self) -> None:
        self.in_extended = False
        try:
            status = self._drain_ready(forward=True)
        except ClientGone:
            self._drain_after_client_gone()
            return
        self._after_ready(status)

    def _after_ready(self, status: str) -> None:
        self.tx_status = status
        if self.backend is not None:
            self.pool.set_backend_tx(self.backend, status)
            self.pool.set_pinned(self.backend, self.pinned)
        if status == "I" and self.pins:
            self.pool.event("unpin", f"client {self.client_id}",
                            f"transaction ended but session state remains pinned: "
                            f"{', '.join(sorted(self.pins))}", "info")
        self.pool.update_client(
            self.client_id,
            tx=status,
            pinned=bool(self.pins),
            pinned_reason=", ".join(sorted(self.pins)) or None,
            state="pinned" if self.pins else ("in_txn" if status == "T" else "active"),
        )
        if status == "I" and not self.pins:
            self._return_backend("session idle and unpinned")

    # --------------------------------------------------------- backend access

    def _ensure_backend(self) -> bool:
        if self.backend is not None and self.backend.state == "assigned":
            return True
        self.backend = None
        backend = self.pool.acquire(
            self.client_id,
            reason="checkout",
            abort=self._client_aborted,
        )
        if backend is None:
            if self._client_aborted():
                self.pool.event("queue", f"client {self.client_id}",
                                "left the queue: client disconnected", "info")
                self.client_gone = True
                return False
            self._send_fatal(
                "53300",
                "timed out waiting for a pooled connection",
                severity="FATAL",
                hint="the pool is saturated; retry later",
            )
            self.pool.event("queue", f"client {self.client_id}",
                            "checkout timed out; connection closed", "error")
            return False
        self.backend = backend
        if self.pins:
            self.pool.pin(backend, self.client_id, ", ".join(sorted(self.pins)))
        return True

    def _client_aborted(self) -> bool:
        """Non-blocking check: has the frontend gone away while we wait?"""
        try:
            readable, _, _ = select.select([self.sock], [], [], 0)
        except (OSError, ValueError):
            return True
        if not readable:
            return False
        try:
            data = self.sock.recv(1, socket.MSG_PEEK)
        except (BlockingIOError, InterruptedError):
            return False
        except OSError:
            return True
        if data == b"":
            return True
        return False

    def _relay(self, tag: bytes, payload: bytes) -> None:
        if self.sent_lost:
            raise wire.ConnectionClosed("frontend is gone")
        backend = self.backend
        if backend is None or backend.sock is None:
            raise wire.ConnectionClosed("no backend assigned")
        backend.sock.sendall(wire.message(tag, payload))

    def _drain_ready(self, *, forward: bool, watch_client: bool = True) -> str:
        """Forward backend messages to the client until ReadyForQuery.

        While the client is still attached we also watch the frontend socket;
        if it disappears we raise ClientGone so the caller can cancel the
        server side and drain it. Once the client is gone there is nothing to
        watch, so the drain continues on the backend socket alone.
        """
        backend = self.backend
        assert backend is not None and backend.sock is not None
        bsock = backend.sock
        watch = [bsock, self.sock] if watch_client else [bsock]
        while True:
            try:
                readable, _, _ = select.select(watch, [], [], self.config.relay_poll_interval)
            except (OSError, ValueError):
                raise wire.ConnectionClosed("socket closed during relay")
            if bsock in readable:
                tag, payload = wire.read_message(bsock)
                if tag is None:
                    raise wire.ConnectionClosed("backend closed mid-statement")
                if forward and not self.sent_lost:
                    try:
                        self.sock.sendall(wire.message(tag, payload))
                    except OSError:
                        self.sent_lost = True
                        raise ClientGone()
                if tag == wire.READY_FOR_QUERY:
                    return payload[:1].decode() if payload else "I"
                continue
            if watch_client and self.sock in readable:
                try:
                    data = self.sock.recv(1, socket.MSG_PEEK)
                except (BlockingIOError, InterruptedError):
                    continue
                except OSError:
                    data = b""
                if data == b"":
                    raise ClientGone()
                # The client pipelined a message; leave it buffered and keep
                # waiting for the backend to finish this statement.
                time.sleep(0.005)

    def _drain_after_client_gone(self) -> None:
        """Client vanished mid-statement: cancel the work and settle the backend."""
        backend = self.backend
        self.sent_lost = True
        self.client_gone = True
        try:
            self.sock.close()
        except OSError:
            pass
        if backend is None or backend.sock is None:
            return
        self.pool.mark_resetting(backend)
        self.pool.event("disconnect", f"client {self.client_id}",
                        f"client vanished mid-statement; cancelling backend {backend.id}", "warn")
        self.pool.cancel_backend(backend, origin=f"client {self.client_id} disconnect", event=self.pool.event)
        try:
            status = self._drain_ready(forward=False, watch_client=False)
        except (OSError, wire.ProtocolError):
            self.pool._drop(backend, "backend unresponsive after client vanished")
            self.backend = None
            return
        self.tx_status = status
        self.pool.set_backend_tx(backend, status)

    def _return_backend(self, reason: str) -> None:
        backend = self.backend
        if backend is None:
            return
        self.backend = None
        self.pool.update_client(self.client_id, backend_id=None, lifted_generation=backend.generation)
        self.pool.release_backend(backend, reason=reason, session=self)

    # ---------------------------------------------------------------- cancel

    def current_backend(self) -> Backend | None:
        return self.backend

    # -------------------------------------------------------------- shutdown

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        backend = self.backend
        self.backend = None
        try:
            self.sock.close()
        except OSError:
            pass
        if backend is not None:
            self.pool.update_client(self.client_id, state="draining", backend_id=backend.id)
            self.pool.event(
                "disconnect", f"client {self.client_id}",
                f"client gone; cleaning backend {backend.id} before returning it",
                "warn",
            )
            self.pool.release_backend(
                backend, reason="client disconnected", session=self, allow_reuse=True,
                finished=self.pool.release_cancel_key, finished_args=(self.cancel_key,),
                cancel_key=self.cancel_key,
            )
        self.pool.unregister_client(self.client_id)

    def _send_fatal(self, code: str, text: str, *, severity: str = "ERROR", hint: str | None = None) -> None:
        try:
            self.sock.sendall(wire.error_response(severity, code, text, hint=hint))
            self.sock.sendall(wire.ready_for_query(b"I"))
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Shared helpers used by Pool when it runs the release / cancel path.
# ---------------------------------------------------------------------------


def _normalise_reason(reason: str) -> str:
    r = reason.strip().lower()
    if r.startswith("begin") or r.startswith("start transaction"):
        return "transaction"
    if r.startswith("commit") or r.startswith("rollback") or r.startswith("end"):
        return "transaction"
    if r.startswith("create") and "temp" in r:
        return "temp table"
    if r.startswith("set role") or r.startswith("reset role"):
        return "role"
    if r.startswith("set session authorization"):
        return "role"
    if r.startswith("listen"):
        return "listen"
    if r.startswith("prepare"):
        return "prepared statement"
    if r.startswith("select pg_advisory") or r.startswith("select pg_try_advisory"):
        return "advisory lock"
    if r.startswith("declare"):
        return "holdable cursor"
    if r.startswith("set") or r.startswith("reset"):
        return "session setting"
    return "session state"


@dataclass
class QueryResult:
    columns: list[str]
    row: dict[str, str]
    status: str


class QueryFailure(Exception):
    def __init__(self, info: wire.ErrorInfo, status: str) -> None:
        super().__init__(str(info))
        self.info = info
        self.status = status


def run_query(sock: socket.socket, sql: str, *, timeout: float) -> QueryResult:
    """Run one simple query on a backend, returning the first row as a dict."""
    sock.settimeout(timeout)
    try:
        sock.sendall(wire.query_message(sql))
        columns: list[str] = []
        row: dict[str, str] = {}
        error: wire.ErrorInfo | None = None
        status = "I"
        while True:
            tag, payload = wire.read_message(sock)
            if tag is None:
                raise wire.ConnectionClosed("backend closed during query")
            if tag == wire.ROW_DESCRIPTION:
                columns = _column_names(payload)
            elif tag == wire.DATA_ROW:
                row = _first_row(payload, columns)
            elif tag == wire.ERROR_RESPONSE:
                error = wire.parse_error_fields(payload)
            elif tag == wire.READY_FOR_QUERY:
                status = payload[:1].decode() if payload else "I"
                if error is not None:
                    raise QueryFailure(error, status)
                return QueryResult(columns, row, status)
    finally:
        sock.settimeout(None)


def _column_names(payload: bytes) -> list[str]:
    count = struct.unpack("!H", payload[:2])[0]
    pos = 2
    names = []
    for _ in range(count):
        end = payload.find(b"\x00", pos)
        names.append(payload[pos:end].decode("utf-8", "replace"))
        pos = end + 1 + 18
    return names


def _first_row(payload: bytes, columns: list[str]) -> dict[str, str]:
    count = struct.unpack("!H", payload[:2])[0]
    pos = 2
    row: dict[str, str] = {}
    for idx in range(count):
        length = struct.unpack("!i", payload[pos : pos + 4])[0]
        pos += 4
        if length < 0:
            value = ""
        else:
            value = payload[pos : pos + length].decode("utf-8", "replace")
            pos += length
        name = columns[idx] if idx < len(columns) else f"col{idx}"
        row[name] = value
    return row


def _parse_startup_params(body: bytes) -> dict[str, str]:
    parts = body.split(b"\x00")
    out: dict[str, str] = {}
    it = iter(parts)
    for key in it:
        if not key:
            break
        value = next(it, b"")
        out[key.decode("utf-8", "replace")] = value.decode("utf-8", "replace")
    return out
