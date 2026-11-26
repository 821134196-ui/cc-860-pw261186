"""Pool bookkeeping: backend slots, cancel routing, cleanup, metrics, events.

All shared state is guarded by one condition variable; network I/O is always
performed with the lock released.
"""

from __future__ import annotations

import collections
import itertools
import socket
import threading
import time
from typing import Any, Callable

from . import wire
from .config import PoolConfig, ServerConnector
from .types import Backend, Event, next_token, now

ErrorEmitter = Callable[[str, str, str, str], None]


class Pool:
    def __init__(self, config: PoolConfig) -> None:
        self.config = config
        self.cond = threading.Condition(threading.RLock())
        self.backends: list[Backend] = []
        self.clients: dict[int, dict[str, Any]] = {}
        self.queue: list[Any] = []
        self.events: collections.deque[Event] = collections.deque(maxlen=config.event_log_size)
        self.t0 = now()
        self._backend_ids = itertools.count(1)
        self._cancel_keys: dict[tuple[int, int], int] = {}
        self._pending_cancels: dict[int, int] = {}
        # cancel key -> backend id while that backend is being cleaned up
        self._draining_keys: dict[tuple[int, int], int] = {}
        self._cancel_key_seq = itertools.count(1)
        self._key_lock = threading.Lock()
        self.stats: dict[str, int] = collections.Counter(
            {
                "handshakes": 0,
                "checkouts": 0,
                "waits": 0,
                "checkout_timeouts": 0,
                "cancels_routed": 0,
                "cancels_late": 0,
                "cancels_ignored": 0,
                "resets": 0,
                "quarantines": 0,
                "dirty_backends": 0,
                "backends_opened": 0,
                "backends_closed_idle": 0,
                "clients_abandoned_in_queue": 0,
                "statements": 0,
            }
        )
        self.series: collections.deque[tuple[float, int, int, int]] = collections.deque(
            maxlen=config.series_points
        )
        self._connector = ServerConnector(config.server, config.application_name)
        self.timeline: collections.deque[dict[str, Any]] = collections.deque(maxlen=config.series_points)

    # ------------------------------------------------------------------ events

    def event(self, kind: str, actor: str, text: str, level: str = "info") -> None:
        with self.cond:
            self.events.append(Event(ts=now(), kind=kind, actor=actor, text=text, level=level))
            self.cond.notify_all()

    # ------------------------------------------------------------ cancel keys

    def allocate_cancel_key(self, session) -> tuple[int, int]:
        """Proxy-local key advertised to the client. It is never a real PID."""
        with self._key_lock:
            seq = next(self._cancel_key_seq)
        key = (990000 + seq, 0xC0FFEE00 + seq)
        with self.cond:
            self._cancel_keys[key] = session.client_id
        return key

    def release_cancel_key(self, key: tuple[int, int]) -> None:
        with self.cond:
            self._cancel_keys.pop(key, None)
            self._draining_keys.pop(key, None)

    def take_pending_cancel(self, backend_id: int) -> bool:
        """Consume a recorded late cancel for a backend during its cleanup."""
        with self.cond:
            pending = self._pending_cancels.get(backend_id, 0)
            if pending:
                pending -= 1
                if pending <= 0:
                    self._pending_cancels.pop(backend_id, None)
                else:
                    self._pending_cancels[backend_id] = pending
                return True
        return False

    def route_cancel(self, pid: int, secret: int, *, observer_client: int) -> None:
        """Handle a CancelRequest; deliver it to whichever backend the client
        currently owns. The cancel is best-effort and must never block."""
        delivered_while_draining = False
        with self.cond:
            key = (pid, secret)
            target = None
            # The client may already have released its backend; the key stays
            # mapped to the draining backend until cleanup finishes.
            draining_id = self._draining_keys.get(key)
            if draining_id is not None:
                probing = next((b for b in self.backends
                                if b.id == draining_id and b.state == "resetting"), None)
                if probing is not None:
                    target = probing
                    delivered_while_draining = True
            if target is None:
                owner = self._cancel_keys.get(key)
                info = (self.clients.get(owner) if owner is not None else None) or {}
                backend_id = info.get("backend_id")
                if backend_id is not None:
                    candidate = next((b for b in self.backends if b.id == backend_id), None)
                    if candidate is not None and candidate.state == "assigned":
                        target = candidate
                if target is None:
                    # Statement already finished and nothing is draining: the
                    # cancel is a genuine no-op (PostgreSQL would ignore it too).
                    self.stats["cancels_ignored"] += 1
        if target is None:
            self.event(
                "cancel", f"client {observer_client}",
                f"cancel for key {pid} dropped: the statement it targets has finished",
                "warn",
            )
            return
        if delivered_while_draining:
            with self.cond:
                self._pending_cancels[target.id] = self._pending_cancels.get(target.id, 0) + 1
                self.stats["cancels_late"] += 1
            self.event(
                "cancel", f"client {observer_client}",
                f"late cancel flagged for backend {target.id}; cleanup will absorb it",
                "warn",
            )
            return
        self.stats["cancels_routed"] += 1
        ok, detail = self._deliver_cancel(target)
        self.event(
            "cancel", f"client {observer_client}",
            f"cancel delivered to backend {target.id} (server pid {target.pid}) {detail}",
            "warn" if ok else "error",
        )

    def _deliver_cancel(self, backend: Backend) -> tuple[bool, str]:
        if backend.pid == 0:
            return False, "- backend had no pid"
        try:
            sock = self.config.server.connect_cancel_socket()
            try:
                sock.sendall(wire.cancel_request_packet(backend.pid, backend.secret))
                try:
                    sock.recv(1)
                except OSError:
                    pass
            finally:
                sock.close()
            return True, ""
        except OSError as exc:
            return False, f"({exc})"

    def cancel_backend(self, backend: Backend, *, origin: str, event: ErrorEmitter) -> None:
        ok, detail = self._deliver_cancel(backend)
        with self.cond:
            self.stats["cancels_routed"] += 1
        event(
            "cancel", f"backend {backend.id}",
            f"cancel from {origin} {detail}".strip(), "warn" if ok else "error",
        )

    # --------------------------------------------------------------- snapshots

    def sample(self) -> None:
        with self.cond:
            busy = sum(1 for b in self.backends if b.state in ("assigned", "resetting"))
            waiting = len(self.queue)
            self.series.append((now(), len(self.backends), busy, waiting))

    def snapshot(self) -> dict[str, Any]:
        with self.cond:
            t0 = self.t0
            backends = [b.snapshot(t0) for b in self.backends]
            clients = [
                {
                    "id": cid,
                    **{k: v for k, v in info.items() if k != "waiter"},
                    "waiting": info.get("state") == "waiting",
                }
                for cid, info in sorted(self.clients.items())
            ]
            queue = [w.snapshot(t0) for w in self.queue]
            events = [e.to_dict(t0) for e in self.events]
            series = [
                {"t": round(ts - t0, 3), "total": total, "busy": busy, "waiting": waiting}
                for ts, total, busy, waiting in self.series
            ]
            stats = {k: int(v) for k, v in self.stats.items()}
            occupied = sum(1 for b in self.backends if b.state == "assigned")
            resetting = sum(1 for b in self.backends if b.state == "resetting")
            pinned = sum(1 for b in self.backends if b.state == "assigned" and b.client_id is not None
                         and (self.clients.get(b.client_id) or {}).get("pinned"))
            return {
                "now": round(time.time(), 3),
                "uptime": round(now() - t0, 1),
                "config": {
                    "max_backends": self.config.max_backends,
                    "checkout_timeout": self.config.checkout_timeout,
                    "application_name": self.config.application_name,
                    "server": self.config.server.describe(),
                },
                "stats": {
                    **stats,
                    "active_backends": len(self.backends),
                    "in_use": occupied,
                    "resetting": resetting,
                    "idle": len(self.backends) - occupied - resetting,
                    "waiting": len(self.queue),
                    "clients": len(self.clients),
                    "pinned_backends": pinned,
                },
                "backends": backends,
                "clients": clients,
                "queue": queue,
                "events": events,
                "series": series,
            }

    # ---------------------------------------------------------- backend slots

    def _reserve_locked(self) -> Backend | None:
        if len(self.backends) >= self.config.max_backends:
            return None
        slot = Backend(id=next(self._backend_ids), pid=0, secret=0, sock=None, state="resetting")
        self.backends.append(slot)
        return slot

    def ensure_backend(self, *, reason: str = "grow") -> Backend | None:
        with self.cond:
            slot = self._reserve_locked()
        if slot is None:
            return None
        try:
            sock, pid, secret, _statuses = self._connector.connect()
        except Exception as exc:  # noqa: BLE001
            with self.cond:
                if slot in self.backends:
                    self.backends.remove(slot)
                self.cond.notify_all()
            self.event("pool", "system", f"backend open failed: {exc}", "error")
            return None
        with self.cond:
            slot.sock = sock
            slot.pid = pid
            slot.secret = secret
            slot.state = "free"
            self.stats["backends_opened"] += 1
            self.cond.notify_all()
        self.event("backend", f"backend {slot.id}",
                   f"opened server connection (server pid {pid}) -- {reason}", "good")
        return slot

    def prewarm(self) -> None:
        while True:
            with self.cond:
                if len(self.backends) >= max(self.config.min_backends, 1):
                    return
            if self.ensure_backend(reason="prewarm") is None:
                return

    def acquire(self, client_id: int, *, reason: str, abort: Callable[[], bool]) -> Backend | None:
        deadline = now() + self.config.checkout_timeout
        with self.cond:
            self.stats["waits"] += 1
            token = next_token()
            waiter = _Waiter(token=token, client_id=client_id, since=now(), reason=reason)
            self.queue.append(waiter)
            info = self.clients.setdefault(client_id, {})
            info["state"] = "waiting"
            info["queue_token"] = token
            self.event("queue", f"client {client_id}",
                       f"queued (token {token}); {len(self.queue)} waiting for a backend", "warn")
            failures = 0
            try:
                while True:
                    # Serve strictly in arrival order: only the head of the
                    # queue may take a free backend.
                    free = None
                    if self.queue and self.queue[0] is waiter:
                        free = next(
                            (b for b in self.backends if b.state == "free" and b.sock is not None),
                            None,
                        )
                    if free is not None:
                        self._drop_waiter_locked(waiter)
                        self._claim_locked(free, client_id)
                        return free
                    if abort():
                        self._drop_waiter_locked(waiter)
                        self.stats["clients_abandoned_in_queue"] += 1
                        return None
                    if len(self.backends) < self.config.max_backends:
                        self.cond.release()
                        try:
                            opened = self.ensure_backend(reason="grow on demand")
                        finally:
                            self.cond.acquire()
                        if opened is None:
                            # Do not spin: back off, the server may be unavailable.
                            failures += 1
                            if failures >= self.config.max_open_failures:
                                self._drop_waiter_locked(waiter)
                                self.stats["checkout_timeouts"] += 1
                                self.event("pool", f"client {client_id}",
                                           "cannot open a backend; server unreachable", "error")
                                return None
                            self.cond.wait(timeout=min(0.25 * failures, 2.0))
                        continue
                    remaining = deadline - now()
                    if remaining <= 0:
                        self._drop_waiter_locked(waiter)
                        self.stats["checkout_timeouts"] += 1
                        self.event("queue", f"client {client_id}",
                                   f"checkout timed out after {self.config.checkout_timeout:.0f}s "
                                   f"with {len(self.queue)} still queued", "error")
                        return None
                    self.cond.wait(timeout=min(remaining, 0.2))
            finally:
                info.pop("queue_token", None)

    def _claim_locked(self, backend: Backend, client_id: int) -> None:
        backend.state = "assigned"
        backend.client_id = client_id
        backend.generation += 1
        backend.checkout_count += 1
        backend.pinned_since = None
        backend.dirty_report = None
        backend.tx_status = "I"
        self.stats["checkouts"] += 1
        info = self.clients.setdefault(client_id, {})
        info.update({"backend_id": backend.id, "state": "active", "pinned": False, "pinned_reason": None})
        self.event("checkout", f"client {client_id}",
                   f"assigned backend {backend.id} (server pid {backend.pid}, "
                   f"generation {backend.generation})", "good")

    def _drop_waiter_locked(self, waiter) -> None:
        try:
            self.queue.remove(waiter)
        except ValueError:
            pass
        self.cond.notify_all()

    def mark_resetting(self, backend: Backend) -> None:
        with self.cond:
            backend.state = "resetting"
            backend.client_id = None
            self.cond.notify_all()

    def pin(self, backend: Backend, client_id: int, reason: str) -> None:
        with self.cond:
            backend.pinned_since = backend.pinned_since or now()
            info = self.clients.setdefault(client_id, {})
            info["state"] = "pinned"

    # ------------------------------------------------------- release / cleanup

    def release_backend(self, backend: Backend, *, reason: str, session, allow_reuse: bool = True,
                        finished: Callable[..., None] | None = None,
                        finished_args: tuple = (), cancel_key: tuple[int, int] | None = None) -> None:
        client_id = getattr(session, "client_id", None)
        """Cancel leftover work, clean the backend and hand it back (or drop it)."""
        with self.cond:
            backend.state = "resetting"
            backend.client_id = None
            if client_id is not None and client_id in self.clients:
                self.clients[client_id]["draining_backend_id"] = backend.id
            if cancel_key is not None:
                self._draining_keys[cancel_key] = backend.id
            self.cond.notify_all()
        threading.Thread(
            target=self._release_worker,
            args=(backend, reason, session, allow_reuse, client_id, finished, finished_args),
            name=f"release-{backend.id}",
            daemon=True,
        ).start()

    def _release_worker(self, backend: Backend, reason: str, session, allow_reuse: bool,
                        client_id: int | None, finished: Callable[..., None] | None = None,
                        finished_args: tuple = ()) -> None:
        try:
            try:
                report = self._clean_and_verify(backend, session, client_id)
            except Exception as exc:  # noqa: BLE001
                self._drop(backend, f"cleanup raised {type(exc).__name__}: {exc}")
                return
        finally:
            # Retire the client's cancel key only once cleanup is over, so a
            # cancel racing the cleanup can still be attributed to this backend.
            if finished is not None:
                try:
                    finished(*finished_args)
                except Exception:  # noqa: BLE001
                    pass
        if report is None:
            return
        if not allow_reuse:
            self._drop(backend, "not reusable")
            return
        with self.cond:
            backend.state = "free"
            backend.last_release = now()
            backend.pinned_since = None
            self._clear_draining_locked(client_id, backend.id)
            self.cond.notify_all()
        self.event("release", f"backend {backend.id}",
                   f"clean and returned to pool ({reason})", "good")

    def _clear_draining_locked(self, client_id: int | None, backend_id: int) -> None:
        if client_id is None:
            return
        info = self.clients.get(client_id)
        if info is not None and info.get("draining_backend_id") == backend_id:
            info.pop("draining_backend_id", None)

    def _clean_and_verify(self, backend: Backend, session, client_id: int | None = None) -> dict[str, str] | None:
        """Return the probe row on success, None if the backend was dropped."""
        from .session import PROBE_SQL, QueryFailure, run_query

        if backend.sock is None:
            self._drop(backend, "socket gone")
            return None
        hooks = self.config.hooks
        if hooks.cleanup_delay:
            time.sleep(hooks.cleanup_delay)

        # Barrier statements: absorb a cancel that arrived after the client's
        # last statement finished but before cleanup began.
        rounds = max(1, self.config.cancel_barrier_rounds)
        if self.take_pending_cancel(backend.id):
            rounds += 1
        for i in range(rounds):
            try:
                run_query(backend.sock, "SELECT 1", timeout=2.0)
                break
            except QueryFailure as exc:
                with self.cond:
                    self.stats["cancels_late"] += 1
                self.event("barrier", f"backend {backend.id}",
                           f"absorbed a late cancel on barrier {i + 1}: {exc.info.code}", "warn")
            except (OSError, wire.ProtocolError) as exc:
                self._drop(backend, f"backend unusable during drain: {exc}")
                return None
        else:
            self._drop(backend, "cancel barrier never settled")
            return None

        if hooks.cleanup_prefix_sql.strip():
            try:
                run_query(backend.sock, hooks.cleanup_prefix_sql, timeout=self.config.reset_timeout)
            except QueryFailure as exc:
                self.event("cleanup", f"backend {backend.id}",
                           f"cleanup prefix interrupted ({exc.info.code}); retrying", "warn")
            except (OSError, wire.ProtocolError) as exc:
                self._drop(backend, f"backend died during cleanup prefix: {exc}")
                return None

        row: dict[str, str] | None = None
        discard_error: str | None = None
        for attempt in range(2):
            if not hooks.cleanup_skip_discard:
                try:
                    result = run_query(backend.sock, "DISCARD ALL", timeout=self.config.reset_timeout)
                except QueryFailure as exc:
                    # A cancel or an aborted transaction landed during DISCARD.
                    discard_error = exc.info.code
                    self.event("cleanup", f"backend {backend.id}",
                               f"DISCARD ALL interrupted on attempt {attempt + 1} "
                               f"({exc.info.code} {exc.info.message}); retrying", "warn")
                    continue
                except (OSError, wire.ProtocolError) as exc:
                    self._drop(backend, f"backend died during DISCARD ALL: {exc}")
                    return None
            try:
                result = run_query(backend.sock, PROBE_SQL, timeout=self.config.reset_timeout)
                break
            except QueryFailure as exc:
                self._drop(backend, f"verification probe failed: {exc.info.code} {exc.info.message}")
                return None
            except (OSError, wire.ProtocolError) as exc:
                self._drop(backend, f"backend died during verification: {exc}")
                return None
        else:
            self._drop(backend, f"DISCARD ALL kept failing ({discard_error})")
            return None

        with self.cond:
            self.stats["resets"] += 1
        backend.reset_count += 1
        row = result.row
        dirty = {k: row.get(k, "0") for k in
                 ("temp_relations", "prepared", "channels", "advisory_locks",
                  "holdable_cursors", "session_gucs")
                 if row.get(k, "0") not in ("0", "", None)}
        if row.get("role") not in (None, "", "none"):
            dirty["role"] = row["role"]
        if row.get("current_user") != row.get("session_user"):
            dirty["current_user"] = f"{row.get('current_user')} != {row.get('session_user')}"
        if dirty:
            with self.cond:
                self.stats["dirty_backends"] += 1
            self._drop(backend, f"residue after cleanup: {_fmt_dirty(dirty)}", dirty)
            return None
        backend.dirty_report = None
        return row

    def _drop(self, backend: Backend, reason: str, report: dict[str, str] | None = None) -> None:
        with self.cond:
            backend.state = "broken"
            backend.client_id = None
            backend.destroy_reason = reason
            backend.dirty_report = report
            self.stats["quarantines"] += 1
            try:
                if backend.sock is not None:
                    backend.sock.close()
            except OSError:
                pass
            self.backends = [b for b in self.backends if b is not backend]
            self._pending_cancels.pop(backend.id, None)
            for info in self.clients.values():
                if info.get("draining_backend_id") == backend.id:
                    info.pop("draining_backend_id", None)
            self.cond.notify_all()
        self.event("backend", f"backend {backend.id}",
                   f"closed and removed: {reason}", "error")
        # Keep the pool at its floor even when nobody is queued right now.
        with self.cond:
            need = len(self.backends) < max(self.config.min_backends, 1)
        if need:
            threading.Thread(target=self.ensure_backend, kwargs={"reason": "refill"}, daemon=True).start()

    # ------------------------------------------------------------ client table

    def register_client(self, client_id: int, *, peer: str, user: str, database: str) -> None:
        with self.cond:
            self.clients[client_id] = {
                "id": client_id,
                "peer": peer,
                "user": user,
                "database": database,
                "state": "handshake",
                "backend_id": None,
                "pinned": False,
                "pinned_reason": None,
                "tx": "I",
                "since": round(now() - self.t0, 3),
                "queries": 0,
                "cancel_key": None,
            }

    def update_client(self, client_id: int, **fields: Any) -> None:
        with self.cond:
            if client_id in self.clients:
                self.clients[client_id].update(fields)

    def unregister_client(self, client_id: int) -> None:
        with self.cond:
            info = self.clients.pop(client_id, None)
            # Normally the release worker retires the cancel key; if the client
            # never owned a backend, retire it here.
            if info is not None and info.get("draining_backend_id") is None:
                for key, owner in list(self._cancel_keys.items()):
                    if owner == client_id:
                        self._cancel_keys.pop(key, None)
            self.cond.notify_all()

    def client_info(self, client_id: int) -> dict[str, Any] | None:
        with self.cond:
            info = self.clients.get(client_id)
            return dict(info) if info else None

    def set_backend_tx(self, backend: Backend, status: str) -> None:
        with self.cond:
            backend.tx_status = status
            if status in ("T", "E"):
                backend.pinned_since = backend.pinned_since or now()

    def set_pinned(self, backend: Backend, pinned: bool) -> None:
        """Track how long a backend has been held by one client's session state."""
        with self.cond:
            if pinned:
                backend.pinned_since = backend.pinned_since or now()
            elif backend.state == "assigned":
                backend.pinned_since = None


class _Waiter:
    __slots__ = ("token", "client_id", "since", "reason")

    def __init__(self, token: int, client_id: int, since: float, reason: str) -> None:
        self.token = token
        self.client_id = client_id
        self.since = since
        self.reason = reason

    def snapshot(self, t0: float) -> dict[str, Any]:
        return {
            "token": self.token,
            "client_id": self.client_id,
            "waited_ms": round((now() - self.since) * 1000),
            "reason": self.reason,
        }


def _fmt_dirty(dirty: dict[str, str]) -> str:
    return ", ".join(f"{k}={v}" for k, v in dirty.items())
