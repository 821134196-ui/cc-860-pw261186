"""Listener + event loop, and the CLI entry point."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import socket
import sys
import threading
import time
from typing import Any

from . import wire
from .config import PoolConfig, ServerTarget, TestHooks
from .dashboard import Dashboard
from .pool import Pool
from .session import Session

log = logging.getLogger("pgpool")


class ProxyServer:
    """TCP or Unix listener spawning one Session thread per client."""

    def __init__(self, config: PoolConfig) -> None:
        self.config = config
        self.pool = Pool(config)
        self._listener: socket.socket | None = None
        self._stop = threading.Event()
        self._sampler: threading.Thread | None = None

    def bind(self, host: str = "127.0.0.1", port: int = 0) -> tuple[Any, int]:
        if host.startswith("/") or host == "unix":
            path = host if host.startswith("/") else f"/tmp/.s.PGSQL.pgpool-{os.getpid()}"
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            listener.bind(path)
            self._unix_path = path
            bind_host = path
        else:
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((host, port))
            self._unix_path = None
            bind_host = listener.getsockname()[0]
        listener.listen(128)
        self._listener = listener
        return bind_host, listener.getsockname()[1]

    @property
    def address(self) -> tuple[Any, int] | None:
        if self._listener is None:
            return None
        return self._listener.getsockname()

    def start(self) -> None:
        self.pool.prewarm()
        self._sampler = threading.Thread(target=self._sample_loop, daemon=True, name="sampler")
        self._sampler.start()

    def _sample_loop(self) -> None:
        while not self._stop.wait(0.5):
            self.pool.sample()
            self._reap_idle()

    def _reap_idle(self) -> None:
        cfg = self.config
        if cfg.idle_backend_timeout <= 0:
            return
        with self.pool.cond:
            if self.pool.queue:
                return
            free = [b for b in self.pool.backends if b.state == "free"]
            if len(self.pool.backends) - len(free) < 1:
                return
            stale = [b for b in free if b.last_release and (time.monotonic() - b.last_release) > cfg.idle_backend_timeout]
            if not stale:
                return
            victim = stale[0]
            self.pool.backends.remove(victim)
            self.pool.stats["backends_closed_idle"] += 1
            self.pool.cond.notify_all()
        try:
            if victim.sock is not None:
                victim.sock.sendall(wire.terminate())
                victim.sock.close()
        except OSError:
            pass
        self.pool.event("backend", f"backend {victim.id}", "closed idle backend", "info")

    def serve_forever(self) -> None:
        assert self._listener is not None
        self._listener.settimeout(0.5)
        while not self._stop.is_set():
            try:
                conn, addr = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                if self._stop.is_set():
                    return
                continue
            conn.setblocking(True)
            thread = threading.Thread(
                target=self._serve_client, args=(conn, addr), daemon=True,
                name=f"client-{addr}",
            )
            thread.start()

    def _serve_client(self, conn: socket.socket, addr: Any) -> None:
        Session(conn, addr, self.pool, self.config).run()

    def shutdown(self) -> None:
        self._stop.set()
        if self._listener is not None:
            try:
                self._listener.close()
            except OSError:
                pass
        with self.pool.cond:
            backends = list(self.pool.backends)
            self.pool.backends.clear()
        for backend in backends:
            try:
                if backend.sock is not None:
                    backend.sock.sendall(wire.terminate())
                    backend.sock.close()
            except OSError:
                pass
        if getattr(self, "_unix_path", None):
            try:
                os.unlink(self._unix_path)
            except OSError:
                pass


def build_config(args: argparse.Namespace) -> PoolConfig:
    hooks = TestHooks(
        cleanup_prefix_sql=args.cleanup_prefix_sql,
        cleanup_skip_discard=args.cleanup_skip_discard,
        cleanup_delay=args.cleanup_delay,
        checkout_delay=args.checkout_delay,
    )
    target = ServerTarget.from_dsn(args.server)
    return PoolConfig(
        max_backends=args.max_backends,
        checkout_timeout=args.checkout_timeout,
        reset_timeout=args.reset_timeout,
        idle_backend_timeout=args.idle_timeout,
        application_name=args.application_name,
        server=target,
        hooks=hooks,
        client_auth=args.client_auth,
        client_password=args.client_password,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Restricted PostgreSQL connection-pool proxy")
    parser.add_argument("--server", required=True, help="upstream DSN, e.g. postgresql://user@host:5432/db")
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", type=int, default=6432)
    parser.add_argument("--max-backends", type=int, default=4)
    parser.add_argument("--checkout-timeout", type=float, default=30.0)
    parser.add_argument("--reset-timeout", type=float, default=10.0)
    parser.add_argument("--idle-timeout", type=float, default=600.0)
    parser.add_argument("--application-name", default="pgpool")
    parser.add_argument("--client-auth", choices=["trust", "cleartext"], default="trust")
    parser.add_argument("--client-password")
    parser.add_argument("--dashboard-host", default="127.0.0.1")
    parser.add_argument("--dashboard-port", type=int, default=8080)
    parser.add_argument("--no-dashboard", action="store_true")
    parser.add_argument("--cleanup-prefix-sql", default="", help="demo/test hook")
    parser.add_argument("--cleanup-delay", type=float, default=0.0, help="demo/test hook")
    parser.add_argument("--cleanup-skip-discard", action="store_true", help="demo/test hook")
    parser.add_argument("--checkout-delay", type=float, default=0.0, help="demo/test hook")
    parser.add_argument("--log-level", default="info")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )

    config = build_config(args)
    server = ProxyServer(config)
    host, port = server.bind(args.listen_host, args.listen_port)
    server.start()
    log.info("pool proxy listening on %s:%s -> %s", host, port, config.server.describe())

    dashboard_httpd = None
    if not args.no_dashboard:
        dashboard = Dashboard(server.pool)
        dashboard_httpd = dashboard.serve(args.dashboard_host, args.dashboard_port)
        threading.Thread(target=dashboard_httpd.serve_forever, daemon=True, name="dashboard").start()
        log.info("dashboard on http://%s:%s/", args.dashboard_host, args.dashboard_port)

    stop = threading.Event()

    def _handle_signal(signum, _frame):
        log.info("received signal %s, shutting down", signum)
        stop.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    thread = threading.Thread(target=server.serve_forever, daemon=True, name="listener")
    thread.start()
    while not stop.is_set():
        stop.wait(0.5)
    if dashboard_httpd is not None:
        dashboard_httpd.shutdown()
    server.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
