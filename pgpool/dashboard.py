"""HTTP + server-sent-events dashboard for the pool."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

STATIC = Path(__file__).parent / "static"


class Dashboard:
    def __init__(self, pool, *, interval: float = 0.5) -> None:
        self.pool = pool
        self.interval = interval
        self._stop = threading.Event()

    def snapshot_json(self) -> str:
        return json.dumps(self.pool.snapshot(), separators=(",", ":"))

    def serve(self, host: str, port: int) -> ThreadingHTTPServer:
        dashboard = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):  # noqa: D401 - quiet
                return

            def do_GET(self):  # noqa: N802
                path = self.path.split("?", 1)[0]
                if path in ("/", "/index.html"):
                    self._file(STATIC / "index.html", "text/html; charset=utf-8")
                elif path == "/api/state":
                    self._json(dashboard.snapshot_json())
                elif path == "/api/events":
                    self._stream()
                elif path.startswith("/assets/"):
                    name = path[len("/assets/"):]
                    target = (STATIC / name).resolve()
                    if STATIC.resolve() in target.parents and target.is_file():
                        self._file(target, _guessed_type(name))
                    else:
                        self.send_error(404)
                else:
                    self.send_error(404)

            def _file(self, target: Path, content_type: str) -> None:
                try:
                    data = target.read_bytes()
                except OSError:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(data)

            def _json(self, body: str) -> None:
                data = body.encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(data)

            def _stream(self) -> None:
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Connection", "keep-alive")
                self.end_headers()
                try:
                    while not dashboard._stop.is_set():
                        chunk = f"event: state\ndata: {dashboard.snapshot_json()}\n\n"
                        self.wfile.write(chunk.encode())
                        self.wfile.flush()
                        if dashboard._stop.wait(dashboard.interval):
                            break
                except (BrokenPipeError, ConnectionResetError, OSError):
                    return

        httpd = ThreadingHTTPServer((host, port), Handler)
        httpd.daemon_threads = True
        return httpd

    def stop(self) -> None:
        self._stop.set()


def _guessed_type(name: str) -> str:
    if name.endswith(".css"):
        return "text/css; charset=utf-8"
    if name.endswith(".js"):
        return "text/javascript; charset=utf-8"
    if name.endswith(".svg"):
        return "image/svg+xml"
    return "application/octet-stream"
