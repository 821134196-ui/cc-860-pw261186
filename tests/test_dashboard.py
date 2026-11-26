"""The dashboard is the observability surface, so it gets its own tests."""

from __future__ import annotations

import json
import threading
import urllib.request
import uuid

import pytest

from helpers import harness_for, residue_report


@pytest.fixture()
def dashboard(pool):
    from pgpool.dashboard import Dashboard

    dash = Dashboard(pool.server.pool, interval=0.1)
    httpd = dash.serve("127.0.0.1", 0)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address
    yield f"http://{host}:{port}"
    dash.stop()
    httpd.shutdown()
    httpd.server_close()


@pytest.fixture()
def pool(target):
    harness = harness_for(target, max_backends=2, min_backends=2)
    yield harness
    harness.close()


def _get(url: str) -> tuple[int, dict[str, str], bytes]:
    with urllib.request.urlopen(url, timeout=10) as response:
        return response.status, dict(response.headers), response.read()


def test_index_page_is_served(dashboard):
    status, headers, body = _get(dashboard + "/")
    assert status == 200
    assert headers["Content-Type"].startswith("text/html")
    text = body.decode()
    assert "Backend ownership" in text
    assert "Waiting queue" in text
    assert "Event feed" in text
    assert "/api/events" in text  # live updates wired up


def test_state_endpoint_reports_ownership_and_queue(dashboard, pool):
    first = pool.client()
    first.ok("BEGIN")
    first.scalar("select pg_backend_pid()")

    holder_id = None
    for _ in range(100):
        status, _, body = _get(dashboard + "/api/state")
        state = json.loads(body)
        assigned = [b for b in state["backends"] if b["state"] == "assigned"]
        if assigned:
            holder_id = assigned[0]["owner"]
            break
        import time
        time.sleep(0.02)

    assert holder_id is not None
    _, _, body = _get(dashboard + "/api/state")
    state = json.loads(body)
    owned = [b for b in state["backends"] if b["owner"] == holder_id]
    assert owned and owned[0]["tx"] == "T"
    client = next(c for c in state["clients"] if c["id"] == holder_id)
    assert client["backend_id"] == owned[0]["id"]
    assert client["peer"].startswith("127.0.0.1:")

    # A second client queues because both backends are taken.
    second = pool.client()
    second.ok("BEGIN")
    second.scalar("select pg_sleep(0)")
    blocked = pool.client()
    blocked.send("select 1")

    import time
    for _ in range(200):
        _, _, body = _get(dashboard + "/api/state")
        state = json.loads(body)
        if state["queue"]:
            break
        time.sleep(0.02)

    assert len(state["queue"]) == 1
    entry = state["queue"][0]
    assert set(entry) >= {"token", "client_id", "waited_ms", "reason"}
    assert entry["waited_ms"] >= 0
    assert state["stats"]["waiting"] == 1

    blocked.abort()
    second.ok("ROLLBACK")
    second.close()
    first.ok("ROLLBACK")
    first.close()


def test_events_stream_is_server_sent_events(dashboard, pool):
    client = pool.client()
    client.scalar("select 1")
    client.close()

    request = urllib.request.Request(dashboard + "/api/events")
    with urllib.request.urlopen(request, timeout=10) as response:
        assert response.headers["Content-Type"].startswith("text/event-stream")
        buffer = b""
        event = None
        while event is None:
            chunk = response.read(1)
            if not chunk:
                break
            buffer += chunk
            if buffer.endswith(b"\n\n"):
                block, buffer = buffer, b""
                lines = block.decode().splitlines()
                if lines and lines[0] == "event: state":
                    event = "\n".join(line[len("data: "):] for line in lines[1:]
                                      if line.startswith("data: "))
    assert event is not None, "no complete SSE event arrived"
    state = json.loads(event)
    assert "backends" in state and "events" in state and "series" in state


def test_snapshot_has_a_table_friendly_series(dashboard, pool):
    client = pool.client()
    client.scalar("select 1")
    client.close()
    pool.wait_idle()

    import time
    time.sleep(1.1)  # let the sampler record a point
    _, _, body = _get(dashboard + "/api/state")
    state = json.loads(body)
    assert state["series"], "the chart needs at least one sample"
    point = state["series"][-1]
    assert set(point) == {"t", "total", "busy", "waiting"}
    assert point["total"] >= 1


def test_dashboard_shows_the_next_client_inherits_nothing(dashboard, pool):
    dirty = pool.client()
    dirty.ok("create temp table seen_by_dashboard(x int)")
    dirty.ok("set application_name = 'previous-session'")
    dirty.close()
    pool.wait_idle()

    _, _, body = _get(dashboard + "/api/state")
    state = json.loads(body)
    assert state["stats"]["quarantines"] == 0
    assert all(b["state"] == "free" for b in state["backends"])
    assert state["stats"]["resets"] >= 1

    fresh = pool.client()
    assert residue_report(fresh) == {}
    assert fresh.scalar("select count(*) from pg_class where relname = 'seen_by_dashboard'") == "0"
    fresh.close()
