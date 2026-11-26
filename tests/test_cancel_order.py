"""Cancel vs. statement_timeout: whichever arrives first, the session survives."""

from __future__ import annotations

import socket
import time

import pytest

from helpers import harness_for, residue_report


def test_cancel_before_timeout(target):
    """cancel wins; the later statement_timeout is a no-op on a healthy session."""
    pool = harness_for(target, max_backends=1, min_backends=1)
    try:
        client = pool.client()
        client.ok("set statement_timeout = 1500")
        client.send("select pg_sleep(4)")
        time.sleep(0.4)
        client.cancel()
        reply = client.collect()
        errors = reply.errors()
        assert [e.code for e in errors] == ["57014"]
        assert reply.tx_status == "I"

        # Past the point the timeout would have fired: still healthy. The SET
        # keeps the session pinned, so the backend must not have been recycled.
        time.sleep(1.4)
        assert client.scalar("select 42") == "42"
        client.ok("reset statement_timeout")
        client.close()
        assert pool.wait_idle()["stats"]["quarantines"] == 0
    finally:
        pool.close()


def test_timeout_before_cancel(target):
    """timeout wins; the late cancel arrives when the backend is idle and is dropped."""
    pool = harness_for(target, max_backends=1, min_backends=1)
    try:
        client = pool.client()
        client.ok("set statement_timeout = 300")
        started = time.monotonic()
        reply = client.query("select pg_sleep(4)")
        elapsed = time.monotonic() - started
        assert [e.code for e in reply.errors()] == ["57014"]
        assert elapsed < 3

        client.cancel()  # statement is already over
        time.sleep(0.2)
        assert client.scalar("select 7") == "7"
        assert client.scalar("select pg_sleep(0.1)") == ""

        client.ok("reset statement_timeout")
        client.close()
        snapshot = pool.wait_idle()
        assert snapshot["stats"]["quarantines"] == 0
        assert snapshot["stats"]["checkout_timeouts"] == 0
    finally:
        pool.close()


def test_cancel_before_cleanup_is_absorbed_by_barrier(target):
    """Client detaches while its last statement is settling: the cancel must not
    be allowed to interrupt DISCARD ALL."""
    pool = harness_for(target, max_backends=1, min_backends=1)
    try:
        client = pool.client()
        pid = int(client.scalar("select pg_backend_pid()"))
        client.ok("create temp table keeps(x int)")
        client.ok("select 1")
        time.sleep(0.02)
        client.abort()  # proxy notices only on its next read/drain
        # Race a cancel in behind the disconnect.
        try:
            cancel = socket.create_connection(("127.0.0.1", pool.port), timeout=5)
            from pgpool import wire
            assert client.key is not None
            cancel.sendall(wire.cancel_request_packet(*client.key))
            cancel.close()
        except OSError:
            pass

        snapshot = pool.wait_idle(timeout=12)
        assert snapshot["stats"]["quarantines"] == 0
        assert snapshot["stats"]["resets"] >= 1
        assert all(b["pid"] == pid for b in snapshot["backends"])

        fresh = pool.client()
        assert residue_report(fresh) == {}
        fresh.close()
    finally:
        pool.close()


def test_cancel_then_disconnect_with_delayed_cleanup(target):
    """A cancel whose reply has been seen, followed by a disconnect: cleanup is
    delayed, and the backend must still come back clean and unreused."""
    from pgpool.config import TestHooks

    pool = harness_for(
        target, max_backends=1, min_backends=1,
        hooks=TestHooks(cleanup_delay=0.25),
    )
    try:
        client = pool.client()
        client.ok("create temp table polluted2(x int)")
        client.ok("listen leftover_chan")
        client.send("select pg_sleep(5)")
        time.sleep(0.2)
        client.cancel()
        reply = client.collect()
        assert [e.code for e in reply.errors()] == ["57014"]
        client.close()  # cleanup sleeps 0.25 s, then DISCARD ALL

        snapshot = pool.wait_idle(timeout=12)
        assert snapshot["stats"]["quarantines"] == 0
        assert snapshot["stats"]["resets"] >= 1
        fresh = pool.client()
        assert residue_report(fresh) == {}
        fresh.close()
    finally:
        pool.close()


@pytest.mark.parametrize("delay", [0.0, 0.2])
def test_late_cancel_flagged_for_draining_backend(target, delay):
    """Between the client's disconnect and DISCARD ALL the cancel key still
    belongs to the draining backend, so the cancel is queued, not lost."""
    from pgpool.config import TestHooks

    hooks = TestHooks(cleanup_prefix_sql="select pg_sleep(0.4)") if delay else None
    pool = harness_for(target, max_backends=1, min_backends=1, hooks=hooks)
    try:
        client = pool.client()
        client.ok("create temp table late(x int)")
        client.ok("set application_name = 'late'")
        client.ok("select pg_advisory_lock(999)")
        client.ok("select 1")
        time.sleep(0.02)
        key = client.key
        client.abort()
        time.sleep(delay if delay else 0.0)
        try:
            cancel = socket.create_connection(("127.0.0.1", pool.port), timeout=5)
            from pgpool import wire
            assert key is not None
            cancel.sendall(wire.cancel_request_packet(*key))
            cancel.close()
        except OSError:
            pass

        snapshot = pool.wait_idle(timeout=12)
        if snapshot["stats"]["quarantines"]:
            for e in snapshot["events"]:
                print(e["kind"], "|", e["text"])
        assert snapshot["stats"]["quarantines"] == 0
        # Late cancels are either absorbed by a barrier or queued for the drain;
        # neither may leave residue.
        fresh = pool.client()
        assert residue_report(fresh) == {}
        fresh.close()
    finally:
        pool.close()
