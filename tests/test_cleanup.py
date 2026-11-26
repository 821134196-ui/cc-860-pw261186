"""Disconnect paths: the backend is cleaned before it can be handed on."""

from __future__ import annotations

import time

from helpers import harness_for, residue_report


def _pollute(client) -> int:
    pid = int(client.scalar("select pg_backend_pid()"))
    client.ok("create temp table survivor(a int)")
    client.ok("insert into survivor values (7)")
    client.ok("set application_name = 'dirty-app'")
    client.ok("prepare leaky(int) as select $1::int")
    client.ok("listen dirty_chan")
    client.ok("select pg_advisory_lock(5150)")
    return pid


def test_graceful_disconnect_cleans_before_reuse(target):
    pool = harness_for(target, max_backends=1, min_backends=1)
    try:
        first = pool.client()
        pid = _pollute(first)
        first.close()

        snapshot = pool.wait_idle()
        assert snapshot["stats"]["resets"] >= 1
        assert snapshot["stats"]["quarantines"] == 0
        assert any(e["kind"] == "release" for e in snapshot["events"])

        second = pool.client()
        assert residue_report(second) == {}
        second.close()
    finally:
        pool.close()


def test_abrupt_disconnect_mid_query_is_cancelled_and_cleaned(target):
    pool = harness_for(target, max_backends=1, min_backends=1)
    try:
        client = pool.client()
        pid = _pollute(client)
        client.send("select pg_sleep(30)")  # leave it running, never read
        time.sleep(0.3)
        client.abort()

        snapshot = pool.wait_for(
            lambda s: s["stats"]["cancels_routed"] >= 1 and s["stats"]["resets"] >= 1
            and all(b["state"] == "free" for b in s["backends"]),
            timeout=10,
        )
        assert snapshot["stats"]["quarantines"] == 0
        assert any("cancel" in e["kind"] for e in snapshot["events"])

        fresh = pool.client()
        assert residue_report(fresh) == {}
        assert fresh.scalar("select 1") == "1"
        fresh.close()
    finally:
        pool.close()


def test_disconnect_inside_transaction_rolls_back_before_returning(target):
    pool = harness_for(target, max_backends=1, min_backends=1)
    try:
        client = pool.client()
        client.ok("BEGIN")
        client.ok("create table committed_row(x int)")
        client.ok("insert into committed_row values (1)")
        client.abort()

        pool.wait_idle()
        fresh = pool.client()
        # DISCARD ALL in an open transaction rolls the transaction back, so the
        # uncommitted row must not exist for the next client.
        assert fresh.scalar("select to_regclass('committed_row')") is None
        fresh.close()
    finally:
        pool.close()


def test_residue_is_quarantined_and_pool_refills(target):
    """If cleanup cannot finish, the backend is destroyed rather than reused."""
    from pgpool.config import TestHooks

    pool = harness_for(
        target, max_backends=2, min_backends=1,
        hooks=TestHooks(cleanup_skip_discard=True),
    )
    try:
        first = pool.client()
        pid = int(first.scalar("select pg_backend_pid()"))
        first.ok("create temp table stubborn(x int)")
        first.close()

        snapshot = pool.wait_for(
            lambda s: s["stats"]["quarantines"] >= 1 and len(s["backends"]) >= 1,
            timeout=12,
        )
        assert snapshot["stats"]["dirty_backends"] >= 1
        assert all(b["pid"] != pid for b in snapshot["backends"])
        assert any("residue" in (b["destroy_reason"] or "") for b in snapshot["backends"]) is False
        assert any("residue after cleanup" in e["text"] for e in snapshot["events"])

        fresh = pool.client()
        assert fresh.scalar("select pg_backend_pid()") != str(pid)
        assert residue_report(fresh) == {}
        fresh.close()
    finally:
        pool.close()
