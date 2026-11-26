"""The core promise: a backend is pinned while state would be visible later."""

from __future__ import annotations

from helpers import harness_for, residue_report


def test_unpinned_session_returns_backend_after_each_statement(target):
    pool = harness_for(target, max_backends=2, min_backends=1)
    try:
        client = pool.client()
        assert client.scalar("select 1") == "1"
        pool.wait_idle()
        stats = pool.stats()
        # one checkout for the query; the release happened without a disconnect
        assert stats["checkouts"] >= 1
        assert stats["resets"] >= 1
        client.close()
    finally:
        pool.close()


def test_transaction_pins_until_commit(target):
    pool = harness_for(target, max_backends=2, min_backends=2)
    try:
        client = pool.client()
        pid = client.scalar("select pg_backend_pid()")
        client.ok("BEGIN")
        client.ok("select 42")
        snapshot = pool.snapshot()
        assigned = [b for b in snapshot["backends"] if b["state"] == "assigned"]
        assert len(assigned) == 1
        assert assigned[0]["tx"] == "T"
        assert assigned[0]["pid"] == int(pid)

        # Same backend while the transaction is open, even for a new statement.
        assert client.scalar("select pg_backend_pid()") == pid
        client.ok("COMMIT")

        # After COMMIT the backend goes back and the next statement may reuse it.
        pool.wait_for(lambda s: all(b["state"] == "free" for b in s["backends"]))
        assert client.scalar("select pg_backend_pid()") == pid
        client.close()
    finally:
        pool.close()


def test_failed_transaction_keeps_backend_pinned(target):
    pool = harness_for(target, max_backends=2, min_backends=2)
    try:
        client = pool.client()
        pid = client.scalar("select pg_backend_pid()")
        client.ok("BEGIN")
        reply = client.query("select 1/0")
        assert reply.error_codes() == ["22012"]
        assert reply.tx_status == "E"
        snapshot = pool.snapshot()
        assigned = [b for b in snapshot["backends"] if b["state"] == "assigned"]
        assert [b["tx"] for b in assigned] == ["E"]
        assert assigned[0]["pid"] == int(pid)
        # Statements are refused inside the aborted transaction, but the
        # backend is still ours: ROLLBACK runs on the same server pid.
        assert client.query("select pg_backend_pid()").error_codes() == ["25P02"]
        client.ok("ROLLBACK")
        assert client.scalar("select pg_backend_pid()") == pid
        client.close()
    finally:
        pool.close()


def test_temp_table_pins_across_commit_and_does_not_leak(target):
    pool = harness_for(target, max_backends=2, min_backends=2)
    try:
        first = pool.client()
        pid = first.scalar("select pg_backend_pid()")
        first.ok("create temp table cart(id int)")
        first.ok("insert into cart values (1)")
        first.ok("COMMIT")  # autocommit-off style: the COMMIT must not release it
        assert first.scalar("select pg_backend_pid()") == pid
        assert first.scalar("select count(*) from cart") == "1"
        snapshot = pool.snapshot()
        pinned = [b for b in snapshot["backends"] if b["state"] == "assigned"]
        assert len(pinned) == 1 and pinned[0]["pinned_since"] is not None

        # A second client cannot see it and must not get that backend.
        second = pool.client()
        other_pid = second.scalar("select pg_backend_pid()")
        assert other_pid != pid
        reply = second.query("select * from cart")
        assert reply.error_codes() == ["42P01"]

        # Once the first client leaves, the backend is cleaned before reuse.
        first.close()
        second.close()
        pool.wait_idle()
        third = pool.client()
        assert residue_report(third) == {}
        assert third.query("select * from cart").error_codes() == ["42P01"]
        third.close()
    finally:
        pool.close()


def test_session_settings_pin_and_are_reset(target):
    pool = harness_for(target, max_backends=1, min_backends=1)
    try:
        first = pool.client()
        first.ok("set statement_timeout = 9000")
        first.ok("set search_path = pg_catalog, public")
        assert first.scalar("show statement_timeout") == "9s"
        # stays on the same backend across a statement boundary
        assert first.scalar("select current_setting('search_path')") == "pg_catalog, public"
        first.close()

        pool.wait_idle()
        second = pool.client()
        assert second.scalar("show statement_timeout") == "0"
        assert second.scalar("select current_setting('search_path')") == '"$user", public'
        assert residue_report(second) == {}
        second.close()
    finally:
        pool.close()


def test_advisory_lock_and_listen_pin(target):
    pool = harness_for(target, max_backends=2, min_backends=2)
    try:
        holder = pool.client()
        pid = holder.scalar("select pg_backend_pid()")
        assert holder.scalar("select pg_try_advisory_lock(4242)") == "t"
        holder.ok("listen leftover_chan")
        assert holder.scalar("select pg_backend_pid()") == pid

        other = pool.client()
        assert other.scalar("select pg_backend_pid()") != pid
        assert other.scalar("select count(*) from pg_locks where locktype = 'advisory'") == "1"

        holder.close()
        pool.wait_idle()
        fresh = pool.client()
        assert fresh.scalar("select count(*) from pg_locks where locktype = 'advisory'") == "0"
        assert fresh.scalar("select count(*) from pg_listening_channels()") == "0"
        other.close()
        fresh.close()
    finally:
        pool.close()


def test_named_prepared_statement_pins_and_is_discarded(target):
    pool = harness_for(target, max_backends=1, min_backends=1)
    try:
        first = pool.client()
        pid = first.scalar("select pg_backend_pid()")
        first.ok("prepare pinned_stmt(int) as select $1::int * 2")
        assert first.scalar("select pg_backend_pid()") == pid
        assert first.scalar("select count(*) from pg_prepared_statements") == "1"
        first.close()

        pool.wait_idle()
        second = pool.client()
        assert second.scalar("select count(*) from pg_prepared_statements") == "0"
        reply = second.query("execute pinned_stmt(21)")
        assert reply.error_codes() == ["26000"]  # prepared statement does not exist
        second.close()
    finally:
        pool.close()


def test_leading_comment_does_not_hide_a_pin(target):
    pool = harness_for(target, max_backends=2, min_backends=2)
    try:
        client = pool.client()
        pid = client.scalar("select pg_backend_pid()")
        client.ok("/* checkout flow */ create temp table commented(x int)")
        assert client.scalar("select pg_backend_pid()") == pid
        snapshot = pool.snapshot()
        assert any(b["pinned_since"] is not None for b in snapshot["backends"] if b["state"] == "assigned")

        other = pool.client()
        assert other.scalar("select pg_backend_pid()") != pid
        assert other.query("select * from commented").error_codes() == ["42P01"]
        client.close()
        other.close()
        pool.wait_idle()
    finally:
        pool.close()


def test_set_local_does_not_pin_outside_its_transaction(target):
    """SET LOCAL dies with the transaction, so it need not hold the session."""
    pool = harness_for(target, max_backends=1, min_backends=1)
    try:
        client = pool.client()
        pid = client.scalar("select pg_backend_pid()")
        default_work_mem = client.scalar("show work_mem")
        client.ok("BEGIN")
        client.ok("set local work_mem = '17MB'")
        assert client.scalar("select pg_backend_pid()") == pid  # transaction pins
        client.ok("COMMIT")
        pool.wait_idle()  # released on commit: LOCAL left nothing behind

        fresh = pool.client()
        assert fresh.scalar("select pg_backend_pid()") == pid
        assert fresh.scalar("show work_mem") == default_work_mem
        fresh.close()
        client.close()
    finally:
        pool.close()
