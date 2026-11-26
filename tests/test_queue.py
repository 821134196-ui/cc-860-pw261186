"""Saturation: a third client queues, then gets in when a backend is released."""

from __future__ import annotations

import threading
import time

from helpers import harness_for, residue_report


def test_queue_is_fifo_and_drains_on_release(target):
    pool = harness_for(target, max_backends=2, min_backends=2)
    release = threading.Event()
    try:
        first, second = pool.client(), pool.client()
        first.ok("BEGIN")
        second.ok("BEGIN")
        assert pool.wait_for(lambda s: s["stats"]["in_use"] == 2)["queue"] == []

        results: dict[int, dict] = {}
        admitted: list[dict] = []
        admitted_lock = threading.Lock()

        def waiter(index: int) -> None:
            started = time.monotonic()
            entry: dict = {"index": index}
            try:
                client = pool.client(timeout=30)
                entry["peer"] = client.peer
                # Hold the backend so the next waiter cannot also get in.
                client.ok("BEGIN")
                entry["admitted"] = time.monotonic() - started
                with admitted_lock:
                    admitted.append(entry)
                entry["outcome"] = "ok"
                release.wait(20)
                client.ok("ROLLBACK")
                client.close()
            except Exception as exc:  # noqa: BLE001
                entry["outcome"] = type(exc).__name__
            results[index] = entry

        threads = [threading.Thread(target=waiter, args=(i,)) for i in range(2)]
        for thread in threads:
            thread.start()

        snapshot = pool.wait_for(lambda s: s["stats"]["waiting"] == 2, timeout=5)
        tokens = [w["token"] for w in snapshot["queue"]]
        assert tokens == sorted(tokens), "the queue must be ordered by arrival token"
        head = snapshot["queue"][0]["client_id"]
        head_peer = next(c["peer"] for c in snapshot["clients"] if c["id"] == head)

        # Releasing one backend admits exactly the head of the queue.
        first.ok("COMMIT")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not admitted:
            time.sleep(0.02)
        time.sleep(0.4)  # give a wrongly-admitted second waiter time to appear
        assert len(admitted) == 1, f"expected exactly one admission, got {admitted}"
        winner = admitted[0]
        assert winner["outcome"] == "ok"
        assert winner["peer"] == head_peer, "the queue must be served FIFO"
        assert winner["admitted"] < 3.0

        # The second waiter is still queued behind the winner.
        assert pool.snapshot()["stats"]["waiting"] == 1

        release.set()
        second.ok("COMMIT")
        for thread in threads:
            thread.join(20)
        assert sorted(r["outcome"] for r in results.values()) == ["ok", "ok"]

        snapshot = pool.wait_idle(timeout=10)
        assert snapshot["queue"] == []
        assert snapshot["stats"]["checkout_timeouts"] == 0
        assert snapshot["stats"]["quarantines"] == 0
        first.close()
        second.close()
    finally:
        release.set()
        pool.close()


def test_checkout_timeout_closes_client_cleanly(target):
    pool = harness_for(target, max_backends=1, min_backends=1, checkout_timeout=0.8)
    try:
        holder = pool.client()
        holder.ok("BEGIN")
        holder.scalar("select 1")

        started = time.monotonic()
        try:
            blocked = pool.client(timeout=15)
            blocked.scalar("select 1")
            raise AssertionError("the blocked client should not have got a backend")
        except Exception as exc:  # noqa: BLE001
            elapsed = time.monotonic() - started
            assert "53300" in str(exc) or "timed out" in str(exc) or "refused" in str(exc)

        assert 0.7 <= elapsed < 6
        snapshot = pool.snapshot()
        assert snapshot["stats"]["checkout_timeouts"] == 1
        assert any("checkout timed out" in e["text"] for e in snapshot["events"])

        # The pool is still usable afterwards.
        holder.ok("ROLLBACK")
        holder.scalar("select 1")
        holder.close()
        assert pool.wait_idle()["stats"]["quarantines"] == 0
    finally:
        pool.close()


def test_client_that_vanishes_while_queued_is_not_leaked(target):
    pool = harness_for(target, max_backends=1, min_backends=1, checkout_timeout=5)
    try:
        holder = pool.client()
        holder.ok("BEGIN")
        holder.scalar("select 1")

        waiting = pool.client()
        waiting.send("select 1")
        time.sleep(0.3)
        waiting.abort()

        snapshot = pool.wait_for(lambda s: s["queue"] == [], timeout=6)
        assert snapshot["stats"]["checkout_timeouts"] == 0
        assert snapshot["stats"]["clients_abandoned_in_queue"] >= 1

        holder.ok("COMMIT")
        holder.close()
        fresh = pool.client()
        assert residue_report(fresh) == {}
        fresh.close()
    finally:
        pool.close()


def test_max_backends_is_never_exceeded(target):
    pool = harness_for(target, max_backends=3, min_backends=1)
    try:
        clients = []
        for _ in range(3):
            client = pool.client()
            client.ok("BEGIN")
            client.scalar("select 1")
            clients.append(client)
        snapshot = pool.wait_for(lambda s: len(s["backends"]) == 3)
        assert snapshot["stats"]["in_use"] == 3
        assert snapshot["stats"]["waiting"] == 0

        extra = pool.client()
        extra.send("select 1")
        snapshot = pool.wait_for(lambda s: s["stats"]["waiting"] == 1, timeout=5)
        assert len(snapshot["backends"]) <= 3
        extra.abort()

        for client in clients:
            client.ok("COMMIT")
            client.close()
        pool.wait_idle()
    finally:
        pool.close()
