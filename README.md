# pgpool — a restricted, session-aware PostgreSQL connection pool

A small PostgreSQL proxy that pools **real backend connections** and keeps one
client's work on one backend for exactly as long as that client's session state
would leak if it were moved: open transactions, temp tables, session GUCs,
`SET ROLE`, `LISTEN`, advisory locks, holdable cursors, prepared statements.

When a client disconnects, the backend is **not** returned as-is. It is cleaned
with `DISCARD ALL`, then checked by a probe query. Only a backend with zero
residue goes back into the pool — anything else is destroyed and replaced.

There is a live dashboard at `/` showing backend ownership, the waiting queue,
and every decision the proxy made.

```
client ──wire v3──▶ pgpool ──wire v3──▶ PostgreSQL
                     │
                     ├── pool of N backend slots
                     ├── FIFO checkout queue with a timeout
                     ├── cancel-key indirection (client → current backend)
                     └── dashboard (HTTP + SSE)
```

## Quick start

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e .            # or: pip install pgserver psycopg[binary] pytest

# point it at any PostgreSQL and open the dashboard
pgpool --server postgresql://user:pass@host:5432/appdb \
       --listen-port 6432 --dashboard-port 8080 --max-backends 8

# clients connect to the proxy exactly as if it were PostgreSQL
psql "postgresql://user@127.0.0.1:6432/appdb"
open http://127.0.0.1:8080/
```

Useful flags: `--checkout-timeout` (seconds a client may wait for a backend),
`--reset-timeout` (per-statement budget during cleanup), `--idle-timeout`
(shed idle backends), `--application-name` (stamped on every backend),
`--client-auth trust|cleartext`, `--no-dashboard`.

The `--cleanup-delay`, `--cleanup-prefix-sql`, `--cleanup-skip-discard` and
`--checkout-delay` flags exist only to make races reproducible in tests; they
are not for production use.

## Running the tests

Tests boot a real PostgreSQL 16 through [`pgserver`](https://pypi.org/project/pgserver/)
and drive the proxy over a hand-written wire-protocol client, so they exercise
the same bytes a real client would send.

```bash
.venv/bin/python -m pytest -q          # 63 tests, ~1 minute
.venv/bin/python -m pytest tests/test_cancel_order.py -q
```

| file | what it proves |
|---|---|
| `tests/test_classify.py` | the SQL classifier's pin reasons and statement splitting |
| `tests/test_pinning.py` | transactions, temp tables, GUCs, roles, locks and prepared statements keep the same backend |
| `tests/test_cleanup.py` | graceful/abrupt disconnect cleans before reuse; residue is quarantined |
| `tests/test_cancel_order.py` | cancel-before-timeout, timeout-before-cancel, and cancels that land inside cleanup |
| `tests/test_queue.py` | FIFO queueing, checkout timeout, abandoned waiters, hard concurrency cap |
| `tests/test_dashboard.py` | the page, `/api/state`, ownership/queue data and the SSE stream |

## How pinning works

`classify.analyze(sql)` looks at each statement and reports a *reason*:
`temp table`, `session setting`, `role`, `listen`, `advisory lock`,
`holdable cursor`, `prepared statement`, `library load`. A statement with a
reason adds that reason to the session's pin set. Pins are **additive** and are
only dropped when the backend is cleaned — a session that ran `SET` stays pinned
even after `COMMIT`.

Transactions do not need a named pin: the proxy tracks the backend's
`ReadyForQuery` status (`I` / `T` / `E`) and treats `T`/`E` as pinned. So:

* `BEGIN` … statement … backend stays assigned, `COMMIT` releases it;
* `CREATE TEMP TABLE` … `COMMIT` … backend **stays** assigned;
* a plain `SELECT` on an unpinned session releases the backend immediately
  after `ReadyForQuery` (transaction pooling).

The classifier is intentionally conservative. A false pin costs concurrency; a
false "safe" leaks one client's session into another's. It handles quotes,
dollar-quoting, line comments, and nested block comments so a statement cannot
hide behind a `/* ... */`.

The one deliberate exception is `SET LOCAL`, which PostgreSQL discards at end
of transaction and therefore must not create a pin of its own.

## Cleanup: `DISCARD ALL` plus a probe

Every release runs, on the backend, with the client already gone:

1. **Barrier** — one or more `SELECT 1` statements that absorb a cancel which
   was sent after the client's last statement finished but before cleanup
   started. A cancel caught here is counted as `cancels_late` and retried.
2. **`DISCARD ALL`** — clears temp tables, prepared statements, advisory locks,
   `LISTEN`, holdable cursors, session GUCs, and role/session authorization.
   It preserves startup parameters, which is why backends keep their
   `application_name`.
3. **Probe** — a single query returning counts of temp relations, prepared
   statements, listening channels, advisory locks, holdable cursors and
   non-default session GUCs, plus `role`, `current_user` and `session_user`.

A backend that fails the probe (or whose `DISCARD ALL` fails twice, or whose
socket dies) is **quarantined**: destroyed and removed from the pool, with the
reason recorded, and a fresh backend is opened to keep `min_backends`.

Two ordering subtleties that the test suite pins down:

* `DISCARD ALL` raises `25001` inside a transaction block, and a multi-statement
  simple query is implicitly wrapped in one. The release path therefore runs it
  as its own single statement and retries.
* A `CancelRequest` that arrives *during* cleanup would abort `DISCARD ALL`.
  The proxy keeps the client's cancel key mapped to the draining backend until
  cleanup finishes, queues such a cancel (`_pending_cancels`), and lets the
  barrier absorb it. Without this, a client that disconnects at the wrong
  moment could leave residue.

## Cancellation

Clients are given **proxy-owned** `BackendKeyData`, not the real backend's PID
and secret. A `CancelRequest` is looked up in that table and forwarded to
whichever backend the client holds *at that moment* — including a backend that
is currently being drained after the client disconnected. A cancel for a client
that holds nothing and has nothing draining is answered with a logged no-op, the
same way PostgreSQL ignores a cancel on an idle connection.

Because the proxy never forwards the real cancel key, a client can only ever
cancel its own work. The backend's secret never leaves the proxy.

## Queueing

Checkout is FIFO by arrival token, and it is **strict**: a waiter may only take
a free backend when it is at the head of the queue, so a freshly woken thread
cannot overtake an older one. Waiters re-check whether the client is still alive
every poll (so a client that disconnects while waiting leaves the queue instead
of being handed a backend it will never use), and give up after
`checkout_timeout` with a clean PostgreSQL `FATAL 53300` plus a hint. The
backend count never exceeds `max_backends`.

## Dashboard

`GET /` serves a single self-contained HTML page (no build step, no CDN):

* **Stat tiles** — backends used/capacity, in use, pinned sessions, waiting
  clients, checkouts, cleanups, quarantines.
* **Pool activity chart** — in use / idle / waiting over the last 45 s, with a
  crosshair tooltip, an accessible table view, direct end labels and a legend.
  Light and dark themes are separately stepped and validated for colour-vision
  deficiency (worst adjacent pair ΔE 9.2 light / 9.4 dark) and contrast; the
  light-mode aqua is below 3:1, so values are always directly labelled and the
  table view is always available.
* **Backend ownership** — which client owns which server PID, transaction
  status, and the reason a session is pinned (with pin age).
* **Waiting queue** — tokens in FIFO order with wait time.
* **Event feed** — connect, auth, checkout, pin/unpin, cancel (routed / late /
  ignored), cleanup, release, quarantine.

`GET /api/state` returns the same snapshot as JSON and `GET /api/events` is an
`text/event-stream` of those snapshots, so the page updates without polling.

## Layout

```
pgpool/
  wire.py       protocol framing, message builders/parsers, ErrorInfo
  classify.py   statement splitting and pin classification
  config.py     PoolConfig, ServerTarget (DSN), backend connector + auth
  pool.py       slots, checkout queue, cancel routing, cleanup/probe, metrics
  session.py    one client: handshake, relay loop, pin tracking, disconnect
  server.py     listener, sampler, idle reaper, CLI
  dashboard.py  HTTP + SSE server
  static/index.html
tests/
  helpers.py    raw wire-protocol client + harness + residue probe
  conftest.py   embedded PostgreSQL (session) and per-test database
```

## Known limitations

* Plaintext only: `SSLRequest` is answered with `N`. TLS termination belongs in
  front of the proxy.
* The classifier is textual; state changed by functions (`SELECT my_setter()`)
  or by `psql` meta-commands is not visible to it. Those cases are still
  covered by the cleanup probe, but they will not delay a release.
* Only the simple `Query` path and the extended-protocol relay are forwarded;
  `COPY` data is relayed byte-for-byte once a backend is assigned but is not
  inspected.
* The dashboard has no authentication and listens on `127.0.0.1` by default.
