"""Classifier unit tests: run without a database."""

from __future__ import annotations

import pytest

from pgpool.classify import analyze, split_statements


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("select 1", ["select 1"]),
        ("", []),
        ("  ;; ", []),
        ("select 1; select 2", ["select 1", "select 2"]),
        ("insert into t values ('a;b')", ["insert into t values ('a;b')"]),
        ('insert into t values ("semi;colon")', ['insert into t values ("semi;colon")']),
        ("do $$ begin raise notice 'x;y'; end $$", ["do $$ begin raise notice 'x;y'; end $$"]),
        ("select 'it''s; fine'", ["select 'it''s; fine'"]),
        ("select 1; -- trailing; comment\nselect 2", ["select 1", "select 2"]),
        ("select 1 /* a;b */ ; select 2", ["select 1", "select 2"]),
        ("/* outer /* nested; */ still; */ set role r", ["set role r"]),
        ("select 1; ; select 2", ["select 1", "select 2"]),
    ],
)
def test_split_statements(sql, expected):
    assert split_statements(sql) == expected


@pytest.mark.parametrize(
    "sql, reason",
    [
        ("create temp table t(x int)", "temp table"),
        ("CREATE GLOBAL TEMPORARY TABLE t(x int)", "temp table"),
        ("alter table pg_temp_3.t add column y int", "temp table"),
        ("set role analyst", "role"),
        ("set session authorization bob", "role"),
        ("listen chan", "listen"),
        ("prepare s(int) as select $1", "prepared statement"),
        ("select pg_advisory_lock(1)", "advisory lock"),
        ("select pg_try_advisory_lock_shared(1)", "advisory lock"),
        ("declare c cursor with hold for select 1", "holdable cursor"),
        ("load 'auto_explain'", "library load"),
        ("set statement_timeout = 5000", "session setting"),
        ("set search_path = a, b", "session setting"),
        ("set time zone 'UTC'", "session setting"),
        ("reset statement_timeout", "session setting"),
        ("set local work_mem = '4MB'", None),  # dies with the transaction
        ("reset all", None),  # DISCARD-adjacent; does not create session state
        ("begin", None),
        ("commit", None),
        ("rollback", None),
        ("prepare transaction 'gid'", None),
    ],
)
def test_analyze_reason(sql, reason):
    assert analyze(sql).reason == reason
    # BEGIN pins through the transaction status instead of a named reason.
    assert analyze(sql).touches_session is (reason is not None or sql.lower().startswith("begin"))


def test_transaction_boundaries():
    analysis = analyze("BEGIN; select 1; COMMIT")
    assert analysis.opens_transaction and analysis.closes_transaction
    assert [s for s in analysis.statements] == ["BEGIN", "select 1", "COMMIT"]
    assert analyze("start transaction isolation level serializable").opens_transaction
    assert analyze("abort").closes_transaction
    assert analyze("end").closes_transaction


def test_session_state_wins_inside_a_transaction():
    # BEGIN alone must not be reported as session state: the tx status pins it.
    assert analyze("begin").reason is None
    # A temp table inside the same string still produces a persistent pin.
    assert analyze("begin; create temp table t(x int); commit").reason == "temp table"
