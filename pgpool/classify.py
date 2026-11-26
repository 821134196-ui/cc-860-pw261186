"""Classify SQL text enough to know when a backend must stay pinned.

This is a heuristic over the bytes a client sends. It errs toward pinning:
a false pin only costs concurrency, while a false "safe to pool" would leak
session state to the next client.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

BEGIN_RE = re.compile(r"^\s*(BEGIN|START\s+TRANSACTION)\b", re.I)
COMMIT_RE = re.compile(r"^\s*(COMMIT|END|ABORT|ROLLBACK|PREPARE\s+TRANSACTION)\b", re.I)
# Statements that are session-scoped even when they look like a bare SET.
SESSION_SCOPED_RE = re.compile(
    r"^\s*("
    r"CREATE\s+(?:GLOBAL\s+|LOCAL\s+)?TEMP(?:ORARY)?\b"
    r"|ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?\"?(?:pg_temp\w*|pg_toast_temp\w*)"
    r"|SET\s+SESSION\s+AUTHORIZATION\b"
    r"|SET\s+ROLE\b"
    r"|SET\s+TIME\s+ZONE\b"
    r"|SET\s+(?:search_path|statement_timeout|lock_timeout|idle_in_transaction_session_timeout"
    r"|application_name|timezone|datestyle|role)\b"
    r"|LISTEN\b"
    r"|PREPARE\s+(?!TRANSACTION\b)"
    r"|DECLARE\s+\S+\s+(?:BINARY\s+)?(?:NO\s+SCROLL\s+)?CURSOR\s+WITH\s+HOLD\b"
    r"|LOAD\b"
    r"|SELECT\s+pg_(?:try_)?advisory_lock\w*\("
    r")",
    re.I,
)
# SET/RESET outside a transaction block (SET LOCAL dies with the transaction).
SESSION_SET_RE = re.compile(r"^\s*(?:SET|RESET)\s+", re.I)
# SET LOCAL never outlives its transaction, so it must not pin on its own.
SESSION_SET_LOCAL_RE = re.compile(r"^\s*SET\s+LOCAL\b", re.I)
SESSION_RESET_ALL_RE = re.compile(r"^\s*RESET\s+ALL\b", re.I)


@dataclass
class SqlAnalysis:
    opens_transaction: bool = False
    closes_transaction: bool = False
    touches_session: bool = False
    reason: str | None = None
    statements: list[str] = field(default_factory=list)

    @property
    def needs_backend(self) -> bool:
        return bool(self.statements)


def split_statements(sql: str) -> list[str]:
    """Split a simple-query string on semicolons, respecting quotes, dollar
    quoting and comments. Comments are dropped so a leading ``/* ... */``
    cannot hide a statement from the classifier."""
    out: list[str] = []
    buf: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch == "-" and sql.startswith("--", i):
            end = sql.find("\n", i)
            i = n if end < 0 else end
            buf.append(" ")
            continue
        if ch == "/" and sql.startswith("/*", i):
            # PostgreSQL block comments nest.
            depth, j = 1, i + 2
            while j < n and depth:
                if sql.startswith("/*", j):
                    depth += 1
                    j += 2
                elif sql.startswith("*/", j):
                    depth -= 1
                    j += 2
                else:
                    j += 1
            i = j
            buf.append(" ")
            continue
        if ch == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'" and j + 1 < n and sql[j + 1] == "'":
                    j += 2
                    continue
                if sql[j] == "\\" and j + 1 < n:
                    j += 2
                    continue
                if sql[j] == "'":
                    break
                j += 1
            buf.append(sql[i : j + 1])
            i = j + 1
            continue
        if ch == '"':
            j = sql.find('"', i + 1)
            j = n - 1 if j < 0 else j
            buf.append(sql[i : j + 1])
            i = j + 1
            continue
        if ch == "$":
            m = re.match(r"\$[A-Za-z_0-9]*\$", sql[i:])
            if m:
                tag = m.group(0)
                end = sql.find(tag, i + len(tag))
                end = n - len(tag) if end < 0 else end
                buf.append(sql[i : end + len(tag)])
                i = end + len(tag)
                continue
        if ch == ";":
            text = "".join(buf).strip()
            if text:
                out.append(text)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    text = "".join(buf).strip()
    if text:
        out.append(text)
    return out


def analyze(sql: str) -> SqlAnalysis:
    result = SqlAnalysis(statements=split_statements(sql))
    for stmt in result.statements:
        if BEGIN_RE.match(stmt):
            result.opens_transaction = True
            result.touches_session = True
            continue
        if COMMIT_RE.match(stmt):
            result.closes_transaction = True
            continue
        if SESSION_SCOPED_RE.match(stmt):
            result.touches_session = True
            result.reason = result.reason or _reason_key(stmt, session_scoped=True)
            continue
        if (
            SESSION_SET_RE.match(stmt)
            and not SESSION_RESET_ALL_RE.match(stmt)
            and not SESSION_SET_LOCAL_RE.match(stmt)
        ):
            result.touches_session = True
            result.reason = result.reason or _reason_key(stmt)
    return result


def _reason_key(stmt: str, *, session_scoped: bool = False) -> str:
    """Map a statement to a short, stable pin label."""
    s = stmt.strip()
    low = s.lower()
    if low.startswith("create") and "temp" in low:
        return "temp table"
    if low.startswith("alter table"):
        return "temp table"
    if low.startswith("set session authorization") or low.startswith("set role") or low.startswith("reset role"):
        return "role"
    if low.startswith("listen"):
        return "listen"
    if low.startswith("prepare"):
        return "prepared statement"
    if "advisory_lock" in low:
        return "advisory lock"
    if low.startswith("declare"):
        return "holdable cursor"
    if low.startswith("load"):
        return "library load"
    if low.startswith("set") or low.startswith("reset"):
        return "session setting"
    return "session state"
