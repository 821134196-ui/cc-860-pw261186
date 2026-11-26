"""Shared data records for the pool."""

from __future__ import annotations

import itertools
import time
from dataclasses import dataclass, field
from typing import Any, Literal

BackendState = Literal["free", "assigned", "resetting", "broken"]

_ids = itertools.count(1)
_token_ids = itertools.count(1)


def next_client_id() -> int:
    return next(_ids)


def next_token() -> int:
    return next(_token_ids)


def now() -> float:
    return time.monotonic()


@dataclass
class Event:
    ts: float
    kind: str
    actor: str
    text: str
    level: str = "info"

    def to_dict(self, t0: float) -> dict[str, Any]:
        return {
            "t": round(self.ts - t0, 3),
            "kind": self.kind,
            "actor": self.actor,
            "text": self.text,
            "level": self.level,
        }


@dataclass
class Backend:
    id: int
    pid: int
    secret: int
    sock: Any
    state: BackendState = "free"
    client_id: int | None = None
    generation: int = 0
    checkout_count: int = 0
    pinned_since: float | None = None
    last_release: float | None = None
    reset_count: int = 0
    destroy_reason: str | None = None
    dirty_report: dict[str, Any] | None = None
    tx_status: str = "I"

    def snapshot(self, t0: float) -> dict[str, Any]:
        return {
            "id": self.id,
            "pid": self.pid,
            "state": self.state,
            "owner": self.client_id,
            "generation": self.generation,
            "checkouts": self.checkout_count,
            "resets": self.reset_count,
            "tx": self.tx_status,
            "pinned_since": None if self.pinned_since is None else round(self.pinned_since - t0, 3),
            "destroy_reason": self.destroy_reason,
            "dirty": self.dirty_report,
        }
