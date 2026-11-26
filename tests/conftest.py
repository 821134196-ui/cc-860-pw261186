"""Session-scoped embedded PostgreSQL, function-scoped per-test databases."""

from __future__ import annotations

import dataclasses
import os
import sys
import tempfile
import uuid

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pgserver  # noqa: E402

from pgpool.config import ServerTarget  # noqa: E402


@pytest.fixture(scope="session")
def pg_server():
    directory = tempfile.mkdtemp(prefix="pgpool-tests-")
    server = pgserver.get_server(directory, cleanup_mode="delete")
    yield server
    try:
        server.cleanup()
    except Exception:  # noqa: BLE001
        pass


@pytest.fixture()
def target(pg_server):
    """A fresh database per test, so parallel state cannot bleed across tests."""
    name = "t_" + uuid.uuid4().hex[:10]
    pg_server.psql(f'CREATE DATABASE "{name}"')
    yield dataclasses.replace(ServerTarget.from_dsn(pg_server.get_uri()), database=name)
    try:
        pg_server.psql(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    except Exception:  # noqa: BLE001
        pass
