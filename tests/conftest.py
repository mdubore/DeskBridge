import pytest
import aiosqlite
from deskbridge.db.schema import apply_schema
from deskbridge.db.store import Store


@pytest.fixture
async def db_conn(tmp_path):
    db_path = tmp_path / "test.db"
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        await apply_schema(conn)
        yield conn


@pytest.fixture
async def store(db_conn):
    return Store(db_conn)
