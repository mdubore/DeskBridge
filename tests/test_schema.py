import asyncio
import aiosqlite
import pytest
from deskbridge.db.schema import SCHEMA_VERSION, apply_schema, TABLE_NAMES


@pytest.fixture
async def db(tmp_path):
    db_path = tmp_path / "test.db"
    async with aiosqlite.connect(db_path) as conn:
        await apply_schema(conn)
        yield conn


async def test_apply_schema_creates_all_tables(db):
    cursor = await db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    rows = await cursor.fetchall()
    existing = {row[0] for row in rows}
    for name in TABLE_NAMES:
        assert name in existing, f"Missing table: {name}"


async def test_apply_schema_is_idempotent(tmp_path):
    db_path = tmp_path / "test.db"
    async with aiosqlite.connect(db_path) as conn:
        await apply_schema(conn)
        await apply_schema(conn)  # should not raise
        cursor = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        rows = await cursor.fetchall()
    existing = {row[0] for row in rows}
    for name in TABLE_NAMES:
        assert name in existing, f"Missing table after second apply: {name}"


async def test_schema_version_stored(db):
    cursor = await db.execute("SELECT version FROM schema_version")
    row = await cursor.fetchone()
    assert row is not None
    assert row[0] == SCHEMA_VERSION


async def test_audit_log_has_required_columns(db):
    cursor = await db.execute("PRAGMA table_info(audit_log)")
    cols = {row[1] for row in await cursor.fetchall()}
    assert {"id", "created_at", "event_type", "identity_id", "payload_json"} <= cols


async def test_cursors_has_required_columns(db):
    cursor = await db.execute("PRAGMA table_info(cursors)")
    cols = {row[1] for row in await cursor.fetchall()}
    assert {"id", "cursor_type", "identity_id", "last_entity_id",
            "last_created_at", "last_imported_at", "raw_json", "updated_at"} <= cols


async def test_foreign_keys_enforced(db):
    with pytest.raises(aiosqlite.IntegrityError):
        await db.execute(
            "INSERT INTO projects (id, name, repo_path, identity_id) VALUES (?, ?, ?, ?)",
            ("p1", "test", "/tmp", "nonexistent-account-id"),
        )
        await db.commit()


async def test_work_items_status_index_exists(db):
    cursor = await db.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='work_items_status_created_at'"
    )
    row = await cursor.fetchone()
    assert row is not None
