import pytest
import aiosqlite
from deskbridge.db.store import Store, bootstrap_accounts_from_config
from deskbridge.config import DeskBridgeConfig, SupervisorConfig, McpConfig, IdentityConfig


def make_config() -> DeskBridgeConfig:
    return DeskBridgeConfig(
        supervisor=SupervisorConfig(db_path="/tmp/test.db"),
        mcp=McpConfig(command="nostrdesk-mcp"),
        identities=[
            IdentityConfig(label="alice", npub="npub1alice", passphrase_ref="env:ALICE"),
            IdentityConfig(label="bob", npub="npub1bob", passphrase_ref="env:BOB"),
        ],
    )


async def test_upsert_account_and_get(store: Store):
    await store.upsert_account(
        id="acc-1",
        npub="npub1alice",
        label="alice",
        passphrase_ref="env:ALICE",
    )
    acc = await store.get_account(id="acc-1")
    assert acc is not None
    assert acc["label"] == "alice"
    assert acc["health"] == "unknown"


async def test_update_account_session(store: Store):
    await store.upsert_account(id="acc-1", npub="npub1alice", label="alice", passphrase_ref="env:A")
    await store.update_account_session(id="acc-1", session_id="sess-abc", health="ok")
    acc = await store.get_account(id="acc-1")
    assert acc["session_id"] == "sess-abc"
    assert acc["health"] == "ok"


async def test_get_account_missing_returns_none(store: Store):
    acc = await store.get_account(id="nonexistent")
    assert acc is None


async def test_upsert_cursor_and_get(store: Store):
    await store.upsert_account(id="acc-1", npub="npub1alice", label="alice", passphrase_ref="env:A")
    await store.upsert_cursor(
        cursor_type="dm_watcher",
        identity_id="acc-1",
        last_entity_id="msg-100",
        last_created_at="2026-01-01T00:00:00Z",
        last_imported_at=None,
        raw_json='{"last_entity_id": "msg-100"}',
    )
    cursor_row = await store.get_cursor(cursor_type="dm_watcher", identity_id="acc-1")
    assert cursor_row is not None
    assert cursor_row["last_entity_id"] == "msg-100"


async def test_upsert_cursor_overwrites(store: Store):
    await store.upsert_account(id="acc-1", npub="npub1alice", label="alice", passphrase_ref="env:A")
    await store.upsert_cursor(
        cursor_type="dm_watcher", identity_id="acc-1",
        last_entity_id="msg-1", last_created_at=None, last_imported_at=None, raw_json="{}"
    )
    await store.upsert_cursor(
        cursor_type="dm_watcher", identity_id="acc-1",
        last_entity_id="msg-2", last_created_at=None, last_imported_at=None, raw_json="{}"
    )
    cursor_row = await store.get_cursor(cursor_type="dm_watcher", identity_id="acc-1")
    assert cursor_row["last_entity_id"] == "msg-2"


async def test_insert_approval_and_get(store: Store):
    await store.insert_approval(
        id="appr-1",
        mcp_approval_id="mcp-appr-abc",
        work_item_id=None,
        action_description="Send payment",
        scope="wallet",
        request_text="Approve sending 1000 sats?",
        expires_at="2026-12-31T23:59:59Z",
    )
    appr = await store.get_approval(id="appr-1")
    assert appr is not None
    assert appr["status"] == "pending"
    assert appr["mcp_approval_id"] == "mcp-appr-abc"


async def test_get_approval_by_mcp_id(store: Store):
    await store.insert_approval(
        id="appr-2",
        mcp_approval_id="mcp-appr-xyz",
        work_item_id=None,
        action_description="Transfer funds",
        scope="wallet",
        request_text="Approve?",
        expires_at=None,
    )
    appr = await store.get_approval_by_mcp_id("mcp-appr-xyz")
    assert appr is not None
    assert appr["id"] == "appr-2"
    assert appr["status"] == "pending"


async def test_log_audit_event(store: Store):
    await store.log_audit(
        id="evt-1",
        event_type="session_unlocked",
        identity_id="acc-1",
        payload_json='{"session_id": "sess-1"}',
    )
    events = await store.get_audit_events(event_type="session_unlocked")
    assert len(events) == 1
    assert events[0]["id"] == "evt-1"


async def test_bootstrap_accounts_creates_accounts(store: Store):
    config = make_config()
    await bootstrap_accounts_from_config(store=store, config=config)
    alice = await store.get_account(id="acc-alice")
    bob = await store.get_account(id="acc-bob")
    assert alice is not None
    assert alice["npub"] == "npub1alice"
    assert bob is not None
    assert bob["npub"] == "npub1bob"


async def test_bootstrap_accounts_is_idempotent(store: Store):
    config = make_config()
    await bootstrap_accounts_from_config(store=store, config=config)
    await bootstrap_accounts_from_config(store=store, config=config)
    alice = await store.get_account(id="acc-alice")
    assert alice is not None
    assert alice["label"] == "alice"


async def test_cursor_rejects_unknown_identity(store: Store):
    with pytest.raises(aiosqlite.IntegrityError):
        await store.upsert_cursor(
            cursor_type="dm_watcher",
            identity_id="acc-does-not-exist",
            last_entity_id=None,
            last_created_at=None,
            last_imported_at=None,
            raw_json="{}",
        )


async def test_upsert_work_item_inserts(store: Store):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await store.upsert_work_item(
        id="wi-1",
        source_type="dm",
        source_id="msg-1",
        identity_id="acc-alice",
        summary="hello world",
        payload_json='{"id":"msg-1","content":"hello world"}',
        idempotency_key="msg-1",
    )
    async with store._conn.execute("SELECT * FROM work_items WHERE id = 'wi-1'") as cur:
        row = await cur.fetchone()
    assert row is not None
    assert row["source_type"] == "dm"
    assert row["source_id"] == "msg-1"
    assert row["idempotency_key"] == "msg-1"
    assert row["summary"] == "hello world"


async def test_upsert_work_item_idempotent(store: Store):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    for _ in range(2):
        await store.upsert_work_item(
            id="wi-1",
            source_type="dm",
            source_id="msg-1",
            identity_id="acc-alice",
            summary="hello",
            payload_json="{}",
            idempotency_key="msg-1",
        )
    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 1


async def _seed_outbox_row(
    conn,
    *,
    id: str,
    identity_id: str = "acc-alice",
    dest_pubkey: str = "npub1dest",
    message_text: str = "hello",
    idempotency_key: str,
    delivery_status: str = "pending",
    delivery_attempts: int = 0,
) -> None:
    await conn.execute(
        """
        INSERT INTO outbox
            (id, identity_id, dest_pubkey, message_text, idempotency_key,
             delivery_status, delivery_attempts)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (id, identity_id, dest_pubkey, message_text, idempotency_key,
         delivery_status, delivery_attempts),
    )
    await conn.commit()


async def test_get_pending_outbox_items_returns_pending(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox_row(db_conn, id="ob-1", idempotency_key="k1")
    rows = await store.get_pending_outbox_items(max_attempts=3)
    assert len(rows) == 1
    assert rows[0]["id"] == "ob-1"


async def test_get_pending_outbox_items_excludes_exhausted(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox_row(db_conn, id="ob-1", idempotency_key="k1", delivery_attempts=3)
    rows = await store.get_pending_outbox_items(max_attempts=3)
    assert rows == []


async def test_get_pending_outbox_items_excludes_failed(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox_row(db_conn, id="ob-1", idempotency_key="k1", delivery_status="failed")
    rows = await store.get_pending_outbox_items(max_attempts=3)
    assert rows == []


async def test_update_outbox_delivery_delivered(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox_row(db_conn, id="ob-1", idempotency_key="k1")
    await store.update_outbox_delivery("ob-1", "delivered", '{"ok":true}')
    async with db_conn.execute("SELECT * FROM outbox WHERE id = 'ob-1'") as cur:
        row = await cur.fetchone()
    assert row["delivery_status"] == "delivered"
    assert row["delivery_attempts"] == 1
    assert row["delivered_at"] is not None
    assert row["delivery_result_json"] == '{"ok":true}'


async def test_update_outbox_delivery_pending_increments(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox_row(db_conn, id="ob-1", idempotency_key="k1")
    await store.update_outbox_delivery("ob-1", "pending", '{"error":"transient"}')
    async with db_conn.execute("SELECT * FROM outbox WHERE id = 'ob-1'") as cur:
        row = await cur.fetchone()
    assert row["delivery_status"] == "pending"
    assert row["delivery_attempts"] == 1
    assert row["delivered_at"] is None


async def test_update_outbox_delivery_failed(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox_row(db_conn, id="ob-1", idempotency_key="k1")
    await store.update_outbox_delivery("ob-1", "failed", '{"error":"reject"}')
    async with db_conn.execute("SELECT * FROM outbox WHERE id = 'ob-1'") as cur:
        row = await cur.fetchone()
    assert row["delivery_status"] == "failed"
    assert row["delivery_attempts"] == 1
    assert row["delivered_at"] is None
