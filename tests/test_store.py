import pytest
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
