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


async def test_get_pending_outbox_items_includes_row_below_max(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox_row(db_conn, id="ob-1", idempotency_key="k1", delivery_attempts=2)
    rows = await store.get_pending_outbox_items(max_attempts=3)
    assert len(rows) == 1
    assert rows[0]["id"] == "ob-1"


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


# ---------------------------------------------------------------------------
# Helpers for Phase 3 store tests
# ---------------------------------------------------------------------------

async def _seed_project(conn, *, id="proj-1", identity_id="acc-alice") -> None:
    await conn.execute(
        "INSERT INTO projects (id, name, repo_path, identity_id, agents_json) VALUES (?, ?, ?, ?, ?)",
        (id, "MyProject", "/repo/myproject", identity_id, '["claude-code"]'),
    )
    await conn.commit()


async def _seed_work_item(store, *, id="wi-1", identity_id="acc-alice",
                          status="pending", idempotency_key="k1") -> None:
    await store.upsert_work_item(
        id=id, source_type="dm", source_id="msg-1",
        identity_id=identity_id, summary="fix the bug",
        payload_json='{"key": "value"}', idempotency_key=idempotency_key,
    )
    if status != "pending":
        async with store._conn.execute(
            "UPDATE work_items SET status = ? WHERE id = ?", (status, id)
        ):
            pass
        await store._conn.commit()


# ---------------------------------------------------------------------------
# get_pending_work_items
# ---------------------------------------------------------------------------

async def test_get_pending_work_items_returns_pending(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    rows = await store.get_pending_work_items("acc-alice", limit=10)
    assert len(rows) == 1
    assert rows[0]["id"] == "wi-1"


async def test_get_pending_work_items_excludes_dispatched(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store, status="dispatched")
    rows = await store.get_pending_work_items("acc-alice", limit=10)
    assert rows == []


async def test_get_pending_work_items_excludes_other_identity(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await store.upsert_account(id="acc-bob", npub="npub1bob", label="bob", passphrase_ref="env:Y")
    await _seed_work_item(store, identity_id="acc-bob")
    rows = await store.get_pending_work_items("acc-alice", limit=10)
    assert rows == []


async def test_get_pending_work_items_respects_limit(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store, id="wi-1", idempotency_key="k1")
    await _seed_work_item(store, id="wi-2", idempotency_key="k2")
    await _seed_work_item(store, id="wi-3", idempotency_key="k3")
    rows = await store.get_pending_work_items("acc-alice", limit=2)
    assert len(rows) == 2


# ---------------------------------------------------------------------------
# claim_work_item
# ---------------------------------------------------------------------------

async def test_claim_work_item_returns_true_and_transitions_status(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    claimed = await store.claim_work_item("wi-1")
    assert claimed is True
    async with db_conn.execute("SELECT status FROM work_items WHERE id = 'wi-1'") as cur:
        row = await cur.fetchone()
    assert row["status"] == "dispatched"


async def test_claim_work_item_second_call_returns_false(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    first = await store.claim_work_item("wi-1")
    second = await store.claim_work_item("wi-1")
    assert first is True
    assert second is False


# ---------------------------------------------------------------------------
# upsert_agent_run
# ---------------------------------------------------------------------------

async def test_upsert_agent_run_inserts_row(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    await store.claim_work_item("wi-1")
    await store.upsert_agent_run("run-1", "wi-1", "claude-code")
    async with db_conn.execute("SELECT * FROM agent_runs WHERE id = 'run-1'") as cur:
        row = await cur.fetchone()
    assert row is not None
    assert row["adapter_type"] == "claude-code"
    assert row["status"] == "running"


async def test_upsert_agent_run_is_noop_on_duplicate(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    await store.claim_work_item("wi-1")
    await store.upsert_agent_run("run-1", "wi-1", "claude-code")
    await store.upsert_agent_run("run-1", "wi-1", "codex")  # second call is a no-op
    async with db_conn.execute("SELECT adapter_type FROM agent_runs WHERE id = 'run-1'") as cur:
        row = await cur.fetchone()
    assert row["adapter_type"] == "claude-code"  # unchanged


# ---------------------------------------------------------------------------
# update_agent_run
# ---------------------------------------------------------------------------

async def test_update_agent_run_sets_heartbeat(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    await store.claim_work_item("wi-1")
    await store.upsert_agent_run("run-1", "wi-1", "claude-code")
    await store.update_agent_run("run-1", heartbeat_at="2026-05-03T12:00:00Z")
    async with db_conn.execute("SELECT heartbeat_at, status FROM agent_runs WHERE id='run-1'") as cur:
        row = await cur.fetchone()
    assert row["heartbeat_at"] == "2026-05-03T12:00:00Z"
    assert row["status"] == "running"  # unchanged


async def test_update_agent_run_sets_status_and_result(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    await store.claim_work_item("wi-1")
    await store.upsert_agent_run("run-1", "wi-1", "claude-code")
    await store.update_agent_run("run-1", status="done", result_summary="all good")
    async with db_conn.execute("SELECT status, result_summary FROM agent_runs WHERE id='run-1'") as cur:
        row = await cur.fetchone()
    assert row["status"] == "done"
    assert row["result_summary"] == "all good"


async def test_update_agent_run_raises_if_no_fields(store: Store):
    with pytest.raises(ValueError, match="at least one field"):
        await store.update_agent_run("run-1")


# ---------------------------------------------------------------------------
# complete_work_item
# ---------------------------------------------------------------------------

async def test_complete_work_item_sets_status(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    await store.complete_work_item("wi-1", "done")
    async with db_conn.execute("SELECT status FROM work_items WHERE id='wi-1'") as cur:
        row = await cur.fetchone()
    assert row["status"] == "done"


# ---------------------------------------------------------------------------
# get_project_for_identity
# ---------------------------------------------------------------------------

async def test_get_project_for_identity_returns_row(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_project(db_conn, identity_id="acc-alice")
    row = await store.get_project_for_identity("acc-alice")
    assert row is not None
    assert row["id"] == "proj-1"


async def test_get_project_for_identity_returns_none_when_missing(store: Store):
    result = await store.get_project_for_identity("acc-nobody")
    assert result is None


# ---------------------------------------------------------------------------
# insert_outbox_item
# ---------------------------------------------------------------------------

async def test_insert_outbox_item_inserts_row(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await store.insert_outbox_item(
        id="ob-1", identity_id="acc-alice", dest_pubkey="npub1op",
        message_text="all done", idempotency_key="idem-1",
    )
    async with db_conn.execute("SELECT * FROM outbox WHERE id='ob-1'") as cur:
        row = await cur.fetchone()
    assert row is not None
    assert row["message_text"] == "all done"
    assert row["delivery_status"] == "pending"


async def test_insert_outbox_item_idempotent_on_same_key(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await store.insert_outbox_item("ob-1", "acc-alice", "npub1op", "first", "idem-1")
    await store.insert_outbox_item("ob-2", "acc-alice", "npub1op", "second", "idem-1")
    async with db_conn.execute("SELECT COUNT(*) FROM outbox") as cur:
        row = await cur.fetchone()
    assert row[0] == 1


# ===========================================================================
# Phase 4 Store methods
# ===========================================================================

async def _seed_work_item_phase4(conn, *, id, identity_id, status="pending"):
    await conn.execute(
        """
        INSERT INTO work_items (id, source_type, source_id, identity_id, status, idempotency_key)
        VALUES (?, 'dm', ?, ?, ?, ?)
        """,
        (id, id, identity_id, status, f"idem-{id}"),
    )
    await conn.commit()


async def _seed_approval(conn, *, id, work_item_id, status="pending"):
    await conn.execute(
        """
        INSERT INTO approvals (id, work_item_id, action_description, status)
        VALUES (?, ?, 'do something', ?)
        """,
        (id, work_item_id, status),
    )
    await conn.commit()


# get_work_item
# ---------------------------------------------------------------------------

async def test_get_work_item_returns_row(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice")
    row = await store.get_work_item("wi-1")
    assert row is not None
    assert row["id"] == "wi-1"


async def test_get_work_item_returns_none_for_missing(store: Store):
    result = await store.get_work_item("nonexistent")
    assert result is None


# get_latest_work_item
# ---------------------------------------------------------------------------

async def test_get_latest_work_item_returns_most_recent(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice")
    await _seed_work_item_phase4(db_conn, id="wi-2", identity_id="acc-alice")
    row = await store.get_latest_work_item("acc-alice")
    assert row is not None
    assert row["id"] == "wi-2"


async def test_get_latest_work_item_returns_none_when_empty(store: Store):
    result = await store.get_latest_work_item("acc-nobody")
    assert result is None


# get_latest_dispatched_work_item
# ---------------------------------------------------------------------------

async def test_get_latest_dispatched_work_item_finds_dispatched(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice", status="pending")
    await _seed_work_item_phase4(db_conn, id="wi-2", identity_id="acc-alice", status="dispatched")
    row = await store.get_latest_dispatched_work_item("acc-alice")
    assert row is not None
    assert row["id"] == "wi-2"


async def test_get_latest_dispatched_work_item_finds_cancel_requested(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice", status="cancel_requested")
    row = await store.get_latest_dispatched_work_item("acc-alice")
    assert row is not None
    assert row["id"] == "wi-1"


async def test_get_latest_dispatched_work_item_ignores_pending(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice", status="pending")
    result = await store.get_latest_dispatched_work_item("acc-alice")
    assert result is None


# mark_work_item_cancel_requested
# ---------------------------------------------------------------------------

async def test_mark_work_item_cancel_requested_updates_status(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice", status="dispatched")
    await store.mark_work_item_cancel_requested("wi-1")
    row = await store.get_work_item("wi-1")
    assert row["status"] == "cancel_requested"


# get_pending_approval
# ---------------------------------------------------------------------------

async def test_get_pending_approval_returns_most_recent(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice")
    await _seed_approval(db_conn, id="appr-1", work_item_id="wi-1")
    row = await store.get_pending_approval("acc-alice")
    assert row is not None
    assert row["id"] == "appr-1"


async def test_get_pending_approval_ignores_resolved(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice")
    await _seed_approval(db_conn, id="appr-1", work_item_id="wi-1", status="approved")
    result = await store.get_pending_approval("acc-alice")
    assert result is None


async def test_get_pending_approval_returns_none_when_empty(store: Store):
    result = await store.get_pending_approval("acc-nobody")
    assert result is None


# resolve_approval
# ---------------------------------------------------------------------------

async def test_resolve_approval_approved(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice")
    await _seed_approval(db_conn, id="appr-1", work_item_id="wi-1")
    await store.resolve_approval("appr-1", "approved")
    async with db_conn.execute("SELECT status, resolved_at FROM approvals WHERE id='appr-1'") as cur:
        row = await cur.fetchone()
    assert row["status"] == "approved"
    assert row["resolved_at"] is not None


async def test_resolve_approval_rejected(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice")
    await _seed_approval(db_conn, id="appr-1", work_item_id="wi-1")
    await store.resolve_approval("appr-1", "rejected")
    async with db_conn.execute("SELECT status FROM approvals WHERE id='appr-1'") as cur:
        row = await cur.fetchone()
    assert row["status"] == "rejected"


# get_project_groups
# ---------------------------------------------------------------------------

async def test_get_project_groups_returns_list(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO projects (id, name, repo_path, identity_id, groups_json) "
        "VALUES ('proj-1', 'P', '/repo', 'acc-alice', '[\"grp-1\", \"grp-2\"]')"
    )
    await db_conn.commit()
    groups = await store.get_project_groups("acc-alice")
    assert groups == ["grp-1", "grp-2"]


async def test_get_project_groups_returns_empty_when_no_project(store: Store):
    result = await store.get_project_groups("acc-nobody")
    assert result == []


async def test_get_project_groups_returns_empty_list_for_empty_json(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO projects (id, name, repo_path, identity_id, groups_json) "
        "VALUES ('proj-1', 'P', '/repo', 'acc-alice', '[]')"
    )
    await db_conn.commit()
    groups = await store.get_project_groups("acc-alice")
    assert groups == []


# insert_outbox_item with dest_group_id
# ---------------------------------------------------------------------------

async def test_insert_outbox_item_with_group_id(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await store.insert_outbox_item(
        "ob-grp-1", "acc-alice", None, "done", "idem-grp-1",
        dest_group_id="grp-1",
    )
    async with db_conn.execute("SELECT dest_pubkey, dest_group_id FROM outbox WHERE id='ob-grp-1'") as cur:
        row = await cur.fetchone()
    assert row["dest_pubkey"] is None
    assert row["dest_group_id"] == "grp-1"


# upsert_work_item return value
# ---------------------------------------------------------------------------

async def test_upsert_work_item_returns_true_on_first_insert(store: Store):
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X"
    )
    result = await store.upsert_work_item(
        id="wi-1",
        source_type="kanban",
        source_id="card-1",
        identity_id="acc-alice",
        summary="Fix bug",
        payload_json="{}",
        idempotency_key="kanban-card-1",
    )
    assert result is True


async def test_upsert_work_item_returns_false_on_duplicate(store: Store):
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X"
    )
    await store.upsert_work_item(
        id="wi-1",
        source_type="kanban",
        source_id="card-1",
        identity_id="acc-alice",
        summary="Fix bug",
        payload_json="{}",
        idempotency_key="kanban-card-1",
    )
    # Different id but same idempotency_key — the UNIQUE constraint on idempotency_key causes INSERT OR IGNORE
    result = await store.upsert_work_item(
        id="wi-2",
        source_type="kanban",
        source_id="card-1",
        identity_id="acc-alice",
        summary="Fix bug",
        payload_json="{}",
        idempotency_key="kanban-card-1",
    )
    assert result is False
