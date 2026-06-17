import json
import pytest
from deskbridge.dm.approval_resolution import resolve_and_audit


async def test_resolve_and_audit_includes_via_when_provided(store, db_conn):
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X"
    )
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, "
        "status, idempotency_key) "
        "VALUES ('wi-1', 'dm', 'src-1', 'acc-alice', 'pending', 'k1')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status) "
        "VALUES ('appr-1', 'wi-1', 'do stuff', 'pending')"
    )
    await db_conn.commit()
    row = await store.get_approval("appr-1")
    await resolve_and_audit(store, "acc-alice", row, "approved", via="cli")
    audits = await store.get_audit_events("approval_resolved")
    assert len(audits) == 1
    payload = json.loads(audits[0]["payload_json"])
    assert payload["via"] == "cli"
    assert payload["resolution"] == "approved"
    assert payload["approval_id"] == "appr-1"


async def test_resolve_and_audit_omits_via_when_not_provided(store, db_conn):
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X"
    )
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, "
        "status, idempotency_key) "
        "VALUES ('wi-2', 'dm', 'src-2', 'acc-alice', 'pending', 'k2')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status) "
        "VALUES ('appr-2', 'wi-2', 'other stuff', 'pending')"
    )
    await db_conn.commit()
    row = await store.get_approval("appr-2")
    await resolve_and_audit(store, "acc-alice", row, "rejected")
    audits = await store.get_audit_events("approval_resolved")
    assert len(audits) == 1
    payload = json.loads(audits[0]["payload_json"])
    assert "via" not in payload
    assert payload["resolution"] == "rejected"
    assert payload["approval_id"] == "appr-2"


async def test_resolve_and_audit_skips_audit_when_guard_blocks(store, db_conn):
    """If resolve_approval returns False (already resolved), no audit is written."""
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X"
    )
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, "
        "status, idempotency_key) "
        "VALUES ('wi-3', 'dm', 'src-3', 'acc-alice', 'pending', 'k3')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status) "
        "VALUES ('appr-3', 'wi-3', 'already done', 'approved')"
    )
    await db_conn.commit()
    row = await store.get_approval("appr-3")
    await resolve_and_audit(store, "acc-alice", row, "rejected")
    audits = await store.get_audit_events("approval_resolved")
    assert len(audits) == 0
