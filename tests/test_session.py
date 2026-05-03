import json
import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock
from deskbridge.mcp.session import SessionBroker
from deskbridge.mcp.client import McpClient, McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.models import McpError, McpErrorCategory
from deskbridge.config import IdentityConfig


def make_identity(label: str = "alice", npub: str = "npub1alice") -> IdentityConfig:
    return IdentityConfig(
        label=label, npub=npub, passphrase_ref="env:ALICE"
    )


@pytest.fixture
def mock_client():
    client = AsyncMock(spec=McpClient)
    return client


@pytest.fixture
async def broker(store, mock_client):
    return SessionBroker(
        store=store,
        client=mock_client,
        identities=[make_identity()],
    )


async def test_unlock_caches_session_id(broker, store, mock_client, monkeypatch):
    monkeypatch.setenv("ALICE", "passphrase123")
    mock_client.call_tool.return_value = {"session_id": "sess-abc"}

    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:ALICE"
    )
    await broker.unlock_all()

    acc = await store.get_account(id="acc-alice")
    assert acc["session_id"] == "sess-abc"
    assert acc["health"] == "ok"


async def test_unlock_logs_audit_event(broker, store, mock_client, monkeypatch):
    monkeypatch.setenv("ALICE", "passphrase123")
    mock_client.call_tool.return_value = {"session_id": "sess-abc"}
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:ALICE"
    )

    await broker.unlock_all()

    events = await store.get_audit_events("session_unlocked")
    assert len(events) == 1


async def test_session_id_for_known_identity(broker, store, mock_client, monkeypatch):
    monkeypatch.setenv("ALICE", "passphrase123")
    mock_client.call_tool.return_value = {"session_id": "sess-xyz"}
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:ALICE"
    )
    await broker.unlock_all()

    session_id = await broker.get_session_id(identity_label="alice")
    assert session_id == "sess-xyz"


async def test_session_id_unknown_identity_returns_none(broker):
    session_id = await broker.get_session_id(identity_label="nobody")
    assert session_id is None


async def test_unlock_failure_sets_health_degraded(broker, store, mock_client, monkeypatch):
    monkeypatch.setenv("ALICE", "passphrase123")
    mock_client.call_tool.side_effect = McpToolError(
        mcp_error=McpError(
            category=McpErrorCategory.INTERNAL_ERROR,
            message="nostrdesk-mcp crashed",
        ),
        routing=RoutingDecision.RETRY,
    )
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:ALICE"
    )

    await broker.unlock_all()

    acc = await store.get_account(id="acc-alice")
    assert acc["health"] == "degraded"


async def test_capture_approval_persists_in_store(broker, store):
    await broker.capture_approval(
        mcp_approval_id="mcp-appr-999",
        action_description="Send 1000 sats",
        scope="wallet",
        request_text="Approve this payment?",
        expires_at=None,
    )
    appr = await store.get_approval_by_mcp_id("mcp-appr-999")
    assert appr is not None
    assert appr["mcp_approval_id"] == "mcp-appr-999"
    assert appr["status"] == "pending"


async def test_refresh_skips_healthy_identity(broker, store, mock_client, monkeypatch):
    monkeypatch.setenv("ALICE", "passphrase123")
    mock_client.call_tool.return_value = {"session_id": "sess-1"}
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:ALICE"
    )
    await broker.unlock_all()
    mock_client.call_tool.reset_mock()

    await broker.refresh_if_needed()

    mock_client.call_tool.assert_not_called()


async def test_refresh_re_unlocks_degraded_identity(broker, store, mock_client, monkeypatch):
    monkeypatch.setenv("ALICE", "passphrase123")
    mock_client.call_tool.return_value = {"session_id": "sess-refreshed"}
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:ALICE"
    )
    await store.update_account_session(id="acc-alice", session_id=None, health="degraded")

    await broker.refresh_if_needed()

    acc = await store.get_account(id="acc-alice")
    assert acc["health"] == "ok"
    assert acc["session_id"] == "sess-refreshed"


async def test_unlock_failure_clears_in_memory_session(broker, store, mock_client, monkeypatch):
    monkeypatch.setenv("ALICE", "passphrase123")
    mock_client.call_tool.return_value = {"session_id": "sess-stale"}
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:ALICE"
    )
    await broker.unlock_all()
    assert await broker.get_session_id("alice") == "sess-stale"

    mock_client.call_tool.side_effect = McpToolError(
        mcp_error=McpError(category=McpErrorCategory.INTERNAL_ERROR, message="crashed"),
        routing=RoutingDecision.RETRY,
    )
    await store.update_account_session(id="acc-alice", session_id=None, health="degraded")
    await broker.refresh_if_needed()

    assert await broker.get_session_id("alice") is None
