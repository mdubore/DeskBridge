import json
import uuid
import structlog

from deskbridge.config import IdentityConfig
from deskbridge.db.store import Store
from deskbridge.mcp.client import McpClient, McpToolError

log = structlog.get_logger()


class SessionBroker:
    def __init__(
        self,
        store: Store,
        client: McpClient,
        identities: list[IdentityConfig],
    ) -> None:
        self._store = store
        self._client = client
        self._identities = {i.label: i for i in identities}
        self._session_ids: dict[str, str] = {}

    def _account_id(self, label: str) -> str:
        return f"acc-{label}"

    async def unlock_all(self) -> None:
        for label, identity in self._identities.items():
            await self._unlock_identity(label, identity)

    async def _unlock_identity(self, label: str, identity: IdentityConfig) -> None:
        account_id = self._account_id(label)
        try:
            passphrase = identity.resolve_passphrase()
        except Exception as e:
            log.error("passphrase_resolve_failed", identity=label, error=str(e))
            await self._store.update_account_session(
                id=account_id, session_id=None, health="degraded"
            )
            return

        try:
            result = await self._client.call_tool(
                "unlock_identity",
                {"npub": identity.npub, "passphrase": passphrase},
            )
            session_id = result["session_id"]
            self._session_ids[label] = session_id
            await self._store.update_account_session(
                id=account_id, session_id=session_id, health="ok"
            )
            await self._store.log_audit(
                id=str(uuid.uuid4()),
                event_type="session_unlocked",
                identity_id=account_id,
                payload_json=json.dumps({"session_id": session_id}),
            )
            log.info("identity_unlocked", identity=label, session_id=session_id)
        except McpToolError as e:
            log.error(
                "unlock_failed",
                identity=label,
                category=e.mcp_error.category,
                message=e.mcp_error.message,
            )
            await self._store.update_account_session(
                id=account_id, session_id=None, health="degraded"
            )
            await self._store.log_audit(
                id=str(uuid.uuid4()),
                event_type="session_unlock_failed",
                identity_id=account_id,
                payload_json=json.dumps({
                    "category": e.mcp_error.category,
                    "message": e.mcp_error.message,
                }),
            )

    async def get_session_id(self, identity_label: str) -> str | None:
        return self._session_ids.get(identity_label)

    async def refresh_if_needed(self) -> None:
        for label, identity in self._identities.items():
            account_id = self._account_id(label)
            row = await self._store.get_account(id=account_id)
            if row is None or row["health"] != "ok":
                log.info("refreshing_degraded_session", identity=label)
                await self._unlock_identity(label, identity)

    async def capture_approval(
        self,
        mcp_approval_id: str,
        action_description: str,
        scope: str | None,
        request_text: str | None,
        expires_at: str | None,
        work_item_id: str | None = None,
    ) -> None:
        await self._store.insert_approval(
            id=str(uuid.uuid4()),
            mcp_approval_id=mcp_approval_id,
            work_item_id=work_item_id,
            action_description=action_description,
            scope=scope,
            request_text=request_text,
            expires_at=expires_at,
        )
        log.warning(
            "approval_captured",
            mcp_approval_id=mcp_approval_id,
            action=action_description,
        )
