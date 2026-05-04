import uuid

import aiosqlite

from deskbridge.config import DeskBridgeConfig


class Store:
    def __init__(self, conn: aiosqlite.Connection) -> None:
        self._conn = conn

    async def upsert_account(
        self,
        id: str,
        npub: str,
        label: str,
        passphrase_ref: str,
    ) -> None:
        async with self._conn.execute(
            """
            INSERT INTO accounts (id, npub, label, passphrase_ref)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                npub = excluded.npub,
                label = excluded.label,
                passphrase_ref = excluded.passphrase_ref
            """,
            (id, npub, label, passphrase_ref),
        ):
            pass
        await self._conn.commit()

    async def get_account(self, id: str) -> aiosqlite.Row | None:
        async with self._conn.execute(
            "SELECT * FROM accounts WHERE id = ?", (id,)
        ) as row_cursor:
            return await row_cursor.fetchone()

    async def update_account_session(
        self,
        id: str,
        session_id: str | None,
        health: str,
    ) -> None:
        async with self._conn.execute(
            """
            UPDATE accounts
            SET session_id = ?,
                health = ?,
                last_unlocked_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE id = ?
            """,
            (session_id, health, id),
        ):
            pass
        await self._conn.commit()

    async def upsert_cursor(
        self,
        cursor_type: str,
        identity_id: str,
        last_entity_id: str | None,
        last_created_at: str | None,
        last_imported_at: str | None,
        raw_json: str,
    ) -> None:
        async with self._conn.execute(
            """
            INSERT INTO cursors
                (id, cursor_type, identity_id, last_entity_id,
                 last_created_at, last_imported_at, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cursor_type, identity_id) DO UPDATE SET
                last_entity_id   = excluded.last_entity_id,
                last_created_at  = excluded.last_created_at,
                last_imported_at = excluded.last_imported_at,
                raw_json         = excluded.raw_json,
                updated_at       = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            """,
            (
                str(uuid.uuid4()),
                cursor_type,
                identity_id,
                last_entity_id,
                last_created_at,
                last_imported_at,
                raw_json,
            ),
        ):
            pass
        await self._conn.commit()

    async def get_cursor(
        self, cursor_type: str, identity_id: str
    ) -> aiosqlite.Row | None:
        async with self._conn.execute(
            "SELECT * FROM cursors WHERE cursor_type = ? AND identity_id = ?",
            (cursor_type, identity_id),
        ) as row_cursor:
            return await row_cursor.fetchone()

    async def insert_approval(
        self,
        id: str,
        mcp_approval_id: str | None,
        work_item_id: str | None,
        action_description: str,
        scope: str | None,
        request_text: str | None,
        expires_at: str | None,
    ) -> None:
        async with self._conn.execute(
            """
            INSERT INTO approvals
                (id, mcp_approval_id, work_item_id, action_description,
                 scope, request_text, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (id, mcp_approval_id, work_item_id, action_description,
             scope, request_text, expires_at),
        ):
            pass
        await self._conn.commit()

    async def get_approval(self, id: str) -> aiosqlite.Row | None:
        async with self._conn.execute(
            "SELECT * FROM approvals WHERE id = ?", (id,)
        ) as row_cursor:
            return await row_cursor.fetchone()

    async def get_approval_by_mcp_id(self, mcp_approval_id: str) -> aiosqlite.Row | None:
        async with self._conn.execute(
            "SELECT * FROM approvals WHERE mcp_approval_id = ?", (mcp_approval_id,)
        ) as row_cursor:
            return await row_cursor.fetchone()

    async def log_audit(
        self,
        id: str,
        event_type: str,
        identity_id: str | None = None,
        project_id: str | None = None,
        work_item_id: str | None = None,
        payload_json: str = "{}",
    ) -> None:
        async with self._conn.execute(
            """
            INSERT INTO audit_log
                (id, event_type, identity_id, project_id, work_item_id, payload_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (id, event_type, identity_id, project_id, work_item_id, payload_json),
        ):
            pass
        await self._conn.commit()

    async def get_audit_events(self, event_type: str) -> list[aiosqlite.Row]:
        async with self._conn.execute(
            "SELECT * FROM audit_log WHERE event_type = ? ORDER BY created_at",
            (event_type,),
        ) as row_cursor:
            return await row_cursor.fetchall()

    async def upsert_work_item(
        self,
        id: str,
        source_type: str,
        source_id: str,
        identity_id: str,
        summary: str,
        payload_json: str,
        idempotency_key: str,
    ) -> None:
        async with self._conn.execute(
            """
            INSERT OR IGNORE INTO work_items
                (id, source_type, source_id, identity_id, summary, payload_json, idempotency_key)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (id, source_type, source_id, identity_id, summary, payload_json, idempotency_key),
        ):
            pass
        await self._conn.commit()

    async def get_pending_outbox_items(self, max_attempts: int = 3) -> list[aiosqlite.Row]:
        async with self._conn.execute(
            """
            SELECT * FROM outbox
            WHERE delivery_status = 'pending'
              AND delivery_attempts < ?
            ORDER BY created_at
            """,
            (max_attempts,),
        ) as cur:
            return await cur.fetchall()

    async def update_outbox_delivery(
        self,
        id: str,
        delivery_status: str,
        delivery_result_json: str,
    ) -> None:
        async with self._conn.execute(
            """
            UPDATE outbox
            SET delivery_status = ?,
                delivery_attempts = delivery_attempts + 1,
                delivery_result_json = ?,
                delivered_at = CASE WHEN ? = 'delivered'
                               THEN strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                               ELSE delivered_at END
            WHERE id = ?
            """,
            (delivery_status, delivery_result_json, delivery_status, id),
        ):
            pass
        await self._conn.commit()


async def bootstrap_accounts_from_config(store: Store, config: DeskBridgeConfig) -> None:
    for identity in config.identities:
        await store.upsert_account(
            id=f"acc-{identity.label}",
            npub=identity.npub,
            label=identity.label,
            passphrase_ref=identity.passphrase_ref,
        )
