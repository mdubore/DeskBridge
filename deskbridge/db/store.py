import json
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

    async def upsert_project(
        self,
        id: str,
        name: str,
        repo_path: str,
        identity_id: str,
        adapter: str,
        openclaw_agent_id: str | None,
        boards_json: str,
        groups_json: str,
        allowed_actions_json: str,
        escalation_dm_target: str | None,
    ) -> None:
        async with self._conn.execute(
            """
            INSERT INTO projects
                (id, name, repo_path, identity_id, adapter, openclaw_agent_id,
                 boards_json, groups_json, allowed_actions_json, escalation_dm_target)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name                 = excluded.name,
                repo_path            = excluded.repo_path,
                identity_id          = excluded.identity_id,
                adapter              = excluded.adapter,
                openclaw_agent_id    = excluded.openclaw_agent_id,
                boards_json          = excluded.boards_json,
                groups_json          = excluded.groups_json,
                allowed_actions_json = excluded.allowed_actions_json,
                escalation_dm_target = excluded.escalation_dm_target
            """,
            (id, name, repo_path, identity_id, adapter, openclaw_agent_id,
             boards_json, groups_json, allowed_actions_json, escalation_dm_target),
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
        identity_id: str | None = None,
    ) -> None:
        async with self._conn.execute(
            """
            INSERT OR IGNORE INTO approvals
                (id, mcp_approval_id, work_item_id, identity_id, action_description,
                 scope, request_text, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (id, mcp_approval_id, work_item_id, identity_id, action_description,
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
    ) -> bool:
        async with self._conn.execute(
            """
            INSERT OR IGNORE INTO work_items
                (id, source_type, source_id, identity_id, summary, payload_json, idempotency_key)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (id, source_type, source_id, identity_id, summary, payload_json, idempotency_key),
        ) as cur:
            inserted = cur.rowcount == 1
        await self._conn.commit()
        return inserted

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

    async def get_pending_work_items(
        self, identity_id: str, limit: int = 10
    ) -> list[aiosqlite.Row]:
        async with self._conn.execute(
            """
            SELECT * FROM work_items
            WHERE status = 'pending' AND identity_id = ?
              AND (next_retry_at IS NULL OR next_retry_at <= strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            ORDER BY priority, created_at
            LIMIT ?
            """,
            (identity_id, limit),
        ) as cur:
            return await cur.fetchall()

    async def claim_work_item(self, id: str) -> bool:
        async with self._conn.execute(
            """
            UPDATE work_items
            SET status = 'dispatched',
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE id = ? AND status = 'pending'
            """,
            (id,),
        ) as cur:
            claimed = cur.rowcount > 0
        await self._conn.commit()
        return claimed

    async def upsert_agent_run(
        self, id: str, work_item_id: str, adapter_type: str
    ) -> None:
        async with self._conn.execute(
            "INSERT OR IGNORE INTO agent_runs (id, work_item_id, adapter_type) VALUES (?, ?, ?)",
            (id, work_item_id, adapter_type),
        ):
            pass
        await self._conn.commit()

    async def update_agent_run(
        self,
        id: str,
        *,
        status: str | None = None,
        result_summary: str | None = None,
        heartbeat_at: str | None = None,
    ) -> None:
        if status is None and result_summary is None and heartbeat_at is None:
            raise ValueError("at least one field must be provided to update_agent_run")
        parts = []
        params: list = []
        if status is not None:
            parts.append("status = ?")
            params.append(status)
        if result_summary is not None:
            parts.append("result_summary = ?")
            params.append(result_summary)
        if heartbeat_at is not None:
            parts.append("heartbeat_at = ?")
            params.append(heartbeat_at)
        parts.append("updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')")
        params.append(id)
        sql = f"UPDATE agent_runs SET {', '.join(parts)} WHERE id = ?"
        async with self._conn.execute(sql, params):
            pass
        await self._conn.commit()

    async def complete_work_item(self, id: str, status: str) -> None:
        async with self._conn.execute(
            """
            UPDATE work_items
            SET status = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE id = ?
            """,
            (status, id),
        ):
            pass
        await self._conn.commit()

    async def retry_work_item(self, id: str, next_retry_at: str) -> None:
        async with self._conn.execute(
            """
            UPDATE work_items
            SET status = 'pending',
                attempt_count = attempt_count + 1,
                next_retry_at = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE id = ?
            """,
            (next_retry_at, id),
        ):
            pass
        await self._conn.commit()

    async def get_project_for_identity(self, identity_id: str) -> aiosqlite.Row | None:
        async with self._conn.execute(
            "SELECT * FROM projects WHERE identity_id = ? LIMIT 1",
            (identity_id,),
        ) as cur:
            return await cur.fetchone()

    async def insert_outbox_item(
        self,
        id: str,
        identity_id: str,
        dest_pubkey: str | None,
        message_text: str,
        idempotency_key: str,
        dest_group_id: str | None = None,
    ) -> None:
        async with self._conn.execute(
            """
            INSERT OR IGNORE INTO outbox
                (id, identity_id, dest_pubkey, dest_group_id, message_text, idempotency_key)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (id, identity_id, dest_pubkey, dest_group_id, message_text, idempotency_key),
        ):
            pass
        await self._conn.commit()

    async def get_work_item(self, id: str) -> aiosqlite.Row | None:
        async with self._conn.execute(
            "SELECT * FROM work_items WHERE id = ?", (id,)
        ) as cur:
            return await cur.fetchone()

    async def get_latest_work_item(self, identity_id: str) -> aiosqlite.Row | None:
        async with self._conn.execute(
            "SELECT * FROM work_items WHERE identity_id = ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (identity_id,),
        ) as cur:
            return await cur.fetchone()

    async def get_latest_dispatched_work_item(self, identity_id: str) -> aiosqlite.Row | None:
        async with self._conn.execute(
            """
            SELECT * FROM work_items
            WHERE identity_id = ? AND status IN ('dispatched', 'cancel_requested')
            ORDER BY created_at DESC, rowid DESC LIMIT 1
            """,
            (identity_id,),
        ) as cur:
            return await cur.fetchone()

    async def mark_work_item_cancel_requested(self, id: str) -> None:
        async with self._conn.execute(
            """
            UPDATE work_items
            SET status = 'cancel_requested',
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE id = ?
            """,
            (id,),
        ):
            pass
        await self._conn.commit()

    async def get_pending_approval(self, identity_id: str) -> aiosqlite.Row | None:
        async with self._conn.execute(
            """
            SELECT a.* FROM approvals a
            LEFT JOIN work_items w ON a.work_item_id = w.id
            WHERE a.status = 'pending'
              AND (w.identity_id = ? OR (a.work_item_id IS NULL AND a.identity_id = ?))
            ORDER BY a.created_at DESC, a.rowid DESC LIMIT 1
            """,
            (identity_id, identity_id),
        ) as cur:
            return await cur.fetchone()

    async def resolve_approval(self, id: str, status: str) -> None:
        async with self._conn.execute(
            """
            UPDATE approvals
            SET status = ?,
                resolved_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE id = ?
            """,
            (status, id),
        ):
            pass
        await self._conn.commit()

    async def get_project_groups(self, identity_id: str) -> list[str]:
        async with self._conn.execute(
            "SELECT groups_json FROM projects WHERE identity_id = ? LIMIT 1",
            (identity_id,),
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return []
        return json.loads(row["groups_json"])


async def bootstrap_accounts_from_config(store: Store, config: DeskBridgeConfig) -> None:
    for identity in config.identities:
        await store.upsert_account(
            id=f"acc-{identity.label}",
            npub=identity.npub,
            label=identity.label,
            passphrase_ref=identity.passphrase_ref,
        )
    for project in config.projects:
        await store.upsert_project(
            id=project.id,
            name=project.name,
            repo_path=project.repo_path,
            identity_id=f"acc-{project.identity}",
            adapter=project.adapter,
            openclaw_agent_id=project.openclaw_agent_id,
            boards_json=json.dumps(project.boards),
            groups_json=json.dumps(project.groups),
            allowed_actions_json=json.dumps(project.allowed_autonomous_actions),
            escalation_dm_target=project.escalation_dm_target,
        )
