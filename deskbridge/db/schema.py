import aiosqlite

SCHEMA_VERSION = 1

TABLE_NAMES = [
    "schema_version",
    "accounts",
    "projects",
    "contacts",
    "work_items",
    "agent_runs",
    "cursors",
    "approvals",
    "outbox",
    "audit_log",
]

_DDL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS accounts (
    id          TEXT PRIMARY KEY,
    npub        TEXT NOT NULL UNIQUE,
    label       TEXT NOT NULL,
    passphrase_ref TEXT NOT NULL,
    session_id  TEXT,
    health      TEXT NOT NULL DEFAULT 'unknown',
    last_unlocked_at TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS projects (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    repo_path       TEXT NOT NULL,
    identity_id     TEXT NOT NULL REFERENCES accounts(id),
    agents_json     TEXT NOT NULL DEFAULT '[]',
    groups_json     TEXT NOT NULL DEFAULT '[]',
    boards_json     TEXT NOT NULL DEFAULT '[]',
    escalation_dm_target TEXT,
    allowed_actions_json TEXT NOT NULL DEFAULT '[]',
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS contacts (
    pubkey      TEXT PRIMARY KEY,
    aliases_json TEXT NOT NULL DEFAULT '[]',
    role        TEXT,
    projects_json TEXT NOT NULL DEFAULT '[]',
    preferred_channel TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS work_items (
    id          TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    source_id   TEXT NOT NULL,
    project_id  TEXT REFERENCES projects(id),
    identity_id TEXT REFERENCES accounts(id),
    priority    INTEGER NOT NULL DEFAULT 5,
    status      TEXT NOT NULL DEFAULT 'pending',
    assigned_agent TEXT,
    summary     TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    idempotency_key TEXT UNIQUE,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS agent_runs (
    id              TEXT PRIMARY KEY,
    work_item_id    TEXT NOT NULL REFERENCES work_items(id),
    adapter_type    TEXT NOT NULL,
    process_info_json TEXT,
    heartbeat_at    TEXT,
    status          TEXT NOT NULL DEFAULT 'running',
    checkpoint_summary TEXT,
    result_summary  TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS cursors (
    id              TEXT PRIMARY KEY,
    cursor_type     TEXT NOT NULL,
    identity_id     TEXT NOT NULL REFERENCES accounts(id),
    last_entity_id  TEXT,
    last_created_at TEXT,
    last_imported_at TEXT,
    raw_json        TEXT NOT NULL DEFAULT '{}',
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    UNIQUE (cursor_type, identity_id)
);

CREATE TABLE IF NOT EXISTS approvals (
    id                  TEXT PRIMARY KEY,
    mcp_approval_id     TEXT,
    work_item_id        TEXT REFERENCES work_items(id),
    identity_id         TEXT REFERENCES accounts(id),
    action_description  TEXT NOT NULL,
    scope               TEXT,
    request_text        TEXT,
    status              TEXT NOT NULL DEFAULT 'pending',
    expires_at          TEXT,
    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    resolved_at         TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS approvals_mcp_approval_id_unique
    ON approvals (mcp_approval_id) WHERE mcp_approval_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS outbox (
    id              TEXT PRIMARY KEY,
    identity_id     TEXT NOT NULL REFERENCES accounts(id),
    dest_pubkey     TEXT,
    dest_group_id   TEXT,
    message_text    TEXT NOT NULL,
    idempotency_key TEXT UNIQUE,
    idempotency_ttl TEXT,
    delivery_attempts INTEGER NOT NULL DEFAULT 0,
    delivery_status TEXT NOT NULL DEFAULT 'pending',
    delivery_result_json TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    delivered_at    TEXT
);

CREATE TABLE IF NOT EXISTS audit_log (
    id          TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    event_type  TEXT NOT NULL,
    identity_id TEXT,
    project_id  TEXT,
    work_item_id TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}'
);
"""

_MIGRATIONS = [
    "ALTER TABLE projects ADD COLUMN adapter TEXT NOT NULL DEFAULT 'claude-code'",
]


async def apply_schema(conn: aiosqlite.Connection) -> None:
    await conn.executescript(_DDL)
    for migration in _MIGRATIONS:
        try:
            await conn.execute(migration)
            await conn.commit()
        except aiosqlite.OperationalError as exc:
            if "duplicate column name" not in str(exc).lower():
                raise
    await conn.execute("PRAGMA foreign_keys = ON")
    async with conn.execute("SELECT COUNT(*) FROM schema_version") as cur:
        row = await cur.fetchone()
    if row[0] == 0:
        await conn.execute(
            "INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
        )
        await conn.commit()
