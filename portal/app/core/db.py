"""
Portal SQLite storage.

Responsibilities:
- users
- sessions
- jobs
- system_state
- user_site_permissions

Supports:
- one-time setup flow via system_state(initialized)
- optional development-only auto creation of admin user
"""

from __future__ import annotations

import aiosqlite

from app.core.settings import (
    PORTAL_DB_PATH,
    AUTO_CREATE_DEV_ADMIN,
    DEV_ADMIN_USERNAME,
    DEV_ADMIN_PASSWORD,
)
from app.core.security import now_iso, hash_password


DDL = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'user',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    site TEXT NOT NULL,
    task TEXT NOT NULL,
    models TEXT NOT NULL,
    treatments TEXT NOT NULL,
    created_at TEXT NOT NULL,
    status TEXT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS system_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_site_permissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    site_id TEXT NOT NULL,
    can_access INTEGER NOT NULL DEFAULT 1,
    can_auto_forecast INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(user_id, site_id),
    FOREIGN KEY(user_id) REFERENCES users(id)
);
"""


async def get_db():
    db = await aiosqlite.connect(PORTAL_DB_PATH)
    db.row_factory = aiosqlite.Row
    return db


async def _ensure_column(db: aiosqlite.Connection, table: str, column: str, ddl_add: str):
    cur = await db.execute(f"PRAGMA table_info({table})")
    rows = await cur.fetchall()
    cols = [r["name"] if isinstance(r, aiosqlite.Row) else r[1] for r in rows]
    if column not in cols:
        await db.execute(ddl_add)


async def _table_exists(db: aiosqlite.Connection, table: str) -> bool:
    cur = await db.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name=?
        """,
        (table,),
    )
    row = await cur.fetchone()
    return row is not None


async def migrate_db(db: aiosqlite.Connection):
    if await _table_exists(db, "jobs"):
        await _ensure_column(
            db,
            table="jobs",
            column="name",
            ddl_add="ALTER TABLE jobs ADD COLUMN name TEXT NOT NULL DEFAULT ''",
        )

    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS system_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS user_site_permissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            site_id TEXT NOT NULL,
            can_access INTEGER NOT NULL DEFAULT 1,
            can_auto_forecast INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(user_id, site_id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )

    if await _table_exists(db, "user_site_permissions"):
        await _ensure_column(
            db,
            table="user_site_permissions",
            column="can_access",
            ddl_add="ALTER TABLE user_site_permissions ADD COLUMN can_access INTEGER NOT NULL DEFAULT 1",
        )
        await _ensure_column(
            db,
            table="user_site_permissions",
            column="can_auto_forecast",
            ddl_add="ALTER TABLE user_site_permissions ADD COLUMN can_auto_forecast INTEGER NOT NULL DEFAULT 0",
        )
        await _ensure_column(
            db,
            table="user_site_permissions",
            column="updated_at",
            ddl_add="ALTER TABLE user_site_permissions ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''",
        )

    await db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_user_site_permissions_user
        ON user_site_permissions(user_id)
        """
    )

    await db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_user_site_permissions_site
        ON user_site_permissions(site_id)
        """
    )


async def _get_system_state(db: aiosqlite.Connection, key: str) -> str | None:
    cur = await db.execute(
        """
        SELECT value
        FROM system_state
        WHERE key=?
        """,
        (key,),
    )
    row = await cur.fetchone()
    return row["value"] if row else None


async def _set_system_state(db: aiosqlite.Connection, key: str, value: str):
    await db.execute(
        """
        INSERT INTO system_state(key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value=excluded.value,
            updated_at=excluded.updated_at
        """,
        (key, value, now_iso()),
    )


async def ensure_dev_admin():
    """
    Development-only bootstrap.

    If AUTO_CREATE_DEV_ADMIN=true and admin user does not exist:
    - create DEV_ADMIN_USERNAME / DEV_ADMIN_PASSWORD as superuser
    - mark system initialized=true
    """
    if not AUTO_CREATE_DEV_ADMIN:
        return

    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT id
            FROM users
            WHERE username=?
            """,
            (DEV_ADMIN_USERNAME,),
        )
        row = await cur.fetchone()

        if not row:
            await db.execute(
                """
                INSERT INTO users(username, password_hash, role, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    DEV_ADMIN_USERNAME,
                    hash_password(DEV_ADMIN_PASSWORD),
                    "superuser",
                    now_iso(),
                ),
            )

        await _set_system_state(db, "initialized", "true")
        await db.commit()
    finally:
        await db.close()


async def init_db():
    db = await get_db()
    try:
        await db.executescript(DDL)
        await migrate_db(db)
        await db.commit()
    finally:
        await db.close()

    await ensure_dev_admin()