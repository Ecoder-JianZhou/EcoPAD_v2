from __future__ import annotations

import asyncio

from app.core.db import get_db


async def main():
    db = await get_db()
    try:
        await db.execute("DELETE FROM scheduled_tasks WHERE id = ?", (1,))
        await db.commit()

        cur = await db.execute("SELECT * FROM scheduled_tasks ORDER BY id")
        rows = await cur.fetchall()
        print([dict(r) for r in rows])
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())