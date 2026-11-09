"""Keep the Pyrogram session files (they hold the channel / user access hashes a bot has learned) in MongoDB.

Why: on hosts with a temporary disk (Render free, Heroku ...) the session file is lost on every deploy / restart,
and a bot that has forgotten a channel fails with "Peer id invalid" even though it IS admin there.
The session is restored from MongoDB before the bot connects, and saved again every few minutes and on shutdown.
"""
import asyncio
import logging
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Set

from config import TelegramCommonConfig
from database import db, utcnow

logger = logging.getLogger("session_store")
_tasks: Set[asyncio.Task] = set()


def _path(name: str) -> Path:
    return TelegramCommonConfig.SESSION_DIR / f"{name}.session"


async def restore(name: str) -> bool:
    """Write the saved session to disk if this host has none. Never raises."""
    try:
        path = _path(name)
        if path.exists() and path.stat().st_size > 0:
            return False  # the disk survived: it is newer than any backup
        if not await db.wait_ready(60):
            return False
        doc = await db.db.session_backups.find_one({"name": name})
        if not doc or not doc.get("data"):
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(bytes(doc["data"]))
        logger.info("Session '%s' restored from MongoDB (%d bytes): known channels are remembered.", name, len(doc["data"]))
        return True
    except Exception as exc:
        logger.warning("Session restore skipped for %s: %s", name, exc)
        return False


def _snapshot(path: Path) -> bytes:
    """Consistent copy of a live SQLite session file."""
    fd, tmp = tempfile.mkstemp(suffix=".session")
    os.close(fd)
    try:
        src = sqlite3.connect(str(path))
        dst = sqlite3.connect(tmp)
        try:
            src.backup(dst)
        finally:
            dst.close()
            src.close()
        return Path(tmp).read_bytes()
    finally:
        Path(tmp).unlink(missing_ok=True)


async def backup(name: str) -> bool:
    try:
        path = _path(name)
        if not path.exists():
            return False
        data = await asyncio.to_thread(_snapshot, path)
        if not await db.wait_ready(5):
            return False
        await db.db.session_backups.update_one(
            {"name": name}, {"$set": {"name": name, "data": data, "updated_at": utcnow()}}, upsert=True
        )
        return True
    except Exception as exc:
        logger.debug("Session backup skipped for %s: %s", name, exc)
        return False


async def _loop(name: str, interval: int) -> None:
    await asyncio.sleep(20)
    try:
        while True:
            await backup(name)
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        await backup(name)  # final save on shutdown
        raise


def start_backup(name: str, interval: int = 300) -> None:
    t = asyncio.create_task(_loop(name, interval))
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)


async def stop_backups() -> None:
    for t in list(_tasks):
        t.cancel()
    await asyncio.gather(*list(_tasks), return_exceptions=True)
