import asyncio
import re
import secrets
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import ReturnDocument, ASCENDING, DESCENDING
from config import MongoConfig, AutoUploaderConfig, FileStoreConfig
from media_names import split_part, name_key, resolution_of_name
from release_match import parse_release

logger = logging.getLogger("database")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Database:
    def __init__(self):
        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None
        self._ready = asyncio.Event()  # set once connected: bots start answering at once and wait on this

    @property
    def is_ready(self) -> bool:
        return self._ready.is_set()

    async def wait_ready(self, timeout: float = 25.0) -> bool:
        """True as soon as MongoDB is connected (immediately if it already is)."""
        if self._ready.is_set():
            return True
        try:
            await asyncio.wait_for(self._ready.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def connect(self) -> None:
        try:
            self.client = AsyncIOMotorClient(
                MongoConfig.URI, serverSelectionTimeoutMS=10000, connectTimeoutMS=8000, maxPoolSize=50
            )
        except TypeError:  # test doubles that only take the URI
            self.client = AsyncIOMotorClient(MongoConfig.URI)
        self.db = self.client[MongoConfig.DB_NAME]
        # indexes + default settings run in PARALLEL (they used to be ~35 sequential round trips to Atlas)
        await asyncio.gather(self._create_indexes(), self._init_default_settings())
        self._ready.set()
        logger.info("Connected to MongoDB (%s) and verified all indexes.", MongoConfig.DB_NAME)

    async def close(self) -> None:
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed.")

    async def _create_indexes(self) -> None:
        specs = [
            (self.db.movies, [("movie_id", ASCENDING)], {"unique": True}),
            (self.db.movies, [("raw_title", ASCENDING)], {"unique": True}),
            (self.db.torrents, [("file_id", ASCENDING)], {"unique": True}),
            (self.db.torrents, [("infohash", ASCENDING)], {"sparse": True}),
            (self.db.torrent_registry, [("infohash", ASCENDING)], {"unique": True}),
            (self.db.torrent_registry, [("movie_id", ASCENDING)], {}),
            (self.db.files, [("file_id", ASCENDING)], {"unique": True}),
            (self.db.files, [("infohash", ASCENDING)], {}),
            (self.db.file_parts, [("file_id", ASCENDING), ("part_number", ASCENDING)], {"unique": True}),
            (self.db.telegram_messages, [("movie_id", ASCENDING)], {}),
            (self.db.download_queue, [("file_id", ASCENDING)], {"unique": True}),
            (self.db.download_queue, [("infohash", ASCENDING)], {"sparse": True}),
            (self.db.download_queue, [("status", ASCENDING), ("created_at", ASCENDING)], {}),
            (self.db.upload_queue, [("movie_id", ASCENDING)], {"unique": True}),
            (self.db.upload_queue, [("status", ASCENDING), ("scheduled_at", ASCENDING)], {}),
            (self.db.redirect_links, [("redirect_id", ASCENDING)], {"unique": True}),
            (self.db.redirect_links, [("file_id", ASCENDING)], {}),
            (self.db.bot_settings, [("bot_name", ASCENDING)], {"unique": True}),
            (self.db.download_history, [("file_id", ASCENDING), ("created_at", DESCENDING)], {}),
            (self.db.users, [("user_id", ASCENDING)], {"unique": True}),
            (self.db.clone_bots, [("owner_id", ASCENDING)], {"unique": True}),
            (self.db.clone_bots, [("bot_username", ASCENDING)], {"sparse": True}),
            (self.db.stored_links, [("link_id", ASCENDING)], {"unique": True}),
            (self.db.verification_tokens, [("token", ASCENDING)], {"unique": True}),
            (self.db.user_settings, [("user_id", ASCENDING)], {"unique": True}),
            (self.db.user_settings, [("public_channel_id", ASCENDING)], {"sparse": True}),
            (self.db.files, [("name_key", ASCENDING)], {}),
            (self.db.file_parts, [("telegram_unique_id", ASCENDING)], {"sparse": True}),
            (self.db.file_parts, [("telegram_message_id", ASCENDING)], {"sparse": True}),
            (self.db.file_aliases, [("alias_id", ASCENDING)], {"unique": True}),
            (self.db.user_posts, [("movie_id", ASCENDING), ("user_id", ASCENDING)], {"unique": True}),
            (self.db.users, [("is_banned", ASCENDING)], {"sparse": True}),
            (self.db.db_channel_files, [("chat_id", ASCENDING), ("message_id", ASCENDING)], {"unique": True}),
            (self.db.db_channel_files, [("title_key", ASCENDING), ("chat_id", ASCENDING)], {}),
            (self.db.db_channel_files, [("telegram_unique_id", ASCENDING)], {"sparse": True}),
        ]
        await asyncio.gather(*(coll.create_index(keys, **kw) for coll, keys, kw in specs))

    async def _backfill_torrent_registry(self) -> None:
        """Torrents saved by older versions get registered too, so they are never re-added as duplicates."""
        try:
            cursor = self.db.torrents.find({"infohash": {"$ne": None}})
            async for t in cursor:
                await self.db.torrent_registry.update_one(
                    {"infohash": t["infohash"]},
                    {"$setOnInsert": {
                        "infohash": t["infohash"],
                        "movie_id": t.get("movie_id"),
                        "file_id": t.get("file_id"),
                        "torrent_url": t.get("torrent_url"),
                        "original_filename": t.get("original_filename"),
                        "created_at": t.get("created_at", utcnow()),
                    }},
                    upsert=True,
                )
        except Exception as exc:
            logger.warning("Torrent registry backfill skipped: %s", exc)

    async def _init_default_settings(self) -> None:
        await self.db.bot_settings.update_one(
            {"bot_name": "auto_uploader"},
            {
                "$setOnInsert": {
                    "bot_name": "auto_uploader",
                    "manual_edit_mode": AutoUploaderConfig.MANUAL_EDIT_MODE,
                    "uploader_name": AutoUploaderConfig.DEFAULT_UPLOADER,
                    "join_channel": AutoUploaderConfig.JOIN_CHANNEL,
                    "post_interval_seconds": AutoUploaderConfig.POST_INTERVAL_SECONDS,
                    "updated_at": utcnow(),
                }
            },
            upsert=True,
        )
        await self.db.bot_settings.update_one(
            {"bot_name": "file_store"},
            {
                "$setOnInsert": {
                    "bot_name": "file_store",
                    "max_concurrent_downloads": FileStoreConfig.MAX_CONCURRENT_DOWNLOADS,
                    "max_split_size_bytes": FileStoreConfig.MAX_SPLIT_SIZE_BYTES,
                    "protect_content": FileStoreConfig.PROTECT_CONTENT,
                    "auto_delete_enabled": FileStoreConfig.AUTO_DELETE_ENABLED,
                    "auto_delete_seconds": FileStoreConfig.AUTO_DELETE_SECONDS,
                    "force_sub_channel": FileStoreConfig.FORCE_SUB_CHANNEL,
                    "custom_caption": FileStoreConfig.CUSTOM_CAPTION,
                    "custom_button_text": FileStoreConfig.CUSTOM_BUTTON_TEXT,
                    "custom_button_url": FileStoreConfig.CUSTOM_BUTTON_URL,
                    "shortener_enabled": FileStoreConfig.SHORTENER_ENABLED,
                    "shortener_domain": FileStoreConfig.SHORTENER_DOMAIN,
                    "shortener_api_key": FileStoreConfig.SHORTENER_API_KEY,
                    "token_verify_enabled": FileStoreConfig.TOKEN_VERIFY_ENABLED,
                    "token_verify_expire_seconds": FileStoreConfig.TOKEN_VERIFY_EXPIRE_SECONDS,
                    "updated_at": utcnow(),
                }
            },
            upsert=True,
        )

    async def get_bot_settings(self, bot_name: str) -> Dict[str, Any]:
        doc = await self.db.bot_settings.find_one({"bot_name": bot_name})
        return doc or {}

    async def update_bot_settings(self, bot_name: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        updates["updated_at"] = utcnow()
        return await self.db.bot_settings.find_one_and_update(
            {"bot_name": bot_name},
            {"$set": updates},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )

    # --- Per-user Auto Uploader settings (uploader name, join tag, caption, thumbnail) ---

    async def get_user_settings(self, user_id: int) -> Dict[str, Any]:
        doc = await self.db.user_settings.find_one({"user_id": user_id})
        return doc or {}

    async def update_user_settings(
        self, user_id: int, updates: Optional[Dict[str, Any]] = None, unset: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        op: Dict[str, Any] = {"$set": {"user_id": user_id, "updated_at": utcnow()}}
        if updates:
            op["$set"].update(updates)
        if unset:
            op["$unset"] = {k: "" for k in unset}
        return await self.db.user_settings.find_one_and_update(
            {"user_id": user_id}, op, upsert=True, return_document=ReturnDocument.AFTER
        )

    async def inc_user_counter(self, user_id: int, field: str, n: int = 1) -> None:
        """Per-user stats (posts sent to their channel, ...)."""
        await self.db.user_settings.update_one(
            {"user_id": user_id},
            {"$inc": {field: n}, "$set": {"updated_at": utcnow(), "last_post_at": utcnow()}},
            upsert=True,
        )

    async def list_users(self, kind: str, limit: int = 30) -> List[Dict[str, Any]]:
        """kind = 'paid' | 'banned' (only currently active ones)."""
        now = utcnow()
        if kind == "paid":
            q = {"is_premium": True, "$or": [{"premium_until": None}, {"premium_until": {"$gt": now}}]}
        else:
            q = {"is_banned": True, "$or": [{"ban_until": None}, {"ban_until": {"$gt": now}}]}
        return await self.db.users.find(q).sort("updated_at", DESCENDING).to_list(length=limit)

    # --- Users, Broadcast & Premium Management ---

    async def register_user(self, user_id: int, first_name: str = "", username: str = "") -> Dict[str, Any]:
        now = utcnow()
        return await self.db.users.find_one_and_update(
            {"user_id": user_id},
            {
                "$set": {"first_name": first_name, "username": username, "last_active": now},
                "$setOnInsert": {
                    "user_id": user_id,
                    "is_premium": False,
                    "premium_until": None,
                    "verified_until": None,
                    "created_at": now,
                },
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )

    async def get_all_user_ids(self) -> List[int]:
        cursor = self.db.users.find({}, {"user_id": 1})
        docs = await cursor.to_list(length=500000)
        return [d["user_id"] for d in docs]

    async def set_user_premium(self, user_id: int, days: int, lifetime: bool = False) -> Dict[str, Any]:
        """days > 0 -> paid for that many days; lifetime=True -> paid with no expiry; days <= 0 -> unpaid."""
        now = utcnow()
        expiry = None if lifetime else (now + timedelta(days=days) if days > 0 else None)
        return await self.db.users.find_one_and_update(
            {"user_id": user_id},
            {"$set": {"is_premium": bool(lifetime or days > 0), "premium_until": expiry, "updated_at": now}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )

    async def is_user_premium(self, user_id: int) -> bool:
        doc = await self.db.users.find_one({"user_id": user_id})
        if not doc or not doc.get("is_premium"):
            return False
        exp = doc.get("premium_until")
        if exp:
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            if exp < utcnow():
                await self.db.users.update_one({"user_id": user_id}, {"$set": {"is_premium": False}})
                return False
        return True

    # --- Ban / unban (set from the /admin panel) ---

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        return await self.db.users.find_one({"user_id": user_id})

    async def ban_user(self, user_id: int, days: int = 0) -> Dict[str, Any]:
        """days > 0 -> banned for that many days; days <= 0 -> permanent."""
        now = utcnow()
        until = now + timedelta(days=days) if days > 0 else None
        return await self.db.users.find_one_and_update(
            {"user_id": user_id},
            {"$set": {"is_banned": True, "ban_until": until, "banned_at": now, "updated_at": now}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )

    async def unban_user(self, user_id: int) -> bool:
        res = await self.db.users.update_one(
            {"user_id": user_id},
            {"$set": {"is_banned": False, "ban_until": None, "updated_at": utcnow()}},
        )
        return res.matched_count > 0

    async def get_ban(self, user_id: int) -> Optional[Dict[str, Any]]:
        """None = not banned. {'until': datetime|None} = banned (None = permanent). Expired bans lift themselves."""
        doc = await self.db.users.find_one({"user_id": user_id}, {"is_banned": 1, "ban_until": 1})
        if not doc or not doc.get("is_banned"):
            return None
        until = doc.get("ban_until")
        if until:
            if until.tzinfo is None:
                until = until.replace(tzinfo=timezone.utc)
            if until <= utcnow():
                await self.unban_user(user_id)
                return None
        return {"until": until}

    async def get_user_stats(self) -> Dict[str, int]:
        now = utcnow()
        total = await self.db.users.count_documents({})
        paid = await self.db.users.count_documents(
            {"is_premium": True, "$or": [{"premium_until": None}, {"premium_until": {"$gt": now}}]}
        )
        banned = await self.db.users.count_documents(
            {"is_banned": True, "$or": [{"ban_until": None}, {"ban_until": {"$gt": now}}]}
        )
        channels = await self.db.user_settings.count_documents({"public_channel_id": {"$exists": True, "$ne": None}})
        return {"total": total, "paid": paid, "banned": banned, "channels": channels}

    # --- Token Verification ---

    async def is_user_token_verified(self, user_id: int) -> bool:
        if await self.is_user_premium(user_id):
            return True
        doc = await self.db.users.find_one({"user_id": user_id})
        if not doc or not doc.get("verified_until"):
            return False
        v_until = doc["verified_until"]
        if v_until.tzinfo is None:
            v_until = v_until.replace(tzinfo=timezone.utc)
        return v_until > utcnow()

    async def create_verification_token(self, user_id: int, requested_payload: str) -> str:
        token = secrets.token_urlsafe(12)
        await self.db.verification_tokens.insert_one(
            {
                "token": token,
                "user_id": user_id,
                "payload": requested_payload,
                "used": False,
                "created_at": utcnow(),
            }
        )
        return token

    async def consume_verification_token(self, token: str, user_id: int, expire_seconds: int) -> Optional[str]:
        doc = await self.db.verification_tokens.find_one_and_update(
            {"token": token, "user_id": user_id, "used": False},
            {"$set": {"used": True, "used_at": utcnow()}},
        )
        if not doc:
            return None
        verified_until = utcnow() + timedelta(seconds=expire_seconds)
        await self.db.users.update_one(
            {"user_id": user_id},
            {"$set": {"verified_until": verified_until}},
            upsert=True,
        )
        return doc.get("payload")

    # --- Clone Bot Management (/clone, /activate, /delete) ---

    async def save_clone_bot(self, owner_id: int, bot_token: str, bot_id: int, bot_username: str, bot_name: str) -> Dict[str, Any]:
        now = utcnow()
        return await self.db.clone_bots.find_one_and_update(
            {"owner_id": owner_id},
            {
                "$set": {
                    "owner_id": owner_id,
                    "bot_token": bot_token,
                    "bot_id": bot_id,
                    "bot_username": bot_username,
                    "bot_name": bot_name,
                    "is_active": True,
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )

    async def get_clone_bot(self, owner_id: int) -> Optional[Dict[str, Any]]:
        return await self.db.clone_bots.find_one({"owner_id": owner_id})

    async def set_clone_bot_active(self, owner_id: int, is_active: bool) -> Optional[Dict[str, Any]]:
        return await self.db.clone_bots.find_one_and_update(
            {"owner_id": owner_id},
            {"$set": {"is_active": is_active, "updated_at": utcnow()}},
            return_document=ReturnDocument.AFTER,
        )

    async def delete_clone_bot(self, owner_id: int) -> bool:
        res = await self.db.clone_bots.delete_one({"owner_id": owner_id})
        return res.deleted_count > 0

    async def get_all_active_clones(self) -> List[Dict[str, Any]]:
        cursor = self.db.clone_bots.find({"is_active": True})
        return await cursor.to_list(length=500)

    # --- Single (/link) & Multi-Message (/batch) Storage ---

    async def save_stored_link(self, link_doc: Dict[str, Any]) -> None:
        link_doc["created_at"] = utcnow()
        await self.db.stored_links.update_one(
            {"link_id": link_doc["link_id"]},
            {"$set": link_doc},
            upsert=True,
        )

    async def get_stored_link(self, link_id: str) -> Optional[Dict[str, Any]]:
        return await self.db.stored_links.find_one({"link_id": link_id})

    # --- Movie & Torrent Registration ---

    async def get_movie_by_raw_title(self, raw_title: str) -> Optional[Dict[str, Any]]:
        return await self.db.movies.find_one({"raw_title": raw_title})

    async def get_movie(self, movie_id: str) -> Optional[Dict[str, Any]]:
        return await self.db.movies.find_one({"movie_id": movie_id})

    async def save_movie(self, movie_doc: Dict[str, Any]) -> None:
        movie_doc["created_at"] = movie_doc.get("created_at", utcnow())
        await self.db.movies.update_one(
            {"movie_id": movie_doc["movie_id"]},
            {"$set": movie_doc},
            upsert=True,
        )

    async def save_torrent(self, torrent_doc: Dict[str, Any]) -> None:
        torrent_doc["created_at"] = torrent_doc.get("created_at", utcnow())
        await self.db.torrents.update_one(
            {"file_id": torrent_doc["file_id"]},
            {"$set": torrent_doc},
            upsert=True,
        )

    async def get_torrent(self, file_id: str) -> Optional[Dict[str, Any]]:
        return await self.db.torrents.find_one({"file_id": file_id})

    async def get_torrents_by_movie(self, movie_id: str) -> List[Dict[str, Any]]:
        cursor = self.db.torrents.find({"movie_id": movie_id}).sort("index", ASCENDING)
        return await cursor.to_list(length=200)

    async def save_redirect_link(self, redirect_doc: Dict[str, Any]) -> None:
        redirect_doc["created_at"] = utcnow()
        await self.db.redirect_links.update_one(
            {"redirect_id": redirect_doc["redirect_id"]},
            {"$set": redirect_doc},
            upsert=True,
        )

    # --- Duplicate protection: every file is keyed by its magnet / torrent infohash ---

    async def claim_infohash(self, infohash: str, doc: Dict[str, Any]) -> bool:
        """Atomically register `infohash`. True = first time seen (new file), False = duplicate."""
        doc = dict(doc)
        doc["infohash"] = infohash
        doc["created_at"] = utcnow()
        res = await self.db.torrent_registry.update_one(
            {"infohash": infohash}, {"$setOnInsert": doc}, upsert=True
        )
        return res.upserted_id is not None

    async def update_registry(self, infohash: str, updates: Dict[str, Any]) -> None:
        await self.db.torrent_registry.update_one({"infohash": infohash}, {"$set": updates})

    async def release_infohash(self, infohash: str) -> None:
        """Undo a claim when processing the file failed afterwards, so a later scrape can retry it."""
        await self.db.torrent_registry.delete_one({"infohash": infohash})

    async def get_registry_entry(self, infohash: str) -> Optional[Dict[str, Any]]:
        return await self.db.torrent_registry.find_one({"infohash": infohash})

    async def get_redirect_link(self, redirect_id: str) -> Optional[Dict[str, Any]]:
        return await self.db.redirect_links.find_one({"redirect_id": redirect_id})

    # --- Posts in paid users' own channels (Advanced Settings) ---

    async def get_auto_post_users(self) -> List[Dict[str, Any]]:
        """Users who set their own public channel and did not switch auto uploading off."""
        cursor = self.db.user_settings.find(
            {"public_channel_id": {"$exists": True, "$ne": None}, "auto_post_enabled": {"$ne": False}}
        )
        return await cursor.to_list(length=5000)

    async def clear_user_data(self, user_id: int) -> Dict[str, int]:
        """/clear: forget everything THIS user set up (channels, look, filters, thumbnail record) and every post
        made to their channel, so uploading starts from scratch. Plan (premium) / ban status is NOT touched."""
        cfg = await self.get_user_settings(user_id)
        posts = await self.db.user_posts.delete_many({"user_id": user_id})
        settings = await self.db.user_settings.delete_one({"user_id": user_id})
        indexed = 0
        if cfg.get("db_channel_id"):  # their own Database channel index (the admin's shared one is never touched here)
            indexed = (await self.db.db_channel_files.delete_many(
                {"chat_id": int(cfg["db_channel_id"]), "owner_id": int(user_id)})).deleted_count
        return {"settings": settings.deleted_count, "posts": posts.deleted_count, "indexed": indexed,
                "thumbnail": cfg.get("thumbnail_path") or ""}

    async def reset_main_posts(self) -> Dict[str, int]:
        """Owner only: forget the main-channel post history so the next scrape posts everything again."""
        a = await self.db.telegram_messages.delete_many({})
        b = await self.db.upload_queue.delete_many({})
        return {"posts": a.deleted_count, "queue": b.deleted_count}

    async def get_user_post(self, movie_id: str, user_id: int) -> Optional[Dict[str, Any]]:
        return await self.db.user_posts.find_one({"movie_id": movie_id, "user_id": user_id})

    async def save_user_post(
        self, movie_id: str, user_id: int, channel_id: int, message_ids: List[int], has_photo: bool
    ) -> None:
        now = utcnow()
        await self.db.user_posts.update_one(
            {"movie_id": movie_id, "user_id": user_id},
            {
                "$set": {"channel_id": channel_id, "message_ids": message_ids, "has_photo": has_photo, "updated_at": now},
                "$setOnInsert": {"movie_id": movie_id, "user_id": user_id, "created_at": now},
            },
            upsert=True,
        )

    # --- Persistent Auto-Post Schedule (5-minute interval) ---

    async def schedule_post(self, movie_id: str, interval_seconds: int = 300) -> datetime:
        now = utcnow()
        last_item = await self.db.upload_queue.find_one(
            {"status": {"$in": ["scheduled", "published"]}},
            sort=[("scheduled_at", DESCENDING)],
        )
        if last_item and last_item.get("scheduled_at"):
            last_time = last_item["scheduled_at"]
            if last_time.tzinfo is None:
                last_time = last_time.replace(tzinfo=timezone.utc)
            next_time = max(now, last_time + timedelta(seconds=interval_seconds))
        else:
            next_time = now

        await self.db.upload_queue.update_one(
            {"movie_id": movie_id},
            {
                "$set": {
                    "movie_id": movie_id,
                    "status": "scheduled",
                    "scheduled_at": next_time,
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )
        return next_time

    async def claim_next_due_post(self) -> Optional[Dict[str, Any]]:
        now = utcnow()
        return await self.db.upload_queue.find_one_and_update(
            {"status": "scheduled", "scheduled_at": {"$lte": now}},
            {"$set": {"status": "publishing", "updated_at": now}},
            sort=[("scheduled_at", ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )

    async def mark_post_published(self, movie_id: str, message_id: int, channel_id: int) -> None:
        now = utcnow()
        await self.db.upload_queue.update_one(
            {"movie_id": movie_id},
            {"$set": {"status": "published", "published_at": now, "updated_at": now}},
        )
        await self.db.telegram_messages.update_one(
            {"movie_id": movie_id},
            {
                "$set": {"message_id": message_id, "channel_id": channel_id, "updated_at": now},
                "$setOnInsert": {"movie_id": movie_id, "created_at": now},
            },
            upsert=True,
        )

    async def get_post_record(self, movie_id: str) -> Optional[Dict[str, Any]]:
        return await self.db.telegram_messages.find_one({"movie_id": movie_id})

    async def save_post_record(self, movie_id: str, channel_id: int, message_ids: List[int], has_photo: bool) -> None:
        """Remember every Telegram message of a post (a long file list spans several messages)."""
        now = utcnow()
        await self.db.telegram_messages.update_one(
            {"movie_id": movie_id},
            {
                "$set": {
                    "message_id": message_ids[0] if message_ids else 0,
                    "message_ids": message_ids,
                    "has_photo": has_photo,
                    "channel_id": channel_id,
                    "updated_at": now,
                },
                "$setOnInsert": {"movie_id": movie_id, "created_at": now},
            },
            upsert=True,
        )
        await self.db.upload_queue.update_one(
            {"movie_id": movie_id},
            {
                "$set": {"status": "published", "published_at": now, "updated_at": now},
                "$setOnInsert": {"movie_id": movie_id, "created_at": now, "scheduled_at": now},
            },
            upsert=True,
        )

    # --- File Store Database-First & Atomic Download Locks ---

    async def find_completed_file(
        self, file_id: str, infohash: Optional[str] = None, names: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        """Database-first lookup. Order: exact file id -> alias -> magnet/torrent hash -> release name
        (this last one finds files the admin uploaded directly, and files stored under another torrent)."""
        file_doc = await self.db.files.find_one({"file_id": file_id, "status": "completed"})
        if file_doc:
            return file_doc
        alias = await self.db.file_aliases.find_one({"alias_id": file_id})
        if alias:
            file_doc = await self.db.files.find_one({"file_id": alias["file_id"], "status": "completed"})
            if file_doc:
                return file_doc
        if infohash:
            file_doc = await self.db.files.find_one({"infohash": infohash, "status": "completed"})
            if file_doc:
                return file_doc
        if names:
            return await self.find_file_by_name(names)
        return None

    async def find_file_by_name(self, names: List[str]) -> Optional[Dict[str, Any]]:
        """Completed file whose normalised release name matches one of `names`. Different resolution = different file."""
        keys = {name_key(n) for n in names if n}
        keys.discard("")
        if not keys:
            return None
        wanted_res = {r for r in (resolution_of_name(n) for n in names if n) if r}
        cursor = self.db.files.find({"name_key": {"$in": list(keys)}, "status": "completed"})
        async for doc in cursor:
            doc_res = doc.get("resolution")
            if doc_res and wanted_res and doc_res not in wanted_res:
                continue
            if await self.db.file_parts.count_documents({"file_id": doc["file_id"]}, limit=1) == 0:
                continue
            return doc
        return None

    async def add_file_alias(self, alias_id: str, file_id: str) -> None:
        """Remember that torrent file `alias_id` is already stored as `file_id` (web links keep working too)."""
        if alias_id == file_id:
            return
        await self.db.file_aliases.update_one(
            {"alias_id": alias_id},
            {"$set": {"alias_id": alias_id, "file_id": file_id, "created_at": utcnow()}},
            upsert=True,
        )

    async def index_media_part(
        self,
        *,
        filename: str,
        size_bytes: int,
        tg_file_id: str,
        tg_unique_id: Optional[str],
        tg_message_id: int,
        duration: int = 0,
        source: str = "admin",
        check_only: bool = False,
    ) -> Dict[str, Any]:
        """Register a file somebody uploaded by hand (admin) so users get it from the database, never a download.

        Split files are mapped: every part lands on ONE file record (part number read from the name), so
        Movie.part01 + Movie.part02 are two parts of one file. Same part twice / same release again = duplicate.
        Returns {"status": "new"|"duplicate"|"skipped", "file_id", "part_number", "quality", ...}.
        check_only=True only answers whether it would be stored (writes nothing)."""
        key = name_key(filename)
        if not key:
            return {"status": "skipped", "reason": "no usable file name"}
        base, part_no = split_part(filename)
        res = resolution_of_name(filename)

        if tg_unique_id and await self.db.file_parts.find_one({"telegram_unique_id": tg_unique_id}):
            return {"status": "duplicate", "reason": "same Telegram file already stored"}
        if tg_message_id and await self.db.file_parts.find_one({"telegram_message_id": tg_message_id}):
            return {"status": "duplicate", "reason": "same message already stored"}

        existing = await self.find_file_by_name([filename])
        if existing:
            if not str(existing["file_id"]).startswith("adm_"):
                return {"status": "duplicate", "reason": "already stored (downloaded earlier)", "file_id": existing["file_id"]}
            file_id = existing["file_id"]
            taken = {p["part_number"] for p in await self.get_file_parts(file_id)}
            part_number = part_no or 1
            if part_number in taken:
                return {"status": "duplicate", "reason": f"part {part_number} already stored", "file_id": file_id}
        else:
            file_id = "adm_" + hashlib.sha1(f"{key}|{res or ''}".encode()).hexdigest()[:16]
            part_number = part_no or 1

        result = {"status": "new", "file_id": file_id, "part_number": part_number, "quality": res or "HD", "base_name": base}
        if check_only:
            return result

        now = utcnow()
        await self.db.file_parts.update_one(
            {"file_id": file_id, "part_number": part_number},
            {"$set": {
                "file_id": file_id,
                "part_number": part_number,
                "part_filename": filename,
                "file_path": None,
                "size_bytes": int(size_bytes or 0),
                "duration": int(duration or 0),
                "thumbnail_path": None,
                "telegram_file_id": tg_file_id,
                "telegram_unique_id": tg_unique_id,
                "telegram_message_id": tg_message_id,
                "name_key": key,
                "resolution": res,
                "source": source,
                "created_at": now,
            }},
            upsert=True,
        )
        parts = await self.get_file_parts(file_id)
        total_parts = max(len(parts), max(p["part_number"] for p in parts))
        await self.db.file_parts.update_many({"file_id": file_id}, {"$set": {"total_parts": total_parts}})
        await self.db.files.update_one(
            {"file_id": file_id},
            {
                "$set": {
                    "file_id": file_id,
                    "movie_id": None,
                    "infohash": None,
                    "original_filename": base,
                    "name_key": key,
                    "resolution": res,
                    "file_size": sum(int(p.get("size_bytes") or 0) for p in parts),
                    "total_parts": total_parts,
                    "source": source,
                    "status": "completed",
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )
        result["total_parts"] = total_parts
        return result

    # --- Database channels: every user can attach a channel that already holds the files ---

    async def get_db_channel_map(self) -> Dict[int, int]:
        """{chat_id: owner_id} of every configured Database channel (admin's in bot settings, users' in their settings)."""
        out: Dict[int, int] = {}
        admin_chat = await self.get_admin_db_channel_id()
        if admin_chat:
            out[admin_chat] = int(AutoUploaderConfig.OWNER_ID)
        async for u in self.db.user_settings.find({"db_channel_id": {"$ne": None}}, {"user_id": 1, "db_channel_id": 1}):
            if u.get("db_channel_id"):
                out.setdefault(int(u["db_channel_id"]), int(u["user_id"]))
        return out

    async def get_db_channel_ids_for(self, owner_id: int) -> List[int]:
        """Channels searched for this owner's posts: their own Database channel first, then the admin's."""
        ids: List[int] = []
        cfg = await self.get_user_settings(owner_id)
        if cfg.get("db_channel_id"):
            ids.append(int(cfg["db_channel_id"]))
        admin_chat = await self.get_admin_db_channel_id()
        if admin_chat and admin_chat not in ids:
            ids.append(admin_chat)
        return ids

    async def get_admin_db_channel_id(self) -> Optional[int]:
        """Admin's Database channel: the one set in the bot wins, else DB_CHANNEL_ID from the environment."""
        glob = await self.db.bot_settings.find_one({"bot_name": "auto_uploader"}) or {}
        cid = glob.get("db_channel_id") or AutoUploaderConfig.DB_CHANNEL_ID
        return int(cid) if cid else None

    async def save_db_channel_file(
        self, *, chat_id: int, owner_id: int, message_id: int, file_name: str, size_bytes: int,
        tg_file_id: str, tg_unique_id: Optional[str], duration: int = 0,
    ) -> str:
        """Index one file of a Database channel. Returns 'new' | 'updated'. Size is stored for display only."""
        info = parse_release(file_name)
        base, part_no = split_part(file_name)
        doc = {
            "chat_id": int(chat_id),
            "owner_id": int(owner_id),
            "message_id": int(message_id),
            "file_name": file_name,
            "base_name": base,
            "part_number": part_no or 1,
            "name_key": name_key(file_name),
            "title": info["title"],
            "title_key": info["title_key"],
            "year": info["year"],
            "release_date": info["release_date"],
            "resolution": info["resolution"],
            "size_bytes": int(size_bytes or 0),
            "duration": int(duration or 0),
            "telegram_file_id": tg_file_id,
            "telegram_unique_id": tg_unique_id,
            "updated_at": utcnow(),
        }
        res = await self.db.db_channel_files.update_one(
            {"chat_id": int(chat_id), "message_id": int(message_id)},
            {"$set": doc, "$setOnInsert": {"created_at": utcnow()}},
            upsert=True,
        )
        return "new" if res.upserted_id else "updated"

    async def list_db_channel_message_ids(self, chat_id: int) -> List[int]:
        cur = self.db.db_channel_files.find({"chat_id": int(chat_id)}, {"message_id": 1})
        return [int(d["message_id"]) async for d in cur]

    async def prune_db_channel_files(self, chat_id: int, keep_ids: set) -> int:
        """Full scan: forget indexed files whose message no longer exists in the channel."""
        stale = [m for m in await self.list_db_channel_message_ids(chat_id) if m not in keep_ids]
        if stale:
            await self.db.db_channel_files.delete_many({"chat_id": int(chat_id), "message_id": {"$in": stale}})
        return len(stale)

    async def get_movies_by_owner(self, owner_id: int) -> List[Dict[str, Any]]:
        return await self.db.movies.find({"owner_id": int(owner_id)}).to_list(length=5000)

    async def mark_torrent_from_db(self, file_id: str, db_file_id: str, name: str, size: int) -> None:
        upd: Dict[str, Any] = {"from_db_channel": True, "db_file_id": db_file_id, "original_filename": name}
        if size:
            upd["size_bytes"] = int(size)
        await self.db.torrents.update_one({"file_id": file_id}, {"$set": upd})

    async def remove_db_channel_file(self, chat_id: int, message_id: int) -> None:
        await self.db.db_channel_files.delete_one({"chat_id": int(chat_id), "message_id": int(message_id)})

    async def clear_db_channel_files(self, chat_id: int) -> int:
        res = await self.db.db_channel_files.delete_many({"chat_id": int(chat_id)})
        return res.deleted_count

    async def count_db_channel_files(self, chat_id: int) -> int:
        return await self.db.db_channel_files.count_documents({"chat_id": int(chat_id)})

    async def find_db_channel_candidates(self, chat_ids: List[int], title_keys: List[str], year: Optional[str]) -> List[Dict[str, Any]]:
        """Cheap pre-filter for the verification passes: same title key (any of the accepted ones), or the same
        year when the title differs a little (fuzzy pass). The strict checks happen in release_match.verify()."""
        if not chat_ids or not title_keys:
            return []
        ors: List[Dict[str, Any]] = [{"title_key": {"$in": list(title_keys)}}]
        if year:
            ors.append({"year": year})
        cursor = self.db.db_channel_files.find({"chat_id": {"$in": [int(c) for c in chat_ids]}, "$or": ors})
        return await cursor.to_list(length=500)

    async def search_db_channel_files(self, chat_ids: List[int], query: str, limit: int = 400) -> List[Dict[str, Any]]:
        """Files of the Database channels whose NAME matches a user's search words: every word must appear (any order),
        or the whole query - ignoring spaces / dots / dashes - must be inside the normalised release name."""
        tokens = [t for t in re.split(r"[^A-Za-z0-9]+", query or "") if t]
        if not chat_ids or not tokens:
            return []
        ors: List[Dict[str, Any]] = [{"file_name": {"$regex": "".join(f"(?=.*{re.escape(t)})" for t in tokens), "$options": "is"}}]
        key = re.sub(r"[^a-z0-9]+", "", (query or "").lower())
        if len(key) >= 3:
            ors.append({"name_key": {"$regex": re.escape(key)}})
        cursor = self.db.db_channel_files.find({"chat_id": {"$in": [int(c) for c in chat_ids]}, "$or": ors}).limit(limit)
        return await cursor.to_list(length=limit)

    async def link_db_channel_release(self, parts: List[Dict[str, Any]]) -> Optional[str]:
        """Turn verified Database-channel file(s) into a normal completed File Store record (source = that channel),
        so the usual delivery path sends them. Returns the file_id (stable per release)."""
        if not parts:
            return None
        first = parts[0]
        key = first.get("name_key") or name_key(first["file_name"])
        file_id = "dbc_" + hashlib.sha1(f"{first['chat_id']}|{key}|{first.get('resolution') or ''}".encode()).hexdigest()[:16]
        now = utcnow()
        total_parts = max(len(parts), max(int(p.get("part_number") or 1) for p in parts))
        for p in parts:
            await self.db.file_parts.update_one(
                {"file_id": file_id, "part_number": int(p.get("part_number") or 1)},
                {"$set": {
                    "file_id": file_id,
                    "part_number": int(p.get("part_number") or 1),
                    "total_parts": total_parts,
                    "part_filename": p["file_name"],
                    "file_path": None,
                    "size_bytes": int(p.get("size_bytes") or 0),
                    "duration": int(p.get("duration") or 0),
                    "thumbnail_path": None,
                    "telegram_file_id": p.get("telegram_file_id"),
                    "telegram_unique_id": p.get("telegram_unique_id"),
                    "telegram_message_id": int(p["message_id"]),
                    "source_chat_id": int(p["chat_id"]),
                    "name_key": key,
                    "resolution": p.get("resolution"),
                    "source": "db_channel",
                    "created_at": now,
                }},
                upsert=True,
            )
        await self.db.files.update_one(
            {"file_id": file_id},
            {"$set": {
                "file_id": file_id, "movie_id": None, "infohash": None,
                "original_filename": first.get("base_name") or first["file_name"],
                "name_key": key, "resolution": first.get("resolution"),
                "file_size": sum(int(p.get("size_bytes") or 0) for p in parts),
                "total_parts": total_parts, "source": "db_channel", "status": "completed", "updated_at": now,
            }, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
        return file_id

    async def get_file_parts(self, file_id: str) -> List[Dict[str, Any]]:
        cursor = self.db.file_parts.find({"file_id": file_id}).sort("part_number", ASCENDING)
        return await cursor.to_list(length=100)

    async def save_completed_file(self, file_doc: Dict[str, Any], parts: List[Dict[str, Any]]) -> None:
        now = utcnow()
        file_doc["status"] = "completed"
        file_doc["updated_at"] = now
        file_doc.setdefault("created_at", now)
        await self.db.files.update_one(
            {"file_id": file_doc["file_id"]},
            {"$set": file_doc},
            upsert=True,
        )
        for part in parts:
            part["created_at"] = now
            await self.db.file_parts.update_one(
                {"file_id": part["file_id"], "part_number": part["part_number"]},
                {"$set": part},
                upsert=True,
            )

    async def acquire_download_lock(self, file_id: str, infohash: Optional[str], user_id: int) -> Dict[str, Any]:
        now = utcnow()
        query = {"file_id": file_id}
        if infohash:
            existing_by_hash = await self.db.download_queue.find_one(
                {"infohash": infohash, "status": {"$in": ["queued", "downloading", "uploading"]}}
            )
            if existing_by_hash:
                query = {"file_id": existing_by_hash["file_id"]}

        doc = await self.db.download_queue.find_one_and_update(
            query,
            {
                "$setOnInsert": {
                    "file_id": file_id,
                    "infohash": infohash,
                    "status": "queued",
                    "progress": 0.0,
                    "download_speed": 0,
                    "eta_seconds": 0,
                    "peers": 0,
                    "retries": 0,
                    "created_at": now,
                },
                "$addToSet": {"requesters": user_id},
                "$set": {"updated_at": now},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if doc["status"] in ("failed", "completed"):  # "completed" here means the stored file vanished: redo it
            doc = await self.db.download_queue.find_one_and_update(
                {"file_id": doc["file_id"]},
                {"$set": {"status": "queued", "retries": 0, "updated_at": now}, "$addToSet": {"requesters": user_id}},
                return_document=ReturnDocument.AFTER,
            )
        return doc

    _FRESH_JOB_FIELDS = {
        "phase": "starting", "progress": 0.0, "download_speed": 0, "eta_seconds": 0, "peers": 0, "seeds": 0,
        "downloaded_bytes": 0, "total_size": 0, "up_done": 0, "up_total": 0, "up_speed": 0, "error": None,
    }

    async def claim_next_download_job(self) -> Optional[Dict[str, Any]]:
        now = utcnow()
        return await self.db.download_queue.find_one_and_update(
            {"status": "queued"},
            {"$set": {"status": "downloading", "started_at": now, "updated_at": now, **self._FRESH_JOB_FIELDS}},
            sort=[("created_at", ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )

    async def claim_download_job(self, file_id: str) -> Optional[Dict[str, Any]]:
        """Start THIS job right now (atomic). Used when a user opens the bot link: no waiting in the queue.
        Also re-claims a job a previous process left in downloading/uploading (restart / crash)."""
        now = utcnow()
        return await self.db.download_queue.find_one_and_update(
            {"file_id": file_id, "status": {"$in": ["queued", "downloading", "uploading"]}},
            {"$set": {"status": "downloading", "started_at": now, "updated_at": now, **self._FRESH_JOB_FIELDS}},
            return_document=ReturnDocument.AFTER,
        )

    async def get_download_job(self, file_id: str) -> Optional[Dict[str, Any]]:
        return await self.db.download_queue.find_one({"file_id": file_id})

    async def requeue_stale_jobs(self) -> int:
        """After a restart nothing is really downloading any more: put those jobs back to 'queued'."""
        res = await self.db.download_queue.update_many(
            {"status": {"$in": ["downloading", "uploading"]}},
            {"$set": {"status": "queued", "phase": "queued", "updated_at": utcnow()}},
        )
        return res.modified_count

    async def update_download_progress(self, file_id: str, stats: Dict[str, Any]) -> None:
        stats["updated_at"] = utcnow()
        await self.db.download_queue.update_one({"file_id": file_id}, {"$set": stats})

    async def finish_download_job(self, file_id: str, status: str, error: Optional[str] = None) -> Optional[Dict[str, Any]]:
        now = utcnow()
        doc = await self.db.download_queue.find_one_and_update(
            {"file_id": file_id},
            {"$set": {"status": status, "error": error, "completed_at": now, "updated_at": now}},
            return_document=ReturnDocument.AFTER,
        )
        await self.db.download_history.insert_one(
            {
                "file_id": file_id,
                "status": status,
                "error": error,
                "requesters": doc.get("requesters", []) if doc else [],
                "created_at": now,
            }
        )
        return doc


db = Database()
