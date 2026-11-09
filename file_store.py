import re
import html
import time
import uuid
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from typing import Optional, Dict, Any, List, Tuple

import aiohttp
from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand,
    BotCommandScopeChat,
)
from pyrogram.enums import ParseMode
from pyrogram.errors import UserNotParticipant, FloodWait, MessageNotModified

from config import FileStoreConfig, WebServerConfig, AutoUploaderConfig, TelegramCommonConfig
from database import db
import db_channel
import session_store
from torrent_engine import torrent_engine, parse_torrent_bytes
from auto_uploader import (
    human_readable_size, guarded, reply_with_buttons, ensure_chat, call_with_peer_retry, is_peer_error, effective_log_chat,
    format_duration, progress_bar, human_speed, session_name,
)
from base_url import is_public_url
from media_names import name_key, resolution_of_name, split_part
from admin_panel import attach_admin_panel, fmt_left
from config import TorrentSearchConfig
from torrent_search_bot import torrent_search_bot

logger = logging.getLogger("file_store")


def parse_tg_message_link(link: str) -> Optional[Tuple[Any, int]]:
    """
    Parses a Telegram message link:
    - https://t.me/c/123456789/45 -> (-100123456789, 45)
    - https://t.me/channelusername/45 -> ("channelusername", 45)
    """
    m_priv = re.search(r"t\.me/c/(\d+)/(\d+)", link)
    if m_priv:
        return int(f"-100{m_priv.group(1)}"), int(m_priv.group(2))
    m_pub = re.search(r"t\.me/([a-zA-Z0-9_]+)/(\d+)", link)
    if m_pub:
        return m_pub.group(1), int(m_pub.group(2))
    return None


class FileStoreBot:
    def __init__(self):
        self.app: Optional[Client] = None
        self.clone_clients: Dict[int, Client] = {}  # owner_id -> running Clone Client
        self.user_input_states: Dict[int, Dict[str, Any]] = {}  # user_id -> pending interactive state
        self._worker_tasks: List[asyncio.Task] = []
        self._active_jobs: set = set()                          # file_ids being processed by THIS process
        self._requester_clients: Dict[Tuple[str, int], Client] = {}  # (job, user) -> bot (main/clone) the user talks to
        self._tracked: set = set()                              # (job, user) that already see a live progress message
        self._uploading: set = set()                            # name keys this process is uploading itself (not admin files)
        self._own_msg_ids: set = set()                          # storage-channel messages the bot created itself (never indexed as admin files)
        self.admin_states: Dict[int, Dict[str, Any]] = {}
        self._post_start_task: Optional[asyncio.Task] = None
        self._db_chats: Dict[int, int] = {}                     # Database channels {chat_id: owner_id} (one per user, + the admin's)
        self._db_syncing: set = set()                           # chat ids with a backfill scan running

    async def refresh_db_channels(self) -> None:
        """Reload the list of Database channels (called at startup and whenever a user sets / removes one)."""
        try:
            self._db_chats = await db.get_db_channel_map()
        except Exception as exc:
            logger.warning("Could not load Database channels: %s", exc)

    async def start(self) -> None:
        """Connect to Telegram and register handlers - returns as soon as the bot can answer /start.
        Command menus, channel checks, stale-job requeue, clone restore and workers run in _post_start."""
        TelegramCommonConfig.SESSION_DIR.mkdir(parents=True, exist_ok=True)
        if FileStoreConfig.BOT_TOKEN:
            _sess = session_name("file_store_bot", FileStoreConfig.BOT_TOKEN)
            await session_store.restore(_sess)  # temporary-disk hosts: get the remembered channels back
            self.app = Client(
                _sess,
                api_id=FileStoreConfig.API_ID,
                api_hash=FileStoreConfig.API_HASH,
                bot_token=FileStoreConfig.BOT_TOKEN,
                workdir=str(TelegramCommonConfig.SESSION_DIR),
            )
            self._attach_handlers(self.app, is_clone=False)
            await self.app.start()
            session_store.start_backup(_sess)
            me = getattr(self.app, "me", None) or await self.app.get_me()
            FileStoreConfig.BOT_USERNAME = getattr(me, "username", None) or FileStoreConfig.BOT_USERNAME
            logger.info("File Store Bot started (@%s).", FileStoreConfig.BOT_USERNAME)
        else:
            logger.warning("FILE_STORE_BOT_TOKEN not set; running File Store worker in headless mode.")
        self._post_start_task = asyncio.create_task(self._post_start())

    async def _post_start(self) -> None:
        try:
            await db.wait_ready(60)

            async def commands() -> None:
                if not self.app:
                    return
                await self._register_bot_commands(self.app)
                try:
                    await self.app.set_bot_commands(
                        [
                            BotCommand("start", "CHECK I AM ALIVE."),
                            BotCommand("id", "GET ID OF TELEGRAM ACCOUNT."),
                            BotCommand("link", "TO STORE A SINGLE MESSAGE OR FILE."),
                            BotCommand("batch", "TO STORE MUTIPLE MESSAGES FROM..."),
                            BotCommand("clone", "CREATE YOUR OWN IDENTICAL CLONE..."),
                            BotCommand("activate", "TO ACTIVATE CLONE BOT."),
                            BotCommand("delete", "TO DELETE CLONE BOT."),
                            BotCommand("settings", "CUSTOMIZE YOUR SETTINGS AS YO..."),
                            BotCommand("broadcast", "REPLY THIS COMMAND TO YOUR..."),
                            BotCommand("admin", "ADMIN PANEL: BAN / PAID USERS."),
                        ]
                        + ([] if TorrentSearchConfig.BOT_TOKEN else [BotCommand("search", "SEARCH TORRENTS: /search NAME")]),
                        scope=BotCommandScopeChat(chat_id=FileStoreConfig.OWNER_ID),
                    )
                except Exception as exc:
                    logger.warning("Could not set admin command scope: %s", exc)

            async def requeue() -> None:
                # A restart killed every running download: put them back so they are picked up again
                try:
                    n = await db.requeue_stale_jobs()
                    if n:
                        logger.info("Re-queued %d download job(s) interrupted by the last shutdown.", n)
                except Exception as exc:
                    logger.warning("Could not re-queue stale jobs: %s", exc)

            async def env_db_channel() -> None:
                """DB_CHANNEL_ID from the environment: index it once at startup (later uploads are indexed live)."""
                await self.refresh_db_channels()
                chat = AutoUploaderConfig.DB_CHANNEL_ID
                if chat and self.app and not await db.count_db_channel_files(chat):
                    logger.info("Indexing DB_CHANNEL_ID %s (first start)...", chat)
                    await self.sync_db_channel(chat, FileStoreConfig.OWNER_ID)

            jobs = [requeue(), self._restore_active_clones(), env_db_channel()]
            if self.app:
                jobs.append(commands())
                jobs.append(ensure_chat(self.app, FileStoreConfig.STORAGE_CHANNEL_ID, "STORAGE_CHANNEL_ID"))
                log_chat = effective_log_chat()
                if log_chat and log_chat != FileStoreConfig.STORAGE_CHANNEL_ID:
                    jobs.append(ensure_chat(self.app, log_chat, "log channel"))
                if FileStoreConfig.STORAGE_CHANNEL_ID == AutoUploaderConfig.PUBLIC_CHANNEL_ID:
                    logger.warning(
                        "STORAGE_CHANNEL_ID equals PUBLIC_CHANNEL_ID: every downloaded video will be uploaded INTO the "
                        "public post channel. Use a separate private channel for storage."
                    )
            # requeue must finish before workers pick jobs up; the rest runs alongside
            await asyncio.gather(*jobs, return_exceptions=True)
            for i in range(FileStoreConfig.MAX_CONCURRENT_DOWNLOADS):
                self._worker_tasks.append(asyncio.create_task(self._download_worker_loop(worker_id=i + 1)))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("File Store background setup failed: %s", exc)

    async def stop(self) -> None:
        for t in self._worker_tasks:
            t.cancel()
        for owner_id, clone_app in list(self.clone_clients.items()):
            try:
                await clone_app.stop()
            except Exception:
                pass
        if self.app:
            await self.app.stop()

    async def _register_bot_commands(self, client: Client) -> None:
        """Sets the exact Bot Commands menu shown in Screenshot 2."""
        commands = [
            BotCommand("start", "CHECK I AM ALIVE."),
            BotCommand("id", "GET ID OF TELEGRAM ACCOUNT."),
            BotCommand("link", "TO STORE A SINGLE MESSAGE OR FILE."),
            BotCommand("batch", "TO STORE MUTIPLE MESSAGES FROM..."),
            BotCommand("clone", "CREATE YOUR OWN IDENTICAL CLONE..."),
            BotCommand("activate", "TO ACTIVATE CLONE BOT."),
            BotCommand("delete", "TO DELETE CLONE BOT."),
            BotCommand("settings", "CUSTOMIZE YOUR SETTINGS AS YO..."),
            BotCommand("broadcast", "REPLY THIS COMMAND TO YOUR..."),
        ]
        if not TorrentSearchConfig.BOT_TOKEN:
            commands.insert(2, BotCommand("search", "SEARCH TORRENTS: /search NAME"))
        try:
            await client.set_bot_commands(commands)
        except Exception as exc:
            logger.warning("Failed to set bot commands: %s", exc)

    # --- Clone Bot Lifecycle Manager (Shared API_ID & API_HASH) ---

    async def _restore_active_clones(self) -> None:
        clones = await db.get_all_active_clones()

        async def one(c_doc: Dict[str, Any]) -> None:
            try:
                await self.spawn_clone_client(c_doc["owner_id"], c_doc["bot_token"])
            except Exception as exc:
                logger.warning("Could not restore clone bot for owner %s: %s", c_doc.get("owner_id"), exc)

        await asyncio.gather(*(one(c) for c in clones))  # all clones come up at the same time

    async def spawn_clone_client(self, owner_id: int, bot_token: str) -> Dict[str, Any]:
        if owner_id in self.clone_clients:
            try:
                await self.clone_clients[owner_id].stop()
            except Exception:
                pass
            self.clone_clients.pop(owner_id, None)

        clone_app = Client(
            session_name(f"clone_bot_{owner_id}", bot_token),
            api_id=FileStoreConfig.API_ID,
            api_hash=FileStoreConfig.API_HASH,
            bot_token=bot_token,
            workdir=str(TelegramCommonConfig.SESSION_DIR),
        )
        self._attach_handlers(clone_app, is_clone=True, clone_owner_id=owner_id)
        await clone_app.start()
        me = await clone_app.get_me()
        await self._register_bot_commands(clone_app)
        self.clone_clients[owner_id] = clone_app
        if not await ensure_chat(clone_app, FileStoreConfig.STORAGE_CHANNEL_ID, "STORAGE_CHANNEL_ID (clone bot)"):
            logger.warning("Clone @%s cannot see the storage channel: add it as ADMIN there so it can copy files.", me.username)

        saved = await db.save_clone_bot(
            owner_id=owner_id,
            bot_token=bot_token,
            bot_id=me.id,
            bot_username=me.username or "",
            bot_name=me.first_name or "Clone Bot",
        )
        logger.info("Spawned Clone Bot @%s for owner_id=%d", me.username, owner_id)
        return saved

    # --- Start screen: 2 buttons per row (4 buttons) + one single START button = 5 ---

    async def build_start(self, user) -> Tuple[str, InlineKeyboardMarkup]:
        uid = user.id
        name = html.escape(user.first_name or "there")
        if uid == FileStoreConfig.OWNER_ID:
            plan = "👑 Admin"
        else:
            plan = "🆓 Free"
            try:
                if await db.is_user_premium(uid):
                    doc = await db.get_user(uid)
                    until = (doc or {}).get("premium_until")
                    plan = f"💎 Premium ✅ ({fmt_left(until)} left)" if until else "💎 Premium ✅ (lifetime)"
            except Exception as exc:  # a DB hiccup must never remove the menu
                logger.warning("Plan lookup failed for %s: %s", uid, exc)
        text = (
            f"👋 <b>Hello {name}, I am Alive!</b>\n\n"
            "🤖 <b>Advanced File Store &amp; Torrent Streaming Bot</b>\n"
            "Open a file link and I send the file instantly - from my database if it is already stored, "
            "otherwise I download it for you and keep it for the next person.\n\n"
            f"🆔 <b>Your ID:</b> <code>{uid}</code>\n"
            f"💼 <b>Plan:</b> {plan}\n\n"
            "• /link - store a single message or file\n"
            "• /batch - store multiple channel posts\n"
            "• /clone - create your own identical clone bot"
        )
        if uid == FileStoreConfig.OWNER_ID:
            text += "\n\n🛡 <b>Admin:</b> /admin · /broadcast · send me a video to store it in the database"
        try:
            clone_doc = await db.get_clone_bot(uid)
        except Exception:
            clone_doc = None
        if clone_doc and clone_doc.get("bot_username"):
            clone_btn = InlineKeyboardButton("🤖 CLONE", url=f"https://t.me/{clone_doc['bot_username']}")
        else:
            clone_btn = InlineKeyboardButton("🤖 CLONE", callback_data="st_menu|clone")
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("⚙️ SETTINGS", callback_data="st_menu|main"),
                    InlineKeyboardButton("💎 PREMIUM", callback_data="st_menu|premium"),
                ],
                [clone_btn, InlineKeyboardButton("🆔 MY ID", callback_data="st_menu|myid")],
                [InlineKeyboardButton("❌ CLOSE", callback_data="st_menu|close")],
            ]
        )
        return text, kb

    # --- Exact `/settings` Menu Builder (Matching Screenshot 1) ---

    async def build_settings_menu_keyboard(self, user_id: int) -> InlineKeyboardMarkup:
        clone_doc = await db.get_clone_bot(user_id)
        if clone_doc and clone_doc.get("bot_username"):
            # Matches the external arrow (↗) on "🤖 MY CLONE BOT" in Screenshot 1
            clone_btn = InlineKeyboardButton(
                "🤖 MY CLONE BOT",
                url=f"https://t.me/{clone_doc['bot_username']}",
            )
        else:
            clone_btn = InlineKeyboardButton("🤖 MY CLONE BOT", callback_data="st_menu|clone_s")

        return InlineKeyboardMarkup(
            [
                [clone_btn],
                [InlineKeyboardButton("💸 PREMIUM PLAN", callback_data="st_menu|premium")],
                [InlineKeyboardButton("📎 LINK SHORTNER", callback_data="st_menu|shortener")],
                [InlineKeyboardButton("⏰ TOKEN VERIFICATION", callback_data="st_menu|token_verify")],
                [InlineKeyboardButton("🍿 CUSTOM CAPTION", callback_data="st_menu|caption")],
                [InlineKeyboardButton("📢 CUSTOM FORCE SUBSCRIBE", callback_data="st_menu|fsub")],
                [InlineKeyboardButton("🔘 CUSTOM BUTTON", callback_data="st_menu|button")],
                [InlineKeyboardButton("♻️ AUTO DELETE", callback_data="st_menu|autodelete")],
                [InlineKeyboardButton("🔒 PROTECT CONTENT", callback_data="st_menu|protect")],
                [InlineKeyboardButton("≼ BACK", callback_data="st_menu|back")],
            ]
        )

    # --- Shortener, Force Subscribe, & Auto-Delete Helpers ---

    async def shorten_url(self, long_url: str, settings: Dict[str, Any]) -> str:
        if not settings.get("shortener_enabled"):
            return long_url
        domain = (settings.get("shortener_domain") or "").strip().rstrip("/")
        api_key = (settings.get("shortener_api_key") or "").strip()
        if not domain or not api_key:
            return long_url
        if not domain.startswith("http"):
            domain = f"https://{domain}"
        api_endpoint = f"{domain}/api?api={api_key}&url={quote(long_url)}"
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(api_endpoint) as resp:
                    data = await resp.json(content_type=None)
                    if isinstance(data, dict) and data.get("shortenedUrl"):
                        return data["shortenedUrl"]
        except Exception as exc:
            logger.warning("Shortener API call failed (%s): %s", domain, exc)
        return long_url

    async def check_force_subscribe(self, client: Client, user_id: int, message: Message, retry_payload: str) -> bool:
        settings = await db.get_bot_settings("file_store")
        fsub_channel = (settings.get("force_sub_channel") or "").strip()
        if not fsub_channel:
            return True

        chat_target: Any = int(fsub_channel) if fsub_channel.lstrip("-").isdigit() else fsub_channel
        try:
            member = await client.get_chat_member(chat_target, user_id)
            if str(member.status) in ("ChatMemberStatus.LEFT", "ChatMemberStatus.BANNED"):
                raise UserNotParticipant
            return True
        except UserNotParticipant:
            invite_link = f"https://t.me/{fsub_channel.lstrip('@')}"
            if isinstance(chat_target, int):
                try:
                    chat_obj = await client.get_chat(chat_target)
                    invite_link = chat_obj.invite_link or await client.export_chat_invite_link(chat_target)
                except Exception:
                    pass
            me = await client.get_me()
            retry_url = f"https://t.me/{me.username}?start={retry_payload}"
            kb = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("📢 Join Our Update Channel", url=invite_link)],
                    [InlineKeyboardButton("🔄 Try Again", url=retry_url)],
                ]
            )
            await message.reply_text(
                "❗ <b>Access Denied!</b>\n\n"
                "You must join our official channel below before accessing files. "
                "Click <b>Join</b> and then tap <b>Try Again</b>.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
            )
            return False
        except Exception as exc:
            logger.warning("Force subscribe check warning: %s", exc)
            return True

    async def check_token_verification(self, client: Client, user_id: int, message: Message, payload: str) -> bool:
        settings = await db.get_bot_settings("file_store")
        if not settings.get("token_verify_enabled"):
            return True
        if await db.is_user_token_verified(user_id):
            return True

        token = await db.create_verification_token(user_id, payload)
        me = await client.get_me()
        deep_verify_url = f"https://t.me/{me.username}?start=verify_{token}"
        short_verify_url = await self.shorten_url(deep_verify_url, settings)
        expire_hours = max(1, int(settings.get("token_verify_expire_seconds", 86400) / 3600))

        kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⏰ Verify Token Now", url=short_verify_url)],
                [InlineKeyboardButton("💸 Buy Premium (No Ads)", callback_data="st_menu|premium")],
            ]
        )
        await message.reply_text(
            f"🔐 <b>Token Verification Required</b>\n\n"
            f"Your free access token has expired or is not active.\n"
            f"Complete 1 quick verification step below to unlock unlimited files for <b>{expire_hours} hours</b>!",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return False

    async def _schedule_auto_delete(self, messages: List[Message], delay_seconds: int) -> None:
        await asyncio.sleep(delay_seconds)
        for msg in messages:
            try:
                await msg.delete()
            except Exception:
                pass

    def _build_media_buttons(
        self, file_id: str, part_number: int = 1, settings: Optional[Dict[str, Any]] = None, web: bool = True
    ) -> InlineKeyboardMarkup:
        watch_url = f"{WebServerConfig.BASE_URL}/watch/{file_id}?part={part_number}"
        dl_url = f"{WebServerConfig.BASE_URL}/download/{file_id}/{part_number}"
        rows = []
        # web=False: the file was uploaded by hand (no copy on this server to stream), so no Watch/Download buttons
        if web and is_public_url(WebServerConfig.BASE_URL):  # Telegram rejects the whole message on a localhost/private button URL
            rows.append(
                [
                    InlineKeyboardButton("▶️ Watch Online", url=watch_url),
                    InlineKeyboardButton("📥 Direct Download", url=dl_url),
                ]
            )
        if settings and settings.get("custom_button_text") and settings.get("custom_button_url"):
            rows.append([InlineKeyboardButton(settings["custom_button_text"], url=settings["custom_button_url"])])
        return InlineKeyboardMarkup(rows)

    def _format_file_caption(
        self, filename: str, size_bytes: int, quality: str, part_label: str, settings: Dict[str, Any]
    ) -> str:
        template = settings.get("custom_caption") or FileStoreConfig.CUSTOM_CAPTION
        size_str = human_readable_size(size_bytes)
        try:
            rendered = template.format(filename=f"{filename}{part_label}", size=size_str, quality=quality)
        except Exception:
            rendered = f"🎬 <b>{filename}{part_label}</b>\n📦 <b>Size:</b> {size_str}"
        return rendered

    async def deliver_completed_file_to_user(
        self, user_id: int, file_id: str, client: Optional[Client] = None
    ) -> int:
        """Sends every part as a real video / document. Returns how many parts went out as media (0 = nothing sent)."""
        active_client = client or self.app
        if not active_client:
            return 0
        file_doc = await db.find_completed_file(file_id)
        if not file_doc:
            return 0
        parts = await db.get_file_parts(file_doc["file_id"])
        settings = await db.get_bot_settings("file_store")
        protect = bool(settings.get("protect_content", False))
        auto_del = bool(settings.get("auto_delete_enabled", False))
        auto_del_secs = int(settings.get("auto_delete_seconds", 600))

        parts = sorted(parts, key=lambda p: p["part_number"])
        # parts may have been uploaded one by one: the label must still say "2/3" even if part 1 is not there yet
        total_parts = max([len(parts), int(file_doc.get("total_parts") or 0)] + [p["part_number"] for p in parts])
        sent_messages: List[Message] = []
        delivered = 0

        for part in parts:
            part_num = part["part_number"]
            part_label = f" (Part {part_num}/{total_parts})" if total_parts > 1 else ""
            quality = (
                part.get("resolution") or file_doc.get("resolution")
                or resolution_of_name(part.get("part_filename")) or "HD"
            )
            caption = self._format_file_caption(
                filename=part["part_filename"],
                size_bytes=part["size_bytes"],
                quality=quality,
                part_label=part_label,
                settings=settings,
            )
            kb = self._build_media_buttons(file_doc["file_id"], part_num, settings, web=bool(part.get("file_path")))

            msg = await self._send_part_media(active_client, user_id, part, caption, kb, protect)
            if msg is not None:
                sent_messages.append(msg)
                if part.get("telegram_file_id") or part.get("telegram_message_id"):
                    delivered += 1
                if total_parts > 1:
                    await asyncio.sleep(0.3)
            else:
                logger.error("Could not deliver part %d of %s to user %d by any route", part_num, file_id, user_id)
                try:
                    await active_client.send_message(user_id, "⚠️ Could not send the file right now. Please tap the link again in a minute.")
                except Exception:
                    pass

        if auto_del and sent_messages:
            mins = max(1, auto_del_secs // 60)
            warn = await active_client.send_message(
                chat_id=user_id,
                text=f"♻️ <b>Auto-Delete Enabled:</b> These files will be automatically deleted in <b>{mins} minute(s)</b>. Forward or save them now!",
                parse_mode=ParseMode.HTML,
            )
            sent_messages.append(warn)
            asyncio.create_task(self._schedule_auto_delete(sent_messages, auto_del_secs))
        return delivered

    # ------------------------------------------------------------------ #
    # File request: instant delivery if stored, otherwise the download starts RIGHT NOW (no queue)
    # and the progress / speed / ETA is shown live in the bot chat - never on a web page.
    # ------------------------------------------------------------------ #

    @staticmethod
    def _valid_requesters(job_doc: Optional[Dict[str, Any]]) -> List[int]:
        # id 0 means "nobody asked yet" (prefetch). Messaging it is what raised 'Peer id invalid'.
        return [u for u in (job_doc or {}).get("requesters", []) if isinstance(u, int) and u > 0]

    @staticmethod
    def _friendly_error(exc: BaseException) -> str:
        if is_peer_error(exc):
            return (
                "Storage channel not reachable (peer not found). Admin: add the File Store bot as ADMIN of "
                "STORAGE_CHANNEL_ID and check the ID."
            )
        return str(exc)[:300] or exc.__class__.__name__

    @staticmethod
    def _elapsed_since(started: Any) -> str:
        if not started:
            return "0s"
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        return format_duration((datetime.now(timezone.utc) - started).total_seconds())

    def _job_text(self, name: str, doc: Dict[str, Any]) -> str:
        phase = doc.get("phase") or "starting"
        elapsed = self._elapsed_since(doc.get("started_at"))
        head = f"📄 <code>{html.escape(name)}</code>\n\n"

        if phase == "uploading":
            total = int(doc.get("up_total") or 0)
            done = int(doc.get("up_done") or 0)
            pct = (done / total * 100) if total else 0.0
            speed = float(doc.get("up_speed") or 0)
            eta = format_duration((total - done) / speed) if speed > 0 and total else "calculating…"
            parts = int(doc.get("up_parts") or 1)
            part_txt = f" (part {int(doc.get('up_part') or 1)}/{parts})" if parts > 1 else ""
            return (
                f"📤 <b>Uploading to Telegram{part_txt}</b>\n\n{head}"
                f"{progress_bar(pct)} <b>{pct:.1f}%</b>\n"
                f"📦 {human_readable_size(done)} / {human_readable_size(total)}\n"
                f"🚀 Speed: {human_speed(speed)}\n"
                f"⏳ ETA: {eta}\n"
                f"⏱ Elapsed: {elapsed}"
            )
        if phase == "processing":
            return f"⚙️ <b>Processing video (thumbnail / split)…</b>\n\n{head}⏱ Elapsed: {elapsed}"
        if phase in ("starting", "queued"):
            return f"🚀 <b>Starting download…</b>\n\n{head}⏱ Elapsed: {elapsed}"

        pct = float(doc.get("progress") or 0.0)
        total = int(doc.get("total_size") or 0)
        done = int(doc.get("downloaded_bytes") or 0)
        speed = float(doc.get("download_speed") or 0)
        eta_s = int(doc.get("eta_seconds") or 0)
        eta = format_duration(eta_s) if speed > 0 and eta_s >= 0 else "calculating…"
        title = "🔎 <b>Connecting to peers…</b>" if phase == "connecting" else "⬇️ <b>Downloading</b>"
        return (
            f"{title}\n\n{head}"
            f"{progress_bar(pct)} <b>{pct:.1f}%</b>\n"
            f"📦 {human_readable_size(done)} / {human_readable_size(total)}\n"
            f"🚀 Speed: {human_speed(speed)}\n"
            f"⏳ ETA: {eta}\n"
            f"⏱ Elapsed: {elapsed}\n"
            f"👥 Peers: {int(doc.get('peers') or 0)} (seeds {int(doc.get('seeds') or 0)})"
        )

    async def _track_progress(self, user_id: int, job_id: str, name: str, status_msg: Message) -> None:
        """Keeps ONE chat message updated (progress bar, speed, ETA, elapsed) until the job ends."""
        key = (job_id, user_id)
        self._tracked.add(key)
        interval = max(2, FileStoreConfig.PROGRESS_UPDATE_SECONDS)
        last_text = None
        edit_failures = 0
        try:
            while True:
                doc = await db.get_download_job(job_id)
                if not doc or doc.get("status") in ("completed", "failed"):
                    break
                text = self._job_text(name, doc)
                if text != last_text:
                    try:
                        await status_msg.edit_text(text, parse_mode=ParseMode.HTML)
                        last_text = text
                        edit_failures = 0
                    except MessageNotModified:
                        last_text = text
                    except FloodWait as fw:
                        await asyncio.sleep(int(getattr(fw, "value", 3) or 3) + 1)
                    except Exception as exc:
                        edit_failures += 1
                        if edit_failures >= 3:
                            logger.warning("Progress message for user %s stopped: %s", user_id, exc)
                            return
                await asyncio.sleep(interval)

            doc = await db.get_download_job(job_id) or {}
            try:
                if doc.get("status") == "completed":
                    await status_msg.edit_text(
                        f"✅ <b>Download complete!</b>\n\n📄 <code>{html.escape(name)}</code>\n"
                        f"⏱ Total time: {self._elapsed_since(doc.get('started_at'))}\n\n<i>Sending your file…</i>",
                        parse_mode=ParseMode.HTML,
                    )
                elif doc.get("status") == "failed":
                    await status_msg.edit_text(
                        f"❌ <b>Download failed</b>\n\n📄 <code>{html.escape(name)}</code>\n"
                        f"⚠️ {html.escape(str(doc.get('error') or 'Unknown error'))}\n\n"
                        f"<i>Open the link again to retry.</i>",
                        parse_mode=ParseMode.HTML,
                    )
            except Exception:
                pass
        finally:
            self._tracked.discard(key)

    def _start_job(self, job_id: str, immediate: bool) -> bool:
        """Run a job in the background right now. False if this process already runs it."""
        if job_id in self._active_jobs:
            return False
        self._active_jobs.add(job_id)  # added synchronously: two users can never start it twice
        asyncio.create_task(self._run_job(job_id, immediate=immediate))
        return True

    async def _run_job(self, file_id: str, immediate: bool = False) -> None:
        self._active_jobs.add(file_id)
        try:
            await self._process_download_job(file_id, immediate=immediate)
        except Exception as exc:  # _process_download_job reports its own errors; this is the last safety net
            logger.exception("Download job %s crashed: %s", file_id, exc)
        finally:
            self._active_jobs.discard(file_id)

    # --- Files the admin uploads by hand: indexed so users get them from the database, never a download ---

    VIDEO_EXTS = (".mkv", ".mp4", ".avi", ".mov", ".webm", ".m4v", ".ts")

    def _media_of(self, message: Message) -> Optional[Tuple[Any, str]]:
        """(media, name) for a video / video-document; name falls back to the caption's first line. None = not a video."""
        media = message.video or message.document
        if not media:
            return None
        name = (getattr(media, "file_name", None) or "").strip()
        if message.document:
            mime = str(getattr(media, "mime_type", "") or "")
            if not (mime.startswith("video/") or name.lower().endswith(self.VIDEO_EXTS)):
                return None
        name = self._best_name(name, message.caption or "")
        return media, name

    def _best_name(self, file_name: str, caption: str) -> str:
        """Admin uploads often have no file name (Telegram 'video') or a caption with several lines: pick the line
        that looks most like a release name (resolution / year / extension), the real file name wins ties."""
        def score(t: str) -> int:
            return (2 if resolution_of_name(t) else 0) + (2 if re.search(r"(?<!\d)(19|20)\d{2}(?!\d)", t) else 0) \
                + (1 if t.lower().endswith(self.VIDEO_EXTS) else 0)
        lines = [re.sub(r"[*_`~|]+", "", ln).strip() for ln in (caption or "").splitlines()]
        cands = [c for c in [file_name] + [ln for ln in lines if ln] if c]
        if not cands:
            return ""
        best = max(cands, key=lambda c: (score(c), c is file_name))
        return best.strip()

    async def index_admin_media(self, message: Message, check_only: bool = False, message_id: Optional[int] = None) -> Dict[str, Any]:
        found = self._media_of(message)
        if not found:
            return {"status": "skipped", "reason": "not a video file"}
        media, name = found
        if not name:
            return {"status": "skipped", "reason": "the file has no name - add the name as the caption"}
        return await db.index_media_part(
            filename=name,
            size_bytes=int(getattr(media, "file_size", 0) or 0),
            tg_file_id=media.file_id,
            tg_unique_id=getattr(media, "file_unique_id", None),
            tg_message_id=message.id if message_id is None else message_id,
            duration=int(getattr(media, "duration", 0) or 0),
            check_only=check_only,
        )

    # ------------------------------------------------------------------ #
    # Database channels (per user): every video posted there is indexed, so a new post can reuse it
    # instead of downloading. A bot cannot search a channel, therefore we keep our own index.
    # ------------------------------------------------------------------ #

    async def index_db_channel_message(self, message: Message) -> Optional[str]:
        chat_id = getattr(getattr(message, "chat", None), "id", None)
        owner = self._db_chats.get(chat_id)
        if owner is None or getattr(message, "empty", False):
            return None
        found = self._media_of(message)
        if not found or not found[1]:
            return None
        media, name = found
        return await db.save_db_channel_file(
            chat_id=chat_id,
            owner_id=owner,
            message_id=message.id,
            file_name=name,
            size_bytes=int(getattr(media, "file_size", 0) or 0),
            tg_file_id=media.file_id,
            tg_unique_id=getattr(media, "file_unique_id", None),
            duration=int(getattr(media, "duration", 0) or 0),
        )

    async def sync_db_channel(self, chat_id: int, owner_id: int, progress=None, full: bool = True) -> Dict[str, int]:
        """Backfill: read the channel's existing messages by id (a bot may read ids it can see) and index the videos.
        Stops after ~1000 empty ids in a row (= past the newest message) or at DBCHANNEL_SCAN_LIMIT."""
        stats = {"scanned": 0, "indexed": 0, "videos": 0, "removed": 0, "unnamed": 0}
        seen: set = set()
        completed = False
        if not self.app or chat_id in self._db_syncing:
            return stats
        self._db_chats[chat_id] = owner_id
        self._db_syncing.add(chat_id)
        try:
            await ensure_chat(self.app, chat_id, "Database channel")
            limit = int(getattr(FileStoreConfig, "DBCHANNEL_SCAN_LIMIT", 30000))
            empty_run, start = 0, 1
            while start <= limit and empty_run < 1000:
                ids = list(range(start, start + 200))
                start += 200
                try:
                    msgs = await self.app.get_messages(chat_id, ids)
                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 1)
                    start -= 200
                    continue
                batch_found = 0
                for m in msgs or []:
                    if not m or getattr(m, "empty", False):
                        continue
                    batch_found += 1
                    stats["scanned"] += 1
                    fm = self._media_of(m)
                    if fm:
                        stats["videos"] += 1
                        if not fm[1]:
                            stats["unnamed"] += 1
                        if await self.index_db_channel_message(m):
                            stats["indexed"] += 1
                            seen.add(m.id)
                empty_run = 0 if batch_found else empty_run + 200
                if progress:
                    try:
                        await progress(stats)
                    except Exception:
                        pass
                await asyncio.sleep(0.4)
            completed = True
            if full and completed:  # forget files that were deleted from the channel
                stats["removed"] = await db.prune_db_channel_files(chat_id, seen)
        finally:
            self._db_syncing.discard(chat_id)
        logger.info("Database channel %s synced: %s", chat_id, stats)
        return stats

    @staticmethod
    def _index_report(res: Dict[str, Any], name: str) -> str:
        if res["status"] == "new":
            part = ""
            if res.get("total_parts", 1) > 1 or res.get("part_number", 1) > 1:
                part = f"\n🧩 <b>Part:</b> {res['part_number']} of {res.get('total_parts', res['part_number'])} stored so far"
            return (
                "✅ <b>Stored in database</b>\n\n"
                f"🎬 <code>{html.escape(name)}</code>\n"
                f"💿 <b>Quality:</b> {res.get('quality', 'HD')}{part}\n\n"
                "<i>Users who request this release get it straight from the database - no download.</i>"
            )
        if res["status"] == "duplicate":
            return f"♻️ <b>Already in the database</b> - not stored again.\n<i>{html.escape(res.get('reason', ''))}</i>"
        return f"ℹ️ Not stored: {html.escape(res.get('reason', 'unsupported file'))}"

    @staticmethod
    def _torrent_names(torrent_doc: Dict[str, Any]) -> List[str]:
        """Every name this torrent file may be stored under (magnet name, torrent name, post name)."""
        return [n for n in (torrent_doc.get("original_filename"), torrent_doc.get("display_name")) if n]

    async def _send_part_media(self, client: Client, user_id: int, part: Dict[str, Any], caption: str, kb, protect: bool):
        """Send ONE part as the real video/document, trying every route before giving up (returns None then).
        main bot : cached file id -> copy from the source channel -> same without buttons
        clone bot: copy from the source channel (clone must be admin) -> main bot copy -> ...
        Only a part that has no Telegram file at all (web-only) gets the text card."""
        fid, mid = part.get("telegram_file_id"), part.get("telegram_message_id")
        src = part.get("source_chat_id") or FileStoreConfig.STORAGE_CHANNEL_ID
        if not fid and not mid:
            return await client.send_message(chat_id=user_id, text=caption, parse_mode=ParseMode.HTML, reply_markup=kb, protect_content=protect)
        is_main = client is self.app or client == self.app
        main = self.app

        def cached(c, markup):
            return lambda: c.send_cached_media(chat_id=user_id, file_id=fid, caption=caption, parse_mode=ParseMode.HTML,
                                               reply_markup=markup, protect_content=protect)

        def copy(c, markup, label):
            return lambda: call_with_peer_retry(
                c, src,
                lambda: c.copy_message(chat_id=user_id, from_chat_id=src, message_id=mid, caption=caption,
                                       parse_mode=ParseMode.HTML, reply_markup=markup, protect_content=protect),
                label,
            )

        steps = []
        if is_main:
            if fid:
                steps += [("cached", cached(client, kb))]
            if mid:
                steps += [("copy", copy(client, kb, "source channel"))]
            if fid:
                steps += [("cached/no buttons", cached(client, None))]
            if mid:
                steps += [("copy/no buttons", copy(client, None, "source channel"))]
        else:
            if mid:
                steps += [("clone copy", copy(client, kb, "source channel (clone bot)"))]
            if mid and main:
                steps += [("main-bot copy", copy(main, kb, "source channel"))]
            if mid:
                steps += [("clone copy/no buttons", copy(client, None, "source channel (clone bot)"))]
        for label, fn in steps:
            for attempt in (1, 2):
                try:
                    return await fn()
                except FloodWait as fw:
                    await asyncio.sleep(min(fw.value + 1, 30))
                    continue
                except Exception as exc:
                    logger.warning("Delivery route '%s' failed for user %d: %s", label, user_id, exc)
                    break
        return None

    async def verify_db_channel_docs(self, docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Check each indexed Database-channel file against the live message. Gone / not a video -> dropped from the
        index. Still there -> its file id is refreshed. Cannot check (bot offline / no access) -> kept."""
        if not self.app:
            return docs
        good: List[Dict[str, Any]] = []
        for d in docs:
            try:
                msg = await self.app.get_messages(d["chat_id"], d["message_id"])
            except FloodWait as fw:
                await asyncio.sleep(min(fw.value + 1, 30))
                good.append(d)
                continue
            except Exception as exc:
                logger.info("Could not re-check %s/%s (%s) - keeping it.", d.get("chat_id"), d.get("message_id"), exc)
                good.append(d)
                continue
            found = None if (not msg or getattr(msg, "empty", False)) else self._media_of(msg)
            if not found:
                logger.info("Database file %s/%s is gone or not a video: removed from the index.", d.get("chat_id"), d.get("message_id"))
                await db.remove_db_channel_file(d["chat_id"], d["message_id"])
                continue
            media = found[0]
            d = dict(d, telegram_file_id=media.file_id, telegram_unique_id=getattr(media, "file_unique_id", None),
                     size_bytes=int(getattr(media, "file_size", 0) or d.get("size_bytes") or 0))
            good.append(d)
        return good

    async def _verify_stored_db_file(self, file_doc: Dict[str, Any]) -> bool:
        """Before sending a Database-channel file: every part must still exist as a video in its channel."""
        parts = await db.get_file_parts(file_doc["file_id"])
        if not parts:
            return False
        docs = [{"chat_id": p.get("source_chat_id"), "message_id": p.get("telegram_message_id"),
                 "telegram_file_id": p.get("telegram_file_id")} for p in parts if p.get("source_chat_id")]
        if len(docs) != len(parts):
            return True  # not a channel-sourced part: nothing to re-check
        ok = await self.verify_db_channel_docs(docs)
        if len(ok) != len(docs):
            await db.db.files.update_one({"file_id": file_doc["file_id"]}, {"$set": {"status": "missing"}})
            return False
        for p, d in zip(parts, ok):
            if d.get("telegram_file_id") and d["telegram_file_id"] != p.get("telegram_file_id"):
                await db.db.file_parts.update_one({"file_id": p["file_id"], "part_number": p["part_number"]},
                                                  {"$set": {"telegram_file_id": d["telegram_file_id"]}})
        return True

    async def _find_stored(self, file_id: str, torrent_doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Database first: by id / alias / magnet hash / release name (also finds files the admin uploaded directly)."""
        found = await db.find_completed_file(file_id, torrent_doc.get("infohash"), names=self._torrent_names(torrent_doc))
        if not found:  # not stored yet: is it in a Database channel (name + year + quality)? then send that, never download
            try:
                movie = await db.get_movie(torrent_doc.get("movie_id") or "") or {}
                owner = movie.get("owner_id")
                if owner is not None:
                    rel = {"original_filename": torrent_doc.get("original_filename"),
                           "display_name": torrent_doc.get("display_name"),
                           "quality_label": torrent_doc.get("quality_label")}
                    hit = await db_channel.find_for_release(int(owner), movie.get("title") or "", movie.get("year"), rel)
                    if hit:
                        found = await db.find_completed_file(hit["file_id"])
            except Exception as exc:
                logger.warning("Database channel check failed for %s: %s", file_id, exc)
        if found and found.get("source") == "db_channel" and not await self._verify_stored_db_file(found):
            logger.info("Stored Database file %s is no longer in its channel: falling back to the download.", found["file_id"])
            return None
        if found and found["file_id"] != file_id:
            await db.add_file_alias(file_id, found["file_id"])  # next time (and the web links) resolve instantly
        return found

    async def _found_text(self, completed: Dict[str, Any]) -> str:
        parts = await db.get_file_parts(completed["file_id"])
        total = max([len(parts), int(completed.get("total_parts") or 0)] + [p["part_number"] for p in parts])
        name = completed.get("original_filename") or (parts[0]["part_filename"] if parts else "")
        quality = completed.get("resolution") or resolution_of_name(name) or "HD"
        size = sum(int(p.get("size_bytes") or 0) for p in parts) or int(completed.get("file_size") or 0)
        return (
            "✅ <b>File found in database! Sending immediately...</b>\n\n"
            f"🎬 <code>{html.escape(name)}</code>\n"
            f"💿 <b>Quality:</b> {quality}\n"
            f"📦 <b>Size:</b> {human_readable_size(size)}"
            + (f"\n🧩 <b>Parts:</b> {total}" if total > 1 else "")
        )

    async def handle_file_request(self, client: Client, user_id: int, file_id: str, message: Message) -> None:
        torrent_doc = await db.get_torrent(file_id)
        if not torrent_doc:
            await message.reply_text("❌ Requested file ID was not found in the system.")
            return

        # 1. Already stored (downloaded earlier, or uploaded by the admin, whole or in parts)? -> send immediately.
        completed = await self._find_stored(file_id, torrent_doc)
        if completed:
            await message.reply_text(await self._found_text(completed), parse_mode=ParseMode.HTML)
            sent = await self.deliver_completed_file_to_user(user_id, completed["file_id"], client=client)
            if sent or completed.get("source") != "db_channel":
                return
            # the stored Database copy could not be sent by any route: never leave the user with only a message
            await db.db.files.update_one({"file_id": completed["file_id"]}, {"$set": {"status": "missing"}})
            await message.reply_text("⚠️ That stored copy could not be sent - downloading it for you instead…")

        # 2. Not stored yet: subscribe this user to the job (one job per infohash) ...
        lock_doc = await db.acquire_download_lock(file_id, torrent_doc.get("infohash"), user_id)
        job_id = lock_doc["file_id"]
        name = torrent_doc.get("original_filename") or torrent_doc.get("display_name") or file_id
        self._requester_clients[(job_id, user_id)] = client

        # 3. ... and START IT NOW. No waiting in a queue: claim this exact job and run it in the background.
        if job_id not in self._active_jobs:
            claimed = await db.claim_download_job(job_id)
            if claimed:
                lock_doc = claimed
                self._start_job(job_id, immediate=True)

        # 4. Live progress message (progress bar, speed, ETA, elapsed) lives in THIS chat.
        status_msg = await message.reply_text(self._job_text(name, lock_doc), parse_mode=ParseMode.HTML)
        asyncio.create_task(self._track_progress(user_id, job_id, name, status_msg))

    # --- Background Torrent Download & Split Workers ---

    async def _download_worker_loop(self, worker_id: int) -> None:
        """Only picks up jobs nobody opened yet (prefetch) or interrupted ones. User clicks never wait for this."""
        logger.info("File Store Download Worker #%d started.", worker_id)
        while True:
            try:
                job = await db.claim_next_download_job()
                if not job:
                    await asyncio.sleep(3)
                    continue
                if job["file_id"] in self._active_jobs:
                    continue
                await self._run_job(job["file_id"], immediate=False)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Worker #%d encountered error: %s", worker_id, exc)
                await asyncio.sleep(5)

    async def _ensure_torrent_file(self, torrent_doc: Dict[str, Any]) -> Optional[str]:
        """.torrent on disk -> use it. Gone (new disk / redeploy)? -> take the copy the Auto Uploader
        stored in the log channel. Still nothing -> None (the engine then re-downloads it from the URL / magnet)."""
        path = torrent_doc.get("torrent_file_path")
        if path and Path(path).exists():
            return path
        chat = torrent_doc.get("log_chat_id") or effective_log_chat()
        msg_id = torrent_doc.get("log_message_id")
        if not (self.app and chat and msg_id):
            return None
        try:
            await ensure_chat(self.app, chat, "log channel")
            msg = await self.app.get_messages(chat, msg_id)
            if msg and getattr(msg, "document", None):
                dest = FileStoreConfig.TORRENT_DIR / f"{torrent_doc.get('infohash') or torrent_doc['file_id']}.torrent"
                dest.parent.mkdir(parents=True, exist_ok=True)
                out = await self.app.download_media(msg, file_name=str(dest))
                if out and parse_torrent_bytes(Path(out).read_bytes()):
                    logger.info("Loaded .torrent for %s from the log channel.", torrent_doc["file_id"])
                    return str(out)
        except Exception as exc:
            logger.warning("Could not load the .torrent from the log channel (%s); falling back to URL/magnet.", exc)
        return None

    async def _deliver_to_requesters(self, job_doc: Optional[Dict[str, Any]], delivered_file_id: str, job_id: str) -> None:
        for uid in self._valid_requesters(job_doc):
            client = self._requester_clients.pop((job_id, uid), None)
            try:
                await self.deliver_completed_file_to_user(uid, delivered_file_id, client=client)
            except Exception as exc:
                logger.warning("Could not deliver %s to user %s: %s", delivered_file_id, uid, exc)

    async def _process_download_job(self, file_id: str, immediate: bool = False) -> None:
        torrent_doc = await db.get_torrent(file_id)
        if not torrent_doc:
            await db.finish_download_job(file_id, status="failed", error="Torrent record missing")
            return

        already_done = await self._find_stored(file_id, torrent_doc)
        if already_done:
            job_doc = await db.finish_download_job(file_id, status="completed")
            await self._deliver_to_requesters(job_doc, already_done["file_id"], file_id)
            return

        async def on_progress(stats: Dict[str, Any]) -> None:
            done = int(stats.get("downloaded_bytes") or 0)
            stats["status"] = "downloading"
            stats["phase"] = "connecting" if (done == 0 and not stats.get("peers")) else "downloading"
            await db.update_download_progress(file_id, stats)

        own_key: Optional[str] = None
        try:
            torrent_path = await self._ensure_torrent_file(torrent_doc)
            dl_result = await torrent_engine.download_torrent(
                file_id=file_id,
                torrent_url=torrent_doc.get("torrent_url"),
                magnet_uri=torrent_doc.get("magnet_uri"),
                progress_callback=on_progress,
                torrent_file_path=torrent_path,
                immediate=immediate,
            )

            # The real file name is known only now: look in the database once more before uploading anything.
            existing = await db.find_completed_file(
                file_id, dl_result["infohash"], names=[dl_result["original_filename"]] + self._torrent_names(torrent_doc)
            )
            if existing:
                logger.info("%s is already stored as %s - skipping upload (no duplicates).", file_id, existing["file_id"])
                await db.add_file_alias(file_id, existing["file_id"])
                job_doc = await db.finish_download_job(file_id, status="completed")
                await self._deliver_to_requesters(job_doc, existing["file_id"], file_id)
                return

            await db.update_download_progress(file_id, {"status": "downloading", "phase": "processing", "progress": 100.0})
            video_path = Path(dl_result["file_path"])
            own_key = name_key(dl_result["original_filename"])
            self._uploading.add(own_key)  # the storage-channel indexer must not treat our own parts as admin uploads
            part_paths = await torrent_engine.split_video_if_needed(
                video_path, FileStoreConfig.MAX_SPLIT_SIZE_BYTES
            )

            parts_metadata: List[Dict[str, Any]] = []
            total_parts = len(part_paths)
            settings = await db.get_bot_settings("file_store")

            for idx, p_path in enumerate(part_paths, start=1):
                thumb_path = await torrent_engine.generate_thumbnail(p_path, f"{file_id}_part{idx}")
                duration = int(await torrent_engine.probe_video_duration(p_path))
                tg_file_id = None
                tg_unique_id = None
                tg_msg_id = None

                if self.app:
                    await db.update_download_progress(
                        file_id,
                        {"status": "uploading", "phase": "uploading", "up_done": 0, "up_total": p_path.stat().st_size,
                         "up_speed": 0, "up_part": idx, "up_parts": total_parts},
                    )
                    kb = self._build_media_buttons(file_id, idx, settings)
                    caption = (
                        f"🎬 <b>{html.escape(p_path.name)}</b>\n"
                        f"📦 Part {idx}/{total_parts} | {human_readable_size(p_path.stat().st_size)}"
                    )
                    upload_cb = self._make_upload_progress(file_id, idx, total_parts)
                    chat = FileStoreConfig.STORAGE_CHANNEL_ID
                    try:
                        if p_path.suffix.lower() in (".mp4", ".mkv", ".avi", ".mov", ".webm"):
                            sent = await call_with_peer_retry(
                                self.app, chat,
                                lambda: self.app.send_video(
                                    chat_id=chat,
                                    video=str(p_path),
                                    caption=caption,
                                    parse_mode=ParseMode.HTML,
                                    duration=duration,
                                    thumb=str(thumb_path) if thumb_path else None,
                                    supports_streaming=True,
                                    reply_markup=kb,
                                    progress=upload_cb,
                                ),
                                "STORAGE_CHANNEL_ID",
                            )
                            media = sent.video or sent.document
                            tg_file_id = media.file_id if media else None
                            tg_unique_id = media.file_unique_id if media else None
                        else:
                            sent = await call_with_peer_retry(
                                self.app, chat,
                                lambda: self.app.send_document(
                                    chat_id=chat,
                                    document=str(p_path),
                                    caption=caption,
                                    parse_mode=ParseMode.HTML,
                                    thumb=str(thumb_path) if thumb_path else None,
                                    reply_markup=kb,
                                    progress=upload_cb,
                                ),
                                "STORAGE_CHANNEL_ID",
                            )
                            tg_file_id = sent.document.file_id if sent.document else None
                            tg_unique_id = sent.document.file_unique_id if sent.document else None
                        tg_msg_id = sent.id
                    except Exception as upload_exc:
                        # Do NOT mark the file as completed without a Telegram copy: users would get an empty message.
                        logger.error("Telegram upload to storage channel failed for %s: %s", p_path.name, upload_exc)
                        raise RuntimeError(f"Upload to the storage channel failed: {self._friendly_error(upload_exc)}") from upload_exc

                parts_metadata.append(
                    {
                        "file_id": file_id,
                        "part_number": idx,
                        "total_parts": total_parts,
                        "part_filename": p_path.name,
                        "file_path": str(p_path.resolve()),
                        "size_bytes": p_path.stat().st_size,
                        "duration": duration,
                        "thumbnail_path": str(thumb_path.resolve()) if thumb_path else None,
                        "telegram_file_id": tg_file_id,
                        "telegram_unique_id": tg_unique_id,
                        "telegram_message_id": tg_msg_id,
                        "infohash": dl_result["infohash"],  # parts are filed under the magnet hash
                        "name_key": own_key,
                        "resolution": resolution_of_name(dl_result["original_filename"]),
                        "source": "torrent",
                        "watch_url": f"{WebServerConfig.BASE_URL}/watch/{file_id}?part={idx}",
                        "download_url": f"{WebServerConfig.BASE_URL}/download/{file_id}/{idx}",
                    }
                )

            # Last duplicate guard (another job may have stored the same release while we were uploading).
            twin = await db.find_completed_file(file_id, dl_result["infohash"], names=[dl_result["original_filename"]])
            if twin and twin["file_id"] != file_id:
                logger.info("%s was stored meanwhile as %s - dropping our copy.", file_id, twin["file_id"])
                await db.add_file_alias(file_id, twin["file_id"])
                if self.app:
                    try:
                        await self.app.delete_messages(
                            FileStoreConfig.STORAGE_CHANNEL_ID,
                            [p["telegram_message_id"] for p in parts_metadata if p.get("telegram_message_id")],
                        )
                    except Exception:
                        pass
                job_doc = await db.finish_download_job(file_id, status="completed")
                await self._deliver_to_requesters(job_doc, twin["file_id"], file_id)
                return

            file_doc = {
                "file_id": file_id,
                "movie_id": torrent_doc["movie_id"],
                "infohash": dl_result["infohash"],
                "name_key": own_key,
                "resolution": resolution_of_name(dl_result["original_filename"]),
                "source": "torrent",
                "original_filename": dl_result["original_filename"],
                "file_path": str(video_path.resolve()),
                "file_size": dl_result["file_size"],
                "total_parts": total_parts,
                "watch_url": f"{WebServerConfig.BASE_URL}/watch/{file_id}",
                "download_url": f"{WebServerConfig.BASE_URL}/download/{file_id}/1",
            }
            await db.save_completed_file(file_doc, parts_metadata)
            finished_job = await db.finish_download_job(file_id, status="completed")
            await self._deliver_to_requesters(finished_job, file_id, file_id)

        except Exception as exc:
            logger.error("Download job failed for %s: %s", file_id, exc)
            error_text = self._friendly_error(exc)
            failed_job = await db.finish_download_job(file_id, status="failed", error=error_text)
            for uid in self._valid_requesters(failed_job):
                key = (file_id, uid)
                client = self._requester_clients.pop(key, None) or self.app
                if key in self._tracked or not client:
                    continue  # their live progress message turns into the failure notice
                try:
                    await client.send_message(
                        uid,
                        f"❌ Download failed for <code>{html.escape(torrent_doc['original_filename'])}</code>: "
                        f"{html.escape(error_text)}",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass
        finally:
            if own_key:
                self._uploading.discard(own_key)

    def _make_upload_progress(self, file_id: str, idx: int, total_parts: int):
        """Pyrogram upload callback -> throttled DB updates (speed is smoothed) shown in the live message."""
        state = {"t": 0.0, "last_bytes": 0, "last_time": time.monotonic(), "speed": 0.0}

        async def cb(current: int, total: int) -> None:
            now = time.monotonic()
            if current < total and (now - state["t"]) < 3:
                return
            dt = now - state["last_time"]
            if dt > 0 and current >= state["last_bytes"]:
                inst = (current - state["last_bytes"]) / dt
                state["speed"] = inst if not state["speed"] else 0.6 * state["speed"] + 0.4 * inst
            state.update(t=now, last_bytes=current, last_time=now)
            try:
                await db.update_download_progress(
                    file_id,
                    {"status": "uploading", "phase": "uploading", "up_done": int(current), "up_total": int(total),
                     "up_speed": int(state["speed"]), "up_part": idx, "up_parts": total_parts},
                )
            except Exception:
                pass

        return cb

    # --- Stored Single/Batch Message Delivery (/link & /batch) ---

    async def deliver_stored_link(self, client: Client, user_id: int, link_id: str) -> None:
        link_doc = await db.get_stored_link(link_id)
        if not link_doc:
            await client.send_message(user_id, "❌ Stored link not found or expired.")
            return

        settings = await db.get_bot_settings("file_store")
        protect = bool(settings.get("protect_content", False))
        auto_del = bool(settings.get("auto_delete_enabled", False))
        auto_del_secs = int(settings.get("auto_delete_seconds", 600))
        custom_btn = None
        if settings.get("custom_button_text") and settings.get("custom_button_url"):
            custom_btn = InlineKeyboardMarkup(
                [[InlineKeyboardButton(settings["custom_button_text"], url=settings["custom_button_url"])]]
            )

        sent_msgs: List[Message] = []
        msg_ids = range(link_doc["start_msg_id"], link_doc["end_msg_id"] + 1)
        for mid in msg_ids:
            try:
                copied = await client.copy_message(
                    chat_id=user_id,
                    from_chat_id=link_doc["chat_id"],
                    message_id=mid,
                    protect_content=protect,
                    reply_markup=custom_btn,
                )
                sent_msgs.append(copied)
                await asyncio.sleep(0.4)
            except FloodWait as fw:
                await asyncio.sleep(fw.value)
            except Exception:
                continue

        if auto_del and sent_msgs:
            mins = max(1, auto_del_secs // 60)
            warn = await client.send_message(
                user_id,
                f"♻️ <b>Auto-Delete Enabled:</b> These messages will be deleted in <b>{mins} minute(s)</b>.",
                parse_mode=ParseMode.HTML,
            )
            sent_msgs.append(warn)
            asyncio.create_task(self._schedule_auto_delete(sent_msgs, auto_del_secs))

    # --- Handlers for Main File Store Bot & All Spawned Clone Bots ---

    def _attach_handlers(self, client: Client, is_clone: bool = False, clone_owner_id: Optional[int] = None) -> None:
        if not TorrentSearchConfig.BOT_TOKEN:
            # No separate Torrent Search bot configured: /search <name> is built into this bot (and every clone).
            # Registered first so no other handler of this bot can swallow the command.
            torrent_search_bot.attach(client, standalone=False)

        if not is_clone:
            # /admin panel (ban / unban / paid / unpaid) - owner only, shared with the Auto Uploader bot
            self.admin_states = attach_admin_panel(client)

        @client.on_message(filters.command("start") & filters.private)
        @guarded
        async def start_cmd(c: Client, message: Message):
            user = message.from_user
            try:
                await db.register_user(user.id, user.first_name or "", user.username or "")
            except Exception as exc:
                logger.warning("register_user failed for %s: %s", user.id, exc)

            if len(message.command) < 2:
                text, kb = await self.build_start(user)
                await reply_with_buttons(message, text, kb)
                return

            payload = message.command[1].strip()

            # 1. Handle Token Verification Callback (/start verify_<token>)
            if payload.startswith("verify_"):
                token = payload.replace("verify_", "", 1)
                settings = await db.get_bot_settings("file_store")
                exp_secs = int(settings.get("token_verify_expire_seconds", 86400))
                orig_payload = await db.consume_verification_token(token, user.id, exp_secs)
                if not orig_payload:
                    await message.reply_text("❌ Verification token is invalid or already used.")
                    return
                await message.reply_text("✅ <b>Token Verified Successfully!</b> Sending your requested file...", parse_mode=ParseMode.HTML)
                payload = orig_payload

            # 2. Enforce Force Subscribe
            if not await self.check_force_subscribe(c, user.id, message, payload):
                return

            # 3. Enforce Token Verification (if enabled & non-premium)
            if not await self.check_token_verification(c, user.id, message, payload):
                return

            # 4. Route Payload: Stored /link or /batch
            if payload.startswith("store_"):
                link_id = payload.replace("store_", "", 1)
                await self.deliver_stored_link(c, user.id, link_id)
                return

            # 5. Route Payload: Movie Torrent Batch
            if payload.startswith("batch_"):
                movie_id = payload.replace("batch_", "", 1)
                torrents = await db.get_torrents_by_movie(movie_id)
                if not torrents:
                    await message.reply_text("❌ No files found for this batch.")
                    return
                buttons = [
                    [
                        InlineKeyboardButton(
                            f"{t['quality_label']} ({t['size_label']})",
                            callback_data=f"req|{t['file_id']}",
                        )
                    ]
                    for t in torrents
                ]
                await message.reply_text(
                    "📦 <b>Select a quality below to download or stream:</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(buttons),
                )
                return

            # 5b. Route Payload: a file the Torrent Search found in a Database channel (dbf_<file_id>)
            if payload.startswith("dbf_"):
                found = await db.find_completed_file(payload[4:])
                if not found or (found.get("source") == "db_channel" and not await self._verify_stored_db_file(found)):
                    await message.reply_text("❌ That file is no longer in the database. Please search again.")
                    return
                await message.reply_text(await self._found_text(found), parse_mode=ParseMode.HTML)
                if not await self.deliver_completed_file_to_user(user.id, found["file_id"], client=c):
                    await message.reply_text("⚠️ Could not send the file right now. Please tap the button again in a minute.")
                return

            # 6. Route Payload: Individual Torrent File
            file_id = payload.replace("file_", "", 1)
            await self.handle_file_request(c, user.id, file_id, message)

        if not is_clone:
            # Admin direct upload: send me a video / video-document (a split file: send its parts, each named
            # ...part01 / ...part02) -> copied into the storage channel and mapped (name, quality, parts) in the DB.
            @client.on_message(
                filters.private & filters.user(FileStoreConfig.OWNER_ID) & (filters.video | filters.document)
            )
            @guarded
            async def admin_direct_media(c: Client, message: Message):
                found = self._media_of(message)
                if not found:
                    return  # not a video (e.g. a JSON file): not ours
                name = found[1]
                pre = await self.index_admin_media(message, check_only=True, message_id=0)
                if pre["status"] != "new":
                    await message.reply_text(self._index_report(pre, name), parse_mode=ParseMode.HTML)
                    return
                copied = await message.copy(FileStoreConfig.STORAGE_CHANNEL_ID)
                self._own_msg_ids.add(copied.id)
                res = await self.index_admin_media(copied)
                await message.reply_text(self._index_report(res, name), parse_mode=ParseMode.HTML)

            # Files the admin posts straight into the storage channel are mapped the same way.
            @client.on_message(filters.chat(FileStoreConfig.STORAGE_CHANNEL_ID) & (filters.video | filters.document))
            async def storage_channel_media(c: Client, message: Message):
                try:
                    found = self._media_of(message)
                    if not found or not found[1]:
                        return
                    await asyncio.sleep(4)  # let our own uploads / /link copies register themselves first
                    if message.id in self._own_msg_ids or name_key(found[1]) in self._uploading:
                        return
                    res = await self.index_admin_media(message)
                    if res["status"] == "new":
                        logger.info("Indexed admin upload %s (%s, part %s)", found[1], res.get("quality"), res.get("part_number"))
                except Exception as exc:
                    logger.warning("Could not index storage channel message %s: %s", getattr(message, "id", "?"), exc)

            # Database channels of users: dynamic filter, new / edited videos are indexed the moment they arrive.
            def _is_db_channel(_, __, m) -> bool:
                return bool(getattr(m, "chat", None)) and m.chat.id in self._db_chats

            @client.on_message(filters.create(_is_db_channel) & (filters.video | filters.document))
            async def db_channel_media(c: Client, message: Message):
                try:
                    await self.index_db_channel_message(message)
                except Exception as exc:
                    logger.warning("Could not index Database channel message %s: %s", getattr(message, "id", "?"), exc)

            @client.on_edited_message(filters.create(_is_db_channel) & (filters.video | filters.document))
            async def db_channel_media_edited(c: Client, message: Message):
                try:
                    await self.index_db_channel_message(message)
                except Exception as exc:
                    logger.warning("Could not re-index Database channel message %s: %s", getattr(message, "id", "?"), exc)

            @client.on_deleted_messages(filters.create(_is_db_channel))
            async def db_channel_deleted(c: Client, messages):
                for m in messages:
                    try:
                        if m.chat:
                            await db.remove_db_channel_file(m.chat.id, m.id)
                    except Exception:
                        pass

        @client.on_message(filters.command("id") & filters.private)
        @guarded
        async def id_cmd(c: Client, message: Message):
            text = (
                f"🆔 <b>Telegram ID Information</b>\n\n"
                f"👤 <b>Your User ID:</b> <code>{message.from_user.id}</code>\n"
                f"💬 <b>Current Chat ID:</b> <code>{message.chat.id}</code>\n"
            )
            if message.reply_to_message:
                rep = message.reply_to_message
                if rep.forward_from_chat:
                    text += f"📢 <b>Forwarded Channel ID:</b> <code>{rep.forward_from_chat.id}</code>\n"
                if rep.forward_from:
                    text += f"➡️ <b>Forwarded User ID:</b> <code>{rep.forward_from.id}</code>\n"
            await message.reply_text(text, parse_mode=ParseMode.HTML)

        @client.on_message(filters.command("link") & filters.private)
        @guarded
        async def link_cmd(c: Client, message: Message):
            if not message.reply_to_message:
                await message.reply_text(
                    "📎 <b>Usage:</b> Reply <code>/link</code> to any message, video, or document to generate a shareable File Store link.",
                    parse_mode=ParseMode.HTML,
                )
                return
            copied = await message.reply_to_message.copy(FileStoreConfig.STORAGE_CHANNEL_ID)
            self._own_msg_ids.add(copied.id)  # never auto-indexed as an admin file (users can use /link too)
            if len(self._own_msg_ids) > 5000:
                self._own_msg_ids = set(sorted(self._own_msg_ids)[-2500:])
            index_note = ""
            if message.from_user.id == FileStoreConfig.OWNER_ID and (copied.video or copied.document):
                res = await self.index_admin_media(copied)
                if res["status"] == "new":
                    index_note = f"\n🗄 <b>Also mapped in the database</b> ({res.get('quality', 'HD')})"
            link_id = uuid.uuid4().hex[:10]
            await db.save_stored_link(
                {
                    "link_id": link_id,
                    "owner_id": message.from_user.id,
                    "chat_id": FileStoreConfig.STORAGE_CHANNEL_ID,
                    "start_msg_id": copied.id,
                    "end_msg_id": copied.id,
                    "is_batch": False,
                }
            )
            redirect_id = f"st_{link_id}"
            await db.save_redirect_link(
                {
                    "redirect_id": redirect_id,
                    "file_id": f"store_{link_id}",
                    "movie_id": link_id,
                    "is_batch": True,
                    "title": "Stored Telegram File",
                    "quality": "Original",
                    "size_label": "1 File",
                }
            )
            me = await c.get_me()
            bot_link = f"https://t.me/{me.username}?start=store_{link_id}"
            web_link = f"{WebServerConfig.BASE_URL}/r/{redirect_id}"
            settings = await db.get_bot_settings("file_store")
            short_link = await self.shorten_url(bot_link, settings)

            await message.reply_text(
                f"✅ <b>Single File Stored Successfully!</b>\n\n"
                f"🤖 <b>Bot Link:</b> <code>{bot_link}</code>\n"
                f"🌐 <b>Countdown Web Link:</b> <code>{web_link}</code>\n"
                f"🔗 <b>Shortened Link:</b> <code>{short_link}</code>{index_note}",
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )

        @client.on_message(filters.command("batch") & filters.private)
        @guarded
        async def batch_cmd(c: Client, message: Message):
            if len(message.command) >= 3:
                first_link, last_link = message.command[1], message.command[2]
                await self._create_batch_from_links(c, message, first_link, last_link)
                return
            self.user_input_states[message.from_user.id] = {"action": "batch_first"}
            await message.reply_text(
                "📦 <b>Batch Link Generator</b>\n\n"
                "Send the <b>First Message Link</b> from your channel (e.g. <code>https://t.me/c/123456789/10</code>):\n"
                "<i>(Make sure this bot is an admin in that channel)</i>",
                parse_mode=ParseMode.HTML,
            )

        @client.on_message(filters.command("clone") & filters.private)
        @guarded
        async def clone_cmd(c: Client, message: Message):
            if len(message.command) >= 2:
                bot_token = message.command[1].strip()
                await self._handle_clone_token(message, bot_token)
                return
            self.user_input_states[message.from_user.id] = {"action": "clone_token"}
            await message.reply_text(
                "🤖 <b>Create Your Own Identical Clone Bot</b>\n\n"
                "1. Open @BotFather and create a new bot with <code>/newbot</code>.\n"
                "2. Copy the <b>Bot Token</b> and send it here (or run <code>/clone YOUR_BOT_TOKEN</code>).",
                parse_mode=ParseMode.HTML,
            )

        @client.on_message(filters.command("activate") & filters.private)
        @guarded
        async def activate_cmd(c: Client, message: Message):
            uid = message.from_user.id
            clone_doc = await db.get_clone_bot(uid)
            if not clone_doc:
                await message.reply_text("❌ You do not have a clone bot yet. Use /clone to create one.")
                return
            status_msg = await message.reply_text("🔄 Activating your Clone Bot...")
            try:
                saved = await self.spawn_clone_client(uid, clone_doc["bot_token"])
                await status_msg.edit_text(
                    f"✅ <b>Clone Bot Activated!</b>\n\n🤖 Username: @{saved['bot_username']}",
                    parse_mode=ParseMode.HTML,
                )
            except Exception as exc:
                await status_msg.edit_text(f"❌ Failed to activate clone bot: {exc}")

        @client.on_message(filters.command("delete") & filters.private)
        @guarded
        async def delete_cmd(c: Client, message: Message):
            uid = message.from_user.id
            if uid in self.clone_clients:
                try:
                    await self.clone_clients[uid].stop()
                except Exception:
                    pass
                self.clone_clients.pop(uid, None)
            deleted = await db.delete_clone_bot(uid)
            if deleted:
                await message.reply_text("🗑 <b>Your Clone Bot has been stopped and deleted.</b>", parse_mode=ParseMode.HTML)
            else:
                await message.reply_text("❌ No existing Clone Bot found for your account.")

        @client.on_message(filters.command("settings") & filters.private)
        @guarded
        async def settings_cmd(c: Client, message: Message):
            kb = await self.build_settings_menu_keyboard(message.from_user.id)
            await message.reply_text(
                "<b>HERE IS THE SETTINGS MENU\n\nCUSTOMIZE YOUR SETTINGS AS PER YOUR NEED</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
            )

        @client.on_message(filters.command("broadcast") & filters.private & filters.user(FileStoreConfig.OWNER_ID))
        @guarded
        async def broadcast_cmd(c: Client, message: Message):
            if not message.reply_to_message:
                await message.reply_text("📢 <b>Usage:</b> Reply <code>/broadcast</code> to any message you want to send to all users.", parse_mode=ParseMode.HTML)
                return
            user_ids = await db.get_all_user_ids()
            status_msg = await message.reply_text(f"📢 Broadcasting to <b>{len(user_ids)}</b> users...", parse_mode=ParseMode.HTML)
            sent_count = 0
            failed_count = 0
            for uid in user_ids:
                try:
                    await message.reply_to_message.copy(uid)
                    sent_count += 1
                    await asyncio.sleep(0.05)
                except FloodWait as fw:
                    await asyncio.sleep(fw.value)
                except Exception:
                    failed_count += 1
            await status_msg.edit_text(
                f"✅ <b>Broadcast Complete!</b>\n\n"
                f"• Total Users: {len(user_ids)}\n"
                f"• Delivered: {sent_count}\n"
                f"• Failed/Blocked: {failed_count}",
                parse_mode=ParseMode.HTML,
            )

        @client.on_message(filters.command("addpremium") & filters.private & filters.user(FileStoreConfig.OWNER_ID))
        @guarded
        async def addpremium_cmd(c: Client, message: Message):
            if len(message.command) < 3:
                await message.reply_text("Usage: <code>/addpremium &lt;user_id&gt; &lt;days&gt;</code>", parse_mode=ParseMode.HTML)
                return
            target_uid = int(message.command[1])
            days = int(message.command[2])
            await db.set_user_premium(target_uid, days)
            await message.reply_text(f"✅ Updated premium status for <code>{target_uid}</code> ({days} days).", parse_mode=ParseMode.HTML)

        # --- Interactive `/settings` Menu Callback Handler ---

        @client.on_callback_query(filters.regex(r"^st_menu\|"))
        @guarded
        async def settings_menu_callbacks(c: Client, cq: CallbackQuery):
            uid = cq.from_user.id
            _, action = cq.data.split("|", 1)
            settings = await db.get_bot_settings("file_store")
            is_owner = uid == FileStoreConfig.OWNER_ID or (is_clone and uid == clone_owner_id)
            back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("≼ BACK", callback_data="st_menu|main")]])

            if action == "main":
                kb = await self.build_settings_menu_keyboard(uid)
                await cq.message.edit_text(
                    "<b>HERE IS THE SETTINGS MENU\n\nCUSTOMIZE YOUR SETTINGS AS PER YOUR NEED</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb,
                )
                return

            # Leaving the clone screen (BACK / settings / CLOSE) must also cancel the "send me your bot token" wait,
            # otherwise the next normal message of the user would be read as a token.
            if action in ("back", "main", "close"):
                pending = self.user_input_states.get(uid)
                if pending and pending.get("action") == "clone_token":
                    self.user_input_states.pop(uid, None)

            if action == "close":
                try:
                    await cq.message.delete()
                except Exception:
                    await cq.answer("Closed.")
                return

            if action == "back":
                text, kb = await self.build_start(cq.from_user)
                await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
                return

            home_kb = InlineKeyboardMarkup([[InlineKeyboardButton("≼ BACK", callback_data="st_menu|back")]])

            if action == "myid":
                await cq.message.edit_text(
                    f"🆔 <b>Your User ID:</b> <code>{uid}</code>\n💬 <b>Chat ID:</b> <code>{cq.message.chat.id}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=home_kb,
                )
                return

            if action == "help":
                await cq.message.edit_text(
                    "❓ <b>HELP</b>\n\n"
                    "• Tap a file link from a channel post - the file is sent to you.\n"
                    "• Already stored files are sent instantly; new ones are downloaded automatically.\n"
                    "• Big files may come in <b>parts</b> - all parts are sent in order.\n"
                    "• /settings - customize the bot · /link · /batch · /clone",
                    parse_mode=ParseMode.HTML,
                    reply_markup=home_kb,
                )
                return

            if action in ("clone", "clone_s"):
                # BACK returns to the screen the user came from: START menu (CLONE button) or SETTINGS (MY CLONE BOT).
                back_to = "st_menu|back" if action == "clone" else "st_menu|main"
                clone_back_kb = [InlineKeyboardButton("≼ BACK", callback_data=back_to)]
                clone_doc = await db.get_clone_bot(uid)
                if clone_doc:
                    self.user_input_states.pop(uid, None)
                    text = (
                        "🤖 <b>MY CLONE BOT</b>\n\n"
                        f"• <b>Name:</b> {html.escape(str(clone_doc.get('bot_name') or '-'))}\n"
                        f"• <b>Username:</b> @{clone_doc.get('bot_username')}\n"
                        f"• <b>Status:</b> {'Active ✅' if clone_doc.get('is_active') else 'Stopped ❌'}\n\n"
                        "• /activate - start your clone again\n"
                        "• /delete - stop and delete your clone"
                    )
                    kb = InlineKeyboardMarkup(
                        [
                            [InlineKeyboardButton("🚀 Open Clone Bot", url=f"https://t.me/{clone_doc['bot_username']}")],
                            clone_back_kb,
                        ]
                    )
                else:
                    # Wait for the token right here: the user can simply paste it, no command needed.
                    self.user_input_states[uid] = {"action": "clone_token"}
                    text = (
                        "🤖 <b>CREATE YOUR OWN CLONE BOT</b>\n\n"
                        "Get a bot that works exactly like this one and shares the same file database.\n\n"
                        "<b>1.</b> Open @BotFather and send <code>/newbot</code>\n"
                        "<b>2.</b> Copy the <b>bot token</b> it gives you\n"
                        "<b>3.</b> <b>Paste the token here</b> in this chat\n\n"
                        "<i>Or use</i> <code>/clone YOUR_BOT_TOKEN</code>"
                    )
                    kb = InlineKeyboardMarkup([clone_back_kb])
                await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
                return

            if action == "premium":
                is_prem = await db.is_user_premium(uid)
                status_line = "✅ <b>Your Status: PREMIUM ACTIVE</b>\n\n" if is_prem else "ℹ️ <b>Your Status: Free Plan</b>\n\n"
                await cq.message.edit_text(
                    status_line + FileStoreConfig.PREMIUM_PLAN_TEXT,
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("≼ BACK", callback_data="st_menu|back")]]),
                )
                return

            # Admin/Owner-protected settings below
            if not is_owner:
                await cq.answer("🔒 Only the bot owner can modify global File Store settings.", show_alert=True)
                return

            if action == "shortener":
                enabled = settings.get("shortener_enabled", False)
                domain = settings.get("shortener_domain") or "Not Set"
                api_key = settings.get("shortener_api_key") or "Not Set"
                kb = InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton(f"Shortener: {'ON ✅' if enabled else 'OFF ❌'}", callback_data="st_menu|toggle_shortener")],
                        [InlineKeyboardButton("✏️ Set Domain & API Key", callback_data="st_menu|set_shortener")],
                        [InlineKeyboardButton("≼ BACK", callback_data="st_menu|main")],
                    ]
                )
                await cq.message.edit_text(
                    f"📎 <b>LINK SHORTNER SETTINGS</b>\n\n"
                    f"• <b>Status:</b> {'Enabled ✅' if enabled else 'Disabled ❌'}\n"
                    f"• <b>Domain:</b> <code>{domain}</code>\n"
                    f"• <b>API Key:</b> <code>{api_key}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb,
                )
                return

            if action == "toggle_shortener":
                new_val = not settings.get("shortener_enabled", False)
                await db.update_bot_settings("file_store", {"shortener_enabled": new_val})
                cq.data = "st_menu|shortener"
                await settings_menu_callbacks(c, cq)
                return

            if action == "set_shortener":
                self.user_input_states[uid] = {"action": "set_shortener"}
                await cq.message.edit_text(
                    "📎 <b>Send your Shortener Domain and API Key separated by a space:</b>\n\n"
                    "Example:\n<code>gplinks.in 839201abcdef123456</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=back_kb,
                )
                return

            if action == "token_verify":
                enabled = settings.get("token_verify_enabled", False)
                exp = settings.get("token_verify_expire_seconds", 86400)
                kb = InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton(f"Token Verification: {'ON ✅' if enabled else 'OFF ❌'}", callback_data="st_menu|toggle_token_verify")],
                        [InlineKeyboardButton("⏱ Set Expiry Duration (Seconds)", callback_data="st_menu|set_token_exp")],
                        [InlineKeyboardButton("≼ BACK", callback_data="st_menu|main")],
                    ]
                )
                await cq.message.edit_text(
                    f"⏰ <b>TOKEN VERIFICATION SETTINGS</b>\n\n"
                    f"• <b>Status:</b> {'Enabled ✅' if enabled else 'Disabled ❌'}\n"
                    f"• <b>Token Validity:</b> <code>{exp} seconds ({exp // 3600}h)</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb,
                )
                return

            if action == "toggle_token_verify":
                new_val = not settings.get("token_verify_enabled", False)
                await db.update_bot_settings("file_store", {"token_verify_enabled": new_val})
                cq.data = "st_menu|token_verify"
                await settings_menu_callbacks(c, cq)
                return

            if action == "set_token_exp":
                self.user_input_states[uid] = {"action": "set_token_exp"}
                await cq.message.edit_text(
                    "⏰ <b>Send token validity duration in seconds</b> (e.g. <code>86400</code> for 24 hours):",
                    parse_mode=ParseMode.HTML,
                    reply_markup=back_kb,
                )
                return

            if action == "caption":
                cap = settings.get("custom_caption", FileStoreConfig.CUSTOM_CAPTION)
                kb = InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("✏️ Edit Custom Caption", callback_data="st_menu|set_caption")],
                        [InlineKeyboardButton("🔄 Reset Default", callback_data="st_menu|reset_caption")],
                        [InlineKeyboardButton("≼ BACK", callback_data="st_menu|main")],
                    ]
                )
                await cq.message.edit_text(
                    f"🍿 <b>CUSTOM CAPTION</b>\n\n"
                    f"<b>Current Template:</b>\n{cap}\n\n"
                    f"<i>Placeholders:</i> <code>{{filename}}</code>, <code>{{size}}</code>, <code>{{quality}}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb,
                )
                return

            if action == "set_caption":
                self.user_input_states[uid] = {"action": "set_caption"}
                await cq.message.edit_text(
                    "🍿 <b>Send your new Custom Caption template:</b>\n"
                    "Available placeholders: <code>{filename}</code>, <code>{size}</code>, <code>{quality}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=back_kb,
                )
                return

            if action == "reset_caption":
                await db.update_bot_settings("file_store", {"custom_caption": FileStoreConfig.CUSTOM_CAPTION})
                cq.data = "st_menu|caption"
                await settings_menu_callbacks(c, cq)
                return

            if action == "fsub":
                fsub = settings.get("force_sub_channel") or "Disabled"
                kb = InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("✏️ Set Force Sub Channel", callback_data="st_menu|set_fsub")],
                        [InlineKeyboardButton("🗑 Clear Force Sub", callback_data="st_menu|clear_fsub")],
                        [InlineKeyboardButton("≼ BACK", callback_data="st_menu|main")],
                    ]
                )
                await cq.message.edit_text(
                    f"📢 <b>CUSTOM FORCE SUBSCRIBE</b>\n\n"
                    f"• <b>Current Channel:</b> <code>{fsub}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb,
                )
                return

            if action == "set_fsub":
                self.user_input_states[uid] = {"action": "set_fsub"}
                await cq.message.edit_text(
                    "📢 <b>Send the Channel Username (e.g. <code>@Ar_kinghd</code>) or Channel ID (e.g. <code>-100123456789</code>):</b>\n"
                    "<i>Make sure the bot is an admin in that channel!</i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=back_kb,
                )
                return

            if action == "clear_fsub":
                await db.update_bot_settings("file_store", {"force_sub_channel": ""})
                cq.data = "st_menu|fsub"
                await settings_menu_callbacks(c, cq)
                return

            if action == "button":
                b_txt = settings.get("custom_button_text") or "None"
                b_url = settings.get("custom_button_url") or "None"
                kb = InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("✏️ Set Custom Button", callback_data="st_menu|set_button")],
                        [InlineKeyboardButton("🗑 Remove Button", callback_data="st_menu|clear_button")],
                        [InlineKeyboardButton("≼ BACK", callback_data="st_menu|main")],
                    ]
                )
                await cq.message.edit_text(
                    f"🔘 <b>CUSTOM BUTTON SETTINGS</b>\n\n"
                    f"• <b>Button Text:</b> {b_txt}\n"
                    f"• <b>Button URL:</b> <code>{b_url}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb,
                )
                return

            if action == "set_button":
                self.user_input_states[uid] = {"action": "set_button"}
                await cq.message.edit_text(
                    "🔘 <b>Send your Custom Button in format:</b>\n"
                    "<code>Button Text | https://t.me/Ar_kinghd</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=back_kb,
                )
                return

            if action == "clear_button":
                await db.update_bot_settings("file_store", {"custom_button_text": "", "custom_button_url": ""})
                cq.data = "st_menu|button"
                await settings_menu_callbacks(c, cq)
                return

            if action == "autodelete":
                enabled = settings.get("auto_delete_enabled", False)
                secs = settings.get("auto_delete_seconds", 600)
                kb = InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton(f"Auto Delete: {'ON ✅' if enabled else 'OFF ❌'}", callback_data="st_menu|toggle_autodelete")],
                        [
                            InlineKeyboardButton("5m", callback_data="st_menu|ad_time_300"),
                            InlineKeyboardButton("10m", callback_data="st_menu|ad_time_600"),
                            InlineKeyboardButton("30m", callback_data="st_menu|ad_time_1800"),
                        ],
                        [InlineKeyboardButton("≼ BACK", callback_data="st_menu|main")],
                    ]
                )
                await cq.message.edit_text(
                    f"♻️ <b>AUTO DELETE SETTINGS</b>\n\n"
                    f"• <b>Status:</b> {'Enabled ✅' if enabled else 'Disabled ❌'}\n"
                    f"• <b>Delete Timer:</b> <code>{secs} seconds ({secs // 60} mins)</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb,
                )
                return

            if action == "toggle_autodelete":
                new_val = not settings.get("auto_delete_enabled", False)
                await db.update_bot_settings("file_store", {"auto_delete_enabled": new_val})
                cq.data = "st_menu|autodelete"
                await settings_menu_callbacks(c, cq)
                return

            if action.startswith("ad_time_"):
                secs = int(action.replace("ad_time_", ""))
                await db.update_bot_settings("file_store", {"auto_delete_seconds": secs})
                cq.data = "st_menu|autodelete"
                await settings_menu_callbacks(c, cq)
                return

            if action == "protect":
                prot = settings.get("protect_content", False)
                kb = InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton(f"Protect Content: {'LOCKED 🔒' if prot else 'UNLOCKED 🔓'}", callback_data="st_menu|toggle_protect")],
                        [InlineKeyboardButton("≼ BACK", callback_data="st_menu|main")],
                    ]
                )
                await cq.message.edit_text(
                    f"🔒 <b>PROTECT CONTENT SETTINGS</b>\n\n"
                    f"• <b>Status:</b> {'Enabled (Forwarding/Saving Blocked) 🔒' if prot else 'Disabled (Users can forward/save) 🔓'}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb,
                )
                return

            if action == "toggle_protect":
                new_val = not settings.get("protect_content", False)
                await db.update_bot_settings("file_store", {"protect_content": new_val})
                cq.data = "st_menu|protect"
                await settings_menu_callbacks(c, cq)
                return

        @client.on_callback_query(filters.regex(r"^req\|"))
        @guarded
        async def req_callback(c: Client, cq: CallbackQuery):
            _, file_id = cq.data.split("|", 1)
            await cq.answer("Checking database...")
            await self.handle_file_request(c, cq.from_user.id, file_id, cq.message)

        @client.on_message(filters.text & filters.private)
        @guarded
        async def handle_interactive_text(c: Client, message: Message):
            uid = message.from_user.id
            if uid not in self.user_input_states:
                return
            state = self.user_input_states.pop(uid)
            act = state["action"]
            text = message.text.strip()

            if act == "clone_token":
                await self._handle_clone_token(message, text, retry=True)
                return

            if act == "batch_first":
                parsed = parse_tg_message_link(text)
                if not parsed:
                    await message.reply_text("❌ Invalid Telegram message link. Please send a valid link (e.g. https://t.me/c/123456/10).")
                    return
                self.user_input_states[uid] = {"action": "batch_last", "first_link": text}
                await message.reply_text("✅ Now send the <b>Last Message Link</b> from the same channel:", parse_mode=ParseMode.HTML)
                return

            if act == "batch_last":
                await self._create_batch_from_links(c, message, state["first_link"], text)
                return

            if act == "set_shortener":
                parts = text.split()
                if len(parts) < 2:
                    await message.reply_text("❌ Please provide both Domain and API key separated by a space.")
                    return
                await db.update_bot_settings("file_store", {"shortener_domain": parts[0], "shortener_api_key": parts[1], "shortener_enabled": True})
                await message.reply_text("✅ <b>Link Shortener updated and enabled!</b> Use /settings to view.", parse_mode=ParseMode.HTML)
                return

            if act == "set_token_exp":
                if not text.isdigit():
                    await message.reply_text("❌ Please send a valid number of seconds.")
                    return
                await db.update_bot_settings("file_store", {"token_verify_expire_seconds": int(text)})
                await message.reply_text(f"✅ Token verification expiry set to <b>{text}s</b>.", parse_mode=ParseMode.HTML)
                return

            if act == "set_caption":
                await db.update_bot_settings("file_store", {"custom_caption": text})
                await message.reply_text("✅ <b>Custom Caption updated!</b>", parse_mode=ParseMode.HTML)
                return

            if act == "set_fsub":
                await db.update_bot_settings("file_store", {"force_sub_channel": text})
                await message.reply_text(f"✅ <b>Force Subscribe channel set to:</b> <code>{text}</code>", parse_mode=ParseMode.HTML)
                return

            if act == "set_button":
                if "|" not in text:
                    await message.reply_text("❌ Invalid format. Use: <code>Button Text | https://t.me/channel</code>", parse_mode=ParseMode.HTML)
                    return
                b_text, b_url = [p.strip() for p in text.split("|", 1)]
                await db.update_bot_settings("file_store", {"custom_button_text": b_text, "custom_button_url": b_url})
                await message.reply_text("✅ <b>Custom Button saved!</b>", parse_mode=ParseMode.HTML)

    async def _handle_clone_token(self, message: Message, bot_token: str, retry: bool = False) -> None:
        if not re.match(r"^\d+:[A-Za-z0-9_-]{30,}$", bot_token):
            if retry:  # came from the CLONE button: keep waiting so the user can just paste the right token
                self.user_input_states[message.from_user.id] = {"action": "clone_token"}
            await message.reply_text(
                "❌ That is not a valid BotFather token.\n"
                "It looks like <code>123456789:AAH...</code> - copy it from @BotFather and send it again.",
                parse_mode=ParseMode.HTML,
            )
            return
        status_msg = await message.reply_text("⚙️ <b>Validating token & spawning your identical Clone Bot...</b>", parse_mode=ParseMode.HTML)
        try:
            saved = await self.spawn_clone_client(message.from_user.id, bot_token)
            kb = InlineKeyboardMarkup(
                [[InlineKeyboardButton(f"🤖 Open @{saved['bot_username']}", url=f"https://t.me/{saved['bot_username']}")]]
            )
            await status_msg.edit_text(
                f"✅ <b>Clone Bot Created & Activated!</b>\n\n"
                f"• <b>Bot Name:</b> {saved['bot_name']}\n"
                f"• <b>Username:</b> @{saved['bot_username']}\n\n"
                f"Your clone bot is now live and shares the File Store database!",
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
            )
        except Exception as exc:
            await status_msg.edit_text(f"❌ Failed to start Clone Bot: <code>{exc}</code>", parse_mode=ParseMode.HTML)

    async def _create_batch_from_links(self, client: Client, message: Message, first_link: str, last_link: str) -> None:
        p1 = parse_tg_message_link(first_link)
        p2 = parse_tg_message_link(last_link)
        if not p1 or not p2 or p1[0] != p2[0]:
            await message.reply_text("❌ Both links must be valid Telegram message links from the exact same channel.")
            return
        chat_id, start_id = p1
        _, end_id = p2
        if start_id > end_id:
            start_id, end_id = end_id, start_id

        link_id = uuid.uuid4().hex[:10]
        await db.save_stored_link(
            {
                "link_id": link_id,
                "owner_id": message.from_user.id,
                "chat_id": chat_id,
                "start_msg_id": start_id,
                "end_msg_id": end_id,
                "is_batch": True,
            }
        )
        redirect_id = f"st_{link_id}"
        count = (end_id - start_id) + 1
        await db.save_redirect_link(
            {
                "redirect_id": redirect_id,
                "file_id": f"store_{link_id}",
                "movie_id": link_id,
                "is_batch": True,
                "title": f"Batch Bundle ({count} Files)",
                "quality": "Multi",
                "size_label": f"{count} Messages",
            }
        )
        me = await client.get_me()
        bot_link = f"https://t.me/{me.username}?start=store_{link_id}"
        web_link = f"{WebServerConfig.BASE_URL}/r/{redirect_id}"
        settings = await db.get_bot_settings("file_store")
        short_link = await self.shorten_url(bot_link, settings)

        await message.reply_text(
            f"✅ <b>Batch Stored Successfully ({count} messages)!</b>\n\n"
            f"🤖 <b>Bot Batch Link:</b> <code>{bot_link}</code>\n"
            f"🌐 <b>Countdown Web Link:</b> <code>{web_link}</code>\n"
            f"🔗 <b>Shortened Link:</b> <code>{short_link}</code>",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )


file_store_bot = FileStoreBot()
