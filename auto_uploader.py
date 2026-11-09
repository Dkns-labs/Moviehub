import re
import json
import uuid
import html
import hashlib
import asyncio
import logging
from datetime import timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import aiohttp
from yarl import URL
from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand,
    BotCommandScopeChat,
)
from pyrogram.enums import ParseMode, ChatMemberStatus
from pyrogram.errors import FloodWait, MessageNotModified, PeerIdInvalid, ChannelInvalid, ChannelPrivate

from config import AutoUploaderConfig, WebServerConfig, FileStoreConfig, TelegramCommonConfig
from database import db, utcnow
import db_channel
import session_store
from base_url import is_public_url
from admin_panel import attach_admin_panel, format_ban_notice, fmt_dt, fmt_left
from torrent_engine import (
    torrent_engine,
    fetch_dkbotz,
    extract_infohash_from_magnet,
    extract_dn_from_magnet,
    extract_size_from_magnet,
    get_origin,
    resolve_url,
)

logger = logging.getLogger("auto_uploader")


def guarded(fn):
    """Handler wrapper: log the real error and ALWAYS give the user feedback (never a silent failure)."""
    import functools

    @functools.wraps(fn)
    async def wrapper(client, update, *a, **kw):
        is_cb = isinstance(update, CallbackQuery)
        try:
            # The bot answers from the first second; if MongoDB is still connecting, wait for it (no error, no lost click)
            if not db.is_ready and not await db.wait_ready(25):
                msg = "⏳ The bot is starting up. Please try again in a few seconds."
                if is_cb:
                    await update.answer(msg, show_alert=True)
                else:
                    await update.reply_text(msg)
                return
            # Banned users (set from /admin) get nothing but the ban notice - in both bots and all clones.
            uid0 = getattr(getattr(update, "from_user", None), "id", None)
            if uid0 and uid0 != TelegramCommonConfig.OWNER_ID:
                try:
                    ban = await db.get_ban(uid0)
                except Exception as exc:  # DB hiccup must not lock everybody out
                    logger.warning("Ban check failed for %s: %s", uid0, exc)
                    ban = None
                if ban:
                    if is_cb:
                        until = ban.get("until")
                        await update.answer(
                            f"🚫 You are banned ({'until ' + fmt_dt(until) if until else 'permanent'}).", show_alert=True
                        )
                    else:
                        await update.reply_text(format_ban_notice(ban), parse_mode=ParseMode.HTML)
                    return
            return await fn(client, update, *a, **kw)
        except MessageNotModified:
            pass
        except FloodWait as fw:
            await asyncio.sleep(int(getattr(fw, "value", 3) or 3))
        except Exception as exc:
            uid = getattr(getattr(update, "from_user", None), "id", "?")
            logger.exception("Handler %s failed for user %s: %s", fn.__name__, uid, exc)
            try:
                if is_cb:
                    await update.answer("⚠️ Something went wrong. Please try again.", show_alert=True)
                else:
                    await update.reply_text("⚠️ Something went wrong. Please try again.")
            except Exception:
                pass
        finally:
            if is_cb:
                try:
                    await update.answer()  # stop the button spinner on every path
                except Exception:
                    pass

    return wrapper


async def reply_with_buttons(message: Message, text: str, kb: InlineKeyboardMarkup) -> None:
    """Send a menu so the BUTTONS always arrive: if the HTML is rejected, resend as plain text WITH the same buttons."""
    try:
        await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
    except FloodWait:
        raise
    except Exception as exc:
        logger.warning("Menu with HTML failed (%s); resending as plain text with buttons.", exc)
        plain = html.unescape(re.sub(r"<[^>]+>", "", text))
        await message.reply_text(plain, reply_markup=kb)


def human_readable_size(num_bytes: int) -> str:
    if num_bytes <= 0:
        return "Unknown"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num_bytes < 1024.0:
            return f"{num_bytes:.2f} {unit}" if unit in ("GB", "TB") else f"{int(num_bytes)} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.2f} PB"


def human_speed(bytes_per_sec: float) -> str:
    return f"{human_readable_size(int(bytes_per_sec))}/s" if bytes_per_sec and bytes_per_sec > 0 else "0 B/s"


def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def progress_bar(pct: float, width: int = 12) -> str:
    pct = max(0.0, min(100.0, float(pct or 0)))
    filled = int(round(width * pct / 100))
    return "█" * filled + "░" * (width - filled)


def session_name(prefix: str, token: str) -> str:
    """Session file name tied to the bot token, so changing a token never reuses another bot's session."""
    return f"{prefix}_{hashlib.sha1((token or '').encode()).hexdigest()[:8]}"


def effective_log_chat() -> int:
    """Channel that receives every .torrent file: LOG_CHANNEL_ID, else the storage channel.
    The storage channel is only used if it is NOT the public channel (never leak .torrent files publicly)."""
    if AutoUploaderConfig.LOG_CHANNEL_ID:
        return AutoUploaderConfig.LOG_CHANNEL_ID
    storage = FileStoreConfig.STORAGE_CHANNEL_ID
    if storage and storage != AutoUploaderConfig.PUBLIC_CHANNEL_ID:
        return storage
    return 0


# Values from the environment; the admin can override both from Advanced Settings (stored in the DB).
_DEFAULT_PUBLIC_CHANNEL = AutoUploaderConfig.PUBLIC_CHANNEL_ID
_DEFAULT_LOG_CHANNEL = AutoUploaderConfig.LOG_CHANNEL_ID


# --------------------------------------------------------------------------- #
# "Peer not found" handling
#
# Pyrogram can only talk to a channel / user whose access_hash it already knows. A bot learns a channel
# when it is a member/admin and calls get_chat(); it learns a user when that user messaged it.
# --------------------------------------------------------------------------- #

def is_peer_error(exc: BaseException) -> bool:
    if isinstance(exc, (PeerIdInvalid, ChannelInvalid, ChannelPrivate)):
        return True
    if isinstance(exc, (KeyError, ValueError)):
        text = str(exc).lower()
        return "peer" in text or "id not found" in text
    return False


async def ensure_chat(client: Optional[Client], chat_id: Any, label: str = "chat") -> bool:
    """Resolve `chat_id` so Pyrogram knows it. Logs the exact fix when the bot cannot see the chat."""
    if not client or not chat_id:
        return False
    for _ in range(3):
        try:
            await client.get_chat(chat_id)
            return True
        except FloodWait as fw:
            await asyncio.sleep(int(getattr(fw, "value", 3) or 3) + 1)
        except Exception as exc:
            logger.error(
                "Cannot reach %s %s (%s: %s). FIX: add this bot as ADMIN with post rights, then post any message in that "
                "channel once so the bot sees it (the session is saved to MongoDB, so it is remembered after restarts).",
                label, chat_id, type(exc).__name__, exc,
            )
            return False
    return False


async def call_with_peer_retry(client: Optional[Client], chat_id: Any, factory, label: str = "chat"):
    """await factory(); if Telegram says the peer is unknown, resolve it once and retry."""
    try:
        return await factory()
    except Exception as exc:
        if not is_peer_error(exc):
            raise
        logger.warning("Peer %s not resolved yet (%s: %s) - resolving and retrying once", chat_id, type(exc).__name__, exc)
        if not await ensure_chat(client, chat_id, label):
            raise
        return await factory()


def parse_movie_metadata(
    raw_title: str, releases: List[Dict[str, Any]], uploader: str, source_url: Optional[str] = None
) -> Dict[str, Any]:
    cleaned = re.sub(r"www\.[a-zA-Z0-9.\-]+\s*-\s*", "", raw_title).strip()
    year_match = re.search(r"\((\d{4})\)", cleaned)
    year = year_match.group(1) if year_match else "N/A"

    if year_match:
        title_part = cleaned[: year_match.start()].strip(" -–:")
    else:
        title_part = cleaned.split("-")[0].strip()

    combined_text = cleaned + " " + " ".join(r.get("title", "") for r in releases)
    lang_map = {
        "Tamil": r"\b(Tamil|Tam)\b",
        "Telugu": r"\b(Telugu|Tel)\b",
        "Hindi": r"\b(Hindi|Hin)\b",
        "Malayalam": r"\b(Malayalam|Mal)\b",
        "Kannada": r"\b(Kannada|Kan)\b",
        "Korean": r"\b(Korean|Kor)\b",
        "English": r"\b(English|Eng)\b",
    }
    detected_langs = [lang for lang, pattern in lang_map.items() if re.search(pattern, combined_text, re.I)]
    if not detected_langs:
        audio = "Tamil"
    elif len(detected_langs) == 1:
        audio = detected_langs[0]
    elif len(detected_langs) > 2:
        audio = f"{detected_langs[0]} + Multi"
    else:
        audio = " + ".join(detected_langs)

    quality_patterns = [
        r"TRUE WEB-DL", r"WEB-DL", r"WEBRip", r"WEB-HD",
        r"BluRay", r"BR-Rip", r"BDRip", r"HQ HDRip", r"HDRip", r"HDTV",
    ]
    quality = "HD"
    for qp in quality_patterns:
        m = re.search(qp, combined_text, re.I)
        if m:
            quality = m.group(0).upper()
            break

    parsed_releases = []
    for rel in releases:
        orig_title = rel.get("title", "Untitled Release").strip()
        clean_rel_name = re.sub(r"^www\.[a-zA-Z0-9.\-]+\s*-\s*", "", orig_title, flags=re.I)
        clean_rel_name = re.sub(r"\.torrent$", "", clean_rel_name, flags=re.I)
        clean_rel_name = re.sub(r"\.mkv$", "", clean_rel_name, flags=re.I)
        display_name = f"{uploader} - {clean_rel_name}"

        size_match = re.search(r"(\d+(?:\.\d+)?\s*(?:GB|MB))", orig_title, re.I)
        res_match = re.search(r"(4K|2160p|1080p|720p|480p|360p|240p|x264|x265|HEVC)", orig_title, re.I)

        magnet_list = rel.get("magnet") or []
        torrent_list = rel.get("torrent") or []
        direct_list = rel.get("direct") or []

        magnet_uri = magnet_list[0] if magnet_list else None
        # Every file has its own base url: resolve against the page it came from, keep its own host.
        torrent_url = resolve_url(torrent_list[0], source_url) if torrent_list else None
        direct_url = resolve_url(direct_list[0], source_url) if direct_list else None

        infohash = extract_infohash_from_magnet(magnet_uri) if magnet_uri else None
        original_filename = extract_dn_from_magnet(magnet_uri) if magnet_uri else None
        if not original_filename:
            original_filename = re.sub(r"\.torrent$", "", orig_title, flags=re.I)

        byte_size = extract_size_from_magnet(magnet_uri) if magnet_uri else 0
        size_label = size_match.group(1).upper() if size_match else human_readable_size(byte_size)
        res_label = res_match.group(1) if res_match else quality

        parsed_releases.append(
            {
                "index": rel.get("index", len(parsed_releases) + 1),
                "original_filename": original_filename,
                "display_name": display_name,
                "torrent_url": torrent_url,
                "base_url": get_origin(torrent_url) or get_origin(direct_url) or rel.get("base_url") or get_origin(source_url),
                "magnet_uri": magnet_uri,
                "direct_url": direct_url,
                "infohash": infohash,
                "size_bytes": byte_size,
                "size_label": size_label,
                "quality_label": res_label,
            }
        )

    return {
        "title": title_part,
        "year": year,
        "audio": audio,
        "quality": quality,
        "uploader": uploader,
        "releases": parsed_releases,
    }


class AutoUploaderBot:
    def __init__(self):
        self.app: Optional[Client] = None
        self.pending_edits: Dict[int, Dict[str, Any]] = {}
        self.user_states: Dict[int, Dict[str, Any]] = {}  # user_id -> pending settings input
        self._scheduler_task: Optional[asyncio.Task] = None
        self._movie_locks: Dict[str, asyncio.Lock] = {}
        self._post_lock = asyncio.Lock()
        self._user_post_lock = asyncio.Lock()
        self._bg_tasks: set = set()
        self.admin_states: Dict[int, Dict[str, Any]] = {}
        self.bot_username: str = ""
        self._post_start_task: Optional[asyncio.Task] = None
        self._scraping: set = set()  # user ids with a ▶️ START SCRAPING run in progress

    async def _load_channel_overrides(self) -> None:
        """Public / log channel the admin chose in Advanced Settings win over the environment values."""
        try:
            glob = await db.get_bot_settings("auto_uploader")
            if glob.get("public_channel_id"):
                AutoUploaderConfig.PUBLIC_CHANNEL_ID = int(glob["public_channel_id"])
            if glob.get("log_channel_id"):
                AutoUploaderConfig.LOG_CHANNEL_ID = int(glob["log_channel_id"])
        except Exception as exc:
            logger.warning("Could not load channel overrides: %s", exc)

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    async def start(self) -> None:
        """Connect to Telegram and register handlers - returns as soon as the bot can answer.
        Everything that needs the DB or is only housekeeping (channel check, command menu) runs in _post_start."""
        if AutoUploaderConfig.BOT_TOKEN:
            TelegramCommonConfig.SESSION_DIR.mkdir(parents=True, exist_ok=True)
            _sess = session_name("auto_uploader_bot", AutoUploaderConfig.BOT_TOKEN)
            await session_store.restore(_sess)  # temporary-disk hosts: get the remembered channels back
            self.app = Client(
                _sess,
                api_id=AutoUploaderConfig.API_ID,
                api_hash=AutoUploaderConfig.API_HASH,
                bot_token=AutoUploaderConfig.BOT_TOKEN,
                workdir=str(TelegramCommonConfig.SESSION_DIR),
            )
            self._register_handlers()
            await self.app.start()
            session_store.start_backup(_sess)
            me = getattr(self.app, "me", None)  # set by Client.start(): no extra get_me round trip
            self.bot_username = getattr(me, "username", "") or ""
            logger.info("Auto Uploader Bot started (@%s).", self.bot_username)
        else:
            logger.warning("AUTO_UPLOADER_BOT_TOKEN not set; running Auto Uploader in headless queue mode.")

        self._post_start_task = asyncio.create_task(self._post_start())
        self._scheduler_task = asyncio.create_task(self._post_scheduler_loop())

    async def wait_post_start(self) -> None:
        if self._post_start_task:
            await self._post_start_task

    async def _post_start(self) -> None:
        """Background setup: channel overrides, channel resolving, command menus (all independent -> concurrent)."""
        try:
            await db.wait_ready(60)
            await self._load_channel_overrides()
            if not self.app:
                return

            async def commands() -> None:
                # Everyone sees /start and /settings; the admin additionally sees /ingest /scrape /admin
                try:
                    await self.app.set_bot_commands(
                        [
                            BotCommand("start", "CHECK AUTO UPLOADER STATUS."),
                            BotCommand("settings", "CUSTOMIZE YOUR UPLOADER SETTINGS."),
                            BotCommand("clear", "RESET MY SETTINGS AND START FROM SCRATCH."),
                        ]
                    )
                    await self.app.set_bot_commands(
                        [
                            BotCommand("start", "CHECK AUTO UPLOADER STATUS."),
                            BotCommand("settings", "CUSTOMIZE YOUR UPLOADER SETTINGS."),
                            BotCommand("ingest", "INGEST JSON POSTS INTO 5M QUEUE."),
                            BotCommand("clear", "RESET MY SETTINGS AND START FROM SCRATCH."),
                            BotCommand("scrape", "RUN LIVE 1TAMILMV SCRAPER."),
                            BotCommand("admin", "ADMIN PANEL: BAN / PAID USERS."),
                        ],
                        scope=BotCommandScopeChat(chat_id=AutoUploaderConfig.OWNER_ID),
                    )
                except Exception as exc:
                    logger.warning("Could not set bot commands: %s", exc)

            tasks = [commands(), ensure_chat(self.app, AutoUploaderConfig.PUBLIC_CHANNEL_ID, "PUBLIC_CHANNEL_ID")]
            log_chat = effective_log_chat()
            if log_chat:
                tasks.append(ensure_chat(self.app, log_chat, "log channel"))
            else:
                logger.warning(
                    "No log channel: .torrent files are NOT stored in Telegram. Set LOG_CHANNEL_ID to a private "
                    "channel (or make STORAGE_CHANNEL_ID a different channel than PUBLIC_CHANNEL_ID)."
                )
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Auto Uploader background setup failed: %s", exc)

    async def stop(self) -> None:
        if self._scheduler_task:
            self._scheduler_task.cancel()
        if self.app:
            await self.app.stop()

    _IMG_EXTS = (".jpg", ".jpeg", ".png", ".webp")

    @staticmethod
    def _sniff_image(data: bytes) -> bool:
        """True when the bytes really are an image, whatever Content-Type the site claimed (many CDNs say octet-stream)."""
        return (
            data[:3] == b"\xff\xd8\xff" or data[:8] == b"\x89PNG\r\n\x1a\n" or data[:6] in (b"GIF87a", b"GIF89a")
            or (data[:4] == b"RIFF" and data[8:12] == b"WEBP") or data[4:12] in (b"ftypavif", b"ftypheic", b"ftypmif1")
            or data[:2] == b"BM"
        )

    @staticmethod
    def _normalize_poster(path: Path, max_side: int = 2560) -> Optional[Path]:
        """Re-encode to a Telegram-safe JPEG (<=10MB, w+h<=10000, ratio<=20). Returns None when the file is not a
        decodable image at all (so a broken file is never posted); needs Pillow, without it the file is kept as is."""
        try:
            from PIL import Image
        except Exception:
            return path
        try:
            with Image.open(path) as im:
                im.load()
                if im.mode in ("RGBA", "LA", "P"):
                    im = im.convert("RGBA")
                    bg = Image.new("RGB", im.size, (255, 255, 255))  # transparent PNG/WebP -> white, not black
                    bg.paste(im, mask=im.split()[-1])
                    im = bg
                else:
                    im = im.convert("RGB")
                w, h = im.size
                if w < 10 or h < 10 or max(w, h) / max(1, min(w, h)) > 19:
                    return None  # tracking pixel / banner that Telegram would refuse (PHOTO_INVALID_DIMENSIONS)
                if max(w, h) > max_side:
                    im.thumbnail((max_side, max_side))
                out = path.with_suffix(".jpg")
                tmp = out.with_suffix(".tmp")
                im.save(tmp, format="JPEG", quality=90, optimize=True)
            tmp.replace(out)
            if out != path:
                path.unlink(missing_ok=True)
            return out
        except Exception as exc:
            logger.warning("Poster %s is not a usable image: %s", path, exc)
            return None

    def _poster_ok(self, path: Optional[Any]) -> bool:
        try:
            return bool(path) and Path(path).exists() and Path(path).stat().st_size > 0
        except OSError:
            return False

    async def download_poster(
        self, image_url: str, movie_id: str, referer: Optional[str] = None, force: bool = False
    ) -> Optional[Path]:
        """Download + normalise the poster. `referer` = the topic page (hot-link protected CDNs want it),
        `force` = ignore the cached file (used when the poster URL was changed)."""
        image_url = resolve_url((image_url or "").strip(), referer) if referer else (image_url or "").strip()
        if not image_url.startswith(("http://", "https://")):
            return None
        AutoUploaderConfig.POSTER_DIR.mkdir(parents=True, exist_ok=True)
        for existing in AutoUploaderConfig.POSTER_DIR.glob(f"{movie_id}.*"):
            if not force and existing.suffix.lower() in self._IMG_EXTS and existing.stat().st_size > 1024:
                return existing
            existing.unlink(missing_ok=True)  # forced refresh / empty / half-written leftover

        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        accept = "image/avif,image/webp,image/apng,image/*,*/*;q=0.8"
        candidates = [image_url]
        if "media-amazon.com" in image_url and "@" in image_url:
            # IMDb/Amazon: "<id>@" + any ._V1_...jpg suffix; the bare form serves the full-size original
            base = image_url.split("@")[0] + "@"
            candidates += [base + "._V1_.jpg", base + ".jpg", base + "._V1_UX1000_.jpg"]
        candidates = list(dict.fromkeys(candidates))
        # hot-link protection: try the image's own site, then the topic page, then no referer at all
        referers = [r for r in dict.fromkeys([
            (get_origin(image_url) + "/") if get_origin(image_url) else "", referer or "", "-"]) if r]
        timeout = aiohttp.ClientTimeout(total=30)
        attempt = 0
        for url in candidates:
            for ref in referers:
                attempt += 1
                headers = {"User-Agent": ua, "Accept": accept}
                if ref != "-":
                    headers["Referer"] = ref
                try:
                    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
                        # encoded=True: keep '@' etc. exactly as the API gave them (no re-quoting)
                        async with session.get(URL(url, encoded=True), allow_redirects=True) as resp:
                            data = await resp.read() if resp.status == 200 else b""
                            ctype = (resp.headers.get("Content-Type") or "").lower()
                    # the bytes decide, not the header: octet-stream / missing / wrong Content-Type is common
                    if len(data) > 512 and self._sniff_image(data):
                        dest = AutoUploaderConfig.POSTER_DIR / f"{movie_id}.jpg"
                        tmp = dest.with_suffix(".part")
                        tmp.write_bytes(data)
                        tmp.replace(dest)
                        ok = await asyncio.to_thread(self._normalize_poster, dest)
                        if ok:
                            return ok
                        dest.unlink(missing_ok=True)
                    else:
                        logger.warning("Poster %s (referer %s): HTTP %s %s %dB - not an image", url, ref, resp.status, ctype, len(data))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("Poster %s (referer %s) failed: %s", url, ref, exc)
                if attempt >= 8:
                    return None
                await asyncio.sleep(min(attempt, 3))
        return None

    async def _ensure_poster(self, movie_doc: Dict[str, Any]) -> Optional[str]:
        """Return a usable poster path for the movie, (re)downloading from image_url if it is missing."""
        path = movie_doc.get("poster_path")
        if self._poster_ok(path):
            return path
        poster = await self.download_poster(
            movie_doc.get("image_url", ""), movie_doc["movie_id"], referer=movie_doc.get("source_url")
        )
        if poster:
            movie_doc["poster_path"] = str(poster)
            await db.save_movie(movie_doc)
            return str(poster)
        return None

    # ------------------------------------------------------------------ #
    # Links: a link that Telegram can't use is never put into a post
    # ------------------------------------------------------------------ #

    def _file_link(self, tor: Dict[str, Any]) -> str:
        base = WebServerConfig.BASE_URL
        if is_public_url(base):
            return f"{base}/r/{tor['redirect_id']}"
        # localhost / private base URLs are silently dropped by Telegram -> use a t.me deep link instead
        return f"https://t.me/{FileStoreConfig.BOT_USERNAME}?start=file_{tor['file_id']}"

    def _batch_link(self, movie_doc: Dict[str, Any]) -> str:
        base = WebServerConfig.BASE_URL
        if is_public_url(base):
            return f"{base}/r/{movie_doc['batch_redirect_id']}"
        return f"https://t.me/{FileStoreConfig.BOT_USERNAME}?start={movie_doc['batch_redirect_id']}"

    _RES_ORDER = ("2160p", "1080p", "720p", "480p", "360p", "240p", "SD")

    @staticmethod
    def _resolution_of(tor: Dict[str, Any]) -> str:
        text = f"{tor.get('display_name', '')} {tor.get('original_filename', '')}"
        if re.search(r"\b(4K|2160p)\b", text, re.I):
            return "2160p"
        m = re.search(r"\b(1080|720|480|360|240)p\b", text, re.I)
        return f"{m.group(1)}p" if m else "SD"

    @staticmethod
    def _size_in_bytes(tor: Dict[str, Any]) -> float:
        m = re.search(r"(\d+(?:\.\d+)?)\s*(GB|MB)", str(tor.get("size_label", "")), re.I)
        if m:
            return float(m.group(1)) * (1024 ** 3 if m.group(2).upper() == "GB" else 1024 ** 2)
        return float(tor.get("size_bytes") or 0)

    def _best_per_resolution(self, torrents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Post shows ONE file per resolution (the biggest); 'Get all' still has every file."""
        best: Dict[str, Dict[str, Any]] = {}
        for t in torrents:
            res = self._resolution_of(t)
            if res not in best or self._size_in_bytes(t) > self._size_in_bytes(best[res]):
                best[res] = t
        return [best[r] for r in self._RES_ORDER if r in best]

    def format_telegram_caption(self, movie_doc: Dict[str, Any], torrents: List[Dict[str, Any]]) -> str:
        if movie_doc.get("custom_caption"):
            return movie_doc["custom_caption"]

        esc = lambda v: html.escape(str(v), quote=False)  # noqa: E731
        if not movie_doc.get("all_files"):  # per-user preference "FILES: ALL" shows every file
            torrents = self._best_per_resolution(torrents)
        footer = movie_doc.get("footer")
        tpl = movie_doc.get("caption_template")
        if tpl:
            files = "\n".join(
                f"• <a href=\"{self._file_link(t)}\">{esc(t['display_name'])}</a>" for t in torrents
            )
            try:
                out = tpl.format(
                    title=esc(movie_doc["title"]),
                    year=movie_doc["year"],
                    audio=movie_doc["audio"],
                    quality=movie_doc["quality"],
                    uploader=movie_doc["uploader"],
                    join_channel=movie_doc["join_channel"],
                    files=files,
                    all_link=self._batch_link(movie_doc),
                )
                return f"{out}\n\n{footer}" if footer else out
            except Exception:
                pass  # bad template -> fall back to the default layout

        lines = [
            f"🎬 <b>Title : {esc(movie_doc['title'])}</b>",
            "",
            f"🗓 <b>Year : {movie_doc['year']}</b>",
            "",
            f"🔊 <b>Audio : {movie_doc['audio']}</b>",
            "",
            f"💿 <b>Quality : {movie_doc['quality']}</b>",
            "",
            f"📥 <b>Upload : {movie_doc['uploader']}</b>",
            "",
            "<b>Telegram Files 👇</b>",
            "",
        ]
        for tor in torrents:
            lines.append(f"• <a href=\"{self._file_link(tor)}\">{esc(tor['display_name'])}</a>")
            lines.append("")

        if not movie_doc.get("hide_all_link"):
            lines.append(f"📦 <a href=\"{self._batch_link(movie_doc)}\"><b>Get all in one Click</b></a>")
            lines.append("")
        lines.append(f"◤ ❤ <b>JOIN : {movie_doc['join_channel']}</b>")
        if footer:
            lines += ["", footer]
        return "\n".join(lines)

    # ------------------------------------------------------------------ #
    # Per-user preferences: how a user's own channel post looks / what it contains
    # ------------------------------------------------------------------ #

    PREF_KEYS = ["poster_enabled", "show_all_link", "files_mode", "res_filter", "lang_filter", "footer", "log_enabled"]
    RES_CHOICES = ["2160p", "1080p", "720p", "480p", "360p", "240p"]
    LANG_CHOICES = ["Tamil", "Telugu", "Hindi", "Malayalam", "Kannada", "English", "Korean"]

    def _render_user_post(
        self, movie_doc: Dict[str, Any], torrents: List[Dict[str, Any]], cfg: Dict[str, Any]
    ) -> Optional[Tuple[str, List[Dict[str, Any]]]]:
        """Caption for one user's channel with THEIR settings + filters. None = this movie is filtered out for them."""
        langs = cfg.get("lang_filter") or []
        if langs:
            audio = str(movie_doc.get("audio", "")).lower()
            if not any(l.lower() in audio for l in langs):
                return None
        ress = cfg.get("res_filter") or []
        if ress:
            torrents = [t for t in torrents if self._resolution_of(t) in ress]
        if not torrents:
            return None
        uploader = cfg.get("uploader_name") or movie_doc["uploader"]
        join_tag = cfg.get("join_channel") or movie_doc["join_channel"]
        view = dict(
            movie_doc, uploader=uploader, join_channel=join_tag, custom_caption=None,
            caption_template=cfg.get("caption_template"),
            all_files=cfg.get("files_mode") == "all",
            hide_all_link=cfg.get("show_all_link") is False,
            footer=cfg.get("footer"),
        )
        user_torrents = [
            dict(t, display_name=re.sub(r"^@\S+\s*-\s*", f"{uploader} - ", t.get("display_name", ""))) for t in torrents
        ]
        return self.format_telegram_caption(view, user_torrents), torrents

    # ------------------------------------------------------------------ #
    # Telegram helpers
    # ------------------------------------------------------------------ #

    CAPTION_LIMIT = 1000  # photo captions are capped at 1024 visible characters
    TEXT_LIMIT = 4000     # text messages are capped at 4096

    @staticmethod
    def _visible_len(text: str) -> int:
        return len(html.unescape(re.sub(r"<[^>]+>", "", text)))

    def _split_caption(self, caption: str, has_photo: bool) -> List[str]:
        """Pack whole lines into messages: the first fits a photo caption, the rest are plain text."""
        limit = self.CAPTION_LIMIT if has_photo else self.TEXT_LIMIT
        chunks: List[str] = []
        cur: List[str] = []
        cur_len = 0
        for line in caption.split("\n"):
            ln = self._visible_len(line) + 1
            if cur and cur_len + ln > limit:
                chunks.append("\n".join(cur).strip("\n"))
                cur, cur_len, limit = [], 0, self.TEXT_LIMIT
            if not cur and not line.strip() and chunks:
                continue  # no blank lines at the top of continuation messages
            cur.append(line)
            cur_len += ln
        if cur:
            chunks.append("\n".join(cur).strip("\n"))
        return [c for c in chunks if c.strip()] or [caption[: self.TEXT_LIMIT]]

    async def _tg(self, func, *args, **kwargs):
        """Call a Telegram API method: waits out FloodWait and fixes 'peer not found' (resolve chat, retry once)."""
        peer_retried = False
        for _ in range(5):
            try:
                return await func(*args, **kwargs)
            except FloodWait as fw:
                wait = int(getattr(fw, "value", 5) or 5) + 1
                logger.warning("FloodWait: sleeping %ss", wait)
                await asyncio.sleep(wait)
            except Exception as exc:
                if not peer_retried and args and is_peer_error(exc):
                    peer_retried = True
                    logger.warning("Peer %s not resolved yet (%s) - resolving and retrying", args[0], exc)
                    if await ensure_chat(self.app, args[0]):
                        continue
                raise
        return await func(*args, **kwargs)

    def _lock_for(self, movie_id: str) -> asyncio.Lock:
        lock = self._movie_locks.get(movie_id)
        if lock is None:
            lock = self._movie_locks[movie_id] = asyncio.Lock()
        return lock

    async def _send_photo_safe(self, channel: int, poster_path: Optional[str], caption: str, label: str):
        """send_photo with a safety net: on any image problem the file is re-encoded as a clean JPEG and sent again
        (then as raw bytes) before the post falls back to text. Returns the message or None."""
        for step in ("file", "re-encoded", "bytes"):
            try:
                if step == "file":
                    src = poster_path
                elif step == "re-encoded":
                    fixed = await asyncio.to_thread(self._reencode_copy, poster_path, 1280)
                    if not fixed:
                        continue
                    src = fixed
                else:
                    import io
                    raw = Path(poster_path).read_bytes()
                    src = io.BytesIO(raw)
                    src.name = "poster.jpg"
                return await self._tg(self.app.send_photo, channel, photo=src, caption=caption, parse_mode=ParseMode.HTML)
            except FloodWait:
                raise
            except Exception as exc:
                logger.warning("send_photo (%s) failed for %s: %s", step, label, exc)
        logger.error("send_photo failed for %s with every method; posting without image", label)
        return None

    @staticmethod
    def _reencode_copy(path: Optional[str], max_side: int) -> Optional[str]:
        """A fresh, small, plain-RGB JPEG copy next to the poster (None if the file is not an image)."""
        try:
            from PIL import Image
            p = Path(path)
            with Image.open(p) as im:
                im = im.convert("RGB")
                im.thumbnail((max_side, max_side))
                out = p.with_name(p.stem + "_fix.jpg")
                im.save(out, "JPEG", quality=85)
            return str(out)
        except Exception:
            return None

    async def _push_chunks(
        self, channel: int, chunks: List[str], ids: List[int], poster_path: Optional[str], has_photo: bool, label: str
    ) -> Tuple[List[int], bool]:
        """Edit the messages that already exist, send the missing ones. Used for the main channel and user channels."""
        for i, chunk in enumerate(chunks):
            if i < len(ids):
                try:
                    if i == 0 and has_photo:
                        await self._tg(self.app.edit_message_caption, channel, ids[0], chunk, parse_mode=ParseMode.HTML)
                    else:
                        await self._tg(
                            self.app.edit_message_text, channel, ids[i], chunk,
                            parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                        )
                except MessageNotModified:
                    pass
                except Exception as exc:
                    logger.error("Could not edit post %s part %d: %s", label, i, exc)
                continue

            msg = None
            if i == 0 and has_photo:
                msg = await self._send_photo_safe(channel, poster_path, chunk, label)
                if msg is None:
                    has_photo = False  # chunk 0 is <=1000 chars, fine as plain text
            if msg is None:
                kwargs = {"reply_to_message_id": ids[0]} if ids else {}
                msg = await self._tg(
                    self.app.send_message, channel, chunk,
                    parse_mode=ParseMode.HTML, disable_web_page_preview=True, **kwargs,
                )
            ids.append(msg.id)
        return ids, has_photo

    async def _auto_post_on(self) -> bool:
        """Admin master switch (Settings -> AUTO POST). Default ON."""
        try:
            return (await db.get_bot_settings("auto_uploader")).get("auto_post_enabled", True) is not False
        except Exception:
            return True  # a DB hiccup must never silently stop posting

    async def catch_up_posts(self, limit: int = 30) -> int:
        """After auto post is switched back ON: post every live movie that was stored while it was OFF (oldest first)."""
        movies = await db.db.movies.find({"status": {"$ne": "pending_review"}}).sort("created_at", -1).to_list(length=limit)
        posted = 0
        for m in reversed(movies):
            try:
                if await db.get_post_record(m["movie_id"]) or not await db.get_torrents_by_movie(m["movie_id"]):
                    continue
                await self.sync_post(m["movie_id"])
                posted += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Catch-up post failed for %s: %s", m.get("movie_id"), exc)
            await asyncio.sleep(1)
        logger.info("Catch-up after auto post ON: %d post(s) sent.", posted)
        return posted

    async def sync_post(self, movie_id: str) -> None:
        """
        Make the channel post match the DB: first call creates the post, every later call
        EDITS it so it grows as more files are scraped (extra messages are added if the list gets long).
        """
        async with self._post_lock:
            movie_doc = await db.get_movie(movie_id)
            if not movie_doc:
                return
            if not await self._auto_post_on():
                logger.info("Auto post is OFF: '%s' is stored but not posted (it is posted when auto post is switched ON).",
                            movie_doc.get("title"))
                return
            torrents = await db.get_torrents_by_movie(movie_id)
            caption = self.format_telegram_caption(movie_doc, torrents)
            channel = AutoUploaderConfig.PUBLIC_CHANNEL_ID
            record = await db.get_post_record(movie_id)

            if not self.app:  # headless mode
                if not record:
                    await db.save_post_record(movie_id, channel, [0], False)
                return

            ids: List[int] = list((record or {}).get("message_ids") or [])
            if not ids and record and record.get("message_id"):
                ids = [record["message_id"]]  # post made by an older version
            if ids:
                poster_path = movie_doc.get("poster_path")
                has_photo = bool(record.get("has_photo"))
            else:
                poster_path = await self._ensure_poster(movie_doc)  # image must be ready BEFORE the first post
                has_photo = bool(poster_path and Path(poster_path).exists())
            chunks = self._split_caption(caption, has_photo)

            ids, has_photo = await self._push_chunks(channel, chunks, ids, poster_path, has_photo, movie_id)

            await db.save_post_record(movie_id, channel, ids, has_photo)
            logger.info("Post synced for '%s': %d file(s) in %d message(s)", movie_doc["title"], len(torrents), len(ids))

        # Paid users who set their own public channel get the same post (their uploader name / tag / caption / thumbnail).
        self._spawn(self.sync_user_posts(movie_id))

    # ------------------------------------------------------------------ #
    # Paid users' own channels (Advanced Settings): the same post is auto-uploaded to their public channel
    # with THEIR uploader name / join tag / caption template / thumbnail, and a line is logged in their log channel.
    # ------------------------------------------------------------------ #

    @staticmethod
    def _post_link(channel_id: int, username: Optional[str], message_id: int) -> str:
        if username:
            return f"https://t.me/{username.lstrip('@')}/{message_id}"
        cid = str(channel_id)
        return f"https://t.me/c/{cid[4:] if cid.startswith('-100') else cid.lstrip('-')}/{message_id}"

    async def sync_user_posts(self, movie_id: str) -> None:
        if not self.app:
            return
        async with self._user_post_lock:
            try:
                users = await db.get_auto_post_users()
            except Exception as exc:
                logger.error("Could not load user channels: %s", exc)
                return
            for cfg in users:
                uid = cfg.get("user_id")
                if not uid or uid == AutoUploaderConfig.OWNER_ID:  # the owner's channel is the main post above
                    continue
                try:
                    if not await self._can_use_advanced(uid):
                        continue  # banned (or premium required and plan ended): stop posting for them
                    await self._sync_one_user_post(movie_id, cfg)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.error("User channel post failed (user %s, channel %s): %s", uid, cfg.get("public_channel_id"), exc)
                    await self._notify_channel_problem(uid)
                await asyncio.sleep(0.5)

    async def _sync_one_user_post(self, movie_id: str, cfg: Dict[str, Any]) -> None:
        movie_doc = await db.get_movie(movie_id)
        torrents = await db.get_torrents_by_movie(movie_id)
        if not movie_doc or not torrents:
            return
        uid = cfg["user_id"]
        channel = int(cfg["public_channel_id"])
        rendered = self._render_user_post(movie_doc, torrents, cfg)
        if not rendered:
            return  # this movie is filtered out by the user's language / resolution filter
        caption, torrents = rendered

        record = await db.get_user_post(movie_id, uid)
        ids: List[int] = list((record or {}).get("message_ids") or [])
        poster_path: Optional[str] = None
        if ids:
            has_photo = bool(record.get("has_photo"))
        elif cfg.get("poster_enabled") is False:
            has_photo = False  # user prefers text-only posts
        else:
            thumb = cfg.get("thumbnail_path")
            poster_path = thumb if thumb and Path(thumb).exists() else await self._ensure_poster(movie_doc)
            has_photo = bool(poster_path and Path(poster_path).exists())
        is_new = not ids
        chunks = self._split_caption(caption, has_photo)
        ids, has_photo = await self._push_chunks(channel, chunks, ids, poster_path, has_photo, f"{movie_id}@{uid}")
        await db.save_user_post(movie_id, uid, channel, ids, has_photo)
        logger.info("User %s channel post synced for '%s' (%d file(s))", uid, movie_doc["title"], len(torrents))

        if is_new:
            await db.inc_user_counter(uid, "posts_total")
        log_id = cfg.get("log_channel_id")
        if log_id and ids and cfg.get("log_enabled") is not False:
            link = self._post_link(channel, cfg.get("public_channel_username"), ids[0])
            text = (
                f"{'📌 <b>New post</b>' if is_new else '✏️ <b>Post updated</b>'}\n\n"
                f"🎬 <b>{html.escape(movie_doc['title'])}</b> ({movie_doc['year']})\n"
                f"📁 Files: {len(torrents)}\n🔗 {link}"
            )
            try:
                await self._tg(self.app.send_message, int(log_id), text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            except Exception as exc:
                logger.warning("Could not write to user %s log channel %s: %s", uid, log_id, exc)

    async def _notify_channel_problem(self, uid: int) -> None:
        """Tell the user (at most every 6 hours) that posting to their channel fails."""
        try:
            cfg = await db.get_user_settings(uid)
            last = cfg.get("channel_error_at")
            if last:
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
                if (utcnow() - last).total_seconds() < 6 * 3600:
                    return
            await db.update_user_settings(uid, {"channel_error_at": utcnow()})
            await self.app.send_message(
                uid,
                "⚠️ <b>I could not post to your channel.</b>\n\n"
                f"Make sure BOTH bots are <b>ADMIN</b> (with post rights) in it: {self._both_bots_text()}\n"
                "Then open Advanced Settings → VERIFY BOTS ADMIN.",
                parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            logger.debug("Could not notify user %s about channel problem: %s", uid, exc)

    async def publish_movie_post(self, movie_id: str) -> None:
        await self.sync_post(movie_id)

    async def _send_torrent_to_log_channel(self, meta: Dict[str, Any], title: str, release_url: str) -> Optional[int]:
        """Store the downloaded .torrent in the log/storage channel; the File Store bot reads it back from there."""
        log_id = effective_log_chat()
        if not (self.app and log_id and meta.get("torrent_file_path")):
            return None
        try:
            caption = (
                f"📥 <b>Torrent saved</b>\n\n"
                f"🎬 <b>{html.escape(title)}</b>\n"
                f"📄 <code>{html.escape(meta.get('name') or '')}</code>\n"
                f"🔑 <b>Hash:</b> <code>{meta['infohash']}</code>\n"
                f"🌐 <b>Base URL:</b> {html.escape(meta.get('base_url') or '')}\n"
                f"🔗 <code>{html.escape(release_url or '')}</code>"
            )[:1000]
            msg = await self._tg(
                self.app.send_document,
                log_id,
                document=meta["torrent_file_path"],
                file_name=f"{meta['infohash']}.torrent",
                caption=caption,
                parse_mode=ParseMode.HTML,
            )
            logger.info("Stored .torrent %s in log channel %s (message %s)", meta["infohash"], log_id, msg.id)
            return msg.id
        except Exception as exc:
            logger.error("Could not send .torrent to log channel %s: %s", log_id, exc)
            return None

    # ------------------------------------------------------------------ #
    # Ingest: one file at a time, post/edit after EACH file
    # ------------------------------------------------------------------ #

    @staticmethod
    def _line_signature(name: str) -> str:
        """Normalised release name: ignores uploader tag, site prefix, extension, case and punctuation."""
        n = re.sub(r"^\s*www\.[a-z0-9.\-]+\s*-\s*", "", name or "", flags=re.I)
        n = re.sub(r"^\s*@\S+\s*-\s*", "", n)
        n = re.sub(r"(\.(torrent|mkv|mp4))+$", "", n, flags=re.I)
        return re.sub(r"[^a-z0-9]+", "", n.lower())

    def _is_duplicate_line(self, existing: List[Dict[str, Any]], rel: Dict[str, Any], meta: Optional[Dict[str, Any]]) -> Optional[str]:
        """Compare this release line with every file already stored for the movie. Returns the reason if duplicate."""
        sigs = {self._line_signature(rel.get("display_name", "")), self._line_signature(rel.get("original_filename", ""))}
        if meta:
            sigs.add(self._line_signature(meta.get("name", "")))
        sigs.discard("")
        size = rel.get("size_bytes") or (meta or {}).get("total_size") or 0
        for t in existing:
            if rel.get("torrent_url") and rel["torrent_url"] == t.get("torrent_url"):
                return "same torrent link"
            if rel.get("direct_url") and rel["direct_url"] == t.get("direct_url"):
                return "same direct link"
            if sigs & {self._line_signature(t.get("display_name", "")), self._line_signature(t.get("original_filename", ""))}:
                return "same file name"
            if size and t.get("size_bytes") == size and meta and self._line_signature(meta.get("name", "")) == self._line_signature(t.get("original_filename", "")):
                return "same name and size"
        return None

    async def _store_db_hit(
        self, movie_id: str, rel: Dict[str, Any], parsed: Dict[str, Any], hit: Dict[str, Any], infohash: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """The release is already in a Database channel: register it like any other file, but point it at the stored
        file (alias) and queue NO download. The post link -> File Store bot sends it instantly."""
        infohash = infohash or "dbc_" + hashlib.sha1(hit["file_id"].encode()).hexdigest()[:32]
        file_id = f"{movie_id}_{infohash[:16]}"
        for t in await db.get_torrents_by_movie(movie_id):  # one line per stored file, whatever the torrent count
            if t.get("db_file_id") == hit["file_id"] and t.get("file_id") != file_id:
                logger.info("Duplicate (same Database file as %s) skipped: %s", t.get("display_name"), rel["display_name"])
                return None
        if not await db.claim_infohash(
            infohash,
            {"movie_id": movie_id, "file_id": file_id, "torrent_url": None, "base_url": None, "original_filename": hit["name"]},
        ):
            logger.info("Duplicate (hash %s) skipped: %s", infohash, rel["display_name"])
            return None
        try:
            existing = await db.get_torrents_by_movie(movie_id)
            index = max([t.get("index", 0) for t in existing] + [0]) + 1
            redirect_id = uuid.uuid5(uuid.NAMESPACE_URL, file_id).hex[:12]
            size = int(hit.get("size_bytes") or 0)
            if size:  # show the size of the file the user really gets
                rel["size_bytes"], rel["size_label"] = size, human_readable_size(size)
            torrent_doc = {
                "file_id": file_id, "movie_id": movie_id, "index": index, "redirect_id": redirect_id,
                "infohash": None, "torrent_url": None, "base_url": None, "torrent_file_path": None,
                "log_message_id": None, "log_chat_id": None, "magnet_uri": None, "direct_url": None,
                "original_filename": hit["name"], "display_name": rel["display_name"],
                "size_bytes": rel["size_bytes"], "size_label": rel["size_label"], "quality_label": rel["quality_label"],
                "from_db_channel": True, "db_file_id": hit["file_id"],
            }
            await db.save_torrent(torrent_doc)
            await db.add_file_alias(file_id, hit["file_id"])
            await db.save_redirect_link(
                {
                    "redirect_id": redirect_id, "file_id": file_id, "movie_id": movie_id, "is_batch": False,
                    "title": parsed["title"], "original_filename": hit["name"],
                    "quality": rel["quality_label"], "size_label": rel["size_label"],
                }
            )
            await db.update_registry(infohash, {"size_bytes": rel["size_bytes"]})
            logger.info("Database channel hit for '%s' -> %s (%s): download skipped.", rel["display_name"], hit["name"], ", ".join(hit["passed"]))
            return torrent_doc
        except Exception:
            await db.release_infohash(infohash)
            raise

    async def _process_release(
        self, movie_id: str, rel: Dict[str, Any], parsed: Dict[str, Any], source_url: Optional[str],
        owner_id: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """Download its .torrent, dedupe by infohash, store everything. Returns the torrent doc if NEW."""
        infohash = rel["infohash"]  # from the magnet link, if there is one

        if infohash and await db.get_registry_entry(infohash):
            logger.info("Duplicate (magnet hash %s) skipped: %s", infohash, rel["display_name"])
            return None

        existing_files = await db.get_torrents_by_movie(movie_id)
        reason = self._is_duplicate_line(existing_files, rel, None)
        if reason:
            logger.info("Duplicate line skipped (%s): %s", reason, rel["display_name"])
            return None

        # Database channel first: same name + year + quality already stored? -> no .torrent, no download.
        if owner_id is not None:
            hit = await db_channel.find_for_release(owner_id, parsed["title"], parsed["year"], rel)
            if hit:
                return await self._store_db_hit(movie_id, rel, parsed, hit, infohash)

        meta: Optional[Dict[str, Any]] = None
        if rel["torrent_url"]:
            meta = await torrent_engine.fetch_and_parse_torrent_file(
                rel["torrent_url"], FileStoreConfig.TORRENT_DIR, source_url
            )
            if meta:
                if not infohash:
                    infohash = meta["infohash"]
                elif infohash != meta["infohash"]:
                    logger.warning("Magnet hash %s != torrent hash %s; magnet hash wins.", infohash, meta["infohash"])
                if not extract_dn_from_magnet(rel["magnet_uri"] or ""):
                    rel["original_filename"] = meta["name"] or rel["original_filename"]
                if not rel["size_bytes"]:
                    rel["size_bytes"] = meta["total_size"]
                if rel["size_label"] in ("Unknown", ""):
                    rel["size_label"] = human_readable_size(meta["total_size"])
                if meta.get("base_url"):
                    rel["base_url"] = meta["base_url"]  # host that really served the file

        if not infohash and rel["direct_url"]:
            infohash = "direct_" + hashlib.sha1(rel["direct_url"].encode()).hexdigest()[:32]
        if not infohash:
            logger.warning("Skipping '%s': no magnet hash and the .torrent could not be downloaded.", rel["display_name"])
            return None

        reason = self._is_duplicate_line(existing_files, rel, meta)
        if reason:
            logger.info("Duplicate line skipped (%s): %s", reason, rel["display_name"])
            return None

        file_id = f"{movie_id}_{infohash[:16]}"
        is_new = await db.claim_infohash(
            infohash,
            {
                "movie_id": movie_id,
                "file_id": file_id,
                "torrent_url": rel["torrent_url"],
                "base_url": rel.get("base_url"),
                "original_filename": rel["original_filename"],
            },
        )
        if not is_new:
            logger.info("Duplicate (hash %s) skipped: %s", infohash, rel["display_name"])
            return None

        try:
            log_msg_id = await self._send_torrent_to_log_channel(meta, parsed["title"], rel["torrent_url"]) if meta else None
            existing = await db.get_torrents_by_movie(movie_id)
            index = max([t.get("index", 0) for t in existing] + [0]) + 1
            redirect_id = uuid.uuid5(uuid.NAMESPACE_URL, file_id).hex[:12]

            torrent_doc = {
                "file_id": file_id,
                "movie_id": movie_id,
                "index": index,
                "redirect_id": redirect_id,
                "infohash": infohash if not infohash.startswith("direct_") else None,
                "torrent_url": rel["torrent_url"],
                "base_url": rel.get("base_url"),
                "torrent_file_path": meta["torrent_file_path"] if meta else None,
                "log_message_id": log_msg_id,
                "log_chat_id": effective_log_chat() if log_msg_id else None,
                "magnet_uri": rel["magnet_uri"],
                "direct_url": rel["direct_url"],
                "original_filename": rel["original_filename"],
                "display_name": rel["display_name"],
                "size_bytes": rel["size_bytes"],
                "size_label": rel["size_label"],
                "quality_label": rel["quality_label"],
            }
            await db.save_torrent(torrent_doc)
            await db.save_redirect_link(
                {
                    "redirect_id": redirect_id,
                    "file_id": file_id,
                    "movie_id": movie_id,
                    "is_batch": False,
                    "title": parsed["title"],
                    "original_filename": rel["original_filename"],
                    "quality": rel["quality_label"],
                    "size_label": rel["size_label"],
                }
            )
            await db.update_registry(
                infohash,
                {
                    "torrent_file_path": torrent_doc["torrent_file_path"],
                    "log_message_id": log_msg_id,
                    "size_bytes": rel["size_bytes"],
                },
            )
            if AutoUploaderConfig.PREFETCH_ON_INGEST and (torrent_doc["torrent_file_path"] or rel["magnet_uri"]):
                # no user yet (id 0 = nobody to notify): the File Store worker downloads it into the storage channel
                await db.acquire_download_lock(file_id, torrent_doc["infohash"], 0)
            return torrent_doc
        except Exception:
            await db.release_infohash(infohash)  # let a later scrape retry this file
            raise

    async def ingest_api_payload(self, payload: List[Dict[str, Any]], owner_id: Optional[int] = None) -> List[str]:
        owner_id = owner_id or AutoUploaderConfig.OWNER_ID
        settings = await db.get_bot_settings("auto_uploader")
        user_cfg = await db.get_user_settings(owner_id)
        uploader = user_cfg.get("uploader_name") or settings.get("uploader_name", AutoUploaderConfig.DEFAULT_UPLOADER)
        join_channel = user_cfg.get("join_channel") or settings.get("join_channel", AutoUploaderConfig.JOIN_CHANNEL)
        user_thumb = user_cfg.get("thumbnail_path")
        manual_mode = settings.get("manual_edit_mode", AutoUploaderConfig.MANUAL_EDIT_MODE)

        ingested_ids: List[str] = []
        for item in payload:
            raw_title = (item.get("title") or "").strip()
            releases = item.get("releases") or []
            if not raw_title or not releases:
                continue
            movie_id = hashlib.sha1(raw_title.encode("utf-8")).hexdigest()[:12]
            try:
                async with self._lock_for(movie_id):
                    added = await self._ingest_item(
                        item, raw_title, movie_id, uploader, join_channel, user_thumb, user_cfg, owner_id, manual_mode
                    )
                if added:
                    ingested_ids.append(movie_id)
            except Exception as exc:
                logger.exception("Failed to ingest '%s': %s", raw_title, exc)
        return ingested_ids

    async def _ingest_item(
        self, item, raw_title, movie_id, uploader, join_channel, user_thumb, user_cfg, owner_id, manual_mode
    ) -> int:
        source_url = item.get("source_url") or item.get("url") or item.get("link")
        parsed = parse_movie_metadata(raw_title, item.get("releases") or [], uploader, source_url)

        movie_doc = await db.get_movie_by_raw_title(raw_title)
        is_new_movie = movie_doc is None
        if is_new_movie:
            if user_thumb and Path(user_thumb).exists():
                poster_path = Path(user_thumb)  # user's custom thumbnail overrides scraped poster
            else:
                poster_path = await self.download_poster(item.get("image", ""), movie_id, referer=source_url)
            batch_redirect_id = f"batch_{movie_id}"
            movie_doc = {
                "movie_id": movie_id,
                "raw_title": raw_title,
                "title": parsed["title"],
                "year": parsed["year"],
                "audio": parsed["audio"],
                "quality": parsed["quality"],
                "uploader": uploader,
                "join_channel": join_channel,
                "image_url": resolve_url(item.get("image", ""), source_url) if item.get("image") else "",
                "source_url": source_url,
                "base_url": item.get("base_url") or get_origin(source_url),
                "poster_path": str(poster_path) if poster_path else None,
                "batch_redirect_id": batch_redirect_id,
                "custom_caption": None,
                "caption_template": user_cfg.get("caption_template"),
                "owner_id": owner_id,
                "status": "pending_review" if manual_mode else "live",
            }
            await db.save_movie(movie_doc)

        added = 0
        for rel in parsed["releases"]:
            try:
                torrent_doc = await self._process_release(movie_doc["movie_id"], rel, parsed, source_url, owner_id)
            except Exception as exc:
                logger.error("Release '%s' failed: %s", rel.get("display_name"), exc)
                continue
            if not torrent_doc:
                continue
            added += 1

            total = len(await db.get_torrents_by_movie(movie_doc["movie_id"]))
            await db.save_redirect_link(
                {
                    "redirect_id": movie_doc["batch_redirect_id"],
                    "file_id": movie_doc["batch_redirect_id"],
                    "movie_id": movie_doc["movie_id"],
                    "is_batch": True,
                    "title": movie_doc["title"],
                    "quality": movie_doc["quality"],
                    "size_label": f"{total} Files",
                }
            )

            # Review OFF  -> post right now (1st file) / edit the same post (every next file).
            # Review ON   -> hold everything back until the owner approves; an already-live post still updates.
            already_posted = await db.get_post_record(movie_doc["movie_id"])
            if not manual_mode or already_posted:
                try:
                    await self.sync_post(movie_doc["movie_id"])
                except Exception as exc:
                    logger.error("Posting '%s' failed: %s", movie_doc["title"], exc)

        if added and manual_mode and is_new_movie and self.app:
            await self._send_owner_review_prompt(movie_doc["movie_id"])
        return added

    async def _post_scheduler_loop(self) -> None:
        # Only used for posts the owner approved in manual-review mode (the "5m Queue").
        await db.wait_ready(3600)
        while True:
            try:
                due_job = await db.claim_next_due_post()
                if due_job:
                    await self.publish_movie_post(due_job["movie_id"])
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Error in post scheduler loop: %s", exc)
            await asyncio.sleep(5)

    def _build_review_keyboard(self, movie_id: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("✏️ Title", callback_data=f"edit|{movie_id}|title"),
                    InlineKeyboardButton("🗓 Year", callback_data=f"edit|{movie_id}|year"),
                    InlineKeyboardButton("🔊 Audio", callback_data=f"edit|{movie_id}|audio"),
                ],
                [
                    InlineKeyboardButton("💿 Quality", callback_data=f"edit|{movie_id}|quality"),
                    InlineKeyboardButton("📥 Uploader", callback_data=f"edit|{movie_id}|uploader"),
                    InlineKeyboardButton("📢 Join Tag", callback_data=f"edit|{movie_id}|join_channel"),
                ],
                [
                    InlineKeyboardButton("🖼 Poster URL", callback_data=f"edit|{movie_id}|image_url"),
                    InlineKeyboardButton("📝 Custom Caption", callback_data=f"edit|{movie_id}|custom_caption"),
                ],
                [
                    InlineKeyboardButton("✅ Approve & Schedule (5m Queue)", callback_data=f"approve|{movie_id}"),
                ],
            ]
        )

    async def _send_owner_review_prompt(self, movie_id: str, chat_id: Optional[int] = None) -> None:
        if not self.app:
            return
        target_chat = chat_id or AutoUploaderConfig.OWNER_ID
        movie_doc = await db.get_movie(movie_id)
        torrents = await db.get_torrents_by_movie(movie_id)
        preview_caption = "<b>[PREVIEW - PENDING APPROVAL]</b>\n\n" + self.format_telegram_caption(movie_doc, torrents)
        kb = self._build_review_keyboard(movie_id)

        poster_path = await self._ensure_poster(movie_doc)
        # photo captions are limited to 1024 chars; a long file list is previewed as plain text instead
        sent = False
        if poster_path and Path(poster_path).exists() and self._visible_len(preview_caption) <= self.CAPTION_LIMIT:
            try:
                await self.app.send_photo(target_chat, photo=poster_path, caption=preview_caption, parse_mode=ParseMode.HTML, reply_markup=kb)
                sent = True
            except Exception as exc:
                logger.warning("Review preview photo failed (%s); sending text", exc)
        if not sent:
            await self.app.send_message(
                target_chat, text=preview_caption[: self.TEXT_LIMIT * 2], parse_mode=ParseMode.HTML,
                reply_markup=kb, disable_web_page_preview=True,
            )

    # --- Admin check, shared actions, settings menu ---

    def _is_admin(self, user_id: int) -> bool:
        return user_id == AutoUploaderConfig.OWNER_ID

    async def run_scrape(self, owner_id: int) -> str:
        scraped = await asyncio.to_thread(fetch_dkbotz)
        if not scraped:
            return "❌ No topics returned from scraper."
        ids = await self.ingest_api_payload(scraped, owner_id=owner_id)
        return f"✅ Scraped {len(scraped)} topics and scheduled {len(ids)} new movie post(s)!"

    async def ingest_json_text(self, raw_text: str, owner_id: int) -> str:
        try:
            data = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            return f"❌ Invalid JSON: {exc}"
        if isinstance(data, dict):
            data = [data]
        ids = await self.ingest_api_payload(data, owner_id=owner_id)
        return f"✅ Ingested {len(ids)} movie(s): {', '.join(ids) if ids else 'none (already exist)'}"

    # ------------------------------------------------------------------ #
    # ▶️ START SCRAPING: a Premium user scrapes the source site and the newest posts go to THEIR OWN channel
    # (only after both bots were verified as admin there). The admin's button runs the normal full scrape.
    # ------------------------------------------------------------------ #

    async def _can_scrape(self, user_id: int) -> bool:
        if self._is_admin(user_id):
            return True
        try:
            if await db.get_ban(user_id):
                return False
            return await db.is_user_premium(user_id)
        except Exception as exc:
            logger.warning("Premium check failed for %s: %s", user_id, exc)
            return False

    async def _scrape_wait_left(self, user_id: int) -> int:
        """Seconds the user still has to wait before the next own scrape (0 = may start now)."""
        if self._is_admin(user_id):
            return 0
        cfg = await db.get_user_settings(user_id)
        last = cfg.get("last_scrape_at")
        if not last:
            return 0
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        left = AutoUploaderConfig.USER_SCRAPE_COOLDOWN_SECONDS - (utcnow() - last).total_seconds()
        return max(0, int(left))

    async def run_user_scrape(self, uid: int) -> str:
        cfg = await db.get_user_settings(uid)
        if not cfg.get("public_channel_id"):
            return "❌ Your public channel is not set. Open 🛠 ADVANCED → 📢 PUBLIC CHANNEL first."

        scraped = await asyncio.to_thread(fetch_dkbotz)
        scraped = [i for i in (scraped or []) if (i.get("title") or "").strip()]
        if not scraped:
            return "❌ The source site returned no topics right now. Please try again in a few minutes."

        # 1) Store the new releases once for everybody (branded with the admin's look; every user's channel
        #    then renders its own post with THAT user's uploader name, tag, caption and thumbnail).
        new_ids = await self.ingest_api_payload(scraped, owner_id=AutoUploaderConfig.OWNER_ID)

        # 2) THIS user's channel gets the newest posts that match THEIR settings (language / resolution filters):
        #    walk the list newest -> oldest, skip what their filters hide, stop at USER_SCRAPE_LIMIT matching posts,
        #    then post them oldest first so the newest ends up last. Posts already in their channel count too.
        limit = max(1, AutoUploaderConfig.USER_SCRAPE_LIMIT)
        posted = already = filtered = failed = 0
        todo: List[str] = []
        matched = 0
        for item in scraped[:max(limit * 8, 60)]:
            if matched >= limit:
                break
            movie_id = hashlib.sha1(item["title"].strip().encode("utf-8")).hexdigest()[:12]
            movie = await db.get_movie(movie_id)
            torrents = await db.get_torrents_by_movie(movie_id) if movie else []
            if not movie or not torrents or not self._render_user_post(movie, torrents, cfg):
                filtered += 1  # not stored, or hidden by this user's language / resolution filter
                continue
            matched += 1
            if await db.get_user_post(movie_id, uid):
                already += 1
            else:
                todo.append(movie_id)
        for movie_id in reversed(todo):
            try:
                async with self._user_post_lock:
                    await self._sync_one_user_post(movie_id, cfg)
                    if await db.get_user_post(movie_id, uid):
                        posted += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failed += 1
                logger.error("Own-scrape post failed (user %s, movie %s): %s", uid, movie_id, exc)
                if failed >= 3:
                    await self._notify_channel_problem(uid)
                    break
                continue
            await asyncio.sleep(0.5)

        lines = [
            f"📥 New releases stored: <b>{len(new_ids)}</b>",
            f"📬 Posted to your channel: <b>{posted}</b>",
        ]
        if already:
            lines.append(f"⏭ Already in your channel: <b>{already}</b>")
        if filtered:
            lines.append(f"🎛 Hidden by your preferences: <b>{filtered}</b>")
        if failed:
            lines.append(f"⚠️ Failed: <b>{failed}</b> - check that both bots are ADMIN in your channel (🔍 VERIFY BOTS).")
        return "\n".join(lines)

    async def _scrape_job(self, uid: int, message: Message) -> None:
        adv_kb = InlineKeyboardMarkup([[InlineKeyboardButton("≼ ADVANCED SETTINGS", callback_data="us|adv")]])
        try:
            if self._is_admin(uid):
                result = html.escape(await self.run_scrape(uid))
            else:
                result = await self.run_user_scrape(uid)
            text = f"🏁 <b>SCRAPING FINISHED</b>\n\n{result}"
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Scrape job failed for %s: %s", uid, exc)
            text = f"⚠️ <b>Scraping failed</b>\n\n<code>{html.escape(str(exc)[:300])}</code>"
        finally:
            self._scraping.discard(uid)
        try:
            await message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=adv_kb)
        except Exception:
            try:
                await self.app.send_message(uid, text, parse_mode=ParseMode.HTML, reply_markup=adv_kb)
            except Exception as exc:
                logger.warning("Could not deliver scrape result to %s: %s", uid, exc)

    USER_FIELDS = {
        "uploader": ("uploader_name", "📥 UPLOADER NAME", "Send the uploader name shown in posts (e.g. <code>@Ar_kinghd</code>):"),
        "join": ("join_channel", "📢 JOIN CHANNEL TAG", "Send the join tag shown at the bottom of posts (e.g. <code>@Ar_kinghd</code>):"),
        "caption": (
            "caption_template",
            "📝 CAPTION TEMPLATE",
            "Send your caption template (HTML allowed). Placeholders:\n"
            "<code>{title} {year} {audio} {quality} {uploader} {join_channel} {files} {all_link}</code>",
        ),
        "footer": ("footer", "📌 CUSTOM FOOTER", "Send a footer line added at the end of every post (HTML allowed, e.g. <code>🔔 Join @MyChannel</code>):"),
    }

    # ------------------------------------------------------------------ #
    # Start screen (2 buttons per row = 4 buttons, + one single START button = 5), settings, Advanced Settings
    # ------------------------------------------------------------------ #

    async def _plan_line(self, user_id: int) -> str:
        if self._is_admin(user_id):
            return "👑 Admin"
        try:
            doc = await db.get_user(user_id)
            if await db.is_user_premium(user_id):
                until = (doc or {}).get("premium_until")
                return f"💎 Premium ✅ ({fmt_left(until)} left)" if until else "💎 Premium ✅ (lifetime)"
        except Exception as exc:  # a DB hiccup must never remove the menu
            logger.warning("Plan lookup failed for %s: %s", user_id, exc)
        return "🆓 Free"

    async def build_start(self, user):
        uid = user.id
        name = html.escape(user.first_name or "there")
        text = (
            f"👋 <b>Hello {name}, I am Alive!</b>\n\n"
            "🤖 <b>Auto Uploader Bot</b>\n"
            "I turn every new movie into a ready channel post with Telegram file links, automatically.\n\n"
            f"🆔 <b>Your ID:</b> <code>{uid}</code>\n"
            f"💼 <b>Plan:</b> {await self._plan_line(uid)}\n\n"
            "⚙️ <b>Settings</b> - uploader name, join tag, caption, thumbnail\n"
            "🛠 <b>Advanced</b> - your own public &amp; log channel\n"
            "💎 <b>Premium</b> - no ads / token wait\n"
            "📊 <b>Status</b> - your settings &amp; channels\n"
            "🚀 <b>Start</b> - quick setup checklist"
        )
        if self._is_admin(uid):
            text += "\n\n🛡 <b>Admin:</b> /admin · /ingest · /scrape"
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("⚙️ SETTINGS", callback_data="us|main"),
                    InlineKeyboardButton("🛠 ADVANCED", callback_data="us|adv"),
                ],
                [
                    InlineKeyboardButton("💎 PREMIUM", callback_data="us|premium"),
                    InlineKeyboardButton("📊 STATUS", callback_data="us|status"),
                ],
                [InlineKeyboardButton("🚀 START", callback_data="us|quick")],
            ]
        )
        return text, kb

    async def build_quick_start(self, user_id: int):
        """🚀 START button: a 3-step setup checklist (own public channel, log channel, post look) with 2 buttons per row."""
        vals = await self._channel_values(user_id)
        cfg = await db.get_user_settings(user_id)
        allowed = await self._can_use_advanced(user_id)
        mark = lambda ok: "✅" if ok else "⬜"  # noqa: E731
        look = bool(cfg.get("uploader_name") or cfg.get("join_channel") or cfg.get("caption_template") or cfg.get("thumbnail_path"))
        text = (
            "🚀 <b>QUICK START</b>\n"
            "<i>Three steps and your channel posts on autopilot.</i>\n\n"
            f"{mark(vals['public_id'])} <b>1. Public channel</b> - your posts are auto-uploaded here\n"
            f"{mark(vals['log_id'])} <b>2. Log channel</b> - a line for every post (optional)\n"
            f"{mark(look)} <b>3. Your look</b> - uploader name, join tag, caption, thumbnail\n\n"
            f"ℹ️ First add {self._both_bots_text()} as <b>ADMIN</b> (with post rights) in your channels.\n"
            "💎 Premium: after verifying, ▶️ <b>START SCRAPING</b> in 🛠 ADVANCED fills your channel on demand."
        )
        if not allowed:
            text += "\n\n🔒 <i>Own channels need Premium (or you are banned).</i>"
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("📢 SET PUBLIC", callback_data="us|adv_set_pub"),
                    InlineKeyboardButton("🗄 SET LOG", callback_data="us|adv_set_log"),
                ],
                [
                    InlineKeyboardButton("⚙️ SETTINGS", callback_data="us|main"),
                    InlineKeyboardButton("🔍 VERIFY", callback_data="us|adv_verify"),
                ],
                [InlineKeyboardButton("≼ BACK", callback_data="us|start")],
            ]
        )
        return text, kb

    async def build_settings_menu(self, user_id: int):
        admin = self._is_admin(user_id)
        cfg = await db.get_user_settings(user_id)
        glob = await db.get_bot_settings("auto_uploader")
        manual = glob.get("manual_edit_mode", False)
        auto_on = glob.get("auto_post_enabled", True) is not False

        text = (
            "<b>⚙️ UPLOADER SETTINGS</b>\n\n"
            f"• <b>Uploader:</b> {html.escape(cfg.get('uploader_name') or 'Default')}\n"
            f"• <b>Join Tag:</b> {html.escape(cfg.get('join_channel') or 'Default')}\n"
            f"• <b>Thumbnail:</b> {'Custom ✅' if cfg.get('thumbnail_path') else 'Scraped poster'}\n"
            f"• <b>Caption:</b> {'Custom template ✅' if cfg.get('caption_template') else 'Default layout'}\n"
        )
        if admin:
            text += (
                f"\n<b>Admin:</b>\n• Auto Post: {'ON' if auto_on else 'OFF'}\n• Manual Review: {'ON' if manual else 'OFF'}\n"
                f"• Post Interval: {glob.get('post_interval_seconds', AutoUploaderConfig.POST_INTERVAL_SECONDS)}s"
            )

        # Admin-only buttons are simply not shown to normal users (no locked buttons).
        rows = []
        if admin:
            rows.append(
                [
                    InlineKeyboardButton("📥 INGEST", callback_data="us|ingest"),
                    InlineKeyboardButton("🔍 SCRAPE", callback_data="us|scrape"),
                ]
            )
        rows += [
            [
                InlineKeyboardButton("🖼 THUMBNAIL", callback_data="us|thumb"),
                InlineKeyboardButton("📥 UPLOADER", callback_data="us|f_uploader"),
            ],
            [
                InlineKeyboardButton("📢 JOIN TAG", callback_data="us|f_join"),
                InlineKeyboardButton("📝 CAPTION", callback_data="us|f_caption"),
            ],
            [
                InlineKeyboardButton("🎛 PREFERENCES", callback_data="us|pf"),
                InlineKeyboardButton("🛠 ADVANCED", callback_data="us|adv"),
            ],
        ]
        if admin:
            rows.append([InlineKeyboardButton(f"🤖 AUTO POST: {'ON ✅' if auto_on else 'OFF ❌'}", callback_data="us|autopost")])
            rows.append(
                [
                    InlineKeyboardButton(f"✏️ REVIEW: {'ON ✅' if manual else 'OFF ❌'}", callback_data="us|manual"),
                    InlineKeyboardButton("⏱ INTERVAL", callback_data="us|interval"),
                ]
            )
        rows.append([InlineKeyboardButton("≼ BACK", callback_data="us|start")])
        return text, InlineKeyboardMarkup(rows)

    # --- Preferences: what goes into MY channel posts (applies to every post auto-uploaded to my public channel) ---

    async def build_prefs_menu(self, uid: int):
        cfg = await db.get_user_settings(uid)
        onoff = lambda v: "ON ✅" if v else "OFF ❌"  # noqa: E731
        poster = cfg.get("poster_enabled") is not False
        admin = self._is_admin(uid)
        if admin:
            auto_post = (await db.get_bot_settings("auto_uploader")).get("auto_post_enabled", True) is not False
        else:
            auto_post = cfg.get("auto_post_enabled", True) is not False
        alllink = cfg.get("show_all_link") is not False
        logs = cfg.get("log_enabled") is not False
        files_all = cfg.get("files_mode") == "all"
        ress = cfg.get("res_filter") or []
        langs = cfg.get("lang_filter") or []
        text = (
            "<b>🎛 PREFERENCES</b>\n"
            "<i>Applied to every post auto-uploaded to your own public channel.</i>\n\n"
            f"🤖 <b>Auto post:</b> {onoff(auto_post)}\n"
            f"🖼 <b>Poster image:</b> {onoff(poster)}\n"
            f"📦 <b>“Get all” line:</b> {onoff(alllink)}\n"
            f"📁 <b>Files shown:</b> {'ALL files' if files_all else 'Best one per resolution'}\n"
            f"🗄 <b>Log-channel lines:</b> {onoff(logs)}\n"
            f"🎞 <b>Resolution filter:</b> {', '.join(ress) if ress else 'All'}\n"
            f"🔊 <b>Language filter:</b> {', '.join(langs) if langs else 'All'}\n"
            f"📌 <b>Footer:</b> {html.escape(cfg['footer']) if cfg.get('footer') else 'None'}"
        )
        kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton(f"🤖 AUTO POST: {onoff(auto_post)}", callback_data="us|pf_auto")],
                [
                    InlineKeyboardButton(f"🖼 POSTER: {onoff(poster)}", callback_data="us|pf_poster"),
                    InlineKeyboardButton(f"📦 GET-ALL: {onoff(alllink)}", callback_data="us|pf_alllink"),
                ],
                [
                    InlineKeyboardButton(f"📁 FILES: {'ALL' if files_all else 'BEST'}", callback_data="us|pf_files"),
                    InlineKeyboardButton(f"🗄 LOG LINES: {onoff(logs)}", callback_data="us|pf_log"),
                ],
                [
                    InlineKeyboardButton("🎞 RESOLUTION", callback_data="us|pf_res"),
                    InlineKeyboardButton("🔊 LANGUAGE", callback_data="us|pf_lang"),
                ],
                [
                    InlineKeyboardButton("📌 FOOTER", callback_data="us|f_footer"),
                    InlineKeyboardButton("👁 PREVIEW", callback_data="us|pf_preview"),
                ],
                [
                    InlineKeyboardButton("♻️ RESET ALL", callback_data="us|pf_reset"),
                    InlineKeyboardButton("≼ BACK", callback_data="us|main"),
                ],
            ]
        )
        return text, kb

    async def build_chip_menu(self, uid: int, kind: str):
        """Multi-select filter (resolution / language): tap to toggle; nothing selected = no filter."""
        cfg = await db.get_user_settings(uid)
        key, choices, title = (
            ("res_filter", self.RES_CHOICES, "🎞 RESOLUTION FILTER")
            if kind == "res" else ("lang_filter", self.LANG_CHOICES, "🔊 LANGUAGE FILTER")
        )
        chosen = cfg.get(key) or []
        text = (
            f"<b>{title}</b>\n\n"
            "Tap to select. Only movies / files that match are posted to your channel.\n"
            f"<b>Selected:</b> {', '.join(chosen) if chosen else 'None (everything is posted)'}"
        )
        btns = [
            InlineKeyboardButton(f"{'✅' if c in chosen else '▫️'} {c}", callback_data=f"us|pf_{kind}_t_{c}")
            for c in choices
        ]
        rows = [btns[i:i + 2] for i in range(0, len(btns), 2)]
        rows.append(
            [
                InlineKeyboardButton("🧹 CLEAR", callback_data=f"us|pf_{kind}_clear"),
                InlineKeyboardButton("≼ BACK", callback_data="us|pf"),
            ]
        )
        return text, InlineKeyboardMarkup(rows)

    async def _prefs_callback(self, cq: CallbackQuery, uid: int, action: str) -> None:
        cfg = await db.get_user_settings(uid)

        async def show_prefs():
            text, kb = await self.build_prefs_menu(uid)
            await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)

        if action == "pf":
            self.user_states.pop(uid, None)
            await show_prefs()
        elif action in ("pf_poster", "pf_alllink", "pf_log"):
            key = {"pf_poster": "poster_enabled", "pf_alllink": "show_all_link", "pf_log": "log_enabled"}[action]
            await db.update_user_settings(uid, {key: cfg.get(key) is False})  # missing/True -> False, False -> True
            await show_prefs()
        elif action == "pf_auto":
            if self._is_admin(uid):  # the owner's channel is the main post: this is the master switch
                turn_on = (await db.get_bot_settings("auto_uploader")).get("auto_post_enabled", True) is False
                await db.update_bot_settings("auto_uploader", {"auto_post_enabled": turn_on})
                if turn_on:
                    self._spawn(self.catch_up_posts())
                await cq.answer("🤖 Auto post ON - catching up on missed posts…" if turn_on else "🤖 Auto post OFF", show_alert=turn_on)
                await show_prefs()
                return
            await db.update_user_settings(uid, {"auto_post_enabled": cfg.get("auto_post_enabled", True) is False})
            await show_prefs()
        elif action == "pf_files":
            await db.update_user_settings(uid, {"files_mode": "best" if cfg.get("files_mode") == "all" else "all"})
            await show_prefs()
        elif action in ("pf_res", "pf_lang"):
            text, kb = await self.build_chip_menu(uid, action[3:])
            await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        elif action.startswith(("pf_res_", "pf_lang_")):
            kind = "res" if action.startswith("pf_res_") else "lang"
            key = "res_filter" if kind == "res" else "lang_filter"
            choices = self.RES_CHOICES if kind == "res" else self.LANG_CHOICES
            rest = action.split("_", 2)[2]
            chosen = list(cfg.get(key) or [])
            if rest == "clear":
                chosen = []
            elif rest.startswith("t_") and rest[2:] in choices:
                opt = rest[2:]
                chosen = [c for c in chosen if c != opt] if opt in chosen else chosen + [opt]
            await db.update_user_settings(uid, {key: chosen})
            text, kb = await self.build_chip_menu(uid, kind)
            await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        elif action == "pf_preview":
            movie = await db.db.movies.find_one({}, sort=[("created_at", -1)])
            rendered = None
            if movie:
                rendered = self._render_user_post(movie, await db.get_torrents_by_movie(movie["movie_id"]), cfg)
            if not rendered:
                await cq.answer("Nothing to preview: no movie yet, or your filters hide the latest one.", show_alert=True)
                return
            chunk = self._split_caption(rendered[0], False)[0]
            await cq.message.reply_text(
                "👁 <b>PREVIEW</b> (your latest post)\n\n" + chunk, parse_mode=ParseMode.HTML, disable_web_page_preview=True
            )
        elif action == "pf_reset":
            kb = InlineKeyboardMarkup(
                [[InlineKeyboardButton("✅ YES, RESET", callback_data="us|pf_reset_yes"),
                  InlineKeyboardButton("≼ CANCEL", callback_data="us|pf")]]
            )
            await cq.message.edit_text(
                "♻️ <b>Reset all preferences?</b>\nChannels, uploader name, tag, caption and thumbnail stay as they are.",
                parse_mode=ParseMode.HTML, reply_markup=kb,
            )
        elif action == "pf_reset_yes":
            await db.update_user_settings(uid, unset=self.PREF_KEYS)
            await cq.answer("Preferences reset.")
            await show_prefs()

    # --- Advanced Settings: own public + log channel (admin and paid users) ---

    def _both_bots_text(self) -> str:
        from config import FileStoreConfig as _FS  # username is refreshed by the File Store bot at start
        auto = f"@{self.bot_username}" if self.bot_username else "the Auto Uploader bot"
        store = f"@{_FS.BOT_USERNAME}" if _FS.BOT_USERNAME else "the File Store bot"
        return f"{auto} and {store}"

    async def _can_use_advanced(self, user_id: int) -> bool:
        """Own public/log channel: open to every (non-banned) user unless REQUIRE_PREMIUM_FOR_CHANNELS=true."""
        if self._is_admin(user_id):
            return True
        if await db.get_ban(user_id):
            return False
        if not AutoUploaderConfig.REQUIRE_PREMIUM_FOR_CHANNELS:
            return True
        return await db.is_user_premium(user_id)

    async def _channel_values(self, user_id: int) -> Dict[str, Any]:
        """Current public / log channel for this person (the admin's live in bot settings, users' in their settings)."""
        if self._is_admin(user_id):
            glob = await db.get_bot_settings("auto_uploader")
            log_id = effective_log_chat()
            return {
                "public_id": AutoUploaderConfig.PUBLIC_CHANNEL_ID or None,
                "public_title": glob.get("public_channel_title"),
                "log_id": log_id or None,
                "log_title": glob.get("log_channel_title"),
                "db_id": glob.get("db_channel_id") or AutoUploaderConfig.DB_CHANNEL_ID or None,
                "db_title": glob.get("db_channel_title"),
                "auto": None,
            }
        cfg = await db.get_user_settings(user_id)
        return {
            "public_id": cfg.get("public_channel_id"),
            "public_title": cfg.get("public_channel_title"),
            "log_id": cfg.get("log_channel_id"),
            "log_title": cfg.get("log_channel_title"),
            "db_id": cfg.get("db_channel_id"),
            "db_title": cfg.get("db_channel_title"),
            "auto": cfg.get("auto_post_enabled", True),
        }

    @staticmethod
    def _chan_label(cid: Any, title: Optional[str]) -> str:
        if not cid:
            return "Not set"
        t = f"{html.escape(title)} " if title else ""
        return f"{t}(<code>{cid}</code>)"

    async def build_adv_menu(self, user_id: int):
        admin = self._is_admin(user_id)
        allowed = await self._can_use_advanced(user_id)
        can_scrape = await self._can_scrape(user_id)
        vals = await self._channel_values(user_id)
        tick = lambda v: "✅" if v else "⬜"  # noqa: E731
        text = (
            "<b>🛠 ADVANCED SETTINGS</b>\n"
            "<i>Your own channel, your own posts - fully automatic.</i>\n\n"
            f"💼 <b>Plan:</b> {await self._plan_line(user_id)}\n\n"
            f"{tick(vals['public_id'])} <b>Public channel</b> (posts go here)\n"
            f"      {self._chan_label(vals['public_id'], vals['public_title'])}\n"
            f"{tick(vals['log_id'])} <b>Log channel</b> (activity log, optional)\n"
            f"      {self._chan_label(vals['log_id'], vals['log_title'])}\n"
            f"{tick(vals['db_id'])} <b>Database channel</b> (files you already have - reused, no download)\n"
            f"      {self._chan_label(vals['db_id'], vals['db_title'])}\n"
        )
        if vals["auto"] is not None:
            text += f"{tick(vals['auto'])} <b>Auto upload:</b> {'ON' if vals['auto'] else 'OFF'}\n"
        if not allowed:
            text += "\n🔒 <i>Premium is required to add your own channels.</i>\n"
        text += (
            "\n<b>How to get started</b>\n"
            f"1️⃣ Add {self._both_bots_text()} as <b>ADMIN</b> in your channel\n"
            "2️⃣ Tap <b>PUBLIC CHANNEL</b> and send its @username or ID\n"
            "3️⃣ Tap <b>VERIFY BOTS</b> to confirm both bots can post\n"
        )
        if can_scrape:
            if vals["public_id"]:
                text += "4️⃣ Tap <b>▶️ START SCRAPING</b> - the newest posts are scraped and sent to your channel"
            else:
                text += "4️⃣ <b>▶️ START SCRAPING</b> unlocks as soon as your public channel is set"
        else:
            text += "💎 <i>Premium users can also scrape on demand with ▶️ START SCRAPING.</i>"

        rows = [
            [
                InlineKeyboardButton("📢 PUBLIC CHANNEL", callback_data="us|adv_pub"),
                InlineKeyboardButton("🗄 LOG CHANNEL", callback_data="us|adv_log"),
            ],
            [InlineKeyboardButton("📚 DATABASE CHANNEL", callback_data="us|adv_db")],
        ]
        second = [InlineKeyboardButton("🔍 VERIFY BOTS", callback_data="us|adv_verify")]
        if not admin:
            second.insert(0, InlineKeyboardButton(f"🔄 AUTO: {'ON ✅' if vals['auto'] else 'OFF ❌'}", callback_data="us|adv_toggle"))
        rows.append(second)
        if can_scrape and vals["public_id"]:
            label = "⏳ SCRAPING IN PROGRESS…" if user_id in self._scraping else "▶️ START SCRAPING"
            rows.append([InlineKeyboardButton(label, callback_data="us|adv_scrape")])
        elif not can_scrape and not admin:
            rows.append([InlineKeyboardButton("💎 START SCRAPING (PREMIUM)", callback_data="us|adv_scrape")])
        rows.append(
            [
                InlineKeyboardButton("🎛 PREFERENCES", callback_data="us|pf"),
                InlineKeyboardButton("≼ BACK", callback_data="us|start"),
            ]
        )
        return text, InlineKeyboardMarkup(rows)

    @staticmethod
    def _parse_channel_input(text: str) -> Optional[Any]:
        t = (text or "").strip()
        m = re.search(r"t\.me/c/(\d+)", t)
        if m:
            return int("-100" + m.group(1))
        digits = t.lstrip("-")
        if digits.isdigit():
            return int(t) if t.startswith("-100") else int("-100" + digits)
        m = re.search(r"(?:t\.me/|@)?([A-Za-z][A-Za-z0-9_]{3,31})/?$", t)
        return m.group(1) if m else None

    @staticmethod
    async def _bot_admin_problem(client: Optional[Client], chat_id: Any) -> Optional[str]:
        """None = this bot is admin with post rights in the chat. Otherwise a short reason."""
        if client is None:
            return None  # that bot is not running here (headless): nothing to check
        try:
            member = await client.get_chat_member(chat_id, "me")
        except Exception as exc:
            logger.info("Admin check for %s failed: %s", chat_id, exc)
            return "cannot see the channel (add it as ADMIN first)"
        if member.status == ChatMemberStatus.OWNER:
            return None
        if member.status != ChatMemberStatus.ADMINISTRATOR:
            return "is not an admin there"
        priv = getattr(member, "privileges", None)
        if priv is not None and getattr(priv, "can_post_messages", None) is False:
            return "has no 'Post messages' right"
        return None

    def _file_store_client(self) -> Optional[Client]:
        try:
            from file_store import file_store_bot  # lazy: file_store imports this module
            return file_store_bot.app
        except Exception:
            return None

    async def check_both_bots(self, chat_id: Any) -> List[str]:
        """Problems (empty list = fine) for the Auto Uploader bot and the File Store bot in `chat_id`."""
        from config import FileStoreConfig as _FS
        problems = []
        auto_p = await self._bot_admin_problem(self.app, chat_id)
        if auto_p:
            problems.append(f"❌ Auto Uploader (@{self.bot_username or '?'}) {auto_p}")
        store_p = await self._bot_admin_problem(self._file_store_client(), chat_id)
        if store_p:
            problems.append(f"❌ File Store (@{_FS.BOT_USERNAME}) {store_p}")
        return problems

    @staticmethod
    async def _bot_is_admin_problem(client: Optional[Client], chat_id: Any) -> Optional[str]:
        """Like _bot_admin_problem but only needs ADMIN status (reading a channel needs no post rights)."""
        if client is None:
            return None
        try:
            member = await client.get_chat_member(chat_id, "me")
        except Exception:
            return "cannot see the channel (add it as ADMIN first)"
        if member.status in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR):
            return None
        return "is not an admin there"

    async def reconcile_with_db_channel(self, uid: int) -> int:
        """After a scan: torrent lines already on this user's posts that the Database channel now covers are switched
        to the stored file (no download). Returns how many lines were switched."""
        switched, touched = 0, []
        for movie in await db.get_movies_by_owner(uid):
            used = {t.get("db_file_id") for t in await db.get_torrents_by_movie(movie["movie_id"]) if t.get("db_file_id")}
            changed = False
            for t in await db.get_torrents_by_movie(movie["movie_id"]):
                if t.get("from_db_channel") or await db.find_completed_file(t["file_id"]):
                    continue
                rel = {"original_filename": t.get("original_filename"), "display_name": t.get("display_name"),
                       "quality_label": t.get("quality_label")}
                hit = await db_channel.find_for_release(uid, movie.get("title") or "", movie.get("year"), rel)
                if not hit or hit["file_id"] in used:
                    continue
                used.add(hit["file_id"])
                await db.add_file_alias(t["file_id"], hit["file_id"])
                await db.mark_torrent_from_db(t["file_id"], hit["file_id"], hit["name"], hit.get("size_bytes") or 0)
                switched += 1
                changed = True
            if changed:
                touched.append(movie["movie_id"])
        for mid in touched:
            try:
                await self.sync_post(mid)
            except Exception as exc:
                logger.warning("Could not refresh post %s after Database scan: %s", mid, exc)
        return switched

    async def _run_db_sync(self, uid: int, chat_id: int, message: Message) -> None:
        """Index the files already in a Database channel and tell the user the result."""
        try:
            from file_store import file_store_bot
            if chat_id in file_store_bot._db_syncing:
                await message.reply_text("⏳ A sync of this channel is already running.")
                return
            status = await message.reply_text("🔍 <b>Full scan of your Database channel…</b>", parse_mode=ParseMode.HTML)
            last = {"t": 0.0}

            async def progress(st: Dict[str, int]) -> None:
                now = asyncio.get_event_loop().time()
                if now - last["t"] > 4:
                    last["t"] = now
                    await status.edit_text(
                        f"🔄 <b>Indexing…</b> read {st['scanned']} messages, {st['indexed']} video files indexed",
                        parse_mode=ParseMode.HTML,
                    )

            owner = (await db.get_db_channel_map()).get(chat_id, uid)
            st = await file_store_bot.sync_db_channel(chat_id, owner, progress, full=True)
            total = await db.count_db_channel_files(chat_id)
            switched = await self.reconcile_with_db_channel(uid)
            await status.edit_text(
                f"✅ <b>Full scan finished.</b>\n📚 {total} video file(s) in the index "
                f"(read {st['scanned']} messages, {st.get('removed', 0)} stale removed).\n"
                f"🔁 {switched} torrent line(s) on your posts now use the Database file - no download.\n"
                + (f"⚠️ {st.get('unnamed', 0)} video(s) have no file name or caption, so they cannot be matched - "
                   "put the release name in the caption.\n" if st.get("unnamed") else "")
                + 
                "<i>New uploads to the channel are indexed automatically.</i>",
                parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            logger.warning("Database channel sync failed: %s", exc)
            await message.reply_text(f"⚠️ Could not index the channel: {html.escape(str(exc)[:200])}")

    async def _save_channel(self, message: Message, uid: int, kind: str, raw: str) -> None:
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("≼ ADVANCED SETTINGS", callback_data="us|adv")]])
        label = {"public": "Public", "log": "Log", "db": "Database"}.get(kind, "Log")
        target = self._parse_channel_input(raw)
        if target is None:
            await message.reply_text("❌ Send the channel <b>@username</b> or its <b>-100…</b> ID.", parse_mode=ParseMode.HTML)
            self.user_states[uid] = {"action": "adv_channel", "kind": kind}
            return
        try:
            chat = await self.app.get_chat(target)
        except Exception:
            await message.reply_text(
                "❌ I cannot find that channel.\n\n"
                f"Add {self._both_bots_text()} as <b>ADMIN</b> in it first, then send the @username / ID again.",
                parse_mode=ParseMode.HTML,
                reply_markup=back_kb,
            )
            return
        if "CHANNEL" not in str(chat.type).upper() and "SUPERGROUP" not in str(chat.type).upper():
            await message.reply_text("❌ That is not a channel. Send a channel.", reply_markup=back_kb)
            return

        admin = self._is_admin(uid)
        reserved = {_DEFAULT_PUBLIC_CHANNEL, AutoUploaderConfig.PUBLIC_CHANNEL_ID, effective_log_chat(),
                    FileStoreConfig.STORAGE_CHANNEL_ID} - {0, None}
        if not admin and chat.id in reserved:
            await message.reply_text("❌ That channel is reserved. Use your own channel.", reply_markup=back_kb)
            return

        if not admin:  # only the real owner/admin of a channel can attach it to themselves
            try:
                member = await self.app.get_chat_member(chat.id, uid)
                ok = member.status in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR)
            except Exception:
                ok = False
            if not ok:
                await message.reply_text("❌ You must be an admin of that channel to use it.", reply_markup=back_kb)
                return

        if kind == "db":  # only the File Store bot reads this channel (it indexes the files), it needs no post rights
            fs_problem = await self._bot_is_admin_problem(self._file_store_client(), chat.id)
            problems = [f"❌ File Store (@{FileStoreConfig.BOT_USERNAME}) {fs_problem}"] if fs_problem else []
        else:
            problems = await self.check_both_bots(chat.id)
        if problems:
            await message.reply_text(
                "⚠️ <b>Channel not saved.</b> "
                + ("The File Store bot must be ADMIN there:" if kind == "db" else "Both bots must be ADMIN with post rights:")
                + "\n\n"
                + "\n".join(problems)
                + "\n\nFix it, then send the channel again.",
                parse_mode=ParseMode.HTML,
                reply_markup=back_kb,
            )
            self.user_states[uid] = {"action": "adv_channel", "kind": kind}
            return

        title = chat.title or ""
        if admin:
            fields = {f"{kind}_channel_id": chat.id, f"{kind}_channel_title": title}
            await db.update_bot_settings("auto_uploader", fields)
            if kind == "public":
                AutoUploaderConfig.PUBLIC_CHANNEL_ID = chat.id
            elif kind == "log":
                AutoUploaderConfig.LOG_CHANNEL_ID = chat.id
        else:
            fields = {f"{kind}_channel_id": chat.id, f"{kind}_channel_title": title}
            if kind == "public":
                fields["public_channel_username"] = chat.username or None
                fields["auto_post_enabled"] = True
            await db.update_user_settings(uid, fields)
        extra = {
            "public": "New posts will be auto-uploaded there.",
            "log": "Post activity is logged there.",
            "db": "Before every post I search this channel by name + year + quality. A match is sent straight from "
                  "here - nothing is downloaded.",
        }[kind]
        done_kb = back_kb
        if kind == "db":
            fs = self._file_store_client()
            try:
                from file_store import file_store_bot
                await file_store_bot.refresh_db_channels()
            except Exception as exc:
                logger.warning("Could not refresh Database channels: %s", exc)
            self._spawn(self._run_db_sync(uid, chat.id, message))
            extra += "\n\n🔄 Indexing the files already in the channel now - new uploads are indexed automatically."
        if kind == "public" and await self._can_scrape(uid):
            extra += "\n\n▶️ Tap <b>START SCRAPING</b> to fill your channel with the newest posts right now."
            done_kb = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("▶️ START SCRAPING", callback_data="us|adv_scrape")],
                    [InlineKeyboardButton("≼ ADVANCED SETTINGS", callback_data="us|adv")],
                ]
            )
        await message.reply_text(
            f"✅ <b>{label} channel verified & saved:</b> {html.escape(title)} (<code>{chat.id}</code>)\n"
            f"Both bots are admin ✔\n{extra}",
            parse_mode=ParseMode.HTML,
            reply_markup=done_kb,
        )

    async def _clear_channel(self, uid: int, kind: str) -> None:
        if self._is_admin(uid):
            await db.update_bot_settings("auto_uploader", {f"{kind}_channel_id": None, f"{kind}_channel_title": None})
            if kind == "public":
                AutoUploaderConfig.PUBLIC_CHANNEL_ID = _DEFAULT_PUBLIC_CHANNEL
            elif kind == "log":
                AutoUploaderConfig.LOG_CHANNEL_ID = _DEFAULT_LOG_CHANNEL
        else:
            keys = [f"{kind}_channel_id", f"{kind}_channel_title"]
            if kind == "public":
                keys.append("public_channel_username")
            await db.update_user_settings(uid, unset=keys)

    async def _verify_report(self, uid: int) -> Tuple[str, bool]:
        """(report text, True when the PUBLIC channel is set and both bots are admin there)."""
        vals = await self._channel_values(uid)
        lines = ["<b>🔍 VERIFY BOTS ADMIN</b>\n"]
        any_set = False
        public_ok = False
        for kind, label, cid in (
            ("public", "📢 Public", vals["public_id"]), ("log", "🗄 Log", vals["log_id"]), ("db", "📚 Database", vals["db_id"])
        ):
            if not cid:
                lines.append(f"{label}: ⬜ not set")
                continue
            any_set = True
            if kind == "db":
                p = await self._bot_is_admin_problem(self._file_store_client(), cid)
                lines.append(f"{label} (<code>{cid}</code>): " + ("✅ File Store bot is admin" if not p else f"⚠️ File Store bot {p}"))
                continue
            problems = await self.check_both_bots(cid)
            lines.append(f"{label} (<code>{cid}</code>): " + ("✅ both bots are admin" if not problems else "⚠️ needs attention"))
            lines += [f"   {p}" for p in problems]
            if kind == "public" and not problems:
                public_ok = True
        if not any_set:
            lines.append("\nSet a channel first (🛠 ADVANCED → 📢 PUBLIC CHANNEL).")
        elif public_ok:
            lines.append("\n✅ <b>Your public channel is verified.</b>")
        else:
            lines.append("\nFix the points above (add the bot as ADMIN with <b>Post messages</b>), then tap VERIFY again.")
        return "\n".join(lines), public_ok

    async def build_verify_text(self, uid: int) -> str:
        return (await self._verify_report(uid))[0]

    async def build_status_text(self, user) -> str:
        uid = user.id
        cfg = await db.get_user_settings(uid)
        vals = await self._channel_values(uid)
        return (
            "<b>📊 MY STATUS</b>\n\n"
            f"🆔 <b>ID:</b> <code>{uid}</code>\n"
            f"💼 <b>Plan:</b> {await self._plan_line(uid)}\n\n"
            f"📥 <b>Uploader:</b> {html.escape(cfg.get('uploader_name') or 'Default')}\n"
            f"📢 <b>Join tag:</b> {html.escape(cfg.get('join_channel') or 'Default')}\n"
            f"🖼 <b>Thumbnail:</b> {'Custom ✅' if cfg.get('thumbnail_path') else 'Default'}\n\n"
            f"📢 <b>Public channel:</b> {self._chan_label(vals['public_id'], vals['public_title'])}\n"
            f"🗄 <b>Log channel:</b> {self._chan_label(vals['log_id'], vals['log_title'])}\n"
            f"🔄 <b>Auto upload:</b> {'ON ✅' if vals['auto'] is not False else 'OFF ❌'}\n\n"
            f"🎞 <b>Resolution filter:</b> {', '.join(cfg.get('res_filter') or []) or 'All'}\n"
            f"🔊 <b>Language filter:</b> {', '.join(cfg.get('lang_filter') or []) or 'All'}\n"
            f"📁 <b>Files shown:</b> {'ALL' if cfg.get('files_mode') == 'all' else 'Best per resolution'}\n\n"
            f"📬 <b>Posts sent to my channel:</b> {int(cfg.get('posts_total') or 0)}\n"
            f"🕒 <b>Last post:</b> {fmt_dt(cfg.get('last_post_at'))}"
        )

    def _register_handlers(self) -> None:
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("≼ BACK", callback_data="us|main")]])
        home_kb = InlineKeyboardMarkup([[InlineKeyboardButton("≼ BACK", callback_data="us|start")]])

        # /admin panel (ban / unban / paid / unpaid) - owner only, shared with the File Store bot
        self.admin_states = attach_admin_panel(self.app)

        @self.app.on_message(filters.command("start") & filters.private)
        @guarded
        async def start_cmd(client: Client, message: Message):
            user = message.from_user
            try:
                await db.register_user(user.id, user.first_name or "", user.username or "")
            except Exception as exc:
                logger.warning("register_user failed for %s: %s", user.id, exc)
            text, kb = await self.build_start(user)
            await reply_with_buttons(message, text, kb)

        @self.app.on_message(filters.command("settings") & filters.private)
        @guarded
        async def settings_cmd(client: Client, message: Message):
            await db.register_user(message.from_user.id, message.from_user.first_name or "", message.from_user.username or "")
            text, kb = await self.build_settings_menu(message.from_user.id)
            await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)

        @self.app.on_message(filters.command("clear") & filters.private)
        @guarded
        async def clear_cmd(client: Client, message: Message):
            uid = message.from_user.id
            self.user_states.pop(uid, None)
            rows = [[InlineKeyboardButton("🗑 YES, CLEAR MY DATA", callback_data="clr|me")]]
            if self._is_admin(uid):
                rows.append([InlineKeyboardButton("🗑 ALSO RESET MAIN-CHANNEL POST HISTORY", callback_data="clr|all")])
            rows.append([InlineKeyboardButton("❌ CANCEL", callback_data="clr|no")])
            await message.reply_text(
                "🗑 <b>CLEAR MY DATA</b>\n\n"
                "This deletes <b>only your own</b> setup so you start from scratch:\n"
                "• your public / log / database channel settings\n"
                "• uploader name, join tag, caption, footer, thumbnail\n"
                "• your resolution / language filters and preferences\n"
                "• the list of posts already sent to your channel (your next scrape posts everything again)\n\n"
                "Your plan (Premium) and the posts already in your channel are <b>not</b> touched.\n"
                + ("\n🛡 <b>Admin:</b> the second button also forgets the main-channel post history, so the next /scrape re-posts there too.\n"
                   if self._is_admin(uid) else "")
                + "\n<b>This cannot be undone.</b>",
                parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(rows),
            )

        @self.app.on_callback_query(filters.regex(r"^clr\|"))
        @guarded
        async def clear_callbacks(client: Client, cq: CallbackQuery):
            uid = cq.from_user.id
            action = cq.data.split("|", 1)[1]
            if action == "no":
                await cq.message.edit_text("❌ Cancelled. Nothing was changed.")
                return
            if action == "all" and not self._is_admin(uid):
                await cq.answer("🔒 Admin only.", show_alert=True)
                return
            self.user_states.pop(uid, None)
            res = await db.clear_user_data(uid)
            thumb = res.get("thumbnail")
            if thumb:
                try:
                    Path(thumb).unlink(missing_ok=True)
                except OSError:
                    pass
            text = (
                "✅ <b>Cleared.</b>\n"
                f"• settings removed: {res['settings']}\n• channel post records removed: {res['posts']}"
                + (f"\n• database-channel index removed: {res['indexed']}" if res["indexed"] else "")
            )
            if action == "all":
                main = await db.reset_main_posts()
                text += f"\n• main-channel post history removed: {main['posts']}"
            text += "\n\nSet your channels again in /settings → 🛠 ADVANCED, then ▶️ START SCRAPING."
            await cq.message.edit_text(text, parse_mode=ParseMode.HTML)

        @self.app.on_message(filters.command("scrape") & filters.private & filters.user(AutoUploaderConfig.OWNER_ID))
        @guarded
        async def scrape_cmd(client: Client, message: Message):
            status_msg = await message.reply_text("🔍 Scraping latest topics from 1TamilMV...")
            await status_msg.edit_text(await self.run_scrape(message.from_user.id))

        @self.app.on_message(filters.command("ingest") & filters.private & filters.user(AutoUploaderConfig.OWNER_ID))
        @guarded
        async def ingest_cmd(client: Client, message: Message):
            if message.reply_to_message and message.reply_to_message.document:
                doc_path = await message.reply_to_message.download()
                raw_text = Path(doc_path).read_text(encoding="utf-8")
                Path(doc_path).unlink(missing_ok=True)
            elif AutoUploaderConfig.INITIAL_JSON_PATH.exists():
                raw_text = AutoUploaderConfig.INITIAL_JSON_PATH.read_text(encoding="utf-8")
            else:
                await message.reply_text("Reply to a JSON file with /ingest or place sample_post.json in the root directory.")
                return
            await message.reply_text(await self.ingest_json_text(raw_text, message.from_user.id))

        # --- Settings menu callbacks (open to everyone; admin buttons are checked inside) ---

        @self.app.on_callback_query(filters.regex(r"^us\|"))
        @guarded
        async def settings_callbacks(client: Client, cq: CallbackQuery):
            uid = cq.from_user.id
            action = cq.data.split("|", 1)[1]
            admin = self._is_admin(uid)
            admin_actions = {"ingest", "ingest_sample", "scrape", "manual", "interval", "autopost"}

            if action in admin_actions and not admin:
                await cq.answer("🔒 Admin only.", show_alert=True)
                return

            if action == "start":
                self.user_states.pop(uid, None)
                text, kb = await self.build_start(cq.from_user)
                await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
                return

            if action.startswith("pf"):
                await self._prefs_callback(cq, uid, action)
                return

            if action == "quick":
                self.user_states.pop(uid, None)
                text, kb = await self.build_quick_start(uid)
                await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
                return

            if action == "premium":
                is_prem = await db.is_user_premium(uid) or self._is_admin(uid)
                text = (
                    "💸 <b>PREMIUM PLAN</b>\n\n"
                    f"💼 <b>Your plan:</b> {await self._plan_line(uid)}\n\n"
                    "✅ No link shortener / token wait in the File Store bot\n"
                    "✅ Priority torrent queueing\n"
                    "ℹ️ Own public &amp; log channel + your uploader name, tag, caption and thumbnail are free for everyone "
                    "(see 🛠 ADVANCED).\n\n"
                )
                text += "You already have everything unlocked 🎉" if is_prem else (
                    f"📩 Contact the admin to upgrade and send your ID: <code>{uid}</code>"
                )
                await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=home_kb)
                return

            if action == "status":
                await cq.message.edit_text(await self.build_status_text(cq.from_user), parse_mode=ParseMode.HTML, reply_markup=home_kb)
                return

            if action == "help":
                await cq.message.edit_text(
                    "❓ <b>HELP</b>\n\n"
                    "• <b>Settings</b> - set your uploader name, join tag, caption template and thumbnail.\n"
                    "• <b>Advanced Settings</b> - add your own <b>public channel</b> (posts) and <b>log channel</b>. "
                    "Both bots must be admin there. Premium users get ▶️ <b>START SCRAPING</b> once the channel is verified.\n"
                    "• Every new movie is posted automatically; file links open in the File Store bot, "
                    "which sends the file instantly if it is already stored.\n"
                    "• /start - this menu · /settings - settings",
                    parse_mode=ParseMode.HTML,
                    reply_markup=home_kb,
                )
                return

            # --- Advanced Settings (own public / log channel) ---
            if action == "adv":
                self.user_states.pop(uid, None)
                text, kb = await self.build_adv_menu(uid)
                await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                return

            if action in ("adv_db", "adv_set_db", "adv_clear_db", "adv_sync_db"):
                if not await self._can_use_advanced(uid):
                    await cq.answer("💎 Premium only. Open 💸 PREMIUM PLAN to upgrade.", show_alert=True)
                    return
                adv_back = InlineKeyboardMarkup([[InlineKeyboardButton("≼ BACK", callback_data="us|adv")]])
                vals = await self._channel_values(uid)
                if action == "adv_db":
                    cur = self._chan_label(vals["db_id"], vals["db_title"])
                    count = await db.count_db_channel_files(int(vals["db_id"])) if vals["db_id"] else 0
                    rows = [[
                        InlineKeyboardButton("✏️ SET", callback_data="us|adv_set_db"),
                        InlineKeyboardButton("🗑 REMOVE", callback_data="us|adv_clear_db"),
                    ]]
                    if vals["db_id"]:
                        rows.append([InlineKeyboardButton("🔍 FULL SCAN", callback_data="us|adv_sync_db")])
                    rows.append([InlineKeyboardButton("≼ BACK", callback_data="us|adv")])
                    await cq.message.edit_text(
                        "<b>📚 DATABASE CHANNEL</b>\n\n"
                        f"<b>Current:</b> {cur}\n📦 <b>Indexed files:</b> {count}\n\n"
                        "ℹ️ Put the files you already have in this channel. Before making a post I search it by "
                        "<b>name → year → quality</b> (size is ignored). If the release is there, the post link sends "
                        "that file directly - <b>no download</b>.\n\n"
                        f"Add <b>@{FileStoreConfig.BOT_USERNAME}</b> (File Store bot) as <b>ADMIN</b> of the channel first.",
                        parse_mode=ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup(rows),
                    )
                    return
                if action == "adv_set_db":
                    self.user_states[uid] = {"action": "adv_channel", "kind": "db"}
                    await cq.message.edit_text(
                        "✏️ <b>Send your Database channel</b> as <code>@username</code> or its <code>-100…</code> ID.\n\n"
                        f"First add <b>@{FileStoreConfig.BOT_USERNAME}</b> as <b>ADMIN</b> in that channel.",
                        parse_mode=ParseMode.HTML,
                        reply_markup=adv_back,
                    )
                    return
                if action == "adv_sync_db":
                    if not vals["db_id"]:
                        await cq.answer("Set a Database channel first.", show_alert=True)
                        return
                    await cq.answer("Full scan started…")
                    self._spawn(self._run_db_sync(uid, int(vals["db_id"]), cq.message))
                    return
                if action == "adv_clear_db":
                    if vals["db_id"]:
                        await db.clear_db_channel_files(int(vals["db_id"]))
                    await self._clear_channel(uid, "db")
                    try:
                        from file_store import file_store_bot
                        await file_store_bot.refresh_db_channels()
                    except Exception:
                        pass
                    await cq.answer("Removed.")
                    text, kb = await self.build_adv_menu(uid)
                    await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                    return

            if action in ("adv_pub", "adv_log", "adv_set_pub", "adv_set_log", "adv_clear_pub", "adv_clear_log", "adv_toggle"):
                if not await self._can_use_advanced(uid):
                    await cq.answer("💎 Premium only. Open 💸 PREMIUM PLAN to upgrade.", show_alert=True)
                    return
                adv_back = InlineKeyboardMarkup([[InlineKeyboardButton("≼ BACK", callback_data="us|adv")]])
                kind = "public" if action.endswith("pub") else "log"

                if action in ("adv_pub", "adv_log"):
                    vals = await self._channel_values(uid)
                    cur = self._chan_label(vals[f"{kind}_id"], vals[f"{kind}_title"])
                    kb = InlineKeyboardMarkup(
                        [
                            [
                                InlineKeyboardButton("✏️ SET", callback_data=f"us|adv_set_{action[-3:]}"),
                                InlineKeyboardButton("🗑 REMOVE", callback_data=f"us|adv_clear_{action[-3:]}"),
                            ],
                            [InlineKeyboardButton("≼ BACK", callback_data="us|adv")],
                        ]
                    )
                    what = "posts are auto-uploaded here" if kind == "public" else "post activity is logged here"
                    await cq.message.edit_text(
                        f"<b>{'📢 PUBLIC' if kind == 'public' else '🗄 LOG'} CHANNEL</b>\n\n"
                        f"<b>Current:</b> {cur}\n\n"
                        f"ℹ️ {what}.\nBoth bots must be <b>ADMIN</b> in it: {self._both_bots_text()}",
                        parse_mode=ParseMode.HTML,
                        reply_markup=kb,
                    )
                    return

                if action in ("adv_set_pub", "adv_set_log"):
                    self.user_states[uid] = {"action": "adv_channel", "kind": kind}
                    await cq.message.edit_text(
                        f"✏️ <b>Send your {kind} channel</b> as <code>@username</code> or its <code>-100…</code> ID.\n\n"
                        f"First add {self._both_bots_text()} as <b>ADMIN</b> (with post rights) in the channel.",
                        parse_mode=ParseMode.HTML,
                        reply_markup=adv_back,
                    )
                    return

                if action in ("adv_clear_pub", "adv_clear_log"):
                    await self._clear_channel(uid, kind)
                    await cq.answer("Removed.")
                    text, kb = await self.build_adv_menu(uid)
                    await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                    return

                if action == "adv_toggle":
                    if self._is_admin(uid):
                        await cq.answer("The admin channel always posts.", show_alert=True)
                        return
                    cfg = await db.get_user_settings(uid)
                    await db.update_user_settings(uid, {"auto_post_enabled": not cfg.get("auto_post_enabled", True)})
                    text, kb = await self.build_adv_menu(uid)
                    await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
                    return

            if action == "adv_verify":
                await cq.answer("Checking…")
                report, public_ok = await self._verify_report(uid)
                rows = []
                if public_ok and await self._can_scrape(uid):
                    rows.append([InlineKeyboardButton("▶️ START SCRAPING", callback_data="us|adv_scrape")])
                rows.append([InlineKeyboardButton("≼ BACK", callback_data="us|adv")])
                await cq.message.edit_text(report, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(rows))
                return

            if action == "adv_scrape":
                if not await self._can_scrape(uid):
                    await cq.answer("💎 Scraping for your own channel is a Premium feature. Open 💎 PREMIUM to upgrade.", show_alert=True)
                    return
                if uid in self._scraping:
                    await cq.answer("⏳ A scrape is already running. The result appears here when it is done.", show_alert=True)
                    return
                wait = await self._scrape_wait_left(uid)
                if wait:
                    await cq.answer(f"⏱ Please wait {format_duration(wait)} before scraping again.", show_alert=True)
                    return
                vals = await self._channel_values(uid)
                if not vals["public_id"]:
                    await cq.answer("📢 Set your public channel first (PUBLIC CHANNEL).", show_alert=True)
                    return
                await cq.answer("Verifying your channel…")
                problems = await self.check_both_bots(vals["public_id"])
                if problems:
                    fix_kb = InlineKeyboardMarkup(
                        [
                            [InlineKeyboardButton("🔍 VERIFY BOTS", callback_data="us|adv_verify")],
                            [InlineKeyboardButton("≼ BACK", callback_data="us|adv")],
                        ]
                    )
                    await cq.message.edit_text(
                        "⚠️ <b>Channel not verified - scraping did not start</b>\n\n"
                        + "\n".join(problems)
                        + f"\n\nAdd {self._both_bots_text()} as <b>ADMIN</b> with <b>Post messages</b>, then try again.",
                        parse_mode=ParseMode.HTML,
                        reply_markup=fix_kb,
                    )
                    return
                self._scraping.add(uid)
                try:
                    await db.update_user_settings(uid, {"last_scrape_at": utcnow()})
                    await cq.message.edit_text(
                        "✅ <b>Channel verified</b> - both bots are admin.\n\n"
                        "🔍 <b>Scraping started…</b>\n"
                        "Fetching the newest releases and sending them to your channel. This can take a few minutes; "
                        "the result appears here when it is done.",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    self._scraping.discard(uid)
                    raise
                self._spawn(self._scrape_job(uid, cq.message))
                return

            if action == "main":
                self.user_states.pop(uid, None)
                text, kb = await self.build_settings_menu(uid)
                await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
                return

            if action == "ingest":
                self.user_states[uid] = {"action": "ingest_file"}
                kb = InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("📄 Use sample_post.json", callback_data="us|ingest_sample")],
                        [InlineKeyboardButton("≼ BACK", callback_data="us|main")],
                    ]
                )
                await cq.message.edit_text(
                    "📥 <b>INGEST</b>\n\nSend a <b>JSON file</b> (or paste JSON text) with movie posts, "
                    "or tap the button to use <code>sample_post.json</code>.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb,
                )
                return

            if action == "ingest_sample":
                self.user_states.pop(uid, None)
                if not AutoUploaderConfig.INITIAL_JSON_PATH.exists():
                    await cq.answer("sample_post.json not found.", show_alert=True)
                    return
                raw = AutoUploaderConfig.INITIAL_JSON_PATH.read_text(encoding="utf-8")
                await cq.answer("Ingesting...")
                await cq.message.edit_text(await self.ingest_json_text(raw, uid), reply_markup=back_kb)
                return

            if action == "scrape":
                await cq.answer("Scraping...")
                await cq.message.edit_text("🔍 Scraping latest topics from 1TamilMV...")
                await cq.message.edit_text(await self.run_scrape(uid), reply_markup=back_kb)
                return

            if action == "manual":
                glob = await db.get_bot_settings("auto_uploader")
                await db.update_bot_settings("auto_uploader", {"manual_edit_mode": not glob.get("manual_edit_mode", False)})
                text, kb = await self.build_settings_menu(uid)
                await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
                return

            if action == "autopost":
                glob = await db.get_bot_settings("auto_uploader")
                turn_on = glob.get("auto_post_enabled", True) is False  # currently OFF -> switch ON
                await db.update_bot_settings("auto_uploader", {"auto_post_enabled": turn_on})
                await cq.answer("🤖 Auto post ON - catching up on missed posts…" if turn_on else "🤖 Auto post OFF - nothing is posted until you switch it ON.", show_alert=True)
                if turn_on:
                    self._spawn(self.catch_up_posts())
                text, kb = await self.build_settings_menu(uid)
                await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
                return

            if action == "interval":
                self.user_states[uid] = {"action": "set_interval"}
                await cq.message.edit_text(
                    "⏱ <b>Send the post interval in seconds</b> (e.g. <code>300</code> for 5 minutes):",
                    parse_mode=ParseMode.HTML,
                    reply_markup=back_kb,
                )
                return

            if action == "thumb":
                cfg = await db.get_user_settings(uid)
                kb = InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("🖼 Set Thumbnail", callback_data="us|set_thumb")],
                        [InlineKeyboardButton("🗑 Remove Thumbnail", callback_data="us|clear_thumb")],
                        [InlineKeyboardButton("≼ BACK", callback_data="us|main")],
                    ]
                )
                await cq.message.edit_text(
                    f"🖼 <b>THUMBNAIL</b>\n\nStatus: {'Custom ✅ (used as the post poster)' if cfg.get('thumbnail_path') else 'Not set (scraped poster is used)'}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb,
                )
                return

            if action == "set_thumb":
                self.user_states[uid] = {"action": "set_thumb"}
                await cq.message.edit_text("🖼 <b>Send a photo</b> to use as your thumbnail:", parse_mode=ParseMode.HTML, reply_markup=back_kb)
                return

            if action == "clear_thumb":
                cfg = await db.get_user_settings(uid)
                if cfg.get("thumbnail_path"):
                    Path(cfg["thumbnail_path"]).unlink(missing_ok=True)
                await db.update_user_settings(uid, unset=["thumbnail_path"])
                await cq.answer("Thumbnail removed.")
                text, kb = await self.build_settings_menu(uid)
                await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
                return

            if action.startswith(("f_", "set_", "reset_")):
                prefix, _, key = action.partition("_")
                if key in self.USER_FIELDS:
                    field, title, prompt = self.USER_FIELDS[key]
                    if prefix == "f":
                        cfg = await db.get_user_settings(uid)
                        kb = InlineKeyboardMarkup(
                            [
                                [InlineKeyboardButton("✏️ Set", callback_data=f"us|set_{key}")],
                                [InlineKeyboardButton("🔄 Reset Default", callback_data=f"us|reset_{key}")],
                                [InlineKeyboardButton("≼ BACK", callback_data="us|main")],
                            ]
                        )
                        cur = cfg.get(field)
                        await cq.message.edit_text(
                            f"<b>{title}</b>\n\n<b>Current:</b>\n{cur if cur else 'Default'}",
                            parse_mode=ParseMode.HTML,
                            reply_markup=kb,
                            disable_web_page_preview=True,
                        )
                    elif prefix == "set":
                        self.user_states[uid] = {"action": "set_field", "field": field}
                        await cq.message.edit_text(prompt, parse_mode=ParseMode.HTML, reply_markup=back_kb)
                    else:
                        await db.update_user_settings(uid, unset=[field])
                        await cq.answer("Reset to default.")
                        text, kb = await self.build_settings_menu(uid)
                        await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)

        # --- Manual review callbacks (admin only) ---

        @self.app.on_callback_query(filters.regex(r"^(edit|approve)\|") & filters.user(AutoUploaderConfig.OWNER_ID))
        @guarded
        async def review_callbacks(client: Client, cq: CallbackQuery):
            data = cq.data
            if data.startswith("edit|"):
                _, movie_id, field = data.split("|", 2)
                self.pending_edits[cq.from_user.id] = {"movie_id": movie_id, "field": field}
                await cq.message.reply_text(
                    f"✏️ Send the new value for <b>{field}</b> (for movie <code>{movie_id}</code>):",
                    parse_mode=ParseMode.HTML,
                )
                await cq.answer()
                return

            if data.startswith("approve|"):
                _, movie_id = data.split("|", 1)
                settings = await db.get_bot_settings("auto_uploader")
                interval = settings.get("post_interval_seconds", AutoUploaderConfig.POST_INTERVAL_SECONDS)
                sched_time = await db.schedule_post(movie_id, interval)
                await cq.answer("Approved & added to 5-minute queue!")
                await cq.message.reply_text(
                    f"✅ Movie <code>{movie_id}</code> scheduled for <b>{sched_time.strftime('%Y-%m-%d %H:%M:%S UTC')}</b>",
                    parse_mode=ParseMode.HTML,
                )

        # --- Input handlers (text / photo / document) ---

        @self.app.on_message(filters.text & filters.private & ~filters.regex(r"^/"))
        @guarded
        async def handle_text_input(client: Client, message: Message):
            uid = message.from_user.id
            text = message.text.strip()

            # Admin: manual review field edit
            if uid in self.pending_edits and self._is_admin(uid):
                state = self.pending_edits.pop(uid)
                movie_id, field = state["movie_id"], state["field"]
                movie_doc = await db.get_movie(movie_id)
                if not movie_doc:
                    await message.reply_text("Movie not found.")
                    return
                movie_doc[field] = text
                if field == "image_url":
                    new_poster = await self.download_poster(text, movie_id, referer=movie_doc.get("source_url"), force=True)
                    if new_poster:
                        movie_doc["poster_path"] = str(new_poster)
                    else:
                        await message.reply_text("⚠️ That URL is not a downloadable image - the old poster is kept.")
                if field == "uploader":
                    for tor in await db.get_torrents_by_movie(movie_id):
                        tor["display_name"] = re.sub(r"^@\S+\s*-\s*", f"{text} - ", tor["display_name"])
                        await db.save_torrent(tor)
                await db.save_movie(movie_doc)
                await message.reply_text(f"✅ Updated <b>{field}</b>. Generating updated preview...", parse_mode=ParseMode.HTML)
                await self._send_owner_review_prompt(movie_id, chat_id=message.chat.id)
                return

            state = self.user_states.get(uid)
            if not state:
                await message.reply_text("ℹ️ Use /settings to customize your uploader.")
                return
            act = state["action"]

            if act == "adv_channel":
                self.user_states.pop(uid, None)
                if not await self._can_use_advanced(uid):
                    await message.reply_text("💎 Premium only.")
                    return
                await self._save_channel(message, uid, state.get("kind", "public"), text)
                return

            if act == "set_field":
                self.user_states.pop(uid, None)
                await db.update_user_settings(uid, {state["field"]: text})
                await message.reply_text("✅ Saved. Use /settings to review.")
                return

            if act == "set_interval":
                if not self._is_admin(uid):
                    self.user_states.pop(uid, None)
                    return
                if not text.isdigit() or int(text) < 5:
                    await message.reply_text("❌ Send a number of seconds (minimum 5).")
                    return
                self.user_states.pop(uid, None)
                await db.update_bot_settings("auto_uploader", {"post_interval_seconds": int(text)})
                await message.reply_text(f"✅ Post interval set to <b>{text}s</b>.", parse_mode=ParseMode.HTML)
                return

            if act == "ingest_file":
                if not self._is_admin(uid):
                    self.user_states.pop(uid, None)
                    return
                self.user_states.pop(uid, None)
                await message.reply_text(await self.ingest_json_text(text, uid))

        @self.app.on_message(filters.photo & filters.private)
        @guarded
        async def handle_photo_input(client: Client, message: Message):
            uid = message.from_user.id
            state = self.user_states.get(uid)
            if not state or state["action"] != "set_thumb":
                return
            self.user_states.pop(uid, None)
            thumb_dir = AutoUploaderConfig.POSTER_DIR / "user_thumbs"
            thumb_dir.mkdir(parents=True, exist_ok=True)
            dest = thumb_dir / f"{uid}.jpg"
            await message.download(file_name=str(dest))
            await db.update_user_settings(uid, {"thumbnail_path": str(dest)})
            await message.reply_text("✅ Thumbnail saved. It will be used as the poster for your posts.")

        @self.app.on_message(filters.document & filters.private)
        @guarded
        async def handle_document_input(client: Client, message: Message):
            uid = message.from_user.id
            state = self.user_states.get(uid)
            if not state or state["action"] != "ingest_file" or not self._is_admin(uid):
                return
            self.user_states.pop(uid, None)
            doc_path = await message.download()
            raw_text = Path(doc_path).read_text(encoding="utf-8")
            Path(doc_path).unlink(missing_ok=True)
            await message.reply_text(await self.ingest_json_text(raw_text, uid))


auto_uploader_bot = AutoUploaderBot()
