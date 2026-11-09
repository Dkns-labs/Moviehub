import io
import time
import html
import uuid
import asyncio
import logging
from typing import Any, Dict, List, Optional

from pyrogram import Client, filters
from pyrogram.enums import ChatType, ParseMode
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand,
)

from config import TorrentSearchConfig, TelegramCommonConfig, FileStoreConfig
import db_channel
from torrent_search import TorrentSearch, SearchOutcome, TorrentResult, human_size

logger = logging.getLogger("torrent_search_bot")

HELP_TEXT = (
    "🔎 <b>TORRENT SEARCH</b>\n"
    "<i>One message, {n} torrent sites.</i>\n\n"
    "{how}\n\n"
    "<b>What you get</b>\n"
    "• Results from all sites at once, best seeded first, duplicates merged\n"
    "• Tap a result for size, seeders and the site\n"
    "• 🧲 <b>Magnet</b> - copy it into any torrent client\n"
    "• 📄 <b>.torrent file</b> - sent to you as a file\n\n"
    "/sites - list of searched sites"
)


class TorrentSearchBot:
    def __init__(self) -> None:
        self.engine = TorrentSearch()
        self.app: Optional[Client] = None
        self._sessions: Dict[str, Dict[str, Any]] = {}   # sid -> {uid, outcome, query, created}
        self._last_search: Dict[int, float] = {}          # uid -> time of last search (cooldown)
        self._busy: set = set()                           # uids with a search running

    # ------------------------------------------------------------------ #
    # lifecycle (own bot, only when TORRENT_SEARCH_BOT_TOKEN is set)
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        if not TorrentSearchConfig.BOT_TOKEN:
            logger.info(
                "TORRENT_SEARCH_BOT_TOKEN not set: /search <name> works inside the File Store bot instead (%d sites).",
                len(self.engine.providers),
            )
            return
        from auto_uploader import session_name  # lazy: auto_uploader is a big module
        TelegramCommonConfig.SESSION_DIR.mkdir(parents=True, exist_ok=True)
        self.app = Client(
            session_name("torrent_search_bot", TorrentSearchConfig.BOT_TOKEN),
            api_id=TelegramCommonConfig.API_ID,
            api_hash=TelegramCommonConfig.API_HASH,
            bot_token=TorrentSearchConfig.BOT_TOKEN,
            workdir=str(TelegramCommonConfig.SESSION_DIR),
        )
        self.attach(self.app, standalone=True)
        await self.app.start()
        me = getattr(self.app, "me", None) or await self.app.get_me()
        try:
            await self.app.set_bot_commands(
                [
                    BotCommand("start", "WHAT I CAN DO"),
                    BotCommand("search", "SEARCH TORRENTS: /search NAME"),
                    BotCommand("sites", "LIST OF SEARCHED SITES"),
                ]
            )
        except Exception as exc:
            logger.warning("Could not set Torrent Search commands: %s", exc)
        logger.info("Torrent Search Bot started (@%s, %d sites).", getattr(me, "username", "?"), len(self.engine.providers))

    async def stop(self) -> None:
        try:
            if self.app:
                await self.app.stop()
        except Exception:
            pass
        await self.engine.close()

    # ------------------------------------------------------------------ #
    # search + rendering
    # ------------------------------------------------------------------ #

    def _purge(self) -> None:
        now = time.time()
        for sid in [s for s, v in self._sessions.items() if now - v["created"] > TorrentSearchConfig.SESSION_TTL_SECONDS]:
            self._sessions.pop(sid, None)
        if len(self._sessions) > 300:  # hard cap: drop the oldest
            for sid, _ in sorted(self._sessions.items(), key=lambda kv: kv[1]["created"])[: len(self._sessions) - 300]:
                self._sessions.pop(sid, None)

    @staticmethod
    def _short(text: str, n: int = 78) -> str:
        return text if len(text) <= n else text[: n - 1] + "…"

    def _page_text(self, sess: Dict[str, Any], page: int) -> str:
        out: SearchOutcome = sess["outcome"]
        size = TorrentSearchConfig.PAGE_SIZE
        pages = max(1, (len(out.results) + size - 1) // size)
        page = min(max(page, 0), pages - 1)
        ok_sites = sum(1 for r in out.report if r.ok)
        lines = [
            f"🔎 <b>{html.escape(out.query)}</b>",
            f"📊 {len(out.results)} results · {ok_sites}/{len(out.report)} sites · {out.seconds:.1f}s\n",
        ]
        for i, r in enumerate(out.results[page * size:(page + 1) * size], start=page * size + 1):
            also = f" (+{len(r.sources)})" if r.sources else ""
            lines.append(f"<b>{i}.</b> {html.escape(self._short(r.title))}")
            lines.append(f"      💾 {human_size(r.size)} · 🌱 {r.seeders} · 🔻 {r.leechers} · 🌐 {html.escape(r.site)}{also}")
        lines.append(f"\n📄 Page {page + 1}/{pages} · tap a number")
        return "\n".join(lines)

    def _page_kb(self, sid: str, sess: Dict[str, Any], page: int) -> InlineKeyboardMarkup:
        out: SearchOutcome = sess["outcome"]
        size = TorrentSearchConfig.PAGE_SIZE
        pages = max(1, (len(out.results) + size - 1) // size)
        page = min(max(page, 0), pages - 1)
        start = page * size
        numbers = [
            InlineKeyboardButton(str(i + 1), callback_data=f"ts|it|{sid}|{i}")
            for i in range(start, min(start + size, len(out.results)))
        ]
        rows = [numbers]
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("◀ PREV", callback_data=f"ts|pg|{sid}|{page - 1}"))
        nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data=f"ts|pg|{sid}|{page}"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton("NEXT ▶", callback_data=f"ts|pg|{sid}|{page + 1}"))
        rows.append(nav)
        rows.append(
            [
                InlineKeyboardButton("🌐 SITES", callback_data=f"ts|rp|{sid}|{page}"),
                InlineKeyboardButton("❌ CLOSE", callback_data="ts|cl"),
            ]
        )
        return InlineKeyboardMarkup(rows)

    async def _show_db_hits(self, message: Message, query: str, hits: List[Dict[str, Any]], uid: int) -> None:
        """Step 1 of every search: the file is already in a Database channel -> offer it directly (no torrent needed)."""
        self._purge()
        sid = uuid.uuid4().hex[:8]
        self._sessions[sid] = {"uid": uid, "kind": "db", "query": query, "created": time.time()}
        bot = FileStoreConfig.BOT_USERNAME
        lines = [f"📚 <b>Found in the database</b> for <b>{html.escape(query)}</b>", "<i>Tap a file - it is sent instantly, no download.</i>\n"]
        rows = []
        for i, h in enumerate(hits, 1):
            parts = f" · 🧩 {h['parts']} parts" if h["parts"] > 1 else ""
            lines.append(f"<b>{i}.</b> {html.escape(self._short(h['name']))}")
            lines.append(f"      💿 {h['resolution'] or 'HD'} · 💾 {human_size(h['size_bytes'])}{parts}")
            rows.append([InlineKeyboardButton(
                f"📥 {i}. {h['resolution'] or 'HD'} · {human_size(h['size_bytes'])}",
                url=f"https://t.me/{bot}?start=dbf_{h['file_id']}",
            )])
        rows.append([
            InlineKeyboardButton("🔎 SEARCH TORRENT SITES TOO", callback_data=f"ts|web|{sid}|0"),
            InlineKeyboardButton("❌ CLOSE", callback_data="ts|cl"),
        ])
        await message.reply_text(
            "\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(rows), disable_web_page_preview=True
        )

    async def _search_and_show(self, message: Message, query: str, uid: Optional[int] = None, skip_db: bool = False) -> None:
        uid = uid or message.from_user.id
        query = " ".join(query.split())
        if len(query) < 2:
            await message.reply_text("🔎 Send at least 2 characters, e.g. <code>/search ubuntu 24.04</code>", parse_mode=ParseMode.HTML)
            return
        if uid in self._busy:
            await message.reply_text("⏳ Your previous search is still running. Please wait a moment.")
            return
        wait = TorrentSearchConfig.COOLDOWN_SECONDS - (time.time() - self._last_search.get(uid, 0))
        if wait > 0 and uid != TelegramCommonConfig.OWNER_ID and not skip_db:
            await message.reply_text(f"⏱ Please wait {int(wait) + 1}s before the next search.")
            return
        self._busy.add(uid)
        self._last_search[uid] = time.time()
        if not skip_db:  # DATABASE CHANNEL FIRST: the same file name may already be stored -> send it, skip the web search
            try:
                hits = await db_channel.search_files(uid, query)
            finally:
                self._busy.discard(uid)
            if hits:
                await self._show_db_hits(message, query, hits, uid)
                return
            self._busy.add(uid)
        status = await message.reply_text(
            f"🔎 Searching <b>{len(self.engine.providers)}</b> sites for <b>{html.escape(query)}</b>…\n<i>This takes a few seconds.</i>",
            parse_mode=ParseMode.HTML,
        )
        try:
            outcome = await self.engine.search(query, limit=TorrentSearchConfig.MAX_RESULTS)
        except Exception as exc:
            logger.exception("Torrent search failed for %r: %s", query, exc)
            await status.edit_text("⚠️ The search failed. Please try again in a moment.")
            return
        finally:
            self._busy.discard(uid)

        self._purge()
        sid = uuid.uuid4().hex[:8]
        sess = {"uid": uid, "outcome": outcome, "created": time.time()}
        self._sessions[sid] = sess

        if not outcome.results:
            ok = sum(1 for r in outcome.report if r.ok)
            await status.edit_text(
                f"❌ <b>No results</b> for <b>{html.escape(query)}</b>\n"
                f"{ok}/{len(outcome.report)} sites answered.\n\n"
                "Try fewer or different words (e.g. just the movie name, no year).",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🌐 SITES", callback_data=f"ts|rp|{sid}|0"), InlineKeyboardButton("❌ CLOSE", callback_data="ts|cl")]]
                ),
            )
            return

        await status.edit_text(
            self._page_text(sess, 0),
            parse_mode=ParseMode.HTML,
            reply_markup=self._page_kb(sid, sess, 0),
            disable_web_page_preview=True,
        )

    def _detail(self, r: TorrentResult, idx: int) -> str:
        lines = [
            f"🎬 <b>{html.escape(r.title)}</b>\n",
            f"💾 <b>Size:</b> {human_size(r.size)}",
            f"🌱 <b>Seeders:</b> {r.seeders}   🔻 <b>Leechers:</b> {r.leechers}",
            f"🌐 <b>Site:</b> {html.escape(r.site)}",
        ]
        if r.sources:
            lines.append(f"🔁 <b>Also on:</b> {html.escape(', '.join(r.sources[:6]))}")
        if r.date:
            lines.append(f"📅 <b>Date:</b> {html.escape(r.date)}")
        if r.category:
            lines.append(f"🏷 <b>Category:</b> {html.escape(r.category)}")
        if r.infohash:
            lines.append(f"🔑 <b>Hash:</b> <code>{r.infohash}</code>")
        return "\n".join(lines)

    # ------------------------------------------------------------------ #
    # handlers
    # ------------------------------------------------------------------ #

    def attach(self, client: Client, standalone: bool = False) -> None:
        from auto_uploader import guarded  # lazy import (auto_uploader imports a lot)

        n = len(self.engine.providers)

        if standalone:
            @client.on_message(filters.command(["start", "help"]))
            @guarded
            async def ts_start(c: Client, message: Message):
                if message.from_user is None:
                    return
                how = "Just <b>type what you are looking for</b> (a movie, a show, a game, a Linux ISO…) and send it."
                await message.reply_text(HELP_TEXT.format(n=n, how=how), parse_mode=ParseMode.HTML)

        @client.on_message(filters.command(["search", "ts", "torrent"]))
        @guarded
        async def ts_search(c: Client, message: Message):
            if message.from_user is None:
                return
            query = " ".join(message.command[1:]).strip()
            if not query and message.reply_to_message and message.reply_to_message.text:
                query = message.reply_to_message.text.strip()
            if not query:
                await message.reply_text(
                    "🔎 <b>Usage:</b> <code>/search movie or show name</code>\nExample: <code>/search ubuntu 24.04</code>",
                    parse_mode=ParseMode.HTML,
                )
                return
            await self._search_and_show(message, query)

        @client.on_message(filters.command("sites"))
        @guarded
        async def ts_sites(c: Client, message: Message):
            names = "\n".join(f"• {html.escape(p.name)}" for p in self.engine.providers)
            await message.reply_text(f"🌐 <b>{n} SITES SEARCHED AT ONCE</b>\n\n{names}", parse_mode=ParseMode.HTML)

        @client.on_message(filters.command("sitetest"))
        @guarded
        async def ts_sitetest(c: Client, message: Message):
            if message.from_user is None or message.from_user.id != TelegramCommonConfig.OWNER_ID:
                return
            note = await message.reply_text("🧪 Testing every site with the query <code>ubuntu</code>…", parse_mode=ParseMode.HTML)
            out = await self.engine.search("ubuntu", limit=200)
            lines = [
                f"{'✅' if r.ok else '❌'} <b>{html.escape(r.name)}</b> - {r.count} results, {r.seconds:.1f}s"
                + (f" <i>({html.escape(r.error)})</i>" if r.error else "")
                for r in out.report
            ]
            await note.edit_text("🧪 <b>SITE TEST</b>\n\n" + "\n".join(lines), parse_mode=ParseMode.HTML)

        if standalone:
            # Plain text = a search. Registered AFTER the command handlers, so commands never reach it.
            @client.on_message(filters.text)
            @guarded
            async def ts_text(c: Client, message: Message):
                if message.from_user is None or message.chat.type != ChatType.PRIVATE:
                    return
                text = (message.text or "").strip()
                if not text or text.startswith("/"):
                    return
                await self._search_and_show(message, text)

        @client.on_callback_query(filters.regex(r"^ts\|"))
        @guarded
        async def ts_callbacks(c: Client, cq: CallbackQuery):
            parts = cq.data.split("|")
            action = parts[1] if len(parts) > 1 else ""
            if action == "cl":
                try:
                    await cq.message.delete()
                except Exception:
                    await cq.answer("Closed.")
                return

            sid = parts[2] if len(parts) > 2 else ""
            sess = self._sessions.get(sid)
            if not sess:
                await cq.answer("⌛ This result list expired. Please search again.", show_alert=True)
                return
            if sess["uid"] != cq.from_user.id:
                await cq.answer("🔒 This is another user's search. Send your own search.", show_alert=True)
                return
            if sess.get("kind") == "db":
                if action == "web":
                    await cq.answer("🔎 Searching the torrent sites…")
                    await self._search_and_show(cq.message, sess["query"], uid=cq.from_user.id, skip_db=True)
                else:
                    await cq.answer()
                return
            out: SearchOutcome = sess["outcome"]
            size = TorrentSearchConfig.PAGE_SIZE
            arg = int(parts[3]) if len(parts) > 3 and parts[3].lstrip("-").isdigit() else 0

            if action == "pg":
                await cq.message.edit_text(
                    self._page_text(sess, arg), parse_mode=ParseMode.HTML,
                    reply_markup=self._page_kb(sid, sess, arg), disable_web_page_preview=True,
                )
                return

            if action == "rp":
                lines = [
                    f"{'✅' if r.ok else '❌'} {html.escape(r.name)} - {r.count} ({r.seconds:.1f}s)"
                    + (f" <i>{html.escape(r.error)}</i>" if r.error else "")
                    for r in out.report
                ]
                back = (
                    [InlineKeyboardButton("≼ BACK", callback_data=f"ts|pg|{sid}|{arg}")]
                    if out.results
                    else [InlineKeyboardButton("❌ CLOSE", callback_data="ts|cl")]
                )
                await cq.message.edit_text(
                    "🌐 <b>SITE REPORT</b>\n<i>✅ answered · ❌ down, blocked or no results</i>\n\n" + "\n".join(lines),
                    parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([back]),
                )
                return

            if not 0 <= arg < len(out.results):
                await cq.answer("Result not found.", show_alert=True)
                return
            r = out.results[arg]

            if action == "it":
                page = arg // size
                rows = [[
                    InlineKeyboardButton("🧲 MAGNET", callback_data=f"ts|mg|{sid}|{arg}"),
                    InlineKeyboardButton("📄 .TORRENT", callback_data=f"ts|tf|{sid}|{arg}"),
                ]]
                if r.detail_url.startswith("http"):
                    rows.append([InlineKeyboardButton("🔗 OPEN PAGE", url=r.detail_url)])
                rows.append([InlineKeyboardButton("≼ BACK", callback_data=f"ts|pg|{sid}|{page}")])
                await cq.message.edit_text(
                    self._detail(r, arg), parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(rows), disable_web_page_preview=True,
                )
                return

            if action == "mg":
                await cq.answer("🧲 Getting the magnet link…")
                try:
                    await self.engine.resolve(r)
                except Exception as exc:
                    logger.info("resolve failed for %s: %s", r.title, exc)
                if not r.magnet:
                    await cq.answer("❌ This site did not give a magnet link. Try 📄 .TORRENT or 🔗 OPEN PAGE.", show_alert=True)
                    return
                await cq.message.reply_text(
                    f"🧲 <b>{html.escape(self._short(r.title, 100))}</b>\n\n<code>{html.escape(r.magnet)}</code>\n\n"
                    "<i>Tap the link to copy it, then open it in your torrent client.</i>",
                    parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                )
                return

            if action == "tf":
                await cq.answer("📄 Getting the .torrent file…")
                try:
                    await self.engine.resolve(r)
                except Exception as exc:
                    logger.debug("resolve before .torrent failed: %s", exc)
                got = await self.engine.fetch_torrent_file(r)
                if not got:
                    await cq.answer("❌ No .torrent file is available for this one. Use 🧲 MAGNET instead.", show_alert=True)
                    return
                filename, raw = got
                bio = io.BytesIO(raw)
                bio.name = filename
                await cq.message.reply_document(
                    bio, caption=f"📄 <b>{html.escape(self._short(r.title, 150))}</b>\n💾 {human_size(r.size)} · 🌐 {html.escape(r.site)}",
                    parse_mode=ParseMode.HTML,
                )
                return


torrent_search_bot = TorrentSearchBot()
