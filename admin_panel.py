"""/admin panel shared by the File Store bot and the Auto Uploader bot (same MongoDB, same users).

Owner only:  /admin  ->  BAN / UNBAN / PAID / UNPAID / USER INFO.
The owner taps an action, sends the user ID, then (for ban and paid) picks the duration with a button or types
a custom number of days. The user gets a message about it (if the bot is able to message them).
"""
import html
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config import TelegramCommonConfig
from database import db, utcnow

logger = logging.getLogger("admin_panel")

# (button label, days)  - 0 = permanent for bans, -1 = lifetime for paid plans
BAN_DURATIONS = [("1 Day", 1), ("3 Days", 3), ("7 Days", 7), ("30 Days", 30), ("90 Days", 90), ("♾ Permanent", 0)]
PAID_DURATIONS = [("7 Days", 7), ("30 Days", 30), ("90 Days", 90), ("180 Days", 180), ("365 Days", 365), ("♾ Lifetime", -1)]


def _aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is not None and dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def fmt_dt(dt: Optional[datetime]) -> str:
    dt = _aware(dt)
    return dt.strftime("%Y-%m-%d %H:%M UTC") if dt else "—"


def fmt_left(dt: Optional[datetime]) -> str:
    dt = _aware(dt)
    if not dt:
        return "∞"
    secs = max(0, int((dt - utcnow()).total_seconds()))
    days, rem = divmod(secs, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def format_ban_notice(ban: Dict[str, Any]) -> str:
    until = ban.get("until")
    duration = f"until {fmt_dt(until)} ({fmt_left(until)} left)" if until else "permanent"
    return (
        "🚫 <b>You are banned from using this bot.</b>\n\n"
        f"⏳ <b>Duration:</b> {duration}\n\n"
        "Contact the admin if you think this is a mistake."
    )


def _duration_keyboard(prefix: str, uid: int, options, custom_cb: str) -> InlineKeyboardMarkup:
    rows, row = [], []
    for label, days in options:
        row.append(InlineKeyboardButton(label, callback_data=f"adm|{prefix}|{uid}|{days}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("✏️ Custom days", callback_data=f"adm|{custom_cb}|{uid}")])
    rows.append([InlineKeyboardButton("≼ CANCEL", callback_data="adm|home")])
    return InlineKeyboardMarkup(rows)


async def build_panel():
    stats = await db.get_user_stats()
    text = (
        "🛡 <b>ADMIN PANEL</b>\n\n"
        f"👥 <b>Users:</b> {stats['total']}\n"
        f"💎 <b>Paid:</b> {stats['paid']}\n"
        f"🚫 <b>Banned:</b> {stats['banned']}\n"
        f"📢 <b>Users with own channel:</b> {stats.get('channels', 0)}\n\n"
        "Pick an action, then send the <b>User ID</b>."
    )
    kb = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🚫 BAN USER", callback_data="adm|ban"), InlineKeyboardButton("✅ UNBAN USER", callback_data="adm|unban")],
            [InlineKeyboardButton("💎 SET PAID", callback_data="adm|paid"), InlineKeyboardButton("🆓 SET UNPAID", callback_data="adm|unpaid")],
            [InlineKeyboardButton("💎 PAID LIST", callback_data="adm|plist"), InlineKeyboardButton("🚫 BANNED LIST", callback_data="adm|blist")],
            [InlineKeyboardButton("🔍 USER INFO", callback_data="adm|info"), InlineKeyboardButton("❌ CLOSE", callback_data="adm|close")],
        ]
    )
    return text, kb


async def build_user_list(kind: str) -> str:
    """Active paid / banned users (newest first, max 30)."""
    docs = await db.list_users(kind, 30)
    head = "💎 <b>PAID USERS</b>" if kind == "paid" else "🚫 <b>BANNED USERS</b>"
    if not docs:
        return f"{head}\n\nNobody right now."
    lines = [head, ""]
    for d in docs:
        name = html.escape(d.get("first_name") or "—")
        uname = f" @{d['username']}" if d.get("username") else ""
        until = d.get("premium_until") if kind == "paid" else d.get("ban_until")
        span = f"{fmt_left(until)} left" if until else ("lifetime" if kind == "paid" else "permanent")
        lines.append(f"• <code>{d['user_id']}</code> {name}{uname} - {span}")
    return "\n".join(lines)


async def _user_info_text(uid: int) -> str:
    doc = await db.get_user(uid)
    if not doc:
        return f"🔍 <b>User</b> <code>{uid}</code>\n\nNot in the database yet (never opened a bot)."
    name = html.escape(doc.get("first_name") or "—")
    uname = f"@{doc['username']}" if doc.get("username") else "—"
    paid = await db.is_user_premium(uid)
    if paid:
        until = doc.get("premium_until")
        plan = f"💎 Paid until {fmt_dt(until)} ({fmt_left(until)} left)" if until else "💎 Paid (lifetime)"
    else:
        plan = "🆓 Free"
    ban = await db.get_ban(uid)
    if ban:
        ban_line = f"🚫 Banned until {fmt_dt(ban['until'])}" if ban["until"] else "🚫 Banned (permanent)"
    else:
        ban_line = "✅ Not banned"
    return (
        f"🔍 <b>USER INFO</b>\n\n"
        f"🆔 <code>{uid}</code>\n👤 {name} ({uname})\n"
        f"{plan}\n{ban_line}\n📅 Joined: {fmt_dt(doc.get('created_at'))}"
    )


def attach_admin_panel(client: Client) -> Dict[int, Dict[str, Any]]:
    """Register /admin and its buttons on `client`. Returns the pending-input state dict (owner id -> state)."""
    from auto_uploader import guarded  # lazy: auto_uploader imports this module

    owner_id = TelegramCommonConfig.OWNER_ID
    states: Dict[int, Dict[str, Any]] = {}

    async def _has_state(_, __, m) -> bool:
        u = getattr(m, "from_user", None)
        return bool(u and u.id in states)

    state_filter = filters.create(_has_state)

    async def notify(uid: int, text: str) -> bool:
        try:
            await client.send_message(uid, text, parse_mode=ParseMode.HTML)
            return True
        except Exception as exc:
            logger.info("Could not notify user %s: %s", uid, exc)
            return False

    async def do_ban(uid: int, days: int) -> str:
        await db.ban_user(uid, days)
        ban = await db.get_ban(uid) or {"until": None}
        sent = await notify(uid, format_ban_notice(ban))
        when = f"until <b>{fmt_dt(ban['until'])}</b> ({days} day{'s' if days != 1 else ''})" if ban["until"] else "<b>permanently</b>"
        return f"🚫 <code>{uid}</code> banned {when}." + ("" if sent else "\n<i>(Could not message the user.)</i>")

    async def do_paid(uid: int, days: int) -> str:
        if days < 0:
            await db.set_user_premium(uid, 0, lifetime=True)
            span, note = "<b>lifetime</b>", "💎 <b>You are now a Premium member (lifetime)!</b>"
        else:
            doc = await db.set_user_premium(uid, days)
            span = f"<b>{days} day{'s' if days != 1 else ''}</b> (until {fmt_dt(doc.get('premium_until'))})"
            note = f"💎 <b>You are now a Premium member for {days} day{'s' if days != 1 else ''}!</b>"
        sent = await notify(uid, note + "\nNo more ads / token wait in the File Store bot.")
        return f"💎 <code>{uid}</code> is now paid for {span}." + ("" if sent else "\n<i>(Could not message the user.)</i>")

    async def do_unpaid(uid: int) -> str:
        await db.set_user_premium(uid, 0)
        sent = await notify(uid, "ℹ️ <b>Your Premium plan has ended.</b>")
        return f"🆓 <code>{uid}</code> is now unpaid." + ("" if sent else "\n<i>(Could not message the user.)</i>")

    async def do_unban(uid: int) -> str:
        was = await db.get_ban(uid)
        await db.unban_user(uid)
        if not was:
            return f"ℹ️ <code>{uid}</code> was not banned."
        sent = await notify(uid, "✅ <b>You have been unbanned.</b> You can use the bot again.")
        return f"✅ <code>{uid}</code> unbanned." + ("" if sent else "\n<i>(Could not message the user.)</i>")

    # ----------------------------------------------------------------- /admin
    @client.on_message(filters.command("admin") & filters.private & filters.user(owner_id), group=-1)
    async def admin_cmd(c: Client, message: Message):
        try:
            states.pop(message.from_user.id, None)
            await db.wait_ready(25)
            text, kb = await build_panel()
            await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        except Exception as exc:
            logger.exception("/admin failed: %s", exc)
            await message.reply_text("⚠️ Something went wrong. Please try again.")
        message.stop_propagation()

    # ----------------------------------------------------------------- buttons
    @client.on_callback_query(filters.regex(r"^adm\|") & filters.user(owner_id))
    @guarded
    async def admin_callbacks(c: Client, cq: CallbackQuery):
        parts = cq.data.split("|")
        action = parts[1]
        me = cq.from_user.id
        cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("≼ CANCEL", callback_data="adm|home")]])

        if action == "home":
            states.pop(me, None)
            text, kb = await build_panel()
            await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
            return

        if action == "close":
            states.pop(me, None)
            await cq.message.delete()
            return

        if action in ("plist", "blist"):
            back = InlineKeyboardMarkup([[InlineKeyboardButton("≼ ADMIN PANEL", callback_data="adm|home")]])
            await cq.message.edit_text(await build_user_list("paid" if action == "plist" else "banned"),
                                       parse_mode=ParseMode.HTML, reply_markup=back)
            return

        prompts = {
            "ban": ("ban_uid", "🚫 <b>BAN USER</b>\n\nSend the <b>User ID</b> to ban:"),
            "unban": ("unban_uid", "✅ <b>UNBAN USER</b>\n\nSend the <b>User ID</b> to unban:"),
            "paid": ("paid_uid", "💎 <b>SET PAID</b>\n\nSend the <b>User ID</b> to make paid:"),
            "unpaid": ("unpaid_uid", "🆓 <b>SET UNPAID</b>\n\nSend the <b>User ID</b> to make unpaid:"),
            "info": ("info_uid", "🔍 <b>USER INFO</b>\n\nSend the <b>User ID</b>:"),
        }
        if action in prompts:
            key, text = prompts[action]
            states[me] = {"action": key}
            await cq.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=cancel_kb)
            return

        # duration buttons: adm|bd|<uid>|<days>   adm|pd|<uid>|<days>
        if action in ("bd", "pd"):
            uid, days = int(parts[2]), int(parts[3])
            states.pop(me, None)
            result = await (do_ban(uid, days) if action == "bd" else do_paid(uid, days))
            back = InlineKeyboardMarkup([[InlineKeyboardButton("≼ ADMIN PANEL", callback_data="adm|home")]])
            await cq.message.edit_text(result, parse_mode=ParseMode.HTML, reply_markup=back)
            return

        # custom duration: adm|bc|<uid>   adm|pc|<uid>
        if action in ("bc", "pc"):
            uid = int(parts[2])
            states[me] = {"action": "ban_days" if action == "bc" else "paid_days", "uid": uid}
            what = "ban" if action == "bc" else "paid plan"
            extra = " (send <code>0</code> for permanent)" if action == "bc" else ""
            await cq.message.edit_text(
                f"✏️ Send the number of <b>days</b> for the {what} of <code>{uid}</code>{extra}:",
                parse_mode=ParseMode.HTML,
                reply_markup=cancel_kb,
            )
            return

    # ----------------------------------------------------------------- typed input (user ID / custom days)
    @client.on_message(
        filters.private & filters.text & ~filters.regex(r"^/") & filters.user(owner_id) & state_filter, group=-1
    )
    async def admin_text(c: Client, message: Message):
        me = message.from_user.id
        state = states.get(me)
        try:
            if state:
                await _handle_input(message, me, state)
        except Exception as exc:
            logger.exception("Admin input failed: %s", exc)
            await message.reply_text("⚠️ Something went wrong. Please try again.")
        message.stop_propagation()  # the bot's normal text handler must not also answer this message

    async def _handle_input(message: Message, me: int, state: Dict[str, Any]) -> None:
        text = (message.text or "").strip()
        act = state["action"]
        home_kb = InlineKeyboardMarkup([[InlineKeyboardButton("≼ ADMIN PANEL", callback_data="adm|home")]])

        if act.endswith("_days"):
            if not text.isdigit() or (act == "paid_days" and int(text) < 1) or int(text) > 36500:
                await message.reply_text("❌ Send a whole number of days (1 - 36500).")
                return
            uid, days = state["uid"], int(text)
            states.pop(me, None)
            result = await (do_ban(uid, days) if act == "ban_days" else do_paid(uid, days))
            await message.reply_text(result, parse_mode=ParseMode.HTML, reply_markup=home_kb)
            return

        if not text.lstrip("-").isdigit():
            await message.reply_text("❌ Send a numeric <b>User ID</b> (use /id in the bot to see it).", parse_mode=ParseMode.HTML)
            return
        uid = int(text)
        if uid <= 0:
            await message.reply_text("❌ A user ID is a positive number.")
            return
        if uid == owner_id and act in ("ban_uid", "unpaid_uid"):
            await message.reply_text("❌ You cannot do that to the owner account.")
            return

        if act == "ban_uid":
            states.pop(me, None)
            await message.reply_text(
                f"🚫 Ban <code>{uid}</code> - choose the duration:",
                parse_mode=ParseMode.HTML,
                reply_markup=_duration_keyboard("bd", uid, BAN_DURATIONS, "bc"),
            )
        elif act == "paid_uid":
            states.pop(me, None)
            await message.reply_text(
                f"💎 Make <code>{uid}</code> paid - choose the duration:",
                parse_mode=ParseMode.HTML,
                reply_markup=_duration_keyboard("pd", uid, PAID_DURATIONS, "pc"),
            )
        elif act == "unban_uid":
            states.pop(me, None)
            await message.reply_text(await do_unban(uid), parse_mode=ParseMode.HTML, reply_markup=home_kb)
        elif act == "unpaid_uid":
            states.pop(me, None)
            await message.reply_text(await do_unpaid(uid), parse_mode=ParseMode.HTML, reply_markup=home_kb)
        elif act == "info_uid":
            states.pop(me, None)
            await message.reply_text(await _user_info_text(uid), parse_mode=ParseMode.HTML, reply_markup=home_kb)

    return states
