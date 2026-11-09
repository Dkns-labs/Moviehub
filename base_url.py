"""
Public base-URL auto detection.

Why this exists: Telegram silently DROPS a text link whose URL points to localhost / a private IP,
so posts built with WEB_BASE_URL=http://localhost:8080 showed the file names as plain text with no link.

Resolution order (first public one wins):
  1. WEB_BASE_URL env (if it is public)
  2. URL learned from a real incoming request (saved in MongoDB, see web_server middleware)
  3. Hosting-platform variables (Render, Railway, Koyeb, Heroku, Fly, HF Spaces, Replit, Vercel, ...)
  4. This machine's public IP + WEB_PORT   (works on a VPS with the port open)
If nothing public is found, post captions fall back to t.me deep links (always valid),
see AutoUploaderBot._file_link / _batch_link in auto_uploader.py.
"""
import os
import re
import ipaddress
import logging
from typing import Optional
from urllib.parse import urlparse

import aiohttp

from config import WebServerConfig

logger = logging.getLogger("base_url")

_SETTINGS_KEY = "web_server"


def is_public_url(url: Optional[str]) -> bool:
    """True if Telegram will accept this URL as a clickable link."""
    if not url:
        return False
    p = urlparse(url if "://" in url else f"https://{url}")
    host = (p.hostname or "").lower()
    if p.scheme not in ("http", "https") or not host:
        return False
    if host in ("localhost", "0.0.0.0") or host.endswith((".local", ".internal", ".localhost", ".lan")):
        return False
    try:
        ip = ipaddress.ip_address(host)
        return not (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_unspecified)
    except ValueError:
        return "." in host  # a real domain name has a dot


def normalize_base_url(url: str) -> str:
    url = (url or "").strip().rstrip("/")
    if url and "://" not in url:
        url = "https://" + url
    return url


def _from_platform_env() -> Optional[str]:
    env = os.environ.get
    candidates = [
        env("RENDER_EXTERNAL_URL"),
        env("KOYEB_PUBLIC_DOMAIN"),
        env("RAILWAY_PUBLIC_DOMAIN"),
        env("RAILWAY_STATIC_URL"),
        f"{env('HEROKU_APP_NAME')}.herokuapp.com" if env("HEROKU_APP_NAME") else None,
        f"{env('FLY_APP_NAME')}.fly.dev" if env("FLY_APP_NAME") else None,
        env("SPACE_HOST"),  # Hugging Face Spaces
        env("REPLIT_DEV_DOMAIN"),
        env("VERCEL_URL"),
        env("CODESPACE_NAME") and f"{env('CODESPACE_NAME')}-{WebServerConfig.PORT}.app.github.dev",
        env("APP_URL"),
        env("BASE_URL"),
    ]
    for c in candidates:
        if c:
            c = normalize_base_url(c)
            if is_public_url(c):
                return c
    return None


async def _from_public_ip() -> Optional[str]:
    """Ask the three IP services at once; the first valid answer wins (was: one after another, 8s each)."""
    import asyncio

    async def ask(endpoint: str) -> Optional[str]:
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=4)) as session:
                async with session.get(endpoint) as resp:
                    ip = (await resp.text()).strip()
            if re.fullmatch(r"\d{1,3}(\.\d{1,3}){3}", ip):
                url = f"http://{ip}:{WebServerConfig.PORT}"
                if is_public_url(url):
                    return url
        except Exception:
            pass
        return None

    pending = {asyncio.ensure_future(ask(e)) for e in ("https://api.ipify.org", "https://ifconfig.me/ip", "https://icanhazip.com")}
    try:
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for d in done:
                if d.result():
                    return d.result()
    finally:
        for p in pending:
            p.cancel()
    return None


async def detect_public_base_url(db=None) -> str:
    """Detect and APPLY the base URL (updates WebServerConfig.BASE_URL so every module uses it)."""
    explicit = normalize_base_url(os.getenv("WEB_BASE_URL", ""))
    if is_public_url(explicit):
        found, how = explicit, "WEB_BASE_URL"
    else:
        found, how = None, ""
        if explicit:
            logger.warning("WEB_BASE_URL=%s is not public (Telegram drops such links); auto-detecting.", explicit)
        if db is not None and os.getenv("LEARN_BASE_URL", "0") == "1":
            try:
                saved = (await db.get_bot_settings(_SETTINGS_KEY)).get("public_base_url")
                if saved and is_public_url(saved):
                    found, how = saved, "learned from a previous request"
            except Exception:
                pass
        if not found:
            found = _from_platform_env()
            how = "hosting platform env"
        if not found:
            found = await _from_public_ip()
            how = "public IP"

    if found:
        WebServerConfig.BASE_URL = found.rstrip("/")
        logger.info("Public base URL = %s (%s)", WebServerConfig.BASE_URL, how)
        logger.info("Test it from ANY device: %s/health  (if it times out: open TCP %s in the cloud firewall / ufw)",
                    WebServerConfig.BASE_URL, WebServerConfig.PORT)
    else:
        WebServerConfig.BASE_URL = explicit or f"http://localhost:{WebServerConfig.PORT}"
        logger.error(
            "Could not detect a public base URL. Post links will use t.me deep links instead. "
            "Set WEB_BASE_URL to your public https URL to use the countdown redirect pages."
        )
    return WebServerConfig.BASE_URL


async def learn_from_request(db, scheme: str, host: str) -> None:
    """Called by the web server on real traffic: remember the public host users reach us on."""
    if not host or os.getenv("LEARN_BASE_URL", "0") != "1":
        return  # Host header is client-controlled: learning from it is opt-in (LEARN_BASE_URL=1)
    candidate = normalize_base_url(f"{scheme}://{host}")
    if not is_public_url(candidate) or candidate == WebServerConfig.BASE_URL:
        return
    if is_public_url(WebServerConfig.BASE_URL) and os.getenv("WEB_BASE_URL"):
        return  # explicit config always wins
    WebServerConfig.BASE_URL = candidate
    try:
        await db.update_bot_settings(_SETTINGS_KEY, {"public_base_url": candidate})
    except Exception as exc:
        logger.warning("Could not persist learned base URL: %s", exc)
    logger.info("Learned public base URL from request: %s", candidate)
