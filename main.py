import json
import time
import asyncio
import logging
import uvicorn

from config import (
    setup_directories,
    setup_logging,
    AutoUploaderConfig,
    WebServerConfig,
)
from database import db
import session_store
from base_url import detect_public_base_url
from torrent_engine import torrent_engine
from auto_uploader import auto_uploader_bot
from file_store import file_store_bot
from torrent_search_bot import torrent_search_bot
from web_server import app as fastapi_app

logger = logging.getLogger("main")


async def load_initial_sample_post() -> None:
    if AutoUploaderConfig.INITIAL_JSON_PATH.exists():
        try:
            raw = AutoUploaderConfig.INITIAL_JSON_PATH.read_text(encoding="utf-8").strip()
            if not raw:
                return
            data = json.loads(raw)
            if isinstance(data, dict):
                data = [data]
            ingested = await auto_uploader_bot.ingest_api_payload(data)
            if ingested:
                logger.info("Ingested initial JSON payload (%d movie(s)): %s", len(ingested), ingested)
        except Exception as exc:
            logger.error("Failed to ingest initial JSON file: %s", exc)


async def keep_alive() -> None:
    """Ping our own PUBLIC /health every minute so hosts that sleep idle services (Render free) stay awake.
    Uses KEEP_ALIVE_URL, else RENDER_EXTERNAL_URL (set automatically on Render). Disabled if neither is set."""
    import os
    import aiohttp

    url = (os.getenv("KEEP_ALIVE_URL") or os.getenv("RENDER_EXTERNAL_URL") or "").rstrip("/")
    if not url:
        return
    interval = max(5, int(os.getenv("KEEP_ALIVE_INTERVAL", "60")))
    target = f"{url}/health"
    logger.info("Keep-alive: pinging %s every %ss", target, interval)
    failing = False
    while True:
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as sess:
                async with sess.get(target) as r:
                    if failing:
                        logger.info("Keep-alive recovered (HTTP %s)", r.status)
                    failing = False
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not failing:
                logger.warning("Keep-alive ping failed: %s", exc)
            failing = True
        await asyncio.sleep(interval)


async def main() -> None:
    setup_logging()
    setup_directories()

    # Web server FIRST: links must open even if a bot / torrent / scraper step is slow or fails.
    uvicorn_config = uvicorn.Config(
        fastapi_app,
        host=WebServerConfig.HOST,
        port=WebServerConfig.PORT,
        log_level="info",
        proxy_headers=True,
        forwarded_allow_ips="*",
    )
    server = uvicorn.Server(uvicorn_config)
    server_task = asyncio.create_task(server.serve())
    await asyncio.sleep(0.3)
    if server_task.done():  # e.g. port already in use -> show the real error
        server_task.result()
    logger.info("Web server listening on %s:%s", WebServerConfig.HOST, WebServerConfig.PORT)

    async def self_check() -> None:
        """Log whether the public URL really answers, with the usual causes if it does not."""
        base = WebServerConfig.BASE_URL
        if not base:
            return
        try:
            import aiohttp
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as sess:
                async with sess.get(f"{base}/health") as r:
                    logger.info("PUBLIC URL CHECK: %s/health -> HTTP %s (reachable)", base, r.status)
        except Exception as exc:
            logger.error(
                "PUBLIC URL CHECK FAILED for %s (%s). Usual causes: port not published (docker -p %s:%s), "
                "cloud Security Group / ufw blocks TCP %s, or WEB_BASE_URL is wrong.",
                base, exc, WebServerConfig.PORT, WebServerConfig.PORT, WebServerConfig.PORT,
            )

    async def connect_db() -> None:
        """Retry until MongoDB answers (Atlas IP allow-list / slow cold start must not stop the web port from opening)."""
        attempt = 0
        while True:
            attempt += 1
            try:
                await db.connect()
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                delay = min(2 * attempt, 10)
                logger.error("MongoDB connect attempt %d failed: %s (retrying in %ss). "
                             "On Atlas, allow 0.0.0.0/0 in Network Access.", attempt, str(exc)[:300], delay)
                await asyncio.sleep(delay)

    async def timed(name: str, step) -> None:
        t = time.monotonic()
        try:
            await step()
            logger.info("Startup step '%s' done in %.1fs", name, time.monotonic() - t)
        except Exception as exc:
            logger.exception("Startup step '%s' failed: %s", name, exc)

    async def startup_steps() -> None:
        t0 = time.monotonic()
        # Everything that does not depend on another step starts AT THE SAME TIME. Both bots connect to Telegram
        # while MongoDB is still connecting: a handler simply waits for the DB (see guarded()), so /start answers
        # the moment the DB is up instead of after "db -> url -> torrent -> bot 1 -> bot 2" one by one.
        await asyncio.gather(
            timed("mongodb", connect_db),
            timed("detect base url", lambda: detect_public_base_url(db)),
            timed("torrent engine", torrent_engine.initialize),
            timed("auto uploader bot", auto_uploader_bot.start),
            timed("file store bot", file_store_bot.start),
            timed("torrent search bot", torrent_search_bot.start),
        )
        logger.info("BOTS READY: bots answer /start (%.1fs after launch)", time.monotonic() - t0)
        # these need the DB + a running bot, but nobody waits for them
        await timed("bot background setup", auto_uploader_bot.wait_post_start)
        await timed("public url check", self_check)
        await timed("initial json", load_initial_sample_post)
        # old torrents registered by earlier versions: slow on big databases, so never on the startup path
        asyncio.create_task(db._backfill_torrent_registry())

    startup_task = asyncio.create_task(startup_steps())
    keepalive_task = asyncio.create_task(keep_alive())

    try:
        await server_task
    finally:
        startup_task.cancel()
        keepalive_task.cancel()
        for closer in (session_store.stop_backups, auto_uploader_bot.stop, file_store_bot.stop, torrent_search_bot.stop, db.close):
            try:
                await closer()
            except Exception:
                pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
