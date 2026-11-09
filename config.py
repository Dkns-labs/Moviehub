import os
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent


class TelegramCommonConfig:
    """Shared API_ID and API_HASH used by Auto Uploader, File Store, and all Clone Bots."""
    API_ID: int = int(os.getenv("TELEGRAM_API_ID", "32967837"))
    API_HASH: str = os.getenv("TELEGRAM_API_HASH", "9983236cf6c036d892df6c5b5bbe8680")
    OWNER_ID: int = int(os.getenv("OWNER_ID", "8931200082"))
    # Pyrogram session files live here (NOT in memory) so the bots remember every channel / user
    # they have seen. In-memory sessions forget them on each restart -> "Peer id invalid".
    SESSION_DIR: Path = Path(os.getenv("SESSION_DIR", str(BASE_DIR / "data" / "sessions")))


class MongoConfig:
    URI: str = os.getenv("MONGO_URI", "mongodb+srv://Hauntingbots:ghost24@cluster0.5drldta.mongodb.net/?appName=Cluster0")
    DB_NAME: str = os.getenv("MONGO_DB_NAME", "telegram_torrent_system")


class AutoUploaderConfig:
    API_ID: int = TelegramCommonConfig.API_ID
    API_HASH: str = TelegramCommonConfig.API_HASH
    OWNER_ID: int = TelegramCommonConfig.OWNER_ID
    BOT_TOKEN: str = os.getenv("AUTO_UPLOADER_BOT_TOKEN", "9192394611:AAFTnJBELmrjSL1uvu926pewH9m05zaQjQk")
    PUBLIC_CHANNEL_ID: int = int(os.getenv("PUBLIC_CHANNEL_ID", "-1003969388091"))
    DEFAULT_UPLOADER: str = os.getenv("DEFAULT_UPLOADER", "@Movxhxhxbddbbot")
    JOIN_CHANNEL: str = os.getenv("JOIN_CHANNEL", "@ExampleUpdates")
    POST_INTERVAL_SECONDS: int = int(os.getenv("POST_INTERVAL_SECONDS", "300"))  # 5 minutes
    POSTER_DIR: Path = Path(os.getenv("POSTER_DIR", str(BASE_DIR / "data" / "posters")))
    MANUAL_EDIT_MODE: bool = os.getenv("MANUAL_EDIT_MODE", "false").lower() == "true"
    # Every downloaded .torrent file is also sent here. Use a PRIVATE channel, not the public one.
    # 0 = use STORAGE_CHANNEL_ID instead (only if that is a different channel than PUBLIC_CHANNEL_ID).
    LOG_CHANNEL_ID: int = int(os.getenv("LOG_CHANNEL_ID", "0"))
    # Admin's default Database channel (files you already have, reused instead of downloading). 0 = none.
    # A channel set in the bot (/settings -> Advanced -> Database channel) overrides this value.
    DB_CHANNEL_ID: int = int(os.getenv("DB_CHANNEL_ID", "-1003923726493"))
    # true = the moment a new file is ingested, the File Store worker already downloads the real video
    # and puts it into the storage channel, so the first user who clicks gets it instantly.
    # false (default) = download starts the moment the first user opens the bot link.
    PREFETCH_ON_INGEST: bool = os.getenv("PREFETCH_ON_INGEST", "false").lower() == "true"
    INITIAL_JSON_PATH: Path = Path(os.getenv("INITIAL_JSON_PATH", str(BASE_DIR / "sample_post.json")))
    # false (default) = EVERY user can set their own public + log channel (each user has own settings).
    # true  = only Premium users (and the owner) can; banned users are always blocked.
    REQUIRE_PREMIUM_FOR_CHANNELS: bool = os.getenv("REQUIRE_PREMIUM_FOR_CHANNELS", "false").lower() == "true"
    # ▶️ START SCRAPING (Advanced Settings, Premium): how many of the newest scraped posts a user's own scrape
    # pushes into their channel, and how long they must wait between two scrapes (the admin has no limit).
    USER_SCRAPE_LIMIT: int = int(os.getenv("USER_SCRAPE_LIMIT", "10"))
    USER_SCRAPE_COOLDOWN_SECONDS: int = int(os.getenv("USER_SCRAPE_COOLDOWN_SECONDS", "600"))


class FileStoreConfig:
    API_ID: int = TelegramCommonConfig.API_ID
    API_HASH: str = TelegramCommonConfig.API_HASH
    OWNER_ID: int = TelegramCommonConfig.OWNER_ID
    BOT_TOKEN: str = os.getenv("FILE_STORE_BOT_TOKEN", "8940523506:AAHsXZ8Rp2mgIyYV3JNEJ9-BF1sOk9of6z8")
    BOT_USERNAME: str = os.getenv("FILE_STORE_BOT_USERNAME", "@Zbbdbdbdbot").lstrip("@")
    STORAGE_CHANNEL_ID: int = int(os.getenv("STORAGE_CHANNEL_ID", "-1003969388091"))
    # Database channels (Advanced Settings): how many message ids the "SYNC" backfill reads at most.
    DBCHANNEL_SCAN_LIMIT: int = int(os.getenv("DBCHANNEL_SCAN_LIMIT", "30000"))
    DOWNLOAD_DIR: Path = Path(os.getenv("DOWNLOAD_DIR", str(BASE_DIR / "data" / "downloads")))
    THUMBNAIL_DIR: Path = Path(os.getenv("THUMBNAIL_DIR", str(BASE_DIR / "data" / "thumbnails")))
    TORRENT_DIR: Path = Path(os.getenv("TORRENT_DIR", str(BASE_DIR / "data" / "torrents")))
    MAX_CONCURRENT_DOWNLOADS: int = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "3"))
    MAX_SPLIT_SIZE_BYTES: int = int(os.getenv("MAX_SPLIT_SIZE_BYTES", str(int(1.86 * 1024 * 1024 * 1024))))
    DOWNLOAD_TIMEOUT_SECONDS: int = int(os.getenv("DOWNLOAD_TIMEOUT_SECONDS", "7200"))
    STALL_TIMEOUT_SECONDS: int = int(os.getenv("STALL_TIMEOUT_SECONDS", "600"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    PROGRESS_UPDATE_SECONDS: int = int(os.getenv("PROGRESS_UPDATE_SECONDS", "4"))  # live progress message refresh

    # Default Advanced File Store Settings
    PROTECT_CONTENT: bool = os.getenv("DEFAULT_PROTECT_CONTENT", "false").lower() == "true"
    AUTO_DELETE_ENABLED: bool = os.getenv("DEFAULT_AUTO_DELETE", "false").lower() == "true"
    AUTO_DELETE_SECONDS: int = int(os.getenv("DEFAULT_AUTO_DELETE_SECONDS", "600"))  # 10 mins
    FORCE_SUB_CHANNEL: str = os.getenv("DEFAULT_FORCE_SUB_CHANNEL", "")
    CUSTOM_CAPTION: str = os.getenv(
        "DEFAULT_CUSTOM_CAPTION",
        "🎬 <b>{filename}</b>\n\n📦 <b>Size:</b> {size}\n💿 <b>Quality:</b> {quality}",
    )
    CUSTOM_BUTTON_TEXT: str = os.getenv("DEFAULT_CUSTOM_BUTTON_TEXT", "")
    CUSTOM_BUTTON_URL: str = os.getenv("DEFAULT_CUSTOM_BUTTON_URL", "")
    SHORTENER_ENABLED: bool = os.getenv("DEFAULT_SHORTENER_ENABLED", "false").lower() == "true"
    SHORTENER_DOMAIN: str = os.getenv("DEFAULT_SHORTENER_DOMAIN", "")
    SHORTENER_API_KEY: str = os.getenv("DEFAULT_SHORTENER_API_KEY", "")
    TOKEN_VERIFY_ENABLED: bool = os.getenv("DEFAULT_TOKEN_VERIFY_ENABLED", "false").lower() == "true"
    TOKEN_VERIFY_EXPIRE_SECONDS: int = int(os.getenv("DEFAULT_TOKEN_VERIFY_EXPIRE_SECONDS", "86400"))  # 24h
    PREMIUM_PLAN_TEXT: str = os.getenv(
        "PREMIUM_PLAN_TEXT",
        "💸 <b>PREMIUM PLAN BENEFITS</b>\n\n"
        "✅ No Link Shortener Ads\n"
        "✅ No Token Verification Wait\n"
        "✅ Direct High-Speed Telegram & Web Streams\n"
        "✅ Priority Torrent Queueing\n\n"
        "💳 <b>Pricing:</b>\n"
        "• 1 Month: $2 / ₹99\n"
        "• 6 Months: $10 / ₹499\n\n"
        "📩 Contact Admin to upgrade your account!",
    )


class TorrentEngineConfig:
    LISTEN_PORTS: tuple = (6881, 6891)
    ENABLE_DHT: bool = True
    ENABLE_PEX: bool = True
    ENABLE_LSD: bool = True
    CONNECTIONS_LIMIT: int = int(os.getenv("TORRENT_CONNECTIONS_LIMIT", "1000"))
    # --- speed tuning (all overridable via env) ---
    MAX_PEERS_PER_TORRENT: int = int(os.getenv("TORRENT_MAX_PEERS_PER_TORRENT", "300"))
    CONNECTION_SPEED: int = int(os.getenv("TORRENT_CONNECTION_SPEED", "100"))        # new outgoing connections / second
    AIO_THREADS: int = int(os.getenv("TORRENT_AIO_THREADS", "16"))                   # disk I/O threads
    CACHE_SIZE_BLOCKS: int = int(os.getenv("TORRENT_CACHE_BLOCKS", "0"))             # 0 = rely on OS page cache (libtorrent 2.x)
    MAX_OUT_REQUEST_QUEUE: int = int(os.getenv("TORRENT_MAX_OUT_REQUEST_QUEUE", "1500"))
    SEND_BUFFER_WATERMARK: int = int(os.getenv("TORRENT_SEND_BUFFER_WATERMARK", str(3 * 1024 * 1024)))
    SOCKET_BUFFER_SIZE: int = int(os.getenv("TORRENT_SOCKET_BUFFER", str(1024 * 1024)))
    # --- trackers ---
    TRACKERS_FILE: str = os.getenv("TRACKERS_FILE", os.path.join(os.path.dirname(os.path.abspath(__file__)), "trackers.txt"))
    TRACKERS_CACHE_FILE: str = os.getenv("TRACKERS_CACHE_FILE", "/tmp/trackers_cache.txt")
    # How many trackers are attached to each torrent (UDP first - they are cheapest). 0 = every tracker.
    TRACKERS_PER_TORRENT: int = int(os.getenv("TRACKERS_PER_TORRENT", "600"))
    TRACKER_REFRESH_SECONDS: int = int(os.getenv("TRACKER_REFRESH_SECONDS", "21600"))  # re-fetch public lists every 6h
    DOWNLOAD_RATE_LIMIT: int = int(os.getenv("TORRENT_DOWNLOAD_RATE_LIMIT", "0"))
    UPLOAD_RATE_LIMIT: int = int(os.getenv("TORRENT_UPLOAD_RATE_LIMIT", "1048576"))
    INITIAL_TEST_TORRENT_URL: str = os.getenv(
        "INITIAL_TEST_TORRENT_URL",
        "https://www.1tamilmv.style/applications/core/interface/file/attachment.php?id=158346&key=8218f8e32a61cf68200c49e8b1321e85",
    )
    DEFAULT_TRACKERS: list = [
        "udp://tracker.opentrackr.org:1337/announce",
        "udp://open.demonii.com:1337/announce",
        "udp://open.stealth.si:80/announce",
        "udp://tracker.torrent.eu.org:451/announce",
        "udp://exodus.desync.com:6969/announce",
    ]
    # Comma-separated list sources (GitHub raw + newTrackon); every one is fetched, merged and de-duplicated.
    # A GitHub raw URL that is blocked / slow is retried through jsDelivr automatically.
    TRACKER_LIST_SOURCE_URL: str = os.getenv(
        "TRACKER_LIST_SOURCE_URL",
        ",".join([
            "https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_all.txt",
            "https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_best.txt",
            "https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_all_udp.txt",
            "https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_best_ip.txt",
            "https://raw.githubusercontent.com/XIU2/TrackersListCollection/master/all.txt",
            "https://raw.githubusercontent.com/XIU2/TrackersListCollection/master/best.txt",
            "https://raw.githubusercontent.com/DeSireFire/animeTrackerList/master/AT_all.txt",
            "https://raw.githubusercontent.com/hezhijie0327/Trackerslist/main/trackerslist_combine.txt",
            "https://raw.githubusercontent.com/adysec/tracker/main/trackers_all.txt",
            "https://newtrackon.com/api/stable",
        ]),
    )
    # --- live tracker filter: probe every tracker, keep the ones that answer (fastest first) ---
    TRACKER_CHECK: bool = os.getenv("TRACKER_CHECK", "true").lower() == "true"
    TRACKERS_LIVE_MAX: int = int(os.getenv("TRACKERS_LIVE_MAX", "150"))             # live trackers attached per torrent
    TRACKERS_UNCHECKED_MAX: int = int(os.getenv("TRACKERS_UNCHECKED_MAX", "120"))   # used until the first check finishes
    TRACKER_CHECK_CONCURRENCY: int = int(os.getenv("TRACKER_CHECK_CONCURRENCY", "250"))
    TRACKERS_LIVE_FILE: str = os.getenv("TRACKERS_LIVE_FILE", "/tmp/trackers_live.txt")
    # --- slow torrent boost: below this speed (bytes/s) after 30 s, more live trackers are added every 45 s ---
    BOOST_BELOW_SPEED: int = int(os.getenv("TORRENT_BOOST_BELOW_SPEED", str(1024 * 1024)))
    # --- engine:
    #   hybrid (default) = libtorrent starts first; if it is slow (< HYBRID_BELOW_SPEED) for HYBRID_RACE_AFTER seconds,
    #                      aria2c is started on the SAME torrent in parallel and the first one to finish wins
    #                      (the other is stopped and its files deleted). Falls back to whichever client exists.
    #   auto = libtorrent first, aria2c as 2nd client only when libtorrent is missing or an attempt failed
    #   libtorrent / aria2 = force one of them ---
    BACKEND: str = os.getenv("TORRENT_BACKEND", "hybrid").lower()
    HYBRID_RACE_AFTER: int = int(os.getenv("TORRENT_HYBRID_RACE_AFTER", "20"))      # 0 = start both at once
    HYBRID_BELOW_SPEED: int = int(os.getenv("TORRENT_HYBRID_BELOW_SPEED", str(2 * 1024 * 1024)))  # bytes/s


class TorrentSearchConfig:
    """Torrent Search bot (22 sites, see torrent_search.py).
    BOT_TOKEN set  -> runs as its OWN bot: just type a movie / show / anything and it searches.
    BOT_TOKEN empty -> the same search is built into the File Store bot (and its clones) as /search <name>."""
    BOT_TOKEN: str = os.getenv("TORRENT_SEARCH_BOT_TOKEN", "").strip()
    PAGE_SIZE: int = int(os.getenv("TS_PAGE_SIZE", "5"))
    SESSION_TTL_SECONDS: int = int(os.getenv("TS_SESSION_TTL", "1800"))   # how long result lists stay clickable
    COOLDOWN_SECONDS: int = int(os.getenv("TS_COOLDOWN", "4"))            # per user, between two searches
    MAX_RESULTS: int = int(os.getenv("TS_MAX_RESULTS", "60"))


class ScraperConfig:
    # Start pages tried in order. The real base URL is detected from the final (post-redirect) URL,
    # and each torrent file keeps its own base URL, so mirrors can change freely.
    SITE_URLS: list = [
        u.strip() for u in os.getenv("SCRAPER_SITE_URLS", os.getenv("SCRAPER_SITE_URL", "https://www.1tamilmv.style/")).split(",") if u.strip()
    ]


class WebServerConfig:
    HOST: str = os.getenv("WEB_HOST", "0.0.0.0")
    PORT: int = int(os.getenv("WEB_PORT") or os.getenv("PORT") or "8080")  # PaaS hosts inject PORT
    # Leave WEB_BASE_URL empty to auto-detect the public URL (see base_url.py).
    BASE_URL: str = os.getenv("WEB_BASE_URL", "").rstrip("/")
    COUNTDOWN_SECONDS: int = int(os.getenv("COUNTDOWN_SECONDS", "15"))
    SECRET_TOKEN: str = os.getenv("WEB_SECRET_TOKEN", "7f9a4b82e1c6305d94a827f1e6b3c09d48e215a7f0c93b64d18e52a0c7f3b91e")


def setup_directories() -> None:
    AutoUploaderConfig.POSTER_DIR.mkdir(parents=True, exist_ok=True)
    FileStoreConfig.DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    FileStoreConfig.THUMBNAIL_DIR.mkdir(parents=True, exist_ok=True)
    FileStoreConfig.TORRENT_DIR.mkdir(parents=True, exist_ok=True)
    TelegramCommonConfig.SESSION_DIR.mkdir(parents=True, exist_ok=True)


def setup_logging() -> None:
    log_level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("pyrogram").setLevel(logging.WARNING)
