import os
import re
import html
import base64
import hashlib
import time
import math
import asyncio
import logging
import shutil
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, unquote
from typing import Optional, Dict, Any, List, Callable, Awaitable

import aiohttp
import requests
from bs4 import BeautifulSoup
try:
    import libtorrent as lt
except Exception as _lt_exc:  # missing / broken build: the aria2c backend takes over (see TORRENT_BACKEND)
    lt = None
    _LT_IMPORT_ERROR = repr(_lt_exc)
else:
    _LT_IMPORT_ERROR = ""

from config import TorrentEngineConfig, FileStoreConfig, ScraperConfig
import tracker_tools
import aria2_backend

logger = logging.getLogger("torrent_engine")

SCRAPER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}


# --------------------------------------------------------------------------- #
# URL helpers: every file / topic carries its OWN base URL (mirrors rotate).
# --------------------------------------------------------------------------- #

def human_speed_short(bps: float) -> str:
    return f"{bps / 1024 / 1024:.2f} MB" if bps >= 1024 * 1024 else f"{bps / 1024:.0f} KB"


def get_origin(url: Optional[str]) -> str:
    """'https://www.1tv.style/a/b?x=1' -> 'https://www.1tv.style' (empty string if not absolute)."""
    if not url:
        return ""
    p = urlparse(url)
    if p.scheme not in ("http", "https") or not p.netloc:
        return ""
    return f"{p.scheme}://{p.netloc}"


def resolve_url(href: Optional[str], base: Optional[str] = None) -> str:
    """Make any href absolute using the page it was found on ('//x', '/x', 'x' all handled)."""
    href = html.unescape((href or "").strip())
    if not href or href.lower().startswith("magnet:"):
        return href
    if href.startswith("//"):
        scheme = urlparse(base).scheme if base and urlparse(base).scheme else "https"
        return f"{scheme}:{href}"
    if base and not urlparse(href).netloc:
        return urljoin(base, href)
    return href


def swap_origin(url: str, new_origin: str) -> str:
    """Same path/query on another host - used as a fallback when a mirror host is dead."""
    p, o = urlparse(url), urlparse(new_origin)
    return urlunparse((o.scheme, o.netloc, p.path, p.params, p.query, p.fragment))


# --------------------------------------------------------------------------- #
# Minimal pure-python .torrent parser (infohash / name / size without libtorrent)
# --------------------------------------------------------------------------- #

def _bdecode(data: bytes, i: int = 0):
    c = data[i:i + 1]
    if c == b"i":
        j = data.index(b"e", i)
        return int(data[i + 1:j]), j + 1
    if c == b"l":
        i += 1
        out = []
        while data[i:i + 1] != b"e":
            v, i = _bdecode(data, i)
            out.append(v)
        return out, i + 1
    if c == b"d":
        i += 1
        out = {}
        while data[i:i + 1] != b"e":
            k, i = _bdecode(data, i)
            start = i
            v, i = _bdecode(data, i)
            out[k] = v
            if k == b"info":
                out[b"__info_raw__"] = data[start:i]
        return out, i + 1
    if c.isdigit():
        j = data.index(b":", i)
        n = int(data[i:j])
        return data[j + 1:j + 1 + n], j + 1 + n
    raise ValueError("invalid bencode")


def parse_torrent_bytes(raw: bytes) -> Optional[Dict[str, Any]]:
    """Returns {infohash, name, total_size, num_files, trackers} or None if `raw` is not a .torrent."""
    try:
        if not raw or raw[:1] != b"d":
            return None
        meta, _ = _bdecode(raw)
        info = meta[b"info"]
        infohash = hashlib.sha1(meta[b"__info_raw__"]).hexdigest()
        name = info.get(b"name", b"").decode("utf-8", "replace")
        if b"files" in info:
            total = sum(int(f[b"length"]) for f in info[b"files"])
            num_files = len(info[b"files"])
        else:
            total = int(info.get(b"length", 0))
            num_files = 1
        trackers = []
        if isinstance(meta.get(b"announce"), bytes):
            trackers.append(meta[b"announce"].decode("utf-8", "replace"))
        return {"infohash": infohash, "name": name, "total_size": total, "num_files": num_files, "trackers": trackers}
    except Exception:
        return None


def fetch_dkbotz_links(site_url: Optional[str] = None) -> List[Dict[str, str]]:
    """
    Scrape the forum home page and return topic links.
    The base URL is detected automatically from where the request really ends up
    (redirects / rotating mirrors), nothing is hardcoded to one domain.
    Returns [{"url": topic_url, "base_url": origin_of_the_page}, ...]
    """
    candidates = [site_url] if site_url else list(ScraperConfig.SITE_URLS)
    for site in candidates:
        try:
            response = requests.get(site, headers=SCRAPER_HEADERS, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            logger.warning("Scraper site %s unreachable: %s", site, exc)
            continue

        base_url = get_origin(response.url) or get_origin(site)  # final URL after redirects
        soup = BeautifulSoup(response.text, "html.parser")
        found: Dict[str, str] = {}
        for anchor in soup.find_all("a", href=True):
            link = resolve_url(anchor["href"], response.url)
            if "/forums/topic/" in link and get_origin(link):
                found[link] = get_origin(link)
        if found:
            logger.info("Detected scraper base URL %s (%d topics)", base_url, len(found))
            return [{"url": u, "base_url": found[u]} for u in sorted(found)]
    return []


_IMG_ATTRS = ("data-full-src", "data-src", "data-original", "data-lazy-src", "data-large-file", "src")
_IMG_SKIP = re.compile(r"(emoji|emoticon|smil(e|ey)|avatar|/reactions?/|spacer|pixel|blank\.|loading|\.svg(\?|$)|\.gif(\?|$)|1x1)", re.I)


def pick_poster_image(body, soup, page_url: str) -> str:
    """First real picture of the post. Handles lazy-load attributes (data-src ...), srcset, and skips emoji / avatars /
    tracking pixels; falls back to the page's og:image. Always returns an absolute URL ('' if none)."""
    for tag in body.find_all("img"):
        urls = [tag.get(a) for a in _IMG_ATTRS if tag.get(a)]
        srcset = tag.get("srcset") or tag.get("data-srcset")
        if srcset:  # "a.jpg 300w, b.jpg 800w" -> the widest one first
            try:
                cands = [c.strip().split() for c in srcset.split(",") if c.strip()]
                cands.sort(key=lambda c: int(re.sub(r"\D", "", c[1]) or 0) if len(c) > 1 else 0, reverse=True)
                urls = [c[0] for c in cands] + urls
            except Exception:
                pass
        for raw in urls:
            raw = (raw or "").strip()
            if not raw or raw.startswith("data:") or _IMG_SKIP.search(raw):
                continue
            w = str(tag.get("width") or "").strip()
            if w.isdigit() and int(w) < 60:
                continue  # icon-sized
            return resolve_url(raw, page_url)
    og = soup.find("meta", attrs={"property": "og:image"}) or soup.find("meta", attrs={"name": "twitter:image"})
    if og and og.get("content"):
        return resolve_url(og["content"].strip(), page_url)
    return ""


def fetch_dkbotz() -> List[Dict[str, Any]]:
    """
    Scraper with automatic base-URL detection.
    Every topic keeps its own `source_url`/`base_url`, and every release keeps the
    `base_url` of its own torrent link (the API can mix several mirror domains).
    """
    links = fetch_dkbotz_links()
    if not links:
        return []

    session = requests.Session()
    session.headers.update(SCRAPER_HEADERS)
    results = []
    seen_urls = set()

    try:
        for entry in links:
            try:
                url = entry["url"].strip()
                parsed = urlparse(url)
                if parsed.scheme not in ("http", "https") or not parsed.netloc or url in seen_urls:
                    continue
                seen_urls.add(url)

                response = session.get(url, timeout=25)
                response.raise_for_status()
                page_url = response.url  # final URL: all relative links resolve against THIS
                soup = BeautifulSoup(response.content, "html.parser")

                title = html.unescape(soup.title.get_text(" ")) if soup.title else ""
                if not title:
                    og = soup.find("meta", attrs={"property": "og:title"})
                    if og:
                        title = og.get("content", "")
                title = re.sub(r"\s+", " ", title).strip()

                body = soup.find(attrs={"data-role": "commentContent"})
                if not body:
                    body = soup.find(class_=lambda value: value and "ipsType_richText" in value)
                if not body:
                    body = soup

                image = pick_poster_image(body, soup, page_url)

                releases = []
                current = None
                last_heading = ""

                for anchor in body.find_all("a"):
                    href = anchor.get("href", "")
                    if not href:
                        continue
                    href = resolve_url(href, page_url)
                    lower_href = href.lower()
                    if lower_href.startswith("magnet:"):
                        kind = "magnet"
                    elif "attachment.php" in lower_href or ".torrent" in lower_href:
                        kind = "torrent"
                    elif re.search(r"(cyberloom|direct|/l/)", lower_href):
                        kind = "direct"
                    else:
                        continue

                    heading = ""
                    for previous in anchor.find_all_previous(["strong", "b"]):
                        text = re.sub(r"\s+", " ", html.unescape(previous.get_text(" "))).strip()
                        if text and len(text) > 12 and not text.lower().startswith("tamilmv"):
                            heading = text
                            break
                    if heading:
                        last_heading = heading

                    anchor_text = re.sub(r"\s+", " ", anchor.get_text(" ")).strip()
                    if kind == "torrent":
                        release_title = anchor_text or last_heading or "Untitled Release"
                        if len(release_title) < 8:
                            release_title = last_heading or "Untitled Release"
                        current = {
                            "index": len(releases) + 1,
                            "title": release_title,
                            "torrent": [],
                            "magnet": [],
                            "direct": [],
                            "base_url": get_origin(href),  # this file's OWN base url
                        }
                        releases.append(current)

                    if not current:
                        current = {
                            "index": len(releases) + 1,
                            "title": last_heading or "Untitled Release",
                            "torrent": [],
                            "magnet": [],
                            "direct": [],
                            "base_url": get_origin(href) if kind != "magnet" else get_origin(page_url),
                        }
                        releases.append(current)

                    if href not in current[kind]:
                        current[kind].append(href)

                if releases:
                    results.append(
                        {
                            "title": title,
                            "image": image,
                            "source_url": page_url,
                            "base_url": get_origin(page_url),
                            "releases": releases,
                        }
                    )
            except Exception as exc:
                logger.warning("Scraper error on %s: %s", entry.get("url"), exc)
                continue
    finally:
        session.close()
    return results


def extract_infohash_from_magnet(magnet_uri: str) -> Optional[str]:
    if not magnet_uri or not magnet_uri.lower().startswith("magnet:"):
        return None
    match = re.search(r"urn:btih:([a-fA-F0-9]{40}|[a-zA-Z2-7]{32})", magnet_uri)
    if not match:
        return None
    h = match.group(1)
    if len(h) == 32:  # base32 form -> the usual 40-char hex so duplicates match
        try:
            return base64.b32decode(h.upper()).hex()
        except Exception:
            return h.lower()
    return h.lower()


def extract_dn_from_magnet(magnet_uri: str) -> Optional[str]:
    try:
        parsed = urlparse(magnet_uri)
        qs = parse_qs(parsed.query)
        if "dn" in qs and qs["dn"]:
            return unquote(qs["dn"][0])
    except Exception:
        pass
    return None


def extract_size_from_magnet(magnet_uri: str) -> int:
    try:
        parsed = urlparse(magnet_uri)
        qs = parse_qs(parsed.query)
        if "xl" in qs and qs["xl"]:
            return int(qs["xl"][0])
    except Exception:
        pass
    return 0


# --------------------------------------------------------------------------- #
# Tracker list handling
# --------------------------------------------------------------------------- #
_TRACKER_RE = re.compile(r"^(udp|http|https)://[A-Za-z0-9.\-\[\]:]+(/\S*)?$", re.I)


def normalize_trackers(lines) -> List[str]:
    """Keep udp/http/https announce URLs only, de-duplicated (scheme+host+port+path), order preserved."""
    seen, out = set(), []
    for raw in lines:
        tr = raw.strip()
        if not _TRACKER_RE.match(tr):
            continue
        u = urlparse(tr)
        try:
            port = u.port
        except ValueError:
            continue
        scheme = u.scheme.lower()
        if scheme == "udp" and not port:
            continue
        key = (scheme, (u.hostname or "").lower(), port or (443 if scheme == "https" else 80), u.path.rstrip("/") or "/announce")
        if key in seen:
            continue
        seen.add(key)
        out.append(tr)
    return out


def load_tracker_file(path: str) -> List[str]:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            return normalize_trackers(fh)
    except OSError:
        return []


def select_trackers(trackers: List[str], limit: int) -> List[str]:
    """UDP first (cheapest to announce to, usually fastest), then https, then http. limit<=0 -> all."""
    rank = {"udp": 0, "https": 1, "http": 2}
    ordered = sorted(trackers, key=lambda t: rank.get(urlparse(t).scheme.lower(), 3))  # stable: keeps curated order
    return ordered[:limit] if limit and limit > 0 else ordered


class TorrentEngine:
    def __init__(self):
        self.ses: Optional[Any] = None
        self.live_trackers: List[str] = load_tracker_file(TorrentEngineConfig.TRACKERS_LIVE_FILE)  # last verified list
        self.extra_trackers: List[str] = normalize_trackers(
            list(TorrentEngineConfig.DEFAULT_TRACKERS) + load_tracker_file(TorrentEngineConfig.TRACKERS_FILE)
            + load_tracker_file(TorrentEngineConfig.TRACKERS_CACHE_FILE)
        )
        self._tracker_task: Optional[asyncio.Task] = None
        self._semaphore = asyncio.Semaphore(FileStoreConfig.MAX_CONCURRENT_DOWNLOADS)

    def _session_settings(self) -> Dict[str, Any]:
        C = TorrentEngineConfig
        port = C.LISTEN_PORTS[0]
        return {
            "listen_interfaces": f"0.0.0.0:{port},[::]:{port}",
            "enable_dht": C.ENABLE_DHT,
            "enable_lsd": C.ENABLE_LSD,
            "enable_upnp": True,
            "enable_natpmp": True,
            "user_agent": "libtorrent/2.0.9",
            "dht_bootstrap_nodes": "router.bittorrent.com:6881,router.utorrent.com:6881,dht.transmissionbt.com:6881,dht.libtorrent.org:25401",
            # throughput / peers
            "connections_limit": C.CONNECTIONS_LIMIT,
            "download_rate_limit": C.DOWNLOAD_RATE_LIMIT,
            "upload_rate_limit": C.UPLOAD_RATE_LIMIT,
            "connection_speed": C.CONNECTION_SPEED,
            "max_out_request_queue": C.MAX_OUT_REQUEST_QUEUE,
            "max_allowed_in_request_queue": 2000,
            "send_buffer_watermark": C.SEND_BUFFER_WATERMARK,
            "send_buffer_low_watermark": 512 * 1024,
            "send_buffer_watermark_factor": 150,
            "recv_socket_buffer_size": C.SOCKET_BUFFER_SIZE,
            "send_socket_buffer_size": C.SOCKET_BUFFER_SIZE,
            "request_timeout": 20,
            "peer_timeout": 60,
            "peer_connect_timeout": 10,
            "min_reconnect_time": 10,
            "max_failcount": 5,
            "whole_pieces_threshold": 10,
            "mixed_mode_algorithm": 0,          # prefer TCP, don't let uTP be throttled below it
            "enable_incoming_utp": True,
            "enable_outgoing_utp": True,
            "enable_incoming_tcp": True,
            "enable_outgoing_tcp": True,
            "allow_multiple_connections_per_ip": True,
            "smooth_connects": False,           # connect to peers in bursts instead of trickling
            "auto_manage_prefer_seeds": False,
            "active_downloads": -1, "active_seeds": -1, "active_limit": -1, "active_checking": 4,
            # disk
            "aio_threads": C.AIO_THREADS,
            "file_pool_size": 256,
            "max_queued_disk_bytes": 16 * 1024 * 1024,
            "max_peerlist_size": 8000,          # remember many more peers -> faster swarm discovery
            "torrent_connect_boost": 100,       # extra connections when a torrent starts
            "request_queue_time": 4,
            "tick_interval": 100,
            "strict_end_game_mode": True,
            "disk_io_write_mode": 1,            # enable_os_cache: let the kernel buffer writes
            "disk_io_read_mode": 1,
            # tracker behaviour: announce to every tracker in every tier in parallel, never wait on a dead one
            "announce_to_all_trackers": True,
            "announce_to_all_tiers": True,
            "tracker_completion_timeout": 20,
            "tracker_receive_timeout": 10,
            "stop_tracker_timeout": 2,
            "min_announce_interval": 60,
            "use_dht_as_fallback": False,
            "prefer_udp_trackers": True,
        }

    async def initialize(self) -> None:
        if lt is None:
            logger.warning("libtorrent is not available (%s). Downloads use aria2c%s.", _LT_IMPORT_ERROR,
                           "" if aria2_backend.aria2_available() else " - BUT aria2c is not installed either!")
        else:
            try:
                settings = self._session_settings()
                try:
                    self.ses = lt.session(settings)
                except (KeyError, TypeError, ValueError) as exc:
                    # a libtorrent build that doesn't know one of the keys: drop unknown keys, keep the rest
                    logger.warning("Some libtorrent settings rejected (%s); applying the supported subset.", exc)
                    self.ses = lt.session()
                    valid = {k: v for k, v in settings.items() if self._try_setting(k, v)}
                    self.ses.apply_settings(valid)
                if TorrentEngineConfig.ENABLE_DHT:
                    self.ses.add_dht_router("router.bittorrent.com", 6881)
                    self.ses.add_dht_router("router.utorrent.com", 6881)
                    self.ses.add_dht_router("dht.transmissionbt.com", 6881)
            except Exception as exc:
                logger.error("libtorrent session failed to start (%s): falling back to aria2c.", exc)
                self.ses = None

        self._tracker_task = asyncio.create_task(self._tracker_refresh_loop())  # network fetch: never blocks startup
        logger.info("Torrent engine ready: libtorrent=%s aria2c=%s backend=%s, %d trackers known (%d live, %d attached per torrent).",
                    self.ses is not None, aria2_backend.aria2_available(), TorrentEngineConfig.BACKEND,
                    len(self.extra_trackers), len(self.live_trackers), len(self._active_trackers()))

    def _try_setting(self, key: str, value: Any) -> bool:
        try:
            lt.session().apply_settings({key: value})
            return True
        except Exception:
            logger.debug("libtorrent setting %s unsupported, skipped", key)
            return False

    def _active_trackers(self) -> List[str]:
        """Verified-live trackers (fastest first). Before the first check has finished: a small UDP-first sample."""
        C = TorrentEngineConfig
        if self.live_trackers:
            return self.live_trackers[: C.TRACKERS_LIVE_MAX] if C.TRACKERS_LIVE_MAX > 0 else list(self.live_trackers)
        limit = min(x for x in (C.TRACKERS_PER_TORRENT or 10 ** 6, C.TRACKERS_UNCHECKED_MAX or 10 ** 6))
        return select_trackers(self.extra_trackers, limit)

    async def _tracker_refresh_loop(self) -> None:
        while True:
            await self._refresh_maintained_trackers()
            await asyncio.sleep(max(600, TorrentEngineConfig.TRACKER_REFRESH_SECONDS))

    async def _refresh_maintained_trackers(self) -> None:
        urls = [u.strip() for u in (TorrentEngineConfig.TRACKER_LIST_SOURCE_URL or "").split(",") if u.strip()]
        if not urls:
            return
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            chunks = await asyncio.gather(*(tracker_tools.fetch_list(session, u) for u in urls))
        fetched = normalize_trackers([ln for chunk in chunks for ln in chunk])
        if fetched:
            merged = normalize_trackers(self.extra_trackers + fetched)
            added = len(merged) - len(self.extra_trackers)
            self.extra_trackers = merged
            try:
                with open(TorrentEngineConfig.TRACKERS_CACHE_FILE, "w", encoding="utf-8") as fh:
                    fh.write("\n".join(merged) + "\n")
            except OSError:
                pass
            logger.info("Tracker lists refreshed: +%d new, %d total (%d of %d sources answered).",
                        added, len(merged), sum(1 for c in chunks if c), len(urls))
        await self._check_live_trackers()

    async def _check_live_trackers(self) -> None:
        C = TorrentEngineConfig
        if not C.TRACKER_CHECK or not self.extra_trackers:
            return
        started = time.monotonic()
        try:
            alive = await tracker_tools.check_trackers(self.extra_trackers, concurrency=C.TRACKER_CHECK_CONCURRENCY)
        except Exception as exc:
            logger.warning("Tracker liveness check failed: %s", exc)
            return
        if len(alive) < 5:  # probably our own network problem: keep what we had
            logger.warning("Tracker check found only %d live trackers - keeping the previous list.", len(alive))
            return
        self.live_trackers = [t for t, _ in alive]
        try:
            with open(C.TRACKERS_LIVE_FILE, "w", encoding="utf-8") as fh:
                fh.write("\n".join(self.live_trackers) + "\n")
        except OSError:
            pass
        logger.info("Live trackers: %d of %d answered (checked in %.0fs, fastest %.2fs).",
                    len(alive), len(self.extra_trackers), time.monotonic() - started, alive[0][1])

    async def fetch_and_parse_torrent_file(
        self,
        torrent_url: str,
        save_dir: Optional[Path] = None,
        source_url: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Download a .torrent from its OWN host (auto-detected from the URL itself),
        validate it, and keep it as <TORRENT_DIR>/<infohash>.torrent.
        If that host fails, the same path is retried on the topic page's host (`source_url`).
        """
        save_dir = save_dir or FileStoreConfig.TORRENT_DIR
        own_origin = get_origin(torrent_url)
        candidates = [torrent_url]
        src_origin = get_origin(source_url)
        if src_origin and own_origin and src_origin != own_origin:
            candidates.append(swap_origin(torrent_url, src_origin))

        for url in candidates:
            origin = get_origin(url)
            headers = dict(SCRAPER_HEADERS)
            headers["Referer"] = (source_url if source_url and get_origin(source_url) == origin else origin + "/")
            headers["Accept"] = "application/x-bittorrent,application/octet-stream,*/*"
            for attempt in range(1, 4):  # the forum rate-limits / hiccups: try each host up to 3 times
                try:
                    timeout = aiohttp.ClientTimeout(total=30)
                    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
                        async with session.get(url, allow_redirects=True) as resp:
                            if resp.status != 200:
                                logger.warning("Torrent URL %s -> HTTP %s (attempt %d/3)", url, resp.status, attempt)
                                await asyncio.sleep(2 * attempt)
                                continue
                            raw_bytes = await resp.read()

                    meta = parse_torrent_bytes(raw_bytes)
                    if not meta:
                        logger.warning("Torrent URL %s did not return a valid .torrent (got %d bytes)", url, len(raw_bytes))
                        break  # a wrong page will not turn into a torrent by retrying; try the next host
                    save_dir.mkdir(parents=True, exist_ok=True)
                    torrent_path = save_dir / f"{meta['infohash']}.torrent"
                    torrent_path.write_bytes(raw_bytes)
                    meta.update({"torrent_file_path": str(torrent_path), "source_url": url, "base_url": origin})
                    return meta
                except Exception as exc:
                    logger.warning("Failed to fetch/parse .torrent URL (%s) attempt %d/3: %s", url, attempt, exc)
                    await asyncio.sleep(2 * attempt)
        return None

    async def download_torrent(
        self,
        file_id: str,
        torrent_url: Optional[str],
        magnet_uri: Optional[str],
        progress_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        torrent_file_path: Optional[str] = None,
        immediate: bool = False,
    ) -> Dict[str, Any]:
        """immediate=True (a user just opened the bot link) skips the concurrency semaphore: never queued."""
        if immediate:
            return await self._download_with_retries(file_id, torrent_url, magnet_uri, progress_callback, torrent_file_path)
        async with self._semaphore:
            return await self._download_with_retries(file_id, torrent_url, magnet_uri, progress_callback, torrent_file_path)

    async def _download_with_retries(self, file_id, torrent_url, magnet_uri, progress_callback, torrent_file_path) -> Dict[str, Any]:
        last_error = "Unknown error"
        for attempt in range(1, FileStoreConfig.MAX_RETRIES + 1):
            engine = self._pick_backend(attempt)
            try:
                if engine == "hybrid":
                    return await self._execute_download_hybrid(file_id, torrent_url, magnet_uri, progress_callback, torrent_file_path)
                if engine == "aria2":
                    return await self._execute_download_aria2(file_id, torrent_url, magnet_uri, progress_callback, torrent_file_path)
                return await self._execute_download(file_id, torrent_url, magnet_uri, progress_callback, torrent_file_path)
            except Exception as exc:
                last_error = str(exc)
                logger.error("Download attempt %d (%s) failed for %s: %s", attempt, engine, file_id, last_error)
                await asyncio.sleep(min(5 * attempt, 15))
        raise RuntimeError(f"All {FileStoreConfig.MAX_RETRIES} download attempts failed: {last_error}")

    def _pick_backend(self, attempt: int) -> str:
        """hybrid (both clients race) by default; libtorrent / aria2 when forced or when only one is installed;
        in auto mode every 2nd attempt uses aria2c as the other client."""
        have_lt, have_aria = self.ses is not None and lt is not None, aria2_backend.aria2_available()
        mode = TorrentEngineConfig.BACKEND
        if mode == "aria2" and have_aria:
            return "aria2"
        if mode == "libtorrent" and have_lt:
            return "libtorrent"
        if not have_lt:
            return "aria2" if have_aria else "libtorrent"  # nothing works -> libtorrent raises a clear error
        if mode == "hybrid" and have_aria:
            return "hybrid"
        if have_aria and attempt % 2 == 0:
            return "aria2"
        return "libtorrent"

    async def _execute_download_hybrid(
        self, file_id, torrent_url, magnet_uri, progress_callback, torrent_file_path=None
    ) -> Dict[str, Any]:
        """libtorrent first; when it is slow (or has failed) aria2c joins on the same torrent in its own folder.
        First client to finish wins, the loser is stopped and its partial files are deleted."""
        C = TorrentEngineConfig
        base = FileStoreConfig.DOWNLOAD_DIR / file_id
        alt = FileStoreConfig.DOWNLOAD_DIR / f"{file_id}__aria2"
        latest: Dict[str, Dict[str, Any]] = {}

        def reporter(name: str):
            async def cb(stats: Dict[str, Any]) -> None:
                latest[name] = stats
                lead = max(latest, key=lambda k: latest[k].get("downloaded_bytes", 0))
                out = dict(latest[lead])
                out["peers"] = max(s.get("peers", 0) for s in latest.values())
                out["seeds"] = max(s.get("seeds", 0) for s in latest.values())
                out["state"] = f"hybrid/{lead}" + ("+race" if len(latest) > 1 else "")
                if progress_callback:
                    await progress_callback(out)
            return cb

        tasks: Dict[str, asyncio.Task] = {
            "libtorrent": asyncio.create_task(
                self._execute_download(file_id, torrent_url, magnet_uri, reporter("libtorrent"), torrent_file_path, target_dir=base)
            )
        }

        def start_aria2(why: str) -> None:
            if "aria2" not in tasks:
                logger.info("Hybrid %s: aria2c joins the race (%s).", file_id, why)
                tasks["aria2"] = asyncio.create_task(
                    self._execute_download_aria2(file_id, torrent_url, magnet_uri, reporter("aria2"), torrent_file_path, target_dir=alt)
                )

        errors: Dict[str, BaseException] = {}
        started = slow_since = time.monotonic()
        winner: Optional[str] = None
        result: Optional[Dict[str, Any]] = None
        try:
            if C.HYBRID_RACE_AFTER <= 0:
                start_aria2("race from the start")
            while winner is None:
                for name, task in list(tasks.items()):
                    if task.done() and name not in errors:
                        if task.cancelled():
                            errors[name] = asyncio.CancelledError()
                        elif task.exception() is not None:
                            errors[name] = task.exception()
                            logger.warning("Hybrid %s: %s failed: %s", file_id, name, errors[name])
                        else:
                            winner, result = name, task.result()
                            break
                if winner:
                    break
                running = [t for t in tasks.values() if not t.done()]
                if not running:
                    if "aria2" not in tasks:
                        start_aria2("libtorrent failed")
                        continue
                    raise RuntimeError("; ".join(f"{k}: {v}" for k, v in errors.items()) or "both clients failed")
                if "aria2" not in tasks:
                    now = time.monotonic()
                    speed = latest.get("libtorrent", {}).get("download_speed", 0)
                    if speed >= C.HYBRID_BELOW_SPEED:
                        slow_since = now  # fast enough: the slow timer restarts
                    elif now - slow_since >= C.HYBRID_RACE_AFTER:
                        start_aria2(f"libtorrent at {human_speed_short(speed)}/s for {int(now - slow_since)}s")
                await asyncio.wait(running, timeout=2, return_when=asyncio.FIRST_COMPLETED)
        finally:
            losers = [t for n, t in tasks.items() if n != winner and not t.done()]
            for t in losers:
                t.cancel()
            if losers:
                await asyncio.gather(*losers, return_exceptions=True)

        assert winner and result
        if winner == "aria2":
            # bring the finished file into the normal download folder; libtorrent's partial files are dropped
            src = Path(result["file_path"])
            shutil.rmtree(base, ignore_errors=True)
            base.mkdir(parents=True, exist_ok=True)
            dest = base / src.name
            os.replace(src, dest)
            result["file_path"] = str(dest)
        shutil.rmtree(alt, ignore_errors=True)
        logger.info("Hybrid %s finished by %s in %.0fs.", file_id, winner, time.monotonic() - started)
        return result

    async def _execute_download_aria2(
        self, file_id, torrent_url, magnet_uri, progress_callback, torrent_file_path=None, target_dir=None
    ) -> Dict[str, Any]:
        if not aria2_backend.aria2_available():
            raise RuntimeError("Neither libtorrent nor aria2c is available (install aria2 or python3-libtorrent).")
        target_dir = target_dir or FileStoreConfig.DOWNLOAD_DIR / file_id
        target_dir.mkdir(parents=True, exist_ok=True)
        meta_path = None
        if torrent_file_path and Path(torrent_file_path).exists():
            meta_path = torrent_file_path
        elif torrent_url:
            parsed = await self.fetch_and_parse_torrent_file(torrent_url, FileStoreConfig.TORRENT_DIR)
            if parsed and Path(parsed["torrent_file_path"]).exists():
                meta_path = parsed["torrent_file_path"]
        source = meta_path or magnet_uri
        if not source:
            raise ValueError("Neither a valid .torrent file nor a magnet URI could be loaded.")
        await aria2_backend.run_download(
            source, target_dir, self._active_trackers(), progress_callback,
            max_peers=TorrentEngineConfig.MAX_PEERS_PER_TORRENT,
            upload_limit=TorrentEngineConfig.UPLOAD_RATE_LIMIT,
            stall_seconds=FileStoreConfig.STALL_TIMEOUT_SECONDS,
            timeout_seconds=FileStoreConfig.DOWNLOAD_TIMEOUT_SECONDS,
            listen_port=TorrentEngineConfig.LISTEN_PORTS[0],
        )
        main_file = self._locate_largest_media_file(target_dir)
        if not main_file:
            raise FileNotFoundError("No downloaded media file found inside torrent directory.")
        infohash = None
        if meta_path:
            try:
                infohash = (parse_torrent_bytes(Path(meta_path).read_bytes()) or {}).get("infohash")
            except Exception:
                infohash = None
        infohash = infohash or extract_infohash_from_magnet(magnet_uri or "") or ""
        return {
            "infohash": str(infohash).lower(),
            "file_path": str(main_file),
            "original_filename": main_file.name,
            "file_size": main_file.stat().st_size,
        }

    async def _execute_download(
        self,
        file_id: str,
        torrent_url: Optional[str],
        magnet_uri: Optional[str],
        progress_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]],
        torrent_file_path: Optional[str] = None,
        target_dir: Optional[Path] = None,
    ) -> Dict[str, Any]:
        target_dir = target_dir or FileStoreConfig.DOWNLOAD_DIR / file_id
        target_dir.mkdir(parents=True, exist_ok=True)

        params = lt.add_torrent_params()
        params.save_path = str(target_dir)
        params.storage_mode = lt.storage_mode_t.storage_mode_sparse

        parsed_meta = None
        if torrent_file_path and Path(torrent_file_path).exists():
            parsed_meta = {"torrent_file_path": torrent_file_path}  # already stored when it was ingested
        elif torrent_url:
            parsed_meta = await self.fetch_and_parse_torrent_file(torrent_url, FileStoreConfig.TORRENT_DIR)

        if parsed_meta and Path(parsed_meta["torrent_file_path"]).exists():
            params.ti = lt.torrent_info(parsed_meta["torrent_file_path"])
        elif magnet_uri:
            params = lt.parse_magnet_uri(magnet_uri)
            params.save_path = str(target_dir)
        else:
            raise ValueError("Neither a valid .torrent file nor a magnet URI could be loaded.")

        n_orig = len(params.trackers)
        existing = set(params.trackers)
        for tr in self._active_trackers():
            if tr not in existing:
                params.trackers.append(tr)
        # tier 0 = trackers the torrent/magnet shipped with, everything else tier 1
        params.tracker_tiers = [0 if i < n_orig else 1 for i in range(len(params.trackers))]

        handle = self.ses.add_torrent(params)
        try:
            handle.set_max_connections(TorrentEngineConfig.MAX_PEERS_PER_TORRENT)
            handle.set_max_uploads(-1)
            handle.set_download_limit(0)
        except Exception:
            pass
        start_time = time.monotonic()
        last_progress_time = time.monotonic()
        last_bytes_done = 0
        attached = set(params.trackers)
        last_boost = start_time

        try:
            while True:
                status = handle.status()
                if status.is_seeding or status.is_finished:
                    break
                now = time.monotonic()

                if status.errc.value() != 0:
                    raise RuntimeError(f"Libtorrent error: {status.errc.message()}")

                if (now - start_time) > FileStoreConfig.DOWNLOAD_TIMEOUT_SECONDS:
                    raise TimeoutError("Download exceeded maximum configured timeout.")

                if status.total_done > last_bytes_done:
                    last_bytes_done = status.total_done
                    last_progress_time = now
                elif (now - last_progress_time) > FileStoreConfig.STALL_TIMEOUT_SECONDS:
                    handle.force_reannounce()
                    if TorrentEngineConfig.ENABLE_DHT:
                        handle.force_dht_announce()
                    if (now - last_progress_time) > (FileStoreConfig.STALL_TIMEOUT_SECONDS * 2):
                        raise TimeoutError("Download stalled with no peer data transfer.")

                # slow torrent: add live trackers it does not have yet and re-announce (every 45 s, after 30 s)
                if (now - start_time) > 30 and (now - last_boost) > 45 and status.download_rate < TorrentEngineConfig.BOOST_BELOW_SPEED:
                    last_boost = now
                    fresh = [t for t in self._active_trackers() if t not in attached][:60]
                    for tr in fresh:
                        try:
                            handle.add_tracker({"url": tr, "tier": 1})
                            attached.add(tr)
                        except Exception:
                            break
                    try:
                        handle.force_reannounce()
                        if TorrentEngineConfig.ENABLE_DHT:
                            handle.force_dht_announce()
                    except Exception:
                        pass
                    logger.info("Slow torrent %s (%s/s): +%d trackers, re-announced.", file_id, human_speed_short(status.download_rate), len(fresh))

                progress_pct = round(status.progress * 100, 2)
                dl_rate = status.download_rate
                total_wanted = status.total_wanted or 0
                remaining_bytes = max(0, total_wanted - status.total_done)
                eta = int(remaining_bytes / dl_rate) if dl_rate > 0 else 0

                stats = {
                    "progress": progress_pct,
                    "download_speed": dl_rate,
                    "eta_seconds": eta,
                    "peers": status.num_peers,
                    "seeds": status.num_seeds,
                    "total_size": total_wanted,
                    "downloaded_bytes": status.total_done,
                    "state": str(status.state),
                }

                if progress_callback:
                    await progress_callback(stats)

                await asyncio.sleep(3)

            status = handle.status()
            infohash = str(status.info_hash).lower()
            main_file = self._locate_largest_media_file(target_dir)
            if not main_file:
                raise FileNotFoundError("No downloaded media file found inside torrent directory.")

            return {
                "infohash": infohash,
                "file_path": str(main_file),
                "original_filename": main_file.name,
                "file_size": main_file.stat().st_size,
            }
        finally:
            try:
                self.ses.remove_torrent(handle)
            except Exception:
                pass

    def _locate_largest_media_file(self, directory: Path) -> Optional[Path]:
        candidates: List[Path] = []
        for root, _, files in os.walk(directory):
            for fname in files:
                if fname.endswith((".torrent", ".resume_data", ".part")):
                    continue
                candidates.append(Path(root) / fname)
        return max(candidates, key=lambda p: p.stat().st_size) if candidates else None

    async def probe_video_duration(self, video_path: Path) -> float:
        cmd = [
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", str(video_path),
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        try:
            return float(stdout.decode().strip())
        except ValueError:
            return 0.0

    async def generate_thumbnail(self, video_path: Path, output_name: str) -> Optional[Path]:
        thumb_path = FileStoreConfig.THUMBNAIL_DIR / f"{output_name}.jpg"
        duration = await self.probe_video_duration(video_path)
        seek_point = max(5, int(duration * 0.15)) if duration > 10 else 2
        cmd = [
            "ffmpeg", "-y", "-ss", str(seek_point), "-i", str(video_path),
            "-frames:v", "1", "-q:v", "2", "-vf", "scale=320:-1", str(thumb_path),
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await proc.communicate()
        return thumb_path if thumb_path.exists() else None

    async def split_video_if_needed(self, video_path: Path, max_bytes: int) -> List[Path]:
        file_size = video_path.stat().st_size
        if file_size <= max_bytes:
            return [video_path]

        num_parts = math.ceil(file_size / max_bytes)
        duration = await self.probe_video_duration(video_path)
        parent = video_path.parent
        stem = video_path.stem
        suffix = video_path.suffix or ".mkv"

        if duration > 0:
            segment_time = max(60, int(duration / num_parts) - 5)
            pattern = str(parent / f"{stem}.part%02d{suffix}")
            cmd = [
                "ffmpeg", "-y", "-i", str(video_path), "-c", "copy", "-map", "0",
                "-f", "segment", "-segment_time", str(segment_time),
                "-reset_timestamps", "1", "-segment_start_number", "1", pattern,
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            await proc.communicate()
            parts = sorted(parent.glob(f"{stem}.part*{suffix}"))
            if parts and all(p.stat().st_size <= max_bytes * 1.05 for p in parts):
                return parts
            for p in parts:
                p.unlink(missing_ok=True)

        parts: List[Path] = []
        with open(video_path, "rb") as src:
            part_idx = 1
            while True:
                chunk = src.read(max_bytes)
                if not chunk:
                    break
                part_path = parent / f"{video_path.name}.part{part_idx:02d}"
                with open(part_path, "wb") as dst:
                    dst.write(chunk)
                parts.append(part_path)
                part_idx += 1
        return parts


torrent_engine = TorrentEngine()
