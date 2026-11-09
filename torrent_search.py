"""
Multi-site torrent search engine (22 built-in sites) used by the Torrent Search bot.

* Every site is a small provider class (JSON API sites) or a declarative HtmlProvider (HTML listing sites).
* Each site has several mirror domains: they are tried in order, so a dead / blocked domain only costs a few seconds.
* All sites are searched at the same time; one slow or broken site never blocks the others.
* Results are de-duplicated by info-hash, sorted by seeders, and every result can be turned into a magnet link
  or a .torrent file on demand (`resolve` / `fetch_torrent_file`).

Add a new site WITHOUT touching the code:
    TS_CUSTOM_SITES="MySite|https://mysite.org|/search/{q}/1/|/torrent/\\d+/ ; Other|https://o.net|/?s={qp}"
Override the mirrors of a built-in site:
    TS_MIRRORS_1337X="https://1337x.to,https://1337x.st"
Disable sites:
    TS_DISABLED_SITES="kickasstorrents,torlock"

Quick self-test (prints one line per site: status, results, seconds):
    python torrent_search.py "ubuntu"
"""
import os
import re
import sys
import json
import time
import html as htmllib
import hashlib
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, quote_plus, urljoin, urlparse, parse_qs, unquote

import aiohttp
from bs4 import BeautifulSoup

from config import TorrentEngineConfig

logger = logging.getLogger("torrent_search")

SITE_TIMEOUT = int(os.getenv("TS_SITE_TIMEOUT", "10"))          # seconds per site (all mirrors together)
REQUEST_TIMEOUT = int(os.getenv("TS_REQUEST_TIMEOUT", "6"))     # seconds per single HTTP request
MAX_PARALLEL_SITES = int(os.getenv("TS_MAX_PARALLEL", "30"))    # every site at once (was 12 -> two waves)
# --- speed: the answer is shown as soon as there is enough, slow sites never hold it back ---
DEADLINE = float(os.getenv("TS_DEADLINE", "9"))                 # hard limit for the whole search (seconds)
ENOUGH_RESULTS = int(os.getenv("TS_ENOUGH_RESULTS", "40"))      # once this many raw results came from ...
ENOUGH_SITES = int(os.getenv("TS_ENOUGH_SITES", "4"))           # ... this many sites, wait only GRACE more
GRACE = float(os.getenv("TS_GRACE", "1.5"))
HEDGE_DELAY = float(os.getenv("TS_HEDGE_DELAY", "1.2"))         # next mirror starts this long after the previous one
CACHE_TTL = int(os.getenv("TS_CACHE_TTL", "600"))               # same search again within 10 min = instant
DOWN_AFTER = int(os.getenv("TS_DOWN_AFTER", "3"))               # failures in a row before a site is skipped ...
DOWN_SECONDS = int(os.getenv("TS_DOWN_SECONDS", "600"))         # ... for this long
PROXY = os.getenv("TORRENT_SEARCH_PROXY", "").strip() or None   # e.g. http://user:pass@host:port

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
HASH40 = re.compile(r"(?<![0-9a-fA-F])([0-9a-fA-F]{40})(?![0-9a-fA-F])")
SIZE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(TiB|GiB|MiB|KiB|TB|GB|MB|KB)\b", re.I)
MAGNET_RE = re.compile(r"magnet:\?xt=urn:btih:[^\s\"'<>]+", re.I)


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

def clean(text: Any) -> str:
    return re.sub(r"\s+", " ", htmllib.unescape(str(text or ""))).strip()


def to_int(text: Any) -> int:
    digits = re.sub(r"[^\d]", "", str(text or ""))
    return int(digits) if digits else 0


def parse_size(text: Any) -> int:
    m = SIZE_RE.search(str(text or ""))
    if not m:
        return 0
    num = float(m.group(1).replace(",", "."))
    unit = m.group(2).upper().replace("IB", "B")
    mult = {"KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3, "TB": 1024 ** 4}[unit]
    return int(num * mult)


def human_size(num: int) -> str:
    if not num:
        return "?"
    size = float(num)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024
    return "?"


def _b32_to_hex(value: str) -> str:
    import base64
    try:
        return base64.b32decode(value.upper()).hex()
    except Exception:
        return ""


def infohash_from_magnet(magnet: str) -> str:
    m = re.search(r"urn:btih:([A-Za-z0-9]+)", magnet or "")
    if not m:
        return ""
    value = m.group(1)
    if len(value) == 40:
        return value.lower()
    if len(value) == 32:
        return _b32_to_hex(value)
    return ""


def title_from_magnet(magnet: str) -> str:
    try:
        dn = parse_qs(urlparse(magnet).query).get("dn", [""])[0]
        return clean(unquote(dn).replace("+", " ")) if dn else ""
    except Exception:
        return ""


def build_magnet(infohash: str, title: str = "") -> str:
    if not infohash:
        return ""
    magnet = f"magnet:?xt=urn:btih:{infohash.lower()}"
    if title:
        magnet += f"&dn={quote(title)}"
    for tracker in TorrentEngineConfig.DEFAULT_TRACKERS[:6]:
        magnet += f"&tr={quote(tracker, safe='')}"
    return magnet


def dash(query: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", query.lower()).strip("-")


def norm_words(text: str) -> List[str]:
    return [w for w in re.split(r"[^a-z0-9]+", text.lower()) if w]


def torrent_infohash(raw: bytes) -> str:
    """SHA-1 of the bencoded `info` dict of a .torrent file ('' when the data is not a valid torrent)."""
    if not raw or raw[:1] != b"d":
        return ""

    def skip(i: int) -> int:  # returns the index right after the bencoded value starting at i
        c = raw[i:i + 1]
        if c == b"i":
            return raw.index(b"e", i) + 1
        if c in (b"l", b"d"):
            i += 1
            while raw[i:i + 1] != b"e":
                i = skip(i)
            return i + 1
        colon = raw.index(b":", i)
        return colon + 1 + int(raw[i:colon])

    try:
        i = 1
        while raw[i:i + 1] != b"e":
            colon = raw.index(b":", i)
            klen = int(raw[i:colon])
            key = raw[colon + 1:colon + 1 + klen]
            vstart = colon + 1 + klen
            vend = skip(vstart)
            if key == b"info":
                return hashlib.sha1(raw[vstart:vend]).hexdigest()
            i = vend
    except Exception:
        return ""
    return ""


# --------------------------------------------------------------------------- #
# data classes
# --------------------------------------------------------------------------- #

@dataclass
class TorrentResult:
    title: str
    site: str
    size: int = 0
    seeders: int = 0
    leechers: int = 0
    magnet: str = ""
    torrent_url: str = ""
    detail_url: str = ""
    infohash: str = ""
    date: str = ""
    category: str = ""
    sources: List[str] = field(default_factory=list)   # other sites that list the same torrent
    provider: Any = None                                # the provider that found it (used to resolve it)
    base: str = ""                                      # mirror it was found on

    @property
    def key(self) -> str:
        return (self.infohash or self.detail_url or f"{self.site}:{self.title}").lower()


@dataclass
class SiteReport:
    name: str
    ok: bool
    count: int = 0
    seconds: float = 0.0
    error: str = ""


@dataclass
class SearchOutcome:
    query: str
    results: List[TorrentResult]
    report: List[SiteReport]
    seconds: float


class SiteError(Exception):
    pass


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #

class Http:
    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None

    async def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9", "Accept": "*/*"},
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                connector=aiohttp.TCPConnector(limit=60, ssl=False),
            )
        return self._session

    async def _request(self, method: str, url: str, **kw) -> bytes:
        sess = await self.session()
        try:
            async with sess.request(method, url, proxy=PROXY, allow_redirects=True, **kw) as resp:
                if resp.status != 200:
                    raise SiteError(f"HTTP {resp.status}")
                return await resp.read()
        except SiteError:
            raise
        except asyncio.TimeoutError:
            raise SiteError("timeout")
        except aiohttp.ClientError as exc:
            raise SiteError(type(exc).__name__)

    async def text(self, url: str) -> str:
        return (await self._request("GET", url)).decode("utf-8", "ignore")

    async def bytes(self, url: str) -> bytes:
        return await self._request("GET", url)

    async def json(self, url: str) -> Any:
        raw = await self._request("GET", url)
        try:
            return json.loads(raw.decode("utf-8", "ignore"))
        except ValueError:
            raise SiteError("not JSON (blocked?)")

    async def post_json(self, url: str, body: Dict[str, Any]) -> Any:
        raw = await self._request("POST", url, json=body)
        try:
            return json.loads(raw.decode("utf-8", "ignore"))
        except ValueError:
            raise SiteError("not JSON (blocked?)")

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# --------------------------------------------------------------------------- #
# providers
# --------------------------------------------------------------------------- #

def _env_mirrors(name: str, default: List[str]) -> List[str]:
    env = os.getenv("TS_MIRRORS_" + re.sub(r"[^A-Z0-9]", "_", name.upper()), "")
    mirrors = [m.strip().rstrip("/") for m in env.split(",") if m.strip()]
    return mirrors or [m.rstrip("/") for m in default]


class Provider:
    name = "Site"
    mirrors: List[str] = []
    kind = "general"

    def __init__(self) -> None:
        self.mirrors = _env_mirrors(self.name, self.mirrors)
        self.fail_streak = 0        # whole-site failures in a row (circuit breaker, see TorrentSearch.search)
        self.skip_until = 0.0
        self._last_good: Optional[str] = None

    def _ordered_mirrors(self) -> List[str]:
        """The mirror that answered last time goes first."""
        if self._last_good in self.mirrors:
            return [self._last_good] + [m for m in self.mirrors if m != self._last_good]
        return list(self.mirrors)

    async def search(self, http: Http, query: str, limit: int) -> List[TorrentResult]:
        """Mirrors are RACED, not tried one after another: the next mirror starts HEDGE_DELAY after the previous one
        (earlier if it failed); the first answer wins and the others are cancelled."""
        mirrors = self._ordered_mirrors()
        if not mirrors:
            raise SiteError("no mirror configured")

        async def attempt(base: str, delay: float):
            if delay:
                await asyncio.sleep(delay)
            res = await self._search_mirror(http, base, query, limit)
            for r in res:
                r.provider, r.base = self, base
            return base, res

        tasks = [asyncio.create_task(attempt(b, i * HEDGE_DELAY)) for i, b in enumerate(mirrors)]
        last: Optional[Exception] = None
        try:
            for fut in asyncio.as_completed(tasks):
                try:
                    base, results = await fut
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    last = exc
                    logger.debug("%s mirror failed: %s", self.name, exc)
                    continue
                self._last_good = base
                return results[:limit]
            raise SiteError(str(last) if last else "no mirror answered")
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _search_mirror(self, http: Http, base: str, query: str, limit: int) -> List[TorrentResult]:
        raise NotImplementedError

    async def resolve(self, http: Http, r: TorrentResult) -> None:
        """Fill r.magnet (and r.infohash) from whatever the result already knows, fetching its page if needed."""
        if r.magnet:
            r.infohash = r.infohash or infohash_from_magnet(r.magnet)
            return
        if r.infohash:
            r.magnet = build_magnet(r.infohash, r.title)
            return
        if r.detail_url:
            pages = [r.detail_url]
            if r.base:  # same page on the other mirrors, in case this one died meanwhile
                path = r.detail_url[len(r.base):] if r.detail_url.startswith(r.base) else ""
                if path:
                    pages += [m + path for m in self.mirrors if m != r.base]
            for page in pages:
                try:
                    text = await http.text(page)
                except Exception:
                    continue
                m = MAGNET_RE.search(htmllib.unescape(text))
                if m:
                    r.magnet = m.group(0)
                    r.infohash = infohash_from_magnet(r.magnet)
                    return
                soup = BeautifulSoup(text, "html.parser")
                for a in soup.find_all("a", href=True):
                    if re.search(r"\.torrent(\?|$)", a["href"]):
                        r.torrent_url = r.torrent_url or urljoin(page, a["href"])
                        break
                h = HASH40.search(text)
                if h:
                    r.infohash = h.group(1).lower()
                    r.magnet = build_magnet(r.infohash, r.title)
                    return
                break
        if r.torrent_url:
            try:
                raw = await http.bytes(r.torrent_url)
                h = torrent_infohash(raw)
                if h:
                    r.infohash = h
                    r.magnet = build_magnet(h, r.title)
            except Exception as exc:
                logger.debug("torrent file resolve failed for %s: %s", r.title, exc)


# ---- JSON / API sites ------------------------------------------------------ #

class Knaben(Provider):
    """Knaben is itself a search engine over dozens of trackers - the biggest single source."""
    name = "Knaben"
    mirrors = ["https://api.knaben.org", "https://api.knaben.eu"]

    async def _search_mirror(self, http, base, query, limit):
        data = await http.post_json(
            f"{base}/v1",
            {"search_type": "100%", "search_field": "title", "query": query,
             "order_by": "seeders", "order_direction": "desc", "size": max(limit, 30)},
        )
        out = []
        for hit in (data or {}).get("hits") or []:
            magnet = hit.get("magnetUrl") or ""
            ih = (hit.get("hash") or "").lower() or infohash_from_magnet(magnet)
            if not magnet and not ih:
                continue
            tracker = hit.get("tracker") or ""
            out.append(TorrentResult(
                title=clean(hit.get("title")), site=f"Knaben/{tracker}" if tracker else "Knaben",
                size=int(hit.get("bytes") or 0), seeders=int(hit.get("seeders") or 0),
                leechers=int(hit.get("peers") or 0), magnet=magnet or build_magnet(ih, hit.get("title") or ""),
                infohash=ih, detail_url=hit.get("details") or "", date=str(hit.get("date") or "")[:10],
                category=str(hit.get("category") or ""),
            ))
        return out


class PirateBay(Provider):
    name = "ThePirateBay"
    mirrors = ["https://apibay.org"]

    async def _search_mirror(self, http, base, query, limit):
        data = await http.json(f"{base}/q.php?q={quote(query)}&cat=0")
        out = []
        for row in data if isinstance(data, list) else []:
            ih = str(row.get("info_hash") or "").lower()
            if not ih or set(ih) == {"0"}:  # "No results returned" placeholder row
                continue
            out.append(TorrentResult(
                title=clean(row.get("name")), site=self.name, size=to_int(row.get("size")),
                seeders=to_int(row.get("seeders")), leechers=to_int(row.get("leechers")),
                infohash=ih, magnet=build_magnet(ih, row.get("name") or ""),
                detail_url=f"https://thepiratebay.org/description.php?id={row.get('id')}",
                date=time.strftime("%Y-%m-%d", time.gmtime(to_int(row.get("added")))) if row.get("added") else "",
            ))
        return out


class YTS(Provider):
    name = "YTS"
    kind = "movies"
    mirrors = ["https://yts.mx", "https://yts.lt", "https://yts.am", "https://yts.rs"]

    async def _search_mirror(self, http, base, query, limit):
        data = await http.json(f"{base}/api/v2/list_movies.json?query_term={quote(query)}&limit=20&sort_by=seeds")
        out = []
        for movie in ((data or {}).get("data") or {}).get("movies") or []:
            for t in movie.get("torrents") or []:
                title = f"{movie.get('title_long') or movie.get('title')} [{t.get('quality')} {t.get('type', '')}]".replace("  ", " ")
                ih = str(t.get("hash") or "").lower()
                out.append(TorrentResult(
                    title=clean(title), site=self.name, size=int(t.get("size_bytes") or 0),
                    seeders=int(t.get("seeds") or 0), leechers=int(t.get("peers") or 0),
                    infohash=ih, magnet=build_magnet(ih, title), torrent_url=t.get("url") or "",
                    detail_url=movie.get("url") or "", date=str(t.get("date_uploaded") or "")[:10], category="Movies",
                ))
        return out


class AnimeTosho(Provider):
    name = "AnimeTosho"
    kind = "anime"
    mirrors = ["https://feed.animetosho.org"]

    async def _search_mirror(self, http, base, query, limit):
        data = await http.json(f"{base}/json?q={quote(query)}")
        out = []
        for row in data if isinstance(data, list) else []:
            magnet = row.get("magnet_uri") or ""
            ih = str(row.get("info_hash") or "").lower() or infohash_from_magnet(magnet)
            if not magnet and not ih:
                continue
            out.append(TorrentResult(
                title=clean(row.get("title")), site=self.name, size=int(row.get("total_size") or 0),
                seeders=int(row.get("seeders") or 0), leechers=int(row.get("leechers") or 0),
                infohash=ih, magnet=magnet or build_magnet(ih, row.get("title") or ""),
                torrent_url=row.get("torrent_url") or "", detail_url=row.get("link") or "", category="Anime",
            ))
        return out


class InternetArchive(Provider):
    """Legal / public-domain media. Archive.org has no seeders count (web seeds), so it always sorts last."""
    name = "InternetArchive"
    kind = "legal"
    mirrors = ["https://archive.org"]

    async def _search_mirror(self, http, base, query, limit):
        q = f'title:({query}) AND (mediatype:movies OR mediatype:audio OR mediatype:software OR mediatype:texts)'
        url = (f"{base}/advancedsearch.php?q={quote(q)}&fl%5B%5D=identifier&fl%5B%5D=title&fl%5B%5D=item_size"
               f"&sort%5B%5D=downloads+desc&rows=15&output=json")
        data = await http.json(url)
        out = []
        for doc in ((data or {}).get("response") or {}).get("docs") or []:
            ident = doc.get("identifier")
            if not ident:
                continue
            out.append(TorrentResult(
                title=clean(doc.get("title") or ident), site=self.name, size=to_int(doc.get("item_size")),
                torrent_url=f"{base}/download/{ident}/{ident}_archive.torrent", detail_url=f"{base}/details/{ident}",
                category="Public domain",
            ))
        return out


# ---- HTML listing sites ----------------------------------------------------- #

class HtmlProvider(Provider):
    """
    Declarative HTML site. `path` is appended to the mirror; placeholders: {q} %20-encoded, {qp} +-encoded,
    {qd} dash-separated, {first} first letter. Rows are read from `row_selector` (or found around every magnet link);
    a row either carries a magnet link directly, or a link to its detail page matching `detail_re`
    (the magnet is then fetched only when the user opens the result).
    """
    path = "/search/{q}/"
    row_selector = ""
    detail_re = ""
    title_sel = ""
    seed_sel = ""
    leech_sel = ""
    size_sel = ""

    def __init__(self, **overrides: Any) -> None:
        for k, v in overrides.items():
            setattr(self, k, v)
        super().__init__()

    def _url(self, base: str, query: str) -> str:
        m = re.search(r"[a-z0-9]", query.lower())
        first = m.group(0) if m else ""
        return base + self.path.format(q=quote(query), qp=quote_plus(query), qd=dash(query), first=first)

    async def _search_mirror(self, http, base, query, limit):
        text = await http.text(self._url(base, query))
        if len(text) < 300:
            raise SiteError("empty page")
        low = text[:6000].lower()
        if "just a moment" in low or "cf-browser-verification" in low or "attention required" in low:
            raise SiteError("Cloudflare challenge")
        return self.parse(text, base, limit)

    # -- parsing ------------------------------------------------------------ #
    @staticmethod
    def _row_of(anchor):
        node = anchor
        for _ in range(7):
            parent = node.parent
            if parent is None or parent.name in ("body", "html", "[document]"):
                break
            if len(parent.select('a[href^="magnet:"]')) > 1:
                break
            node = parent
            if node.name in ("tr", "li", "article"):
                break
        return node

    def parse(self, text: str, base: str, limit: int) -> List[TorrentResult]:
        soup = BeautifulSoup(text, "html.parser")
        rows = soup.select(self.row_selector) if self.row_selector else []
        if not rows:
            rows = [self._row_of(a) for a in soup.select('a[href^="magnet:"]')]
        if not rows and self.detail_re:
            rows = soup.find_all("tr") or soup.find_all("li")
        out: List[TorrentResult] = []
        seen = set()
        for row in rows:
            r = self._parse_row(row, base)
            if r and r.key not in seen:
                seen.add(r.key)
                out.append(r)
            if len(out) >= limit * 2:
                break
        return out

    def _num(self, row, selector: str, class_hint: str) -> int:
        node = row.select_one(selector) if selector else None
        if node is None and not selector:
            node = row.select_one(f'[class*="{class_hint}"]')
        return to_int(node.get_text(" ", strip=True)) if node is not None else -1

    def _parse_row(self, row, base: str) -> Optional[TorrentResult]:
        mag_a = row.select_one('a[href^="magnet:"]')
        magnet = htmllib.unescape(mag_a["href"]).strip() if mag_a else ""
        title, detail = "", ""
        if self.title_sel:
            t = row.select_one(self.title_sel)
            if t is not None:
                title = clean(t.get_text(" "))
                if t.name == "a" and t.get("href"):
                    detail = urljoin(base + "/", t["href"])
        if self.detail_re and not detail:
            for a in row.find_all("a", href=True):
                if re.search(self.detail_re, a["href"]) and clean(a.get_text(" ")):
                    detail = urljoin(base + "/", a["href"])
                    title = title or clean(a.get_text(" "))
                    break
        torrent_url = ""
        for a in row.find_all("a", href=True):
            if re.search(r"\.torrent(\?|$)", a["href"]):
                torrent_url = urljoin(base + "/", htmllib.unescape(a["href"]))
                break
        infohash = infohash_from_magnet(magnet)
        if not infohash:
            for candidate in (torrent_url, detail):
                m = HASH40.search(candidate or "")
                if m:
                    infohash = m.group(1).lower()
                    break
        if magnet and not title:
            title = title_from_magnet(magnet)
        if not title:
            title = clean(row.get_text(" "))[:120]
        if not title or not (magnet or detail or torrent_url or infohash):
            return None

        text = row.get_text(" ", strip=True)
        size_node = row.select_one(self.size_sel) if self.size_sel else None
        size = parse_size(size_node.get_text(" ", strip=True)) if size_node is not None else 0
        size = size or parse_size(text)
        seeders = self._num(row, self.seed_sel, "seed")
        leechers = self._num(row, self.leech_sel, "leech")
        if seeders < 0 or leechers < 0:  # no usable class names: take the first two plain-number table cells
            ints = [to_int(td.get_text(strip=True)) for td in row.find_all("td")
                    if re.fullmatch(r"\d[\d,.]*", td.get_text(strip=True) or "")]
            seeders = ints[0] if seeders < 0 and ints else max(seeders, 0)
            leechers = ints[1] if leechers < 0 and len(ints) > 1 else max(leechers, 0)
        return TorrentResult(
            title=title, site=self.name, size=size, seeders=seeders, leechers=leechers,
            magnet=magnet, torrent_url=torrent_url, detail_url=detail, infohash=infohash,
        )


def _site(name, mirrors, path, kind="general", **spec):
    return type(name, (HtmlProvider,), {"name": name, "mirrors": mirrors, "path": path, "kind": kind, **spec})


Nyaa = _site("Nyaa", ["https://nyaa.si", "https://nyaa.land"], "/?f=0&c=0_0&q={qp}&s=seeders&o=desc", "anime",
             row_selector="table.torrent-list tbody tr")
TorrentGalaxy = _site("TorrentGalaxy", ["https://torrentgalaxy.to", "https://tgx.rs", "https://torrentgalaxy.mx"],
                      "/torrents.php?search={qp}&sort=seeders&order=desc")
EZTV = _site("EZTV", ["https://eztvx.to", "https://eztv.re", "https://eztv.wf"], "/search/{qd}", "tv",
             row_selector="tr.forum_header_border")
MagnetDL = _site("MagnetDL", ["https://www.magnetdl.com"], "/{first}/{qd}/", row_selector="table.download tbody tr")
BitSearch = _site("BitSearch", ["https://bitsearch.to"], "/search?q={qp}&sort=seeders")
Glodls = _site("GloTorrents", ["https://glodls.to"], "/search_results.php?search={qp}&sort=seeders&order=desc",
               row_selector="tr.t-row")
KickassTorrents = _site("KickassTorrents", ["https://kickasstorrents.to", "https://kat.am"], "/usearch/{q}/")
Rutor = _site("Rutor", ["https://rutor.info", "https://rutor.is"], "/search/0/0/000/0/{q}", row_selector="tr.gai, tr.tum",
              seed_sel="span.green", leech_sel="span.red")
TokyoTosho = _site("TokyoTosho", ["https://www.tokyotosho.info"], "/search.php?terms={qp}&type=0", "anime")
X1337 = _site("1337x", ["https://1337x.to", "https://1337x.st", "https://1337x.gd", "https://x1337x.ws"],
              "/sort-search/{q}/seeders/desc/1/", row_selector="table.table-list tbody tr",
              detail_re=r"^/torrent/\d+/", seed_sel="td.seeds", leech_sel="td.leeches", size_sel="td.size")
LimeTorrents = _site("LimeTorrents", ["https://www.limetorrents.lol", "https://www.limetorrents.info"],
                     "/search/all/{qd}/seeds/1/", row_selector="table.table2 tr", detail_re=r"-torrent-\d+\.html",
                     seed_sel="td.tdseed", leech_sel="td.tdleech")
Torlock = _site("Torlock", ["https://www.torlock.com", "https://www.torlock2.com"], "/all/torrents/{qd}.html?sort=seeds",
                row_selector="table.table tr", detail_re=r"^/torrent/\d+/")
TorrentDownload = _site("TorrentDownload", ["https://www.torrentdownload.info"], "/search?q={qp}",
                        row_selector="table.table2 tr", detail_re=r"^/[0-9A-Fa-f]{40}/")
TorrentFunk = _site("TorrentFunk", ["https://www.torrentfunk.com", "https://www.torrentfunk2.com"], "/all/torrents/{qd}.html",
                    row_selector="table.tmain tr", detail_re=r"^/torrent/\d+/")
YourBittorrent = _site("YourBittorrent", ["https://yourbittorrent.com", "https://yourbittorrent2.com"], "/?q={qp}",
                       row_selector="table tr", detail_re=r"^/torrent/\d+/")
IDope = _site("iDope", ["https://idope.se", "https://idope.top"], "/torrent-list/{qd}/", row_selector="div.resultdiv",
              detail_re=r"^/torrent/")
TorrentProject2 = _site("TorrentProject2", ["https://torrentproject2.com", "https://torrentproject.cc"], "/?t={qp}",
                        row_selector="div.torrent", detail_re=r"^/[0-9A-Fa-f]{40}/|^/\w")

BUILTIN_PROVIDERS = [
    Knaben, PirateBay, YTS, AnimeTosho, InternetArchive, Nyaa, TorrentGalaxy, EZTV, MagnetDL, BitSearch, Glodls,
    KickassTorrents, Rutor, TokyoTosho, X1337, LimeTorrents, Torlock, TorrentDownload, TorrentFunk, YourBittorrent,
    IDope, TorrentProject2,
]


def _custom_providers() -> List[Provider]:
    """TS_CUSTOM_SITES="Name|https://base|/search/{q}/|detail_regex ; Name2|https://base2|/?s={qp}" """
    out: List[Provider] = []
    for chunk in (os.getenv("TS_CUSTOM_SITES") or "").split(";"):
        parts = [p.strip() for p in chunk.strip().split("|")]
        if len(parts) < 3 or not parts[0] or not parts[1].startswith("http"):
            continue
        try:
            out.append(HtmlProvider(name=parts[0], mirrors=[parts[1].rstrip("/")], path=parts[2],
                                    detail_re=parts[3] if len(parts) > 3 else ""))
        except Exception as exc:
            logger.warning("Bad TS_CUSTOM_SITES entry %r: %s", chunk, exc)
    return out


# --------------------------------------------------------------------------- #
# the engine
# --------------------------------------------------------------------------- #

class TorrentSearch:
    def __init__(self) -> None:
        self.http = Http()
        disabled = {s.strip().lower() for s in (os.getenv("TS_DISABLED_SITES") or "").split(",") if s.strip()}
        providers = [cls() for cls in BUILTIN_PROVIDERS] + _custom_providers()
        self.providers: List[Provider] = [p for p in providers if p.name.lower() not in disabled]
        self._cache: Dict[Tuple, Tuple[float, SearchOutcome]] = {}

    @property
    def site_names(self) -> List[str]:
        return [p.name for p in self.providers]

    async def close(self) -> None:
        await self.http.close()

    async def search(self, query: str, sites: Optional[List[str]] = None, limit: int = 80, strict: bool = True) -> SearchOutcome:
        query = clean(query)
        started = time.monotonic()
        wanted = {s.lower() for s in sites} if sites else None
        ckey = (query.lower(), tuple(sorted(wanted)) if wanted else None, limit, strict)
        hit = self._cache.get(ckey)
        if hit and time.monotonic() - hit[0] < CACHE_TTL:
            return SearchOutcome(hit[1].query, hit[1].results, hit[1].report, 0.0)
        providers = [p for p in self.providers if not wanted or p.name.lower() in wanted]
        now = time.monotonic()
        live = [p for p in providers if p.skip_until <= now] or providers  # sites that keep failing sit out for a while
        gate = asyncio.Semaphore(MAX_PARALLEL_SITES)
        per_site = max(20, limit)

        async def run(p: Provider):
            t0 = time.monotonic()
            async with gate:
                try:
                    res = await asyncio.wait_for(p.search(self.http, query, per_site), SITE_TIMEOUT)
                    p.fail_streak, p.skip_until = 0, 0.0
                    return p, res, SiteReport(p.name, True, len(res), time.monotonic() - t0)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    p.fail_streak += 1
                    if p.fail_streak >= DOWN_AFTER:
                        p.skip_until = time.monotonic() + DOWN_SECONDS
                    err = "timeout" if isinstance(exc, asyncio.TimeoutError) else (str(exc)[:60] or type(exc).__name__)
                    return p, [], SiteReport(p.name, False, 0, time.monotonic() - t0, err)

        # Collect answers as they arrive. Stop when everything is in, OR enough results are in and only GRACE seconds
        # passed, OR the hard DEADLINE is reached - a slow site can no longer hold the whole search back.
        tasks = {asyncio.create_task(run(p)): p for p in live}
        done: List[Tuple[Provider, List[TorrentResult], SiteReport]] = []
        pending = set(tasks)
        raw_count, ok_sites, enough_at = 0, 0, None
        deadline = started + DEADLINE
        while pending:
            now = time.monotonic()
            wait_for = deadline - now
            if enough_at is not None:
                wait_for = min(wait_for, enough_at + GRACE - now)
            if wait_for <= 0:
                break
            finished, pending = await asyncio.wait(pending, timeout=wait_for, return_when=asyncio.FIRST_COMPLETED)
            for t in finished:
                p, res, rep = t.result()
                done.append((p, res, rep))
                raw_count += len(res)
                ok_sites += 1 if res else 0
            if enough_at is None and raw_count >= ENOUGH_RESULTS and ok_sites >= ENOUGH_SITES:
                enough_at = time.monotonic()
        for t in pending:  # too slow: dropped from this search (they are not marked as broken)
            t.cancel()
            done.append((tasks[t], [], SiteReport(tasks[t].name, False, 0, time.monotonic() - started, "too slow - skipped")))
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        for p in providers:
            if p not in live:
                done.append((p, [], SiteReport(p.name, False, 0, 0.0, "down - retrying later")))
        merged: Dict[str, TorrentResult] = {}
        raw_all: List[TorrentResult] = []
        report: List[SiteReport] = []
        words = [w for w in norm_words(query) if len(w) > 1 or w.isdigit()]
        for _p, res, rep in done:
            report.append(rep)
            raw_all.extend(res)
        pool = raw_all
        if strict and words:
            matching = [r for r in raw_all if all(w in set(norm_words(r.title)) or w in r.title.lower() for w in words)]
            pool = matching or raw_all
        for r in pool:
            known = merged.get(r.key)
            if not known:
                merged[r.key] = r
                continue
            if r.seeders > known.seeders:
                r.sources = known.sources + [known.site]
                merged[r.key] = r
            elif r.site not in known.sources and r.site != known.site:
                known.sources.append(r.site)
            if not known.magnet and r.magnet:
                known.magnet = r.magnet
        results = sorted(merged.values(), key=lambda r: (r.seeders, r.size), reverse=True)[:limit]
        report.sort(key=lambda x: (not x.ok, -x.count, x.name))
        outcome = SearchOutcome(query, results, report, time.monotonic() - started)
        if results:
            if len(self._cache) > 100:
                self._cache.clear()
            self._cache[ckey] = (time.monotonic(), outcome)
        return outcome

    async def resolve(self, r: TorrentResult) -> TorrentResult:
        """Make sure `r` has a magnet link (opens the result's page when the listing had none)."""
        prov = r.provider or next((p for p in self.providers if r.site.startswith(p.name)), None)
        if prov is None:
            prov = Provider()
        await asyncio.wait_for(prov.resolve(self.http, r), SITE_TIMEOUT)
        return r

    async def fetch_torrent_file(self, r: TorrentResult) -> Optional[Tuple[str, bytes]]:
        """(filename, bytes) of a valid .torrent for this result, or None when no cache/site has it."""
        urls: List[str] = []
        if r.torrent_url:
            urls.append(r.torrent_url)
        if r.infohash:
            h = r.infohash.upper()
            urls += [f"https://itorrents.org/torrent/{h}.torrent", f"https://torrage.info/torrent.php?h={h}"]
        for url in urls:
            try:
                raw = await self.http.bytes(url)
            except Exception:
                continue
            ih = torrent_infohash(raw)
            if ih:
                r.infohash = r.infohash or ih
                name = re.sub(r"[^\w.\- ]+", "_", r.title)[:80].strip() or ih
                return f"{name}.torrent", raw
        return None


# --------------------------------------------------------------------------- #
# CLI self-test:  python torrent_search.py "ubuntu"
# --------------------------------------------------------------------------- #

async def _cli(query: str) -> None:
    engine = TorrentSearch()
    try:
        out = await engine.search(query)
    finally:
        await engine.close()
    print(f"\n{len(engine.providers)} sites, {len(out.results)} unique results in {out.seconds:.1f}s\n")
    for rep in out.report:
        mark = "OK  " if rep.ok else "FAIL"
        print(f"  {mark} {rep.name:<18} {rep.count:>3} results  {rep.seconds:>5.1f}s  {rep.error}")
    print()
    for i, r in enumerate(out.results[:15], 1):
        print(f"{i:>2}. {r.title[:70]}  [{human_size(r.size)}  S:{r.seeders} L:{r.leechers}  {r.site}]")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(_cli(" ".join(sys.argv[1:]) or "ubuntu"))
