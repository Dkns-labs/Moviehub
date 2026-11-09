"""Tracker helpers: fetch public lists (GitHub raw, with jsDelivr mirror fallback) and keep only LIVE trackers.

Why: attaching thousands of trackers - most of them dead - makes every announce round slow and hides the good
ones. We probe each tracker once (UDP BEP-15 "connect" handshake / HTTP request), keep the ones that answer and
order them by response time, so the fastest trackers are announced to first.
"""
import asyncio
import logging
import random
import re
import struct
import time
from typing import Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import aiohttp

logger = logging.getLogger("tracker_tools")

_RAW_RE = re.compile(r"^https://raw\.githubusercontent\.com/([^/]+)/([^/]+)/([^/]+)/(.+)$")


def mirror_urls(url: str) -> List[str]:
    """The URL plus mirrors for GitHub raw links (used when raw.githubusercontent.com is blocked / slow)."""
    out = [url]
    m = _RAW_RE.match(url)
    if m:
        user, repo, branch, path = m.groups()
        out.append(f"https://cdn.jsdelivr.net/gh/{user}/{repo}@{branch}/{path}")
        out.append(f"https://fastly.jsdelivr.net/gh/{user}/{repo}@{branch}/{path}")
    return out


async def fetch_list(session: aiohttp.ClientSession, url: str) -> List[str]:
    """Lines of one tracker list; tries the URL, then its mirrors. [] when none answers."""
    for candidate in mirror_urls(url):
        try:
            async with session.get(candidate) as resp:
                if resp.status == 200:
                    return (await resp.text()).splitlines()
        except Exception as exc:
            logger.debug("Tracker list %s failed: %s", candidate, exc)
    logger.warning("Tracker list unreachable (also via mirrors): %s", url)
    return []


# ------------------------------------------------------------------ liveness probes

class _UdpProbe(asyncio.DatagramProtocol):
    def __init__(self, tid: int, fut: "asyncio.Future[bool]"):
        self.tid, self.fut = tid, fut

    def datagram_received(self, data: bytes, addr) -> None:
        if len(data) >= 8 and not self.fut.done():
            action, tid = struct.unpack(">II", data[:8])
            if action == 0 and tid == self.tid:  # connect response with our transaction id
                self.fut.set_result(True)

    def error_received(self, exc) -> None:
        if not self.fut.done():
            self.fut.set_result(False)


async def probe_udp(host: str, port: int, timeout: float = 3.0) -> Optional[float]:
    """Seconds the UDP tracker took to answer a BEP-15 connect request, or None (dead / no answer)."""
    loop = asyncio.get_running_loop()
    fut: "asyncio.Future[bool]" = loop.create_future()
    tid = random.getrandbits(32)
    start = time.monotonic()
    try:
        transport, _ = await asyncio.wait_for(
            loop.create_datagram_endpoint(lambda: _UdpProbe(tid, fut), remote_addr=(host, port)), timeout
        )
    except Exception:
        return None
    try:
        transport.sendto(struct.pack(">QII", 0x41727101980, 0, tid))
        ok = await asyncio.wait_for(fut, timeout)
        return (time.monotonic() - start) if ok else None
    except Exception:
        return None
    finally:
        transport.close()


async def probe_http(session: aiohttp.ClientSession, url: str, timeout: float = 5.0) -> Optional[float]:
    """Any real HTTP answer below 500 (even a 'bad request' from the tracker) means the tracker is up."""
    start = time.monotonic()
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout), allow_redirects=False) as resp:
            return (time.monotonic() - start) if resp.status < 500 else None
    except Exception:
        return None


async def check_trackers(
    trackers: Iterable[str], concurrency: int = 250, udp_timeout: float = 3.0, http_timeout: float = 5.0
) -> List[Tuple[str, float]]:
    """[(tracker, seconds)] of the trackers that answered, fastest first."""
    sem = asyncio.Semaphore(max(1, concurrency))
    results: List[Tuple[str, float]] = []
    connector = aiohttp.TCPConnector(limit=concurrency, ssl=False)

    async with aiohttp.ClientSession(connector=connector) as session:
        async def one(tr: str) -> None:
            u = urlparse(tr)
            async with sem:
                if u.scheme.lower() == "udp":
                    if not u.hostname or not u.port:
                        return
                    dt = await probe_udp(u.hostname, u.port, udp_timeout)
                else:
                    dt = await probe_http(session, tr, http_timeout)
            if dt is not None:
                results.append((tr, dt))

        await asyncio.gather(*(one(t) for t in trackers), return_exceptions=True)
    results.sort(key=lambda x: x[1])
    return results
