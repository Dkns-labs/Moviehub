"""aria2c as a second BitTorrent client.

Used when libtorrent cannot be imported / started, or when a libtorrent attempt failed (see TORRENT_BACKEND).
aria2c speaks the same BitTorrent protocol (trackers, DHT, PEX, LPD, uTP-less TCP) and is a single apt package.
"""
import asyncio
import logging
import re
import shutil
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

logger = logging.getLogger("aria2_backend")

_UNITS = {"B": 1, "KIB": 1024, "MIB": 1024 ** 2, "GIB": 1024 ** 3, "TIB": 1024 ** 4,
          "KB": 1000, "MB": 1000 ** 2, "GB": 1000 ** 3}
# [#2f2f2f 1.2GiB/3.0GiB(40%) CN:50 SD:10 DL:5.2MiB ETA:3m2s]   (a magnet's metadata phase has no percentage)
_LINE_RE = re.compile(
    r"\[#\w+\s+(?P<done>[\d.]+\s?[KMGT]?i?B)/(?P<total>[\d.]+\s?[KMGT]?i?B)(?:\((?P<pct>\d+)%\))?"
    r"(?:\s+CN:(?P<cn>\d+))?(?:\s+SD:(?P<sd>\d+))?(?:\s+DL:(?P<dl>[\d.]+\s?[KMGT]?i?B))?(?:\s+ETA:(?P<eta>[\dhms]+))?",
    re.I,
)


def aria2_path() -> Optional[str]:
    return shutil.which("aria2c")


def aria2_available() -> bool:
    return aria2_path() is not None


def _to_bytes(text: Optional[str]) -> int:
    if not text:
        return 0
    m = re.match(r"([\d.]+)\s?([KMGT]?i?B)", text.strip(), re.I)
    if not m:
        return 0
    return int(float(m.group(1)) * _UNITS.get(m.group(2).upper(), 1))


def _eta_seconds(text: Optional[str]) -> int:
    if not text:
        return 0
    total = 0
    for num, unit in re.findall(r"(\d+)([hms])", text):
        total += int(num) * {"h": 3600, "m": 60, "s": 1}[unit]
    return total


def parse_progress(line: str) -> Optional[Dict[str, Any]]:
    """One aria2 summary line -> the same stats dict the libtorrent loop reports. None = not a progress line."""
    m = _LINE_RE.search(line)
    if not m:
        return None
    done, total = _to_bytes(m.group("done")), _to_bytes(m.group("total"))
    pct = float(m.group("pct")) if m.group("pct") else (round(done * 100 / total, 2) if total else 0.0)
    return {
        "progress": pct,
        "download_speed": _to_bytes(m.group("dl")),
        "eta_seconds": _eta_seconds(m.group("eta")),
        "peers": int(m.group("cn") or 0),
        "seeds": int(m.group("sd") or 0),
        "total_size": total,
        "downloaded_bytes": done,
        "state": "aria2",
    }


def build_command(
    source: str, target_dir: Path, trackers: List[str], *, max_peers: int, upload_limit: int, stall_seconds: int,
    listen_port: int,
) -> List[str]:
    cmd = [
        aria2_path() or "aria2c",
        f"--dir={target_dir}",
        "--seed-time=0",                       # stop right after the download is complete
        "--enable-dht=true", "--enable-dht6=true",
        "--bt-enable-lpd=true", "--enable-peer-exchange=true",
        f"--bt-max-peers={max_peers}",
        "--bt-request-peer-speed-limit=10M",
        "--bt-tracker-connect-timeout=10", "--bt-tracker-timeout=15",
        f"--bt-stop-timeout={max(60, stall_seconds)}",   # no data for this long -> give up (-> retry / other engine)
        f"--max-overall-upload-limit={upload_limit or 0}",
        f"--listen-port={listen_port}-{listen_port + 10}",
        f"--dht-listen-port={listen_port}-{listen_port + 10}",
        "--file-allocation=none", "--allow-overwrite=true", "--auto-file-renaming=false",
        "--check-integrity=false", "--bt-save-metadata=false", "--follow-torrent=true",
        "--summary-interval=1", "--console-log-level=warn", "--download-result=hide",
    ]
    if trackers:
        cmd.append("--bt-tracker=" + ",".join(trackers))
    cmd.append(source)
    return cmd


async def run_download(
    source: str,
    target_dir: Path,
    trackers: List[str],
    progress_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]],
    *,
    max_peers: int = 300,
    upload_limit: int = 0,
    stall_seconds: int = 600,
    timeout_seconds: int = 7200,
    listen_port: int = 6881,
) -> None:
    """Run aria2c for a .torrent path or a magnet URI. Returns when the download is complete, raises otherwise."""
    cmd = build_command(source, target_dir, trackers, max_peers=max_peers, upload_limit=upload_limit,
                        stall_seconds=stall_seconds, listen_port=listen_port)
    logger.info("aria2c fallback started (%d extra trackers).", len(trackers))
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT, limit=1024 * 1024
    )
    start = time.monotonic()
    last_cb = 0.0
    tail: List[str] = []

    async def reader() -> None:
        nonlocal last_cb
        buf = b""
        assert proc.stdout is not None
        while True:
            chunk = await proc.stdout.read(4096)
            if not chunk:
                break
            buf += chunk
            parts = re.split(rb"[\r\n]+", buf)
            buf = parts.pop()
            for raw in parts:
                line = raw.decode("utf-8", "replace").strip()
                if not line:
                    continue
                stats = parse_progress(line)
                if stats:
                    now = time.monotonic()
                    if progress_callback and now - last_cb >= 3:
                        last_cb = now
                        await progress_callback(stats)
                else:
                    tail.append(line)
                    del tail[:-8]

    read_task = asyncio.create_task(reader())
    try:
        while proc.returncode is None:
            if time.monotonic() - start > timeout_seconds:
                raise TimeoutError("aria2 download exceeded the maximum configured timeout.")
            try:
                await asyncio.wait_for(asyncio.shield(proc.wait()), timeout=2)
            except asyncio.TimeoutError:
                pass
        await read_task
        if proc.returncode != 0:
            raise RuntimeError(f"aria2c exited with code {proc.returncode}: {' | '.join(tail[-3:])[:300]}")
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
        if not read_task.done():
            read_task.cancel()
