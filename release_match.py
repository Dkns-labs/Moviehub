"""Release-name parsing and verification for the "Database channel" lookup.

A file in a user's Database channel is only accepted as "the same release" when it passes THREE independent
checks (size is never used):

  1. NAME     - the extracted movie title matches (exact normalised key; a close fuzzy match is only allowed
                when the year agrees too, so "Con City" never matches "Con City 2" or "Concrete City").
  2. RELEASE  - the release year / date agrees (a missing year on one side is tolerated only with an exact name).
  3. QUALITY  - the resolution is the same (1080p == 1080p, 4K == 2160p). 720p never stands in for 1080p.

Everything else (codec, source, language) is used only to choose between several files that passed all three.
"""
import re
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional, Tuple

from media_names import split_part

_SPLIT_RE = re.compile(r"[\s._\-\[\]\(\)\{\}+,]+")
_YEAR_RE = re.compile(r"^(19[2-9]\d|20[0-4]\d)$")
_DATE_RE = re.compile(r"(?<!\d)((?:19|20)\d{2})[.\-_ /](0?[1-9]|1[0-2])[.\-_ /](0?[1-9]|[12]\d|3[01])(?!\d)")
_RES_RE = re.compile(r"(?<![a-z0-9])(2160|1080|720|576|480|360|240)p(?![a-z0-9])", re.I)
_4K_RE = re.compile(r"(?<![a-z0-9])(4k|uhd)(?![a-z0-9])", re.I)

# The title ends at the first of these tokens (when no year came first).
_STOP = {
    "2160p", "1080p", "720p", "576p", "480p", "360p", "240p", "4k", "uhd", "hd", "fhd", "sd",
    "web", "webdl", "webrip", "webhd", "nf", "amzn", "dsnp", "hotstar", "zee5", "sonyliv", "jc", "sun", "aha",
    "hdrip", "hdtv", "bluray", "brrip", "bdrip", "dvdrip", "dvdscr", "hdcam", "cam", "pre", "predvd", "tc", "hq", "true",
    "x264", "x265", "h264", "h265", "hevc", "avc", "10bit", "8bit", "hdr", "hdr10", "sdr", "dv",
    "aac", "ac3", "eac3", "ddp5", "ddp", "dd", "dd5", "atmos", "mp3",
    "tamil", "telugu", "hindi", "malayalam", "kannada", "english", "korean", "bengali", "marathi", "punjabi",
    "dual", "multi", "esub", "esubs", "msub", "untouched", "proper", "repack", "extended", "uncut", "imax",
    "mkv", "mp4", "avi", "mov", "webm", "m4v", "ts",
}
_LANGS = {
    "tamil": "tamil", "tam": "tamil", "telugu": "telugu", "tel": "telugu", "hindi": "hindi", "hin": "hindi",
    "malayalam": "malayalam", "mal": "malayalam", "kannada": "kannada", "kan": "kannada",
    "english": "english", "eng": "english", "korean": "korean", "kor": "korean",
}
_NOISE_PREFIX = (re.compile(r"^\s*www\.[a-z0-9.\-]+\s*-\s*", re.I), re.compile(r"^\s*@\S+\s*-\s*"))


def _clean(name: str) -> str:
    n = name or ""
    for rx in _NOISE_PREFIX:
        n = rx.sub("", n)
    n, _ = split_part(n)  # drops extension and ".part01"
    return n


def resolution_of(text: Optional[str]) -> Optional[str]:
    """'1080p' | '720p' | ... | '2160p' (4K / UHD) | None. An explicit NNNp always wins over a 4K tag, and
    'DS4K' / 'x4k' style glued tags are not read as 4K."""
    t = text or ""
    m = _RES_RE.search(t)
    if m:
        return f"{m.group(1)}p"
    if _4K_RE.search(t):
        return "2160p"
    return None


def parse_release(name: Optional[str]) -> Dict[str, Any]:
    """Extract everything the verification needs from a release / file name."""
    raw = name or ""
    base = _clean(raw)
    tokens = [t for t in _SPLIT_RE.split(base) if t]
    low = [t.lower() for t in tokens]

    date_m = _DATE_RE.search(base)
    release_date = None
    if date_m:
        release_date = f"{date_m.group(1)}-{int(date_m.group(2)):02d}-{int(date_m.group(3)):02d}"

    # first technical token (resolution / source / codec / language ...)
    stop_idx = next((i for i, t in enumerate(low) if t in _STOP and i > 0), len(tokens))
    # a year BEFORE that token (and not the very first token: "2012", "1917" are titles) ends the title
    year_idx = None
    for i in range(1, stop_idx):
        if _YEAR_RE.match(low[i]):
            year_idx = i  # last one wins: "2012 2009 1080p" -> title 2012, year 2009
    if year_idx is not None:
        title_tokens = tokens[:year_idx]
        year = low[year_idx]
    else:
        title_tokens = tokens[:stop_idx]
        year = None
        # a year right after the technical part ("Movie 1080p 2026"): take it, the title is already cut
        for i in range(stop_idx, len(tokens)):
            if _YEAR_RE.match(low[i]):
                year = low[i]
                break
    if release_date and not year:
        year = release_date[:4]
    if not title_tokens and tokens:
        title_tokens = tokens[:1]

    title = " ".join(title_tokens).strip()
    title_key = re.sub(r"[^a-z0-9]+", "", title.lower())
    langs = sorted({_LANGS[t] for t in low if t in _LANGS})
    joined = " ".join(low)
    return {
        "raw": raw,
        "title": title,
        "title_key": title_key,
        "year": year,
        "release_date": release_date,
        "resolution": resolution_of(raw),
        "codec": "x265" if re.search(r"\b(x265|h265|hevc)\b", joined) or "h 265" in joined else (
            "x264" if re.search(r"\b(x264|h264|avc)\b", joined) or "h 264" in joined else None),
        "source": "webdl" if re.search(r"web\s?dl|webhd", joined) else ("webrip" if "webrip" in joined else (
            "hdrip" if "hdrip" in joined else ("bluray" if re.search(r"blu\s?ray|brrip|bdrip", joined) else None))),
        "languages": langs,
    }


def _title_ok(want_keys: Iterable[str], cand_key: str, years_agree: bool) -> Optional[str]:
    """Pass 1. Returns 'exact' / 'fuzzy' / None."""
    keys = [k for k in want_keys if k]
    if not cand_key or not keys:
        return None
    if cand_key in keys:
        return "exact"
    if years_agree:  # fuzzy only with a confirmed year; digits must be identical so sequels never match
        for k in keys:
            if re.sub(r"\D", "", k) == re.sub(r"\D", "", cand_key) and SequenceMatcher(None, k, cand_key).ratio() >= 0.92:
                return "fuzzy"
    return None


def verify(want: Dict[str, Any], cand: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    """Run the three passes. `want` = what the new post needs, `cand` = a Database-channel file (both parse_release
    dicts; `want['title_keys']` may carry extra accepted title keys). Returns (accepted, passed, failed)."""
    passed: List[str] = []

    wy, cy = want.get("year"), cand.get("year")
    year_known = bool(wy and cy)
    years_agree = year_known and wy == cy

    # pass 1 - NAME
    how = _title_ok(want.get("title_keys") or [want.get("title_key", "")], cand.get("title_key", ""), years_agree)
    if not how:
        return False, [], ["name"]
    passed.append(f"name({how})")

    # pass 2 - RELEASE YEAR / DATE
    if year_known:
        if not years_agree:
            return False, passed, ["year"]
        wd, cd = want.get("release_date"), cand.get("release_date")
        if wd and cd and wd != cd:
            return False, passed, ["release_date"]
        passed.append("year")
    else:
        if how != "exact":
            return False, passed, ["year"]
        passed.append("year(unknown, exact name)")

    # pass 3 - QUALITY (resolution only; size is ignored on purpose)
    wr, cr = want.get("resolution"), cand.get("resolution")
    if wr != cr:
        return False, passed, ["quality"]
    passed.append(f"quality({wr or 'SD'})")
    return True, passed, []


def score(want: Dict[str, Any], cand: Dict[str, Any]) -> int:
    """Tie-breaker between files that all passed verification (never a reason to reject)."""
    s = 0
    if want.get("codec") and want.get("codec") == cand.get("codec"):
        s += 4
    if want.get("source") and want.get("source") == cand.get("source"):
        s += 3
    s += len(set(want.get("languages") or []) & set(cand.get("languages") or []))
    return s


def pick_release(
    wants: List[Dict[str, Any]], candidates: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Best verified candidate group for any of the `wants` (the movie post title and each release name).

    `candidates` are Database-channel docs with at least name_key, title_key, year, resolution, file_name, message_id.
    Parts of a split file share a name_key and are returned together under 'parts'. None = nothing verified.
    """
    best: Optional[Tuple[int, Dict[str, Any]]] = None
    title_keys = sorted({w.get("title_key") for w in wants if w.get("title_key")})
    for want in wants:
        want = dict(want, title_keys=title_keys)
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for c in candidates:
            parsed = parse_release(c.get("file_name"))
            for k in ("title_key", "year", "release_date", "resolution"):
                if c.get(k):  # values stored at index time win; a missing one falls back to the file name
                    parsed[k] = c[k]
            ok, passed, _failed = verify(want, parsed)
            if ok:
                groups.setdefault(c.get("name_key") or c.get("file_name"), []).append(dict(c, _passed=passed, _parsed=parsed))
        for items in groups.values():
            sc = score(want, items[0]["_parsed"]) * 10 + len(items)
            if best is None or sc > best[0]:
                best = (sc, {"parts": sorted(items, key=lambda x: x.get("part_number") or 1), "passed": items[0]["_passed"]})
    return best[1] if best else None
