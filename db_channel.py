"""Database channel lookup: "do we already have this release?" - answered BEFORE anything is downloaded.

Flow for every release of a new post:
  1. Build what we want from the post title/year AND from the release's own file name (two independent extractions).
  2. Pre-filter the indexed Database-channel files (the owner's channel first, then the admin's).
  3. Verify with release_match.verify(): NAME -> RELEASE YEAR/DATE -> QUALITY (size never matters).
  4. A verified file becomes a normal completed File Store record; the post link then sends it directly.
"""
import logging
from typing import Any, Dict, List, Optional

from database import db
from release_match import parse_release, pick_release

logger = logging.getLogger("db_channel")


async def _verify_parts(parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Re-check every part against its channel (still there, still a video, fresh file id). Offline -> unchanged."""
    try:
        from file_store import file_store_bot  # lazy: file_store imports this module
        return await file_store_bot.verify_db_channel_docs(parts)
    except Exception as exc:
        logger.debug("Part verification skipped: %s", exc)
        return parts


def build_wants(title: str, year: Optional[str], rel: Dict[str, Any]) -> List[Dict[str, Any]]:
    """What the post needs, extracted two ways: from the movie title+year, and from the release file name."""
    wants: List[Dict[str, Any]] = []
    res = None
    for n in (rel.get("original_filename"), rel.get("display_name"), rel.get("quality_label")):
        r = parse_release(n or "")["resolution"]
        if r:
            res = r
            break
    rel_info = parse_release(rel.get("original_filename") or rel.get("display_name") or "")
    y = year if (year and year.isdigit()) else None

    by_title = parse_release(f"{title} {y}" if y else title)
    by_title["year"] = y or rel_info.get("year")
    by_title["resolution"] = res
    by_title["codec"], by_title["source"] = rel_info.get("codec"), rel_info.get("source")
    by_title["languages"] = rel_info.get("languages")
    by_title["release_date"] = rel_info.get("release_date")
    wants.append(by_title)

    if rel_info.get("title_key") and rel_info["title_key"] != by_title["title_key"]:
        alt = dict(rel_info)
        alt["year"] = alt.get("year") or by_title["year"]
        alt["resolution"] = res
        wants.append(alt)
    return wants


async def find_for_release(owner_id: int, title: str, year: Optional[str], rel: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Returns {"file_id", "name", "passed", "chat_id"} when a verified file is in a Database channel, else None.
    Never raises: any failure simply means "not found" and the normal download path runs."""
    try:
        chat_ids = await db.get_db_channel_ids_for(owner_id)
        if not chat_ids:
            return None
        wants = build_wants(title, year, rel)
        keys = sorted({w["title_key"] for w in wants if w.get("title_key")})
        years = {w.get("year") for w in wants if w.get("year")}
        cands: List[Dict[str, Any]] = []
        seen = set()
        for y in (years or {None}):
            for c in await db.find_db_channel_candidates(chat_ids, keys, y):
                if (c["chat_id"], c["message_id"]) not in seen:
                    seen.add((c["chat_id"], c["message_id"]))
                    cands.append(c)
        if not cands:
            return None
        # the owner's own channel wins over the admin's: search in that order, stop at the first verified hit
        for chat_id in chat_ids:
            pool = [c for c in cands if c["chat_id"] == chat_id]
            for _ in range(3):  # a hit whose message vanished is dropped and the next best file is tried
                hit = pick_release(wants, pool)
                if not hit:
                    break
                parts = await _verify_parts(hit["parts"])
                if len(parts) == len(hit["parts"]):
                    file_id = await db.link_db_channel_release(parts)
                    if file_id:
                        return {
                            "file_id": file_id, "name": parts[0]["file_name"], "passed": hit["passed"], "chat_id": chat_id,
                            "size_bytes": sum(int(x.get("size_bytes") or 0) for x in parts), "parts": len(parts),
                        }
                    break
                gone = {(p["chat_id"], p["message_id"]) for p in hit["parts"]} - {(p["chat_id"], p["message_id"]) for p in parts}
                pool = [c for c in pool if (c["chat_id"], c["message_id"]) not in gone]
    except Exception as exc:
        logger.warning("Database channel lookup failed for '%s': %s", title, exc)
    return None


_RES_RANK = {"2160p": 6, "1080p": 5, "720p": 4, "480p": 3, "360p": 2, "240p": 1}


async def search_files(uid: int, query: str, limit: int = 8) -> List[Dict[str, Any]]:
    """User search, step 1: is the file already in a Database channel (the user's own, then the admin's)?
    Returns releases (split parts grouped), best first, each already linked as a stored File Store record:
    [{"file_id", "name", "resolution", "size_bytes", "parts", "exact"}]. Never raises: [] means "not there"."""
    try:
        from media_names import name_key
        chat_ids = await db.get_db_channel_ids_for(uid)
        docs = await db.search_db_channel_files(chat_ids, query)
        if not docs:
            return []
        qkey = name_key(query) or "".join(ch for ch in query.lower() if ch.isalnum())
        groups: Dict[Any, List[Dict[str, Any]]] = {}
        for d in docs:
            groups.setdefault((d["chat_id"], d.get("name_key") or name_key(d["file_name"])), []).append(d)
        releases: List[Dict[str, Any]] = []
        for (_chat, key), parts in groups.items():
            parts.sort(key=lambda p: int(p.get("part_number") or 1))
            exact = key == qkey or key.startswith(qkey)
            releases.append({"_parts": parts, "exact": exact, "key": key})
        releases.sort(key=lambda r: (
            not r["exact"], -_RES_RANK.get(r["_parts"][0].get("resolution") or "", 0), r["_parts"][0]["file_name"].lower()
        ))
        out: List[Dict[str, Any]] = []
        for r in releases[:limit]:
            parts = r["_parts"]
            file_id = await db.link_db_channel_release(parts)
            if not file_id:
                continue
            out.append({
                "file_id": file_id,
                "name": parts[0].get("base_name") or parts[0]["file_name"],
                "resolution": parts[0].get("resolution"),
                "size_bytes": sum(int(p.get("size_bytes") or 0) for p in parts),
                "parts": len(parts),
                "exact": r["exact"],
            })
        return out
    except Exception as exc:
        logger.warning("Database channel search failed for %r: %s", query, exc)
        return []
