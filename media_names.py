"""Pure helpers that map a file name -> (base name, part number), a comparison key and a resolution.

They are what makes "database first" work:
  * the same release is recognised whether it came from a torrent, from the bot's own upload or from a file
    the admin uploaded by hand (uploader tag, site prefix, extension, case and punctuation are ignored);
  * a file that was split in parts (Movie.part01.mkv, Movie.part02.mkv, Movie.mkv.001, "Movie (Part 1/2)")
    maps to ONE record with several parts - the parts are never treated as duplicates of each other;
  * a different quality (720p vs 1080p ...) is a different file, never a duplicate.
"""
import re
from typing import Optional, Tuple

_EXT_RE = re.compile(r"(?:\.(?:mkv|mp4|avi|mov|webm|m4v|ts|torrent))+$", re.I)

# Split-part markers. Deliberately strict so a sequel title such as "Dune Part 2" is NOT mistaken for a split:
#   Movie.part01 / Movie_part1 / Movie-part02      (separator right before "part", digits glued to it)
#   Movie (Part 1/2) / Movie [part 1 of 2]         (explicit "x of y" / "x/y")
#   Movie.001 (zero-padded 3 digit suffix, e.g. 7z/split style)
_PART_RES = (
    re.compile(r"[\s._\-]*[._\-]part0*(\d{1,3})$", re.I),
    re.compile(r"[\s._\-]*[\(\[]?\s*\bpart\s*0*(\d{1,3})\s*(?:of|/)\s*\d{1,3}\s*[\)\]]?$", re.I),
    re.compile(r"\.(0\d{2})$"),
)


def split_part(name: Optional[str]) -> Tuple[str, Optional[int]]:
    """'Movie.1080p.part02.mkv' -> ('Movie.1080p', 2);  'Movie.mkv' -> ('Movie', None)."""
    n = (name or "").strip()
    part: Optional[int] = None
    for _ in range(4):
        before = n
        n = _EXT_RE.sub("", n).strip()
        for rx in _PART_RES:
            m = rx.search(n)
            if m:
                if part is None:
                    part = int(m.group(1))
                n = n[: m.start()]
                break
        if n == before:
            break
    return n.strip(" ._-"), part


def name_key(name: Optional[str]) -> str:
    """Comparison key: ignores site prefix, @uploader tag, extension, part marker, case and punctuation."""
    base, _ = split_part(name)
    base = re.sub(r"^\s*www\.[a-z0-9.\-]+\s*-\s*", "", base, flags=re.I)
    base = re.sub(r"^\s*@\S+\s*-\s*", "", base)
    return re.sub(r"[^a-z0-9]+", "", base.lower())


def resolution_of_name(name: Optional[str]) -> Optional[str]:
    """'2160p' / '1080p' / '720p' / '480p' / '360p' / '240p', or None when the name does not say."""
    text = name or ""
    if re.search(r"(?<![a-z0-9])(4K|2160p)(?![a-z0-9])", text, re.I):
        return "2160p"
    m = re.search(r"(?<![a-z0-9])(1080|720|480|360|240)p(?![a-z0-9])", text, re.I)
    return f"{m.group(1)}p" if m else None
