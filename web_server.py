import html
import json
import mimetypes
from pathlib import Path
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Request, HTTPException, Header, Query
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse, JSONResponse

from config import WebServerConfig, FileStoreConfig
from database import db
from auto_uploader import auto_uploader_bot, human_readable_size
from base_url import learn_from_request

app = FastAPI(title="Telegram Torrent Redirect & Streaming Portal")


@app.middleware("http")
async def learn_public_base_url(request: Request, call_next):
    # Remember the public host people really reach us on (used for post links if WEB_BASE_URL is unset)
    try:
        host = request.headers.get("x-forwarded-host") or request.headers.get("host") or ""
        scheme = request.headers.get("x-forwarded-proto") or request.url.scheme
        await learn_from_request(db, scheme.split(",")[0].strip(), host.split(",")[0].strip())
    except Exception:
        pass
    return await call_next(request)


def validate_safe_file_path(file_path_str: str) -> Path:
    base_dir = FileStoreConfig.DOWNLOAD_DIR.resolve()
    target_path = Path(file_path_str).resolve()
    if not str(target_path).startswith(str(base_dir)) or not target_path.is_file():
        raise HTTPException(status_code=404, detail="Media file not found or access denied.")
    return target_path


def render_countdown_html(
    title: str,
    filename: str,
    quality: str,
    size_label: str,
    countdown: int,
    destination_url: str,
    watch_url: Optional[str] = None,
) -> str:
    # scraped titles are untrusted: escape everything that lands in HTML
    title = html.escape(title or "")
    filename = html.escape(filename or "")
    quality = html.escape(str(quality or ""))
    size_label = html.escape(str(size_label or ""))
    dest_attr = html.escape(destination_url, quote=True)
    watch_btn = (
        f'<a href="{html.escape(watch_url, quote=True)}" id="watchBtn" class="btn btn-secondary">▶️ Watch on Web</a>'
        if watch_url else ""
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title} - Preparing File</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; }}
  body {{
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    background: #0b0f19;
    color: #ffffff;
    overflow: hidden;
    position: relative;
    padding: 20px;
  }}
  .blob {{
    position: absolute;
    border-radius: 50%;
    filter: blur(85px);
    opacity: 0.65;
    animation: float 12s infinite ease-in-out alternate;
    z-index: 0;
  }}
  .blob-1 {{ width: 380px; height: 380px; background: linear-gradient(135deg, #6366f1, #ec4899); top: -80px; left: -80px; }}
  .blob-2 {{ width: 340px; height: 340px; background: linear-gradient(135deg, #06b6d4, #3b82f6); bottom: -80px; right: -80px; animation-delay: -5s; }}
  @keyframes float {{
    0% {{ transform: translate(0, 0) scale(1); }}
    100% {{ transform: translate(50px, 40px) scale(1.15); }}
  }}
  .glass-card {{
    position: relative;
    z-index: 1;
    width: 100%;
    max-width: 480px;
    background: rgba(255, 255, 255, 0.06);
    backdrop-filter: blur(20px);
    -webkit-backdrop-filter: blur(20px);
    border: 1px solid rgba(255, 255, 255, 0.15);
    border-radius: 24px;
    padding: 32px 28px;
    box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.6);
    text-align: center;
  }}
  .badge {{
    display: inline-block;
    padding: 6px 14px;
    background: rgba(99, 102, 241, 0.25);
    border: 1px solid rgba(99, 102, 241, 0.5);
    border-radius: 999px;
    font-size: 0.78rem;
    font-weight: 600;
    letter-spacing: 0.5px;
    color: #a5b4fc;
    margin-bottom: 16px;
  }}
  h1 {{ font-size: 1.45rem; font-weight: 700; margin-bottom: 8px; line-height: 1.3; }}
  .filename {{ font-size: 0.85rem; color: #94a3b8; word-break: break-word; margin-bottom: 22px; }}
  .meta-grid {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 12px;
    margin-bottom: 26px;
  }}
  .meta-box {{
    background: rgba(255, 255, 255, 0.04);
    border: 1px solid rgba(255, 255, 255, 0.08);
    border-radius: 14px;
    padding: 12px;
  }}
  .meta-label {{ font-size: 0.72rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.6px; }}
  .meta-val {{ font-size: 1.05rem; font-weight: 700; color: #f8fafc; margin-top: 4px; }}
  .timer-circle {{
    font-size: 2.6rem;
    font-weight: 800;
    background: linear-gradient(90deg, #38bdf8, #818cf8, #f472b6);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin-bottom: 12px;
  }}
  .progress-track {{
    width: 100%;
    height: 10px;
    background: rgba(255, 255, 255, 0.08);
    border-radius: 999px;
    overflow: hidden;
    margin-bottom: 20px;
  }}
  .progress-bar {{
    height: 100%;
    width: 0%;
    background: linear-gradient(90deg, #06b6d4, #6366f1, #ec4899);
    border-radius: 999px;
    transition: width 1s linear;
  }}
  .btn {{
    display: none;
    width: 100%;
    padding: 14px 20px;
    border-radius: 14px;
    font-weight: 600;
    font-size: 0.95rem;
    text-decoration: none;
    color: #fff;
    background: linear-gradient(135deg, #6366f1, #ec4899);
    box-shadow: 0 10px 20px -5px rgba(99, 102, 241, 0.5);
    margin-top: 10px;
    transition: transform 0.2s ease;
  }}
  .btn:hover {{ transform: translateY(-2px); }}
  .btn-secondary {{
    background: rgba(255, 255, 255, 0.09);
    border: 1px solid rgba(255, 255, 255, 0.18);
    box-shadow: none;
  }}
</style>
</head>
<body>
  <div class="blob blob-1"></div>
  <div class="blob blob-2"></div>
  <div class="glass-card">
    <span class="badge">SECURE CLOUD GATEWAY</span>
    <h1>{title}</h1>
    <p class="filename">{filename}</p>
    <div class="meta-grid">
      <div class="meta-box">
        <div class="meta-label">Video Quality</div>
        <div class="meta-val">{quality}</div>
      </div>
      <div class="meta-box">
        <div class="meta-label">File Size</div>
        <div class="meta-val">{size_label}</div>
      </div>
    </div>
    <div class="timer-circle" id="countdown">{countdown}s</div>
    <div class="progress-track">
      <div class="progress-bar" id="progressBar"></div>
    </div>
    <p id="statusText" style="font-size:0.85rem;color:#cbd5e1;">Verifying... you will be sent to the Telegram File Store Bot.</p>
    <a href="{dest_attr}" id="tgBtn" class="btn">🚀 Open in Telegram File Store Bot</a>
    {watch_btn}
  </div>
<script>
  const total = {countdown};
  let remaining = total;
  const countEl = document.getElementById('countdown');
  const barEl = document.getElementById('progressBar');
  const statusEl = document.getElementById('statusText');
  const tgBtn = document.getElementById('tgBtn');
  const watchBtn = document.getElementById('watchBtn');
  const DEST = {json.dumps(destination_url)};

  const interval = setInterval(() => {{
    remaining--;
    const pct = ((total - remaining) / total) * 100;
    barEl.style.width = pct + '%';
    if (remaining > 0) {{
      countEl.textContent = remaining + 's';
    }} else {{
      clearInterval(interval);
      countEl.textContent = 'Ready!';
      statusEl.textContent = 'Opening the Telegram bot - your download starts there...';
      tgBtn.style.display = 'block';
      if (watchBtn) watchBtn.style.display = 'block';
      window.location.href = DEST;  // the bot starts the download; this page never does
    }}
  }}, 1000);
</script>
</body>
</html>"""


def render_watch_html(
    movie_title: str,
    file_id: str,
    current_part: int,
    parts: List[Dict[str, Any]],
) -> str:
    if not parts:
        # NOT downloaded yet. The web page never starts a download and never shows progress:
        # that all happens in the File Store bot.
        tg_deep_link = f"https://t.me/{FileStoreConfig.BOT_USERNAME}?start=file_{file_id}"
        movie_title = html.escape(movie_title or "")
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{movie_title}</title>
<style>
  body {{ min-height:100vh; display:flex; align-items:center; justify-content:center; background:#0b0f19; color:#fff; font-family:sans-serif; padding:20px; }}
  .card {{ max-width:500px; width:100%; background:rgba(255,255,255,0.06); border:1px solid rgba(255,255,255,0.15); border-radius:24px; padding:32px; text-align:center; }}
  .btn {{ display:inline-block; margin-top:18px; padding:12px 22px; background:linear-gradient(135deg,#6366f1,#ec4899); color:#fff; border-radius:12px; text-decoration:none; font-weight:600; }}
</style>
</head>
<body>
  <div class="card">
    <h2>📥 Get this file in Telegram</h2>
    <p style="color:#94a3b8;margin:12px 0;">{movie_title}</p>
    <p style="font-size:0.9rem;color:#cbd5e1;">Open the File Store Bot - the download starts there immediately and you see the live progress in the chat.</p>
    <a class="btn" href="{tg_deep_link}">🚀 Open in File Store Bot</a>
  </div>
</body></html>"""

    active_part = next((p for p in parts if p["part_number"] == current_part), parts[0])
    stream_src = f"/stream/{file_id}/{active_part['part_number']}"
    download_src = f"/download/{file_id}/{active_part['part_number']}"
    part_buttons_html = ""
    for p in parts:
        p_num = p["part_number"]
        active_cls = "active" if p_num == active_part["part_number"] else ""
        size_str = human_readable_size(p["size_bytes"])
        part_buttons_html += f'<a href="/watch/{file_id}?part={p_num}" class="part-btn {active_cls}">Part {p_num} ({size_str})</a>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{html.escape(movie_title or "")} - Watch Online</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; font-family: 'Inter', -apple-system, sans-serif; }}
  body {{
    min-height: 100vh;
    background: radial-gradient(circle at top left, #1e1b4b, #0f172a 55%, #090d16);
    color: #f8fafc;
    padding: 24px 16px;
    display: flex;
    justify-content: center;
  }}
  .container {{ width: 100%; max-width: 960px; }}
  .glass-panel {{
    background: rgba(255, 255, 255, 0.06);
    backdrop-filter: blur(22px);
    -webkit-backdrop-filter: blur(22px);
    border: 1px solid rgba(255, 255, 255, 0.14);
    border-radius: 24px;
    padding: 24px;
    box-shadow: 0 25px 50px -12px rgba(0,0,0,0.65);
    margin-bottom: 20px;
  }}
  video {{
    width: 100%;
    max-height: 540px;
    border-radius: 16px;
    background: #000;
    outline: none;
    margin-bottom: 18px;
  }}
  h1 {{ font-size: 1.35rem; margin-bottom: 6px; }}
  .sub {{ font-size: 0.88rem; color: #94a3b8; margin-bottom: 18px; word-break: break-all; }}
  .actions {{ display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 18px; }}
  .btn {{
    padding: 12px 22px;
    border-radius: 12px;
    text-decoration: none;
    font-weight: 600;
    font-size: 0.92rem;
    color: #fff;
    background: linear-gradient(135deg, #6366f1, #ec4899);
    transition: transform 0.2s;
  }}
  .btn:hover {{ transform: translateY(-2px); }}
  .parts-bar {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 12px; }}
  .part-btn {{
    padding: 9px 16px;
    border-radius: 10px;
    font-size: 0.85rem;
    text-decoration: none;
    color: #cbd5e1;
    background: rgba(255,255,255,0.06);
    border: 1px solid rgba(255,255,255,0.12);
  }}
  .part-btn.active {{
    background: rgba(99, 102, 241, 0.35);
    border-color: #818cf8;
    color: #fff;
    font-weight: 700;
  }}
</style>
</head>
<body>
  <div class="container">
    <div class="glass-panel">
      <video controls preload="metadata" src="{stream_src}"></video>
      <h1>{html.escape(movie_title or "")}</h1>
      <p class="sub">{html.escape(active_part['part_filename'])} • {human_readable_size(active_part['size_bytes'])}</p>
      <div class="actions">
        <a href="{download_src}" class="btn">📥 Download This Part</a>
        <a href="https://t.me/{FileStoreConfig.BOT_USERNAME}?start=file_{file_id}" class="btn" style="background:rgba(255,255,255,0.12);">🤖 Get in Telegram</a>
      </div>
      <div>
        <span style="font-size:0.8rem;color:#94a3b8;text-transform:uppercase;">Select File Part:</span>
        <div class="parts-bar">{part_buttons_html}</div>
      </div>
    </div>
  </div>
</body>
</html>"""


@app.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
async def root():
    return {"status": "ok", "service": "auto-uploader"}


@app.get("/health")
async def health():
    return {"status": "ok", "ok": True, "base_url": WebServerConfig.BASE_URL}


@app.get("/r/{redirect_id}", response_class=HTMLResponse)
async def redirect_countdown_page(redirect_id: str):
    link_doc = await db.get_redirect_link(redirect_id)
    if not link_doc:
        raise HTTPException(status_code=404, detail="Redirect link not found.")

    file_id = link_doc["file_id"]
    watch_url = None
    if link_doc.get("is_batch"):
        # movie "get all" / stored link / batch: the bot shows the file list or sends the stored messages
        destination_url = f"https://t.me/{FileStoreConfig.BOT_USERNAME}?start={file_id}"
        filename = link_doc.get("title", "All Available Qualities Bundle")
    else:
        destination_url = f"https://t.me/{FileStoreConfig.BOT_USERNAME}?start=file_{file_id}"
        filename = link_doc.get("original_filename", link_doc["title"])
        # A web player is offered ONLY if the file is already stored; this page never starts a download.
        torrent_doc = await db.get_torrent(file_id)
        if torrent_doc and await db.find_completed_file(file_id, torrent_doc.get("infohash")):
            watch_url = f"/watch/{file_id}"

    return render_countdown_html(
        title=link_doc.get("title", "Movie Download"),
        filename=filename,
        quality=link_doc.get("quality", "HD"),
        size_label=link_doc.get("size_label", "Unknown"),
        countdown=WebServerConfig.COUNTDOWN_SECONDS,
        destination_url=destination_url,
        watch_url=watch_url,
    )


@app.get("/m/{movie_id}", response_class=HTMLResponse)
async def movie_files_page(movie_id: str):
    """Public web list of every file of a movie (the 'get all' link, no Telegram needed)."""
    movie_doc = await db.get_movie(movie_id)
    if not movie_doc:
        raise HTTPException(status_code=404, detail="Movie not found.")
    rows = ""
    for t in await db.get_torrents_by_movie(movie_id):
        name = html.escape(t.get("display_name") or t.get("original_filename") or t["file_id"])
        rows += (
            f'<a class="row" href="/watch/{t["file_id"]}"><span>{name}</span>'
            f'<b>{html.escape(str(t.get("size_label", "")))}</b></a>'
        )
    title = html.escape(movie_doc["title"])
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0"><title>{title}</title>
<style>body{{margin:0;min-height:100vh;background:#0b0f19;color:#fff;font-family:-apple-system,Inter,sans-serif;padding:24px 16px}}
.w{{max-width:760px;margin:auto}}h1{{font-size:1.4rem}}
.row{{display:flex;justify-content:space-between;gap:12px;padding:14px 16px;margin:10px 0;border-radius:12px;
background:rgba(255,255,255,.07);color:#fff;text-decoration:none;word-break:break-all}}.row:hover{{background:rgba(99,102,241,.35)}}</style>
</head><body><div class="w"><h1>{title}</h1>{rows or "<p>No files yet.</p>"}</div></body></html>"""


@app.get("/watch/{file_id}", response_class=HTMLResponse)
async def watch_online_page(file_id: str, part: int = Query(1, ge=1)):
    torrent_doc = await db.get_torrent(file_id)
    if not torrent_doc:
        raise HTTPException(status_code=404, detail="File not found.")

    movie_doc = await db.get_movie(torrent_doc["movie_id"])
    movie_title = movie_doc["title"] if movie_doc else torrent_doc["original_filename"]

    # Read-only: opening this page NEVER creates a download job (that used to enqueue it with user id 0).
    completed = await db.find_completed_file(file_id, torrent_doc.get("infohash"))
    parts = await db.get_file_parts(completed["file_id"]) if completed else []

    return render_watch_html(
        movie_title=movie_title,
        file_id=file_id,
        current_part=part,
        parts=parts,
    )


@app.get("/stream/{file_id}/{part_number}")
async def stream_video_part(
    file_id: str,
    part_number: int,
    range_header: Optional[str] = Header(None, alias="Range"),
):
    torrent_doc = await db.get_torrent(file_id)
    if not torrent_doc:
        raise HTTPException(status_code=404, detail="File record not found.")

    completed = await db.find_completed_file(file_id, torrent_doc.get("infohash"))
    if not completed:
        raise HTTPException(status_code=404, detail="Video is not yet downloaded.")

    parts = await db.get_file_parts(completed["file_id"])
    part_doc = next((p for p in parts if p["part_number"] == part_number), None)
    if not part_doc:
        raise HTTPException(status_code=404, detail="Requested file part not found.")

    safe_path = validate_safe_file_path(part_doc["file_path"])
    file_size = safe_path.stat().st_size
    content_type, _ = mimetypes.guess_type(str(safe_path))
    content_type = content_type or "video/x-matroska"

    start = 0
    end = file_size - 1
    status_code = 200

    if range_header and range_header.startswith("bytes="):
        status_code = 206
        range_spec = range_header.replace("bytes=", "").split(",")[0].strip()
        start_str, end_str = range_spec.split("-")
        if start_str:
            start = int(start_str)
            if end_str:
                end = min(int(end_str), file_size - 1)
        elif end_str:
            start = max(0, file_size - int(end_str))

    if start >= file_size or start > end:
        raise HTTPException(status_code=416, detail="Requested Range Not Satisfiable")

    chunk_length = (end - start) + 1

    def file_iterator(path: Path, offset: int, length: int, chunk_size: int = 1024 * 256):
        with open(path, "rb") as f:
            f.seek(offset)
            remaining = length
            while remaining > 0:
                read_size = min(chunk_size, remaining)
                data = f.read(read_size)
                if not data:
                    break
                remaining -= len(data)
                yield data

    headers = {
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(chunk_length),
        "Content-Disposition": f'inline; filename="{safe_path.name}"',
    }
    return StreamingResponse(
        file_iterator(safe_path, start, chunk_length),
        status_code=status_code,
        media_type=content_type,
        headers=headers,
    )


@app.get("/download/{file_id}/{part_number}")
async def download_video_part(file_id: str, part_number: int):
    torrent_doc = await db.get_torrent(file_id)
    if not torrent_doc:
        raise HTTPException(status_code=404, detail="File record not found.")

    completed = await db.find_completed_file(file_id, torrent_doc.get("infohash"))
    if not completed:
        raise HTTPException(status_code=404, detail="File is not yet downloaded.")

    parts = await db.get_file_parts(completed["file_id"])
    part_doc = next((p for p in parts if p["part_number"] == part_number), None)
    if not part_doc:
        raise HTTPException(status_code=404, detail="Requested file part not found.")

    safe_path = validate_safe_file_path(part_doc["file_path"])
    return FileResponse(
        path=str(safe_path),
        filename=part_doc["part_filename"],
        media_type="application/octet-stream",
    )


@app.post("/api/ingest")
async def api_ingest_movies(request: Request, x_api_token: Optional[str] = Header(None)):
    if x_api_token != WebServerConfig.SECRET_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized token")
    payload = await request.json()
    if isinstance(payload, dict):
        payload = [payload]
    ids = await auto_uploader_bot.ingest_api_payload(payload)
    return JSONResponse({"status": "ok", "ingested_movie_ids": ids})
