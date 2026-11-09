# Deploy on Render
1. Push this folder to GitHub -> Render -> New -> Blueprint (uses `render.yaml`), or New Web Service -> Docker.
2. Set the secret env vars in the dashboard (MONGO_URI, bot tokens, TELEGRAM_API_ID/HASH, OWNER_ID, channel ids).
3. Health check path: `/health` (already in render.yaml). The app listens on Render's `$PORT` automatically.
4. Links use `RENDER_EXTERNAL_URL` automatically; leave `WEB_BASE_URL` empty.
5. Keep-alive: the app pings `<your-render-url>/health` every 60s (change with `KEEP_ALIVE_INTERVAL`).
   For non-Render hosts set `KEEP_ALIVE_URL=https://your-domain` to enable it.
Note: Render's free disk is ephemeral and BitTorrent peer ports are not reachable there; large torrent downloads work best on a VPS.

## Flow (v2.1)
1. Auto Uploader downloads the `.torrent`, stores it in the log channel (`LOG_CHANNEL_ID`, else `STORAGE_CHANNEL_ID`) and posts in the public channel.
2. Post link -> `/r/<id>` -> 15 s timer -> redirects to the File Store bot (`t.me/<bot>?start=file_<id>`). The web page never downloads anything and never shows progress.
3. In the bot: stored file -> sent instantly. Otherwise the download starts immediately (no queue) and ONE chat message shows progress, speed, ETA, elapsed, peers, then the upload to the storage channel.
4. "Peer id invalid" fixes: persistent session files (`SESSION_DIR`, keep it on a volume), channels resolved at startup, resolve+retry on first use. Both bots must be ADMIN of the storage / log channel (and clone bots of the storage channel).
5. Test payload: send `sardar_test.json` to the Auto Uploader bot with /ingest.

## Startup (v2.2)
- Both bots connect to Telegram at the same time as MongoDB; a tap/`/start` that arrives while MongoDB is still connecting simply waits for it (max 25 s) and then answers with the buttons.
- Command menus, channel checks, clone restore and the tracker list load in the background. Look for `BOTS READY` in the logs.
- The Dockerfile now copies every `*.py` (before, `admin_panel.py` and `media_names.py` were missing -> ImportError on Render).

## Update (v2.3)
- **File Store**: the CLONE button opens the clone screen (paste the BotFather token right there); BACK returns to the START menu; the START button is now a CLOSE button that removes the menu.
- **Advanced Settings (Auto Uploader)**: Premium users get **START SCRAPING** once their public channel is set. The tap re-verifies that both bots are admin in the channel, scrapes the source site and sends the newest posts (`USER_SCRAPE_LIMIT`, default 10) to the user's own channel with their uploader name / tag / caption / thumbnail. Cooldown between runs: `USER_SCRAPE_COOLDOWN_SECONDS` (default 600; the admin has none).
- **Torrent Search**: 22 sites searched at once (`torrent_search.py`). Set `TORRENT_SEARCH_BOT_TOKEN` to run it as its own bot, or leave it empty to use `/search <name>` inside the File Store bot and its clones. Check which sites answer from your server with `python torrent_search.py "ubuntu"` or `/sitetest` (owner). Add sites with `TS_CUSTOM_SITES`, see `.env.example`.
- Tests: `python test_torrent_search.py`, `python test_new_features.py`.

## Update (v2.4) - Database channel (per user)
- `/settings` -> 🛠 ADVANCED -> **📚 DATABASE CHANNEL**: every user (the admin too) can attach a channel that already holds their files. Add the **File Store bot as ADMIN** there (no post rights needed), send the @username / ID. Existing files are indexed automatically (🔄 SYNC EXISTING FILES to redo it); new uploads are indexed live, edits/deletes are followed.
- Before a post is made, each release is searched in that channel (the user's own first, then the admin's) with three verifications: **1. name** (extracted title; sequels never match) -> **2. year / release date** -> **3. quality** (1080p / 720p / 2160p=4K...). **Size is ignored.** Codec / source / language only break ties.
- Hit: no `.torrent`, no download. The post link -> `/r/<id>` -> File Store bot sends the stored file instantly (clone bots must be ADMIN of the Database channel, like the storage channel). Miss: the normal flow runs.
- Telegram bots cannot search a channel, so the bot keeps its own index (`db_channel_files`). Tests: `python test_dbchannel.py`.

## Update (v2.5) - Full scan
- **🔍 FULL SCAN** (Advanced -> 📚 DATABASE CHANNEL): re-reads the whole channel, drops entries of deleted files, then switches torrent lines already on the user's posts to the stored file where name + year + quality verify (no download). It also runs once when the channel is set.
- Per quality: with 1080p + 720p in the Database channel and 4 qualities in the torrent post, 1080p and 720p come from the channel and the other two follow the normal torrent flow. Two torrent lines of the same quality never both map to one Database file.
- Rule (no modes): name + year + quality found in the Database channel -> sent from the File Store; otherwise the normal download runs. The same check also runs when a user opens a link, so files added to the channel later are still found.

## Update (v2.6) - faster torrents
- **More tracker sources**: 9 GitHub lists (ngosang all/best/udp/best_ip, XIU2 all/best, DeSireFire, hezhijie0327, adysec) + newTrackon "stable". A GitHub raw URL that is blocked is retried through jsDelivr automatically. Add your own with `TRACKER_LIST_SOURCE_URL` (comma separated).
- **Live trackers only** (`tracker_tools.py`): every tracker is probed (UDP BEP-15 handshake / HTTP) after each list refresh; only the ones that answer are attached to torrents, fastest first (`TRACKERS_LIVE_MAX`, default 150). The log shows `Live trackers: N of M answered`. Until the first check finishes a small UDP-first sample is used.
- **Slow-torrent boost**: below `TORRENT_BOOST_BELOW_SPEED` (default 1 MB/s) after 30 s, 60 more live trackers are added every 45 s and the torrent is re-announced (+DHT).
- **Second BitTorrent client** (`aria2_backend.py`): if libtorrent cannot be imported / started, downloads run with `aria2c` (installed in the Dockerfile). `TORRENT_BACKEND=auto` (default) = libtorrent first, aria2c on every 2nd retry after a failure; `libtorrent` / `aria2` force one. `libtorrent` is no longer in requirements.txt (a failing pip build no longer breaks the install) - Docker gets it from apt.
- Tests: `python test_trackers.py`.

## Update (v2.7) - Database files are always sent as the real video
- Delivery tries every route (cached file id -> copy from the source channel -> without buttons; clone bots copy, then the main bot copies). Only a part with no Telegram file at all gets a text card.
- Each Database file is re-checked against its channel message before it is used (still there, still a video, fresh file id). Deleted files are dropped from the index and the next best match is tried.
- If a stored copy still cannot be sent, the user is told and the normal download starts - never just a message.
- Names of admin uploads: the real file name, or the caption line that looks most like a release (resolution / year). Videos with neither are counted in the scan result as "unnamed" - put the release name in the caption.
- Tests: `python test_delivery.py`.

- `DB_CHANNEL_ID` (.env): the admin's default Database channel, indexed at startup. A channel set in the bot overrides it; users set their own in /settings.

## Update (v2.8) - faster /search
- All sites are searched at once (was 12 at a time), mirrors of one site are raced instead of tried one by one, and the answer is shown as soon as enough results are in (`TS_ENOUGH_RESULTS` from `TS_ENOUGH_SITES` sites, then only `TS_GRACE` s more) or at the hard `TS_DEADLINE` (9 s). A slow site is skipped for that search only.
- The same search again within `TS_CACHE_TTL` (10 min) is instant; the mirror that answered last goes first; a site failing `TS_DOWN_AFTER` times in a row is skipped for `TS_DOWN_SECONDS`.
- Tests: `python test_search_speed.py`.
