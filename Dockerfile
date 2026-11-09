FROM python:3.11-slim-bookworm

# Prevent interactive prompts and buffer delays
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/usr/lib/python3/dist-packages:${PYTHONPATH}"

# Install system dependencies:
# - python3-libtorrent: native C++ BitTorrent binding (main client)
# - aria2: second BitTorrent client, used when libtorrent is missing or an attempt failed
# - ffmpeg: video duration probing, thumbnail extraction, and 1.86 GB segment splitting
# - gcc & python3-dev: required to compile TgCrypto for high-speed Pyrogram transfers
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-libtorrent \
    aria2 \
    ffmpeg \
    gcc \
    python3-dev \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first for better layer caching
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application source files
COPY *.py sample_post.json trackers.txt /app/

# Persistent storage: every path the app writes to lives under /app/data (mount it as a volume)
ENV POSTER_DIR=/app/data/posters \
    DOWNLOAD_DIR=/app/data/downloads \
    THUMBNAIL_DIR=/app/data/thumbnails \
    TORRENT_DIR=/app/data/torrents \
    SESSION_DIR=/app/data/sessions \
    WEB_HOST=0.0.0.0
RUN mkdir -p /app/data/posters /app/data/downloads /app/data/thumbnails /app/data/torrents /app/data/sessions
VOLUME ["/app/data"]

# NOTE: EXPOSE is documentation only. The port is reachable from the internet ONLY if you run with
#   docker run -p 8080:8080 ...   (or use the provided docker-compose.yml)
# AND the VPS firewall / cloud security group allows inbound TCP 8080.
EXPOSE 8080 6881-6891/tcp 6881-6891/udp

# Healthcheck against the FastAPI server
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD curl -f http://localhost:${PORT:-${WEB_PORT:-8080}}/health || exit 1

CMD ["python", "main.py"]
