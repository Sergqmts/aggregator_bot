import asyncio
import hashlib
import ipaddress
import json
import logging
import os
import re
import socket
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
import aiosqlite
import feedparser
from dotenv import load_dotenv
from groq import AsyncGroq

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
SOCIAL_API_URL = os.getenv("SOCIAL_API_URL", "http://localhost:5000")
DB_PATH = os.getenv("DB_PATH", "aggregator/aggregator.db")
RSSHUB_BASE = os.getenv("RSSHUB_BASE", "https://rsshub.app")

CHANNEL_INTERVAL = 15 * 60  # 15 minutes between channels
CLEANUP_INTERVAL = 24 * 60 * 60  # 24 hours
CLEANUP_TTL_DAYS = 7
GROQ_REQUEST_DELAY = 2  # seconds between groq calls within a channel

HARD_STOPWORDS = ["erid", "реклама", "промокод", "партнёрский", "рекламный", "#реклама", "спонсор"]

TOPIC_STOPWORDS = {
    "cooking":       ["доставка", "меню", "забронировать", "г. москва", "курьер"],
    "it":            ["вакансия", "зарплата", "курсы", "senior", "middle", "оффер", "найм"],
    "auto":          ["в продаже", "пробег", "подбор авто", "дилер", "цена"],
    "travel":        ["горящий тур", "вылет из", "отель дня", "виза", "турагентство"],
    "entertainment": ["билеты", "сеансы", "ставки", "казино"],
    "leisure":       [],
}

REWRITE_PROMPT = (
    "Перепиши этот пост для публикации в соцсети.\n"
    "Удали: авторство, призывы к подписке, ссылки на каналы (@mentions).\n"
    "Сохрани: полезные внешние ссылки, форматирование, списки.\n"
    "Пиши нейтрально, без авторского голоса оригинала.\n"
    "Если пост короче 400 символов — удали только атрибуцию, не расширяй.\n"
    "Если пост длиннее 800 символов — сожми до 800 символов."
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("aggregator")


def _is_safe_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        hostname = parsed.hostname
        if not hostname:
            return False
        for info in socket.getaddrinfo(hostname, None):
            addr = ipaddress.ip_address(info[4][0])
            if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved or addr.is_multicast:
                return False
        return True
    except Exception:
        return False


def load_config():
    base = Path(__file__).parent
    with open(base / "channels.json", encoding="utf-8") as f:
        channels = json.load(f)
    with open(base / "bots.json", encoding="utf-8") as f:
        bots_list = json.load(f)
    bots = {}
    for b in bots_list:
        token = os.getenv(b["bot_token_env"], "")
        if not token:
            log.warning("Missing env var %s for topic %s", b["bot_token_env"], b["topic"])
        bots[b["topic"]] = {"bot_token": token, "community_id": b["community_id"]}
    return channels, bots


async def init_db(db: aiosqlite.Connection):
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS publish_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            md5 TEXT NOT NULL UNIQUE,
            source_url TEXT,
            original_text TEXT,
            rewritten_text TEXT NOT NULL,
            media_url TEXT,
            topic TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            published_at TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS sent_hashes (
            md5 TEXT PRIMARY KEY,
            source_url TEXT,
            published_at TIMESTAMP NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_sent_hashes_source_url
            ON sent_hashes(source_url);
    """)
    await db.commit()


def is_stop_word(text: str, topic: str) -> bool:
    lower = text.lower()
    for w in HARD_STOPWORDS:
        if w in lower:
            return True
    for w in TOPIC_STOPWORDS.get(topic, []):
        if w in lower:
            return True
    return False


def parse_rss_entry(entry) -> tuple[str, str | None, str]:
    raw_html = entry.get("summary") or entry.get("description") or ""
    img_in_html = None
    m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', raw_html)
    if m:
        img_in_html = m.group(1)

    text = re.sub(r"<[^>]+>", "", raw_html).strip()
    source_url = entry.get("link") or ""

    media_url = None

    enclosures = entry.get("enclosures", [])
    if enclosures:
        media_url = enclosures[0].get("href")

    if not media_url:
        for item in entry.get("media_content", []):
            url = item.get("url", "")
            if url:
                media_url = url
                break

    if not media_url:
        for item in entry.get("media_thumbnail", []):
            url = item.get("url", "")
            if url:
                media_url = url
                break

    if not media_url:
        media_url = img_in_html

    return text, media_url, source_url


def compute_md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


async def is_duplicate(md5: str, source_url: str, db: aiosqlite.Connection) -> bool:
    async with db.execute(
        "SELECT 1 FROM sent_hashes WHERE md5 = ? OR source_url = ?", (md5, source_url)
    ) as cur:
        return await cur.fetchone() is not None


async def rewrite_with_groq(text: str, client: AsyncGroq) -> str | None:
    await asyncio.sleep(GROQ_REQUEST_DELAY)
    for attempt in range(4):
        try:
            resp = await client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": REWRITE_PROMPT},
                    {"role": "user", "content": text},
                ],
                max_tokens=1024,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            if "429" in str(e) or "rate" in str(e).lower():
                wait = 30 * (2 ** attempt)
                log.warning("Groq rate limit, retrying in %ds", wait)
                await asyncio.sleep(wait)
            else:
                log.error("Groq error: %s", e)
                return None
    log.error("Groq: exhausted retries, skipping post")
    return None


async def fetch_channel(
    session: aiohttp.ClientSession,
    username: str,
    topic: str,
    db: aiosqlite.Connection,
    groq_client: AsyncGroq,
):
    url = f"{RSSHUB_BASE}/telegram/channel/{username}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                log.warning("RSS fetch %s status %d", username, resp.status)
                return
            content = await resp.text()
    except Exception as e:
        log.error("RSS fetch %s error: %s", username, e)
        return

    feed = feedparser.parse(content)
    for entry in feed.entries:
        text, media_url, source_url = parse_rss_entry(entry)
        if not text:
            continue
        if is_stop_word(text, topic):
            continue
        md5 = compute_md5(text)
        if await is_duplicate(md5, source_url, db):
            continue

        rewritten = await rewrite_with_groq(text, groq_client)
        if not rewritten:
            continue
        if is_stop_word(rewritten, topic):
            continue

        try:
            await db.execute(
                """
                INSERT OR IGNORE INTO publish_queue
                    (md5, source_url, original_text, rewritten_text, media_url, topic)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (md5, source_url, text, rewritten, media_url, topic),
            )
            await db.commit()
            log.info("Queued post from @%s [%s]", username, topic)
        except Exception as e:
            log.error("DB insert error: %s", e)


async def publish_pending_for_topic(topic: str, bots: dict, db: aiosqlite.Connection):
    bot_config = bots.get(topic)
    if not bot_config:
        log.warning("No bot config for topic %s", topic)
        return
    async with aiohttp.ClientSession() as session:
        while True:
            async with db.execute(
                "SELECT id, md5, source_url, rewritten_text, media_url, topic "
                "FROM publish_queue WHERE status='pending' AND topic=? ORDER BY created_at LIMIT 1",
                (topic,),
            ) as cur:
                row = await cur.fetchone()
            if not row:
                break
            post = {
                "id": row[0], "md5": row[1], "source_url": row[2],
                "rewritten_text": row[3], "media_url": row[4], "topic": row[5],
            }
            ok = await publish_one(session, post, bot_config)
            status = "published" if ok else "failed"
            await db.execute(
                "UPDATE publish_queue SET status=?, published_at=datetime('now') WHERE id=?",
                (status, post["id"]),
            )
            if ok:
                await db.execute(
                    "INSERT OR IGNORE INTO sent_hashes (md5, source_url, published_at) "
                    "VALUES (?, ?, datetime('now'))",
                    (post["md5"], post["source_url"]),
                )
            await db.commit()
            log.info("Post %d → %s", post["id"], status)


async def fetch_loop(channels, bots, db: aiosqlite.Connection, groq_client: AsyncGroq):
    while True:
        for ch in channels:
            try:
                async with aiohttp.ClientSession() as session:
                    await fetch_channel(session, ch["username"], ch["topic"], db, groq_client)
                await publish_pending_for_topic(ch["topic"], bots, db)
            except Exception as e:
                log.error("fetch_loop error [%s]: %s", ch["username"], e)
            log.info("Channel @%s done, sleeping %ds", ch["username"], CHANNEL_INTERVAL)
            await asyncio.sleep(CHANNEL_INTERVAL)


async def publish_one(
    session: aiohttp.ClientSession,
    post: dict,
    bot_config: dict,
) -> bool:
    token = bot_config["bot_token"]
    community_id = bot_config["community_id"]
    endpoint = f"{SOCIAL_API_URL}/bot{token}/sendPost"
    source_url = post.get("source_url", "")
    text = post["rewritten_text"]
    if source_url:
        text = f"{text}\n\nИсточник: {source_url}"
    media_url = post["media_url"]

    if media_url:
        if not _is_safe_url(media_url):
            log.warning("media_url failed safety check, skipping media")
            media_url = None

    if media_url:
        try:
            async with session.get(media_url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 200:
                    content_type = r.headers.get("Content-Type", "").split(";")[0].strip()
                    if not content_type.startswith("image/"):
                        log.warning("media_url returned non-image content-type %r, skipping media", content_type)
                    else:
                        image_bytes = await r.read(10 * 1024 * 1024)  # 10 MB max
                        ext = content_type.split("/")[-1] or "jpg"
                        form = aiohttp.FormData()
                        form.add_field("community_id", str(community_id))
                        form.add_field("body", text)
                        form.add_field("media", image_bytes, filename=f"image.{ext}", content_type=content_type)
                        async with session.post(endpoint, data=form, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                            if resp.status == 200:
                                return True
                            body = await resp.text()
                            log.warning("sendPost with media: status %d body=%r", resp.status, body[:200])
                            if resp.status not in (400,):
                                return False
                            # 400 → fallback without media
                else:
                    log.warning("media download: status %d", r.status)
        except Exception as e:
            log.warning("media fetch/send error, fallback: %s", type(e).__name__)

    # JSON fallback (no media)
    try:
        async with session.post(
            endpoint,
            json={"community_id": community_id, "body": text},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status == 200:
                return True
            log.warning("sendPost (no media) status %d", resp.status)
            return False
    except Exception as e:
        log.error("sendPost error: %s", type(e).__name__)
        return False



async def cleanup_loop(db: aiosqlite.Connection):
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL)
        try:
            await db.execute(
                "DELETE FROM sent_hashes WHERE published_at < datetime('now', ?)",
                (f"-{CLEANUP_TTL_DAYS} days",),
            )
            await db.commit()
            log.info("Cleanup done")
        except Exception as e:
            log.error("cleanup_loop error: %s", e)


async def main():
    channels, bots = load_config()
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    groq_client = AsyncGroq(api_key=GROQ_API_KEY)

    async with aiosqlite.connect(DB_PATH) as db:
        await init_db(db)
        await asyncio.gather(
            fetch_loop(channels, bots, db, groq_client),
            cleanup_loop(db),
        )


if __name__ == "__main__":
    asyncio.run(main())
