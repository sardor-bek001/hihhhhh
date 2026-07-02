"""
MUSIQA BOT — Telegram orqali YouTube'dan musiqa qidirish va yuklab olish
Python 3.12 + Aiogram 3.x + yt-dlp + aiosqlite + ffmpeg

Ishga tushirish:
    1) pip install -r requirements.txt
       (yoki kamida: pip install --upgrade yt-dlp aiogram aiosqlite aiohttp)
    2) ffmpeg o'rnatilganligini tekshiring (apt install ffmpeg)
    3) BOT_TOKEN va ADMIN_IDS environment variable orqali bering (pastda CONFIG bo'limida)
    4) python3 main.py
"""

import os
import re
import time
import asyncio
import logging
import functools
import hashlib
from collections import deque

import aiohttp
import aiosqlite
import yt_dlp

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    CallbackQuery,
    FSInputFile,
    ReplyKeyboardRemove,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ParseMode, ChatAction
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError


# =====================================================================================
#  CONFIG
# =====================================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Local .env faylini yuklash (agar mavjud bo'lsa)
env_path = os.path.join(BASE_DIR, ".env")
if os.path.exists(env_path):
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN environment variable topilmadi. "
        "Terminalda: export BOT_TOKEN=\"...\" yoki .env faylga yozing."
    )

# Adminlar ID'lari
ADMIN_IDS = {
    int(x) for x in os.getenv("ADMIN_IDS", "6452883628").split(",") if x.strip().isdigit()
}

# Add BASE_DIR to system PATH so that yt-dlp and ffmpeg commands can find ffmpeg.exe and ffprobe.exe
os.environ["PATH"] = BASE_DIR + os.pathsep + os.environ.get("PATH", "")

DB_PATH = os.path.join(BASE_DIR, "musicbot.db")
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
LOG_FILE = os.path.join(BASE_DIR, "bot.log")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

MAX_TELEGRAM_FILE_MB = 50          # Telegram Bot API yuklash limiti
AUDIO_BITRATE_PRIMARY = "192"      # kbps
AUDIO_BITRATE_FALLBACK = "128"     # agar fayl katta bo'lsa

RATE_LIMIT_WINDOW_SEC = 60
RATE_LIMIT_MAX_REQUESTS = 8        # 60 soniyada nechta so'rov

GLOBAL_DOWNLOAD_SEMAPHORE = asyncio.Semaphore(3)   # bir vaqtda max 3 ta yuklash
SEARCH_RESULTS_LIMIT = 5

YOUTUBE_URL_REGEX = re.compile(
    r"(?:youtube\.com/(?:watch\?v=|shorts/|embed/)|youtu\.be/)([a-zA-Z0-9_-]{11})"
)

# Instagram URL'larini aniqlash uchun regex
INSTAGRAM_URL_REGEX = re.compile(
    r"(?:instagram\.com|instagr\.am)/(?:p|reel|reels|tv)/([A-Za-z0-9_-]+)"
)

# ffmpeg loyiha papkasida mavjud bo'lsa (masalan Windows-da local ishga tushirganda) shuni ko'rsatamiz, 
# aks holda tizimdagisini (Docker/Linux) ishlatishi uchun bo'sh qoldiramiz.
if os.path.exists(os.path.join(BASE_DIR, "ffmpeg.exe")) or os.path.exists(os.path.join(BASE_DIR, "ffmpeg")):
    FFMPEG_DIR = BASE_DIR
else:
    FFMPEG_DIR = None


# YouTube 2025-2026'dan beri "PO Token" talab qilib, ko'p so'rovlarni bloklamoqda.
# tv_embedded va android_creator clientlari PO Token talab qilmaydi va video uchun yaxshi ishlaydi.
YTDLP_EXTRACTOR_ARGS_PRIMARY = {
    "youtube": {
        "player_client": ["tv_embedded", "mweb"],
        "formats": ["missing_pot"],
    }
}
YTDLP_EXTRACTOR_ARGS_FALLBACK = {
    "youtube": {
        "player_client": ["web_embedded", "android_vr"],
        "formats": ["missing_pot"],
    }
}
YTDLP_EXTRACTOR_ARGS_EXTRA = {
    "youtube": {
        "player_client": ["ios", "android"],
        "formats": ["missing_pot"],
    }
}
# COOKIES_FILE: agar yt-dlp "Sign in to confirm you're not a bot" deb xato bersa,
# brauzerdan eksport qilingan cookies.txt faylini shu yo'lga qo'ying (Netscape format).
COOKIES_FILE = os.getenv("YTDLP_COOKIES_FILE", "")

# yt-dlp uchun umumiy User-Agent (ba'zi YouTube bloklarini kamaytirishga yordam beradi)
YTDLP_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


# =====================================================================================
#  LOGGING
# =====================================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("musicbot")
logging.getLogger("aiogram").setLevel(logging.WARNING)


# =====================================================================================
#  DATABASE
# =====================================================================================

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER PRIMARY KEY,
                username    TEXT,
                first_name  TEXT,
                joined_at   TEXT DEFAULT (datetime('now')),
                last_active TEXT DEFAULT (datetime('now')),
                is_banned   INTEGER DEFAULT 0,
                request_count INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS downloads (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                video_id    TEXT,
                title       TEXT,
                fmt         TEXT,           -- 'mp3' yoki 'mp4'
                file_size   INTEGER,
                from_cache  INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS cache (
                video_id     TEXT,
                fmt          TEXT,           -- 'mp3' yoki 'mp4'
                title        TEXT,
                artist       TEXT,
                duration     INTEGER,
                thumbnail    TEXT,
                file_id      TEXT,
                file_size    INTEGER,
                download_count INTEGER DEFAULT 0,
                created_at   TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (video_id, fmt)
            );

            CREATE TABLE IF NOT EXISTS statistics (
                key   TEXT PRIMARY KEY,
                value INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS error_logs (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER,
                error_text TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS url_map (
                url_hash   TEXT PRIMARY KEY,
                url        TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
            """
        )
        await db.executemany(
            "INSERT OR IGNORE INTO statistics (key, value) VALUES (?, 0)",
            [("total_searches",), ("total_downloads",), ("cache_hits",), ("errors",)],
        )
        await db.commit()
    logger.info("Database tayyor: %s", DB_PATH)


async def db_execute(query: str, params: tuple = ()) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(query, params)
        await db.commit()


async def db_fetchone(query: str, params: tuple = ()):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(query, params)
        return await cur.fetchone()


async def db_fetchall(query: str, params: tuple = ()):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(query, params)
        return await cur.fetchall()


async def increment_stat(key: str, by: int = 1) -> None:
    await db_execute(
        "UPDATE statistics SET value = value + ? WHERE key = ?", (by, key)
    )


async def upsert_user(user_id: int, username: str | None, first_name: str | None) -> None:
    await db_execute(
        """
        INSERT INTO users (user_id, username, first_name)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username = excluded.username,
            first_name = excluded.first_name,
            last_active = datetime('now'),
            request_count = request_count + 1
        """,
        (user_id, username, first_name),
    )


async def is_user_banned(user_id: int) -> bool:
    row = await db_fetchone("SELECT is_banned FROM users WHERE user_id = ?", (user_id,))
    return bool(row and row[0] == 1)


async def set_ban(user_id: int, banned: bool) -> None:
    await db_execute("UPDATE users SET is_banned = ? WHERE user_id = ?", (1 if banned else 0, user_id))


async def get_cache(video_id: str, fmt: str):
    return await db_fetchone(
        "SELECT title, artist, duration, thumbnail, file_id, file_size FROM cache WHERE video_id = ? AND fmt = ?",
        (video_id, fmt),
    )


async def save_cache(video_id: str, fmt: str, title: str, artist: str, duration: int,
                      thumbnail: str, file_id: str, file_size: int) -> None:
    await db_execute(
        """
        INSERT INTO cache (video_id, fmt, title, artist, duration, thumbnail, file_id, file_size, download_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(video_id, fmt) DO UPDATE SET
            file_id = excluded.file_id,
            download_count = download_count + 1
        """,
        (video_id, fmt, title, artist, duration, thumbnail, file_id, file_size),
    )


async def bump_cache_hit(video_id: str, fmt: str) -> None:
    await db_execute(
        "UPDATE cache SET download_count = download_count + 1 WHERE video_id = ? AND fmt = ?",
        (video_id, fmt),
    )


async def log_download(user_id: int, video_id: str, title: str, fmt: str, file_size: int, from_cache: bool) -> None:
    await db_execute(
        "INSERT INTO downloads (user_id, video_id, title, fmt, file_size, from_cache) VALUES (?, ?, ?, ?, ?, ?)",
        (user_id, video_id, title, fmt, file_size, 1 if from_cache else 0),
    )
    await increment_stat("total_downloads")
    if from_cache:
        await increment_stat("cache_hits")


async def log_error(user_id: int | None, error_text: str) -> None:
    await db_execute(
        "INSERT INTO error_logs (user_id, error_text) VALUES (?, ?)", (user_id, error_text[:2000])
    )
    await increment_stat("errors")


async def get_top_songs(limit: int = 10):
    return await db_fetchall(
        "SELECT title, artist, fmt, download_count FROM cache ORDER BY download_count DESC LIMIT ?",
        (limit,),
    )


async def get_user_history(user_id: int, limit: int = 10):
    return await db_fetchall(
        "SELECT title, fmt, created_at FROM downloads WHERE user_id = ? ORDER BY id DESC LIMIT ?",
        (user_id, limit),
    )


async def get_user_stats(user_id: int):
    row = await db_fetchone(
        "SELECT COUNT(*), COALESCE(SUM(file_size),0) FROM downloads WHERE user_id = ?", (user_id,)
    )
    return row  # (count, total_bytes)


async def get_global_stats():
    total_users = (await db_fetchone("SELECT COUNT(*) FROM users"))[0]
    banned_users = (await db_fetchone("SELECT COUNT(*) FROM users WHERE is_banned = 1"))[0]
    stats_rows = await db_fetchall("SELECT key, value FROM statistics")
    stats = {k: v for k, v in stats_rows}
    today_downloads = (await db_fetchone(
        "SELECT COUNT(*) FROM downloads WHERE date(created_at) = date('now')"
    ))[0]
    return total_users, banned_users, stats, today_downloads


async def get_all_user_ids():
    rows = await db_fetchall("SELECT user_id FROM users WHERE is_banned = 0")
    return [r[0] for r in rows]


async def save_url_mapping(url_hash: str, url: str) -> None:
    await db_execute(
        "INSERT OR REPLACE INTO url_map (url_hash, url) VALUES (?, ?)",
        (url_hash, url),
    )


async def get_url_from_hash(url_hash: str) -> str | None:
    row = await db_fetchone(
        "SELECT url FROM url_map WHERE url_hash = ?",
        (url_hash,),
    )
    return row[0] if row else None


def get_url_hash(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()


# =====================================================================================
#  RATE LIMITING (xotirada, oddiy sliding-window)
# =====================================================================================

_user_request_times: dict[int, deque] = {}


def check_rate_limit(user_id: int) -> bool:
    """True qaytaradi -> ruxsat berilgan. False -> limitga tegib qolgan."""
    now = time.monotonic()
    dq = _user_request_times.setdefault(user_id, deque())
    while dq and now - dq[0] > RATE_LIMIT_WINDOW_SEC:
        dq.popleft()
    if len(dq) >= RATE_LIMIT_MAX_REQUESTS:
        return False
    dq.append(now)
    return True


# =====================================================================================
#  IN-MEMORY YORDAMCHI HOLATLAR
# =====================================================================================

# Qidiruv natijalarini vaqtincha saqlash: {user_id: {video_id: info_dict}}
SEARCH_CACHE: dict[int, dict[str, dict]] = {}

# Admin "reklama yuborish" holatini kutish: {admin_id: True/False}
ADMIN_WAITING_BROADCAST: set[int] = set()

# Progress holatlari: {progress_key: percent}
PROGRESS_STORE: dict[str, int] = {}

# Har bir foydalanuvchi uchun ketma-ket yuklash navbati (multi-download)
USER_LOCKS: dict[int, asyncio.Lock] = {}


def get_user_lock(user_id: int) -> asyncio.Lock:
    if user_id not in USER_LOCKS:
        USER_LOCKS[user_id] = asyncio.Lock()
    return USER_LOCKS[user_id]


# =====================================================================================
#  YORDAMCHI FUNKSIYALAR
# =====================================================================================

def format_duration(seconds: int | float | None) -> str:
    if not seconds:
        return "—"
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def make_progress_bar(percent: int, length: int = 14) -> str:
    percent = max(0, min(100, percent))
    filled = int(length * percent / 100)
    bar = "█" * filled + "░" * (length - filled)
    return f"[{bar}] {percent}%"


def human_size(num_bytes: int | float | None) -> str:
    if not num_bytes:
        return "0 MB"
    mb = num_bytes / (1024 * 1024)
    return f"{mb:.1f} MB"


def extract_video_id_from_url(text: str) -> str | None:
    match = YOUTUBE_URL_REGEX.search(text)
    return match.group(1) if match else None


def safe_filename(video_id: str, suffix: str) -> str:
    return os.path.join(DOWNLOAD_DIR, f"{video_id}_{suffix}")


def base_ydl_opts() -> dict:
    """Barcha yt-dlp chaqiruvlari uchun umumiy sozlamalar (PO-token mitigatsiyasi,
    cookies, user-agent, ffmpeg yo'li)."""
    opts = {
        "extractor_args": YTDLP_EXTRACTOR_ARGS_PRIMARY,
        "http_headers": {"User-Agent": YTDLP_USER_AGENT},
        "ffmpeg_location": FFMPEG_DIR,
        "retries": 5,
        "fragment_retries": 5,
        "continuedl": False,        # Eskirgan partial fayldan davom ettirmaslik (416 xatosini oldini oladi)
        "noresizebuffer": True,     # Buffer o'lchamini o'zgartirmaslik
        "socket_timeout": 30,       # Ulanish vaqt limiti
    }
    if COOKIES_FILE and os.path.exists(COOKIES_FILE):
        opts["cookiefile"] = COOKIES_FILE
    return opts


# =====================================================================================
#  YT-DLP: QIDIRUV
# =====================================================================================

async def search_youtube(query: str, limit: int = SEARCH_RESULTS_LIMIT) -> list[dict]:
    loop = asyncio.get_event_loop()

    def run() -> list[dict]:
        opts = {
            **base_ydl_opts(),
            "quiet": True,
            "no_warnings": True,
            "extract_flat": "in_playlist",
            "default_search": f"ytsearch{limit}",
            "noplaylist": True,
        }
        with yt_dlp.YoutubeDL(opts) as ydl:
            result = ydl.extract_info(query, download=False)
            return result.get("entries", []) or []

    entries = await loop.run_in_executor(None, run)
    cleaned = []
    for e in entries:
        if not e:
            continue
        cleaned.append(
            {
                "id": e.get("id"),
                "title": e.get("title") or "Noma'lum",
                "duration": e.get("duration"),
                "uploader": e.get("uploader") or e.get("channel") or "Noma'lum",
                "thumbnail": e.get("thumbnail") or (e.get("thumbnails") or [{}])[-1].get("url"),
            }
        )
    return cleaned


# =====================================================================================
#  YT-DLP: YUKLASH (audio / video) — progress hook bilan
# =====================================================================================

def _build_download_opts(video_id: str, fmt: str, hook, extractor_args: dict) -> dict:
    common = {
        **base_ydl_opts(),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "progress_hooks": [hook],
    }
    common["extractor_args"] = extractor_args

    if fmt == "mp3":
        common.update(
            {
                "format": "bestaudio/best",
                "outtmpl": safe_filename(video_id, "%(id)s.%(ext)s"),
                "postprocessors": [
                    {
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": "mp3",
                        "preferredquality": AUDIO_BITRATE_PRIMARY,
                    }
                ],
            }
        )
    else:  # mp4
        common.update(
            {
                "format": "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/bestvideo[height<=720]+bestaudio/best[height<=720]/best",
                "outtmpl": safe_filename(video_id, "%(id)s_video.%(ext)s"),
                "merge_output_format": "mp4",
            }
        )
    return common


async def download_media(video_id: str, fmt: str, progress_key: str, source: str = "yt") -> dict:
    """
    fmt: 'mp3' yoki 'mp4'
    source: 'yt' (YouTube) yoki 'ig' (Instagram)
    Qaytaradi: {"path": ..., "title": ..., "artist": ..., "duration": ..., "thumbnail": ...}
    """
    if source == "ig":
        url = f"https://www.instagram.com/reel/{video_id}/"
    elif source == "yt":
        url = f"https://www.youtube.com/watch?v={video_id}"
    else:  # source == "url"
        url = await get_url_from_hash(video_id)
        if not url:
            raise ValueError("Havola topilmadi yoki muddati o'tgan.")
    loop = asyncio.get_event_loop()

    def hook(d: dict) -> None:
        if d.get("status") == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            downloaded = d.get("downloaded_bytes", 0)
            percent = int(downloaded / total * 100) if total else 0
            PROGRESS_STORE[progress_key] = min(percent, 99)
        elif d.get("status") == "finished":
            PROGRESS_STORE[progress_key] = 100

    def run_with(extractor_args: dict) -> dict:
        opts = _build_download_opts(video_id, fmt, hook, extractor_args)
        # Instagram uchun extractor_args kerak emas
        if source == "ig":
            opts.pop("extractor_args", None)
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            filepath = ydl.prepare_filename(info)
            if fmt == "mp3":
                filepath = os.path.splitext(filepath)[0] + ".mp3"

            # Agar fayl limitdan katta bo'lsa va mp3 bo'lsa -> pastroq bitrate bilan qayta kodlash
            if fmt == "mp3" and os.path.exists(filepath):
                size_mb = os.path.getsize(filepath) / (1024 * 1024)
                if size_mb > MAX_TELEGRAM_FILE_MB:
                    compressed = filepath.replace(".mp3", "_low.mp3")
                    os.system(
                        f'ffmpeg -y -i "{filepath}" -b:a {AUDIO_BITRATE_FALLBACK}k "{compressed}" -loglevel quiet'
                    )
                    if os.path.exists(compressed):
                        os.remove(filepath)
                        filepath = compressed

            return {
                "path": filepath,
                "title": info.get("title") or "Noma'lum",
                "artist": info.get("uploader") or info.get("channel") or "Noma'lum",
                "duration": int(info.get("duration") or 0),
                "thumbnail": info.get("thumbnail"),
            }

    def run() -> dict:
        try:
            return run_with(YTDLP_EXTRACTOR_ARGS_PRIMARY)
        except Exception as primary_err:
            logger.warning(
                "Asosiy yt-dlp client(lar) bilan yuklab bo'lmadi (%s), fallback client bilan urinilmoqda...",
                primary_err,
            )
            try:
                return run_with(YTDLP_EXTRACTOR_ARGS_FALLBACK)
            except Exception as fallback_err:
                logger.warning("Fallback ham muvaffaqiyatsiz (%s), extra client bilan urinilmoqda...", fallback_err)
                try:
                    return run_with(YTDLP_EXTRACTOR_ARGS_EXTRA)
                except Exception as extra_err:
                    logger.error("Barcha clientlar muvaffaqiyatsiz: %s", extra_err)
                    raise extra_err

    return await loop.run_in_executor(None, run)


async def download_thumbnail(url: str | None, video_id: str) -> str | None:
    if not url:
        return None
    path = safe_filename(video_id, "thumb.jpg")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    with open(path, "wb") as f:
                        f.write(await resp.read())
                    return path
    except Exception as e:
        logger.warning("Thumbnail yuklab bo'lmadi: %s", e)
    return None


async def progress_updater(bot: Bot, chat_id: int, message_id: int, progress_key: str, title: str) -> None:
    """Yuklash davom etayotganda xabarni progress bar bilan yangilab turadi."""
    last_percent = -1
    while True:
        percent = PROGRESS_STORE.get(progress_key, 0)
        if percent != last_percent:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=f"⬇️ Yuklanmoqda: <b>{title}</b>\n\n{make_progress_bar(percent)}",
                )
            except TelegramBadRequest:
                pass
            last_percent = percent
        if percent >= 100:
            break
        await asyncio.sleep(1.5)


# =====================================================================================
#  BOT / DISPATCHER
# =====================================================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)


# =====================================================================================
#  KLAVIATURALAR
# =====================================================================================

def main_menu_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="🔥 Top Hitlar", callback_data="top")
    kb.button(text="📜 Tarixim", callback_data="history")
    kb.button(text="📊 Statistikam", callback_data="mystats")
    kb.button(text="💡 Yordam", callback_data="help")
    kb.adjust(2, 2)
    return kb.as_markup()


def search_results_kb(user_id: int, results: list[dict]):
    NUM_EMOJIS = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    kb = InlineKeyboardBuilder()
    for i, r in enumerate(results):
        emoji = NUM_EMOJIS[i] if i < len(NUM_EMOJIS) else "🎵"
        label = f"{emoji} {r['title'][:38]} • {format_duration(r['duration'])}"
        kb.button(text=label, callback_data=f"sel:{r['id']}")
    kb.adjust(1)
    return kb.as_markup()


def format_choice_kb(video_id: str, source: str = "yt"):
    kb = InlineKeyboardBuilder()
    kb.button(text="🎧 MP3 Audio", callback_data=f"fmt:{video_id}:mp3:{source}")
    kb.button(text="🎬 MP4 Video", callback_data=f"fmt:{video_id}:mp4:{source}")
    kb.button(text="✨ Ikkalasi (MP3 + MP4)", callback_data=f"fmt:{video_id}:both:{source}")
    kb.adjust(2, 1)
    return kb.as_markup()


def admin_panel_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Statistika", callback_data="adm:stats")
    kb.button(text="🔥 Top Hitlar", callback_data="adm:top")
    kb.button(text="📢 Reklama", callback_data="adm:broadcast")
    kb.button(text="🧹 Cache tozalash", callback_data="adm:clearcache")
    kb.adjust(2, 2)
    return kb.as_markup()


def is_generic_title(title: str) -> bool:
    """Video sarlavhasi umumiy/noaniq ekanligini tekshiradi."""
    if not title or len(title.strip()) < 3:
        return True
    title_lower = title.lower().strip()
    generic_keywords = [
        "video_note", "video musiqasi", "user_vid", "savevid_net",
        "telenavobot", "video-to-mp3", "telegram", "mp4", "mp3", "video",
    ]
    return any(kw in title_lower for kw in generic_keywords)


def video_options_kb(file_hash: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="🎧 Videodagi musiqa (MP3)", callback_data=f"vidopt:{file_hash}:mp3")
    kb.button(text="🎬 Videoning o'zi (MP4)", callback_data=f"vidopt:{file_hash}:mp4")
    kb.button(text="🔍 To'liq musiqasini qidirish", callback_data=f"vidopt:{file_hash}:search")
    kb.adjust(1)
    return kb.as_markup()


# =====================================================================================
#  ERROR WRAPPER
# =====================================================================================

def safe_handler(func):
    @functools.wraps(func)
    async def wrapper(event, *args, **kwargs):
        user_id = None
        try:
            if isinstance(event, Message):
                user_id = event.from_user.id
            elif isinstance(event, CallbackQuery):
                user_id = event.from_user.id
            return await func(event, *args, **kwargs)
        except TelegramForbiddenError:
            pass  # foydalanuvchi botni bloklagan
        except Exception as e:
            logger.exception("Handler xatosi: %s", e)
            await log_error(user_id, repr(e))
            try:
                target = event.message if isinstance(event, CallbackQuery) else event
                await target.answer(
                    "⚠️ Kutilmagan xatolik yuz berdi. Iltimos, birozdan so'ng qaytadan urinib ko'ring."
                )
            except Exception:
                pass
    return wrapper


# =====================================================================================
#  USER HANDLERS
# =====================================================================================

@router.message(CommandStart())
@safe_handler
async def cmd_start(message: Message):
    await upsert_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    # Eski Reply Keyboard (avtosalon va boshqa botlar qoldirgan) ni o'chirish
    remove_msg = await message.answer(".", reply_markup=ReplyKeyboardRemove())
    await remove_msg.delete()
    text = (
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"  🎶 <b>MUSIQA BOT</b> 🎶\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👋 Salom, <b>{message.from_user.first_name}</b>!\n\n"
        f"🎵 Men sizga musiqa va video yuklab beraman:\n\n"
        f"🔹 Qo'shiq nomini yozing — qidirib topaman\n"
        f"🔹 YouTube havolasini yuboring\n"
        f"🔹 Instagram reels havolasini yuboring\n"
        f"🔹 MP3, MP4 yoki ikkalasini tanlang\n\n"
        f"🎤 Boshlash uchun qo'shiq nomini yozing 👇"
    )
    await message.answer(text, reply_markup=main_menu_kb())


@router.message(Command("help"))
@safe_handler
async def cmd_help(message: Message):
    await message.answer(
        "━━━━━━━━━━━━━━━━━━━━\n"
        "  💡 <b>YORDAM</b> 💡\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "📋 <b>Qanday foydalanish:</b>\n\n"
        "1️⃣ Qo'shiq nomini yozing\n"
        "   ↳ Natijalardan tanlang\n"
        "   ↳ 🎧 MP3 / 🎬 MP4 / ✨ Ikkalasi\n\n"
        "2️⃣ 🔗 YouTube havolasini yuboring\n"
        "3️⃣ 📸 Instagram reels havolasini yuboring\n"
        "4️⃣ 📝 Bir nechta nom yozing — barchasini yuklab beraman\n\n"
        "━━━ <b>Buyruqlar</b> ━━━\n\n"
        "🔥 /top — eng mashhur qo'shiqlar\n"
        "📜 /history — yuklashlar tarixingiz\n"
        "📊 /mystats — shaxsiy statistika\n"
        "🛠 /admin — admin panel"
    )


@router.message(Command("top"))
@router.callback_query(F.data == "top")
@safe_handler
async def cmd_top(event: Message | CallbackQuery):
    rows = await get_top_songs(10)
    if not rows:
        text = "😔 Hozircha hech narsa yuklanmagan."
    else:
        MEDALS = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
        lines = [
            "━━━━━━━━━━━━━━━━━━━━\n"
            "  🔥 <b>TOP HITLAR</b> 🔥\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
        ]
        for i, (title, artist, fmt, count) in enumerate(rows, start=0):
            medal = MEDALS[i] if i < len(MEDALS) else f"{i+1}."
            fmt_icon = "🎧" if fmt == "mp3" else "🎬"
            lines.append(f"{medal} <b>{title}</b>\n     ┗ 🎤 {artist} {fmt_icon} {count}x")
        text = "\n".join(lines)
    target = event.message if isinstance(event, CallbackQuery) else event
    await target.answer(text)
    if isinstance(event, CallbackQuery):
        await event.answer()


@router.message(Command("history"))
@router.callback_query(F.data == "history")
@safe_handler
async def cmd_history(event: Message | CallbackQuery):
    user_id = event.from_user.id
    rows = await get_user_history(user_id, 10)
    if not rows:
        text = "😔 Sizda hali yuklashlar tarixi yo'q."
    else:
        lines = [
            "━━━━━━━━━━━━━━━━━━━━\n"
            "  📜 <b>YUKLASHLAR TARIXI</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
        ]
        for title, fmt, created_at in rows:
            fmt_icon = "🎧" if fmt == "mp3" else "🎬"
            lines.append(f"{fmt_icon} <b>{title}</b>\n     ┗ 📅 {created_at}")
        text = "\n".join(lines)
    target = event.message if isinstance(event, CallbackQuery) else event
    await target.answer(text)
    if isinstance(event, CallbackQuery):
        await event.answer()


@router.message(Command("mystats"))
@router.callback_query(F.data == "mystats")
@safe_handler
async def cmd_mystats(event: Message | CallbackQuery):
    user_id = event.from_user.id
    count, total_bytes = await get_user_stats(user_id)
    text = (
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"  📊 <b>STATISTIKANGIZ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⬇️ Jami yuklashlar: <b>{count}</b>\n"
        f"💾 Jami hajm: <b>{human_size(total_bytes)}</b>\n\n"
        f"🎵 Yangi musiqa yuklash uchun nom yozing!"
    )
    target = event.message if isinstance(event, CallbackQuery) else event
    await target.answer(text)
    if isinstance(event, CallbackQuery):
        await event.answer()


@router.callback_query(F.data == "help")
@safe_handler
async def cb_help(callback: CallbackQuery):
    await cmd_help(callback.message)
    await callback.answer()


# =====================================================================================
#  ASOSIY OQIM: MATN XABAR (QIDIRUV YOKI YOUTUBE LINK)
# =====================================================================================

@router.message(F.text & ~F.text.startswith("/"))
@safe_handler
async def handle_text(message: Message):
    user_id = message.from_user.id

    # Agar admin "reklama yuborish" rejimida bo'lsa — bu xabar qidiruv emas, broadcast matni
    if is_admin(user_id) and user_id in ADMIN_WAITING_BROADCAST:
        await run_broadcast(message)
        return

    await upsert_user(user_id, message.from_user.username, message.from_user.first_name)

    if await is_user_banned(user_id):
        await message.answer("🚫 Kechirasiz, siz botdan foydalanishdan bloklangansiz.")
        return

    if not check_rate_limit(user_id):
        await message.answer(
            f"⏳ Juda ko'p so'rov! Iltimos, <b>{RATE_LIMIT_WINDOW_SEC} soniya</b> kutib, qayta urinib ko'ring."
        )
        return

    # Bir nechta qator = multi-download (har bir qator alohida so'rov)
    lines = [ln.strip() for ln in message.text.split("\n") if ln.strip()]

    for line in lines:
        # Instagram havolasini tekshirish
        ig_match = INSTAGRAM_URL_REGEX.search(line)
        if ig_match:
            ig_id = ig_match.group(1)
            status_msg_mp3 = await message.answer("📸 Instagram havola aniqlandi.\n🎧 MP3 tayyorlanmoqda... ⏳")
            lock = get_user_lock(user_id)
            async with lock:
                await process_download(user_id, ig_id, "mp3", status_msg_mp3, source="ig")
            status_msg_mp4 = await message.answer("🎬 MP4 tayyorlanmoqda... ⏳")
            async with lock:
                await process_download(user_id, ig_id, "mp4", status_msg_mp4, source="ig")
            continue

        video_id = extract_video_id_from_url(line)
        if video_id:
            status_msg_mp3 = await message.answer("🔗 YouTube havola aniqlandi.\n🎧 MP3 tayyorlanmoqda... ⏳")
            lock = get_user_lock(user_id)
            async with lock:
                await process_download(user_id, video_id, "mp3", status_msg_mp3, source="yt")
            status_msg_mp4 = await message.answer("🎬 MP4 tayyorlanmoqda... ⏳")
            async with lock:
                await process_download(user_id, video_id, "mp4", status_msg_mp4, source="yt")
            continue

        # Boshqa har qanday URL havola (TikTok, Facebook, Likee, va h.k.)
        if line.lower().startswith(("http://", "https://")):
            url_hash = get_url_hash(line)
            await save_url_mapping(url_hash, line)
            status_msg_mp3 = await message.answer("🔗 Havola aniqlandi.\n🎧 MP3 tayyorlanmoqda... ⏳")
            lock = get_user_lock(user_id)
            async with lock:
                await process_download(user_id, url_hash, "mp3", status_msg_mp3, source="url")
            status_msg_mp4 = await message.answer("🎬 MP4 tayyorlanmoqda... ⏳")
            async with lock:
                await process_download(user_id, url_hash, "mp4", status_msg_mp4, source="url")
            continue

        # Havola bo'lmasa qidiruv deb hisoblaymiz
        await handle_search_query(message, line)


async def handle_search_query(message: Message, query: str) -> None:
    await increment_stat("total_searches")
    searching_msg = await message.answer(f"🔍 <i>Qidirilmoqda:</i> <b>{query}</b>\n\n⏳ Iltimos kuting...")
    await bot.send_chat_action(message.chat.id, ChatAction.TYPING)

    try:
        results = await search_youtube(query, SEARCH_RESULTS_LIMIT)
    except Exception as e:
        logger.exception("Qidirishda xatolik: %s", e)
        await log_error(message.from_user.id, repr(e))
        await searching_msg.edit_text("❌ Qidirishda xatolik yuz berdi.\n💡 Boshqa nom bilan urinib ko'ring.")
        return

    if not results:
        await searching_msg.edit_text("😕 Hech narsa topilmadi.\n💡 Boshqa so'z bilan qidirib ko'ring!")
        return

    SEARCH_CACHE.setdefault(message.from_user.id, {})
    for r in results:
        SEARCH_CACHE[message.from_user.id][r["id"]] = r

    await searching_msg.edit_text(
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"  🔍 <b>NATIJALAR</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🎵 <b>{query}</b> bo'yicha:\n"
        f"👇 Birini tanlang:",
        reply_markup=search_results_kb(message.from_user.id, results),
    )


# =====================================================================================
#  CALLBACK: QIDIRUV NATIJASINI TANLASH
# =====================================================================================

@router.callback_query(F.data.startswith("sel:"))
@safe_handler
async def cb_select_result(callback: CallbackQuery):
    video_id = callback.data.split(":", 1)[1]
    user_id = callback.from_user.id
    await callback.answer("Boshlandi ⏳")
    
    status_msg_mp3 = await callback.message.edit_text("✅ <b>Tanlandi!</b>\n\n🎧 MP3 tayyorlanmoqda... ⏳")
    lock = get_user_lock(user_id)
    async with lock:
        await process_download(user_id, video_id, "mp3", status_msg_mp3, source="yt")
    
    status_msg_mp4 = await callback.message.answer("🎬 MP4 tayyorlanmoqda... ⏳")
    async with lock:
        await process_download(user_id, video_id, "mp4", status_msg_mp4, source="yt")


# =====================================================================================
#  ASOSIY OQIM: VIDEO FAYL QABUL QILISH — FORMAT TANLASH SO'RASIN
# =====================================================================================

@router.message(F.video | F.video_note | (F.document & F.document.mime_type.startswith("video/")))
@safe_handler
async def handle_video_file(message: Message):
    user_id = message.from_user.id

    if await is_user_banned(user_id):
        await message.answer("🚫 Kechirasiz, siz botdan foydalanishdan bloklangansiz.")
        return

    # File ID, turi va sarlavhasini aniqlash
    original_title = "Video Musiqasi"
    file_type = "video"
    if message.video:
        file_id = message.video.file_id
        file_type = "video"
        if message.video.file_name:
            original_title = os.path.splitext(message.video.file_name)[0]
    elif message.video_note:
        file_id = message.video_note.file_id
        file_type = "video_note"
    else:  # F.document
        file_id = message.document.file_id
        file_type = "document"
        if message.document.file_name:
            original_title = os.path.splitext(message.document.file_name)[0]

    # Caption (yozuv) borligini tekshirish — qidiruv uchun foydali
    if message.caption and not is_generic_title(message.caption):
        original_title = message.caption.strip()

    # Avtomatik ravishda videoni qaytarish va audiosini ajratib jo'natish
    status_msg_mp4 = await message.reply("🎬 Video yuborilmoqda... ⏳")
    try:
        if file_type == "video":
            await bot.send_video(chat_id=user_id, video=file_id)
        elif file_type == "video_note":
            await bot.send_video_note(chat_id=user_id, video_note=file_id)
        else:
            await bot.send_document(chat_id=user_id, document=file_id)
        await status_msg_mp4.delete()
    except Exception as e:
        logger.exception("Video yuborishda xatolik: %s", e)
        await status_msg_mp4.edit_text("❌ Videoni yuborishda xatolik yuz berdi.")
        
    status_msg_mp3 = await message.answer("🎧 Videodan MP3 musiqa ajratib olinmoqda... ⏳")
    await process_video_to_mp3(user_id, file_id, original_title, status_msg_mp3)


@router.callback_query(F.data.startswith("vidopt:"))
@safe_handler
async def cb_video_options(callback: CallbackQuery):
    parts = callback.data.split(":")
    file_hash = parts[1]
    action = parts[2]
    user_id = callback.from_user.id

    payload = await get_url_from_hash(file_hash)
    if not payload:
        await callback.answer("⚠️ Fayl ma'lumotlari topilmadi yoki eskirgan.", show_alert=True)
        return

    subparts = payload.split("|", 3)
    if len(subparts) < 4 or subparts[0] != "file":
        await callback.answer("⚠️ Noto'g'ri fayl ma'lumotlari.", show_alert=True)
        return

    file_type = subparts[1]
    file_id = subparts[2]
    original_title = subparts[3]

    await callback.answer()

    if action == "mp4":
        await callback.message.edit_text("🎬 Video yuborilmoqda...")
        try:
            if file_type == "video":
                await bot.send_video(chat_id=user_id, video=file_id)
            elif file_type == "video_note":
                await bot.send_video_note(chat_id=user_id, video_note=file_id)
            else:
                await bot.send_document(chat_id=user_id, document=file_id)
            await callback.message.delete()
        except Exception as e:
            logger.exception("Video yuborishda xatolik: %s", e)
            await callback.message.edit_text("❌ Videoni yuborishda xatolik yuz berdi.")

    elif action == "search":
        if is_generic_title(original_title):
            await callback.message.edit_text(
                "⚠️ <b>Videodan qo'shiq nomi aniqlanmadi.</b>\n\n"
                "💡 To'liq musiqasini topish uchun qo'shiq nomini yozing 👇\n"
                "<i>(masalan: Uzeyir Mehdizade - Yaxshi olar)</i>",
                reply_markup=main_menu_kb()
            )
        else:
            await callback.message.edit_text(
                f"🔍 <b>Qidirilmoqda:</b> <i>{original_title}</i>\n\n⏳ Iltimos kuting..."
            )
            await handle_search_query(callback.message, original_title)

    elif action == "mp3":
        status_msg = await callback.message.edit_text("🎧 Videodan MP3 musiqa ajratib olinmoqda... ⏳")
        await process_video_to_mp3(user_id, file_id, original_title, status_msg)


async def process_video_to_mp3(user_id: int, file_id: str, original_title: str, status_msg: Message) -> None:
    """Telegram video file_id'sidan MP3 musiqa ajratib oladi va foydalanuvchiga yuboradi."""
    await bot.send_chat_action(status_msg.chat.id, ChatAction.RECORD_VOICE)

    safe_name = f"user_vid_{user_id}_{int(time.time())}"
    local_video_path = os.path.join(DOWNLOAD_DIR, f"{safe_name}.mp4")
    local_audio_path = os.path.join(DOWNLOAD_DIR, f"{safe_name}.mp3")

    try:
        file_info = await bot.get_file(file_id)
        await bot.download_file(file_info.file_path, local_video_path)

        if not os.path.exists(local_video_path):
            await status_msg.edit_text("❌ Videoni yuklab olishda xatolik yuz berdi.")
            return

        os.system(f'ffmpeg -y -i "{local_video_path}" -vn -ab 192k -ar 44100 "{local_audio_path}" -loglevel quiet')

        if os.path.exists(local_audio_path):
            await status_msg.edit_text("📤 MP3 tayyor! Yuborilmoqda...")
            audio_file = FSInputFile(local_audio_path)
            await bot.send_audio(
                chat_id=user_id,
                audio=audio_file,
                title=original_title[:64],
                performer="Video-to-MP3",
            )
            await status_msg.delete()
        else:
            await status_msg.edit_text("❌ Videodan audio ajratib olishda xatolik yuz berdi (ffmpeg xatosi).")

    except Exception as e:
        logger.exception("Video fayl bilan ishlashda xatolik: %s", e)
        await log_error(user_id, f"Video-to-MP3 xatosi: {repr(e)}")
        await status_msg.edit_text("❌ Tizimda xatolik yuz berdi. Iltimos, qaytadan urinib ko'ring.")
    finally:
        cleanup_files(local_video_path, local_audio_path)


# =====================================================================================
#  CALLBACK: FORMAT TANLASH -> YUKLASH
# =====================================================================================

@router.callback_query(F.data.startswith("fmt:"))
@safe_handler
async def cb_choose_format(callback: CallbackQuery):
    parts = callback.data.split(":")
    # fmt:video_id:format:source
    video_id = parts[1]
    fmt = parts[2]
    source = parts[3] if len(parts) > 3 else "yt"
    user_id = callback.from_user.id

    if await is_user_banned(user_id):
        await callback.answer("🚫 Siz bloklangansiz.", show_alert=True)
        return

    if not check_rate_limit(user_id):
        await callback.answer("⏳ Juda ko'p so'rov. Birozdan so'ng urinib ko'ring.", show_alert=True)
        return

    await callback.answer("Boshlandi ⏳")

    if fmt == "both":
        # Ikkalasini yuklash: avval MP3, keyin MP4
        status_msg_mp3 = await callback.message.edit_text("🎧 MP3 tayyorlanmoqda... ⏳")
        lock = get_user_lock(user_id)
        async with lock:
            await process_download(user_id, video_id, "mp3", status_msg_mp3, source=source)
        status_msg_mp4 = await callback.message.answer("🎬 MP4 tayyorlanmoqda... ⏳")
        async with lock:
            await process_download(user_id, video_id, "mp4", status_msg_mp4, source=source)
    else:
        status_msg = await callback.message.edit_text("📥 Tayyorlanmoqda... ⏳")
        lock = get_user_lock(user_id)
        async with lock:
            await process_download(user_id, video_id, fmt, status_msg, source=source)


async def process_download(user_id: int, video_id: str, fmt: str, status_msg: Message, source: str = "yt") -> None:
    cached = await get_cache(video_id, fmt)

    if cached:
        title, artist, duration, thumbnail, file_id, file_size = cached
        await status_msg.edit_text(f"⚡ Keshdan topildi: <b>{title}</b>\n📤 Yuborilmoqda...")
        try:
            await send_media_to_user(user_id, fmt, file_id=file_id, title=title, artist=artist,
                                      duration=duration, caption_extra="(♻️ keshdan)")
            await bump_cache_hit(video_id, fmt)
            await log_download(user_id, video_id, title, fmt, file_size or 0, from_cache=True)
            await status_msg.delete()
        except Exception as e:
            logger.warning("Cache file_id eskirgan, qayta yuklanadi: %s", e)
            await download_fresh_and_send(user_id, video_id, fmt, status_msg, source=source)
        return

    await download_fresh_and_send(user_id, video_id, fmt, status_msg, source=source)


async def download_fresh_and_send(user_id: int, video_id: str, fmt: str, status_msg: Message, source: str = "yt") -> None:
    progress_key = f"{user_id}:{video_id}:{fmt}:{time.monotonic()}"
    PROGRESS_STORE[progress_key] = 0

    title_display = video_id
    if source == "url":
        url = await get_url_from_hash(video_id)
        if url:
            from urllib.parse import urlparse
            title_display = urlparse(url).netloc or "Havola"
        else:
            title_display = "Havola"

    updater_task = asyncio.create_task(
        progress_updater(bot, status_msg.chat.id, status_msg.message_id, progress_key, title_display)
    )

    async with GLOBAL_DOWNLOAD_SEMAPHORE:
        try:
            data = await download_media(video_id, fmt, progress_key, source=source)
        except Exception as e:
            PROGRESS_STORE[progress_key] = 100  # progress_updater tsiklini majburan to'xtatish
            await updater_task
            PROGRESS_STORE.pop(progress_key, None)
            logger.exception("Yuklashda xatolik: %s", e)
            await log_error(user_id, repr(e))
            await status_msg.edit_text(
                "━━━━━━━━━━━━━━━━━━━━\n"
                "  ❌ <b>XATOLIK</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "Yuklab bo'lmadi. Sabablar:\n\n"
                "🔸 Video mavjud emas\n"
                "🔸 Mintaqada bloklangan\n"
                "🔸 DRM himoyalangan\n"
                "🔸 Fayl juda katta\n\n"
                "💡 Boshqa video bilan urinib ko'ring!"
            )
            return

    # Yuklash muvaffaqiyatli tugadi — progress_updater'ni xotirjam to'xtatamiz,
    # SO'NGRA kalitni o'chiramiz (avval o'chirilsa, tsikl hech qachon 100%ni
    # ko'rmay abadiy kutib qolishi mumkin edi).
    PROGRESS_STORE[progress_key] = 100
    await updater_task
    PROGRESS_STORE.pop(progress_key, None)

    filepath = data["path"]
    if not os.path.exists(filepath):
        await status_msg.edit_text("❌ Fayl tayyorlanmadi.\n💡 Qaytadan urinib ko'ring!")
        return

    file_size = os.path.getsize(filepath)
    if file_size > MAX_TELEGRAM_FILE_MB * 1024 * 1024:
        await status_msg.edit_text(
            f"⚠️ Fayl hajmi {human_size(file_size)} — Telegram {MAX_TELEGRAM_FILE_MB}MB limitidan katta. "
            "Yuborib bo'lmaydi."
        )
        cleanup_files(filepath)
        return

    await status_msg.edit_text("📤 Fayl tayyor! Yuborilmoqda...")
    thumb_path = await download_thumbnail(data["thumbnail"], video_id)

    try:
        sent_file_id = await send_media_to_user(
            user_id,
            fmt,
            local_path=filepath,
            title=data["title"],
            artist=data["artist"],
            duration=data["duration"],
            thumb_path=thumb_path,
        )
        await save_cache(video_id, fmt, data["title"], data["artist"], data["duration"],
                          data["thumbnail"] or "", sent_file_id, file_size)
        await log_download(user_id, video_id, data["title"], fmt, file_size, from_cache=False)
        await status_msg.delete()
    except Exception as e:
        logger.exception("Yuborishda xatolik: %s", e)
        await log_error(user_id, repr(e))
        await status_msg.edit_text("❌ Faylni yuborishda xatolik yuz berdi.\n💡 Qayta urinib ko'ring.")
    finally:
        cleanup_files(filepath, thumb_path)


async def send_media_to_user(
    user_id: int,
    fmt: str,
    *,
    file_id: str | None = None,
    local_path: str | None = None,
    title: str,
    artist: str,
    duration: int,
    thumb_path: str | None = None,
    caption_extra: str = "",
) -> str:
    """Audio/video yuboradi va natijada Telegram file_id qaytaradi (keshlash uchun)."""
    caption = (
        f"🎵 <b>{title}</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"🎤 {artist}\n"
        f"⏱ {format_duration(duration)}\n"
        f"{caption_extra}"
    ).strip()
    thumb = FSInputFile(thumb_path) if thumb_path and os.path.exists(thumb_path) else None

    if fmt == "mp3":
        audio = file_id if file_id else FSInputFile(local_path)
        msg = await bot.send_audio(
            chat_id=user_id,
            audio=audio,
            title=title[:64],
            performer=artist[:64],
            duration=duration,
            caption=caption,
            thumbnail=thumb,
        )
        return msg.audio.file_id
    else:
        video = file_id if file_id else FSInputFile(local_path)
        msg = await bot.send_video(
            chat_id=user_id,
            video=video,
            duration=duration,
            caption=caption,
            thumbnail=thumb,
            supports_streaming=True,
        )
        return msg.video.file_id


def cleanup_files(*paths: str | None) -> None:
    for p in paths:
        if p and os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass


# =====================================================================================
#  ADMIN PANEL
# =====================================================================================

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


@router.message(Command("admin"))
@safe_handler
async def cmd_admin(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("🚫 Bu buyruq faqat adminlar uchun.")
        return
    await message.answer("🛠 <b>Admin panel</b>", reply_markup=admin_panel_kb())


@router.message(Command("ban"))
@safe_handler
async def cmd_ban(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Foydalanish: /ban <user_id>")
        return
    await set_ban(int(parts[1]), True)
    await message.answer(f"🚫 Foydalanuvchi {parts[1]} bloklandi.")


@router.message(Command("unban"))
@safe_handler
async def cmd_unban(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Foydalanish: /unban <user_id>")
        return
    await set_ban(int(parts[1]), False)
    await message.answer(f"✅ Foydalanuvchi {parts[1]} blokdan chiqarildi.")


@router.callback_query(F.data == "adm:stats")
@safe_handler
async def cb_admin_stats(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫", show_alert=True)
        return
    total_users, banned_users, stats, today = await get_global_stats()
    text = (
        "📊 <b>Umumiy statistika</b>\n\n"
        f"👥 Foydalanuvchilar: {total_users} (bloklangan: {banned_users})\n"
        f"⬇️ Jami yuklashlar: {stats.get('total_downloads', 0)}\n"
        f"📅 Bugungi yuklashlar: {today}\n"
        f"♻️ Keshdan berildi: {stats.get('cache_hits', 0)}\n"
        f"🔎 Jami qidiruvlar: {stats.get('total_searches', 0)}\n"
        f"❌ Xatoliklar: {stats.get('errors', 0)}"
    )
    await callback.message.edit_text(text, reply_markup=admin_panel_kb())
    await callback.answer()


@router.callback_query(F.data == "adm:top")
@safe_handler
async def cb_admin_top(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫", show_alert=True)
        return
    await cmd_top(callback)


@router.callback_query(F.data == "adm:clearcache")
@safe_handler
async def cb_admin_clear_cache(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫", show_alert=True)
        return
    await db_execute("DELETE FROM cache")
    await callback.message.edit_text("🧹 Cache tozalandi.", reply_markup=admin_panel_kb())
    await callback.answer("Tozalandi")


@router.callback_query(F.data == "adm:broadcast")
@safe_handler
async def cb_admin_broadcast_ask(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫", show_alert=True)
        return
    ADMIN_WAITING_BROADCAST.add(callback.from_user.id)
    await callback.message.answer(
        "📢 Reklama/xabar matnini yuboring. Bu xabar barcha foydalanuvchilarga jo'natiladi.\n"
        "Bekor qilish uchun /cancel yuboring."
    )
    await callback.answer()


@router.message(Command("cancel"))
@safe_handler
async def cmd_cancel(message: Message):
    ADMIN_WAITING_BROADCAST.discard(message.from_user.id)
    await message.answer("❎ Bekor qilindi.")


async def run_broadcast(message: Message) -> None:
    """Admin reklama/xabar matnini yuborganda barcha foydalanuvchilarga tarqatadi."""
    ADMIN_WAITING_BROADCAST.discard(message.from_user.id)

    user_ids = await get_all_user_ids()
    sent, failed = 0, 0
    progress_msg = await message.answer(f"📢 Yuborilmoqda... 0/{len(user_ids)}")

    for i, uid in enumerate(user_ids, start=1):
        try:
            await bot.copy_message(chat_id=uid, from_chat_id=message.chat.id, message_id=message.message_id)
            sent += 1
        except (TelegramForbiddenError, TelegramBadRequest):
            failed += 1
        await asyncio.sleep(0.05)  # flood-control uchun
        if i % 25 == 0:
            try:
                await progress_msg.edit_text(f"📢 Yuborilmoqda... {i}/{len(user_ids)}")
            except TelegramBadRequest:
                pass

    await progress_msg.edit_text(f"✅ Reklama yuborildi!\nMuvaffaqiyatli: {sent}\nXato/bloklangan: {failed}")


# =====================================================================================
#  ISHGA TUSHIRISH
# =====================================================================================

async def main() -> None:
    await init_db()
    logger.info("Bot ishga tushmoqda...")
    await bot.delete_webhook(drop_pending_updates=True)
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot to'xtatildi.")
        