"""
Discord Bot — Activity Tracker + Giveaways + Community Features
===============================================================
Requirements:
    pip install discord.py flask flask-cors psycopg2-binary tzdata
"""

import discord
from discord.ext import commands, tasks
import psycopg2
import psycopg2.extras
import datetime
import threading
import os
import hmac
import hashlib
import random as _random
import re as _re
import asyncio
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS

# ─── CONFIG ──────────────────────────────────────────────────────────────────
BOT_TOKEN        = os.environ.get("BOT_TOKEN", "")
DATABASE_URL     = os.environ.get("DATABASE_URL", "")
FLASK_PORT       = int(os.environ.get("PORT", 8080))
DASHBOARD        = "."
FW_SECRET        = os.environ.get("FW_SECRET", "")
FW_ORDER_CHANNEL = int(os.environ.get("FW_ORDER_CHANNEL", 0) or 0)
FW_GIFT_CHANNEL  = int(os.environ.get("FW_GIFT_CHANNEL", 0) or 0)

# Giveaway roles
GIVEAWAY_MANAGER_ROLE_IDS = {1166453168121581579, 1166454283630301375}
GIVEAWAY_ENTRY_ROLE_ID    = 1166481664176828496

# Community channels
GUILD_ID                 = 1127292710290735134
COTW_CHANNEL_ID          = 1486465944300818624   # Conspiracy of the Week + Polls
RABBIT_HOLE_CHANNEL_ID   = 1486466347821957252   # Daily Rabbit Hole
CIPHER_CHANNEL_ID        = 1486466469750378566   # Cipher of the Day
KEYWORD_ALERT_CHANNEL_ID = 1486468826739642468   # Private mod keyword alerts

EASTERN   = ZoneInfo("America/New_York")
POST_HOUR = 20  # 8 PM Eastern — daily posts + COTW phase triggers

# Podcast RSS
PODCAST_RSS_URL       = "https://anchor.fm/s/c0340eac/podcast/rss"
PODCAST_CHANNEL_ID    = 1127292711012139062
PODCAST_LISTENER_ROLE = 1166481664176828496
# ─────────────────────────────────────────────────────────────────────────────


# ═══════════════════════════════════════════════════════════════════════════════
# EDITABLE CONTENT LISTS  ← FIND THIS SECTION TO ADD / REMOVE CONTENT
# ═══════════════════════════════════════════════════════════════════════════════

# ─── RABBIT HOLES ─────────────────────────────────────────────────────────────
# Add as many dicts as you like. title + summary are required. tags are optional.
RABBIT_HOLES = [
    {
        "title": "The Philadelphia Experiment",
        "summary": (
            "In October 1943, the USS Eldridge was allegedly rendered invisible to radar and "
            "physically teleported from Philadelphia to Norfolk, Virginia and back. The experiment "
            "supposedly caused crew members to go insane, with some said to have been fused into "
            "the ship's hull. The US Navy denies it ever happened."
        ),
        "tags": ["military", "teleportation", "WWII"],
    },
    {
        "title": "The Phantom Time Hypothesis",
        "summary": (
            "German historian Heribert Illig proposed in 1991 that roughly 297 years of history — "
            "from 614 to 911 AD — were entirely fabricated. He claims Holy Roman Emperor Otto III, "
            "Pope Sylvester II, and Charlemagne conspired to place themselves at the year 1000 AD. "
            "If true, it is currently the year 1726."
        ),
        "tags": ["history", "time", "fabrication"],
    },
    {
        "title": "Project MKUltra",
        "summary": (
            "Declassified CIA documents confirm that from 1953–1973, the CIA ran a covert program "
            "experimenting with mind control — using LSD, hypnosis, and psychological torture on "
            "unwitting subjects including US and Canadian citizens. This one isn't a theory. It "
            "actually happened. Which raises the question: what else?"
        ),
        "tags": ["CIA", "mind control", "declassified", "confirmed"],
    },
    {
        "title": "The Tartaria Mud Flood",
        "summary": (
            "A theory holds that a vast empire called Tartaria was erased from history, and that "
            "a 'mud flood' — a catastrophic wave of mud — buried entire cities in the 1800s. "
            "Proponents point to buildings with ground-floor windows below street level as evidence. "
            "Grand old buildings, they claim, were dug out after the flood and repurposed."
        ),
        "tags": ["history", "lost civilisation", "architecture"],
    },
    {
        "title": "The Dyatlov Pass Incident",
        "summary": (
            "In February 1959, nine experienced Soviet hikers died in the Ural mountains under "
            "bizarre circumstances. Their tent was ripped open from the inside. They fled into "
            "-30°C cold without shoes. One had a fractured skull, another was missing her tongue. "
            "Soviet authorities closed the area for 3 years. The official cause: 'unknown compelling force.'"
        ),
        "tags": ["USSR", "unexplained", "death", "mountains"],
    },
    {
        "title": "The Denver Airport",
        "summary": (
            "Denver International Airport contains murals depicting a gas-masked soldier killing a "
            "dove, burning cities, and a child in a coffin. A statue of Anubis guards the terminal. "
            "A capstone refers to the 'New World Airport Commission' — an organisation that doesn't "
            "exist. The airport was built 16 months late and $2 billion over budget. Nobody knows "
            "what's underground."
        ),
        "tags": ["NWO", "airports", "symbolism", "underground"],
    },
    {
        "title": "Operation Northwoods",
        "summary": (
            "Declassified in 1997: in 1962, the US Joint Chiefs of Staff proposed Operation "
            "Northwoods — a plan to stage fake terrorist attacks on US soil and blame Cuba, "
            "justifying an invasion. Proposals included sinking a US ship, shooting down a drone "
            "disguised as a passenger plane, and bombing Miami. JFK rejected it."
        ),
        "tags": ["false flag", "Cuba", "declassified", "confirmed"],
    },
    {
        "title": "The Simulation Hypothesis",
        "summary": (
            "Philosopher Nick Bostrom argued in 2003 that at least one of three things must be "
            "true: civilisations go extinct before becoming technologically advanced; advanced "
            "civilisations lose interest in running simulations; or we are almost certainly living "
            "in a computer simulation. Elon Musk has said the odds we're in base reality are "
            "'one in billions.'"
        ),
        "tags": ["reality", "philosophy", "technology"],
    },
    {
        "title": "The Dead Internet Theory",
        "summary": (
            "A theory claiming the internet has been mostly dead since around 2016-2017 — populated "
            "largely by AI bots, automated content, and astroturfing campaigns by corporations and "
            "government agencies. Real human interaction online is now the minority. The goal: "
            "manufacture consensus, suppress dissent, and make you feel alone in your views."
        ),
        "tags": ["internet", "AI", "bots", "social media"],
    },
    {
        "title": "The Voynich Manuscript",
        "summary": (
            "A 240-page illustrated book written in an unknown script that no cryptographer, "
            "linguist, or AI has been able to decode. Carbon-dated to the 15th century. It contains "
            "diagrams of unknown plants, astrological charts, and what appear to be naked women in "
            "green tubes. Theories range from alien language to elaborate hoax. Author and purpose "
            "remain completely unknown."
        ),
        "tags": ["cryptography", "mystery", "history"],
    },
    {
        "title": "The Mandela Effect",
        "summary": (
            "Millions of people share identical false memories — the Berenstain/Berenstein Bears "
            "spelling, Nelson Mandela dying in prison in the 1980s, the Monopoly Man's monocle. "
            "Some attribute this to parallel universes bleeding together, others to CERN's Large "
            "Hadron Collider punching holes in reality. Psychologists call it confabulation. "
            "Theorists call it a cover-up."
        ),
        "tags": ["memory", "parallel universes", "CERN"],
    },
    {
        "title": "The Stargate Project",
        "summary": (
            "Declassified: from 1978 to 1995, the US government spent $20 million on Project "
            "Stargate — training psychic spies to remotely view Soviet military installations "
            "using only their minds. Participants claim to have viewed Mars in 1 million BC. "
            "The CIA concluded it had 'limited intelligence value.' They did not say it didn't work."
        ),
        "tags": ["CIA", "psychic", "remote viewing", "declassified"],
    },
    {
        "title": "The Georgia Guidestones",
        "summary": (
            "Erected in 1980 by an unknown person using a pseudonym, the Georgia Guidestones were "
            "granite slabs inscribed with ten commandments for humanity — including maintaining world "
            "population under 500 million. A time capsule buried beneath was never dated. They were "
            "destroyed by an explosion in 2022. The bomber was never found. The owner was never identified."
        ),
        "tags": ["NWO", "population control", "mystery"],
    },
    {
        "title": "The Black Knight Satellite",
        "summary": (
            "An unidentified object in polar orbit has been photographed by NASA since the 1960s. "
            "It allegedly has a 13,000-year-old orbit, based on signals decoded by radio operator "
            "Duncan Lunan. NASA claims it's a thermal blanket. Others claim it's an alien probe, "
            "pointing to Tesla's 1899 interception of a repeating signal from space as early evidence."
        ),
        "tags": ["space", "alien", "satellites", "NASA"],
    },
    {
        "title": "Operation Paperclip",
        "summary": (
            "Confirmed and declassified: after WWII, the US government secretly recruited over "
            "1,600 Nazi scientists, engineers, and doctors — including those who ran concentration "
            "camp experiments — and gave them new identities and government jobs. Werner von Braun, "
            "a former SS officer, became the head of NASA. The programme was hidden for decades."
        ),
        "tags": ["NASA", "Nazi", "confirmed", "declassified"],
    },
    {
        "title": "The Tavistock Institute",
        "summary": (
            "Founded in London in 1947, the Tavistock Institute is accused of being the world's "
            "foremost centre for mass brainwashing and social engineering. Theorists claim it "
            "orchestrated the 1960s counterculture movement — including the Beatles — to destabilise "
            "society, and that it continues to shape media, politics, and education to manufacture "
            "consent and state dependency."
        ),
        "tags": ["mind control", "UK", "social engineering", "Beatles"],
    },
    {
        "title": "The Hollow Earth Theory",
        "summary": (
            "Edmond Halley — of Halley's Comet — first proposed in 1692 that Earth is hollow, with "
            "nested inner spheres and a small interior sun. Admiral Richard Byrd's 1947 Arctic "
            "expedition diary allegedly describes flying into a hole at the pole and discovering a "
            "tropical land with mammoths. The diary's authenticity is disputed. The hole, they say, "
            "is not."
        ),
        "tags": ["earth", "inner world", "exploration"],
    },
    {
        "title": "The Bielefeld Conspiracy",
        "summary": (
            "Since 1994, a German internet joke-turned-serious theory claims the city of Bielefeld "
            "doesn't exist. It was allegedly invented by a shadowy group called 'SIE' to cover up "
            "something at Bielefeld University. In 2019, the city offered €1 million to anyone who "
            "could prove it doesn't exist. Nobody claimed the prize."
        ),
        "tags": ["cities", "Germany", "existence"],
    },
    {
        "title": "The Montauk Project",
        "summary": (
            "Allegedly a classified government programme run out of Montauk Air Force Station, New "
            "York, involving time travel, mind control, and inter-dimensional portals. Whistleblowers "
            "claim experiments were conducted on kidnapped children. The facility is real. The "
            "underground bunkers are real. What happened inside them is fiercely disputed."
        ),
        "tags": ["time travel", "military", "mind control", "underground"],
    },
    {
        "title": "The Jonestown Massacre",
        "summary": (
            "On November 18, 1978, 918 members of the Peoples Temple died in Jonestown, Guyana. "
            "Official story: mass suicide by cyanide-laced punch. But autopsy reports show injection "
            "marks on many bodies, and witnesses describe people being shot while fleeing. Jim Jones "
            "had documented CIA connections. Some researchers believe it was a mind-control experiment "
            "gone wrong."
        ),
        "tags": ["CIA", "cults", "mass death", "cover-up"],
    },
]

# ─── CIPHERS ──────────────────────────────────────────────────────────────────
# Add as many dicts as you like. All fields required.
CIPHERS = [
    {
        "title": "Caesar Cipher — Shift 3",
        "encoded":     "WKH WUXWK LV RXW WKHUH",
        "hint":        "Shift each letter **back** by 3 in the alphabet.",
        "solution":    "THE TRUTH IS OUT THERE",
        "explanation": "The classic Caesar cipher — used by Julius Caesar himself to protect military orders.",
    },
    {
        "title": "Atbash Cipher",
        "encoded":     "GSV TLEWH ZIV DZGXSRMT",
        "hint":        "Mirror the alphabet: A=Z, B=Y, C=X...",
        "solution":    "THE GODS ARE WATCHING",
        "explanation": "The Atbash cipher appears in the Hebrew Bible. Each letter maps to its mirror opposite.",
    },
    {
        "title": "ROT13",
        "encoded":     "GURL YVIR NZBAT HF",
        "hint":        "Rotate each letter by 13 positions.",
        "solution":    "THEY LIVE AMONG US",
        "explanation": "ROT13 is its own inverse — you encode and decode with the exact same operation.",
    },
    {
        "title": "Morse Code",
        "encoded":     "-- --- -. . -.-- / .. ... / -.-. --- -. - .-. --- .-..",
        "hint":        "Dots and dashes. Slash ( / ) separates words.",
        "solution":    "MONEY IS CONTROL",
        "explanation": "Invented by Samuel Morse in 1837 — the internet of its day.",
    },
    {
        "title": "Binary — ASCII",
        "encoded":     "01010111 01000001 01001011 01000101 / 01010101 01010000",
        "hint":        "Convert each 8-bit group from binary to ASCII. Slash separates words.",
        "solution":    "WAKE UP",
        "explanation": "Everything in a computer is ones and zeros. This message hides in plain byte.",
    },
    {
        "title": "Reverse Cipher",
        "encoded":     "DLROW EHT LORTNOC YEHT",
        "hint":        "Read it backwards.",
        "solution":    "THEY CONTROL THE WORLD",
        "explanation": "The simplest cipher — reverse the entire string.",
    },
    {
        "title": "Caesar Cipher — Shift 7",
        "encoded":     "AOL JSVJR PZ H SPL",
        "hint":        "Shift each letter back by 7.",
        "solution":    "THE CLOCK IS A LIE",
        "explanation": "A slightly deeper Caesar shift, harder to brute force at a glance.",
    },
    {
        "title": "Atbash Cipher II",
        "encoded":     "WLMG GIFHG GSV NVWRZ",
        "hint":        "Mirror the alphabet: A=Z, B=Y, C=X...",
        "solution":    "DONT TRUST THE MEDIA",
        "explanation": "Once you know the mirror, no message stays secret.",
    },
    {
        "title": "NATO Phonetic Acrostic",
        "encoded":     "ECHO VICTOR ECHO ROMEO YANKEE TANGO HOTEL INDIA NOVEMBER GOLF",
        "hint":        "Take the first letter of each NATO word.",
        "solution":    "EVERYTHING",
        "explanation": "The NATO phonetic alphabet was designed for clarity — but doubles as a clean acrostic cipher.",
    },
    {
        "title": "Caesar Cipher — Shift 17",
        "encoded":     "ESPCP TD EFOOLZ MPY MPSVD",
        "hint":        "Shift each letter back by 17.",
        "solution":    "THERE IS TUNNEL BELOW",
        "explanation": "A deep Caesar shift. Only 25 possible keys — but you have to know to try.",
    },
    {
        "title": "Hexadecimal — ASCII",
        "encoded":     "4E 6F 74 68 69 6E 67 20 49 73 20 52 65 61 6C",
        "hint":        "Convert each hex pair to its ASCII character.",
        "solution":    "Nothing Is Real",
        "explanation": "Hexadecimal is base-16. Each pair represents one character — used in low-level computing.",
    },
    {
        "title": "Caesar Cipher — Shift 13 Reverse",
        "encoded":     "FBIYL ZNBRF FU LHQOBF",
        "hint":        "First reverse the whole string, then apply ROT13.",
        "solution":    "FOLLOW THE MONEY",
        "explanation": "Layered ciphers compound difficulty — a classic technique in real cryptography.",
    },
    {
        "title": "Caesar Cipher — Shift 5",
        "encoded":     "YMJD FSN QNXYJS",
        "hint":        "Shift each letter back by 5.",
        "solution":    "THEY ARE LISTEN",
        "explanation": "Simple, fast, surprisingly effective for field communications.",
    },
    {
        "title": "Pig Latin Variant",
        "encoded":     "ETHAY UTHTRUSTAY ILLWAY EEFRAY OUYAY",
        "hint":        "Move the first consonant cluster to the end and add -AY.",
        "solution":    "THE TRUTH WILL FREE YOU",
        "explanation": "Pig Latin variants have been used as simple verbal obfuscation codes for centuries.",
    },
    {
        "title": "Atbash Cipher III",
        "encoded":     "GSV HFMWRZO RHOZMWH ZIV IVZO",
        "hint":        "Mirror the alphabet: A=Z, B=Y, C=X...",
        "solution":    "THE SUNDIAL ISLANDS ARE REAL",
        "explanation": "A harder Atbash challenge with longer words. Frequency analysis is your friend.",
    },
]

# ─── RATE MY THEORY — CONTENT BANKS (edit freely) ────────────────────────────
EVIDENCE_COMMENTS = [
    (1, "Basically vibes"),
    (2, "A Reddit post from 2009"),
    (2, "One grainy YouTube video"),
    (3, "Your cousin told you"),
    (3, "An anonymous forum post"),
    (4, "A deleted Wikipedia edit"),
    (4, "Three blurry photographs"),
    (5, "A dream you had"),
    (5, "'They' don't want you to know"),
    (6, "A documentary your friend sent"),
    (6, "Declassified-but-misread documents"),
    (7, "Pattern-matched across 12 open tabs"),
    (8, "A surprisingly well-sourced thread"),
    (9, "Actual primary sources (suspicious)"),
]

PLAUSIBILITY_COMMENTS = [
    (1, "Physics would like a word"),
    (2, "Only if gravity is fake too"),
    (3, "Deep in tinfoil territory"),
    (4, "Possible on a slow Tuesday"),
    (5, "Could happen honestly"),
    (6, "Surprisingly reasonable"),
    (7, "Actually kind of compelling"),
    (8, "Uncomfortably plausible"),
    (9, "We'd be shocked if this wasn't true"),
]

TINFOIL_LEVELS = [
    (1, "🧢 Bare-headed. Zero protection whatsoever."),
    (2, "🎩 Light coverage. Amateur hour."),
    (3, "🎩🎩 Double-hatted. Respectable."),
    (4, "🎩🎩🎩 Triple threat. They've noticed you."),
    (5, "🎩🎩🎩🎩 Full cranial fortress. Stay safe out there."),
]

THEORY_VERDICTS = [
    "Your sources are 'trust me bro' but your energy is immaculate. 🕵️",
    "The government is shaking right now. Probably.",
    "We'd rate this higher but the black helicopters keep circling our servers.",
    "Compelling. Unhinged. We love it.",
    "This theory has been filed with the Bureau of Plausible Nonsense.",
    "5/5 would lose sleep over this. Thank you for your service.",
    "The lizard people give this a 4/10. Make of that what you will.",
    "Bold. Brave. Possibly monitored. Carry on.",
    "We cannot confirm or deny. Which, as you know, means yes.",
    "This is either genius or a cry for help. Either way, we're in.",
    "Somewhere a CIA analyst just choked on their coffee.",
    "The Illuminati would like you to stop. Immediately.",
    "Your tinfoil is showing. Don't change.",
    "This theory has legs. We recommend running.",
    "Approved by the Shadow Council. Temporarily.",
]

NUMBER_EMOJIS = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]

# ═══════════════════════════════════════════════════════════════════════════════
# END EDITABLE CONTENT
# ═══════════════════════════════════════════════════════════════════════════════


api = Flask(__name__, static_folder=DASHBOARD)
CORS(api)

# ─── DATABASE ────────────────────────────────────────────────────────────────
def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn

def init_db():
    with get_db() as db:
        with db.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id           SERIAL PRIMARY KEY,
                    user_id      TEXT NOT NULL,
                    username     TEXT NOT NULL,
                    channel_id   TEXT NOT NULL,
                    channel_name TEXT NOT NULL,
                    guild_id     TEXT NOT NULL,
                    hour         INTEGER NOT NULL,
                    day_of_week  INTEGER NOT NULL,
                    timestamp    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(user_id, channel_id, timestamp)
                );
                CREATE TABLE IF NOT EXISTS user_stats (
                    user_id    TEXT PRIMARY KEY,
                    username   TEXT NOT NULL,
                    guild_id   TEXT NOT NULL,
                    msg_count  INTEGER DEFAULT 0,
                    last_seen  TIMESTAMPTZ,
                    first_seen TIMESTAMPTZ,
                    avatar_url TEXT
                );
                CREATE TABLE IF NOT EXISTS orders (
                    id           SERIAL PRIMARY KEY,
                    order_id     TEXT UNIQUE,
                    event_type   TEXT NOT NULL,
                    buyer_name   TEXT,
                    buyer_email  TEXT,
                    product_name TEXT,
                    total_amount NUMERIC(10,2),
                    currency     TEXT DEFAULT 'USD',
                    status       TEXT,
                    raw          JSONB,
                    timestamp    TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS giveaways (
                    id           SERIAL PRIMARY KEY,
                    guild_id     TEXT NOT NULL,
                    channel_id   TEXT NOT NULL,
                    message_id   TEXT,
                    prize        TEXT NOT NULL,
                    description  TEXT,
                    host_id      TEXT NOT NULL,
                    winner_mode  TEXT NOT NULL DEFAULT 'announce',
                    winner_count INTEGER NOT NULL DEFAULT 1,
                    ends_at      TIMESTAMPTZ NOT NULL,
                    ended        BOOLEAN DEFAULT FALSE,
                    winner_ids   TEXT[] DEFAULT '{}',
                    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS giveaway_entries (
                    id          SERIAL PRIMARY KEY,
                    giveaway_id INTEGER NOT NULL REFERENCES giveaways(id) ON DELETE CASCADE,
                    user_id     TEXT NOT NULL,
                    username    TEXT NOT NULL,
                    avatar_url  TEXT,
                    entered_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(giveaway_id, user_id)
                );
                CREATE TABLE IF NOT EXISTS cotw_submissions (
                    id           SERIAL PRIMARY KEY,
                    guild_id     TEXT NOT NULL,
                    user_id      TEXT NOT NULL,
                    username     TEXT NOT NULL,
                    theory       TEXT NOT NULL,
                    week         TEXT NOT NULL,
                    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(guild_id, user_id, week)
                );
                CREATE TABLE IF NOT EXISTS cotw_state (
                    guild_id    TEXT NOT NULL,
                    week        TEXT NOT NULL,
                    phase       TEXT DEFAULT 'pending',
                    vote_msg_id TEXT,
                    PRIMARY KEY (guild_id, week)
                );
                CREATE TABLE IF NOT EXISTS keyword_alerts (
                    id       SERIAL PRIMARY KEY,
                    guild_id TEXT NOT NULL,
                    keyword  TEXT NOT NULL,
                    added_by TEXT NOT NULL,
                    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(guild_id, keyword)
                );
                CREATE TABLE IF NOT EXISTS daily_tasks_fired (
                    task_name  TEXT NOT NULL,
                    fired_date DATE NOT NULL,
                    PRIMARY KEY (task_name, fired_date)
                );
                CREATE INDEX IF NOT EXISTS idx_user             ON messages(user_id);
                CREATE INDEX IF NOT EXISTS idx_channel          ON messages(channel_id);
                CREATE INDEX IF NOT EXISTS idx_ts               ON messages(timestamp);
                CREATE INDEX IF NOT EXISTS idx_hour             ON messages(hour);
                CREATE INDEX IF NOT EXISTS idx_dow              ON messages(day_of_week);
                CREATE INDEX IF NOT EXISTS idx_order_ts         ON orders(timestamp);
                CREATE INDEX IF NOT EXISTS idx_order_type       ON orders(event_type);
                CREATE INDEX IF NOT EXISTS idx_giveaway_active  ON giveaways(ended, ends_at);
                CREATE INDEX IF NOT EXISTS idx_giveaway_entries ON giveaway_entries(giveaway_id);
                CREATE INDEX IF NOT EXISTS idx_cotw_week        ON cotw_submissions(guild_id, week);
                CREATE INDEX IF NOT EXISTS idx_keywords         ON keyword_alerts(guild_id);
            """)

def fetchall(query, params=()):
    with get_db() as db:
        with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()

def fetchone(query, params=()):
    with get_db() as db:
        with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchone()

def execute(query, params=()):
    with get_db() as db:
        with db.cursor() as cur:
            cur.execute(query, params)

def task_already_fired(task_name: str) -> bool:
    """Returns True if this named task has already fired today. Inserts a record if not."""
    today = datetime.date.today().isoformat()
    try:
        execute(
            "INSERT INTO daily_tasks_fired (task_name, fired_date) VALUES (%s, %s)",
            (task_name, today),
        )
        return False  # Successfully inserted → hasn't fired yet
    except Exception:
        return True   # Unique violation → already fired today

def current_week() -> str:
    iso = datetime.date.today().isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"

# ─── KEYWORD CACHE ───────────────────────────────────────────────────────────
_kw_cache: dict = {}
_kw_cache_ts: dict = {}

def get_cached_keywords(guild_id: str) -> list:
    now = datetime.datetime.utcnow()
    last = _kw_cache_ts.get(guild_id)
    if last is None or (now - last).total_seconds() > 300:
        rows = fetchall("SELECT keyword FROM keyword_alerts WHERE guild_id = %s", (guild_id,))
        _kw_cache[guild_id]    = [r["keyword"].lower() for r in rows]
        _kw_cache_ts[guild_id] = now
    return _kw_cache.get(guild_id, [])

def invalidate_kw_cache(guild_id: str):
    _kw_cache.pop(guild_id, None)
    _kw_cache_ts.pop(guild_id, None)

# ─── STATIC ROUTES ───────────────────────────────────────────────────────────
@api.route("/")
def index():
    return send_from_directory(DASHBOARD, "dashboard.html")

@api.route("/logo.png")
def logo():
    return send_from_directory(DASHBOARD, "logo.png")

# ─── DISCORD API ROUTES ──────────────────────────────────────────────────────
@api.route("/api/overview")
def overview():
    try:
        msgs  = fetchone("SELECT COUNT(*) as c FROM messages")["c"]
        users = fetchone("SELECT COUNT(*) as c FROM user_stats")["c"]
        chans = fetchone("SELECT COUNT(DISTINCT channel_id) as c FROM messages")["c"]
        day   = fetchone("SELECT DATE(timestamp) as day, COUNT(*) as cnt FROM messages GROUP BY day ORDER BY cnt DESC LIMIT 1")
        return jsonify({"total_messages": msgs, "total_users": users, "total_channels": chans,
                        "most_active_day": str(day["day"]) if day else None,
                        "most_active_day_count": day["cnt"] if day else 0})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/online")
def online():
    try:
        count = sum(1 for guild in bot.guilds for member in guild.members
                    if member.status != discord.Status.offline and not member.bot)
        return jsonify({"online": count})
    except Exception:
        return jsonify({"online": 0})

@api.route("/api/leaderboard")
def leaderboard():
    try:
        rows = fetchall("""
            SELECT u.user_id, u.username, u.msg_count, u.last_seen, u.first_seen, u.avatar_url,
                   COUNT(DISTINCT DATE(m.timestamp)) as active_days,
                   SUM(CASE WHEN m.timestamp >= NOW() - INTERVAL '7 days' THEN 1 ELSE 0 END) as recent_msgs
            FROM user_stats u LEFT JOIN messages m ON u.user_id = m.user_id
            GROUP BY u.user_id, u.username, u.msg_count, u.last_seen, u.first_seen, u.avatar_url
            ORDER BY u.msg_count DESC LIMIT 25
        """)
        result = []
        for r in rows:
            d = dict(r)
            try:
                days_since = max(1, (datetime.datetime.utcnow() - r["first_seen"].replace(tzinfo=None)).days)
            except:
                days_since = 1
            d["engagement_score"] = round(
                min(100, ((r["active_days"] or 0) / days_since) * 100) * 0.4
                + min(100, (r["recent_msgs"] or 0) * 10) * 0.35
                + min(100, (r["msg_count"] or 0) / 10) * 0.25
            )
            d["last_seen"]  = r["last_seen"].isoformat()  if r["last_seen"]  else None
            d["first_seen"] = r["first_seen"].isoformat() if r["first_seen"] else None
            result.append(d)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/channels")
def channels():
    try:
        rows = fetchall("SELECT channel_name, COUNT(*) as cnt FROM messages GROUP BY channel_id, channel_name ORDER BY cnt DESC LIMIT 15")
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/heatmap")
def heatmap():
    try:
        rows   = fetchall("SELECT hour, COUNT(*) as cnt FROM messages GROUP BY hour")
        counts = {r["hour"]: r["cnt"] for r in rows}
        return jsonify([{"hour": h, "count": counts.get(h, 0)} for h in range(24)])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/daily")
def daily():
    try:
        rows = fetchall("SELECT DATE(timestamp) as day, COUNT(*) as cnt FROM messages GROUP BY day ORDER BY day DESC LIMIT 30")
        return jsonify(list(reversed([{"day": str(r["day"]), "cnt": r["cnt"]} for r in rows])))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/dayofweek")
def dayofweek():
    try:
        rows   = fetchall("SELECT day_of_week, COUNT(*) as cnt FROM messages GROUP BY day_of_week ORDER BY day_of_week")
        days   = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        counts = {r["day_of_week"]: r["cnt"] for r in rows}
        return jsonify([{"day": days[i], "count": counts.get(i, 0)} for i in range(7)])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/newmembers")
def newmembers():
    try:
        rows = fetchall("SELECT TO_CHAR(first_seen, 'IYYY-IW') as week, COUNT(*) as cnt FROM user_stats GROUP BY week ORDER BY week DESC LIMIT 12")
        return jsonify(list(reversed([dict(r) for r in rows])))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/compare")
def compare():
    try:
        rows  = fetchall("""SELECT SUM(CASE WHEN timestamp >= NOW()-INTERVAL '30 days' THEN 1 ELSE 0 END) as period_a,
                             SUM(CASE WHEN timestamp >= NOW()-INTERVAL '60 days' AND timestamp < NOW()-INTERVAL '30 days' THEN 1 ELSE 0 END) as period_b
                            FROM messages""")
        r     = rows[0] if rows else {}
        a, b  = r.get("period_a") or 0, r.get("period_b") or 0
        change   = round(((a - b) / b * 100) if b > 0 else 0, 1)
        daily_a  = fetchall("SELECT DATE(timestamp) as day, COUNT(*) as cnt FROM messages WHERE timestamp >= NOW()-INTERVAL '30 days' GROUP BY day ORDER BY day")
        daily_b  = fetchall("SELECT DATE(timestamp) as day, COUNT(*) as cnt FROM messages WHERE timestamp >= NOW()-INTERVAL '60 days' AND timestamp < NOW()-INTERVAL '30 days' GROUP BY day ORDER BY day")
        return jsonify({"period_a_total": a, "period_b_total": b, "change_pct": change,
                        "daily_a": [{"day": str(r["day"]), "cnt": r["cnt"]} for r in daily_a],
                        "daily_b": [{"day": str(r["day"]), "cnt": r["cnt"]} for r in daily_b]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/loyalty")
def loyalty():
    try:
        rows     = fetchall("SELECT msg_count FROM user_stats")
        max_msgs = max((r["msg_count"] for r in rows), default=1)
        core = active = member = 0
        for r in rows:
            ratio = r["msg_count"] / max_msgs
            if ratio > 0.6:    core   += 1
            elif ratio > 0.25: active += 1
            else:              member += 1
        return jsonify({"core": core, "active": active, "member": member})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/radar")
def radar():
    try:
        rows = fetchall("""
            SELECT u.user_id, u.username, u.msg_count, u.first_seen,
                   COUNT(DISTINCT DATE(m.timestamp)) as active_days,
                   SUM(CASE WHEN m.timestamp >= NOW() - INTERVAL '7 days' THEN 1 ELSE 0 END) as recent_msgs
            FROM user_stats u LEFT JOIN messages m ON u.user_id = m.user_id
            GROUP BY u.user_id, u.username, u.msg_count, u.first_seen ORDER BY u.msg_count DESC LIMIT 5
        """)
        result = []
        for r in rows:
            try:
                days_since = max(1, (datetime.datetime.utcnow() - r["first_seen"].replace(tzinfo=None)).days)
            except:
                days_since = 1
            result.append({"username": r["username"].split("#")[0],
                           "volume":      min(100, round((r["msg_count"] or 0) / 10)),
                           "consistency": min(100, round(((r["active_days"] or 0) / days_since) * 100)),
                           "recency":     min(100, (r["recent_msgs"] or 0) * 10)})
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/timeline")
def timeline():
    try:
        top = fetchall("SELECT user_id, username FROM user_stats ORDER BY msg_count DESC LIMIT 10")
        result = []
        for u in top:
            days = fetchall("SELECT DATE(timestamp) as day, COUNT(*) as cnt FROM messages WHERE user_id = %s AND timestamp >= NOW() - INTERVAL '30 days' GROUP BY day ORDER BY day", (u["user_id"],))
            result.append({"username": u["username"].split("#")[0], "days": [{"day": str(d["day"]), "cnt": d["cnt"]} for d in days]})
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/engagement-leaderboard")
def engagement_leaderboard():
    try:
        rows = fetchall("""
            SELECT u.user_id, u.username, u.msg_count, u.last_seen, u.first_seen, u.avatar_url,
                   COUNT(DISTINCT DATE(m.timestamp)) as active_days,
                   SUM(CASE WHEN m.timestamp >= NOW() - INTERVAL '7 days' THEN 1 ELSE 0 END) as recent_msgs
            FROM user_stats u LEFT JOIN messages m ON u.user_id = m.user_id
            GROUP BY u.user_id, u.username, u.msg_count, u.last_seen, u.first_seen, u.avatar_url
        """)
        result = []
        for r in rows:
            try:
                days_since = max(1, (datetime.datetime.utcnow() - r["first_seen"].replace(tzinfo=None)).days)
            except:
                days_since = 1
            score = round(
                min(100, ((r["active_days"] or 0) / days_since) * 100) * 0.4
                + min(100, (r["recent_msgs"] or 0) * 10) * 0.35
                + min(100, (r["msg_count"] or 0) / 10) * 0.25
            )
            result.append({"username": r["username"].split("#")[0], "avatar_url": r["avatar_url"],
                           "engagement_score": score,
                           "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None})
        result.sort(key=lambda x: x["engagement_score"], reverse=True)
        return jsonify(result[:15])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/daterange")
def daterange():
    try:
        start, end = request.args.get("start"), request.args.get("end")
        if not start or not end:
            return jsonify({"error": "start and end required"}), 400
        total     = fetchone("SELECT COUNT(*) as cnt FROM messages WHERE timestamp >= %s AND timestamp <= %s", (start, end))
        users     = fetchone("SELECT COUNT(DISTINCT user_id) as cnt FROM messages WHERE timestamp >= %s AND timestamp <= %s", (start, end))
        daily     = fetchall("SELECT DATE(timestamp) as day, COUNT(*) as cnt FROM messages WHERE timestamp >= %s AND timestamp <= %s GROUP BY day ORDER BY day", (start, end))
        top_users = fetchall("SELECT username, COUNT(*) as cnt FROM messages WHERE timestamp >= %s AND timestamp <= %s GROUP BY username ORDER BY cnt DESC LIMIT 10", (start, end))
        top_chans = fetchall("SELECT channel_name, COUNT(*) as cnt FROM messages WHERE timestamp >= %s AND timestamp <= %s GROUP BY channel_name ORDER BY cnt DESC LIMIT 8", (start, end))
        peak_hour = fetchone("SELECT hour, COUNT(*) as cnt FROM messages WHERE timestamp >= %s AND timestamp <= %s GROUP BY hour ORDER BY cnt DESC LIMIT 1", (start, end))
        return jsonify({"total_messages": total["cnt"] if total else 0, "unique_users": users["cnt"] if users else 0,
                        "peak_hour": peak_hour["hour"] if peak_hour else None,
                        "daily": [{"day": str(r["day"]), "cnt": r["cnt"]} for r in daily],
                        "top_users": [dict(r) for r in top_users], "top_channels": [dict(r) for r in top_chans]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─── GIVEAWAY DASHBOARD API ───────────────────────────────────────────────────
@api.route("/api/giveaways")
def api_giveaways():
    try:
        rows = fetchall("""
            SELECT g.id, g.prize, g.description, g.host_id, g.winner_mode, g.winner_count,
                   g.ends_at, g.ended, g.winner_ids, g.created_at, g.channel_id,
                   COUNT(e.id) as entry_count
            FROM giveaways g LEFT JOIN giveaway_entries e ON g.id = e.giveaway_id
            GROUP BY g.id ORDER BY g.created_at DESC LIMIT 50
        """)
        result = []
        for r in rows:
            d = dict(r)
            d["ends_at"]    = r["ends_at"].isoformat()    if r["ends_at"]    else None
            d["created_at"] = r["created_at"].isoformat() if r["created_at"] else None
            result.append(d)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/giveaways/<int:giveaway_id>/entries")
def api_giveaway_entries(giveaway_id):
    try:
        gw = fetchone("SELECT * FROM giveaways WHERE id = %s", (giveaway_id,))
        if not gw:
            return jsonify({"error": "Giveaway not found"}), 404
        entries = fetchall("SELECT user_id, username, avatar_url, entered_at FROM giveaway_entries WHERE giveaway_id = %s ORDER BY entered_at", (giveaway_id,))
        return jsonify({"giveaway": {"id": gw["id"], "prize": gw["prize"], "description": gw["description"],
                                     "winner_mode": gw["winner_mode"], "winner_count": gw["winner_count"],
                                     "ended": gw["ended"], "ends_at": gw["ends_at"].isoformat() if gw["ends_at"] else None,
                                     "winner_ids": gw["winner_ids"], "entry_count": len(entries)},
                        "entries": [{"user_id": e["user_id"], "username": e["username"],
                                     "avatar_url": e["avatar_url"], "entered_at": e["entered_at"].isoformat()} for e in entries]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─── FOURTHWALL API ───────────────────────────────────────────────────────────
@api.route("/api/fw/overview")
def fw_overview():
    try:
        total_orders  = fetchone("SELECT COUNT(*) as c FROM orders WHERE event_type = 'order.placed'")["c"]
        total_revenue = fetchone("SELECT COALESCE(SUM(total_amount),0) as r FROM orders WHERE event_type = 'order.placed'")["r"]
        total_gifts   = fetchone("SELECT COUNT(*) as c FROM orders WHERE event_type = 'gift.purchased'")["c"]
        total_subs    = fetchone("SELECT COUNT(*) as c FROM orders WHERE event_type = 'subscription.purchased'")["c"]
        last_order    = fetchone("SELECT buyer_name, product_name, total_amount, timestamp FROM orders ORDER BY timestamp DESC LIMIT 1")
        return jsonify({"total_orders": total_orders, "total_revenue": float(total_revenue),
                        "total_gifts": total_gifts, "total_subs": total_subs,
                        "last_order": {"buyer_name": last_order["buyer_name"], "product_name": last_order["product_name"],
                                       "total_amount": float(last_order["total_amount"]) if last_order["total_amount"] else 0,
                                       "timestamp": last_order["timestamp"].isoformat()} if last_order else None})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/fw/revenue")
def fw_revenue():
    try:
        rows = fetchall("SELECT DATE(timestamp) as day, SUM(total_amount) as revenue, COUNT(*) as orders FROM orders WHERE event_type = 'order.placed' GROUP BY day ORDER BY day DESC LIMIT 30")
        return jsonify(list(reversed([{"day": str(r["day"]), "revenue": float(r["revenue"]), "orders": r["orders"]} for r in rows])))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/fw/products")
def fw_products():
    try:
        rows = fetchall("SELECT product_name, COUNT(*) as cnt, SUM(total_amount) as revenue FROM orders WHERE event_type = 'order.placed' AND product_name IS NOT NULL GROUP BY product_name ORDER BY cnt DESC LIMIT 10")
        return jsonify([{"product": r["product_name"], "count": r["cnt"], "revenue": float(r["revenue"])} for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/fw/orders")
def fw_orders():
    try:
        rows = fetchall("SELECT order_id, event_type, buyer_name, product_name, total_amount, status, timestamp FROM orders ORDER BY timestamp DESC LIMIT 20")
        result = []
        for r in rows:
            d = dict(r)
            d["timestamp"]    = r["timestamp"].isoformat()
            d["total_amount"] = float(r["total_amount"]) if r["total_amount"] else 0
            result.append(d)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─── FOURTHWALL WEBHOOK ───────────────────────────────────────────────────────
@api.route("/webhook/fourthwall", methods=["POST"])
def fourthwall_webhook():
    body = request.get_data()
    data = request.get_json(silent=True) or {}
    print(f"✅ FW Webhook received! Type: {data.get('type','unknown')}")
    if FW_SECRET:
        sig      = request.headers.get("X-Fourthwall-Signature", "")
        expected = "sha256=" + hmac.new(FW_SECRET.encode(), body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            print(f"⚠️ FW Signature mismatch.")
    event_type   = data.get("type", "")
    payload      = data.get("data", {})
    buyer_name   = payload.get("buyerName") or payload.get("email", "Someone")
    total_raw    = payload.get("totalAmount") or payload.get("amount") or 0
    try:    total = float(str(total_raw).replace("$", "").replace(",", ""))
    except: total = 0.0
    total_fmt    = payload.get("totalFormatted") or f"${total:.2f}"
    status       = payload.get("status", "Processing")
    items        = payload.get("lineItems", [])
    product_name = items[0].get("productName", "Item") if items else payload.get("productName", "Item")
    if len(items) > 1: product_name += f" + {len(items)-1} more"
    order_id = payload.get("id") or payload.get("orderId") or str(datetime.datetime.utcnow().timestamp())
    try:
        execute("INSERT INTO orders (order_id,event_type,buyer_name,buyer_email,product_name,total_amount,status,raw) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (order_id) DO NOTHING",
                (order_id, event_type, buyer_name, payload.get("email",""), product_name, total, status, psycopg2.extras.Json(payload)))
    except Exception as e:
        print(f"⚠️ FW DB error: {e}")
    if event_type == "order.placed":
        msg = f"🛒 **New Order — The Conspiracy Podcast Store!**\n**{buyer_name}** just ordered **{product_name}**\n💰 Total: **{total_fmt}** · 📦 {status}\nThanks for supporting the podcast! 🎙️"
        asyncio.run_coroutine_threadsafe(_send_fw_alert(msg, FW_ORDER_CHANNEL), bot.loop)
    elif event_type == "gift.purchased":
        msg = f"🎁 **Gift Purchase!**\n**{buyer_name}** gifted **{product_name}**\n💰 Value: **{total_fmt}**\nWhat a legend! 🙌"
        asyncio.run_coroutine_threadsafe(_send_fw_alert(msg, FW_GIFT_CHANNEL or FW_ORDER_CHANNEL), bot.loop)
    elif event_type == "subscription.purchased":
        msg = f"🔔 **New Subscription!**\n**{buyer_name}** just subscribed to **{product_name}**\n💰 **{total_fmt}**\nWelcome to the inner circle! 🕵️"
        asyncio.run_coroutine_threadsafe(_send_fw_alert(msg, FW_GIFT_CHANNEL or FW_ORDER_CHANNEL), bot.loop)
    return jsonify({"ok": True}), 200

async def _send_fw_alert(msg, channel_id):
    if not channel_id: return
    channel = bot.get_channel(channel_id)
    if channel: await channel.send(msg)

# ─── BOT SETUP ───────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
intents.members         = True
intents.presences       = True
bot = commands.Bot(command_prefix="!", intents=intents)

def member_can_manage(member: discord.Member) -> bool:
    if member.guild_permissions.administrator: return True
    return bool({r.id for r in member.roles} & GIVEAWAY_MANAGER_ROLE_IDS)

def member_can_enter(member: discord.Member) -> bool:
    return any(r.id == GIVEAWAY_ENTRY_ROLE_ID for r in member.roles)

def check_manager():
    async def predicate(ctx):
        if member_can_manage(ctx.author): return True
        await ctx.send("❌ You don't have permission to use this command.", delete_after=10)
        return False
    return commands.check(predicate)


# ═══════════════════════════════════════════════════════════════════════════════
# GIVEAWAY SYSTEM
# ═══════════════════════════════════════════════════════════════════════════════

def parse_duration(text: str):
    m = _re.fullmatch(r"(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?", text.strip().lower())
    if not m or not any(m.groups()): return None
    td = datetime.timedelta(days=int(m.group(1) or 0), hours=int(m.group(2) or 0),
                            minutes=int(m.group(3) or 0), seconds=int(m.group(4) or 0))
    return td if td.total_seconds() > 0 else None

def format_remaining(ends_at) -> str:
    remaining = ends_at.replace(tzinfo=None) - datetime.datetime.utcnow()
    if remaining.total_seconds() <= 0: return "**ENDED**"
    total = int(remaining.total_seconds())
    d, h, m, s = total // 86400, (total % 86400) // 3600, (total % 3600) // 60, total % 60
    parts = []
    if d: parts.append(f"{d}d")
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    if not d and not h: parts.append(f"{s}s")
    return " ".join(parts) or "< 1s"

def build_giveaway_embed(gw, entry_count: int, ended: bool = False) -> discord.Embed:
    embed = discord.Embed(title=f"🎉  GIVEAWAY: {gw['prize']}", description=gw.get("description") or "",
                          color=discord.Color.gold() if not ended else discord.Color.light_grey())
    embed.add_field(name="⏰ Time Remaining" if not ended else "⏰ Status",
                    value=format_remaining(gw["ends_at"]) if not ended else "**ENDED**", inline=True)
    embed.add_field(name="🏆 Winners",  value=str(gw["winner_count"]), inline=True)
    embed.add_field(name="🎟️ Entries", value=str(entry_count),         inline=True)
    mode_labels = {"announce": "📢 Announced in channel", "dm_host": "🔒 DM'd to host only", "manual": "✋ Host picks manually"}
    embed.add_field(name="🎯 Winner Selection", value=mode_labels.get(gw["winner_mode"], "Random"), inline=True)
    embed.set_footer(text=f"Giveaway ID: {gw['id']}  •  You must have the required role to enter.")
    return embed

class GiveawayView(discord.ui.View):
    def __init__(self, giveaway_id: int):
        super().__init__(timeout=None)
        self.giveaway_id = giveaway_id
        btn = discord.ui.Button(label="🎉  Enter Giveaway", style=discord.ButtonStyle.green,
                                custom_id=f"giveaway_enter_{giveaway_id}")
        btn.callback = self._enter_callback
        self.add_item(btn)

    async def _enter_callback(self, interaction: discord.Interaction):
        gw = fetchone("SELECT * FROM giveaways WHERE id = %s", (self.giveaway_id,))
        if not gw or gw["ended"] or datetime.datetime.utcnow() > gw["ends_at"].replace(tzinfo=None):
            await interaction.response.send_message("❌ This giveaway has already ended!", ephemeral=True); return
        if not member_can_enter(interaction.user):
            await interaction.response.send_message("❌ You don't have the required role to enter.", ephemeral=True); return
        avatar = str(interaction.user.display_avatar.url) if interaction.user.display_avatar else ""
        try:
            execute("INSERT INTO giveaway_entries (giveaway_id, user_id, username, avatar_url) VALUES (%s,%s,%s,%s)",
                    (self.giveaway_id, str(interaction.user.id), str(interaction.user), avatar))
            count = fetchone("SELECT COUNT(*) as c FROM giveaway_entries WHERE giveaway_id = %s", (self.giveaway_id,))["c"]
            await interaction.response.send_message(f"✅ You're in! Good luck 🍀\n*{count} total {'entry' if count == 1 else 'entries'}*", ephemeral=True)
        except Exception as e:
            if "unique" in str(e).lower():
                await interaction.response.send_message("⚠️ You've already entered!", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Something went wrong — please try again.", ephemeral=True)

async def end_giveaway(giveaway_id: int):
    gw = fetchone("SELECT * FROM giveaways WHERE id = %s", (giveaway_id,))
    if not gw or gw["ended"]: return
    entries     = fetchall("SELECT user_id, username FROM giveaway_entries WHERE giveaway_id = %s ORDER BY entered_at", (giveaway_id,))
    entry_count = len(entries)
    picks, winner_ids, winner_mentions = [], [], []
    if entries and gw["winner_mode"] != "manual":
        picks           = _random.sample(entries, min(gw["winner_count"], len(entries)))
        winner_ids      = [p["user_id"] for p in picks]
        winner_mentions = [f"<@{p['user_id']}>" for p in picks]
    execute("UPDATE giveaways SET ended = TRUE, winner_ids = %s WHERE id = %s", (winner_ids, giveaway_id))
    gw_final = fetchone("SELECT * FROM giveaways WHERE id = %s", (giveaway_id,))
    channel  = bot.get_channel(int(gw["channel_id"]))
    if channel and gw["message_id"]:
        try:
            msg         = await channel.fetch_message(int(gw["message_id"]))
            ended_embed = build_giveaway_embed(gw_final, entry_count, ended=True)
            if winner_ids:               ended_embed.add_field(name="🏆 Winner(s)", value="\n".join(winner_mentions), inline=False)
            elif gw["winner_mode"] == "manual": ended_embed.add_field(name="🏆 Winner(s)", value="Host is selecting manually…", inline=False)
            else:                        ended_embed.add_field(name="🏆 Winner(s)", value="No entries — no winner!", inline=False)
            ended_view = discord.ui.View()
            ended_view.add_item(discord.ui.Button(label="Giveaway Ended", style=discord.ButtonStyle.grey,
                                                   disabled=True, custom_id=f"giveaway_ended_{giveaway_id}"))
            await msg.edit(embed=ended_embed, view=ended_view)
        except Exception as e:
            print(f"⚠️ Giveaway embed update error #{giveaway_id}: {e}")
    if channel:
        if gw["winner_mode"] == "announce" and winner_ids:
            await channel.send(f"🎉 **Giveaway ended!**\nCongratulations to {', '.join(winner_mentions)}! You won **{gw['prize']}**! 🏆\nPlease contact a moderator to claim your prize.")
        elif gw["winner_mode"] == "manual":
            await channel.send(f"🎉 **The '{gw['prize']}' giveaway has ended!**\nThe host will announce the winner(s) shortly.")
        elif gw["winner_mode"] == "dm_host" and winner_ids:
            await channel.send(f"🎉 **The '{gw['prize']}' giveaway has ended!** The winner has been notified privately. 🔒")
    if gw["winner_mode"] == "dm_host" and winner_ids:
        try:
            guild = bot.get_guild(int(gw["guild_id"]))
            host  = guild.get_member(int(gw["host_id"])) if guild else None
            if host:
                await host.send(f"🎉 **Giveaway ended — {gw['prize']}**\n\n**Winner(s):**\n" +
                                 "\n".join(f"• {p['username']} (ID: {p['user_id']})" for p in picks) +
                                 f"\n\nTotal entries: **{entry_count}**\nGiveaway ID: `{giveaway_id}`")
        except Exception as e:
            print(f"⚠️ Could not DM host: {e}")
    if entries:
        guild = bot.get_guild(int(gw["guild_id"]))
        for entry in entries:
            try:
                member = guild.get_member(int(entry["user_id"])) if guild else None
                if not member: continue
                dm_text = (f"🎉 **Congratulations — you won the '{gw['prize']}' giveaway!** 🏆\nA moderator will be in touch shortly."
                           if entry["user_id"] in winner_ids else
                           f"👋 The **'{gw['prize']}'** giveaway has ended. Better luck next time! 🍀")
                await member.send(dm_text)
                await asyncio.sleep(0.4)
            except Exception:
                pass

async def giveaway_countdown(giveaway_id: int):
    while True:
        gw = fetchone("SELECT * FROM giveaways WHERE id = %s", (giveaway_id,))
        if not gw or gw["ended"]: break
        remaining_secs = (gw["ends_at"].replace(tzinfo=None) - datetime.datetime.utcnow()).total_seconds()
        if remaining_secs <= 0:
            await end_giveaway(giveaway_id); break
        try:
            count   = fetchone("SELECT COUNT(*) as c FROM giveaway_entries WHERE giveaway_id = %s", (giveaway_id,))["c"]
            channel = bot.get_channel(int(gw["channel_id"]))
            if channel and gw["message_id"]:
                msg = await channel.fetch_message(int(gw["message_id"]))
                await msg.edit(embed=build_giveaway_embed(gw, count))
        except Exception as e:
            print(f"⚠️ Giveaway refresh error #{giveaway_id}: {e}")
        if remaining_secs > 3600:   await asyncio.sleep(300)
        elif remaining_secs > 600:  await asyncio.sleep(60)
        elif remaining_secs > 60:   await asyncio.sleep(30)
        else:                        await asyncio.sleep(max(1, remaining_secs - 1))

@bot.group(name="giveaway", aliases=["gw"], invoke_without_command=True)
async def giveaway_group(ctx):
    embed = discord.Embed(title="🎉 Giveaway Commands", color=discord.Color.gold())
    embed.add_field(name="Commands", value=(
        "`!giveaway start`         — Launch the setup wizard\n"
        "`!giveaway list`          — See all active giveaways\n"
        "`!giveaway end <id>`      — Force-end a giveaway early\n"
        "`!giveaway entries <id>`  — View everyone who entered\n"), inline=False)
    await ctx.send(embed=embed)

@giveaway_group.command(name="start")
@check_manager()
async def giveaway_start(ctx):
    async def ask(prompt, validator=None, error_msg="❌ Invalid input, please try again."):
        await ctx.send(prompt)
        for _ in range(3):
            try:
                reply = await bot.wait_for("message", timeout=60,
                                           check=lambda m: m.author == ctx.author and m.channel == ctx.channel)
                if reply.content.strip().lower() == "cancel": return "CANCEL"
                val = reply.content.strip()
                if validator is None or validator(val): return val
                await ctx.send(error_msg)
            except asyncio.TimeoutError:
                await ctx.send("⏰ Setup timed out."); return None
        await ctx.send("❌ Too many invalid attempts."); return None

    await ctx.send("🎉 **Giveaway Setup Wizard**\nAnswer 6 questions *(60s each)*. Type `cancel` to abort.\n─────────────────────────────────")
    prize = await ask("**[1/6] 🏆 What is the prize?**")
    if not prize or prize == "CANCEL": await ctx.send("❌ Cancelled."); return
    desc_raw = await ask("**[2/6] 📝 Description?** Type it or `skip` to leave blank:")
    if desc_raw is None or desc_raw == "CANCEL": await ctx.send("❌ Cancelled."); return
    description = None if desc_raw.lower() == "skip" else desc_raw
    channel_raw = await ask("**[3/6] 📣 Which channel?** Mention it or type `here`:")
    if channel_raw is None or channel_raw == "CANCEL": await ctx.send("❌ Cancelled."); return
    target_channel = ctx.channel
    if channel_raw.lower() != "here":
        match = _re.search(r"<#(\d+)>", channel_raw)
        if match:
            found = ctx.guild.get_channel(int(match.group(1)))
            target_channel = found if found else ctx.channel
    def valid_dur(v):
        td = parse_duration(v)
        return td is not None and 60 <= td.total_seconds() <= 86_400 * 30
    duration_raw = await ask("**[4/6] ⏰ Duration?** Format: `30m` `2h` `1d` `1h30m` *(min 1m, max 30d)*",
                             validator=valid_dur, error_msg="❌ Invalid. Try `30m`, `2h`, `1d`.")
    if duration_raw is None or duration_raw == "CANCEL": await ctx.send("❌ Cancelled."); return
    winners_raw = await ask("**[5/6] 🏆 How many winners?** *(1–20)*",
                            validator=lambda v: v.isdigit() and 1 <= int(v) <= 20,
                            error_msg="❌ Enter a number 1–20.")
    if winners_raw is None or winners_raw == "CANCEL": await ctx.send("❌ Cancelled."); return
    mode_raw = await ask("**[6/6] 🎯 Winner selection?**\n`1` — 📢 Announce in channel\n`2` — 🔒 DM host privately\n`3` — ✋ Host picks manually",
                         validator=lambda v: v.strip() in ("1","2","3"), error_msg="❌ Type 1, 2, or 3.")
    if mode_raw is None or mode_raw == "CANCEL": await ctx.send("❌ Cancelled."); return
    winner_mode = {"1":"announce","2":"dm_host","3":"manual"}[mode_raw.strip()]
    ends_at = datetime.datetime.utcnow() + parse_duration(duration_raw)
    with get_db() as db:
        with db.cursor() as cur:
            cur.execute("INSERT INTO giveaways (guild_id,channel_id,prize,description,host_id,winner_mode,winner_count,ends_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                        (str(ctx.guild.id), str(target_channel.id), prize, description, str(ctx.author.id), winner_mode, int(winners_raw), ends_at))
            giveaway_id = cur.fetchone()[0]
    gw   = fetchone("SELECT * FROM giveaways WHERE id = %s", (giveaway_id,))
    view = GiveawayView(giveaway_id)
    bot.add_view(view)
    gmsg = await target_channel.send(embed=build_giveaway_embed(gw, 0), view=view)
    execute("UPDATE giveaways SET message_id = %s WHERE id = %s", (str(gmsg.id), giveaway_id))
    if target_channel.id != ctx.channel.id:
        await ctx.send(f"✅ Giveaway launched in {target_channel.mention}! Prize: **{prize}** · ID: `{giveaway_id}`")
    asyncio.ensure_future(giveaway_countdown(giveaway_id))

@giveaway_group.command(name="list")
@check_manager()
async def giveaway_list(ctx):
    rows = fetchall("SELECT g.id, g.prize, g.ends_at, g.winner_count, g.channel_id, g.winner_mode, COUNT(e.id) as entry_count FROM giveaways g LEFT JOIN giveaway_entries e ON g.id = e.giveaway_id WHERE g.guild_id = %s AND g.ended = FALSE AND g.ends_at > NOW() GROUP BY g.id ORDER BY g.ends_at ASC", (str(ctx.guild.id),))
    if not rows: await ctx.send("📭 No active giveaways. Start one with `!giveaway start`!"); return
    embed = discord.Embed(title=f"🎉 Active Giveaways ({len(rows)})", color=discord.Color.gold())
    mode_icons = {"announce":"📢","dm_host":"🔒","manual":"✋"}
    for r in rows:
        embed.add_field(name=f"#{r['id']}  —  {r['prize']}",
                        value=f"📣 <#{r['channel_id']}>  ·  ⏰ {format_remaining(r['ends_at'])}  ·  🎟️ {r['entry_count']} entries  ·  🏆 {r['winner_count']}  ·  {mode_icons.get(r['winner_mode'],'?')}",
                        inline=False)
    await ctx.send(embed=embed)

@giveaway_group.command(name="end")
@check_manager()
async def giveaway_end(ctx, giveaway_id: int = None):
    if giveaway_id is None: await ctx.send("❌ Usage: `!giveaway end <id>`"); return
    gw = fetchone("SELECT * FROM giveaways WHERE id = %s AND guild_id = %s", (giveaway_id, str(ctx.guild.id)))
    if not gw: await ctx.send(f"❌ Giveaway `#{giveaway_id}` not found."); return
    if gw["ended"]: await ctx.send(f"⚠️ Already ended."); return
    await ctx.send(f"⏩ Force-ending **#{giveaway_id}: {gw['prize']}**…")
    await end_giveaway(giveaway_id)
    await ctx.send(f"✅ Done.")

@giveaway_group.command(name="entries")
@check_manager()
async def giveaway_entries_cmd(ctx, giveaway_id: int = None):
    if giveaway_id is None: await ctx.send("❌ Usage: `!giveaway entries <id>`"); return
    gw = fetchone("SELECT * FROM giveaways WHERE id = %s AND guild_id = %s", (giveaway_id, str(ctx.guild.id)))
    if not gw: await ctx.send(f"❌ Not found."); return
    entries = fetchall("SELECT username, entered_at FROM giveaway_entries WHERE giveaway_id = %s ORDER BY entered_at", (giveaway_id,))
    embed = discord.Embed(title=f"🎟️ Entries — #{giveaway_id}: {gw['prize']}", color=discord.Color.blurple())
    embed.description = f"**Status:** {'🟢 Active' if not gw['ended'] else '🔴 Ended'}  ·  **Total:** {len(entries)}"
    if not entries:
        embed.add_field(name="Entrants", value="No entries yet!", inline=False)
    else:
        names  = [f"`{e['username'].split('#')[0]}`" for e in entries]
        chunks = [names[i:i+20] for i in range(0, len(names), 20)]
        for i, chunk in enumerate(chunks):
            embed.add_field(name=f"Entrants {i*20+1}–{i*20+len(chunk)}", value="  ".join(chunk), inline=False)
    if gw["winner_ids"]:
        embed.add_field(name="🏆 Winner(s)", value=" ".join(f"<@{uid}>" for uid in gw["winner_ids"]), inline=False)
    await ctx.send(embed=embed)


# ═══════════════════════════════════════════════════════════════════════════════
# POLLS
# ═══════════════════════════════════════════════════════════════════════════════

@bot.command(name="poll")
@check_manager()
async def poll_cmd(ctx, *, args: str = ""):
    """Usage: !poll "Question" "Option 1" "Option 2" ... (up to 10 options)"""
    parts = _re.findall(r'"([^"]+)"', args)
    if len(parts) < 3:
        await ctx.send('❌ Usage: `!poll "Question" "Option 1" "Option 2" ...` (min 2 options, max 10, all in quotes)')
        return
    question = parts[0]
    options  = parts[1:11]  # max 10

    embed = discord.Embed(title=f"📊 {question}", color=discord.Color.blue())
    lines = []
    for i, opt in enumerate(options):
        lines.append(f"{NUMBER_EMOJIS[i]}  {opt}")
    embed.description = "\n\n".join(lines)
    embed.set_footer(text=f"Poll by {ctx.author.display_name}  •  React to vote!")

    try:
        await ctx.message.delete()
    except Exception:
        pass

    poll_msg = await ctx.channel.send(embed=embed)
    for i in range(len(options)):
        await poll_msg.add_reaction(NUMBER_EMOJIS[i])


# ═══════════════════════════════════════════════════════════════════════════════
# CONSPIRACY OF THE WEEK (COTW)
# ═══════════════════════════════════════════════════════════════════════════════

async def cotw_open_submissions(guild_id: str = None, manual: bool = False):
    gid  = guild_id or str(GUILD_ID)
    week = current_week()
    state = fetchone("SELECT * FROM cotw_state WHERE guild_id = %s AND week = %s", (gid, week))
    if state and state["phase"] != "pending" and not manual:
        return  # Already handled this week
    execute("INSERT INTO cotw_state (guild_id, week, phase) VALUES (%s,%s,'submission') ON CONFLICT (guild_id,week) DO UPDATE SET phase='submission'", (gid, week))
    channel = bot.get_channel(COTW_CHANNEL_ID)
    if not channel: return
    embed = discord.Embed(
        title="🕵️ Conspiracy of the Week — SUBMISSIONS OPEN!",
        description=(
            f"**Week {week}**\n\n"
            "Got a theory that keeps you up at night? This is your moment.\n\n"
            "📬 **Submit your conspiracy theory** using:\n"
            "`!submit <your theory>`\n\n"
            "📅 Submissions close **Friday at 8 PM ET** — then the community votes.\n"
            "🏆 Winner announced **Sunday at 8 PM ET**.\n\n"
            "*One submission per member per week. Keep it spicy. 🌶️*"
        ),
        color=discord.Color.dark_gold(),
    )
    embed.set_footer(text="The truth is out there. Probably.")
    await channel.send(embed=embed)

async def cotw_open_voting(guild_id: str = None, manual: bool = False):
    gid  = guild_id or str(GUILD_ID)
    week = current_week()
    state = fetchone("SELECT * FROM cotw_state WHERE guild_id = %s AND week = %s", (gid, week))
    if state and state["phase"] == "voting" and not manual:
        return
    submissions = fetchall("SELECT id, username, theory FROM cotw_submissions WHERE guild_id = %s AND week = %s ORDER BY submitted_at LIMIT 10", (gid, week))
    channel = bot.get_channel(COTW_CHANNEL_ID)
    if not channel: return
    if not submissions:
        await channel.send("📭 **No submissions this week — voting skipped.** Be ready to submit next Monday!")
        execute("INSERT INTO cotw_state (guild_id,week,phase) VALUES (%s,%s,'ended') ON CONFLICT (guild_id,week) DO UPDATE SET phase='ended'", (gid, week))
        return
    lines = []
    for i, sub in enumerate(submissions):
        lines.append(f"{NUMBER_EMOJIS[i]}  **{sub['username'].split('#')[0]}:** {sub['theory']}")
    embed = discord.Embed(
        title="🗳️ Conspiracy of the Week — VOTE NOW!",
        description="\n\n".join(lines) + "\n\n*React with the number of your favourite theory. Voting closes Sunday at 8 PM ET!*",
        color=discord.Color.purple(),
    )
    embed.set_footer(text=f"Week {week}  •  {len(submissions)} theories in the running")
    vote_msg = await channel.send(embed=embed)
    for i in range(len(submissions)):
        await vote_msg.add_reaction(NUMBER_EMOJIS[i])
    execute("INSERT INTO cotw_state (guild_id,week,phase,vote_msg_id) VALUES (%s,%s,'voting',%s) ON CONFLICT (guild_id,week) DO UPDATE SET phase='voting', vote_msg_id=%s",
            (gid, week, str(vote_msg.id), str(vote_msg.id)))

async def cotw_announce_winner(guild_id: str = None, manual: bool = False):
    gid  = guild_id or str(GUILD_ID)
    week = current_week()
    state = fetchone("SELECT * FROM cotw_state WHERE guild_id = %s AND week = %s", (gid, week))
    if not state or not state["vote_msg_id"]: return
    if state["phase"] == "ended" and not manual: return
    channel     = bot.get_channel(COTW_CHANNEL_ID)
    submissions = fetchall("SELECT id, username, user_id, theory FROM cotw_submissions WHERE guild_id = %s AND week = %s ORDER BY submitted_at LIMIT 10", (gid, week))
    if not channel or not submissions: return
    try:
        vote_msg = await channel.fetch_message(int(state["vote_msg_id"]))
        reaction_counts = {}
        for reaction in vote_msg.reactions:
            emoji = str(reaction.emoji)
            if emoji in NUMBER_EMOJIS:
                idx = NUMBER_EMOJIS.index(emoji)
                if idx < len(submissions):
                    reaction_counts[idx] = max(0, reaction.count - 1)  # subtract bot's own reaction
        execute("UPDATE cotw_state SET phase='ended' WHERE guild_id=%s AND week=%s", (gid, week))
        if not reaction_counts or max(reaction_counts.values()) == 0:
            await channel.send(f"📭 **Conspiracy of the Week ({week}) — No votes were cast!** The mystery lives on... for another week.")
            return
        winner_idx   = max(reaction_counts, key=reaction_counts.get)
        winner_sub   = submissions[winner_idx]
        winner_votes = reaction_counts[winner_idx]
        embed = discord.Embed(
            title="🏆 Conspiracy of the Week — WINNER ANNOUNCED!",
            description=(
                f"**Week {week} winner:**\n\n"
                f"*\"{winner_sub['theory']}\"*\n\n"
                f"— **{winner_sub['username'].split('#')[0]}** with **{winner_votes} vote{'s' if winner_votes != 1 else ''}** 🎉"
            ),
            color=discord.Color.gold(),
        )
        embed.set_footer(text="Submissions reopen Monday at 8 PM ET. Stay paranoid. 🕵️")
        winner_msg = await channel.send(embed=embed)
        try:
            await winner_msg.pin()
        except Exception:
            pass
        try:
            guild  = bot.get_guild(GUILD_ID)
            member = guild.get_member(int(winner_sub["user_id"])) if guild else None
            if member:
                await member.send(f"🏆 **Congratulations!** Your theory won this week's Conspiracy of the Week!\n\n*\"{winner_sub['theory']}\"*\n\nCheck {channel.mention} for the announcement!")
        except Exception:
            pass
    except Exception as e:
        print(f"⚠️ COTW winner error: {e}")

@bot.command(name="submit")
async def cotw_submit(ctx, *, theory: str = ""):
    """Submit a theory for Conspiracy of the Week."""
    if not theory or len(theory) < 10:
        await ctx.send("❌ Please provide a theory of at least 10 characters.\nUsage: `!submit The moon landing was staged by Kubrick`", delete_after=15)
        return
    if len(theory) > 300:
        await ctx.send("❌ Theory must be 300 characters or fewer.", delete_after=10)
        return
    week  = current_week()
    state = fetchone("SELECT phase FROM cotw_state WHERE guild_id = %s AND week = %s", (str(ctx.guild.id), week))
    if not state or state["phase"] != "submission":
        await ctx.send("❌ Submissions aren't open right now. They open **Monday at 8 PM ET**!", delete_after=15)
        return
    try:
        execute("INSERT INTO cotw_submissions (guild_id, user_id, username, theory, week) VALUES (%s,%s,%s,%s,%s)",
                (str(ctx.guild.id), str(ctx.author.id), str(ctx.author), theory, week))
        count = fetchone("SELECT COUNT(*) as c FROM cotw_submissions WHERE guild_id=%s AND week=%s", (str(ctx.guild.id), week))["c"]
        await ctx.send(f"✅ **Theory submitted!** 🕵️\n*\"{theory}\"*\n\nYou're entry #{count} this week. Voting opens Friday at 8 PM ET!", delete_after=30)
        try:
            await ctx.message.delete()
        except Exception:
            pass
    except Exception as e:
        if "unique" in str(e).lower():
            await ctx.send("⚠️ You've already submitted a theory this week! One per member.", delete_after=10)
        else:
            await ctx.send("❌ Something went wrong — please try again.", delete_after=10)

@bot.group(name="cotw", invoke_without_command=True)
@check_manager()
async def cotw_group(ctx):
    week  = current_week()
    state = fetchone("SELECT * FROM cotw_state WHERE guild_id = %s AND week = %s", (str(ctx.guild.id), week))
    count = fetchone("SELECT COUNT(*) as c FROM cotw_submissions WHERE guild_id=%s AND week=%s", (str(ctx.guild.id), week))
    embed = discord.Embed(title=f"🕵️ COTW Status — {week}", color=discord.Color.dark_gold())
    embed.add_field(name="Phase",       value=state["phase"] if state else "pending", inline=True)
    embed.add_field(name="Submissions", value=str(count["c"] if count else 0),        inline=True)
    embed.add_field(name="Commands",
                    value=("`!cotw open`    — Open submissions\n"
                           "`!cotw vote`    — Close submissions, open voting\n"
                           "`!cotw end`     — Announce winner now\n"
                           "`!cotw list`    — View all submissions this week\n"),
                    inline=False)
    await ctx.send(embed=embed)

@cotw_group.command(name="open")
@check_manager()
async def cotw_open_cmd(ctx):
    await cotw_open_submissions(str(ctx.guild.id), manual=True)
    await ctx.send("✅ Submissions opened.", delete_after=10)

@cotw_group.command(name="vote")
@check_manager()
async def cotw_vote_cmd(ctx):
    await cotw_open_voting(str(ctx.guild.id), manual=True)
    await ctx.send("✅ Voting phase started.", delete_after=10)

@cotw_group.command(name="end")
@check_manager()
async def cotw_end_cmd(ctx):
    await cotw_announce_winner(str(ctx.guild.id), manual=True)
    await ctx.send("✅ Winner announced.", delete_after=10)

@cotw_group.command(name="list")
@check_manager()
async def cotw_list_cmd(ctx):
    week  = current_week()
    subs  = fetchall("SELECT username, theory, submitted_at FROM cotw_submissions WHERE guild_id=%s AND week=%s ORDER BY submitted_at", (str(ctx.guild.id), week))
    if not subs: await ctx.send(f"📭 No submissions yet for week {week}."); return
    embed = discord.Embed(title=f"🕵️ Submissions — {week} ({len(subs)} total)", color=discord.Color.dark_gold())
    for i, s in enumerate(subs, 1):
        embed.add_field(name=f"{i}. {s['username'].split('#')[0]}", value=f"*\"{s['theory']}\"*", inline=False)
    await ctx.send(embed=embed)


# ═══════════════════════════════════════════════════════════════════════════════
# RATE MY THEORY
# ═══════════════════════════════════════════════════════════════════════════════

@bot.command(name="ratemy")
async def rate_my_theory(ctx, *, theory: str = ""):
    if len(theory) < 10:
        await ctx.send("❌ Give me something to work with — at least 10 characters.\nUsage: `!ratemy The moon landing was filmed by Kubrick`")
        return
    evidence     = _random.choice(EVIDENCE_COMMENTS)
    plausibility = _random.choice(PLAUSIBILITY_COMMENTS)
    tinfoil_idx  = min(4, (evidence[0] + plausibility[0]) // 4)
    tinfoil      = TINFOIL_LEVELS[tinfoil_idx]
    verdict      = _random.choice(THEORY_VERDICTS)

    def bar(score, mx=10):
        return "█" * round(score) + "░" * (mx - round(score))

    embed = discord.Embed(
        title="🕵️ Theory Assessment Report",
        description=f'*"{theory[:200]}{"..." if len(theory) > 200 else ""}"*',
        color=discord.Color.dark_green(),
    )
    embed.add_field(name=f"📁 Evidence Quality — {evidence[0]}/10",    value=f"`{bar(evidence[0])}`  {evidence[1]}",       inline=False)
    embed.add_field(name=f"🤔 Plausibility — {plausibility[0]}/10",    value=f"`{bar(plausibility[0])}`  {plausibility[1]}", inline=False)
    embed.add_field(name=f"🎩 Tinfoil Hat Level — {tinfoil[0]}/5",     value=tinfoil[1],                                    inline=False)
    embed.add_field(name="📋 Official Verdict",                         value=verdict,                                       inline=False)
    embed.set_footer(text=f"Assessment by the Bureau of Plausible Nonsense  •  Submitted by {ctx.author.display_name}")
    await ctx.send(embed=embed)


# ═══════════════════════════════════════════════════════════════════════════════
# RANDOM RABBIT HOLE
# ═══════════════════════════════════════════════════════════════════════════════

def build_rabbit_hole_embed(hole: dict) -> discord.Embed:
    embed = discord.Embed(
        title=f"🕳️ Today's Rabbit Hole: {hole['title']}",
        description=hole["summary"],
        color=discord.Color.dark_red(),
    )
    if hole.get("tags"):
        embed.add_field(name="Tags", value="  ".join(f"`{t}`" for t in hole["tags"]), inline=False)
    embed.set_footer(text="A thread has been opened below for discussion. How deep does this go? 🐇")
    return embed

async def post_rabbit_hole(channel_override=None):
    channel = channel_override or bot.get_channel(RABBIT_HOLE_CHANNEL_ID)
    if not channel: return
    hole  = _random.choice(RABBIT_HOLES)
    embed = build_rabbit_hole_embed(hole)
    msg   = await channel.send(embed=embed)
    try:
        await msg.create_thread(name=f"🕳️ {hole['title']}", auto_archive_duration=1440)
    except Exception as e:
        print(f"⚠️ Could not create rabbit hole thread: {e}")

@bot.command(name="rabbithole")
@check_manager()
async def rabbithole_cmd(ctx):
    """Post a random rabbit hole on demand."""
    await post_rabbit_hole(ctx.channel)
    try:
        await ctx.message.delete()
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# CIPHER OF THE DAY
# ═══════════════════════════════════════════════════════════════════════════════

def build_cipher_embed(cipher: dict) -> discord.Embed:
    embed = discord.Embed(
        title=f"🔐 Cipher of the Day: {cipher['title']}",
        color=discord.Color.teal(),
    )
    embed.add_field(name="📟 Encoded Message", value=f"```{cipher['encoded']}```",                     inline=False)
    embed.add_field(name="💡 Hint",            value=f"||{cipher['hint']}||",                          inline=False)
    embed.add_field(name="✅ Solution",        value=f"||{cipher['solution']}||",                      inline=True)
    embed.add_field(name="📖 About",           value=cipher["explanation"],                             inline=False)
    embed.set_footer(text="Click the Solution field to reveal the answer. Can you crack it first? 🔍")
    return embed

async def post_cipher(channel_override=None):
    channel = channel_override or bot.get_channel(CIPHER_CHANNEL_ID)
    if not channel: return
    cipher = _random.choice(CIPHERS)
    await channel.send(embed=build_cipher_embed(cipher))

@bot.command(name="cipher")
@check_manager()
async def cipher_cmd(ctx):
    """Post a random cipher on demand."""
    await post_cipher(ctx.channel)
    try:
        await ctx.message.delete()
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# KEYWORD ALERTS
# ═══════════════════════════════════════════════════════════════════════════════

@bot.group(name="keyword", aliases=["kw"], invoke_without_command=True)
@check_manager()
async def keyword_group(ctx):
    embed = discord.Embed(title="🚨 Keyword Alert Commands", color=discord.Color.red())
    embed.add_field(name="Commands", value=(
        "`!keyword add <word>`    — Add a keyword to watch\n"
        "`!keyword remove <word>` — Remove a keyword\n"
        "`!keyword list`          — Show all active keywords\n"), inline=False)
    await ctx.send(embed=embed)

@keyword_group.command(name="add")
@check_manager()
async def keyword_add(ctx, *, keyword: str = ""):
    if not keyword:
        await ctx.send("❌ Usage: `!keyword add <word or phrase>`"); return
    keyword = keyword.lower().strip()
    try:
        execute("INSERT INTO keyword_alerts (guild_id, keyword, added_by) VALUES (%s,%s,%s)",
                (str(ctx.guild.id), keyword, str(ctx.author)))
        invalidate_kw_cache(str(ctx.guild.id))
        await ctx.send(f"✅ Keyword `{keyword}` added. The bot will now alert this channel whenever it appears.", delete_after=15)
    except Exception as e:
        if "unique" in str(e).lower():
            await ctx.send(f"⚠️ `{keyword}` is already being monitored.", delete_after=10)
        else:
            await ctx.send(f"❌ Error: {e}", delete_after=10)

@keyword_group.command(name="remove")
@check_manager()
async def keyword_remove(ctx, *, keyword: str = ""):
    if not keyword:
        await ctx.send("❌ Usage: `!keyword remove <word or phrase>`"); return
    keyword = keyword.lower().strip()
    execute("DELETE FROM keyword_alerts WHERE guild_id = %s AND keyword = %s", (str(ctx.guild.id), keyword))
    invalidate_kw_cache(str(ctx.guild.id))
    await ctx.send(f"✅ Keyword `{keyword}` removed.", delete_after=10)

@keyword_group.command(name="list")
@check_manager()
async def keyword_list(ctx):
    rows = fetchall("SELECT keyword, added_by, added_at FROM keyword_alerts WHERE guild_id = %s ORDER BY added_at", (str(ctx.guild.id),))
    if not rows:
        await ctx.send("📭 No keywords configured. Add one with `!keyword add <word>`."); return
    embed = discord.Embed(title=f"🚨 Monitored Keywords ({len(rows)})", color=discord.Color.red())
    lines = [f"`{r['keyword']}`  — added by {r['added_by'].split('#')[0]}" for r in rows]
    embed.description = "\n".join(lines)
    embed.set_footer(text=f"Alerts post to <#{KEYWORD_ALERT_CHANNEL_ID}>")
    await ctx.send(embed=embed)


# ═══════════════════════════════════════════════════════════════════════════════
# /message SLASH COMMAND
# ═══════════════════════════════════════════════════════════════════════════════

@bot.tree.command(name="message", description="Send a message as the bot to a specified channel.")
@discord.app_commands.describe(channel="The channel to send the message in", text="The message to send")
async def slash_message(interaction: discord.Interaction, channel: discord.TextChannel, text: str):
    if not member_can_manage(interaction.user):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True); return
    try:
        await channel.send(text)
        await interaction.response.send_message(f"✅ Message sent to {channel.mention}.", ephemeral=True)
    except discord.Forbidden:
        await interaction.response.send_message(f"❌ I don't have permission to post in {channel.mention}.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"❌ Failed: {e}", ephemeral=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PODCAST RSS FEED CHECKER — every 60 minutes
# ═══════════════════════════════════════════════════════════════════════════════

def get_latest_rss_episode(feed_url: str) -> dict | None:
    """Fetch the RSS feed and return the latest episode as a dict, or None on error."""
    try:
        import feedparser
        feed = feedparser.parse(feed_url)
        if not feed.entries:
            return None
        entry = feed.entries[0]
        # Duration — try itunes:duration first, fallback to enclosure length
        duration = ""
        if hasattr(entry, "itunes_duration"):
            raw = entry.itunes_duration
            # Convert seconds integer to h:mm:ss if needed
            if raw.isdigit():
                secs = int(raw)
                h, m, s = secs // 3600, (secs % 3600) // 60, secs % 60
                duration = f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"
            else:
                duration = raw
        # Episode number
        ep_num = getattr(entry, "itunes_episode", None)
        # Image — episode image first, fallback to feed image
        image_url = None
        if hasattr(entry, "image") and hasattr(entry.image, "href"):
            image_url = entry.image.href
        elif feed.feed.get("image"):
            image_url = feed.feed.image.get("href")
        # Listen link
        link = entry.get("link") or feed.feed.get("link", "")
        return {
            "guid":        entry.get("id") or entry.get("link") or entry.title,
            "title":       entry.title,
            "summary":     entry.get("summary", ""),
            "duration":    duration,
            "ep_num":      ep_num,
            "image_url":   image_url,
            "link":        link,
            "podcast_name": feed.feed.get("title", "The Conspiracy Podcast"),
        }
    except Exception as e:
        print(f"⚠️ RSS fetch error: {e}")
        return None


def rss_episode_already_posted(guid: str) -> bool:
    """Return True if we've already announced this episode GUID."""
    try:
        execute("""
            CREATE TABLE IF NOT EXISTS rss_posted (
                guid       TEXT PRIMARY KEY,
                posted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        execute("INSERT INTO rss_posted (guid) VALUES (%s)", (guid,))
        return False   # Successfully inserted — new episode
    except Exception:
        return True    # Unique violation — already posted


@tasks.loop(minutes=60)
async def podcast_rss_checker():
    """Check the podcast RSS feed every 60 minutes and announce new episodes."""
    episode = get_latest_rss_episode(PODCAST_RSS_URL)
    if not episode:
        return
    if rss_episode_already_posted(episode["guid"]):
        return

    channel = bot.get_channel(PODCAST_CHANNEL_ID)
    if not channel:
        print(f"⚠️ RSS: Could not find podcast channel {PODCAST_CHANNEL_ID}")
        return

    # Build embed
    embed = discord.Embed(
        title=episode["title"],
        url=episode["link"] or None,
        color=discord.Color.from_rgb(30, 215, 96),  # Spotify green
    )
    embed.set_author(name=f"🎙️ New Episode — {episode['podcast_name']}")

    # Trim summary to 300 chars
    summary = episode.get("summary", "").strip()
    if summary:
        # Strip basic HTML tags that sometimes appear in RSS summaries
        summary = _re.sub(r"<[^>]+>", "", summary)
        embed.description = summary[:300] + ("…" if len(summary) > 300 else "")

    if episode["duration"]:
        embed.add_field(name="⏱️ Duration", value=episode["duration"], inline=True)
    if episode["ep_num"]:
        embed.add_field(name="🎧 Episode",  value=f"#{episode['ep_num']}", inline=True)
    if episode["link"]:
        embed.add_field(name="🔗 Listen",   value=f"[Click here to listen]({episode['link']})", inline=False)

    if episode["image_url"]:
        embed.set_thumbnail(url=episode["image_url"])

    embed.set_footer(text="New episode just dropped! 🕵️  Don't forget to subscribe & leave a review.")

    role_mention = f"<@&{PODCAST_LISTENER_ROLE}>"
    await channel.send(content=role_mention, embed=embed)
    print(f"📡 RSS: Announced new episode — {episode['title']}")


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEDULER — 8 PM EASTERN, every day
# ═══════════════════════════════════════════════════════════════════════════════

@tasks.loop(minutes=1)
async def community_scheduler():
    now = datetime.datetime.now(EASTERN)
    if now.hour != POST_HOUR or now.minute != 0:
        return

    weekday = now.weekday()   # 0=Mon, 6=Sun
    today   = now.strftime("%A %d %b")
    print(f"⏰ Scheduler firing — {today} {now.strftime('%H:%M %Z')}")

    # Daily posts
    if not task_already_fired("rabbit_hole"):
        print("🐇 Posting daily rabbit hole…")
        await post_rabbit_hole()

    if not task_already_fired("cipher"):
        print("🔐 Posting daily cipher…")
        await post_cipher()

    # Weekly COTW phases
    gid = str(GUILD_ID)
    if weekday == 0 and not task_already_fired("cotw_open"):        # Monday
        print("🕵️ Opening COTW submissions…")
        await cotw_open_submissions(gid)

    elif weekday == 4 and not task_already_fired("cotw_vote"):      # Friday
        print("🗳️ Opening COTW voting…")
        await cotw_open_voting(gid)

    elif weekday == 6 and not task_already_fired("cotw_winner"):    # Sunday
        print("🏆 Announcing COTW winner…")
        await cotw_announce_winner(gid)


# ═══════════════════════════════════════════════════════════════════════════════
# BOT EVENTS
# ═══════════════════════════════════════════════════════════════════════════════

@bot.event
async def on_ready():
    init_db()
    print(f"✅  Logged in as {bot.user}")
    print(f"📊  Dashboard → http://localhost:{FLASK_PORT}")
    print(f"🎉  Giveaway manager role IDs: {GIVEAWAY_MANAGER_ROLE_IDS}")
    print(f"🎟️  Giveaway entry role ID:    {GIVEAWAY_ENTRY_ROLE_ID}")

    # Sync slash commands instantly to guild
    try:
        guild  = discord.Object(id=GUILD_ID)
        bot.tree.copy_global_to(guild=guild)
        synced = await bot.tree.sync(guild=guild)
        print(f"⚡  Synced {len(synced)} slash command(s) to guild")
    except Exception as e:
        print(f"⚠️  Could not sync slash commands: {e}")

    # Restore active giveaways
    try:
        active = fetchall("SELECT id FROM giveaways WHERE ended = FALSE AND ends_at > NOW()")
        for gw in active:
            bot.add_view(GiveawayView(gw["id"]))
            asyncio.ensure_future(giveaway_countdown(gw["id"]))
        if active:
            print(f"🔄  Restored {len(active)} active giveaway(s)")
    except Exception as e:
        print(f"⚠️  Could not restore giveaways: {e}")

    # Start scheduler
    if not community_scheduler.is_running():
        community_scheduler.start()
        print(f"⏰  Scheduler started — daily posts at {POST_HOUR}:00 ET")

    # Start RSS checker
    if not podcast_rss_checker.is_running():
        podcast_rss_checker.start()
        print(f"📡  RSS checker started — checking every 60 minutes")


@bot.event
async def on_message(message):
    if message.author.bot or not message.guild:
        return

    now         = datetime.datetime.utcnow()
    hour        = now.hour
    day_of_week = now.weekday()
    avatar      = str(message.author.display_avatar.url) if message.author.display_avatar else ""

    # Activity tracking
    try:
        execute("INSERT INTO messages (user_id,username,channel_id,channel_name,guild_id,hour,day_of_week,timestamp) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (str(message.author.id), str(message.author), str(message.channel.id),
                 message.channel.name, str(message.guild.id), hour, day_of_week, now))
        execute("INSERT INTO user_stats (user_id,username,guild_id,msg_count,last_seen,first_seen,avatar_url) VALUES (%s,%s,%s,1,%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET username=EXCLUDED.username, msg_count=user_stats.msg_count+1, last_seen=GREATEST(user_stats.last_seen,EXCLUDED.last_seen), avatar_url=EXCLUDED.avatar_url",
                (str(message.author.id), str(message.author), str(message.guild.id), now, now, avatar))
    except Exception as e:
        print(f"DB error: {e}")

    # Keyword alerts
    try:
        keywords = get_cached_keywords(str(message.guild.id))
        if keywords:
            content_lower = message.content.lower()
            matched = [kw for kw in keywords if kw in content_lower]
            if matched:
                alert_channel = bot.get_channel(KEYWORD_ALERT_CHANNEL_ID)
                if alert_channel:
                    embed = discord.Embed(title="🚨 Keyword Alert", color=discord.Color.red(),
                                          timestamp=datetime.datetime.utcnow())
                    embed.set_author(name=str(message.author),
                                     icon_url=str(message.author.display_avatar.url) if message.author.display_avatar else discord.Embed.Empty)
                    embed.add_field(name="Keywords Matched", value=", ".join(f"`{k}`" for k in matched), inline=False)
                    embed.add_field(name="Message",  value=message.content[:1000] or "*(empty)*", inline=False)
                    embed.add_field(name="Channel",  value=message.channel.mention, inline=True)
                    embed.add_field(name="Jump To",  value=f"[Click here]({message.jump_url})",   inline=True)
                    await alert_channel.send(embed=embed)
    except Exception as e:
        print(f"Keyword alert error: {e}")

    await bot.process_commands(message)


@bot.command(name="backfill")
@commands.has_permissions(administrator=True)
async def backfill(ctx, days: int = 30):
    if days < 1 or days > 365:
        await ctx.send("❌ Please specify between 1 and 365 days."); return
    cutoff     = datetime.datetime.utcnow() - datetime.timedelta(days=days)
    status_msg = await ctx.send(f"🔍 Starting backfill for the last **{days} days**...")
    total_msgs = total_chans = skipped = 0
    for channel in ctx.guild.text_channels:
        if not channel.permissions_for(ctx.guild.me).read_message_history:
            skipped += 1; continue
        chan_count = 0
        try:
            async for message in channel.history(limit=None, after=cutoff, oldest_first=True):
                if message.author.bot: continue
                ts = message.created_at.replace(tzinfo=None)
                av = str(message.author.display_avatar.url) if message.author.display_avatar else ""
                try:
                    execute("INSERT INTO messages (user_id,username,channel_id,channel_name,guild_id,hour,day_of_week,timestamp) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                            (str(message.author.id), str(message.author), str(channel.id), channel.name, str(ctx.guild.id), ts.hour, ts.weekday(), ts))
                    execute("INSERT INTO user_stats (user_id,username,guild_id,msg_count,last_seen,first_seen,avatar_url) VALUES (%s,%s,%s,1,%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET username=EXCLUDED.username, msg_count=user_stats.msg_count+1, last_seen=GREATEST(user_stats.last_seen,EXCLUDED.last_seen), first_seen=LEAST(user_stats.first_seen,EXCLUDED.first_seen), avatar_url=EXCLUDED.avatar_url",
                            (str(message.author.id), str(message.author), str(ctx.guild.id), ts, ts, av))
                    chan_count += 1; total_msgs += 1
                except Exception as e:
                    print(f"Backfill DB error: {e}")
            if chan_count > 0: total_chans += 1
        except discord.Forbidden:
            skipped += 1
        except Exception as e:
            print(f"Backfill channel error ({channel.name}): {e}")
    await status_msg.edit(content=f"✅ **Backfill complete!**\n📨 **{total_msgs:,}** messages · 📣 **{total_chans}** channels · 🔒 **{skipped}** skipped\nRefresh your dashboard!")


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    flask_thread = threading.Thread(
        target=lambda: api.run(host="0.0.0.0", port=FLASK_PORT, debug=False, use_reloader=False),
        daemon=True,
    )
    flask_thread.start()
    bot.run(BOT_TOKEN)
