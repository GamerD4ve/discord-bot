"""
Discord Bot — Activity Tracker + Giveaways + Community Features
===============================================================
All commands are slash commands.
Requirements:
    pip install discord.py flask flask-cors psycopg2-binary tzdata feedparser
"""

import discord
from discord import app_commands
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
from typing import Optional
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

GIVEAWAY_MANAGER_ROLE_IDS = {1166453168121581579, 1166454283630301375}
GIVEAWAY_ENTRY_ROLE_ID    = 1166481664176828496

GUILD_ID                 = 1127292710290735134
COTW_CHANNEL_ID          = 1486465944300818624
RABBIT_HOLE_CHANNEL_ID   = 1486466347821957252
CIPHER_CHANNEL_ID        = 1486466469750378566
KEYWORD_ALERT_CHANNEL_ID = 1486468826739642468

JORGE_USER_ID            = 488070524191375361
JORGE_WATCH_CHANNEL      = 1486488345826820236
JORGE_EXCLUDE_CHANNEL    = 1166458975341006908
JORGE_PING_COOLDOWN_HOURS = 12

PODCAST_RSS_URL          = "https://anchor.fm/s/c0340eac/podcast/rss"
PODCAST_CHANNEL_ID       = 1127292711012139062
PODCAST_LISTENER_ROLE    = 1166481664176828496

EASTERN   = ZoneInfo("America/New_York")
POST_HOUR = 20
# ─────────────────────────────────────────────────────────────────────────────


# ═══════════════════════════════════════════════════════════════════════════════
# EDITABLE CONTENT LISTS
# ═══════════════════════════════════════════════════════════════════════════════

RABBIT_HOLES = [
    {"title":"The Philadelphia Experiment","summary":"In October 1943, the USS Eldridge was allegedly rendered invisible to radar and physically teleported from Philadelphia to Norfolk, Virginia and back. Crew members reportedly went insane, some fused into the ship's hull. The US Navy denies it.","tags":["military","teleportation","WWII"]},
    {"title":"The Phantom Time Hypothesis","summary":"German historian Heribert Illig proposed that 297 years of history — 614 to 911 AD — were entirely fabricated. He claims Charlemagne and others conspired to place themselves at year 1000 AD. If true, it is currently the year 1726.","tags":["history","time","fabrication"]},
    {"title":"Project MKUltra","summary":"Declassified CIA documents confirm that from 1953–1973, the CIA ran covert experiments using LSD, hypnosis, and psychological torture on unwitting subjects. This one isn't a theory. It actually happened.","tags":["CIA","mind control","declassified","confirmed"]},
    {"title":"The Tartaria Mud Flood","summary":"A theory holds that a vast empire called Tartaria was erased from history, and that a 'mud flood' buried entire cities in the 1800s. Proponents point to buildings with ground-floor windows below street level as evidence.","tags":["history","lost civilisation","architecture"]},
    {"title":"The Dyatlov Pass Incident","summary":"In February 1959, nine experienced Soviet hikers died under bizarre circumstances. The tent was ripped from the inside. They fled in -30°C without shoes. One had a fractured skull, another was missing her tongue. Official cause: 'unknown compelling force.'","tags":["USSR","unexplained","death","mountains"]},
    {"title":"The Denver Airport","summary":"Denver International Airport contains murals depicting gas-masked soldiers and burning cities. A capstone refers to the 'New World Airport Commission' — an organisation that doesn't exist. Built 16 months late and $2 billion over budget.","tags":["NWO","airports","symbolism","underground"]},
    {"title":"Operation Northwoods","summary":"Declassified in 1997: in 1962, the US Joint Chiefs proposed staging fake terrorist attacks on US soil and blaming Cuba. Proposals included sinking a ship and bombing Miami. JFK rejected it.","tags":["false flag","Cuba","declassified","confirmed"]},
    {"title":"The Simulation Hypothesis","summary":"Philosopher Nick Bostrom argued at least one of three things must be true: civilisations go extinct early; advanced civilisations lose interest in simulations; or we are almost certainly living in a simulation.","tags":["reality","philosophy","technology"]},
    {"title":"The Dead Internet Theory","summary":"A theory claiming the internet has been mostly populated by bots and automated content since around 2016–2017. Real human interaction online is now the minority. The goal: manufacture consensus.","tags":["internet","AI","bots","social media"]},
    {"title":"The Voynich Manuscript","summary":"A 240-page illustrated book written in an unknown script that no cryptographer, linguist, or AI has been able to decode. Carbon-dated to the 15th century. Contains diagrams of unknown plants and naked women in green tubes.","tags":["cryptography","mystery","history"]},
    {"title":"The Mandela Effect","summary":"Millions of people share identical false memories — the Berenstain/Berenstein Bears, Nelson Mandela dying in prison in the 1980s. Some attribute this to parallel universes bleeding together, others to CERN's LHC.","tags":["memory","parallel universes","CERN"]},
    {"title":"The Stargate Project","summary":"From 1978 to 1995, the US government spent $20 million training psychic spies to remotely view Soviet military installations. The CIA concluded it had 'limited intelligence value.' They did not say it didn't work.","tags":["CIA","psychic","remote viewing","declassified"]},
    {"title":"The Georgia Guidestones","summary":"Erected in 1980 by an unknown person, the Georgia Guidestones were granite slabs inscribed with ten commandments including maintaining world population under 500 million. Destroyed by explosion in 2022. Bomber never found.","tags":["NWO","population control","mystery"]},
    {"title":"The Black Knight Satellite","summary":"An unidentified object in polar orbit has been photographed by NASA since the 1960s. Allegedly 13,000 years old. NASA claims it's a thermal blanket.","tags":["space","alien","satellites","NASA"]},
    {"title":"Operation Paperclip","summary":"Confirmed: after WWII, the US recruited over 1,600 Nazi scientists and gave them new identities and government jobs. Werner von Braun, a former SS officer, became the head of NASA.","tags":["NASA","Nazi","confirmed","declassified"]},
    {"title":"The Tavistock Institute","summary":"Founded in London in 1947, the Tavistock Institute is accused of orchestrating the 1960s counterculture movement — including the Beatles — to destabilise society and manufacture state dependency.","tags":["mind control","UK","social engineering","Beatles"]},
    {"title":"The Hollow Earth Theory","summary":"Edmond Halley proposed in 1692 that Earth is hollow with a small interior sun. Admiral Byrd's 1947 Arctic expedition diary allegedly describes flying into a hole at the pole and discovering a tropical land with mammoths.","tags":["earth","inner world","exploration"]},
    {"title":"The Montauk Project","summary":"Allegedly a classified programme run at Montauk Air Force Station involving time travel and inter-dimensional portals. The facility is real. The underground bunkers are real.","tags":["time travel","military","mind control","underground"]},
    {"title":"The Jonestown Massacre","summary":"On November 18, 1978, 918 members of the Peoples Temple died in Guyana. Autopsy reports show injection marks on many bodies. Jim Jones had documented CIA connections.","tags":["CIA","cults","mass death","cover-up"]},
    {"title":"The Bielefeld Conspiracy","summary":"Since 1994, a theory claims the city of Bielefeld doesn't exist. In 2019, the city offered €1 million to anyone who could prove it doesn't exist. Nobody claimed the prize.","tags":["cities","Germany","existence"]},
]

CIPHERS = [
    {"title":"Caesar Cipher — Shift 3","encoded":"WKH WUXWK LV RXW WKHUH","hint":"Shift each letter back by 3.","solution":"THE TRUTH IS OUT THERE","explanation":"The classic Caesar cipher — used by Julius Caesar himself."},
    {"title":"Atbash Cipher","encoded":"GSV TLEWH ZIV DZGXSRMT","hint":"Mirror the alphabet: A=Z, B=Y, C=X...","solution":"THE GODS ARE WATCHING","explanation":"The Atbash cipher appears in the Hebrew Bible."},
    {"title":"ROT13","encoded":"GURL YVIR NZBAT HF","hint":"Rotate each letter by 13 positions.","solution":"THEY LIVE AMONG US","explanation":"ROT13 is its own inverse."},
    {"title":"Morse Code","encoded":"-- --- -. . -.-- / .. ... / -.-. --- -. - .-. --- .-.","hint":"Dots and dashes. Slash separates words.","solution":"MONEY IS CONTROL","explanation":"Invented by Samuel Morse in 1837."},
    {"title":"Binary — ASCII","encoded":"01010111 01000001 01001011 01000101 / 01010101 01010000","hint":"Convert each 8-bit group from binary to ASCII.","solution":"WAKE UP","explanation":"Everything in a computer is ones and zeros."},
    {"title":"Reverse Cipher","encoded":"DLROW EHT LORTNOC YEHT","hint":"Read it backwards.","solution":"THEY CONTROL THE WORLD","explanation":"The simplest cipher — reverse the entire string."},
    {"title":"Caesar Cipher — Shift 7","encoded":"AOL JSVJR PZ H SPL","hint":"Shift each letter back by 7.","solution":"THE CLOCK IS A LIE","explanation":"A deeper Caesar shift."},
    {"title":"Atbash Cipher II","encoded":"WLMG GIFHG GSV NVWRZ","hint":"Mirror the alphabet: A=Z, B=Y, C=X...","solution":"DONT TRUST THE MEDIA","explanation":"Once you know the mirror, no message stays secret."},
    {"title":"NATO Phonetic Acrostic","encoded":"ECHO VICTOR ECHO ROMEO YANKEE TANGO HOTEL INDIA NOVEMBER GOLF","hint":"Take the first letter of each NATO word.","solution":"EVERYTHING","explanation":"The NATO alphabet doubles as a clean acrostic cipher."},
    {"title":"Caesar Cipher — Shift 17","encoded":"ESPCP TD EFOOLZ MPY MPSVD","hint":"Shift each letter back by 17.","solution":"THERE IS TUNNEL BELOW","explanation":"A deep Caesar shift."},
    {"title":"Hexadecimal — ASCII","encoded":"4E 6F 74 68 69 6E 67 20 49 73 20 52 65 61 6C","hint":"Convert each hex pair to its ASCII character.","solution":"Nothing Is Real","explanation":"Hexadecimal is base-16."},
    {"title":"Caesar + Reverse","encoded":"FBIYL ZNBRF FU LHQOBF","hint":"First reverse the string, then apply ROT13.","solution":"FOLLOW THE MONEY","explanation":"Layered ciphers."},
    {"title":"Caesar Cipher — Shift 5","encoded":"YMJD FSJ QNXYJS","hint":"Shift each letter back by 5.","solution":"THEY ARE LISTEN","explanation":"Simple, fast, effective."},
    {"title":"Pig Latin Variant","encoded":"ETHAY UTHTRUSTAY ILLWAY EEFRAY OUYAY","hint":"Move the first consonant cluster to the end and add -AY.","solution":"THE TRUTH WILL FREE YOU","explanation":"Pig Latin variants used as verbal obfuscation."},
    {"title":"Atbash Cipher III","encoded":"GSV HFMWRZO RHOZMWH ZIV IVZO","hint":"Mirror the alphabet: A=Z, B=Y, C=X...","solution":"THE SUNDIAL ISLANDS ARE REAL","explanation":"Frequency analysis is your friend."},
]

EVIDENCE_COMMENTS   = [(1,"Basically vibes"),(2,"A Reddit post from 2009"),(2,"One grainy YouTube video"),(3,"Your cousin told you"),(3,"An anonymous forum post"),(4,"A deleted Wikipedia edit"),(4,"Three blurry photographs"),(5,"A dream you had"),(5,"'They' don't want you to know"),(6,"A documentary your friend sent"),(6,"Declassified-but-misread documents"),(7,"Pattern-matched across 12 open tabs"),(8,"A surprisingly well-sourced thread"),(9,"Actual primary sources (suspicious)")]
PLAUSIBILITY_COMMENTS=[(1,"Physics would like a word"),(2,"Only if gravity is fake too"),(3,"Deep in tinfoil territory"),(4,"Possible on a slow Tuesday"),(5,"Could happen honestly"),(6,"Surprisingly reasonable"),(7,"Actually kind of compelling"),(8,"Uncomfortably plausible"),(9,"We'd be shocked if this wasn't true")]
TINFOIL_LEVELS      = [(1,"🧢 Bare-headed. Zero protection whatsoever."),(2,"🎩 Light coverage. Amateur hour."),(3,"🎩🎩 Double-hatted. Respectable."),(4,"🎩🎩🎩 Triple threat. They've noticed you."),(5,"🎩🎩🎩🎩 Full cranial fortress. Stay safe out there.")]
THEORY_VERDICTS     = ["Your sources are 'trust me bro' but your energy is immaculate. 🕵️","The government is shaking right now. Probably.","We'd rate this higher but the black helicopters keep circling our servers.","Compelling. Unhinged. We love it.","This theory has been filed with the Bureau of Plausible Nonsense.","5/5 would lose sleep over this. Thank you for your service.","The lizard people give this a 4/10. Make of that what you will.","Bold. Brave. Possibly monitored. Carry on.","We cannot confirm or deny. Which, as you know, means yes.","This is either genius or a cry for help. Either way, we're in.","Somewhere a CIA analyst just choked on their coffee.","The Illuminati would like you to stop. Immediately.","Your tinfoil is showing. Don't change.","This theory has legs. We recommend running.","Approved by the Shadow Council. Temporarily."]
NUMBER_EMOJIS       = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]

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
                    id SERIAL PRIMARY KEY, user_id TEXT NOT NULL, username TEXT NOT NULL,
                    channel_id TEXT NOT NULL, channel_name TEXT NOT NULL, guild_id TEXT NOT NULL,
                    hour INTEGER NOT NULL, day_of_week INTEGER NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(user_id, channel_id, timestamp)
                );
                CREATE TABLE IF NOT EXISTS user_stats (
                    user_id TEXT PRIMARY KEY, username TEXT NOT NULL, guild_id TEXT NOT NULL,
                    msg_count INTEGER DEFAULT 0, last_seen TIMESTAMPTZ, first_seen TIMESTAMPTZ, avatar_url TEXT
                );
                CREATE TABLE IF NOT EXISTS orders (
                    id SERIAL PRIMARY KEY, order_id TEXT UNIQUE, event_type TEXT NOT NULL,
                    buyer_name TEXT, buyer_email TEXT, product_name TEXT,
                    total_amount NUMERIC(10,2), currency TEXT DEFAULT 'USD', status TEXT,
                    raw JSONB, timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS giveaways (
                    id SERIAL PRIMARY KEY, guild_id TEXT NOT NULL, channel_id TEXT NOT NULL,
                    message_id TEXT, prize TEXT NOT NULL, description TEXT, host_id TEXT NOT NULL,
                    winner_mode TEXT NOT NULL DEFAULT 'announce', winner_count INTEGER NOT NULL DEFAULT 1,
                    ends_at TIMESTAMPTZ NOT NULL, ended BOOLEAN DEFAULT FALSE,
                    winner_ids TEXT[] DEFAULT '{}', created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS giveaway_entries (
                    id SERIAL PRIMARY KEY, giveaway_id INTEGER NOT NULL REFERENCES giveaways(id) ON DELETE CASCADE,
                    user_id TEXT NOT NULL, username TEXT NOT NULL, avatar_url TEXT,
                    entered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(giveaway_id, user_id)
                );
                CREATE TABLE IF NOT EXISTS polls (
                    id SERIAL PRIMARY KEY, guild_id TEXT NOT NULL, channel_id TEXT NOT NULL,
                    message_id TEXT UNIQUE, question TEXT NOT NULL, options JSONB NOT NULL,
                    created_by TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    closed BOOLEAN DEFAULT FALSE
                );
                CREATE TABLE IF NOT EXISTS cotw_submissions (
                    id SERIAL PRIMARY KEY, guild_id TEXT NOT NULL, user_id TEXT NOT NULL,
                    username TEXT NOT NULL, theory TEXT NOT NULL, week TEXT NOT NULL,
                    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(guild_id, user_id, week)
                );
                CREATE TABLE IF NOT EXISTS cotw_state (
                    guild_id TEXT NOT NULL, week TEXT NOT NULL, phase TEXT DEFAULT 'pending',
                    vote_msg_id TEXT, PRIMARY KEY (guild_id, week)
                );
                CREATE TABLE IF NOT EXISTS cotw_winners (
                    id SERIAL PRIMARY KEY, guild_id TEXT NOT NULL, week TEXT NOT NULL,
                    winner_user_id TEXT NOT NULL, winner_username TEXT NOT NULL,
                    theory TEXT NOT NULL, vote_count INTEGER NOT NULL DEFAULT 0,
                    total_entries INTEGER NOT NULL DEFAULT 0,
                    announced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(guild_id, week)
                );
                CREATE TABLE IF NOT EXISTS keyword_alerts (
                    id SERIAL PRIMARY KEY, guild_id TEXT NOT NULL, keyword TEXT NOT NULL,
                    added_by TEXT NOT NULL, added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(guild_id, keyword)
                );
                CREATE TABLE IF NOT EXISTS jorge_mentions (
                    id SERIAL PRIMARY KEY, channel_id TEXT NOT NULL, message_id TEXT NOT NULL UNIQUE,
                    author_id TEXT NOT NULL, author_name TEXT NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS jorge_pings (
                    id SERIAL PRIMARY KEY, user_id TEXT NOT NULL, username TEXT NOT NULL,
                    avatar_url TEXT, pinged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS jorge_panel (
                    id SERIAL PRIMARY KEY, message_id TEXT NOT NULL, channel_id TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS rabbit_hole_panel (
                    id SERIAL PRIMARY KEY, message_id TEXT NOT NULL, channel_id TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS cipher_panel (
                    id SERIAL PRIMARY KEY, message_id TEXT NOT NULL, channel_id TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS daily_tasks_fired (
                    task_name TEXT NOT NULL, fired_date DATE NOT NULL,
                    PRIMARY KEY (task_name, fired_date)
                );
                CREATE TABLE IF NOT EXISTS rss_posted (
                    guid TEXT PRIMARY KEY, posted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
                CREATE INDEX IF NOT EXISTS idx_polls_guild      ON polls(guild_id);
                CREATE INDEX IF NOT EXISTS idx_cotw_week        ON cotw_submissions(guild_id, week);
                CREATE INDEX IF NOT EXISTS idx_cotw_winners_week ON cotw_winners(guild_id, week);
                CREATE INDEX IF NOT EXISTS idx_keywords         ON keyword_alerts(guild_id);
                CREATE INDEX IF NOT EXISTS idx_jorge_ts         ON jorge_mentions(timestamp);
                CREATE INDEX IF NOT EXISTS idx_jorge_pings      ON jorge_pings(user_id, pinged_at);
            """)

def fetchall(q, p=()):
    with get_db() as db:
        with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(q, p); return cur.fetchall()

def fetchone(q, p=()):
    with get_db() as db:
        with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(q, p); return cur.fetchone()

def execute(q, p=()):
    with get_db() as db:
        with db.cursor() as cur:
            cur.execute(q, p)

def task_already_fired(task_name):
    today = datetime.date.today().isoformat()
    try:
        execute("INSERT INTO daily_tasks_fired (task_name, fired_date) VALUES (%s,%s)", (task_name, today))
        return False
    except Exception:
        return True

def current_week():
    iso = datetime.date.today().isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"

_kw_cache, _kw_cache_ts = {}, {}
def get_cached_keywords(guild_id):
    now = datetime.datetime.utcnow()
    last = _kw_cache_ts.get(guild_id)
    if last is None or (now - last).total_seconds() > 300:
        rows = fetchall("SELECT keyword FROM keyword_alerts WHERE guild_id = %s", (guild_id,))
        _kw_cache[guild_id] = [r["keyword"].lower() for r in rows]
        _kw_cache_ts[guild_id] = now
    return _kw_cache.get(guild_id, [])

def invalidate_kw_cache(guild_id):
    _kw_cache.pop(guild_id, None); _kw_cache_ts.pop(guild_id, None)

# ─── STATIC ──────────────────────────────────────────────────────────────────
@api.route("/")
def index(): return send_from_directory(DASHBOARD, "dashboard.html")
@api.route("/logo.png")
def logo(): return send_from_directory(DASHBOARD, "logo.png")

# ─── DASHBOARD API ROUTES ─────────────────────────────────────────────────────
@api.route("/api/overview")
def overview():
    try:
        msgs  = fetchone("SELECT COUNT(*) as c FROM messages")["c"]
        users = fetchone("SELECT COUNT(*) as c FROM user_stats")["c"]
        chans = fetchone("SELECT COUNT(DISTINCT channel_id) as c FROM messages")["c"]
        day   = fetchone("SELECT DATE(timestamp) as day, COUNT(*) as cnt FROM messages GROUP BY day ORDER BY cnt DESC LIMIT 1")
        return jsonify({"total_messages":msgs,"total_users":users,"total_channels":chans,
                        "most_active_day":str(day["day"]) if day else None,
                        "most_active_day_count":day["cnt"] if day else 0})
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/online")
def online():
    try:
        count = sum(1 for g in bot.guilds for m in g.members if m.status != discord.Status.offline and not m.bot)
        return jsonify({"online":count})
    except: return jsonify({"online":0})

@api.route("/api/leaderboard")
def leaderboard():
    try:
        rows = fetchall("""SELECT u.user_id,u.username,u.msg_count,u.last_seen,u.first_seen,u.avatar_url,
            COUNT(DISTINCT DATE(m.timestamp)) as active_days,
            SUM(CASE WHEN m.timestamp >= NOW()-INTERVAL '7 days' THEN 1 ELSE 0 END) as recent_msgs
            FROM user_stats u LEFT JOIN messages m ON u.user_id=m.user_id
            GROUP BY u.user_id,u.username,u.msg_count,u.last_seen,u.first_seen,u.avatar_url
            ORDER BY u.msg_count DESC LIMIT 25""")
        result=[]
        for r in rows:
            d=dict(r)
            try: ds=max(1,(datetime.datetime.utcnow()-r["first_seen"].replace(tzinfo=None)).days)
            except: ds=1
            d["engagement_score"]=round(min(100,((r["active_days"]or 0)/ds)*100)*0.4+min(100,(r["recent_msgs"]or 0)*10)*0.35+min(100,(r["msg_count"]or 0)/10)*0.25)
            d["last_seen"]=r["last_seen"].isoformat() if r["last_seen"] else None
            d["first_seen"]=r["first_seen"].isoformat() if r["first_seen"] else None
            result.append(d)
        return jsonify(result)
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/channels")
def channels():
    try: return jsonify([dict(r) for r in fetchall("SELECT channel_name,COUNT(*) as cnt FROM messages GROUP BY channel_id,channel_name ORDER BY cnt DESC LIMIT 15")])
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/heatmap")
def heatmap():
    try:
        rows=fetchall("SELECT hour,COUNT(*) as cnt FROM messages GROUP BY hour")
        counts={r["hour"]:r["cnt"] for r in rows}
        return jsonify([{"hour":h,"count":counts.get(h,0)} for h in range(24)])
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/daily")
def daily():
    try:
        rows=fetchall("SELECT DATE(timestamp) as day,COUNT(*) as cnt FROM messages GROUP BY day ORDER BY day DESC LIMIT 30")
        return jsonify(list(reversed([{"day":str(r["day"]),"cnt":r["cnt"]} for r in rows])))
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/dayofweek")
def dayofweek():
    try:
        rows=fetchall("SELECT day_of_week,COUNT(*) as cnt FROM messages GROUP BY day_of_week ORDER BY day_of_week")
        days=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]; counts={r["day_of_week"]:r["cnt"] for r in rows}
        return jsonify([{"day":days[i],"count":counts.get(i,0)} for i in range(7)])
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/newmembers")
def newmembers():
    try:
        rows=fetchall("SELECT TO_CHAR(first_seen,'IYYY-IW') as week,COUNT(*) as cnt FROM user_stats GROUP BY week ORDER BY week DESC LIMIT 12")
        return jsonify(list(reversed([dict(r) for r in rows])))
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/compare")
def compare():
    try:
        rows=fetchall("SELECT SUM(CASE WHEN timestamp>=NOW()-INTERVAL '30 days' THEN 1 ELSE 0 END) as period_a,SUM(CASE WHEN timestamp>=NOW()-INTERVAL '60 days' AND timestamp<NOW()-INTERVAL '30 days' THEN 1 ELSE 0 END) as period_b FROM messages")
        r=rows[0] if rows else {}; a,b=r.get("period_a") or 0,r.get("period_b") or 0
        change=round(((a-b)/b*100) if b>0 else 0,1)
        da=fetchall("SELECT DATE(timestamp) as day,COUNT(*) as cnt FROM messages WHERE timestamp>=NOW()-INTERVAL '30 days' GROUP BY day ORDER BY day")
        db_=fetchall("SELECT DATE(timestamp) as day,COUNT(*) as cnt FROM messages WHERE timestamp>=NOW()-INTERVAL '60 days' AND timestamp<NOW()-INTERVAL '30 days' GROUP BY day ORDER BY day")
        return jsonify({"period_a_total":a,"period_b_total":b,"change_pct":change,
                        "daily_a":[{"day":str(r["day"]),"cnt":r["cnt"]} for r in da],
                        "daily_b":[{"day":str(r["day"]),"cnt":r["cnt"]} for r in db_]})
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/loyalty")
def loyalty():
    try:
        rows=fetchall("SELECT msg_count FROM user_stats"); mx=max((r["msg_count"] for r in rows),default=1)
        core=active=member=0
        for r in rows:
            rat=r["msg_count"]/mx
            if rat>0.6: core+=1
            elif rat>0.25: active+=1
            else: member+=1
        return jsonify({"core":core,"active":active,"member":member})
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/radar")
def radar():
    try:
        rows=fetchall("""SELECT u.user_id,u.username,u.msg_count,u.first_seen,
            COUNT(DISTINCT DATE(m.timestamp)) as active_days,
            SUM(CASE WHEN m.timestamp>=NOW()-INTERVAL '7 days' THEN 1 ELSE 0 END) as recent_msgs
            FROM user_stats u LEFT JOIN messages m ON u.user_id=m.user_id
            GROUP BY u.user_id,u.username,u.msg_count,u.first_seen ORDER BY u.msg_count DESC LIMIT 5""")
        result=[]
        for r in rows:
            try: ds=max(1,(datetime.datetime.utcnow()-r["first_seen"].replace(tzinfo=None)).days)
            except: ds=1
            result.append({"username":r["username"].split("#")[0],"volume":min(100,round((r["msg_count"]or 0)/10)),"consistency":min(100,round(((r["active_days"]or 0)/ds)*100)),"recency":min(100,(r["recent_msgs"]or 0)*10)})
        return jsonify(result)
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/timeline")
def timeline():
    try:
        top=fetchall("SELECT user_id,username FROM user_stats ORDER BY msg_count DESC LIMIT 10")
        result=[]
        for u in top:
            days=fetchall("SELECT DATE(timestamp) as day,COUNT(*) as cnt FROM messages WHERE user_id=%s AND timestamp>=NOW()-INTERVAL '30 days' GROUP BY day ORDER BY day",(u["user_id"],))
            result.append({"username":u["username"].split("#")[0],"days":[{"day":str(d["day"]),"cnt":d["cnt"]} for d in days]})
        return jsonify(result)
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/engagement-leaderboard")
def engagement_leaderboard():
    try:
        rows=fetchall("""SELECT u.user_id,u.username,u.msg_count,u.last_seen,u.first_seen,u.avatar_url,
            COUNT(DISTINCT DATE(m.timestamp)) as active_days,
            SUM(CASE WHEN m.timestamp>=NOW()-INTERVAL '7 days' THEN 1 ELSE 0 END) as recent_msgs
            FROM user_stats u LEFT JOIN messages m ON u.user_id=m.user_id
            GROUP BY u.user_id,u.username,u.msg_count,u.last_seen,u.first_seen,u.avatar_url""")
        result=[]
        for r in rows:
            try: ds=max(1,(datetime.datetime.utcnow()-r["first_seen"].replace(tzinfo=None)).days)
            except: ds=1
            score=round(min(100,((r["active_days"]or 0)/ds)*100)*0.4+min(100,(r["recent_msgs"]or 0)*10)*0.35+min(100,(r["msg_count"]or 0)/10)*0.25)
            result.append({"username":r["username"].split("#")[0],"avatar_url":r["avatar_url"],"engagement_score":score,"last_seen":r["last_seen"].isoformat() if r["last_seen"] else None})
        result.sort(key=lambda x:x["engagement_score"],reverse=True)
        return jsonify(result[:15])
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/daterange")
def daterange():
    try:
        start,end=request.args.get("start"),request.args.get("end")
        if not start or not end: return jsonify({"error":"start and end required"}),400
        total=fetchone("SELECT COUNT(*) as cnt FROM messages WHERE timestamp>=%s AND timestamp<=%s",(start,end))
        users=fetchone("SELECT COUNT(DISTINCT user_id) as cnt FROM messages WHERE timestamp>=%s AND timestamp<=%s",(start,end))
        daily=fetchall("SELECT DATE(timestamp) as day,COUNT(*) as cnt FROM messages WHERE timestamp>=%s AND timestamp<=%s GROUP BY day ORDER BY day",(start,end))
        top_users=fetchall("SELECT username,COUNT(*) as cnt FROM messages WHERE timestamp>=%s AND timestamp<=%s GROUP BY username ORDER BY cnt DESC LIMIT 10",(start,end))
        top_chans=fetchall("SELECT channel_name,COUNT(*) as cnt FROM messages WHERE timestamp>=%s AND timestamp<=%s GROUP BY channel_name ORDER BY cnt DESC LIMIT 8",(start,end))
        peak_hour=fetchone("SELECT hour,COUNT(*) as cnt FROM messages WHERE timestamp>=%s AND timestamp<=%s GROUP BY hour ORDER BY cnt DESC LIMIT 1",(start,end))
        return jsonify({"total_messages":total["cnt"] if total else 0,"unique_users":users["cnt"] if users else 0,"peak_hour":peak_hour["hour"] if peak_hour else None,
                        "daily":[{"day":str(r["day"]),"cnt":r["cnt"]} for r in daily],"top_users":[dict(r) for r in top_users],"top_channels":[dict(r) for r in top_chans]})
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/giveaways")
def api_giveaways():
    try:
        rows=fetchall("""SELECT g.id,g.prize,g.description,g.host_id,g.winner_mode,g.winner_count,
            g.ends_at,g.ended,g.winner_ids,g.created_at,g.channel_id,COUNT(e.id) as entry_count
            FROM giveaways g LEFT JOIN giveaway_entries e ON g.id=e.giveaway_id
            GROUP BY g.id ORDER BY g.created_at DESC LIMIT 50""")
        result=[]
        for r in rows:
            d=dict(r); d["ends_at"]=r["ends_at"].isoformat() if r["ends_at"] else None; d["created_at"]=r["created_at"].isoformat() if r["created_at"] else None; result.append(d)
        return jsonify(result)
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/giveaways/<int:gid>/entries")
def api_giveaway_entries(gid):
    try:
        gw=fetchone("SELECT * FROM giveaways WHERE id=%s",(gid,))
        if not gw: return jsonify({"error":"Not found"}),404
        entries=fetchall("SELECT user_id,username,avatar_url,entered_at FROM giveaway_entries WHERE giveaway_id=%s ORDER BY entered_at",(gid,))
        return jsonify({"giveaway":{"id":gw["id"],"prize":gw["prize"],"description":gw["description"],"winner_mode":gw["winner_mode"],"winner_count":gw["winner_count"],"ended":gw["ended"],"ends_at":gw["ends_at"].isoformat() if gw["ends_at"] else None,"winner_ids":gw["winner_ids"],"entry_count":len(entries)},
                        "entries":[{"user_id":e["user_id"],"username":e["username"],"avatar_url":e["avatar_url"],"entered_at":e["entered_at"].isoformat()} for e in entries]})
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/polls")
def api_polls():
    try:
        rows=fetchall("SELECT id,guild_id,channel_id,message_id,question,options,created_by,created_at,closed FROM polls ORDER BY created_at DESC LIMIT 50")
        result=[]
        for r in rows:
            d=dict(r); d["created_at"]=r["created_at"].isoformat() if r["created_at"] else None; result.append(d)
        return jsonify(result)
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/polls/<message_id>/results")
def api_poll_results(message_id):
    try:
        poll=fetchone("SELECT * FROM polls WHERE message_id=%s",(message_id,))
        if not poll: return jsonify({"error":"Not found"}),404
        options=poll["options"]; reaction_map={}; total_votes=0
        try:
            guild=bot.get_guild(int(poll["guild_id"])); channel=guild.get_channel(int(poll["channel_id"])) if guild else None
            if channel:
                import asyncio
                future=asyncio.run_coroutine_threadsafe(channel.fetch_message(int(message_id)),bot.loop)
                msg=future.result(timeout=10)
                for reaction in msg.reactions:
                    emoji=str(reaction.emoji)
                    if emoji in NUMBER_EMOJIS:
                        idx=NUMBER_EMOJIS.index(emoji)
                        if idx<len(options):
                            votes=max(0,reaction.count-1); reaction_map[idx]=votes; total_votes+=votes
        except Exception as e: print(f"⚠️ Poll reaction fetch: {e}")
        winner_idx=max(reaction_map,key=reaction_map.get) if reaction_map and max(reaction_map.values())>0 else None
        results=[{"index":i,"option":opt,"votes":reaction_map.get(i,0),"pct":round((reaction_map.get(i,0)/total_votes)*100) if total_votes>0 else 0,"is_winner":(i==winner_idx)} for i,opt in enumerate(options)]
        return jsonify({"poll":{"message_id":poll["message_id"],"question":poll["question"],"created_by":poll["created_by"],"created_at":poll["created_at"].isoformat() if poll["created_at"] else None,"closed":poll["closed"],"total_votes":total_votes},"results":results})
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/cotw/current")
def api_cotw_current():
    try:
        week=current_week()
        state=fetchone("SELECT * FROM cotw_state WHERE guild_id=%s AND week=%s",(str(GUILD_ID),week))
        subs=fetchall("SELECT user_id,username,theory,submitted_at FROM cotw_submissions WHERE guild_id=%s AND week=%s ORDER BY submitted_at",(str(GUILD_ID),week))
        return jsonify({"week":week,"phase":state["phase"] if state else "pending","vote_msg_id":state["vote_msg_id"] if state else None,
                        "submissions":[{"username":r["username"].split("#")[0],"theory":r["theory"],"submitted_at":r["submitted_at"].isoformat()} for r in subs]})
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/cotw/winners")
def api_cotw_winners():
    try:
        page=max(1,int(request.args.get("page",1))); limit=max(1,min(50,int(request.args.get("limit",10)))); offset=(page-1)*limit
        total=fetchone("SELECT COUNT(*) as c FROM cotw_winners WHERE guild_id=%s",(str(GUILD_ID),))["c"]
        rows=fetchall("SELECT week,winner_username,theory,vote_count,total_entries,announced_at FROM cotw_winners WHERE guild_id=%s ORDER BY announced_at DESC LIMIT %s OFFSET %s",(str(GUILD_ID),limit,offset))
        return jsonify({"total":total,"page":page,"limit":limit,"has_more":(offset+limit)<total,
                        "winners":[{"week":r["week"],"winner_username":r["winner_username"].split("#")[0],"theory":r["theory"],"vote_count":r["vote_count"],"total_entries":r["total_entries"],"announced_at":r["announced_at"].isoformat()} for r in rows]})
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/jorge/debug")
def api_jorge_debug():
    try:
        rows=fetchall("SELECT channel_id,channel_name,MAX(timestamp) as last_post,COUNT(*) as msg_count FROM messages WHERE user_id=%s GROUP BY channel_id,channel_name ORDER BY last_post DESC",(str(JORGE_USER_ID),))
        return jsonify([{"channel_id":r["channel_id"],"channel_name":r["channel_name"],"last_post":r["last_post"].isoformat() if r["last_post"] else None,"msg_count":r["msg_count"]} for r in rows])
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/jorge")
def api_jorge():
    try:
        last_post=fetchone("SELECT timestamp,channel_name FROM messages WHERE user_id=%s AND channel_id!=%s ORDER BY timestamp DESC LIMIT 1",(str(JORGE_USER_ID),str(JORGE_EXCLUDE_CHANNEL)))
        since=last_post["timestamp"] if last_post else None
        if since:
            ping_count=fetchone("SELECT COUNT(*) as c FROM jorge_pings WHERE pinged_at>=%s",(since,))["c"]
            mention_count=fetchone("SELECT COUNT(*) as c FROM jorge_mentions WHERE timestamp>=%s",(since,))["c"]
            recent_pings=fetchall("SELECT username,pinged_at FROM jorge_pings WHERE pinged_at>=%s ORDER BY pinged_at DESC LIMIT 10",(since,))
            top_pinger=fetchone("SELECT username,COUNT(*) as c FROM jorge_pings WHERE pinged_at>=%s GROUP BY username ORDER BY c DESC LIMIT 1",(since,))
        else:
            ping_count=fetchone("SELECT COUNT(*) as c FROM jorge_pings")["c"]
            mention_count=fetchone("SELECT COUNT(*) as c FROM jorge_mentions")["c"]
            recent_pings=fetchall("SELECT username,pinged_at FROM jorge_pings ORDER BY pinged_at DESC LIMIT 10")
            top_pinger=fetchone("SELECT username,COUNT(*) as c FROM jorge_pings GROUP BY username ORDER BY c DESC LIMIT 1")
        atp=fetchone("SELECT COUNT(*) as c FROM jorge_pings")["c"]
        atm=fetchone("SELECT COUNT(*) as c FROM jorge_mentions")["c"]
        return jsonify({"last_post_at":since.isoformat() if since else None,"last_post_channel":last_post["channel_name"] if last_post else None,
                        "mentions_since":ping_count+mention_count,"ping_count":ping_count,"mention_count":mention_count,
                        "all_time_mentions":atp+atm,"all_time_pings":atp,"all_time_manual":atm,
                        "top_tagger":top_pinger["username"].split("#")[0] if top_pinger else None,
                        "top_tagger_count":top_pinger["c"] if top_pinger else 0,
                        "recent_mentions":[{"author":r["username"].split("#")[0],"at":r["pinged_at"].isoformat()} for r in recent_pings]})
    except Exception as e: return jsonify({"error":str(e)}),500

# ─── FOURTHWALL API ───────────────────────────────────────────────────────────
@api.route("/api/fw/overview")
def fw_overview():
    try:
        to=fetchone("SELECT COUNT(*) as c FROM orders WHERE event_type='order.placed'")["c"]
        tr=fetchone("SELECT COALESCE(SUM(total_amount),0) as r FROM orders WHERE event_type='order.placed'")["r"]
        tg=fetchone("SELECT COUNT(*) as c FROM orders WHERE event_type='gift.purchased'")["c"]
        ts=fetchone("SELECT COUNT(*) as c FROM orders WHERE event_type='subscription.purchased'")["c"]
        lo=fetchone("SELECT buyer_name,product_name,total_amount,timestamp FROM orders ORDER BY timestamp DESC LIMIT 1")
        return jsonify({"total_orders":to,"total_revenue":float(tr),"total_gifts":tg,"total_subs":ts,
                        "last_order":{"buyer_name":lo["buyer_name"],"product_name":lo["product_name"],"total_amount":float(lo["total_amount"]) if lo["total_amount"] else 0,"timestamp":lo["timestamp"].isoformat()} if lo else None})
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/fw/revenue")
def fw_revenue():
    try:
        rows=fetchall("SELECT DATE(timestamp) as day,SUM(total_amount) as revenue,COUNT(*) as orders FROM orders WHERE event_type='order.placed' GROUP BY day ORDER BY day DESC LIMIT 30")
        return jsonify(list(reversed([{"day":str(r["day"]),"revenue":float(r["revenue"]),"orders":r["orders"]} for r in rows])))
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/fw/products")
def fw_products():
    try:
        rows=fetchall("SELECT product_name,COUNT(*) as cnt,SUM(total_amount) as revenue FROM orders WHERE event_type='order.placed' AND product_name IS NOT NULL GROUP BY product_name ORDER BY cnt DESC LIMIT 10")
        return jsonify([{"product":r["product_name"],"count":r["cnt"],"revenue":float(r["revenue"])} for r in rows])
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/api/fw/orders")
def fw_orders():
    try:
        rows=fetchall("SELECT order_id,event_type,buyer_name,product_name,total_amount,status,timestamp FROM orders ORDER BY timestamp DESC LIMIT 20")
        result=[]
        for r in rows:
            d=dict(r); d["timestamp"]=r["timestamp"].isoformat(); d["total_amount"]=float(r["total_amount"]) if r["total_amount"] else 0; result.append(d)
        return jsonify(result)
    except Exception as e: return jsonify({"error":str(e)}),500

@api.route("/webhook/fourthwall",methods=["POST"])
def fourthwall_webhook():
    body=request.get_data(); data=request.get_json(silent=True) or {}
    if FW_SECRET:
        sig=request.headers.get("X-Fourthwall-Signature","")
        expected="sha256="+hmac.new(FW_SECRET.encode(),body,hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig,expected): print("⚠️ FW Signature mismatch")
    event_type=data.get("type",""); payload=data.get("data",{})
    buyer_name=payload.get("buyerName") or payload.get("email","Someone")
    try: total=float(str(payload.get("totalAmount") or payload.get("amount") or 0).replace("$","").replace(",",""))
    except: total=0.0
    total_fmt=payload.get("totalFormatted") or f"${total:.2f}"
    status=payload.get("status","Processing"); items=payload.get("lineItems",[])
    product_name=items[0].get("productName","Item") if items else payload.get("productName","Item")
    if len(items)>1: product_name+=f" + {len(items)-1} more"
    order_id=payload.get("id") or payload.get("orderId") or str(datetime.datetime.utcnow().timestamp())
    try: execute("INSERT INTO orders (order_id,event_type,buyer_name,buyer_email,product_name,total_amount,status,raw) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (order_id) DO NOTHING",(order_id,event_type,buyer_name,payload.get("email",""),product_name,total,status,psycopg2.extras.Json(payload)))
    except Exception as e: print(f"⚠️ FW DB error: {e}")
    if event_type=="order.placed": msg=f"🛒 **New Order!**\n**{buyer_name}** ordered **{product_name}**\n💰 **{total_fmt}** · 📦 {status}\nThanks for supporting the podcast! 🎙️"; asyncio.run_coroutine_threadsafe(_send_fw_alert(msg,FW_ORDER_CHANNEL),bot.loop)
    elif event_type=="gift.purchased": msg=f"🎁 **Gift Purchase!**\n**{buyer_name}** gifted **{product_name}**\n💰 **{total_fmt}**\nWhat a legend! 🙌"; asyncio.run_coroutine_threadsafe(_send_fw_alert(msg,FW_GIFT_CHANNEL or FW_ORDER_CHANNEL),bot.loop)
    elif event_type=="subscription.purchased": msg=f"🔔 **New Subscription!**\n**{buyer_name}** subscribed to **{product_name}**\n💰 **{total_fmt}**\nWelcome to the inner circle! 🕵️"; asyncio.run_coroutine_threadsafe(_send_fw_alert(msg,FW_GIFT_CHANNEL or FW_ORDER_CHANNEL),bot.loop)
    return jsonify({"ok":True}),200

async def _send_fw_alert(msg,channel_id):
    if not channel_id: return
    ch=bot.get_channel(channel_id)
    if ch: await ch.send(msg)

# ─── BOT SETUP ───────────────────────────────────────────────────────────────
intents=discord.Intents.default(); intents.message_content=True; intents.members=True; intents.presences=True
bot=commands.Bot(command_prefix="!",intents=intents)

def member_can_manage(member):
    if member.guild_permissions.administrator: return True
    return bool({r.id for r in member.roles} & GIVEAWAY_MANAGER_ROLE_IDS)

def member_can_enter(member):
    return any(r.id==GIVEAWAY_ENTRY_ROLE_ID for r in member.roles)

def ago_sync(ts):
    if not ts: return "a while ago"
    diff=datetime.datetime.utcnow()-ts.replace(tzinfo=None); s=int(diff.total_seconds())
    if s<60: return "just now"
    if s<3600: return f"{s//60}m ago"
    if s<86400: return f"{s//3600}h ago"
    return f"{s//86400}d ago"

def parse_duration(text):
    m=_re.fullmatch(r"(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?",text.strip().lower())
    if not m or not any(m.groups()): return None
    td=datetime.timedelta(days=int(m.group(1) or 0),hours=int(m.group(2) or 0),minutes=int(m.group(3) or 0),seconds=int(m.group(4) or 0))
    return td if td.total_seconds()>0 else None

def format_remaining(ends_at):
    remaining=ends_at.replace(tzinfo=None)-datetime.datetime.utcnow()
    if remaining.total_seconds()<=0: return "**ENDED**"
    total=int(remaining.total_seconds()); d,h,m,s=total//86400,(total%86400)//3600,(total%3600)//60,total%60
    parts=[]
    if d: parts.append(f"{d}d")
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    if not d and not h: parts.append(f"{s}s")
    return " ".join(parts) or "< 1s"

def build_giveaway_embed(gw,entry_count,ended=False):
    embed=discord.Embed(title=f"🎉  GIVEAWAY: {gw['prize']}",description=gw.get("description") or "",color=discord.Color.gold() if not ended else discord.Color.light_grey())
    embed.add_field(name="⏰ Time Remaining" if not ended else "⏰ Status",value=format_remaining(gw["ends_at"]) if not ended else "**ENDED**",inline=True)
    embed.add_field(name="🏆 Winners",value=str(gw["winner_count"]),inline=True)
    embed.add_field(name="🎟️ Entries",value=str(entry_count),inline=True)
    mode_labels={"announce":"📢 Announced in channel","dm_host":"🔒 DM'd to host only","manual":"✋ Host picks manually"}
    embed.add_field(name="🎯 Winner Selection",value=mode_labels.get(gw["winner_mode"],"Random"),inline=True)
    embed.set_footer(text=f"Giveaway ID: {gw['id']}  •  You must have the required role to enter.")
    return embed


# ═══════════════════════════════════════════════════════════════════════════════
# GIVEAWAY
# ═══════════════════════════════════════════════════════════════════════════════

class GiveawayStartModal(discord.ui.Modal, title="🎉 Start a Giveaway"):
    prize = discord.ui.TextInput(label="Prize", placeholder="e.g. Discord Nitro, $50 Gift Card", max_length=200)
    description = discord.ui.TextInput(label="Description (optional)", required=False, style=discord.TextStyle.paragraph, max_length=500, placeholder="Extra details about the prize...")
    duration = discord.ui.TextInput(label="Duration", placeholder="e.g. 30m  2h  1d  1h30m  (min 1m, max 30d)", max_length=20)
    winner_count = discord.ui.TextInput(label="Number of Winners (1–20)", placeholder="1", max_length=2, default="1")

    def __init__(self, channel: discord.TextChannel, winner_mode: str):
        super().__init__()
        self.target_channel = channel
        self.winner_mode    = winner_mode

    async def on_submit(self, interaction: discord.Interaction):
        duration = parse_duration(self.duration.value.strip())
        if not duration or not (60 <= duration.total_seconds() <= 86400*30):
            await interaction.response.send_message("❌ Invalid duration. Examples: `30m`, `2h`, `1d`. Min 1 min, max 30 days.", ephemeral=True); return
        wc_val = self.winner_count.value.strip()
        if not wc_val.isdigit() or not 1 <= int(wc_val) <= 20:
            await interaction.response.send_message("❌ Winner count must be a number between 1 and 20.", ephemeral=True); return
        prize       = self.prize.value.strip()
        description = self.description.value.strip() or None
        winner_count= int(wc_val)
        ends_at     = datetime.datetime.utcnow() + duration
        with get_db() as db:
            with db.cursor() as cur:
                cur.execute("INSERT INTO giveaways (guild_id,channel_id,prize,description,host_id,winner_mode,winner_count,ends_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                            (str(interaction.guild_id),str(self.target_channel.id),prize,description,str(interaction.user.id),self.winner_mode,winner_count,ends_at))
                giveaway_id=cur.fetchone()[0]
        gw=fetchone("SELECT * FROM giveaways WHERE id=%s",(giveaway_id,))
        view=GiveawayView(giveaway_id); bot.add_view(view)
        gmsg=await self.target_channel.send(embed=build_giveaway_embed(gw,0),view=view)
        execute("UPDATE giveaways SET message_id=%s WHERE id=%s",(str(gmsg.id),giveaway_id))
        await interaction.response.send_message(f"✅ Giveaway launched in {self.target_channel.mention}!\nPrize: **{prize}** · Duration: **{self.duration.value}** · ID: `{giveaway_id}`",ephemeral=True)
        asyncio.ensure_future(giveaway_countdown(giveaway_id))


class GiveawayView(discord.ui.View):
    def __init__(self,giveaway_id):
        super().__init__(timeout=None); self.giveaway_id=giveaway_id
        btn=discord.ui.Button(label="🎉  Enter Giveaway",style=discord.ButtonStyle.green,custom_id=f"giveaway_enter_{giveaway_id}")
        btn.callback=self._enter_callback; self.add_item(btn)

    async def _enter_callback(self,interaction):
        gw=fetchone("SELECT * FROM giveaways WHERE id=%s",(self.giveaway_id,))
        if not gw or gw["ended"] or datetime.datetime.utcnow()>gw["ends_at"].replace(tzinfo=None):
            await interaction.response.send_message("❌ This giveaway has already ended!",ephemeral=True); return
        if not member_can_enter(interaction.user):
            await interaction.response.send_message("❌ You don't have the required role to enter.",ephemeral=True); return
        avatar=str(interaction.user.display_avatar.url) if interaction.user.display_avatar else ""
        try:
            execute("INSERT INTO giveaway_entries (giveaway_id,user_id,username,avatar_url) VALUES (%s,%s,%s,%s)",(self.giveaway_id,str(interaction.user.id),str(interaction.user),avatar))
            count=fetchone("SELECT COUNT(*) as c FROM giveaway_entries WHERE giveaway_id=%s",(self.giveaway_id,))["c"]
            await interaction.response.send_message(f"✅ You're in! Good luck 🍀\n*{count} total {'entry' if count==1 else 'entries'}*",ephemeral=True)
        except Exception as e:
            if "unique" in str(e).lower(): await interaction.response.send_message("⚠️ You've already entered!",ephemeral=True)
            else: await interaction.response.send_message("❌ Something went wrong.",ephemeral=True)

async def end_giveaway(giveaway_id):
    gw=fetchone("SELECT * FROM giveaways WHERE id=%s",(giveaway_id,))
    if not gw or gw["ended"]: return
    entries=fetchall("SELECT user_id,username FROM giveaway_entries WHERE giveaway_id=%s ORDER BY entered_at",(giveaway_id,))
    entry_count=len(entries); picks,winner_ids,winner_mentions=[],[],[]
    if entries and gw["winner_mode"]!="manual":
        picks=_random.sample(entries,min(gw["winner_count"],len(entries))); winner_ids=[p["user_id"] for p in picks]; winner_mentions=[f"<@{p['user_id']}>" for p in picks]
    execute("UPDATE giveaways SET ended=TRUE,winner_ids=%s WHERE id=%s",(winner_ids,giveaway_id))
    gw_final=fetchone("SELECT * FROM giveaways WHERE id=%s",(giveaway_id,)); channel=bot.get_channel(int(gw["channel_id"]))
    if channel and gw["message_id"]:
        try:
            msg=await channel.fetch_message(int(gw["message_id"])); ended_embed=build_giveaway_embed(gw_final,entry_count,ended=True)
            if winner_ids: ended_embed.add_field(name="🏆 Winner(s)",value="\n".join(winner_mentions),inline=False)
            elif gw["winner_mode"]=="manual": ended_embed.add_field(name="🏆 Winner(s)",value="Host is selecting manually…",inline=False)
            else: ended_embed.add_field(name="🏆 Winner(s)",value="No entries — no winner!",inline=False)
            ev=discord.ui.View(); ev.add_item(discord.ui.Button(label="Giveaway Ended",style=discord.ButtonStyle.grey,disabled=True,custom_id=f"giveaway_ended_{giveaway_id}"))
            await msg.edit(embed=ended_embed,view=ev)
        except Exception as e: print(f"⚠️ Giveaway embed error #{giveaway_id}: {e}")
    if channel:
        if gw["winner_mode"]=="announce" and winner_ids: await channel.send(f"🎉 **Giveaway ended!**\nCongratulations to {', '.join(winner_mentions)}! You won **{gw['prize']}**! 🏆\nPlease contact a moderator to claim your prize.")
        elif gw["winner_mode"]=="manual": await channel.send(f"🎉 **The '{gw['prize']}' giveaway has ended!**\nThe host will announce the winner(s) shortly.")
        elif gw["winner_mode"]=="dm_host" and winner_ids: await channel.send(f"🎉 **The '{gw['prize']}' giveaway has ended!** The winner has been notified privately. 🔒")
    if gw["winner_mode"]=="dm_host" and winner_ids:
        try:
            guild=bot.get_guild(int(gw["guild_id"])); host=guild.get_member(int(gw["host_id"])) if guild else None
            if host: await host.send(f"🎉 **Giveaway ended — {gw['prize']}**\n\n**Winner(s):**\n"+"\n".join(f"• {p['username']} (ID: {p['user_id']})" for p in picks)+f"\n\nTotal entries: **{entry_count}**\nGiveaway ID: `{giveaway_id}`")
        except Exception as e: print(f"⚠️ Could not DM host: {e}")
    if entries:
        guild=bot.get_guild(int(gw["guild_id"]))
        for entry in entries:
            try:
                member=guild.get_member(int(entry["user_id"])) if guild else None
                if not member: continue
                dm=f"🎉 **Congratulations — you won the '{gw['prize']}' giveaway!** 🏆\nA moderator will be in touch shortly." if entry["user_id"] in winner_ids else f"👋 The **'{gw['prize']}'** giveaway has ended. Better luck next time! 🍀"
                await member.send(dm); await asyncio.sleep(0.4)
            except: pass

async def giveaway_countdown(giveaway_id):
    while True:
        gw=fetchone("SELECT * FROM giveaways WHERE id=%s",(giveaway_id,))
        if not gw or gw["ended"]: break
        remaining_secs=(gw["ends_at"].replace(tzinfo=None)-datetime.datetime.utcnow()).total_seconds()
        if remaining_secs<=0: await end_giveaway(giveaway_id); break
        try:
            count=fetchone("SELECT COUNT(*) as c FROM giveaway_entries WHERE giveaway_id=%s",(giveaway_id,))["c"]
            ch=bot.get_channel(int(gw["channel_id"]))
            if ch and gw["message_id"]:
                msg=await ch.fetch_message(int(gw["message_id"])); await msg.edit(embed=build_giveaway_embed(gw,count))
        except Exception as e: print(f"⚠️ Giveaway refresh #{giveaway_id}: {e}")
        if remaining_secs>3600: await asyncio.sleep(300)
        elif remaining_secs>600: await asyncio.sleep(60)
        elif remaining_secs>60: await asyncio.sleep(30)
        else: await asyncio.sleep(max(1,remaining_secs-1))

# Giveaway slash command group
giveaway_grp = app_commands.Group(name="giveaway", description="Giveaway commands")

@giveaway_grp.command(name="start", description="Launch a new giveaway")
@app_commands.describe(channel="Channel to post the giveaway in", mode="How to select the winner")
@app_commands.choices(mode=[
    app_commands.Choice(name="📢 Announce winner in channel", value="announce"),
    app_commands.Choice(name="🔒 DM winner to host only",     value="dm_host"),
    app_commands.Choice(name="✋ Host picks manually",         value="manual"),
])
async def giveaway_start(interaction: discord.Interaction, channel: discord.TextChannel, mode: app_commands.Choice[str]):
    if not member_can_manage(interaction.user):
        await interaction.response.send_message("❌ You don't have permission to manage giveaways.", ephemeral=True); return
    await interaction.response.send_modal(GiveawayStartModal(channel=channel, winner_mode=mode.value))

@giveaway_grp.command(name="list", description="Show all active giveaways")
async def giveaway_list(interaction: discord.Interaction):
    if not member_can_manage(interaction.user):
        await interaction.response.send_message("❌ No permission.", ephemeral=True); return
    rows=fetchall("SELECT g.id,g.prize,g.ends_at,g.winner_count,g.channel_id,g.winner_mode,COUNT(e.id) as entry_count FROM giveaways g LEFT JOIN giveaway_entries e ON g.id=e.giveaway_id WHERE g.guild_id=%s AND g.ended=FALSE AND g.ends_at>NOW() GROUP BY g.id ORDER BY g.ends_at ASC",(str(interaction.guild_id),))
    if not rows: await interaction.response.send_message("📭 No active giveaways. Start one with `/giveaway start`!",ephemeral=True); return
    embed=discord.Embed(title=f"🎉 Active Giveaways ({len(rows)})",color=discord.Color.gold())
    mode_icons={"announce":"📢","dm_host":"🔒","manual":"✋"}
    for r in rows: embed.add_field(name=f"#{r['id']}  —  {r['prize']}",value=f"📣 <#{r['channel_id']}>  ·  ⏰ {format_remaining(r['ends_at'])}  ·  🎟️ {r['entry_count']} entries  ·  🏆 {r['winner_count']}  ·  {mode_icons.get(r['winner_mode'],'?')}",inline=False)
    embed.set_footer(text="Use /giveaway end <id> to force-end any of these.")
    await interaction.response.send_message(embed=embed,ephemeral=True)

@giveaway_grp.command(name="end", description="Force-end a giveaway early and pick winners")
@app_commands.describe(giveaway_id="The giveaway ID to end")
async def giveaway_end(interaction: discord.Interaction, giveaway_id: int):
    if not member_can_manage(interaction.user):
        await interaction.response.send_message("❌ No permission.", ephemeral=True); return
    gw=fetchone("SELECT * FROM giveaways WHERE id=%s AND guild_id=%s",(giveaway_id,str(interaction.guild_id)))
    if not gw: await interaction.response.send_message(f"❌ Giveaway `#{giveaway_id}` not found.",ephemeral=True); return
    if gw["ended"]: await interaction.response.send_message("⚠️ Already ended.",ephemeral=True); return
    await interaction.response.send_message(f"⏩ Force-ending **#{giveaway_id}: {gw['prize']}**…",ephemeral=True)
    await end_giveaway(giveaway_id)

@giveaway_grp.command(name="entries", description="View everyone who entered a giveaway")
@app_commands.describe(giveaway_id="The giveaway ID to check")
async def giveaway_entries(interaction: discord.Interaction, giveaway_id: int):
    if not member_can_manage(interaction.user):
        await interaction.response.send_message("❌ No permission.", ephemeral=True); return
    gw=fetchone("SELECT * FROM giveaways WHERE id=%s AND guild_id=%s",(giveaway_id,str(interaction.guild_id)))
    if not gw: await interaction.response.send_message("❌ Not found.",ephemeral=True); return
    entries=fetchall("SELECT username,entered_at FROM giveaway_entries WHERE giveaway_id=%s ORDER BY entered_at",(giveaway_id,))
    embed=discord.Embed(title=f"🎟️ Entries — #{giveaway_id}: {gw['prize']}",color=discord.Color.blurple())
    embed.description=f"**Status:** {'🟢 Active' if not gw['ended'] else '🔴 Ended'}  ·  **Total:** {len(entries)}"
    if not entries: embed.add_field(name="Entrants",value="No entries yet!",inline=False)
    else:
        names=[f"`{e['username'].split('#')[0]}`" for e in entries]
        for i,chunk in enumerate([names[j:j+20] for j in range(0,len(names),20)]):
            embed.add_field(name=f"Entrants {i*20+1}–{i*20+len(chunk)}",value="  ".join(chunk),inline=False)
    if gw["winner_ids"]: embed.add_field(name="🏆 Winner(s)",value=" ".join(f"<@{uid}>" for uid in gw["winner_ids"]),inline=False)
    await interaction.response.send_message(embed=embed,ephemeral=True)


# ═══════════════════════════════════════════════════════════════════════════════
# JORGE PING PANEL
# ═══════════════════════════════════════════════════════════════════════════════

def jorge_get_since():
    row=fetchone("SELECT timestamp FROM messages WHERE user_id=%s AND channel_id!=%s ORDER BY timestamp DESC LIMIT 1",(str(JORGE_USER_ID),str(JORGE_EXCLUDE_CHANNEL)))
    return row["timestamp"] if row else None

def jorge_ping_count_since(since):
    return fetchone("SELECT COUNT(*) as c FROM jorge_pings WHERE pinged_at>=%s",(since,))["c"] if since else fetchone("SELECT COUNT(*) as c FROM jorge_pings")["c"]

def jorge_mention_count_since(since):
    return fetchone("SELECT COUNT(*) as c FROM jorge_mentions WHERE timestamp>=%s",(since,))["c"] if since else fetchone("SELECT COUNT(*) as c FROM jorge_mentions")["c"]

def jorge_user_last_ping(user_id):
    row=fetchone("SELECT pinged_at FROM jorge_pings WHERE user_id=%s ORDER BY pinged_at DESC LIMIT 1",(user_id,))
    return row["pinged_at"] if row else None

def jorge_recent_pingers(since,limit=8):
    if since: return fetchall("SELECT username,avatar_url,pinged_at FROM jorge_pings WHERE pinged_at>=%s ORDER BY pinged_at DESC LIMIT %s",(since,limit))
    return fetchall("SELECT username,avatar_url,pinged_at FROM jorge_pings ORDER BY pinged_at DESC LIMIT %s",(limit,))

def build_jorge_embed(since,ping_count,mention_count):
    total=ping_count+mention_count
    if total==0: flavour="👀 Nobody has pinged Jorge yet. Be the first."
    elif total<5: flavour="🕵️ The hunt has begun..."
    elif total<10: flavour="😬 He can feel it. Somewhere."
    elif total<20: flavour="📣 Jorge. JORGE. We know you're out there."
    elif total<50: flavour="🗳️ At this point it's an organised campaign."
    elif total<100: flavour="🚨 This is getting out of hand."
    else: flavour="☠️ Someone physically go find Jorge."
    embed=discord.Embed(title="🕵️  WHERE IS JORGE?",
        description=(f"*Jorge was last spotted **{ago_sync(since)}** ago.*\n\n{flavour}") if since else (f"*Jorge has never been seen in this server.*\n\n{flavour}"),
        color=discord.Color.from_rgb(192,57,43))
    embed.add_field(name="🔘 Button Pings",value=f"**{ping_count}**\nsince last sighting",inline=True)
    embed.add_field(name="📣 Manual @Mentions",value=f"**{mention_count}**\nacross all channels",inline=True)
    embed.add_field(name="📊 Total",value=f"**{total}**\ncombined signals",inline=True)
    recent=jorge_recent_pingers(since)
    if recent:
        names="  ".join(f"`{r['username'].split('#')[0]}`  *{ago_sync(r['pinged_at'])}*" for r in recent)
        embed.add_field(name="🕐 Recent Pings",value=names[:1024],inline=False)
    embed.set_footer(text="One ping per person every 12 hours  •  Updates in real time")
    return embed

async def update_jorge_panel():
    row=fetchone("SELECT message_id,channel_id FROM jorge_panel ORDER BY id DESC LIMIT 1")
    if not row: return
    ch=bot.get_channel(int(row["channel_id"]))
    if not ch: return
    try:
        msg=await ch.fetch_message(int(row["message_id"]))
        since=jorge_get_since()
        await msg.edit(embed=build_jorge_embed(since,jorge_ping_count_since(since),jorge_mention_count_since(since)),view=JorgePingView())
    except Exception as e: print(f"⚠️ Jorge panel update error: {e}")

async def post_jorge_panel():
    ch=bot.get_channel(JORGE_WATCH_CHANNEL)
    if not ch: print("⚠️ Jorge panel: watch channel not found"); return
    since=jorge_get_since()
    view=JorgePingView(); bot.add_view(view)
    msg=await ch.send(embed=build_jorge_embed(since,jorge_ping_count_since(since),jorge_mention_count_since(since)),view=view)
    execute("DELETE FROM jorge_panel")
    execute("INSERT INTO jorge_panel (message_id,channel_id) VALUES (%s,%s)",(str(msg.id),str(ch.id)))
    print(f"🕵️ Jorge panel posted (message {msg.id})")

class JorgePingView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)

    @discord.ui.button(label="📡  Ping Jorge",style=discord.ButtonStyle.danger,custom_id="jorge_ping_button")
    async def ping_jorge(self,interaction: discord.Interaction,button: discord.ui.Button):
        user_id=str(interaction.user.id); now=datetime.datetime.utcnow()
        last=jorge_user_last_ping(user_id)
        if last:
            elapsed=(now-last.replace(tzinfo=None)).total_seconds(); remaining=JORGE_PING_COOLDOWN_HOURS*3600-elapsed
            if remaining>0:
                h,m=int(remaining//3600),int((remaining%3600)//60)
                await interaction.response.send_message(f"⏳ You already pinged Jorge! Try again in **{h}h {m}m**.\n*The search continues...*",ephemeral=True); return
        avatar=str(interaction.user.display_avatar.url) if interaction.user.display_avatar else ""
        execute("INSERT INTO jorge_pings (user_id,username,avatar_url,pinged_at) VALUES (%s,%s,%s,%s)",(user_id,str(interaction.user),avatar,now))
        since=jorge_get_since(); total=jorge_ping_count_since(since)+jorge_mention_count_since(since)
        await interaction.response.send_message(f"📡 **Ping sent!** Jorge has been signalled **{total}** time{'s' if total!=1 else ''} since his last sighting.\n*Come out, come out, wherever you are... 🕵️*",ephemeral=True)
        await update_jorge_panel()


# ═══════════════════════════════════════════════════════════════════════════════
# POLLS
# ═══════════════════════════════════════════════════════════════════════════════

@bot.tree.command(name="poll", description="Create a poll with up to 10 options")
@app_commands.describe(question="The poll question",option1="Option 1",option2="Option 2",option3="Option 3 (optional)",option4="Option 4 (optional)",option5="Option 5 (optional)",option6="Option 6 (optional)",option7="Option 7 (optional)",option8="Option 8 (optional)",option9="Option 9 (optional)",option10="Option 10 (optional)")
async def poll_cmd(interaction: discord.Interaction, question: str, option1: str, option2: str,
                   option3: Optional[str]=None, option4: Optional[str]=None, option5: Optional[str]=None,
                   option6: Optional[str]=None, option7: Optional[str]=None, option8: Optional[str]=None,
                   option9: Optional[str]=None, option10: Optional[str]=None):
    if not member_can_manage(interaction.user):
        await interaction.response.send_message("❌ No permission.", ephemeral=True); return
    options=[o for o in [option1,option2,option3,option4,option5,option6,option7,option8,option9,option10] if o]
    embed=discord.Embed(title=f"📊 {question}",color=discord.Color.blue())
    embed.description="\n\n".join(f"{NUMBER_EMOJIS[i]}  {opt}" for i,opt in enumerate(options))
    embed.set_footer(text=f"Poll by {interaction.user.display_name}  •  React to vote!")
    await interaction.response.send_message("✅ Poll posted!",ephemeral=True)
    poll_msg=await interaction.channel.send(embed=embed)
    for i in range(len(options)): await poll_msg.add_reaction(NUMBER_EMOJIS[i])
    try: execute("INSERT INTO polls (guild_id,channel_id,message_id,question,options,created_by) VALUES (%s,%s,%s,%s,%s,%s)",(str(interaction.guild_id),str(interaction.channel_id),str(poll_msg.id),question,psycopg2.extras.Json(options),str(interaction.user)))
    except Exception as e: print(f"⚠️ Poll DB save error: {e}")

@bot.tree.command(name="pollresults", description="Show a results embed for a poll")
@app_commands.describe(message_id="The message ID of the poll (right-click → Copy Message ID)")
async def poll_results(interaction: discord.Interaction, message_id: str):
    if not member_can_manage(interaction.user):
        await interaction.response.send_message("❌ No permission.", ephemeral=True); return
    poll=fetchone("SELECT * FROM polls WHERE message_id=%s AND guild_id=%s",(message_id,str(interaction.guild_id)))
    if not poll: await interaction.response.send_message("❌ Poll not found. Make sure you're using the correct message ID.",ephemeral=True); return
    options=poll["options"]
    try:
        poll_channel=interaction.guild.get_channel(int(poll["channel_id"]))
        poll_msg=await poll_channel.fetch_message(int(message_id))
    except Exception as e:
        await interaction.response.send_message(f"❌ Could not fetch the original poll message — it may have been deleted.\n`{e}`",ephemeral=True); return
    reaction_map={}
    for reaction in poll_msg.reactions:
        emoji=str(reaction.emoji)
        if emoji in NUMBER_EMOJIS:
            idx=NUMBER_EMOJIS.index(emoji)
            if idx<len(options): reaction_map[idx]=max(0,reaction.count-1)
    total_votes=sum(reaction_map.values())
    embed=discord.Embed(title="📊 Poll Results",description=f"**{poll['question']}**",color=discord.Color.blue(),timestamp=datetime.datetime.utcnow())
    if total_votes==0:
        embed.add_field(name="No votes yet!",value="Nobody has voted on this poll.",inline=False)
    else:
        winner_idx=max(reaction_map,key=reaction_map.get) if reaction_map else None
        for i,opt in enumerate(options):
            votes=reaction_map.get(i,0); pct=round((votes/total_votes)*100) if total_votes>0 else 0
            bar="█"*round(pct/10)+"░"*(10-round(pct/10))
            embed.add_field(name=f"{NUMBER_EMOJIS[i]}  {'🏆 ' if i==winner_idx and votes>0 else ''}{opt}",value=f"`{bar}` {pct}%  —  {votes} vote{'s' if votes!=1 else ''}",inline=False)
        embed.add_field(name="Total Votes",value=str(total_votes),inline=True)
        if winner_idx is not None: embed.add_field(name="🏆 Leading",value=options[winner_idx],inline=True)
    embed.set_footer(text=f"Poll by {poll['created_by'].split('#')[0]}  •  Message ID: {message_id}")
    await interaction.response.send_message(embed=embed)
    try: execute("UPDATE polls SET closed=TRUE WHERE message_id=%s",(message_id,))
    except: pass


# ═══════════════════════════════════════════════════════════════════════════════
# CONSPIRACY OF THE WEEK
# ═══════════════════════════════════════════════════════════════════════════════

async def cotw_open_submissions(guild_id=None,manual=False):
    gid=guild_id or str(GUILD_ID); week=current_week()
    state=fetchone("SELECT * FROM cotw_state WHERE guild_id=%s AND week=%s",(gid,week))
    if state and state["phase"]!="pending" and not manual: return
    execute("INSERT INTO cotw_state (guild_id,week,phase) VALUES (%s,%s,'submission') ON CONFLICT (guild_id,week) DO UPDATE SET phase='submission'",(gid,week))
    ch=bot.get_channel(COTW_CHANNEL_ID)
    if not ch: return
    embed=discord.Embed(title="🕵️ Conspiracy of the Week — SUBMISSIONS OPEN!",description=f"**Week {week}**\n\nGot a theory? Use `/submit` to nominate one.\n\n📅 Submissions close **Friday at 8 PM ET** — then the community votes.\n🏆 Winner announced **Sunday at 8 PM ET**.\n\n*One submission per member per week. 🌶️*",color=discord.Color.dark_gold())
    embed.set_footer(text="The truth is out there. Probably.")
    await ch.send(embed=embed)

async def cotw_open_voting(guild_id=None,manual=False):
    gid=guild_id or str(GUILD_ID); week=current_week()
    state=fetchone("SELECT * FROM cotw_state WHERE guild_id=%s AND week=%s",(gid,week))
    if state and state["phase"]=="voting" and not manual: return
    subs=fetchall("SELECT id,username,theory FROM cotw_submissions WHERE guild_id=%s AND week=%s ORDER BY submitted_at LIMIT 10",(gid,week))
    ch=bot.get_channel(COTW_CHANNEL_ID)
    if not ch: return
    if not subs:
        await ch.send("📭 **No submissions this week — voting skipped.**"); execute("INSERT INTO cotw_state (guild_id,week,phase) VALUES (%s,%s,'ended') ON CONFLICT (guild_id,week) DO UPDATE SET phase='ended'",(gid,week)); return
    lines=[f"{NUMBER_EMOJIS[i]}  **{s['username'].split('#')[0]}:** {s['theory']}" for i,s in enumerate(subs)]
    embed=discord.Embed(title="🗳️ Conspiracy of the Week — VOTE NOW!",description="\n\n".join(lines)+"\n\n*React with the number of your favourite. Voting closes Sunday at 8 PM ET!*",color=discord.Color.purple())
    embed.set_footer(text=f"Week {week}  •  {len(subs)} theories in the running")
    vote_msg=await ch.send(embed=embed)
    for i in range(len(subs)): await vote_msg.add_reaction(NUMBER_EMOJIS[i])
    execute("INSERT INTO cotw_state (guild_id,week,phase,vote_msg_id) VALUES (%s,%s,'voting',%s) ON CONFLICT (guild_id,week) DO UPDATE SET phase='voting',vote_msg_id=%s",(gid,week,str(vote_msg.id),str(vote_msg.id)))

async def cotw_announce_winner(guild_id=None,manual=False):
    gid=guild_id or str(GUILD_ID); week=current_week()
    state=fetchone("SELECT * FROM cotw_state WHERE guild_id=%s AND week=%s",(gid,week))
    if not state or not state["vote_msg_id"]: return
    if state["phase"]=="ended" and not manual: return
    ch=bot.get_channel(COTW_CHANNEL_ID)
    subs=fetchall("SELECT id,username,user_id,theory FROM cotw_submissions WHERE guild_id=%s AND week=%s ORDER BY submitted_at LIMIT 10",(gid,week))
    if not ch or not subs: return
    try:
        vote_msg=await ch.fetch_message(int(state["vote_msg_id"]))
        reaction_counts={}
        for reaction in vote_msg.reactions:
            emoji=str(reaction.emoji)
            if emoji in NUMBER_EMOJIS:
                idx=NUMBER_EMOJIS.index(emoji)
                if idx<len(subs): reaction_counts[idx]=max(0,reaction.count-1)
        execute("UPDATE cotw_state SET phase='ended' WHERE guild_id=%s AND week=%s",(gid,week))
        if not reaction_counts or max(reaction_counts.values())==0:
            await ch.send(f"📭 **Conspiracy of the Week ({week}) — No votes were cast!**"); return
        winner_idx=max(reaction_counts,key=reaction_counts.get); winner_sub=subs[winner_idx]; winner_votes=reaction_counts[winner_idx]
        try:
            execute("""INSERT INTO cotw_winners (guild_id,week,winner_user_id,winner_username,theory,vote_count,total_entries) VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (guild_id,week) DO UPDATE SET winner_user_id=EXCLUDED.winner_user_id,winner_username=EXCLUDED.winner_username,theory=EXCLUDED.theory,vote_count=EXCLUDED.vote_count,total_entries=EXCLUDED.total_entries""",
                (gid,week,winner_sub["user_id"],winner_sub["username"],winner_sub["theory"],winner_votes,len(subs)))
        except Exception as e: print(f"⚠️ COTW winner DB: {e}")
        embed=discord.Embed(title="🏆 Conspiracy of the Week — WINNER ANNOUNCED!",description=f"**Week {week} winner:**\n\n*\"{winner_sub['theory']}\"*\n\n— **{winner_sub['username'].split('#')[0]}** with **{winner_votes} vote{'s' if winner_votes!=1 else ''}** 🎉",color=discord.Color.gold())
        embed.set_footer(text="Submissions reopen Monday at 8 PM ET. Stay paranoid. 🕵️")
        winner_msg=await ch.send(embed=embed)
        try: await winner_msg.pin()
        except: pass
        try:
            guild=bot.get_guild(GUILD_ID); member=guild.get_member(int(winner_sub["user_id"])) if guild else None
            if member: await member.send(f"🏆 **Congratulations!** Your theory won the Conspiracy of the Week!\n\n*\"{winner_sub['theory']}\"*\n\nCheck {ch.mention} for the announcement!")
        except: pass
    except Exception as e: print(f"⚠️ COTW winner error: {e}")

# COTW slash command group
cotw_grp = app_commands.Group(name="cotw", description="Conspiracy of the Week commands")

@bot.tree.command(name="submit", description="Submit a theory for Conspiracy of the Week")
@app_commands.describe(theory="Your conspiracy theory (max 300 characters)")
async def cotw_submit(interaction: discord.Interaction, theory: str):
    if len(theory)<10: await interaction.response.send_message("❌ Theory must be at least 10 characters.",ephemeral=True); return
    if len(theory)>300: await interaction.response.send_message("❌ Theory must be 300 characters or fewer.",ephemeral=True); return
    week=current_week()
    state=fetchone("SELECT phase FROM cotw_state WHERE guild_id=%s AND week=%s",(str(interaction.guild_id),week))
    if not state or state["phase"]!="submission":
        await interaction.response.send_message("❌ Submissions aren't open right now. They open **Monday at 8 PM ET**!",ephemeral=True); return
    try:
        execute("INSERT INTO cotw_submissions (guild_id,user_id,username,theory,week) VALUES (%s,%s,%s,%s,%s)",(str(interaction.guild_id),str(interaction.user.id),str(interaction.user),theory,week))
        count=fetchone("SELECT COUNT(*) as c FROM cotw_submissions WHERE guild_id=%s AND week=%s",(str(interaction.guild_id),week))["c"]
        await interaction.response.send_message(f"✅ **Theory submitted!** 🕵️\n*\"{theory}\"*\n\nYou're entry #{count} this week. Voting opens Friday at 8 PM ET!",ephemeral=True)
    except Exception as e:
        if "unique" in str(e).lower(): await interaction.response.send_message("⚠️ You've already submitted a theory this week!",ephemeral=True)
        else: await interaction.response.send_message("❌ Something went wrong.",ephemeral=True)

@cotw_grp.command(name="status", description="Show current week's COTW status")
async def cotw_status(interaction: discord.Interaction):
    if not member_can_manage(interaction.user):
        await interaction.response.send_message("❌ No permission.", ephemeral=True); return
    week=current_week()
    state=fetchone("SELECT * FROM cotw_state WHERE guild_id=%s AND week=%s",(str(interaction.guild_id),week))
    count=fetchone("SELECT COUNT(*) as c FROM cotw_submissions WHERE guild_id=%s AND week=%s",(str(interaction.guild_id),week))
    embed=discord.Embed(title=f"🕵️ COTW Status — {week}",color=discord.Color.dark_gold())
    embed.add_field(name="Phase",value=state["phase"] if state else "pending",inline=True)
    embed.add_field(name="Submissions",value=str(count["c"] if count else 0),inline=True)
    embed.add_field(name="Commands",value="`/cotw open` — Open submissions\n`/cotw vote` — Open voting\n`/cotw end` — Announce winner\n`/cotw list` — View submissions",inline=False)
    await interaction.response.send_message(embed=embed,ephemeral=True)

@cotw_grp.command(name="open", description="Manually open COTW submissions")
async def cotw_open_cmd(interaction: discord.Interaction):
    if not member_can_manage(interaction.user):
        await interaction.response.send_message("❌ No permission.", ephemeral=True); return
    await cotw_open_submissions(str(interaction.guild_id),manual=True)
    await interaction.response.send_message("✅ Submissions opened.",ephemeral=True)

@cotw_grp.command(name="vote", description="Close submissions and open voting")
async def cotw_vote_cmd(interaction: discord.Interaction):
    if not member_can_manage(interaction.user):
        await interaction.response.send_message("❌ No permission.", ephemeral=True); return
    await cotw_open_voting(str(interaction.guild_id),manual=True)
    await interaction.response.send_message("✅ Voting phase started.",ephemeral=True)

@cotw_grp.command(name="end", description="Announce the COTW winner now")
async def cotw_end_cmd(interaction: discord.Interaction):
    if not member_can_manage(interaction.user):
        await interaction.response.send_message("❌ No permission.", ephemeral=True); return
    await cotw_announce_winner(str(interaction.guild_id),manual=True)
    await interaction.response.send_message("✅ Winner announced.",ephemeral=True)

@cotw_grp.command(name="list", description="View all submissions this week")
async def cotw_list_cmd(interaction: discord.Interaction):
    if not member_can_manage(interaction.user):
        await interaction.response.send_message("❌ No permission.", ephemeral=True); return
    week=current_week()
    subs=fetchall("SELECT username,theory,submitted_at FROM cotw_submissions WHERE guild_id=%s AND week=%s ORDER BY submitted_at",(str(interaction.guild_id),week))
    if not subs: await interaction.response.send_message(f"📭 No submissions yet for week {week}.",ephemeral=True); return
    embed=discord.Embed(title=f"🕵️ Submissions — {week} ({len(subs)} total)",color=discord.Color.dark_gold())
    for i,s in enumerate(subs,1): embed.add_field(name=f"{i}. {s['username'].split('#')[0]}",value=f"*\"{s['theory']}\"*",inline=False)
    await interaction.response.send_message(embed=embed,ephemeral=True)


# ═══════════════════════════════════════════════════════════════════════════════
# RATE MY THEORY
# ═══════════════════════════════════════════════════════════════════════════════

@bot.tree.command(name="ratemy", description="Get a humorous credibility score for your conspiracy theory")
@app_commands.describe(theory="The theory you want rated")
async def rate_my_theory(interaction: discord.Interaction, theory: str):
    if len(theory)<10: await interaction.response.send_message("❌ Give me something to work with — at least 10 characters.",ephemeral=True); return
    evidence=_random.choice(EVIDENCE_COMMENTS); plausibility=_random.choice(PLAUSIBILITY_COMMENTS)
    tinfoil_idx=min(4,(evidence[0]+plausibility[0])//4); tinfoil=TINFOIL_LEVELS[tinfoil_idx]; verdict=_random.choice(THEORY_VERDICTS)
    def bar(score,mx=10): return "█"*round(score)+"░"*(mx-round(score))
    embed=discord.Embed(title="🕵️ Theory Assessment Report",description=f'*"{theory[:200]}{"..." if len(theory)>200 else ""}"*',color=discord.Color.dark_green())
    embed.add_field(name=f"📁 Evidence Quality — {evidence[0]}/10",value=f"`{bar(evidence[0])}`  {evidence[1]}",inline=False)
    embed.add_field(name=f"🤔 Plausibility — {plausibility[0]}/10",value=f"`{bar(plausibility[0])}`  {plausibility[1]}",inline=False)
    embed.add_field(name=f"🎩 Tinfoil Hat Level — {tinfoil[0]}/5",value=tinfoil[1],inline=False)
    embed.add_field(name="📋 Official Verdict",value=verdict,inline=False)
    embed.set_footer(text=f"Assessment by the Bureau of Plausible Nonsense  •  Submitted by {interaction.user.display_name}")
    await interaction.response.send_message(embed=embed)


# ═══════════════════════════════════════════════════════════════════════════════
# RABBIT HOLE
# ═══════════════════════════════════════════════════════════════════════════════

def build_rabbit_hole_embed(hole: dict) -> discord.Embed:
    embed = discord.Embed(
        title=f"🕳️ {hole['title']}",
        description=hole["summary"],
        color=discord.Color.dark_purple(),
    )
    if hole.get("tags"):
        embed.add_field(name="Tags", value="  ".join(f"`{t}`" for t in hole["tags"]), inline=False)
    embed.set_footer(text="🐇 Press the button to go down a new rabbit hole  •  A discussion thread opens automatically")
    return embed


class RabbitHoleView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="🕳️  New Rabbit Hole",
        style=discord.ButtonStyle.primary,
        custom_id="rabbit_hole_button",
    )
    async def new_rabbit_hole(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        hole  = _random.choice(RABBIT_HOLES)
        embed = build_rabbit_hole_embed(hole)
        await interaction.message.edit(embed=embed, view=RabbitHoleView())
        # Create a fresh discussion thread
        try:
            await interaction.message.create_thread(
                name=f"🕳️ {hole['title']}", auto_archive_duration=1440
            )
        except Exception:
            pass  # Thread creation can fail if one already exists on the message


async def post_rabbit_hole_panel():
    """Post or refresh the persistent rabbit hole panel."""
    channel = bot.get_channel(RABBIT_HOLE_CHANNEL_ID)
    if not channel:
        print("⚠️ Rabbit hole panel: channel not found")
        return
    hole  = _random.choice(RABBIT_HOLES)
    view  = RabbitHoleView()
    bot.add_view(view)
    msg = await channel.send(embed=build_rabbit_hole_embed(hole), view=view)
    execute("DELETE FROM rabbit_hole_panel")
    execute("INSERT INTO rabbit_hole_panel (message_id, channel_id) VALUES (%s,%s)",
            (str(msg.id), str(channel.id)))
    print(f"🕳️  Rabbit hole panel posted (message {msg.id})")


async def restore_rabbit_hole_panel():
    bot.add_view(RabbitHoleView())
    row = fetchone("SELECT message_id, channel_id FROM rabbit_hole_panel ORDER BY id DESC LIMIT 1")
    if not row:
        await post_rabbit_hole_panel(); return
    channel = bot.get_channel(int(row["channel_id"]))
    if not channel:
        return
    try:
        await channel.fetch_message(int(row["message_id"]))
        print(f"🕳️  Rabbit hole panel restored (message {row['message_id']})")
    except discord.NotFound:
        print("🕳️  Rabbit hole panel message missing — posting fresh")
        await post_rabbit_hole_panel()


async def post_rabbit_hole(channel_override=None):
    """Legacy helper used by the daily scheduler — posts a standalone embed + thread."""
    ch = channel_override or bot.get_channel(RABBIT_HOLE_CHANNEL_ID)
    if not ch: return
    hole = _random.choice(RABBIT_HOLES)
    msg  = await ch.send(embed=build_rabbit_hole_embed(hole))
    try:
        await msg.create_thread(name=f"🕳️ {hole['title']}", auto_archive_duration=1440)
    except Exception as e:
        print(f"⚠️ Rabbit hole thread error: {e}")


@bot.tree.command(name="rabbithole", description="Drop a random rabbit hole in this channel (anyone can use)")
async def rabbithole_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    hole  = _random.choice(RABBIT_HOLES)
    embed = build_rabbit_hole_embed(hole)
    msg   = await interaction.channel.send(embed=embed)
    try:
        await msg.create_thread(name=f"🕳️ {hole['title']}", auto_archive_duration=1440)
    except Exception:
        pass
    await interaction.followup.send("🕳️ Rabbit hole dropped!", ephemeral=True)


# ═══════════════════════════════════════════════════════════════════════════════
# CIPHER OF THE DAY
# ═══════════════════════════════════════════════════════════════════════════════

def build_cipher_embed(cipher: dict) -> discord.Embed:
    embed = discord.Embed(title=f"🔐 {cipher['title']}", color=discord.Color.teal())
    embed.add_field(name="📟 Encoded Message", value=f"```{cipher['encoded']}```", inline=False)
    embed.add_field(name="💡 Hint",            value=f"||{cipher['hint']}||",       inline=False)
    embed.add_field(name="✅ Solution",        value=f"||{cipher['solution']}||",   inline=True)
    embed.add_field(name="📖 About",           value=cipher["explanation"],          inline=False)
    embed.set_footer(text="🔍 Click Hint or Solution to reveal  •  Press the button for a new cipher")
    return embed


class CipherView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="🔐  New Cipher",
        style=discord.ButtonStyle.success,
        custom_id="cipher_button",
    )
    async def new_cipher(self, interaction: discord.Interaction, button: discord.ui.Button):
        cipher = _random.choice(CIPHERS)
        await interaction.response.edit_message(embed=build_cipher_embed(cipher), view=CipherView())


async def post_cipher_panel():
    """Post or refresh the persistent cipher panel."""
    channel = bot.get_channel(CIPHER_CHANNEL_ID)
    if not channel:
        print("⚠️ Cipher panel: channel not found")
        return
    view = CipherView()
    bot.add_view(view)
    msg = await channel.send(embed=build_cipher_embed(_random.choice(CIPHERS)), view=view)
    execute("DELETE FROM cipher_panel")
    execute("INSERT INTO cipher_panel (message_id, channel_id) VALUES (%s,%s)",
            (str(msg.id), str(channel.id)))
    print(f"🔐  Cipher panel posted (message {msg.id})")


async def restore_cipher_panel():
    bot.add_view(CipherView())
    row = fetchone("SELECT message_id, channel_id FROM cipher_panel ORDER BY id DESC LIMIT 1")
    if not row:
        await post_cipher_panel(); return
    channel = bot.get_channel(int(row["channel_id"]))
    if not channel:
        return
    try:
        await channel.fetch_message(int(row["message_id"]))
        print(f"🔐  Cipher panel restored (message {row['message_id']})")
    except discord.NotFound:
        print("🔐  Cipher panel message missing — posting fresh")
        await post_cipher_panel()


async def post_cipher(channel_override=None):
    """Legacy helper used by the daily scheduler."""
    ch = channel_override or bot.get_channel(CIPHER_CHANNEL_ID)
    if not ch: return
    await ch.send(embed=build_cipher_embed(_random.choice(CIPHERS)))


@bot.tree.command(name="cipher", description="Drop a random cipher in this channel (anyone can use)")
async def cipher_cmd(interaction: discord.Interaction):
    await interaction.response.send_message(
        embed=build_cipher_embed(_random.choice(CIPHERS)), view=CipherView()
    )


# ═══════════════════════════════════════════════════════════════════════════════
# KEYWORD ALERTS
# ═══════════════════════════════════════════════════════════════════════════════

keyword_grp = app_commands.Group(name="keyword", description="Manage keyword alerts")

@keyword_grp.command(name="add", description="Add a keyword to monitor")
@app_commands.describe(keyword="The word or phrase to watch for")
async def keyword_add(interaction: discord.Interaction, keyword: str):
    if not member_can_manage(interaction.user):
        await interaction.response.send_message("❌ No permission.", ephemeral=True); return
    keyword=keyword.lower().strip()
    try:
        execute("INSERT INTO keyword_alerts (guild_id,keyword,added_by) VALUES (%s,%s,%s)",(str(interaction.guild_id),keyword,str(interaction.user)))
        invalidate_kw_cache(str(interaction.guild_id))
        await interaction.response.send_message(f"✅ Keyword `{keyword}` added. The bot will alert <#{KEYWORD_ALERT_CHANNEL_ID}> whenever it appears.",ephemeral=True)
    except Exception as e:
        if "unique" in str(e).lower(): await interaction.response.send_message(f"⚠️ `{keyword}` is already being monitored.",ephemeral=True)
        else: await interaction.response.send_message(f"❌ Error: {e}",ephemeral=True)

@keyword_grp.command(name="remove", description="Stop monitoring a keyword")
@app_commands.describe(keyword="The keyword to remove")
async def keyword_remove(interaction: discord.Interaction, keyword: str):
    if not member_can_manage(interaction.user):
        await interaction.response.send_message("❌ No permission.", ephemeral=True); return
    keyword=keyword.lower().strip()
    execute("DELETE FROM keyword_alerts WHERE guild_id=%s AND keyword=%s",(str(interaction.guild_id),keyword))
    invalidate_kw_cache(str(interaction.guild_id))
    await interaction.response.send_message(f"✅ Keyword `{keyword}` removed.",ephemeral=True)

@keyword_grp.command(name="list", description="Show all monitored keywords")
async def keyword_list(interaction: discord.Interaction):
    if not member_can_manage(interaction.user):
        await interaction.response.send_message("❌ No permission.", ephemeral=True); return
    rows=fetchall("SELECT keyword,added_by,added_at FROM keyword_alerts WHERE guild_id=%s ORDER BY added_at",(str(interaction.guild_id),))
    if not rows: await interaction.response.send_message("📭 No keywords configured. Add one with `/keyword add`.",ephemeral=True); return
    embed=discord.Embed(title=f"🚨 Monitored Keywords ({len(rows)})",color=discord.Color.red())
    embed.description="\n".join(f"`{r['keyword']}`  — added by {r['added_by'].split('#')[0]}" for r in rows)
    embed.set_footer(text=f"Alerts post to #{KEYWORD_ALERT_CHANNEL_ID}")
    await interaction.response.send_message(embed=embed,ephemeral=True)


# ═══════════════════════════════════════════════════════════════════════════════
# /message SLASH COMMAND
# ═══════════════════════════════════════════════════════════════════════════════

@bot.tree.command(name="message", description="Send a message as the bot to a specified channel")
@app_commands.describe(channel="The channel to send the message in", text="The message to send")
async def slash_message(interaction: discord.Interaction, channel: discord.TextChannel, text: str):
    if not member_can_manage(interaction.user):
        await interaction.response.send_message("❌ No permission.", ephemeral=True); return
    try:
        await channel.send(text)
        await interaction.response.send_message(f"✅ Message sent to {channel.mention}.",ephemeral=True)
    except discord.Forbidden: await interaction.response.send_message(f"❌ I don't have permission to post in {channel.mention}.",ephemeral=True)
    except Exception as e: await interaction.response.send_message(f"❌ Failed: {e}",ephemeral=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEDULER — 8 PM EASTERN, every day
# ═══════════════════════════════════════════════════════════════════════════════

@tasks.loop(minutes=1)
async def community_scheduler():
    now=datetime.datetime.now(EASTERN)
    if now.hour!=POST_HOUR or now.minute!=0: return
    weekday=now.weekday()
    if not task_already_fired("rabbit_hole"): await post_rabbit_hole()
    if not task_already_fired("cipher"): await post_cipher()
    gid=str(GUILD_ID)
    if weekday==0 and not task_already_fired("cotw_open"): await cotw_open_submissions(gid)
    elif weekday==4 and not task_already_fired("cotw_vote"): await cotw_open_voting(gid)
    elif weekday==6 and not task_already_fired("cotw_winner"): await cotw_announce_winner(gid)


# ═══════════════════════════════════════════════════════════════════════════════
# RSS CHECKER — every 60 minutes
# ═══════════════════════════════════════════════════════════════════════════════

def get_latest_rss_episode(feed_url):
    try:
        import feedparser; feed=feedparser.parse(feed_url)
        if not feed.entries: return None
        entry=feed.entries[0]
        duration=""
        if hasattr(entry,"itunes_duration"):
            raw=entry.itunes_duration
            if raw.isdigit():
                secs=int(raw); h,m,s=secs//3600,(secs%3600)//60,secs%60
                duration=f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"
            else: duration=raw
        ep_num=getattr(entry,"itunes_episode",None)
        image_url=None
        if hasattr(entry,"image") and hasattr(entry.image,"href"): image_url=entry.image.href
        elif feed.feed.get("image"): image_url=feed.feed.image.get("href")
        return {"guid":entry.get("id") or entry.get("link") or entry.title,"title":entry.title,"summary":_re.sub(r"<[^>]+>","",(entry.get("summary","")).strip()),"duration":duration,"ep_num":ep_num,"image_url":image_url,"link":entry.get("link") or feed.feed.get("link",""),"podcast_name":feed.feed.get("title","The Conspiracy Podcast")}
    except Exception as e: print(f"⚠️ RSS error: {e}"); return None

def rss_episode_already_posted(guid):
    try: execute("INSERT INTO rss_posted (guid) VALUES (%s)",(guid,)); return False
    except: return True

@tasks.loop(minutes=60)
async def podcast_rss_checker():
    episode=get_latest_rss_episode(PODCAST_RSS_URL)
    if not episode or rss_episode_already_posted(episode["guid"]): return
    ch=bot.get_channel(PODCAST_CHANNEL_ID)
    if not ch: return
    embed=discord.Embed(title=episode["title"],url=episode["link"] or None,color=discord.Color.from_rgb(30,215,96))
    embed.set_author(name=f"🎙️ New Episode — {episode['podcast_name']}")
    summary=episode.get("summary","").strip()
    if summary: embed.description=summary[:300]+("…" if len(summary)>300 else "")
    if episode["duration"]: embed.add_field(name="⏱️ Duration",value=episode["duration"],inline=True)
    if episode["ep_num"]: embed.add_field(name="🎧 Episode",value=f"#{episode['ep_num']}",inline=True)
    if episode["link"]: embed.add_field(name="🔗 Listen",value=f"[Click here to listen]({episode['link']})",inline=False)
    if episode["image_url"]: embed.set_thumbnail(url=episode["image_url"])
    embed.set_footer(text="New episode just dropped! 🕵️  Don't forget to subscribe & leave a review.")
    await ch.send(content=f"<@&{PODCAST_LISTENER_ROLE}>",embed=embed)
    print(f"📡 RSS: Announced — {episode['title']}")


# ═══════════════════════════════════════════════════════════════════════════════
# BOT EVENTS
# ═══════════════════════════════════════════════════════════════════════════════

@bot.event
async def on_ready():
    init_db()
    print(f"✅  Logged in as {bot.user}")
    print(f"📊  Dashboard → http://localhost:{FLASK_PORT}")

    # Register command groups
    bot.tree.add_command(giveaway_grp)
    bot.tree.add_command(cotw_grp)
    bot.tree.add_command(keyword_grp)

    # Sync slash commands to guild (instant)
    try:
        guild=discord.Object(id=GUILD_ID)
        bot.tree.copy_global_to(guild=guild)
        synced=await bot.tree.sync(guild=guild)
        print(f"⚡  Synced {len(synced)} slash command(s) to guild")
    except Exception as e:
        print(f"⚠️  Could not sync slash commands: {e}")

    # Restore active giveaways
    try:
        active=fetchall("SELECT id FROM giveaways WHERE ended=FALSE AND ends_at>NOW()")
        for gw in active:
            bot.add_view(GiveawayView(gw["id"])); asyncio.ensure_future(giveaway_countdown(gw["id"]))
        if active: print(f"🔄  Restored {len(active)} active giveaway(s)")
    except Exception as e: print(f"⚠️  Could not restore giveaways: {e}")

    # Jorge ping panel
    try:
        bot.add_view(JorgePingView())
        panel=fetchone("SELECT message_id,channel_id FROM jorge_panel ORDER BY id DESC LIMIT 1")
        if panel:
            ch=bot.get_channel(int(panel["channel_id"]))
            if ch:
                try:
                    await ch.fetch_message(int(panel["message_id"]))
                    await update_jorge_panel(); print(f"🕵️  Jorge panel restored")
                except discord.NotFound:
                    print("🕵️  Jorge panel missing — posting fresh"); await post_jorge_panel()
        else:
            print("🕵️  No Jorge panel — posting fresh"); await post_jorge_panel()
    except Exception as e: print(f"⚠️  Jorge panel error: {e}")

    # Rabbit hole panel
    try:
        await restore_rabbit_hole_panel()
    except Exception as e:
        print(f"⚠️  Rabbit hole panel error: {e}")

    # Cipher panel
    try:
        await restore_cipher_panel()
    except Exception as e:
        print(f"⚠️  Cipher panel error: {e}")

    # Start schedulers
    if not community_scheduler.is_running():
        community_scheduler.start(); print(f"⏰  Scheduler started — {POST_HOUR}:00 ET daily")
    if not podcast_rss_checker.is_running():
        podcast_rss_checker.start(); print("📡  RSS checker started — every 60 minutes")


@bot.event
async def on_message(message):
    if message.author.bot or not message.guild: return
    now=datetime.datetime.utcnow(); hour=now.hour; dow=now.weekday()
    avatar=str(message.author.display_avatar.url) if message.author.display_avatar else ""
    try:
        execute("INSERT INTO messages (user_id,username,channel_id,channel_name,guild_id,hour,day_of_week,timestamp) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (str(message.author.id),str(message.author),str(message.channel.id),message.channel.name,str(message.guild.id),hour,dow,now))
        execute("INSERT INTO user_stats (user_id,username,guild_id,msg_count,last_seen,first_seen,avatar_url) VALUES (%s,%s,%s,1,%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET username=EXCLUDED.username,msg_count=user_stats.msg_count+1,last_seen=GREATEST(user_stats.last_seen,EXCLUDED.last_seen),avatar_url=EXCLUDED.avatar_url",
                (str(message.author.id),str(message.author),str(message.guild.id),now,now,avatar))
    except Exception as e: print(f"DB error: {e}")

    # Jorge mention tracker
    try:
        jorge_mentioned=f"<@{JORGE_USER_ID}>" in message.content or f"<@!{JORGE_USER_ID}>" in message.content
        if message.channel.id==JORGE_WATCH_CHANNEL and message.author.id!=JORGE_USER_ID and jorge_mentioned:
            execute("INSERT INTO jorge_mentions (channel_id,message_id,author_id,author_name,timestamp) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (message_id) DO NOTHING",
                    (str(message.channel.id),str(message.id),str(message.author.id),str(message.author),now))
            since=jorge_get_since(); ping_count=jorge_ping_count_since(since); mention_count=jorge_mention_count_since(since); total=ping_count+mention_count
            if total==1: flavour="👀 The countdown begins."
            elif total<5: flavour="😬 He can feel it."
            elif total<10: flavour="📣 Jorge. JORGE."
            elif total<20: flavour="🗳️ At this point it's a campaign."
            elif total<50: flavour="🚨 This is getting out of hand."
            else: flavour="☠️ Someone call Jorge. Please."
            await message.channel.send(f"🕵️ **Jorge Tag Counter** — **{total}** signal{'s' if total!=1 else ''} since last sighting {ago_sync(since)}\n*{flavour}*")
    except Exception as e: print(f"Jorge tracker error: {e}")

    # Keyword alerts
    try:
        keywords=get_cached_keywords(str(message.guild.id))
        if keywords:
            content_lower=message.content.lower(); matched=[kw for kw in keywords if kw in content_lower]
            if matched:
                alert_ch=bot.get_channel(KEYWORD_ALERT_CHANNEL_ID)
                if alert_ch:
                    embed=discord.Embed(title="🚨 Keyword Alert",color=discord.Color.red(),timestamp=datetime.datetime.utcnow())
                    embed.set_author(name=str(message.author),icon_url=str(message.author.display_avatar.url) if message.author.display_avatar else discord.Embed.Empty)
                    embed.add_field(name="Keywords Matched",value=", ".join(f"`{k}`" for k in matched),inline=False)
                    embed.add_field(name="Message",value=message.content[:1000] or "*(empty)*",inline=False)
                    embed.add_field(name="Channel",value=message.channel.mention,inline=True)
                    embed.add_field(name="Jump To",value=f"[Click here]({message.jump_url})",inline=True)
                    await alert_ch.send(embed=embed)
    except Exception as e: print(f"Keyword alert error: {e}")

    await bot.process_commands(message)  # kept for safety — no prefix commands active


@bot.tree.command(name="backfill", description="Backfill message history into the database (Admin only)")
@app_commands.describe(days="How many days back to scan (1–365, default 30)")
@app_commands.default_permissions(administrator=True)
async def backfill(interaction: discord.Interaction, days: int = 30):
    if days < 1 or days > 365:
        await interaction.response.send_message("❌ Please specify between 1 and 365 days.", ephemeral=True)
        return
    await interaction.response.send_message(f"🔍 Starting backfill for the last **{days} days**... this may take a while.")
    cutoff       = datetime.datetime.utcnow() - datetime.timedelta(days=days)
    total_msgs   = total_chans = skipped = 0
    guild        = interaction.guild
    for channel in guild.text_channels:
        if not channel.permissions_for(guild.me).read_message_history:
            skipped += 1; continue
        chan_count = 0
        try:
            async for message in channel.history(limit=None, after=cutoff, oldest_first=True):
                if message.author.bot: continue
                ts = message.created_at.replace(tzinfo=None)
                av = str(message.author.display_avatar.url) if message.author.display_avatar else ""
                try:
                    execute("INSERT INTO messages (user_id,username,channel_id,channel_name,guild_id,hour,day_of_week,timestamp) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                            (str(message.author.id), str(message.author), str(channel.id), channel.name, str(guild.id), ts.hour, ts.weekday(), ts))
                    execute("INSERT INTO user_stats (user_id,username,guild_id,msg_count,last_seen,first_seen,avatar_url) VALUES (%s,%s,%s,1,%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET username=EXCLUDED.username,msg_count=user_stats.msg_count+1,last_seen=GREATEST(user_stats.last_seen,EXCLUDED.last_seen),first_seen=LEAST(user_stats.first_seen,EXCLUDED.first_seen),avatar_url=EXCLUDED.avatar_url",
                            (str(message.author.id), str(message.author), str(guild.id), ts, ts, av))
                    chan_count += 1; total_msgs += 1
                except Exception as e:
                    print(f"Backfill DB error: {e}")
            if chan_count > 0: total_chans += 1
        except discord.Forbidden:
            skipped += 1
        except Exception as e:
            print(f"Backfill channel error ({channel.name}): {e}")
    await interaction.edit_original_response(content=(
        f"✅ **Backfill complete!**\n"
        f"📨 **{total_msgs:,}** messages · 📣 **{total_chans}** channels · 🔒 **{skipped}** skipped\n"
        f"Refresh your dashboard!"
    ))


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────
if __name__=="__main__":
    init_db()
    flask_thread=threading.Thread(target=lambda:api.run(host="0.0.0.0",port=FLASK_PORT,debug=False,use_reloader=False),daemon=True)
    flask_thread.start()
    bot.run(BOT_TOKEN)
