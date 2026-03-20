"""
Discord Activity Tracker — Bot + Dashboard API
================================================
Requirements:
    pip install discord.py flask flask-cors
"""

import discord
from discord.ext import commands
import sqlite3
import datetime
import threading
import os
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS

# ─── CONFIG ──────────────────────────────────────────────────────────────────
BOT_TOKEN  = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
DB_FILE    = "discord_stats.db"
DASHBOARD  = "."
FLASK_PORT = int(os.environ.get("PORT", 8080))
# ─────────────────────────────────────────────────────────────────────────────

api = Flask(__name__, static_folder=DASHBOARD)
CORS(api)

def get_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS messages (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      TEXT NOT NULL,
                username     TEXT NOT NULL,
                channel_id   TEXT NOT NULL,
                channel_name TEXT NOT NULL,
                guild_id     TEXT NOT NULL,
                hour         INTEGER NOT NULL,
                day_of_week  INTEGER NOT NULL,
                timestamp    TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS user_stats (
                user_id    TEXT PRIMARY KEY,
                username   TEXT NOT NULL,
                guild_id   TEXT NOT NULL,
                msg_count  INTEGER DEFAULT 0,
                last_seen  TEXT,
                first_seen TEXT,
                avatar_url TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_user    ON messages(user_id);
            CREATE INDEX IF NOT EXISTS idx_channel ON messages(channel_id);
            CREATE INDEX IF NOT EXISTS idx_ts      ON messages(timestamp);
            CREATE INDEX IF NOT EXISTS idx_hour    ON messages(hour);
            CREATE INDEX IF NOT EXISTS idx_dow     ON messages(day_of_week);
        """)

# ─── ROUTES ──────────────────────────────────────────────────────────────────

@api.route("/")
def index():
    return send_from_directory(DASHBOARD, "dashboard.html")

@api.route("/api/overview")
def overview():
    try:
        init_db()
        with get_db() as db:
            msgs  = db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            users = db.execute("SELECT COUNT(*) FROM user_stats").fetchone()[0]
            chans = db.execute("SELECT COUNT(DISTINCT channel_id) FROM messages").fetchone()[0]
            day   = db.execute("""
                SELECT DATE(timestamp) as day, COUNT(*) as cnt
                FROM messages GROUP BY day ORDER BY cnt DESC LIMIT 1
            """).fetchone()
        return jsonify({
            "total_messages":        msgs,
            "total_users":           users,
            "total_channels":        chans,
            "most_active_day":       day["day"] if day else None,
            "most_active_day_count": day["cnt"] if day else 0,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/online")
def online():
    try:
        count = sum(
            1 for guild in bot.guilds
            for member in guild.members
            if member.status != discord.Status.offline and not member.bot
        )
        return jsonify({"online": count})
    except Exception as e:
        return jsonify({"online": 0, "error": str(e)})

@api.route("/api/leaderboard")
def leaderboard():
    try:
        with get_db() as db:
            rows = db.execute("""
                SELECT u.user_id, u.username, u.msg_count, u.last_seen,
                       u.first_seen, u.avatar_url,
                       COUNT(DISTINCT DATE(m.timestamp)) as active_days,
                       SUM(CASE WHEN m.timestamp >= datetime('now','-7 days') THEN 1 ELSE 0 END) as recent_msgs
                FROM user_stats u
                LEFT JOIN messages m ON u.user_id = m.user_id
                GROUP BY u.user_id
                ORDER BY u.msg_count DESC LIMIT 25
            """).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            try:
                days_since = max(1, (datetime.datetime.utcnow() -
                    datetime.datetime.fromisoformat(r["first_seen"])).days)
            except:
                days_since = 1
            consistency = min(100, ((r["active_days"] or 0) / days_since) * 100)
            recency     = min(100, (r["recent_msgs"] or 0) * 10)
            volume      = min(100, (r["msg_count"] or 0) / 10)
            d["engagement_score"] = round(consistency * 0.4 + recency * 0.35 + volume * 0.25)
            result.append(d)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/channels")
def channels():
    try:
        with get_db() as db:
            rows = db.execute("""
                SELECT channel_name, COUNT(*) as cnt
                FROM messages GROUP BY channel_id ORDER BY cnt DESC LIMIT 15
            """).fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/heatmap")
def heatmap():
    try:
        with get_db() as db:
            rows = db.execute(
                "SELECT hour, COUNT(*) as cnt FROM messages GROUP BY hour"
            ).fetchall()
        counts = {r["hour"]: r["cnt"] for r in rows}
        return jsonify([{"hour": h, "count": counts.get(h, 0)} for h in range(24)])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/daily")
def daily():
    try:
        with get_db() as db:
            rows = db.execute("""
                SELECT DATE(timestamp) as day, COUNT(*) as cnt
                FROM messages GROUP BY day ORDER BY day DESC LIMIT 30
            """).fetchall()
        return jsonify(list(reversed([dict(r) for r in rows])))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/dayofweek")
def dayofweek():
    try:
        with get_db() as db:
            rows = db.execute("""
                SELECT day_of_week, COUNT(*) as cnt
                FROM messages GROUP BY day_of_week
            """).fetchall()
        days   = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        counts = {r["day_of_week"]: r["cnt"] for r in rows}
        return jsonify([{"day": days[i], "count": counts.get(i, 0)} for i in range(7)])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/newmembers")
def newmembers():
    try:
        with get_db() as db:
            rows = db.execute("""
                SELECT strftime('%Y-W%W', first_seen) as week, COUNT(*) as cnt
                FROM user_stats GROUP BY week ORDER BY week DESC LIMIT 12
            """).fetchall()
        return jsonify(list(reversed([dict(r) for r in rows])))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/lastseen")
def lastseen():
    try:
        with get_db() as db:
            rows = db.execute("""
                SELECT username, msg_count, last_seen, first_seen
                FROM user_stats ORDER BY last_seen DESC LIMIT 25
            """).fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─── DISCORD BOT ─────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
intents.members         = True
intents.presences       = True

bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f"✅  Logged in as {bot.user}")
    print(f"📊  Dashboard → http://localhost:{FLASK_PORT}")

@bot.event
async def on_message(message):
    if message.author.bot or not message.guild:
        return
    now         = datetime.datetime.utcnow().isoformat()
    hour        = datetime.datetime.utcnow().hour
    day_of_week = datetime.datetime.utcnow().weekday()  # 0=Mon, 6=Sun
    avatar      = str(message.author.display_avatar.url) if message.author.display_avatar else ""
    with get_db() as db:
        db.execute("""
            INSERT INTO messages
                (user_id, username, channel_id, channel_name, guild_id, hour, day_of_week, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (str(message.author.id), str(message.author),
              str(message.channel.id), message.channel.name,
              str(message.guild.id), hour, day_of_week, now))
        db.execute("""
            INSERT INTO user_stats
                (user_id, username, guild_id, msg_count, last_seen, first_seen, avatar_url)
            VALUES (?, ?, ?, 1, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username   = excluded.username,
                msg_count  = msg_count + 1,
                last_seen  = excluded.last_seen,
                avatar_url = excluded.avatar_url
        """, (str(message.author.id), str(message.author),
              str(message.guild.id), now, now, avatar))
    await bot.process_commands(message)

# ─── ENTRY POINT ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    flask_thread = threading.Thread(
        target=lambda: api.run(host="0.0.0.0", port=FLASK_PORT, debug=False, use_reloader=False),
        daemon=True
    )
    flask_thread.start()
    bot.run(BOT_TOKEN)
