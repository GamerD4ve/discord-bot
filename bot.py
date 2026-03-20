"""
Discord Activity Tracker — Bot + Dashboard API
================================================
Requirements:
    pip install discord.py flask flask-cors psycopg2-binary
"""

import discord
from discord.ext import commands
import psycopg2
import psycopg2.extras
import datetime
import threading
import os
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS

# ─── CONFIG ──────────────────────────────────────────────────────────────────
BOT_TOKEN    = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
DASHBOARD    = "."
FLASK_PORT   = int(os.environ.get("PORT", 8080))
# ─────────────────────────────────────────────────────────────────────────────

api = Flask(__name__, static_folder=DASHBOARD)
CORS(api)

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
                CREATE INDEX IF NOT EXISTS idx_user    ON messages(user_id);
                CREATE INDEX IF NOT EXISTS idx_channel ON messages(channel_id);
                CREATE INDEX IF NOT EXISTS idx_ts      ON messages(timestamp);
                CREATE INDEX IF NOT EXISTS idx_hour    ON messages(hour);
                CREATE INDEX IF NOT EXISTS idx_dow     ON messages(day_of_week);
            """)

# ─── HELPERS ─────────────────────────────────────────────────────────────────

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

# ─── FLASK ROUTES ─────────────────────────────────────────────────────────────

@api.route("/")
def index():
    return send_from_directory(DASHBOARD, "dashboard.html")

@api.route("/logo.png")
def logo():
    return send_from_directory(DASHBOARD, "logo.png")

@api.route("/api/overview")
def overview():
    try:
        msgs  = fetchone("SELECT COUNT(*) as c FROM messages")["c"]
        users = fetchone("SELECT COUNT(*) as c FROM user_stats")["c"]
        chans = fetchone("SELECT COUNT(DISTINCT channel_id) as c FROM messages")["c"]
        day   = fetchone("""
            SELECT DATE(timestamp) as day, COUNT(*) as cnt
            FROM messages GROUP BY day ORDER BY cnt DESC LIMIT 1
        """)
        return jsonify({
            "total_messages":        msgs,
            "total_users":           users,
            "total_channels":        chans,
            "most_active_day":       str(day["day"]) if day else None,
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
        rows = fetchall("""
            SELECT u.user_id, u.username, u.msg_count, u.last_seen,
                   u.first_seen, u.avatar_url,
                   COUNT(DISTINCT DATE(m.timestamp)) as active_days,
                   SUM(CASE WHEN m.timestamp >= NOW() - INTERVAL '7 days' THEN 1 ELSE 0 END) as recent_msgs
            FROM user_stats u
            LEFT JOIN messages m ON u.user_id = m.user_id
            GROUP BY u.user_id, u.username, u.msg_count, u.last_seen, u.first_seen, u.avatar_url
            ORDER BY u.msg_count DESC LIMIT 25
        """)
        result = []
        for r in rows:
            d = dict(r)
            try:
                days_since = max(1, (datetime.datetime.utcnow() -
                    r["first_seen"].replace(tzinfo=None)).days)
            except:
                days_since = 1
            consistency = min(100, ((r["active_days"] or 0) / days_since) * 100)
            recency     = min(100, (r["recent_msgs"] or 0) * 10)
            volume      = min(100, (r["msg_count"] or 0) / 10)
            d["engagement_score"] = round(consistency * 0.4 + recency * 0.35 + volume * 0.25)
            d["last_seen"]  = r["last_seen"].isoformat()  if r["last_seen"]  else None
            d["first_seen"] = r["first_seen"].isoformat() if r["first_seen"] else None
            result.append(d)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/channels")
def channels():
    try:
        rows = fetchall("""
            SELECT channel_name, COUNT(*) as cnt
            FROM messages GROUP BY channel_id, channel_name ORDER BY cnt DESC LIMIT 15
        """)
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/heatmap")
def heatmap():
    try:
        rows = fetchall("SELECT hour, COUNT(*) as cnt FROM messages GROUP BY hour")
        counts = {r["hour"]: r["cnt"] for r in rows}
        return jsonify([{"hour": h, "count": counts.get(h, 0)} for h in range(24)])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/daily")
def daily():
    try:
        rows = fetchall("""
            SELECT DATE(timestamp) as day, COUNT(*) as cnt
            FROM messages GROUP BY day ORDER BY day DESC LIMIT 30
        """)
        return jsonify(list(reversed([
            {"day": str(r["day"]), "cnt": r["cnt"]} for r in rows
        ])))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/dayofweek")
def dayofweek():
    try:
        rows = fetchall("""
            SELECT day_of_week, COUNT(*) as cnt
            FROM messages GROUP BY day_of_week ORDER BY day_of_week
        """)
        days   = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        counts = {r["day_of_week"]: r["cnt"] for r in rows}
        return jsonify([{"day": days[i], "count": counts.get(i, 0)} for i in range(7)])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/newmembers")
def newmembers():
    try:
        rows = fetchall("""
            SELECT TO_CHAR(first_seen, 'IYYY-IW') as week, COUNT(*) as cnt
            FROM user_stats GROUP BY week ORDER BY week DESC LIMIT 12
        """)
        return jsonify(list(reversed([dict(r) for r in rows])))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/compare")
def compare():
    try:
        # Period A = last 30 days, Period B = 30-60 days ago
        rows = fetchall("""
            SELECT
                SUM(CASE WHEN timestamp >= NOW() - INTERVAL '30 days' THEN 1 ELSE 0 END) as period_a,
                SUM(CASE WHEN timestamp >= NOW() - INTERVAL '60 days'
                          AND timestamp <  NOW() - INTERVAL '30 days' THEN 1 ELSE 0 END) as period_b
            FROM messages
        """)
        r = rows[0] if rows else {}
        a = r.get("period_a") or 0
        b = r.get("period_b") or 0
        change = round(((a - b) / b * 100) if b > 0 else 0, 1)

        # Daily breakdown for both periods
        daily_a = fetchall("""
            SELECT DATE(timestamp) as day, COUNT(*) as cnt FROM messages
            WHERE timestamp >= NOW() - INTERVAL '30 days'
            GROUP BY day ORDER BY day
        """)
        daily_b = fetchall("""
            SELECT DATE(timestamp) as day, COUNT(*) as cnt FROM messages
            WHERE timestamp >= NOW() - INTERVAL '60 days'
              AND timestamp <  NOW() - INTERVAL '30 days'
            GROUP BY day ORDER BY day
        """)
        return jsonify({
            "period_a_total":  a,
            "period_b_total":  b,
            "change_pct":      change,
            "period_a_label": "Last 30 days",
            "period_b_label": "Previous 30 days",
            "daily_a": [{"day": str(r["day"]), "cnt": r["cnt"]} for r in daily_a],
            "daily_b": [{"day": str(r["day"]), "cnt": r["cnt"]} for r in daily_b],
        })
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
    init_db()
    print(f"✅  Logged in as {bot.user}")
    print(f"📊  Dashboard → http://localhost:{FLASK_PORT}")

@bot.event
async def on_message(message):
    if message.author.bot or not message.guild:
        return
    now         = datetime.datetime.utcnow()
    hour        = now.hour
    day_of_week = now.weekday()
    avatar      = str(message.author.display_avatar.url) if message.author.display_avatar else ""
    try:
        execute("""
            INSERT INTO messages (user_id, username, channel_id, channel_name, guild_id, hour, day_of_week, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (str(message.author.id), str(message.author),
              str(message.channel.id), message.channel.name,
              str(message.guild.id), hour, day_of_week, now))
        execute("""
            INSERT INTO user_stats (user_id, username, guild_id, msg_count, last_seen, first_seen, avatar_url)
            VALUES (%s, %s, %s, 1, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                username   = EXCLUDED.username,
                msg_count  = user_stats.msg_count + 1,
                last_seen  = EXCLUDED.last_seen,
                avatar_url = EXCLUDED.avatar_url
        """, (str(message.author.id), str(message.author),
              str(message.guild.id), now, now, avatar))
    except Exception as e:
        print(f"DB error: {e}")
    await bot.process_commands(message)

# ─── BACKFILL COMMAND ────────────────────────────────────────────────────────

@bot.command(name="backfill")
@commands.has_permissions(administrator=True)
async def backfill(ctx, days: int = 30):
    """
    !backfill [days]
    Crawls all channels and loads the last X days of history into the database.
    Only usable by server admins. Default is 30 days.
    Example: !backfill 7   (last week)
             !backfill 30  (last month)
             !backfill 90  (last 3 months)
    """
    if days < 1 or days > 365:
        await ctx.send("❌ Please specify between 1 and 365 days.")
        return

    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=days)
    status_msg = await ctx.send(f"🔍 Starting backfill for the last **{days} days**... this may take a while!")

    total_msgs  = 0
    total_chans = 0
    skipped     = 0

    for channel in ctx.guild.text_channels:
        # Skip channels the bot can't read
        if not channel.permissions_for(ctx.guild.me).read_message_history:
            skipped += 1
            continue

        chan_count = 0
        try:
            async for message in channel.history(limit=None, after=cutoff, oldest_first=True):
                if message.author.bot:
                    continue

                ts          = message.created_at.replace(tzinfo=None)
                hour        = ts.hour
                day_of_week = ts.weekday()
                avatar      = str(message.author.display_avatar.url) if message.author.display_avatar else ""

                try:
                    execute("""
                        INSERT INTO messages
                            (user_id, username, channel_id, channel_name, guild_id, hour, day_of_week, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (str(message.author.id), str(message.author),
                          str(channel.id), channel.name,
                          str(ctx.guild.id), hour, day_of_week, ts))

                    execute("""
                        INSERT INTO user_stats
                            (user_id, username, guild_id, msg_count, last_seen, first_seen, avatar_url)
                        VALUES (%s, %s, %s, 1, %s, %s, %s)
                        ON CONFLICT (user_id) DO UPDATE SET
                            username   = EXCLUDED.username,
                            msg_count  = user_stats.msg_count + 1,
                            last_seen  = GREATEST(user_stats.last_seen, EXCLUDED.last_seen),
                            first_seen = LEAST(user_stats.first_seen, EXCLUDED.first_seen),
                            avatar_url = EXCLUDED.avatar_url
                    """, (str(message.author.id), str(message.author),
                          str(ctx.guild.id), ts, ts, avatar))

                    chan_count  += 1
                    total_msgs  += 1
                except Exception as e:
                    print(f"Backfill DB error: {e}")

            if chan_count > 0:
                total_chans += 1

        except discord.Forbidden:
            skipped += 1
        except Exception as e:
            print(f"Backfill channel error ({channel.name}): {e}")

    await status_msg.edit(content=(
        f"✅ **Backfill complete!**\n"
        f"📨 **{total_msgs:,}** messages loaded from the last **{days}** days\n"
        f"📣 **{total_chans}** channels scanned\n"
        f"🔒 **{skipped}** channels skipped (no access)\n\n"
        f"Refresh your dashboard to see the data!"
    ))

# ─── ENTRY POINT ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    flask_thread = threading.Thread(
        target=lambda: api.run(host="0.0.0.0", port=FLASK_PORT, debug=False, use_reloader=False),
        daemon=True
    )
    flask_thread.start()
    bot.run(BOT_TOKEN)
