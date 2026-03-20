"""
Discord Activity Tracker — Bot + Dashboard API
================================================
Requirements:
    pip install discord.py flask flask-cors

Setup:
    1. Go to https://discord.com/developers/applications
    2. Create an app → Bot → copy the Token
    3. Under "Privileged Gateway Intents", enable:
         - PRESENCE INTENT
         - SERVER MEMBERS INTENT
         - MESSAGE CONTENT INTENT
    4. Paste your bot token below
    5. Run:  python bot.py
    6. Open: http://localhost:5000  in your browser

Bot invite URL (replace CLIENT_ID with your app's Client ID):
    https://discord.com/oauth2/authorize?client_id=CLIENT_ID&permissions=274877908992&scope=bot
"""

import discord
from discord.ext import commands
import sqlite3
import datetime
import threading
import os
import hmac, hashlib
from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS

# ─── CONFIG ──────────────────────────────────────────────────────────────────
BOT_TOKEN   = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
DB_FILE     = "discord_stats.db"
DASHBOARD   = "."          # folder containing dashboard.html
FLASK_PORT  = int(os.environ.get("PORT", 5000))

# ── Fourthwall ───────────────────────────────────────────────────────────────
# 1. In Fourthwall: Settings → Webhooks → Add Endpoint
#    URL: http://YOUR_PC_IP:5000/webhook/fourthwall
#    (or use ngrok - see instructions below)
# 2. Copy the signing secret Fourthwall gives you and paste it below
# 3. Set the Discord channel ID where order alerts should be posted
#    (Right-click a channel in Discord → Copy Channel ID — needs Developer Mode on)
FW_SECRET          = "YOUR_FOURTHWALL_SIGNING_SECRET"  # or "" to skip verification
FW_DISCORD_CHANNEL = 123456789012345678                # channel ID as a number

ORDER_MESSAGE = """🛒 **New Order on The Conspiracy Podcast Store!**

**{buyer}** just ordered **{product}**
💰 Total: **{total}**
📦 Status: {status}

Thanks for supporting the podcast! 🎙️"""
# ─────────────────────────────────────────────────────────────────────────────

# ─── FLASK API ────────────────────────────────────────────────────────────────
api = Flask(__name__, static_folder=DASHBOARD)
CORS(api)

def get_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

@api.route("/")
def index():
    return send_from_directory(DASHBOARD, "dashboard.html")

@api.route("/api/overview")
def overview():
    with get_db() as db:
        msgs  = db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        users = db.execute("SELECT COUNT(*) FROM user_stats").fetchone()[0]
        chans = db.execute("SELECT COUNT(DISTINCT channel_id) FROM messages").fetchone()[0]
        day   = db.execute("""
            SELECT DATE(timestamp) as day, COUNT(*) as cnt
            FROM messages GROUP BY day ORDER BY cnt DESC LIMIT 1
        """).fetchone()
    return jsonify({
        "total_messages":       msgs,
        "total_users":          users,
        "total_channels":       chans,
        "most_active_day":      day["day"] if day else None,
        "most_active_day_count":day["cnt"] if day else 0,
    })

@api.route("/api/leaderboard")
def leaderboard():
    with get_db() as db:
        rows = db.execute("""
            SELECT user_id, username, msg_count, last_seen, first_seen
            FROM user_stats ORDER BY msg_count DESC LIMIT 25
        """).fetchall()
    return jsonify([dict(r) for r in rows])

@api.route("/api/channels")
def channels():
    with get_db() as db:
        rows = db.execute("""
            SELECT channel_name, COUNT(*) as cnt
            FROM messages GROUP BY channel_id ORDER BY cnt DESC LIMIT 15
        """).fetchall()
    return jsonify([dict(r) for r in rows])

@api.route("/api/heatmap")
def heatmap():
    with get_db() as db:
        rows = db.execute(
            "SELECT hour, COUNT(*) as cnt FROM messages GROUP BY hour"
        ).fetchall()
    counts = {r["hour"]: r["cnt"] for r in rows}
    return jsonify([{"hour": h, "count": counts.get(h, 0)} for h in range(24)])

@api.route("/api/daily")
def daily():
    with get_db() as db:
        rows = db.execute("""
            SELECT DATE(timestamp) as day, COUNT(*) as cnt
            FROM messages GROUP BY day ORDER BY day DESC LIMIT 30
        """).fetchall()
    return jsonify(list(reversed([dict(r) for r in rows])))

@api.route("/webhook/fourthwall", methods=["POST"])
def fourthwall_webhook():
    # ── Verify signature (if secret is set) ──────────────────────────────────
    if FW_SECRET:
        sig   = request.headers.get("X-Fourthwall-Signature", "")
        body  = request.get_data()
        expected = "sha256=" + hmac.new(
            FW_SECRET.encode(), body, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return jsonify({"error": "Invalid signature"}), 401

    data  = request.get_json(silent=True) or {}
    event = data.get("type", "")

    if event == "order.placed":
        order   = data.get("data", {})
        buyer   = order.get("buyerName") or order.get("email", "Someone")
        total   = order.get("totalFormatted") or f"${order.get('totalAmount', '?')}"
        status  = order.get("status", "Processing")
        items   = order.get("lineItems", [])
        product = items[0].get("productName", "an item") if items else "an item"
        if len(items) > 1:
            product += f" + {len(items)-1} more"

        msg = ORDER_MESSAGE.format(
            buyer=buyer, product=product, total=total, status=status
        )
        # Schedule the Discord message from the bot thread
        import asyncio
        asyncio.run_coroutine_threadsafe(_send_order_alert(msg), bot.loop)

    return jsonify({"ok": True}), 200

async def _send_order_alert(msg):
    channel = bot.get_channel(FW_DISCORD_CHANNEL)
    if channel:
        await channel.send(msg)
    else:
        print(f"⚠️  Fourthwall: couldn't find channel {FW_DISCORD_CHANNEL}")

@api.route("/api/lastseen")
def lastseen():
    with get_db() as db:
        rows = db.execute("""
            SELECT username, msg_count, last_seen, first_seen
            FROM user_stats ORDER BY last_seen DESC LIMIT 25
        """).fetchall()
    return jsonify([dict(r) for r in rows])

# ─── DISCORD BOT ─────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
intents.members         = True
intents.presences       = True

bot = commands.Bot(command_prefix="!", intents=intents)

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
                timestamp    TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS user_stats (
                user_id    TEXT PRIMARY KEY,
                username   TEXT NOT NULL,
                guild_id   TEXT NOT NULL,
                msg_count  INTEGER DEFAULT 0,
                last_seen  TEXT,
                first_seen TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_user    ON messages(user_id);
            CREATE INDEX IF NOT EXISTS idx_channel ON messages(channel_id);
            CREATE INDEX IF NOT EXISTS idx_ts      ON messages(timestamp);
            CREATE INDEX IF NOT EXISTS idx_hour    ON messages(hour);
        """)

@bot.event
async def on_ready():
    init_db()
    print(f"✅  Logged in as {bot.user}")
    print(f"📊  Dashboard → http://localhost:{FLASK_PORT}")

@bot.event
async def on_message(message):
    if message.author.bot or not message.guild:
        return
    now  = datetime.datetime.utcnow().isoformat()
    hour = datetime.datetime.utcnow().hour
    with get_db() as db:
        db.execute("""
            INSERT INTO messages
                (user_id, username, channel_id, channel_name, guild_id, hour, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (str(message.author.id), str(message.author),
              str(message.channel.id), message.channel.name,
              str(message.guild.id), hour, now))
        db.execute("""
            INSERT INTO user_stats
                (user_id, username, guild_id, msg_count, last_seen, first_seen)
            VALUES (?, ?, ?, 1, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username  = excluded.username,
                msg_count = msg_count + 1,
                last_seen = excluded.last_seen
        """, (str(message.author.id), str(message.author),
              str(message.guild.id), now, now))
    await bot.process_commands(message)

# ─── ENTRY POINT ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    flask_thread = threading.Thread(
        target=lambda: api.run(port=FLASK_PORT, debug=False, use_reloader=False),
        daemon=True
    )
    flask_thread.start()
    bot.run(BOT_TOKEN)