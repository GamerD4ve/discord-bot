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
import hmac
import hashlib
from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS

# ─── CONFIG ──────────────────────────────────────────────────────────────────
BOT_TOKEN    = os.environ.get("BOT_TOKEN", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
FLASK_PORT   = int(os.environ.get("PORT", 8080))
DASHBOARD    = "."
FW_SECRET         = os.environ.get("FW_SECRET", "")
FW_ORDER_CHANNEL  = int(os.environ.get("FW_ORDER_CHANNEL", 0) or 0)
FW_GIFT_CHANNEL   = int(os.environ.get("FW_GIFT_CHANNEL", 0) or 0)
# ─────────────────────────────────────────────────────────────────────────────

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
                CREATE INDEX IF NOT EXISTS idx_user      ON messages(user_id);
                CREATE INDEX IF NOT EXISTS idx_channel   ON messages(channel_id);
                CREATE INDEX IF NOT EXISTS idx_ts        ON messages(timestamp);
                CREATE INDEX IF NOT EXISTS idx_hour      ON messages(hour);
                CREATE INDEX IF NOT EXISTS idx_dow       ON messages(day_of_week);
                CREATE INDEX IF NOT EXISTS idx_order_ts  ON orders(timestamp);
                CREATE INDEX IF NOT EXISTS idx_order_type ON orders(event_type);
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
        return jsonify({"online": 0})

@api.route("/api/leaderboard")
def leaderboard():
    try:
        rows = fetchall("""
            SELECT u.user_id, u.username, u.msg_count, u.last_seen, u.first_seen, u.avatar_url,
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
                days_since = max(1, (datetime.datetime.utcnow() - r["first_seen"].replace(tzinfo=None)).days)
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
        rows = fetchall("""
            SELECT
                SUM(CASE WHEN timestamp >= NOW() - INTERVAL '30 days' THEN 1 ELSE 0 END) as period_a,
                SUM(CASE WHEN timestamp >= NOW() - INTERVAL '60 days'
                          AND timestamp < NOW() - INTERVAL '30 days' THEN 1 ELSE 0 END) as period_b
            FROM messages
        """)
        r = rows[0] if rows else {}
        a = r.get("period_a") or 0
        b = r.get("period_b") or 0
        change  = round(((a - b) / b * 100) if b > 0 else 0, 1)
        daily_a = fetchall("SELECT DATE(timestamp) as day, COUNT(*) as cnt FROM messages WHERE timestamp >= NOW() - INTERVAL '30 days' GROUP BY day ORDER BY day")
        daily_b = fetchall("SELECT DATE(timestamp) as day, COUNT(*) as cnt FROM messages WHERE timestamp >= NOW() - INTERVAL '60 days' AND timestamp < NOW() - INTERVAL '30 days' GROUP BY day ORDER BY day")
        return jsonify({
            "period_a_total": a, "period_b_total": b, "change_pct": change,
            "daily_a": [{"day": str(r["day"]), "cnt": r["cnt"]} for r in daily_a],
            "daily_b": [{"day": str(r["day"]), "cnt": r["cnt"]} for r in daily_b],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/loyalty")
def loyalty():
    try:
        rows     = fetchall("SELECT u.user_id, u.msg_count FROM user_stats u")
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
            GROUP BY u.user_id, u.username, u.msg_count, u.first_seen
            ORDER BY u.msg_count DESC LIMIT 5
        """)
        result = []
        for r in rows:
            try:
                days_since = max(1, (datetime.datetime.utcnow() - r["first_seen"].replace(tzinfo=None)).days)
            except:
                days_since = 1
            result.append({
                "username":    r["username"].split("#")[0],
                "volume":      min(100, round((r["msg_count"] or 0) / 10)),
                "consistency": min(100, round(((r["active_days"] or 0) / days_since) * 100)),
                "recency":     min(100, (r["recent_msgs"] or 0) * 10),
            })
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/timeline")
def timeline():
    try:
        top = fetchall("SELECT user_id, username FROM user_stats ORDER BY msg_count DESC LIMIT 10")
        result = []
        for u in top:
            days = fetchall("""
                SELECT DATE(timestamp) as day, COUNT(*) as cnt FROM messages
                WHERE user_id = %s AND timestamp >= NOW() - INTERVAL '30 days'
                GROUP BY day ORDER BY day
            """, (u["user_id"],))
            result.append({
                "username": u["username"].split("#")[0],
                "days": [{"day": str(d["day"]), "cnt": d["cnt"]} for d in days]
            })
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
            consistency = min(100, ((r["active_days"] or 0) / days_since) * 100)
            recency     = min(100, (r["recent_msgs"] or 0) * 10)
            volume      = min(100, (r["msg_count"] or 0) / 10)
            score       = round(consistency * 0.4 + recency * 0.35 + volume * 0.25)
            result.append({
                "username":         r["username"].split("#")[0],
                "avatar_url":       r["avatar_url"],
                "engagement_score": score,
                "last_seen":        r["last_seen"].isoformat() if r["last_seen"] else None,
            })
        result.sort(key=lambda x: x["engagement_score"], reverse=True)
        return jsonify(result[:15])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/daterange")
def daterange():
    try:
        start     = request.args.get("start")
        end       = request.args.get("end")
        if not start or not end:
            return jsonify({"error": "start and end required"}), 400
        total     = fetchone("SELECT COUNT(*) as cnt FROM messages WHERE timestamp >= %s AND timestamp <= %s", (start, end))
        users     = fetchone("SELECT COUNT(DISTINCT user_id) as cnt FROM messages WHERE timestamp >= %s AND timestamp <= %s", (start, end))
        daily     = fetchall("SELECT DATE(timestamp) as day, COUNT(*) as cnt FROM messages WHERE timestamp >= %s AND timestamp <= %s GROUP BY day ORDER BY day", (start, end))
        top_users = fetchall("SELECT username, COUNT(*) as cnt FROM messages WHERE timestamp >= %s AND timestamp <= %s GROUP BY username ORDER BY cnt DESC LIMIT 10", (start, end))
        top_chans = fetchall("SELECT channel_name, COUNT(*) as cnt FROM messages WHERE timestamp >= %s AND timestamp <= %s GROUP BY channel_name ORDER BY cnt DESC LIMIT 8", (start, end))
        peak_hour = fetchone("SELECT hour, COUNT(*) as cnt FROM messages WHERE timestamp >= %s AND timestamp <= %s GROUP BY hour ORDER BY cnt DESC LIMIT 1", (start, end))
        return jsonify({
            "total_messages": total["cnt"] if total else 0,
            "unique_users":   users["cnt"] if users else 0,
            "peak_hour":      peak_hour["hour"] if peak_hour else None,
            "daily":          [{"day": str(r["day"]), "cnt": r["cnt"]} for r in daily],
            "top_users":      [dict(r) for r in top_users],
            "top_channels":   [dict(r) for r in top_chans],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─── FOURTHWALL API ROUTES ────────────────────────────────────────────────────
@api.route("/api/fw/overview")
def fw_overview():
    try:
        total_orders  = fetchone("SELECT COUNT(*) as c FROM orders WHERE event_type = 'order.placed'")["c"]
        total_revenue = fetchone("SELECT COALESCE(SUM(total_amount),0) as r FROM orders WHERE event_type = 'order.placed'")["r"]
        total_gifts   = fetchone("SELECT COUNT(*) as c FROM orders WHERE event_type = 'gift.purchased'")["c"]
        total_subs    = fetchone("SELECT COUNT(*) as c FROM orders WHERE event_type = 'subscription.purchased'")["c"]
        last_order    = fetchone("SELECT buyer_name, product_name, total_amount, timestamp FROM orders ORDER BY timestamp DESC LIMIT 1")
        return jsonify({
            "total_orders":  total_orders,
            "total_revenue": float(total_revenue),
            "total_gifts":   total_gifts,
            "total_subs":    total_subs,
            "last_order":    {
                "buyer_name":   last_order["buyer_name"],
                "product_name": last_order["product_name"],
                "total_amount": float(last_order["total_amount"]) if last_order["total_amount"] else 0,
                "timestamp":    last_order["timestamp"].isoformat(),
            } if last_order else None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/fw/revenue")
def fw_revenue():
    try:
        rows = fetchall("""
            SELECT DATE(timestamp) as day, SUM(total_amount) as revenue, COUNT(*) as orders
            FROM orders WHERE event_type = 'order.placed'
            GROUP BY day ORDER BY day DESC LIMIT 30
        """)
        return jsonify(list(reversed([
            {"day": str(r["day"]), "revenue": float(r["revenue"]), "orders": r["orders"]}
            for r in rows
        ])))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api.route("/api/fw/products")
def fw_products():
    try:
        rows = fetchall("""
            SELECT product_name, COUNT(*) as cnt, SUM(total_amount) as revenue
            FROM orders WHERE event_type = 'order.placed' AND product_name IS NOT NULL
            GROUP BY product_name ORDER BY cnt DESC LIMIT 10
        """)
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

    # Verify signature — log mismatch but allow through for now
    if FW_SECRET:
        sig      = request.headers.get("X-Fourthwall-Signature", "")
        expected = "sha256=" + hmac.new(FW_SECRET.encode(), body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            print(f"⚠️ FW Signature mismatch. Got: {sig} | Expected: {expected}")

    event_type   = data.get("type", "")
    payload      = data.get("data", {})
    buyer_name   = payload.get("buyerName") or payload.get("email", "Someone")
    total_raw    = payload.get("totalAmount") or payload.get("amount") or 0
    try:
        total = float(str(total_raw).replace("$", "").replace(",", ""))
    except:
        total = 0.0
    total_fmt    = payload.get("totalFormatted") or f"${total:.2f}"
    status       = payload.get("status", "Processing")
    items        = payload.get("lineItems", [])
    product_name = items[0].get("productName", "Item") if items else payload.get("productName", "Item")
    if len(items) > 1:
        product_name += f" + {len(items)-1} more"
    order_id     = payload.get("id") or payload.get("orderId") or str(datetime.datetime.utcnow().timestamp())

    # Store in database
    try:
        execute("""
            INSERT INTO orders (order_id, event_type, buyer_name, buyer_email, product_name, total_amount, status, raw)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (order_id) DO NOTHING
        """, (order_id, event_type, buyer_name,
              payload.get("email", ""), product_name,
              total, status, psycopg2.extras.Json(payload)))
        print(f"✅ FW Order saved: {order_id}")
    except Exception as e:
        print(f"⚠️ FW DB error: {e}")

    # Send Discord alert
    import asyncio
    if event_type == "order.placed":
        msg = (
            f"🛒 **New Order — The Conspiracy Podcast Store!**\n"
            f"**{buyer_name}** just ordered **{product_name}**\n"
            f"💰 Total: **{total_fmt}** · 📦 {status}\n"
            f"Thanks for supporting the podcast! 🎙️"
        )
        asyncio.run_coroutine_threadsafe(_send_fw_alert(msg, FW_ORDER_CHANNEL), bot.loop)
    elif event_type == "gift.purchased":
        msg = (
            f"🎁 **Gift Purchase!**\n"
            f"**{buyer_name}** gifted **{product_name}**\n"
            f"💰 Value: **{total_fmt}**\n"
            f"What a legend! 🙌"
        )
        asyncio.run_coroutine_threadsafe(_send_fw_alert(msg, FW_GIFT_CHANNEL or FW_ORDER_CHANNEL), bot.loop)
    elif event_type == "subscription.purchased":
        msg = (
            f"🔔 **New Subscription!**\n"
            f"**{buyer_name}** just subscribed to **{product_name}**\n"
            f"💰 **{total_fmt}**\n"
            f"Welcome to the inner circle! 🕵️"
        )
        asyncio.run_coroutine_threadsafe(_send_fw_alert(msg, FW_GIFT_CHANNEL or FW_ORDER_CHANNEL), bot.loop)
    else:
        print(f"ℹ️ FW unhandled event type: {event_type}")

    return jsonify({"ok": True}), 200

async def _send_fw_alert(msg, channel_id):
    if not channel_id:
        print("⚠️ FW: No channel ID configured")
        return
    channel = bot.get_channel(channel_id)
    if channel:
        await channel.send(msg)
        print(f"✅ FW alert sent to channel {channel_id}")
    else:
        print(f"⚠️ FW: Could not find channel {channel_id}")

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
    print(f"📣  FW Order Channel: {FW_ORDER_CHANNEL}")
    print(f"🎁  FW Gift Channel:  {FW_GIFT_CHANNEL}")

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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
        """, (str(message.author.id), str(message.author),
              str(message.channel.id), message.channel.name,
              str(message.guild.id), hour, day_of_week, now))
        execute("""
            INSERT INTO user_stats (user_id, username, guild_id, msg_count, last_seen, first_seen, avatar_url)
            VALUES (%s, %s, %s, 1, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                username   = EXCLUDED.username,
                msg_count  = user_stats.msg_count + 1,
                last_seen  = GREATEST(user_stats.last_seen, EXCLUDED.last_seen),
                avatar_url = EXCLUDED.avatar_url
        """, (str(message.author.id), str(message.author),
              str(message.guild.id), now, now, avatar))
    except Exception as e:
        print(f"DB error: {e}")
    await bot.process_commands(message)

@bot.command(name="backfill")
@commands.has_permissions(administrator=True)
async def backfill(ctx, days: int = 30):
    if days < 1 or days > 365:
        await ctx.send("❌ Please specify between 1 and 365 days.")
        return
    cutoff     = datetime.datetime.utcnow() - datetime.timedelta(days=days)
    status_msg = await ctx.send(f"🔍 Starting backfill for the last **{days} days**...")
    total_msgs = total_chans = skipped = 0
    for channel in ctx.guild.text_channels:
        if not channel.permissions_for(ctx.guild.me).read_message_history:
            skipped += 1
            continue
        chan_count = 0
        try:
            async for message in channel.history(limit=None, after=cutoff, oldest_first=True):
                if message.author.bot:
                    continue
                ts = message.created_at.replace(tzinfo=None)
                av = str(message.author.display_avatar.url) if message.author.display_avatar else ""
                try:
                    execute("""
                        INSERT INTO messages (user_id, username, channel_id, channel_name, guild_id, hour, day_of_week, timestamp)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
                    """, (str(message.author.id), str(message.author), str(channel.id),
                          channel.name, str(ctx.guild.id), ts.hour, ts.weekday(), ts))
                    execute("""
                        INSERT INTO user_stats (user_id, username, guild_id, msg_count, last_seen, first_seen, avatar_url)
                        VALUES (%s,%s,%s,1,%s,%s,%s)
                        ON CONFLICT (user_id) DO UPDATE SET
                            username=EXCLUDED.username,
                            msg_count=user_stats.msg_count+1,
                            last_seen=GREATEST(user_stats.last_seen,EXCLUDED.last_seen),
                            first_seen=LEAST(user_stats.first_seen,EXCLUDED.first_seen),
                            avatar_url=EXCLUDED.avatar_url
                    """, (str(message.author.id), str(message.author),
                          str(ctx.guild.id), ts, ts, av))
                    chan_count += 1
                    total_msgs += 1
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
        f"📨 **{total_msgs:,}** messages · 📣 **{total_chans}** channels · 🔒 **{skipped}** skipped\n"
        f"Refresh your dashboard!"
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
