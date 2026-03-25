"""
Discord Activity Tracker — Bot + Dashboard API + Giveaway System
=================================================================
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
import random as _random
import re as _re
import asyncio
from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS

# ─── CONFIG ──────────────────────────────────────────────────────────────────
BOT_TOKEN         = os.environ.get("BOT_TOKEN", "")
DATABASE_URL      = os.environ.get("DATABASE_URL", "")
FLASK_PORT        = int(os.environ.get("PORT", 8080))
DASHBOARD         = "."
FW_SECRET         = os.environ.get("FW_SECRET", "")
FW_ORDER_CHANNEL  = int(os.environ.get("FW_ORDER_CHANNEL", 0) or 0)
FW_GIFT_CHANNEL   = int(os.environ.get("FW_GIFT_CHANNEL", 0) or 0)

# Roles that can START / END / LIST / VIEW giveaways (Admins always bypass)
GIVEAWAY_MANAGER_ROLE_IDS = {1166453168121581579, 1166454283630301375}

# Role required to click Enter on a giveaway
GIVEAWAY_ENTRY_ROLE_ID = 1166481664176828496
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
                CREATE INDEX IF NOT EXISTS idx_user             ON messages(user_id);
                CREATE INDEX IF NOT EXISTS idx_channel          ON messages(channel_id);
                CREATE INDEX IF NOT EXISTS idx_ts               ON messages(timestamp);
                CREATE INDEX IF NOT EXISTS idx_hour             ON messages(hour);
                CREATE INDEX IF NOT EXISTS idx_dow              ON messages(day_of_week);
                CREATE INDEX IF NOT EXISTS idx_order_ts         ON orders(timestamp);
                CREATE INDEX IF NOT EXISTS idx_order_type       ON orders(event_type);
                CREATE INDEX IF NOT EXISTS idx_giveaway_active  ON giveaways(ended, ends_at);
                CREATE INDEX IF NOT EXISTS idx_giveaway_entries ON giveaway_entries(giveaway_id);
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
        start = request.args.get("start")
        end   = request.args.get("end")
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

# ─── GIVEAWAY DASHBOARD API ROUTES ───────────────────────────────────────────
@api.route("/api/giveaways")
def api_giveaways():
    """All giveaways (active + ended), newest first."""
    try:
        rows = fetchall("""
            SELECT g.id, g.prize, g.description, g.host_id, g.winner_mode,
                   g.winner_count, g.ends_at, g.ended, g.winner_ids,
                   g.created_at, g.channel_id,
                   COUNT(e.id) as entry_count
            FROM giveaways g
            LEFT JOIN giveaway_entries e ON g.id = e.giveaway_id
            GROUP BY g.id
            ORDER BY g.created_at DESC
            LIMIT 50
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
    """Full entrant list for a single giveaway."""
    try:
        gw = fetchone("SELECT * FROM giveaways WHERE id = %s", (giveaway_id,))
        if not gw:
            return jsonify({"error": "Giveaway not found"}), 404
        entries = fetchall("""
            SELECT user_id, username, avatar_url, entered_at
            FROM giveaway_entries WHERE giveaway_id = %s ORDER BY entered_at
        """, (giveaway_id,))
        return jsonify({
            "giveaway": {
                "id":           gw["id"],
                "prize":        gw["prize"],
                "description":  gw["description"],
                "winner_mode":  gw["winner_mode"],
                "winner_count": gw["winner_count"],
                "ended":        gw["ended"],
                "ends_at":      gw["ends_at"].isoformat() if gw["ends_at"] else None,
                "winner_ids":   gw["winner_ids"],
                "entry_count":  len(entries),
            },
            "entries": [
                {
                    "user_id":    e["user_id"],
                    "username":   e["username"],
                    "avatar_url": e["avatar_url"],
                    "entered_at": e["entered_at"].isoformat(),
                }
                for e in entries
            ],
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
            "last_order": {
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
    order_id = payload.get("id") or payload.get("orderId") or str(datetime.datetime.utcnow().timestamp())

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

# ═══════════════════════════════════════════════════════════════════════════════
# GIVEAWAY SYSTEM
# ═══════════════════════════════════════════════════════════════════════════════

def parse_duration(text: str):
    """Parse '30m', '2h', '1d', '1h30m' etc. Returns a timedelta or None."""
    m = _re.fullmatch(r"(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?", text.strip().lower())
    if not m or not any(m.groups()):
        return None
    td = datetime.timedelta(
        days=int(m.group(1) or 0),
        hours=int(m.group(2) or 0),
        minutes=int(m.group(3) or 0),
        seconds=int(m.group(4) or 0),
    )
    return td if td.total_seconds() > 0 else None


def format_remaining(ends_at) -> str:
    """Human-friendly countdown string."""
    remaining = ends_at.replace(tzinfo=None) - datetime.datetime.utcnow()
    if remaining.total_seconds() <= 0:
        return "**ENDED**"
    total = int(remaining.total_seconds())
    d, h, m, s = total // 86400, (total % 86400) // 3600, (total % 3600) // 60, total % 60
    parts = []
    if d: parts.append(f"{d}d")
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    if not d and not h: parts.append(f"{s}s")
    return " ".join(parts) or "< 1s"


def build_giveaway_embed(gw, entry_count: int, ended: bool = False) -> discord.Embed:
    color = discord.Color.gold() if not ended else discord.Color.light_grey()
    embed = discord.Embed(
        title=f"🎉  GIVEAWAY: {gw['prize']}",
        description=gw.get("description") or "",
        color=color,
    )
    embed.add_field(
        name="⏰ Time Remaining" if not ended else "⏰ Status",
        value=format_remaining(gw["ends_at"]) if not ended else "**ENDED**",
        inline=True,
    )
    embed.add_field(name="🏆 Winners",  value=str(gw["winner_count"]), inline=True)
    embed.add_field(name="🎟️ Entries", value=str(entry_count),         inline=True)
    mode_labels = {
        "announce": "📢 Announced in channel",
        "dm_host":  "🔒 DM'd to host only",
        "manual":   "✋ Host picks manually",
    }
    embed.add_field(name="🎯 Winner Selection", value=mode_labels.get(gw["winner_mode"], "Random"), inline=True)
    embed.set_footer(text=f"Giveaway ID: {gw['id']}  •  You must have the required role to enter.")
    return embed


def member_can_manage(member: discord.Member) -> bool:
    """True if the member is an admin or has one of the two manager role IDs."""
    if member.guild_permissions.administrator:
        return True
    return bool({r.id for r in member.roles} & GIVEAWAY_MANAGER_ROLE_IDS)


def member_can_enter(member: discord.Member) -> bool:
    """True if the member holds the entry role."""
    return any(r.id == GIVEAWAY_ENTRY_ROLE_ID for r in member.roles)


# ─── PERSISTENT BUTTON VIEW ──────────────────────────────────────────────────

class GiveawayView(discord.ui.View):

    def __init__(self, giveaway_id: int):
        super().__init__(timeout=None)
        self.giveaway_id = giveaway_id
        btn = discord.ui.Button(
            label="🎉  Enter Giveaway",
            style=discord.ButtonStyle.green,
            custom_id=f"giveaway_enter_{giveaway_id}",
        )
        btn.callback = self._enter_callback
        self.add_item(btn)

    async def _enter_callback(self, interaction: discord.Interaction):
        gw = fetchone("SELECT * FROM giveaways WHERE id = %s", (self.giveaway_id,))

        if not gw or gw["ended"] or datetime.datetime.utcnow() > gw["ends_at"].replace(tzinfo=None):
            await interaction.response.send_message("❌ This giveaway has already ended!", ephemeral=True)
            return

        if not member_can_enter(interaction.user):
            await interaction.response.send_message(
                "❌ You don't have the required role to enter this giveaway.", ephemeral=True
            )
            return

        avatar = str(interaction.user.display_avatar.url) if interaction.user.display_avatar else ""
        try:
            execute(
                "INSERT INTO giveaway_entries (giveaway_id, user_id, username, avatar_url) VALUES (%s, %s, %s, %s)",
                (self.giveaway_id, str(interaction.user.id), str(interaction.user), avatar),
            )
            count = fetchone(
                "SELECT COUNT(*) as c FROM giveaway_entries WHERE giveaway_id = %s", (self.giveaway_id,)
            )["c"]
            await interaction.response.send_message(
                f"✅ You're in! Good luck 🍀\n*{count} total {'entry' if count == 1 else 'entries'}*",
                ephemeral=True,
            )
        except Exception as e:
            if "unique" in str(e).lower():
                await interaction.response.send_message("⚠️ You've already entered this giveaway!", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Something went wrong — please try again.", ephemeral=True)


# ─── END GIVEAWAY ────────────────────────────────────────────────────────────

async def end_giveaway(giveaway_id: int):
    gw = fetchone("SELECT * FROM giveaways WHERE id = %s", (giveaway_id,))
    if not gw or gw["ended"]:
        return

    entries     = fetchall(
        "SELECT user_id, username FROM giveaway_entries WHERE giveaway_id = %s ORDER BY entered_at",
        (giveaway_id,),
    )
    entry_count = len(entries)
    picks, winner_ids, winner_mentions = [], [], []

    if entries and gw["winner_mode"] != "manual":
        picks           = _random.sample(entries, min(gw["winner_count"], len(entries)))
        winner_ids      = [p["user_id"] for p in picks]
        winner_mentions = [f"<@{p['user_id']}>" for p in picks]

    execute("UPDATE giveaways SET ended = TRUE, winner_ids = %s WHERE id = %s", (winner_ids, giveaway_id))

    gw_final = fetchone("SELECT * FROM giveaways WHERE id = %s", (giveaway_id,))
    channel  = bot.get_channel(int(gw["channel_id"]))

    # Update the embed
    if channel and gw["message_id"]:
        try:
            msg         = await channel.fetch_message(int(gw["message_id"]))
            ended_embed = build_giveaway_embed(gw_final, entry_count, ended=True)
            if winner_ids:
                ended_embed.add_field(name="🏆 Winner(s)", value="\n".join(winner_mentions), inline=False)
            elif gw["winner_mode"] == "manual":
                ended_embed.add_field(name="🏆 Winner(s)", value="Host is selecting manually…", inline=False)
            else:
                ended_embed.add_field(name="🏆 Winner(s)", value="No entries — no winner!", inline=False)
            ended_view = discord.ui.View()
            ended_view.add_item(discord.ui.Button(
                label="Giveaway Ended", style=discord.ButtonStyle.grey,
                disabled=True, custom_id=f"giveaway_ended_{giveaway_id}",
            ))
            await msg.edit(embed=ended_embed, view=ended_view)
        except Exception as e:
            print(f"⚠️ Could not update giveaway embed #{giveaway_id}: {e}")

    # Channel announcement
    if channel:
        if gw["winner_mode"] == "announce" and winner_ids:
            await channel.send(
                f"🎉 **Giveaway ended!**\n"
                f"Congratulations to {', '.join(winner_mentions)}! "
                f"You won **{gw['prize']}**! 🏆\n"
                f"Please contact a moderator to claim your prize."
            )
        elif gw["winner_mode"] == "manual":
            await channel.send(
                f"🎉 **The '{gw['prize']}' giveaway has ended!**\n"
                f"The host will announce the winner(s) shortly. Stay tuned!"
            )
        elif gw["winner_mode"] == "dm_host" and winner_ids:
            await channel.send(
                f"🎉 **The '{gw['prize']}' giveaway has ended!**\n"
                f"The winner has been notified privately. 🔒"
            )

    # DM host (dm_host mode)
    if gw["winner_mode"] == "dm_host" and winner_ids:
        try:
            guild = bot.get_guild(int(gw["guild_id"]))
            host  = guild.get_member(int(gw["host_id"])) if guild else None
            if host:
                winners_list = "\n".join(f"• {p['username']}  (ID: {p['user_id']})" for p in picks)
                await host.send(
                    f"🎉 **Giveaway ended — {gw['prize']}**\n\n"
                    f"**Winner(s) ({len(picks)}/{gw['winner_count']}):**\n{winners_list}\n\n"
                    f"Total entries: **{entry_count}**\nGiveaway ID: `{giveaway_id}`"
                )
        except Exception as e:
            print(f"⚠️ Could not DM giveaway host: {e}")

    # DM all entrants
    if entries:
        guild = bot.get_guild(int(gw["guild_id"]))
        for entry in entries:
            try:
                member = guild.get_member(int(entry["user_id"])) if guild else None
                if not member:
                    continue
                if entry["user_id"] in winner_ids:
                    dm_text = (
                        f"🎉 **Congratulations — you won the '{gw['prize']}' giveaway!** 🏆\n"
                        f"A moderator will be in touch shortly to arrange your prize."
                    )
                else:
                    dm_text = (
                        f"👋 The **'{gw['prize']}'** giveaway has ended.\n"
                        f"Unfortunately you didn't win this time — better luck next time! 🍀"
                    )
                await member.send(dm_text)
                await asyncio.sleep(0.4)
            except Exception:
                pass


# ─── COUNTDOWN TASK ──────────────────────────────────────────────────────────

async def giveaway_countdown(giveaway_id: int):
    """Refreshes the embed on a smart schedule, then fires end_giveaway() at expiry."""
    while True:
        gw = fetchone("SELECT * FROM giveaways WHERE id = %s", (giveaway_id,))
        if not gw or gw["ended"]:
            break

        remaining_secs = (gw["ends_at"].replace(tzinfo=None) - datetime.datetime.utcnow()).total_seconds()

        if remaining_secs <= 0:
            await end_giveaway(giveaway_id)
            break

        try:
            count   = fetchone("SELECT COUNT(*) as c FROM giveaway_entries WHERE giveaway_id = %s", (giveaway_id,))["c"]
            channel = bot.get_channel(int(gw["channel_id"]))
            if channel and gw["message_id"]:
                msg   = await channel.fetch_message(int(gw["message_id"]))
                await msg.edit(embed=build_giveaway_embed(gw, count))
        except Exception as e:
            print(f"⚠️ Could not refresh giveaway embed #{giveaway_id}: {e}")

        # Adaptive sleep
        if remaining_secs > 3600:
            await asyncio.sleep(300)
        elif remaining_secs > 600:
            await asyncio.sleep(60)
        elif remaining_secs > 60:
            await asyncio.sleep(30)
        else:
            await asyncio.sleep(max(1, remaining_secs - 1))


# ─── PERMISSION CHECK ────────────────────────────────────────────────────────

def check_giveaway_manager():
    async def predicate(ctx):
        if member_can_manage(ctx.author):
            return True
        await ctx.send("❌ You don't have permission to manage giveaways.", delete_after=10)
        return False
    return commands.check(predicate)


# ─── GIVEAWAY COMMANDS ───────────────────────────────────────────────────────

@bot.group(name="giveaway", aliases=["gw"], invoke_without_command=True)
async def giveaway_group(ctx):
    embed = discord.Embed(title="🎉 Giveaway Commands", color=discord.Color.gold())
    embed.add_field(
        name="Commands",
        value=(
            "`!giveaway start`         — Launch the setup wizard\n"
            "`!giveaway list`          — See all active giveaways\n"
            "`!giveaway end <id>`      — Force-end a giveaway early\n"
            "`!giveaway entries <id>`  — View everyone who entered\n"
        ),
        inline=False,
    )
    embed.set_footer(text="Requires a giveaway manager role or Administrator.")
    await ctx.send(embed=embed)


@giveaway_group.command(name="start")
@check_giveaway_manager()
async def giveaway_start(ctx):
    """Interactive 6-question wizard to launch a giveaway."""

    async def ask(prompt, validator=None, error_msg="❌ Invalid input, please try again."):
        await ctx.send(prompt)
        for _ in range(3):
            try:
                reply = await bot.wait_for(
                    "message", timeout=60,
                    check=lambda m: m.author == ctx.author and m.channel == ctx.channel,
                )
                if reply.content.strip().lower() == "cancel":
                    return "CANCEL"
                val = reply.content.strip()
                if validator is None or validator(val):
                    return val
                await ctx.send(error_msg)
            except asyncio.TimeoutError:
                await ctx.send("⏰ Setup timed out. Run `!giveaway start` to begin again.")
                return None
        await ctx.send("❌ Too many invalid attempts. Run `!giveaway start` to try again.")
        return None

    await ctx.send(
        "🎉 **Giveaway Setup Wizard**\n"
        "Answer the following 6 questions *(60 seconds each)*.\n"
        "Type `cancel` at any time to abort.\n"
        "─────────────────────────────────"
    )

    # Q1 – Prize
    prize = await ask("**[1/6] 🏆 What is the prize?**\n*(e.g. Discord Nitro, $50 Amazon Gift Card)*")
    if not prize or prize == "CANCEL":
        await ctx.send("❌ Giveaway setup cancelled.")
        return

    # Q2 – Description
    desc_raw = await ask(
        "**[2/6] 📝 Add a description** *(optional extra info)*\n"
        "Type your description or `skip` to leave blank:"
    )
    if desc_raw is None or desc_raw == "CANCEL":
        await ctx.send("❌ Giveaway setup cancelled.")
        return
    description = None if desc_raw.lower() == "skip" else desc_raw

    # Q3 – Channel
    channel_raw = await ask(
        "**[3/6] 📣 Which channel should the giveaway be posted in?**\n"
        "Mention it (e.g. `#giveaways`) or type `here` for this channel:"
    )
    if channel_raw is None or channel_raw == "CANCEL":
        await ctx.send("❌ Giveaway setup cancelled.")
        return
    target_channel = ctx.channel
    if channel_raw.lower() != "here":
        match = _re.search(r"<#(\d+)>", channel_raw)
        if match:
            found = ctx.guild.get_channel(int(match.group(1)))
            if found:
                target_channel = found
            else:
                await ctx.send("⚠️ Channel not found — using this channel instead.")

    # Q4 – Duration
    def valid_duration(v):
        td = parse_duration(v)
        return td is not None and 60 <= td.total_seconds() <= 86_400 * 30

    duration_raw = await ask(
        "**[4/6] ⏰ How long should the giveaway run?**\n"
        "Format: `30m`  `2h`  `1d`  `1h30m`  *(min 1 minute — max 30 days)*",
        validator=valid_duration,
        error_msg="❌ Invalid duration. Examples: `30m`, `2h`, `1d`. Min 1 min, max 30 days.",
    )
    if duration_raw is None or duration_raw == "CANCEL":
        await ctx.send("❌ Giveaway setup cancelled.")
        return
    duration = parse_duration(duration_raw)

    # Q5 – Winner count
    winners_raw = await ask(
        "**[5/6] 🏆 How many winners?** *(1 – 20)*",
        validator=lambda v: v.isdigit() and 1 <= int(v) <= 20,
        error_msg="❌ Please enter a whole number between 1 and 20.",
    )
    if winners_raw is None or winners_raw == "CANCEL":
        await ctx.send("❌ Giveaway setup cancelled.")
        return
    winner_count = int(winners_raw)

    # Q6 – Winner mode
    mode_raw = await ask(
        "**[6/6] 🎯 How should the winner(s) be selected?**\n\n"
        "`1` — 📢 Bot picks randomly & **announces in channel**\n"
        "`2` — 🔒 Bot picks randomly & **DMs you privately**\n"
        "`3` — ✋ **You pick manually** from the entries list\n",
        validator=lambda v: v.strip() in ("1", "2", "3"),
        error_msg="❌ Please type `1`, `2`, or `3`.",
    )
    if mode_raw is None or mode_raw == "CANCEL":
        await ctx.send("❌ Giveaway setup cancelled.")
        return
    winner_mode = {"1": "announce", "2": "dm_host", "3": "manual"}[mode_raw.strip()]

    # Create in DB
    ends_at = datetime.datetime.utcnow() + duration
    with get_db() as db:
        with db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO giveaways
                    (guild_id, channel_id, prize, description, host_id, winner_mode, winner_count, ends_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                """,
                (str(ctx.guild.id), str(target_channel.id), prize, description,
                 str(ctx.author.id), winner_mode, winner_count, ends_at),
            )
            giveaway_id = cur.fetchone()[0]

    # Post embed
    gw           = fetchone("SELECT * FROM giveaways WHERE id = %s", (giveaway_id,))
    view         = GiveawayView(giveaway_id)
    bot.add_view(view)
    giveaway_msg = await target_channel.send(embed=build_giveaway_embed(gw, 0), view=view)
    execute("UPDATE giveaways SET message_id = %s WHERE id = %s", (str(giveaway_msg.id), giveaway_id))

    if target_channel.id != ctx.channel.id:
        await ctx.send(
            f"✅ **Giveaway launched in {target_channel.mention}!**\n"
            f"Prize: **{prize}** · Duration: **{duration_raw}** · ID: `{giveaway_id}`"
        )

    asyncio.ensure_future(giveaway_countdown(giveaway_id))


@giveaway_group.command(name="list")
@check_giveaway_manager()
async def giveaway_list(ctx):
    rows = fetchall(
        """
        SELECT g.id, g.prize, g.ends_at, g.winner_count, g.channel_id, g.winner_mode,
               COUNT(e.id) as entry_count
        FROM giveaways g
        LEFT JOIN giveaway_entries e ON g.id = e.giveaway_id
        WHERE g.guild_id = %s AND g.ended = FALSE AND g.ends_at > NOW()
        GROUP BY g.id ORDER BY g.ends_at ASC
        """,
        (str(ctx.guild.id),),
    )
    if not rows:
        await ctx.send("📭 No active giveaways. Start one with `!giveaway start`!")
        return
    embed = discord.Embed(title=f"🎉 Active Giveaways ({len(rows)})", color=discord.Color.gold())
    mode_icons = {"announce": "📢", "dm_host": "🔒", "manual": "✋"}
    for r in rows:
        embed.add_field(
            name=f"#{r['id']}  —  {r['prize']}",
            value=(
                f"📣 <#{r['channel_id']}>  ·  ⏰ {format_remaining(r['ends_at'])}  ·  "
                f"🎟️ {r['entry_count']} entries  ·  🏆 {r['winner_count']} winner(s)  ·  "
                f"{mode_icons.get(r['winner_mode'], '?')} {r['winner_mode']}"
            ),
            inline=False,
        )
    embed.set_footer(text="Use !giveaway end <id> to force-end any of these.")
    await ctx.send(embed=embed)


@giveaway_group.command(name="end")
@check_giveaway_manager()
async def giveaway_end(ctx, giveaway_id: int = None):
    if giveaway_id is None:
        await ctx.send("❌ Please provide the giveaway ID. Example: `!giveaway end 5`")
        return
    gw = fetchone("SELECT * FROM giveaways WHERE id = %s AND guild_id = %s", (giveaway_id, str(ctx.guild.id)))
    if not gw:
        await ctx.send(f"❌ Giveaway `#{giveaway_id}` not found in this server.")
        return
    if gw["ended"]:
        await ctx.send(f"⚠️ Giveaway `#{giveaway_id}` has already ended.")
        return
    await ctx.send(f"⏩ Force-ending giveaway **#{giveaway_id}: {gw['prize']}**…")
    await end_giveaway(giveaway_id)
    await ctx.send(f"✅ Giveaway **#{giveaway_id}** ended and winner(s) selected.")


# ─── /message SLASH COMMAND ──────────────────────────────────────────────────

@bot.tree.command(name="message", description="Send a message as the bot to a specified channel.")
@discord.app_commands.describe(
    channel="The channel to send the message in",
    text="The message to send",
)
async def slash_message(interaction: discord.Interaction, channel: discord.TextChannel, text: str):
    # Check manager role or admin
    if not member_can_manage(interaction.user):
        await interaction.response.send_message(
            "❌ You don't have permission to use this command.", ephemeral=True
        )
        return

    try:
        await channel.send(text)
        await interaction.response.send_message(
            f"✅ Message sent to {channel.mention}.", ephemeral=True
        )
    except discord.Forbidden:
        await interaction.response.send_message(
            f"❌ I don't have permission to send messages in {channel.mention}.", ephemeral=True
        )
    except Exception as e:
        await interaction.response.send_message(
            f"❌ Failed to send message: {e}", ephemeral=True
        )

# ─────────────────────────────────────────────────────────────────────────────

@giveaway_group.command(name="entries")
@check_giveaway_manager()
async def giveaway_entries_cmd(ctx, giveaway_id: int = None):
    if giveaway_id is None:
        await ctx.send("❌ Please provide the giveaway ID. Example: `!giveaway entries 5`")
        return
    gw = fetchone("SELECT * FROM giveaways WHERE id = %s AND guild_id = %s", (giveaway_id, str(ctx.guild.id)))
    if not gw:
        await ctx.send(f"❌ Giveaway `#{giveaway_id}` not found.")
        return
    entries = fetchall(
        "SELECT username, entered_at FROM giveaway_entries WHERE giveaway_id = %s ORDER BY entered_at",
        (giveaway_id,),
    )
    embed = discord.Embed(
        title=f"🎟️ Entries — #{giveaway_id}: {gw['prize']}",
        color=discord.Color.blurple(),
    )
    embed.description = (
        f"**Status:** {'🟢 Active' if not gw['ended'] else '🔴 Ended'}  ·  "
        f"**Total entries:** {len(entries)}"
    )
    if not entries:
        embed.add_field(name="Entrants", value="No entries yet!", inline=False)
    else:
        names  = [f"`{e['username'].split('#')[0]}`" for e in entries]
        chunks = [names[i : i + 20] for i in range(0, len(names), 20)]
        for i, chunk in enumerate(chunks):
            start = i * 20 + 1
            embed.add_field(
                name=f"Entrants {start}–{start + len(chunk) - 1}",
                value="  ".join(chunk),
                inline=False,
            )
    if gw["winner_ids"]:
        embed.add_field(
            name="🏆 Winner(s)",
            value=" ".join(f"<@{uid}>" for uid in gw["winner_ids"]),
            inline=False,
        )
    embed.set_footer(text=f"Giveaway ID: {giveaway_id}")
    await ctx.send(embed=embed)

# ═══════════════════════════════════════════════════════════════════════════════
# BOT EVENTS
# ═══════════════════════════════════════════════════════════════════════════════

@bot.event
async def on_ready():
    init_db()
    print(f"✅  Logged in as {bot.user}")
    print(f"📊  Dashboard → http://localhost:{FLASK_PORT}")
    print(f"📣  FW Order Channel: {FW_ORDER_CHANNEL}")
    print(f"🎁  FW Gift Channel:  {FW_GIFT_CHANNEL}")
    print(f"🎉  Giveaway manager role IDs: {GIVEAWAY_MANAGER_ROLE_IDS}")
    print(f"🎟️  Giveaway entry role ID:    {GIVEAWAY_ENTRY_ROLE_ID}")

    # Sync slash commands instantly to the specific guild
    try:
        guild = discord.Object(id=1127292710290735134)
        bot.tree.copy_global_to(guild=guild)
        synced = await bot.tree.sync(guild=guild)
        print(f"⚡  Synced {len(synced)} slash command(s) to guild")
    except Exception as e:
        print(f"⚠️  Could not sync slash commands: {e}")

    # Restore active giveaways — re-register buttons & restart countdown tasks
    try:
        active = fetchall("SELECT id FROM giveaways WHERE ended = FALSE AND ends_at > NOW()")
        for gw in active:
            bot.add_view(GiveawayView(gw["id"]))
            asyncio.ensure_future(giveaway_countdown(gw["id"]))
        if active:
            print(f"🔄  Restored {len(active)} active giveaway(s)")
    except Exception as e:
        print(f"⚠️  Could not restore giveaways: {e}")


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
        daemon=True,
    )
    flask_thread.start()
    bot.run(BOT_TOKEN)
