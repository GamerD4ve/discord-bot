"""
===================================================================================
GIVEAWAY SYSTEM — ADD TO bot.py
===================================================================================

STEP 1 ── Add these to your imports at the top of bot.py
          (random and re may already be partially imported, just add if missing)

    import random as _random
    import re as _re

─────────────────────────────────────────────────────────────────────────────────

STEP 2 ── Add this env var near your other CONFIG vars

    GIVEAWAY_ROLE = os.environ.get("GIVEAWAY_ROLE", "Moderator")

─────────────────────────────────────────────────────────────────────────────────

STEP 3 ── Add GIVEAWAY_TABLES to init_db()
          Inside init_db(), add this to the same cur.execute(""" ... """) block:

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
    CREATE INDEX IF NOT EXISTS idx_giveaway_active   ON giveaways(ended, ends_at);
    CREATE INDEX IF NOT EXISTS idx_giveaway_entries  ON giveaway_entries(giveaway_id);

─────────────────────────────────────────────────────────────────────────────────

STEP 4 ── Update your on_ready() event to restore active giveaways on restart
          Replace your existing on_ready with this:

@bot.event
async def on_ready():
    init_db()
    print(f"✅  Logged in as {bot.user}")
    print(f"📊  Dashboard → http://localhost:{FLASK_PORT}")
    print(f"📣  FW Order Channel: {FW_ORDER_CHANNEL}")
    print(f"🎁  FW Gift Channel:  {FW_GIFT_CHANNEL}")
    # ── Restore active giveaways so buttons survive restarts ──
    try:
        active = fetchall("SELECT id FROM giveaways WHERE ended = FALSE AND ends_at > NOW()")
        for gw in active:
            bot.add_view(GiveawayView(gw["id"]))
            asyncio.ensure_future(giveaway_countdown(gw["id"]))
        if active:
            print(f"🎉  Restored {len(active)} active giveaway(s)")
    except Exception as e:
        print(f"⚠️  Could not restore giveaways: {e}")

─────────────────────────────────────────────────────────────────────────────────

STEP 5 ── Paste ALL code below this line directly into bot.py,
          anywhere after your DB helper functions (fetchall, execute etc.)
          and before the on_ready / on_message events.

===================================================================================
"""

import random as _random
import re as _re

GIVEAWAY_ROLE = os.environ.get("GIVEAWAY_ROLE", "Moderator")


# ─── GIVEAWAY HELPERS ────────────────────────────────────────────────────────

def parse_duration(text: str):
    """Parse '30m', '2h', '1d', '1h30m' etc. into a timedelta. Returns None if invalid."""
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
    """Return a human-friendly countdown string from a DB timestamp."""
    remaining = ends_at.replace(tzinfo=None) - datetime.datetime.utcnow()
    if remaining.total_seconds() <= 0:
        return "**ENDED**"
    total = int(remaining.total_seconds())
    d, h, m, s = total // 86400, (total % 86400) // 3600, (total % 3600) // 60, total % 60
    parts = []
    if d:
        parts.append(f"{d}d")
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    if not d and not h:          # show seconds only when < 1 hour left
        parts.append(f"{s}s")
    return " ".join(parts) or "< 1s"


def build_giveaway_embed(gw, entry_count: int, ended: bool = False) -> discord.Embed:
    """Build the main giveaway embed."""
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
    embed.add_field(name="🏆 Winners", value=str(gw["winner_count"]), inline=True)
    embed.add_field(name="🎟️ Entries", value=str(entry_count), inline=True)

    mode_labels = {
        "announce": "📢 Announced in channel",
        "dm_host":  "🔒 DM'd to host only",
        "manual":   "✋ Host picks manually",
    }
    embed.add_field(
        name="🎯 Winner Selection",
        value=mode_labels.get(gw["winner_mode"], "Random"),
        inline=True,
    )
    embed.set_footer(text=f"Giveaway ID: {gw['id']}  •  Click the button below to enter!")
    return embed


# ─── GIVEAWAY VIEW (persistent button) ───────────────────────────────────────

class GiveawayView(discord.ui.View):
    """Persistent view — survives bot restarts by re-registering via bot.add_view()."""

    def __init__(self, giveaway_id: int):
        super().__init__(timeout=None)   # Never time out
        self.giveaway_id = giveaway_id
        btn = discord.ui.Button(
            label="🎉  Enter Giveaway",
            style=discord.ButtonStyle.green,
            custom_id=f"giveaway_enter_{giveaway_id}",  # unique per giveaway
        )
        btn.callback = self._enter_callback
        self.add_item(btn)

    async def _enter_callback(self, interaction: discord.Interaction):
        gw = fetchone("SELECT * FROM giveaways WHERE id = %s", (self.giveaway_id,))

        # Guard: already ended or expired
        if not gw or gw["ended"] or datetime.datetime.utcnow() > gw["ends_at"].replace(tzinfo=None):
            await interaction.response.send_message(
                "❌ This giveaway has already ended!", ephemeral=True
            )
            return

        avatar = str(interaction.user.display_avatar.url) if interaction.user.display_avatar else ""
        try:
            execute(
                """
                INSERT INTO giveaway_entries (giveaway_id, user_id, username, avatar_url)
                VALUES (%s, %s, %s, %s)
                """,
                (self.giveaway_id, str(interaction.user.id), str(interaction.user), avatar),
            )
            count = fetchone(
                "SELECT COUNT(*) as c FROM giveaway_entries WHERE giveaway_id = %s",
                (self.giveaway_id,),
            )["c"]
            await interaction.response.send_message(
                f"✅ You're in! Good luck 🍀\n*{count} total {'entry' if count == 1 else 'entries'}*",
                ephemeral=True,
            )
        except Exception as e:
            if "unique" in str(e).lower():
                await interaction.response.send_message(
                    "⚠️ You've already entered this giveaway!", ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    "❌ Something went wrong — please try again.", ephemeral=True
                )


# ─── GIVEAWAY CORE LOGIC ─────────────────────────────────────────────────────

async def end_giveaway(giveaway_id: int):
    """Pick winner(s), update the embed, announce, and DM all entrants."""
    gw = fetchone("SELECT * FROM giveaways WHERE id = %s", (giveaway_id,))
    if not gw or gw["ended"]:
        return

    entries = fetchall(
        "SELECT user_id, username FROM giveaway_entries WHERE giveaway_id = %s ORDER BY entered_at",
        (giveaway_id,),
    )
    entry_count = len(entries)
    picks, winner_ids, winner_mentions = [], [], []

    if entries and gw["winner_mode"] != "manual":
        picks         = _random.sample(entries, min(gw["winner_count"], len(entries)))
        winner_ids    = [p["user_id"] for p in picks]
        winner_mentions = [f"<@{p['user_id']}>" for p in picks]

    # ── Mark ended in DB ──────────────────────────────────────────────────────
    execute(
        "UPDATE giveaways SET ended = TRUE, winner_ids = %s WHERE id = %s",
        (winner_ids, giveaway_id),
    )

    # ── Update the embed ──────────────────────────────────────────────────────
    gw_final  = fetchone("SELECT * FROM giveaways WHERE id = %s", (giveaway_id,))
    channel   = bot.get_channel(int(gw["channel_id"]))

    if channel and gw["message_id"]:
        try:
            msg         = await channel.fetch_message(int(gw["message_id"]))
            ended_embed = build_giveaway_embed(gw_final, entry_count, ended=True)

            if winner_ids:
                ended_embed.add_field(
                    name="🏆 Winner(s)", value="\n".join(winner_mentions), inline=False
                )
            elif gw["winner_mode"] == "manual":
                ended_embed.add_field(
                    name="🏆 Winner(s)", value="Host is selecting manually…", inline=False
                )
            else:
                ended_embed.add_field(
                    name="🏆 Winner(s)", value="No entries — no winner!", inline=False
                )

            # Replace button with disabled placeholder
            ended_view = discord.ui.View()
            ended_view.add_item(
                discord.ui.Button(
                    label="Giveaway Ended",
                    style=discord.ButtonStyle.grey,
                    disabled=True,
                    custom_id=f"giveaway_ended_{giveaway_id}",
                )
            )
            await msg.edit(embed=ended_embed, view=ended_view)
        except Exception as e:
            print(f"⚠️ Could not update giveaway embed #{giveaway_id}: {e}")

    # ── Channel announcement ──────────────────────────────────────────────────
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

    # ── DM the host (dm_host mode) ────────────────────────────────────────────
    if gw["winner_mode"] == "dm_host" and winner_ids:
        try:
            guild = bot.get_guild(int(gw["guild_id"]))
            host  = guild.get_member(int(gw["host_id"])) if guild else None
            if host:
                winners_list = "\n".join(
                    f"• {p['username']}  (ID: {p['user_id']})" for p in picks
                )
                await host.send(
                    f"🎉 **Giveaway ended — {gw['prize']}**\n\n"
                    f"**Winner(s) ({len(picks)}/{gw['winner_count']}):**\n{winners_list}\n\n"
                    f"Total entries: **{entry_count}**\n"
                    f"Giveaway ID: `{giveaway_id}`"
                )
        except Exception as e:
            print(f"⚠️ Could not DM giveaway host: {e}")

    # ── DM all entrants ───────────────────────────────────────────────────────
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
                await asyncio.sleep(0.4)   # avoid hitting DM rate limits
            except Exception:
                pass   # member has DMs closed — silently skip


async def giveaway_countdown(giveaway_id: int):
    """
    Background task: updates the embed periodically and triggers end_giveaway()
    when time is up. Uses adaptive sleep to be efficient without hammering Discord.
    """
    while True:
        gw = fetchone("SELECT * FROM giveaways WHERE id = %s", (giveaway_id,))
        if not gw or gw["ended"]:
            break

        remaining_secs = (
            gw["ends_at"].replace(tzinfo=None) - datetime.datetime.utcnow()
        ).total_seconds()

        if remaining_secs <= 0:
            await end_giveaway(giveaway_id)
            break

        # Update the embed with fresh entry count + countdown
        try:
            count   = fetchone(
                "SELECT COUNT(*) as c FROM giveaway_entries WHERE giveaway_id = %s",
                (giveaway_id,),
            )["c"]
            channel = bot.get_channel(int(gw["channel_id"]))
            if channel and gw["message_id"]:
                msg   = await channel.fetch_message(int(gw["message_id"]))
                embed = build_giveaway_embed(gw, count)
                await msg.edit(embed=embed)
        except Exception as e:
            print(f"⚠️ Could not refresh giveaway embed #{giveaway_id}: {e}")

        # Adaptive sleep — update more frequently as deadline approaches
        if remaining_secs > 3600:
            await asyncio.sleep(300)          # every 5 min when > 1 hr left
        elif remaining_secs > 600:
            await asyncio.sleep(60)           # every 1 min when > 10 min left
        elif remaining_secs > 60:
            await asyncio.sleep(30)           # every 30 s when > 1 min left
        else:
            await asyncio.sleep(max(1, remaining_secs - 1))


# ─── ROLE CHECK ──────────────────────────────────────────────────────────────

def has_giveaway_role():
    async def predicate(ctx):
        if ctx.author.guild_permissions.administrator:
            return True
        role = discord.utils.get(ctx.guild.roles, name=GIVEAWAY_ROLE)
        if role is None:
            await ctx.send(
                f"⚠️ The role **{GIVEAWAY_ROLE}** doesn't exist in this server.\n"
                f"Set the `GIVEAWAY_ROLE` env var to match an existing role name."
            )
            return False
        if role in ctx.author.roles:
            return True
        await ctx.send(f"❌ You need the **{GIVEAWAY_ROLE}** role to manage giveaways.")
        return False

    return commands.check(predicate)


# ─── GIVEAWAY COMMANDS ───────────────────────────────────────────────────────

@bot.group(name="giveaway", aliases=["gw"], invoke_without_command=True)
async def giveaway_group(ctx):
    """Shows the giveaway help menu."""
    embed = discord.Embed(title="🎉 Giveaway Commands", color=discord.Color.gold())
    embed.add_field(
        name="Commands",
        value=(
            "`!giveaway start`            — Launch the setup wizard\n"
            "`!giveaway list`             — See all active giveaways\n"
            "`!giveaway end <id>`         — Force-end a giveaway early\n"
            "`!giveaway entries <id>`     — View everyone who entered\n"
        ),
        inline=False,
    )
    embed.set_footer(text=f"Requires the '{GIVEAWAY_ROLE}' role (or Administrator).")
    await ctx.send(embed=embed)


@giveaway_group.command(name="start")
@has_giveaway_role()
async def giveaway_start(ctx):
    """Interactive setup wizard — asks 6 questions then launches the giveaway."""

    async def ask(prompt: str, validator=None, error_msg: str = "❌ Invalid input, please try again."):
        """Send a prompt and wait for the author's reply. Returns None on timeout or 3 failed attempts."""
        await ctx.send(prompt)
        for _ in range(3):
            try:
                reply = await bot.wait_for(
                    "message",
                    timeout=60,
                    check=lambda m: m.author == ctx.author and m.channel == ctx.channel,
                )
                if reply.content.lower() == "cancel":
                    return "CANCEL"
                if validator is None or validator(reply.content):
                    return reply.content
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
        "─────────────────────────────"
    )

    # ── Q1: Prize ────────────────────────────────────────────────────────────
    prize = await ask("**[1/6] 🏆 What is the prize?**\n*(e.g. Discord Nitro, $50 Amazon Gift Card)*")
    if not prize or prize == "CANCEL":
        await ctx.send("❌ Giveaway setup cancelled.")
        return

    # ── Q2: Description ──────────────────────────────────────────────────────
    desc_raw = await ask(
        "**[2/6] 📝 Add a description** *(optional extra info)*\n"
        "Type your description or `skip` to leave blank:"
    )
    if desc_raw is None or desc_raw == "CANCEL":
        await ctx.send("❌ Giveaway setup cancelled.")
        return
    description = None if desc_raw.lower() == "skip" else desc_raw

    # ── Q3: Channel ──────────────────────────────────────────────────────────
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
            target_channel = found if found else ctx.channel
            if not found:
                await ctx.send("⚠️ Channel not found — using this channel instead.")

    # ── Q4: Duration ─────────────────────────────────────────────────────────
    def valid_duration(v):
        td = parse_duration(v)
        return td is not None and 60 <= td.total_seconds() <= 86_400 * 30

    duration_raw = await ask(
        "**[4/6] ⏰ How long should the giveaway run?**\n"
        "Format: `30m`  `2h`  `1d`  `1h30m`  *(min 1 minute — max 30 days)*",
        validator=valid_duration,
        error_msg="❌ Invalid duration. Use e.g. `30m`, `2h`, `1d`. Min 1 min, max 30 days.",
    )
    if duration_raw is None or duration_raw == "CANCEL":
        await ctx.send("❌ Giveaway setup cancelled.")
        return
    duration = parse_duration(duration_raw)

    # ── Q5: Winner count ─────────────────────────────────────────────────────
    winners_raw = await ask(
        "**[5/6] 🏆 How many winners?** *(1 – 20)*",
        validator=lambda v: v.isdigit() and 1 <= int(v) <= 20,
        error_msg="❌ Please enter a whole number between 1 and 20.",
    )
    if winners_raw is None or winners_raw == "CANCEL":
        await ctx.send("❌ Giveaway setup cancelled.")
        return
    winner_count = int(winners_raw)

    # ── Q6: Winner mode ──────────────────────────────────────────────────────
    mode_raw = await ask(
        "**[6/6] 🎯 How should the winner(s) be selected?**\n\n"
        "`1` — 📢 Bot picks randomly & **announces in channel**\n"
        "`2` — 🔒 Bot picks randomly & **DMs you privately** (host only sees winner)\n"
        "`3` — ✋ **You pick manually** from the entries list\n",
        validator=lambda v: v.strip() in ("1", "2", "3"),
        error_msg="❌ Please type `1`, `2`, or `3`.",
    )
    if mode_raw is None or mode_raw == "CANCEL":
        await ctx.send("❌ Giveaway setup cancelled.")
        return
    winner_mode = {"1": "announce", "2": "dm_host", "3": "manual"}[mode_raw.strip()]

    # ── Create giveaway in DB ─────────────────────────────────────────────────
    ends_at = datetime.datetime.utcnow() + duration

    with get_db() as db:
        with db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO giveaways
                    (guild_id, channel_id, prize, description, host_id, winner_mode, winner_count, ends_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    str(ctx.guild.id), str(target_channel.id), prize, description,
                    str(ctx.author.id), winner_mode, winner_count, ends_at,
                ),
            )
            giveaway_id = cur.fetchone()[0]

    # ── Post giveaway embed with Enter button ─────────────────────────────────
    gw      = fetchone("SELECT * FROM giveaways WHERE id = %s", (giveaway_id,))
    embed   = build_giveaway_embed(gw, 0)
    view    = GiveawayView(giveaway_id)
    bot.add_view(view)

    giveaway_msg = await target_channel.send(embed=embed, view=view)

    # Store message ID so we can edit it later
    execute("UPDATE giveaways SET message_id = %s WHERE id = %s", (str(giveaway_msg.id), giveaway_id))

    # Confirm to the host if they set it up in a different channel
    if target_channel.id != ctx.channel.id:
        await ctx.send(
            f"✅ **Giveaway launched in {target_channel.mention}!**\n"
            f"Prize: **{prize}** · Duration: **{duration_raw}** · ID: `{giveaway_id}`"
        )

    # Kick off the countdown background task
    asyncio.ensure_future(giveaway_countdown(giveaway_id))


@giveaway_group.command(name="list")
@has_giveaway_role()
async def giveaway_list(ctx):
    """Lists all currently active giveaways in this server."""
    rows = fetchall(
        """
        SELECT g.id, g.prize, g.ends_at, g.winner_count, g.channel_id, g.winner_mode,
               COUNT(e.id) as entry_count
        FROM giveaways g
        LEFT JOIN giveaway_entries e ON g.id = e.giveaway_id
        WHERE g.guild_id = %s AND g.ended = FALSE AND g.ends_at > NOW()
        GROUP BY g.id
        ORDER BY g.ends_at ASC
        """,
        (str(ctx.guild.id),),
    )

    if not rows:
        await ctx.send("📭 No active giveaways right now. Start one with `!giveaway start`!")
        return

    embed = discord.Embed(
        title=f"🎉 Active Giveaways ({len(rows)})",
        color=discord.Color.gold(),
    )
    mode_icons = {"announce": "📢", "dm_host": "🔒", "manual": "✋"}
    for r in rows:
        embed.add_field(
            name=f"#{r['id']}  —  {r['prize']}",
            value=(
                f"📣 <#{r['channel_id']}>  ·  "
                f"⏰ {format_remaining(r['ends_at'])}  ·  "
                f"🎟️ {r['entry_count']} entries  ·  "
                f"🏆 {r['winner_count']} winner(s)  ·  "
                f"{mode_icons.get(r['winner_mode'], '?')} {r['winner_mode']}"
            ),
            inline=False,
        )
    embed.set_footer(text="Use !giveaway end <id> to force-end any of these.")
    await ctx.send(embed=embed)


@giveaway_group.command(name="end")
@has_giveaway_role()
async def giveaway_end(ctx, giveaway_id: int = None):
    """Force-ends a giveaway immediately and picks the winner(s) now."""
    if giveaway_id is None:
        await ctx.send("❌ Please provide the giveaway ID. Example: `!giveaway end 5`")
        return

    gw = fetchone(
        "SELECT * FROM giveaways WHERE id = %s AND guild_id = %s",
        (giveaway_id, str(ctx.guild.id)),
    )
    if not gw:
        await ctx.send(f"❌ Giveaway `#{giveaway_id}` not found in this server.")
        return
    if gw["ended"]:
        await ctx.send(f"⚠️ Giveaway `#{giveaway_id}` has already ended.")
        return

    await ctx.send(f"⏩ Force-ending giveaway **#{giveaway_id}: {gw['prize']}**…")
    await end_giveaway(giveaway_id)
    await ctx.send(f"✅ Giveaway **#{giveaway_id}** has been ended and winner(s) selected.")


@giveaway_group.command(name="entries")
@has_giveaway_role()
async def giveaway_entries_cmd(ctx, giveaway_id: int = None):
    """Shows every user who has entered a given giveaway."""
    if giveaway_id is None:
        await ctx.send("❌ Please provide the giveaway ID. Example: `!giveaway entries 5`")
        return

    gw = fetchone(
        "SELECT * FROM giveaways WHERE id = %s AND guild_id = %s",
        (giveaway_id, str(ctx.guild.id)),
    )
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
    status = "🟢 Active" if not gw["ended"] else "🔴 Ended"
    embed.description = f"**Status:** {status}  ·  **Total entries:** {len(entries)}"

    if not entries:
        embed.add_field(name="Entrants", value="No entries yet!", inline=False)
    else:
        # Split into chunks of 20 so we don't hit Discord field limits
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
        winner_mentions = " ".join(f"<@{uid}>" for uid in gw["winner_ids"])
        embed.add_field(name="🏆 Winner(s)", value=winner_mentions, inline=False)

    embed.set_footer(text=f"Giveaway ID: {giveaway_id}")
    await ctx.send(embed=embed)


# ─── GIVEAWAY DASHBOARD API ROUTES ───────────────────────────────────────────

@api.route("/api/giveaways")
def api_giveaways():
    """Returns all giveaways (active + ended), newest first."""
    try:
        rows = fetchall(
            """
            SELECT g.id, g.prize, g.description, g.host_id, g.winner_mode,
                   g.winner_count, g.ends_at, g.ended, g.winner_ids,
                   g.created_at, g.channel_id,
                   COUNT(e.id) as entry_count
            FROM giveaways g
            LEFT JOIN giveaway_entries e ON g.id = e.giveaway_id
            GROUP BY g.id
            ORDER BY g.created_at DESC
            LIMIT 50
            """
        )
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
    """Returns giveaway metadata + full list of entrants for a given giveaway."""
    try:
        gw = fetchone("SELECT * FROM giveaways WHERE id = %s", (giveaway_id,))
        if not gw:
            return jsonify({"error": "Giveaway not found"}), 404

        entries = fetchall(
            """
            SELECT user_id, username, avatar_url, entered_at
            FROM giveaway_entries
            WHERE giveaway_id = %s
            ORDER BY entered_at
            """,
            (giveaway_id,),
        )
        return jsonify(
            {
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
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500
