"""
VHQCoins economy Discord bot built with discord.py.

Commands (all usable as !command or /command):
- stats / balance, daily, weekly, work, beg, fish, crime, luck, give, leaderboard, shop
- games: cf, duel, roulette, slots, highlow, blackjack
- addmoney, ownerboost (owner-only, prefix only)

Passive earning from chatting is also kept.

Settings (payouts, cooldowns, odds, shop catalog, on/off switch) live in
economy_config.json next to this file. It is created with defaults on first
run and can be edited by hand while the bot is stopped.

Setup:
1. pip install discord.py
2. Fill in config.py with your bot token (or set DISCORD_TOKEN env var)
3. Run: python app.py
"""

import asyncio
import copy
import json
import logging
import os
import random
import secrets
import time
from typing import Optional

# All persisted JSON data files (economy balances, economy config) are
# anchored here, next to app.py itself, instead of using bare relative
# paths — a bare "economy.json" resolves against whatever the current
# working directory happens to be when the process is launched, which can
# silently differ between a manual run and however the host starts the
# container. If that ever points somewhere new, every persisted file —
# including how long ago someone last claimed !daily — reads back empty.
# Set the DATA_DIR environment variable to a PERSISTENT folder (e.g. the mount path of a
# Render/Railway disk, like /data) so economy.json survives redeploys. Without it the files
# live next to app.py, which many hosts wipe on every new deploy.
DATA_DIR = os.getenv("DATA_DIR") or os.path.dirname(os.path.abspath(__file__))
os.makedirs(DATA_DIR, exist_ok=True)


def _data_path(filename: str) -> str:
    return os.path.join(DATA_DIR, filename)


# Build marker — bump the string whenever you hand over a new version so it's
# possible to tell from the process's stdout at startup whether a given host
# is actually running the file you think it's running.
APP_BUILD_TAG = "economy-shop-v5-2026-11-15"
print(f"[voidhq] app.py loaded — build tag: {APP_BUILD_TAG} — file: {os.path.abspath(__file__)}")

import discord
from discord import app_commands
from discord.ext import commands

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# The token is read from config.py (not committed anywhere public). If
# config.py is missing, fall back to the DISCORD_TOKEN environment variable.

try:
    from config import DISCORD_TOKEN as TOKEN
except ImportError:
    TOKEN = os.getenv("DISCORD_TOKEN")

COMMAND_PREFIX = "!"

# Owner-only commands (!addmoney, !ownerboost). Works if you are the bot's owner in the
# Discord Developer Portal, OR if your Discord user ID is listed here. Put your ID in
# OWNER_ID (env var, or OWNER_ID in config.py); several IDs separated by commas are fine.
# Find your ID: Discord > Settings > Advanced > Developer Mode, then right-click yourself > Copy User ID.
try:
    from config import OWNER_ID as _OWNER_ID_RAW
except ImportError:
    _OWNER_ID_RAW = os.getenv("OWNER_ID", "")
OWNER_IDS = {int(x) for x in str(_OWNER_ID_RAW).replace(" ", "").split(",") if x.isdigit()}


def owner_only():
    async def predicate(ctx):
        if ctx.author.id in OWNER_IDS or await ctx.bot.is_owner(ctx.author):
            return True
        raise commands.NotOwner("Owner only")
    return commands.check(predicate)

_economy_autosave_started = False
_web_started = False
_keepalive_started = False


async def _keepalive_loop():
    """Pings this service's own public URL every 10 minutes so Render's free tier doesn't put it
    to sleep (it sleeps after ~15 min without incoming web traffic, which takes the bot offline).
    RENDER_EXTERNAL_URL is set automatically by Render; without it (e.g. local runs) this does nothing."""
    url = os.getenv("RENDER_EXTERNAL_URL")
    if not url:
        return
    import aiohttp
    while True:
        await asyncio.sleep(600)
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                async with session.get(url) as r:
                    await r.read()
        except Exception as e:
            print(f"Keep-alive ping failed: {e}")

# ---------------------------------------------------------------------------
# VHQCoins economy — global (cross-server) per-user balances + a shop of
# items purchasable with coins. Same load/save-a-JSON-file pattern as guild
# settings above, just keyed by user ID instead of guild ID.
# ---------------------------------------------------------------------------

ECONOMY_FILE = _data_path("economy.json")
ECONOMY_CONFIG_FILE = _data_path("economy_config.json")  # global on/off switch + the editable shop catalog

# ---------------------------------------------------------------------------
# Optional cloud storage (Neon / any Postgres): set the DATABASE_URL environment
# variable and the economy survives every deploy and restart on any host.
# On startup the saved data is pulled into the local files; on every autosave it
# is pushed back. If DATABASE_URL is not set, nothing changes (plain files).
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
_DB_FILES = ("economy.json", "economy_config.json")
_db_ready = False


def _db_connect():
    import psycopg2
    return psycopg2.connect(DATABASE_URL, connect_timeout=15)


def _db_pull() -> None:
    global _db_ready
    if not DATABASE_URL:
        return
    try:
        conn = _db_connect()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
                for fname in _DB_FILES:
                    cur.execute("SELECT value FROM kv WHERE key = %s", (fname,))
                    row = cur.fetchone()
                    if row:
                        with open(_data_path(fname), "w", encoding="utf-8") as f:
                            f.write(row[0])
                        print(f"[db] loaded {fname} from database")
                    else:
                        print(f"[db] no {fname} in database yet (will be saved on first autosave)")
        finally:
            conn.close()
        _db_ready = True
    except Exception as e:
        # Pushing stays disabled so a failed start can never overwrite good data in the database.
        print(f"[db] WARNING: could not reach the database, running WITHOUT cloud saving: {e!r}")


def _db_push(path: str, data: dict) -> None:
    if not (DATABASE_URL and _db_ready):
        return
    fname = os.path.basename(path)
    if fname not in _DB_FILES:
        return
    try:
        conn = _db_connect()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO kv (key, value) VALUES (%s, %s) "
                    "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                    (fname, json.dumps(data)),
                )
        finally:
            conn.close()
    except Exception as e:
        print(f"[db] WARNING: could not save {fname} to the database: {e!r}")


_db_pull()

DEFAULT_ECONOMY_USER = {
    "balance": 0,
    "last_daily": 0.0,     # epoch seconds, 0 = never claimed
    "last_weekly": 0.0,
    "last_work": 0.0,
    "last_beg": 0.0,
    "last_fish": 0.0,
    "last_crime": 0.0,
    "last_luck": 0.0,      # epoch seconds of last !luck use — once per day
    "luck": 0,             # luck points from the last !luck use, 1 point = 1% edge
    "luck_until": 0.0,     # epoch seconds — luck above is only active while now < this
    "last_message_earn": 0.0,
    "passive_streak": 0,   # qualifying messages sent since the last passive payout
    "inventory": [],       # list of shop item ids owned
    "boost_until": 0.0,    # epoch seconds — earnings multiplied (boost_mult) while now < this
    "boost_mult": 2,       # multiplier of the active boost (2 = double, 3 = triple)
    "passive_boost_until": 0.0,  # epoch seconds — chat coins doubled while now < this
    "resubmit_passes": [],       # expiry timestamps (epoch seconds), one per unused Resubmit Pass
    "owner_mult": 1,             # owner-only permanent multiplier (set with !ownerboost), 1 = off
}

# Seed catalog — only used to populate economy_config.json the first time
# the bot runs. After that, economy_config.json is the source of truth: item
# name/price/description/emoji can all be edited (or items added/removed)
# there, and this constant is never read again. Card-theme items
# auto-equip as active_theme on purchase (only one active at a time); the
# boost item extends a time-limited earnings multiplier instead of going
# into the inventory list.
SHOP_VERSION = 5  # bump this to push a new DEFAULT_SHOP_ITEMS catalog to an existing economy_config.json

TICKET_NOTE = "Please screenshot this and open a ticket to claim your reward."

DEFAULT_SHOP_ITEMS = [
    {"id": "vip_badge", "emoji": "🏅", "name": "VIP Badge", "price": 15000,
     "kind": "badge",
     "description": "Adds a badge next to your name on !stats and !leaderboard, permanently."},
    {"id": "custom_role", "emoji": "🎨", "name": "Custom Role", "price": 200000,
     "kind": "perk", "redeem_note": TICKET_NOTE,
     "description": "Get your own role with a custom name and color of your choice for 30 days."},
    {"id": "honorable_uploads", "emoji": "🌟", "name": "Honorable Uploads Spotlight", "price": 100000,
     "kind": "perk", "redeem_note": TICKET_NOTE,
     "description": "Get your edit featured in Honorable Uploads."},
    {"id": "feedback_priority", "emoji": "🔍", "name": "Feedback Priority", "price": 50000,
     "kind": "perk", "redeem_note": TICKET_NOTE,
     "description": "Your edit gets reviewed first, with priority feedback from the team."},
    {"id": "resubmit_pass", "emoji": "🔁", "name": "Resubmit Pass", "price": 50000,
     "kind": "resubmit_pass", "days": 7,
     "description": "Lost in Top Edits? This pass lets you resubmit an edit once within 7 days of buying it. The Top Edits bot checks it automatically."},
    {"id": "ae_presets_noxy_mayto", "emoji": "🎞️", "name": "AE Presets: Noxy & Mayto Pack", "price": 1000000,
     "kind": "perk", "redeem_note": TICKET_NOTE,
     "description": "Unlock the After Effects preset pack made by Noxy and Mayto."},
    {"id": "am_presets_zexo", "emoji": "🎬", "name": "AM Presets: Zexo", "price": 500000,
     "kind": "perk", "redeem_note": TICKET_NOTE,
     "description": "Unlock the Alight Motion (AM) presets made by Zexo."},
    {"id": "shoutout", "emoji": "📢", "name": "Shoutout", "price": 50000,
     "kind": "perk", "redeem_note": TICKET_NOTE,
     "description": "Get a shoutout for you and your content."},
    {"id": "rating_slot", "emoji": "⭐", "name": "Rating Slot", "price": 150000,
     "kind": "perk", "redeem_note": TICKET_NOTE,
     "description": "Get your edit into an upcoming editing rating video. You buy the slot, not the score — ratings stay honest."},
    {"id": "mentor_session", "emoji": "🎓", "name": "Mentor Session", "price": 500000,
     "kind": "perk", "redeem_note": TICKET_NOTE,
     "description": "A 30-minute 1-on-1 session with a mentor to level up your edits."},
    {"id": "luck_refill", "emoji": "🍀", "name": "Luck Refill", "price": 20000,
     "kind": "luck_refill",
     "description": "Resets your !luck cooldown so you can roll for luck again right away."},
    {"id": "triple_earner", "emoji": "💎", "name": "12h Triple Earner", "price": 200000,
     "kind": "boost", "hours": 12, "multiplier": 3,
     "description": "Triples your coin earnings from messages, !work, !daily and more for 12 hours."},
    {"id": "passive_boost", "emoji": "🧲", "name": "24h Passive Boost", "price": 30000,
     "kind": "passive_boost", "hours": 24,
     "description": "Doubles the coins you earn from chatting for 24 hours."},
]


_economy_config_cache: Optional[dict] = None
_economy_config_dirty = False


def load_economy_config() -> dict:
    """Global economy config: {"enabled": bool, "items": {item_id: {...}}}.
    Seeded from DEFAULT_SHOP_ITEMS on first run, then edited directly in
    economy_config.json (with the bot stopped) from that point on.

    Cached in memory after the first call — this used to re-read and
    re-parse the whole file from disk on every single call, which matters
    because nearly every command in the bot (via _economy_gate and the
    universal cooldown check) calls this once per invocation. See
    _economy_autosave_loop for how edits actually reach disk now."""
    global _economy_config_cache
    if _economy_config_cache is not None:
        return _economy_config_cache
    if not os.path.exists(ECONOMY_CONFIG_FILE):
        seeded = {
            "enabled": True,
            "shop_version": SHOP_VERSION,
            "items": {item["id"]: {k: v for k, v in item.items() if k != "id"} for item in DEFAULT_SHOP_ITEMS},
        }
        save_economy_config(seeded)
        return _economy_config_cache
    with open(ECONOMY_CONFIG_FILE, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            data = {}
    data.setdefault("enabled", True)
    data.setdefault("items", {})
    # Push the current default catalog into an older config file once.
    if data.get("shop_version") != SHOP_VERSION:
        data["items"] = {item["id"]: {k: v for k, v in item.items() if k != "id"} for item in DEFAULT_SHOP_ITEMS}
        data["shop_version"] = SHOP_VERSION
        global _economy_config_dirty
        _economy_config_dirty = True
    data.setdefault("daily_min", 150)
    data.setdefault("daily_max", 300)
    data.setdefault("daily_cooldown_hours", 20)
    data.setdefault("weekly_min", 800)
    data.setdefault("weekly_max", 1500)
    data.setdefault("weekly_cooldown_days", 7)
    data.setdefault("work_min", 20)
    data.setdefault("work_max", 60)
    data.setdefault("work_cooldown_minutes", 30)
    # !beg — fast, low-friction, and can come up empty (unlike !work/!fish,
    # which always pay out on cooldown). Meant to fill the gaps between the
    # longer work/crime cooldowns, not replace them.
    data.setdefault("beg_min", 5)
    data.setdefault("beg_max", 25)
    data.setdefault("beg_cooldown_minutes", 5)
    data.setdefault("beg_success_chance", 0.8)
    # !fish — sits between !beg and !work in both cooldown and payout.
    data.setdefault("fish_min", 15)
    data.setdefault("fish_max", 80)
    data.setdefault("fish_cooldown_minutes", 15)
    # Passive earning from chatting — a reward is paid out once every
    # passive_messages qualifying messages (defaults preserve the original
    # hardcoded behavior: 1-3 coins per message, at most once every 60s).
    data.setdefault("passive_min", 1)
    data.setdefault("passive_max", 3)
    data.setdefault("passive_messages", 1)
    data.setdefault("passive_cooldown_seconds", 60)
    # !duel start / !duel join — a multiplayer sword-duel bet. Everyone who
    # joins throws their bet into the pot; after the join window closes, one
    # participant is picked with an equal chance to win the whole thing.
    data.setdefault("duel_min_bet", 100)
    data.setdefault("duel_join_seconds", 30)
    # Universal per-person command cooldown — applies bot-wide, to every
    # command (not just economy ones). After running any command, a member
    # has to wait this many seconds before running another. 0 = disabled.
    data.setdefault("universal_cooldown_seconds", 0)
    # !crime — higher risk/reward than !work: a coin-flip on a success
    # chance, paying out a coin range on success or costing a % of the
    # player's current balance on failure.
    data.setdefault("crime_min", 50)
    data.setdefault("crime_max", 200)
    data.setdefault("crime_success_chance", 0.55)   # base chance, before luck
    data.setdefault("crime_fail_min_pct", 10)        # % of balance lost on fail
    data.setdefault("crime_fail_max_pct", 30)
    data.setdefault("crime_cooldown_minutes", 45)
    # !luck — a small, temporary edge. Each use grants a random number of
    # luck points (1 point = 1% bonus) that expires after luck_duration_
    # minutes; the command itself can only be used once every
    # luck_cooldown_hours, regardless of whether the last luck has expired.
    data.setdefault("luck_min_points", 1)
    data.setdefault("luck_max_points", 10)
    data.setdefault("luck_duration_minutes", 60)
    data.setdefault("luck_cooldown_hours", 24)
    # !cf — chance the HOUSE wins a coinflip, before luck. 0.5 = a true
    # 50/50 coin. Luck (see LUCK SYSTEM note) subtracts from this the same
    # way it does for crime's fail chance.
    data.setdefault("cf_lose_chance", 0.5)
    # !roulette — a shared 30s entry window (like !duel's join window):
    # anyone can place a Red/Black/Green entry via buttons while the table is
    # open, then one spin resolves every entry at once. Colors use
    # American-wheel-style odds (18/18/2 out of 38 slots).
    data.setdefault("roulette_min_entry", 10)
    data.setdefault("roulette_entry_seconds", 30)
    data.setdefault("roulette_red_black_multiplier", 2.0)
    data.setdefault("roulette_green_multiplier", 14.0)
    # !slots — 3 reels drawn from a weighted symbol table. 3-of-a-kind pays
    # that symbol's own multiplier; 2-of-a-kind pays a small flat
    # consolation multiplier; anything else loses the entry.
    data.setdefault("slots_min_entry", 10)
    data.setdefault("slots_two_match_multiplier", 1.2)
    data.setdefault("slots_symbols", [
        {"emoji": "🍒", "weight": 40, "multiplier": 2.0},
        {"emoji": "🍋", "weight": 30, "multiplier": 3.0},
        {"emoji": "🔔", "weight": 15, "multiplier": 5.0},
        {"emoji": "⭐", "weight": 10, "multiplier": 10.0},
        {"emoji": "💎", "weight": 5, "multiplier": 20.0},
    ])
    # !highlow — bot secretly picks 1-highlow_range; you call Higher or Lower
    # on the next number. The rarer call pays more, the safer call pays
    # less — the multiplier is just 1/probability of that call being right,
    # clamped to the range below so nothing is ever a guaranteed win or a
    # joke payout. highlow_range is a fixed choice: 10 or 20.
    data.setdefault("highlow_min_bet", 10)
    data.setdefault("highlow_min_multiplier", 1.01)
    data.setdefault("highlow_max_multiplier", 10.0)
    # Higher/Lower payout shape is a fixed anchor curve (see the module note
    # above _highlow_side_multiplier) that scales automatically with
    # highlow_min_multiplier/highlow_max_multiplier — no separate curve
    # setting needed. Same always pays highlow_max_multiplier.
    data.setdefault("highlow_range", 20)
    if data.get("highlow_range") not in (10, 20):
        data["highlow_range"] = 20
    # !blackjack — standard rules, dealer stands on 17. A natural blackjack
    # (21 on the first two cards) pays this on top of the entry returned
    # (1.5 = the usual 3:2).
    data.setdefault("blackjack_min_entry", 10)
    data.setdefault("blackjack_blackjack_payout", 1.5)
    # Compliance settings — the coin games (cf, duel, roulette, slots,
    # highlow, blackjack) only run in age-restricted (NSFW-flagged) channels
    # when this is true. Everything else (daily, work, shop, ...) works
    # everywhere. Set to false in economy_config.json to allow all channels.
    data.setdefault("games_age_restricted_only", True)
    data.setdefault("max_give_per_transfer", 5000)   # 0 = no limit
    _economy_config_cache = data
    return data


def save_economy_config(data: dict) -> None:
    """Updates the in-memory config cache immediately (so every read stays
    consistent right away) and marks it dirty for _economy_autosave_loop
    to persist to disk — does NOT write to disk directly/synchronously."""
    global _economy_config_cache, _economy_config_dirty
    _economy_config_cache = data
    _economy_config_dirty = True


def economy_enabled() -> bool:
    return load_economy_config().get("enabled", True)


def set_economy_enabled(enabled: bool) -> None:
    cfg = load_economy_config()
    cfg["enabled"] = enabled
    save_economy_config(cfg)


def get_shop_items() -> list:
    """Returns the current shop catalog as a list of dicts (each with its
    "id" folded back in), in the order they're stored."""
    cfg = load_economy_config()
    return [{"id": item_id, **item} for item_id, item in cfg["items"].items()]


def get_shop_item(item_id: str):
    cfg = load_economy_config()
    item = cfg["items"].get(item_id)
    return {"id": item_id, **item} if item else None


def set_shop_item(item_id: str, **fields) -> dict:
    """Adds a new item (if item_id is new) or updates an existing one —
    available for adding or editing shop items from code."""
    cfg = load_economy_config()
    current = cfg["items"].get(item_id, {})
    current.update(fields)
    cfg["items"][item_id] = current
    save_economy_config(cfg)
    return {"id": item_id, **current}


def delete_shop_item(item_id: str) -> None:
    cfg = load_economy_config()
    cfg["items"].pop(item_id, None)
    save_economy_config(cfg)


_economy_cache: Optional[dict] = None
_economy_dirty = False


def load_economy() -> dict:
    """In-memory cache of economy.json (every user's balance/cooldowns),
    populated from disk once and kept there from then on.

    This used to do a full file read on every single call — including
    from get_user_economy, which the passive coin-drip in on_message
    calls on nearly every chat message in every server, not just bot
    commands. That was synchronous, blocking I/O on the bot's one shared
    event loop, on the single hottest path in the whole bot, and it only
    got slower as the file grew (every user who's ever touched the
    economy stays in it forever). See _economy_autosave_loop for how
    writes actually reach disk now — never directly from here."""
    global _economy_cache
    if _economy_cache is None:
        if os.path.exists(ECONOMY_FILE):
            with open(ECONOMY_FILE, "r", encoding="utf-8") as f:
                try:
                    _economy_cache = json.load(f)
                except json.JSONDecodeError:
                    _economy_cache = {}
        else:
            _economy_cache = {}
    return _economy_cache


def save_economy(data: dict) -> None:
    """Marks the in-memory economy cache dirty for _economy_autosave_loop
    to persist — does NOT write to disk directly. `data` is always the
    same dict load_economy() returned (mutated in place by every caller
    in this file), so this just flips the dirty flag."""
    global _economy_cache, _economy_dirty
    _economy_cache = data
    _economy_dirty = True


def get_user_economy(user_id: int) -> dict:
    data = load_economy()
    return {**DEFAULT_ECONOMY_USER, **data.get(str(user_id), {})}


def set_user_economy(user_id: int, **updates) -> dict:
    data = load_economy()
    current = {**DEFAULT_ECONOMY_USER, **data.get(str(user_id), {})}
    current.update(updates)
    data[str(user_id)] = current
    save_economy(data)
    return current


def add_coins(user_id: int, amount: int) -> int:
    """Adjusts a user's balance (amount can be negative for spends/gives)
    and returns the new balance. Never lets balance go below 0."""
    current = get_user_economy(user_id)
    new_balance = max(0, current["balance"] + amount)
    set_user_economy(user_id, balance=new_balance)
    return new_balance


def valid_resubmit_passes(econ: dict) -> list:
    """Unexpired Resubmit Pass expiry times, soonest first."""
    now = time.time()
    return sorted(t for t in econ.get("resubmit_passes", []) if t > now)


def has_active_boost(econ: dict) -> bool:
    return time.time() < econ.get("boost_until", 0)


def _earn_rate_multiplier(econ: dict) -> int:
    base = econ.get("boost_mult", 2) if has_active_boost(econ) else 1
    return base * max(1, econ.get("owner_mult", 1))


def _owner_payout(user_id: int, payout: int, stake: int = 0) -> int:
    """Owner-only game boost: multiplies only the PROFIT of a win (payout - stake),
    so the stake itself is never multiplied. Normal users (owner_mult = 1) are untouched."""
    mult = get_user_economy(user_id).get("owner_mult", 1)
    if mult <= 1:
        return payout
    return int(stake + (payout - stake) * mult)


def _write_json_file(path: str, data: dict) -> None:
    """Blocking file write — only ever meant to run inside a thread
    executor (see _economy_autosave_loop below), never called directly
    from a coroutine on the event loop."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    _db_push(path, data)


async def _economy_autosave_loop():
    """Economy reads/writes now happen entirely in memory (see load_economy
    and load_economy_config above) — this is the only place that actually
    touches disk for them. Writes are batched on a short timer rather than
    happening synchronously on every single call, and the disk write
    itself runs in a thread executor, so nothing in the economy system can
    block the bot's one shared event loop anymore, no matter how much
    message traffic (and passive-earning writes) a server has.

    A deep copy is taken before handing data to the executor thread —
    the write runs concurrently with the event loop, which could still be
    mutating the SAME nested per-user dicts in the live cache while the
    background thread is serializing them otherwise.

    5 seconds is a deliberate tradeoff: short enough that a crash between
    autosaves loses at most a few seconds of activity, long enough that
    heavy message traffic doesn't turn this back into a write-every-call
    pattern."""
    global _economy_dirty, _economy_config_dirty
    while True:
        await asyncio.sleep(5)
        loop = asyncio.get_running_loop()
        if _economy_dirty:
            _economy_dirty = False
            snapshot = copy.deepcopy(_economy_cache) if _economy_cache is not None else {}
            await loop.run_in_executor(None, _write_json_file, ECONOMY_FILE, snapshot)
        if _economy_config_dirty:
            _economy_config_dirty = False
            snapshot = copy.deepcopy(_economy_config_cache) if _economy_config_cache is not None else {}
            await loop.run_in_executor(None, _write_json_file, ECONOMY_CONFIG_FILE, snapshot)


# ---------------------------------------------------------------------------
# 🍀 LUCK SYSTEM — NOTE FOR FUTURE AI / DEVELOPERS, READ BEFORE ADDING A NEW
# ECONOMY COMMAND
# ---------------------------------------------------------------------------
# Every economy user can hold a temporary "luck" stat (points earned from
# !luck, 1 point = a 1% edge) that EXPIRES after luck_duration_minutes
# (default 60 = 1 hour). !luck itself can only be used once every
# luck_cooldown_hours (default 24), independent of whether the previous
# luck has already expired — so it's a short window of benefit on a much
# longer refresh timer, not a permanently stacking stat. Luck is meant to
# apply across EVERY economy command, not just !crime and !cf.
#
# When adding a new command that involves a random chance of winning or
# losing coins:
#   1. Fetch the player's econ dict — get_user_economy(user_id) — and call
#      get_luck_bonus(econ) to get their CURRENT (non-expired) luck as a
#      decimal (23 luck -> 0.23). It automatically returns 0 once expired —
#      never read econ["luck"] directly, since it isn't cleared to 0 the
#      instant it expires, only ignored by this helper.
#   2. Apply it in the direction that favors the player — add it to a
#      success/win chance, or subtract it from a lose/bust/house-edge chance.
#   3. ALWAYS clamp the resulting probability to a sane range afterward
#      (e.g. min(0.9, chance) or max(0.05, chance)) — luck should nudge the
#      odds, never make an outcome guaranteed. See !crime and !cf below for
#      the pattern to copy.
#   4. If the command's base odds/payouts should be admin-tunable, add them
#      as their own named fields in load_economy_config() (like
#      crime_success_chance) instead of hardcoding, so economy_config.json
#      stays the single source of truth for the whole economy.
# ---------------------------------------------------------------------------

def get_luck_percent(econ: dict) -> int:
    """The user's CURRENTLY ACTIVE luck points (1 point = 1% bonus) — 0 if
    they've never used !luck, or if their last luck has expired
    (luck_until has passed). The underlying econ["luck"] value is left in
    place after expiry for display/debugging purposes, so always read luck
    through this function (or get_luck_bonus below), never econ["luck"]
    directly."""
    if time.time() >= econ.get("luck_until", 0):
        return 0
    return econ.get("luck", 0)


def get_luck_bonus(econ: dict) -> float:
    """Currently active luck expressed as a decimal bonus (e.g. 23 luck ->
    0.23), ready to be added to a win/success chance or subtracted from a
    lose/house-edge chance. Automatically 0 once luck has expired — see the
    LUCK SYSTEM note above for the convention to follow."""
    return get_luck_percent(econ) / 100.0


def get_effective_prefix(guild_id: int = None) -> str:
    return COMMAND_PREFIX


def _resolve_prefix(bot_: commands.Bot, message: discord.Message):
    return commands.when_mentioned_or(COMMAND_PREFIX)(bot_, message)


intents = discord.Intents.default()
intents.message_content = True
# members intent stays ON, same as before — it keeps member lookups
# (e.g. !give @someone, !duel) working regardless of the member cache.
# chunk_guilds_at_startup=False stops discord.py from downloading and caching
# every member of every server at connect time.
intents.members = True

bot = commands.AutoShardedBot(
    command_prefix=_resolve_prefix,
    intents=intents,
    help_command=None,
    chunk_guilds_at_startup=False,
)


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    print(f"[voidhq] running build tag: {APP_BUILD_TAG}")

    # Discord global application-command updates are cached/propagated and
    # cannot be forced to become instant everywhere. For development and
    # active servers, mirror the command tree into guild command scope on
    # startup. Guild-scoped commands update immediately. Set
    # VOIDHQ_SYNC_GUILD_ID to limit this to one server (recommended when the
    # bot is in many servers).
    try:
        sync_guild_id = int(os.getenv("VOIDHQ_SYNC_GUILD_ID", "0") or 0)
        if sync_guild_id:
            target = bot.get_guild(sync_guild_id)
            if target is None:
                print(f"Instant command sync: guild {sync_guild_id} is not available to the bot.")
            else:
                bot.tree.copy_global_to(guild=target)
                synced = await bot.tree.sync(guild=target)
                print(f"Instantly synced {len(synced)} slash commands to {target.name} ({target.id}).")
        else:
            # No guild was specified, so keep every connected guild current.
            total_synced = 0
            for target in bot.guilds:
                bot.tree.copy_global_to(guild=target)
                synced = await bot.tree.sync(guild=target)
                total_synced += len(synced)
            print(f"Instantly synced {total_synced} guild-scoped slash commands across {len(bot.guilds)} servers.")
    except Exception as e:
        print(f"Instant guild slash-command sync failed: {e}")

    global _web_started
    if not _web_started:
        _web_started = True
        try:
            from aiohttp import web
            async def _health(_request):
                return web.Response(text="ok")
            def _authorized(request) -> bool:
                key = os.getenv("RESUBMIT_API_KEY", "")
                given = request.headers.get("X-API-Key", "")
                return bool(key) and secrets.compare_digest(given.encode(), key.encode())

            def _uid(request):
                try:
                    return int(request.query.get("user_id", ""))
                except ValueError:
                    return None

            async def _resubmit_check(request):
                if not _authorized(request):
                    return web.json_response({"error": "unauthorized"}, status=401)
                uid = _uid(request)
                if uid is None:
                    return web.json_response({"error": "bad user_id"}, status=400)
                passes = valid_resubmit_passes(get_user_economy(uid))
                return web.json_response({"valid": bool(passes), "count": len(passes),
                                          "expires_at": passes[0] if passes else None})

            async def _resubmit_consume(request):
                if not _authorized(request):
                    return web.json_response({"error": "unauthorized"}, status=401)
                uid = _uid(request)
                if uid is None:
                    return web.json_response({"error": "bad user_id"}, status=400)
                passes = valid_resubmit_passes(get_user_economy(uid))
                if not passes:
                    return web.json_response({"consumed": False, "remaining": 0})
                set_user_economy(uid, resubmit_passes=passes[1:])
                return web.json_response({"consumed": True, "remaining": len(passes) - 1})

            web_app = web.Application()
            web_app.router.add_get("/", _health)
            web_app.router.add_get("/resubmit/check", _resubmit_check)
            web_app.router.add_post("/resubmit/consume", _resubmit_consume)
            runner = web.AppRunner(web_app)
            await runner.setup()
            port = int(os.getenv("PORT", "10000"))
            await web.TCPSite(runner, "0.0.0.0", port).start()
            print(f"[voidhq] health server listening on port {port}")
        except Exception as e:
            print(f"Health server failed to start: {e}")

    global _keepalive_started
    if not _keepalive_started:
        _keepalive_started = True
        asyncio.create_task(_keepalive_loop())

    global _economy_autosave_started
    if not _economy_autosave_started:
        _economy_autosave_started = True
        asyncio.create_task(_economy_autosave_loop())


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    if not message.guild:
        await bot.process_commands(message)
        return

    # Passive VHQCoins drip — configurable in economy_config.json: pays out a random
    # reward once every N qualifying messages, gated by a minimum gap
    # between messages so it can't be farmed by spamming. Doesn't fire on
    # messages that turn out to be commands (checked further down) to
    # avoid double-dipping with !work/!daily etc.
    cfg = load_economy_config()
    if cfg.get("enabled", True):
        econ = get_user_economy(message.author.id)
        now = time.time()
        cooldown = cfg.get("passive_cooldown_seconds", 60)
        needed = max(1, cfg.get("passive_messages", 1))
        if now - econ.get("last_message_earn", 0) >= cooldown and not message.content.startswith(get_effective_prefix(message.guild.id)):
            streak = econ.get("passive_streak", 0) + 1
            updates = {"last_message_earn": now}
            if streak >= needed:
                p_min, p_max = cfg.get("passive_min", 1), cfg.get("passive_max", 3)
                if p_min > p_max:
                    p_min, p_max = p_max, p_min
                reward = random.randint(p_min, p_max) * _earn_rate_multiplier(econ) * (2 if now < econ.get("passive_boost_until", 0) else 1)
                add_coins(message.author.id, reward)
                streak = 0
            updates["passive_streak"] = streak
            set_user_economy(message.author.id, **updates)

    await bot.process_commands(message)


async def _safe_ctx_send(ctx: commands.Context, *, content: str | None = None, embed: discord.Embed | None = None, view: discord.ui.View | None = None, ephemeral: bool = False, delete_after: float | None = None, allowed_mentions: discord.AllowedMentions | None = None):
    """Centralized command response helper. Keeps prefix/slash responses consistent.

    Hybrid commands can expose an interaction-backed response where ephemeral
    is supported; prefix commands simply receive the normal message. The helper
    deliberately avoids passing optional arguments whose value is ``None`` to
    discord.py, because some discord.py builds reject those explicitly.
    """
    interaction = getattr(ctx, "interaction", None)
    try:
        kwargs = {}
        if content is not None:
            kwargs["content"] = content
        if embed is not None:
            kwargs["embed"] = embed
        if ephemeral:
            kwargs["ephemeral"] = True
        if view is not None:
            kwargs["view"] = view
        if allowed_mentions is not None:
            kwargs["allowed_mentions"] = allowed_mentions

        if interaction is not None:
            if not interaction.response.is_done():
                await interaction.response.send_message(**kwargs)
                if delete_after is not None:
                    try:
                        message = await interaction.original_response()
                        await message.delete(delay=delete_after)
                    except (discord.HTTPException, discord.NotFound):
                        pass
            else:
                await interaction.followup.send(**kwargs)
        else:
            prefix_kwargs = dict(kwargs)
            if delete_after is not None:
                prefix_kwargs["delete_after"] = delete_after
            await ctx.send(**prefix_kwargs)
    except (discord.HTTPException, discord.NotFound, TypeError):
        # A response failure must never become a second, confusing command
        # failure. Log it, then make one conservative fallback attempt for
        # interaction commands without optional parameters.
        logging.exception("Failed to send command response")
        if interaction is not None:
            try:
                fallback = {}
                if allowed_mentions is not None:
                    fallback["allowed_mentions"] = allowed_mentions
                if content is not None:
                    fallback["content"] = content
                elif embed is not None:
                    fallback["content"] = "⚠️ The command finished, but VOIDHQ could not render the rich response."
                if not interaction.response.is_done():
                    await interaction.response.send_message(**fallback, ephemeral=ephemeral)
                else:
                    await interaction.followup.send(**fallback, ephemeral=ephemeral)
            except Exception:
                logging.exception("Fallback command response also failed")


async def _safe_interaction_error(interaction: discord.Interaction, content: str) -> None:
    """Best-effort error response for slash/context-menu commands.

    This prevents an exception in an application command from leaving Discord
    stuck on the generic loading state with no useful message.
    """
    try:
        if not interaction.response.is_done():
            await interaction.response.send_message(content, ephemeral=True)
        else:
            await interaction.followup.send(content, ephemeral=True)
    except Exception:
        logging.exception("Failed to send application-command error response")


def _command_error_id() -> str:
    return f"VH-{secrets.token_hex(3).upper()}"


async def _owner_error_detail(user, error: BaseException) -> str:
    """Bot owners also get the exception type + message in the error reply, so
    the cause is visible in Discord without digging through the console log.
    Everyone else only sees the Error ID."""
    try:
        if await bot.is_owner(user):
            text = f"{type(error).__name__}: {error}".replace("`", "'")
            return f"\n🛠️ Owner debug: `{text[:300]}`"
    except Exception:
        pass
    return ""


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """Global slash-command error handler.

    Prefix commands have ``on_command_error`` below, but application
    commands use the command tree's error pipeline instead. Without this
    handler, an unexpected slash-command exception can leave the client on
    ``VOIDHQ is thinking`` and only show the traceback in the host console.
    """
    original = getattr(error, "original", error)
    name = getattr(getattr(interaction, "command", None), "qualified_name", None) or "unknown"

    if isinstance(original, app_commands.CommandOnCooldown):
        retry = max(0.0, float(original.retry_after))
        await _safe_interaction_error(interaction, f"⏳ **On cooldown**\nTry again in **{retry:.1f}s**.")
        return
    if isinstance(original, app_commands.MissingPermissions):
        missing = ", ".join(original.missing_permissions) or "the required permission"
        await _safe_interaction_error(interaction, f"🔒 **Permission denied**\nYou need: `{missing}`")
        return
    if isinstance(original, app_commands.BotMissingPermissions):
        missing = ", ".join(original.missing_permissions) or "the required permission"
        await _safe_interaction_error(interaction, f"🔐 **I need more permissions**\nMissing: `{missing}`")
        return
    if isinstance(original, app_commands.CheckFailure):
        await _safe_interaction_error(interaction, "🔒 **You can't use this command here or right now.**")
        return

    error_id = _command_error_id()
    logging.error("Unhandled application command error [%s] in %s", error_id, name, exc_info=(type(original), original, getattr(original, "__traceback__", None)))
    await _safe_interaction_error(
        interaction,
        f"⚠️ **Something went wrong**\nError ID: `{error_id}`\nPlease try again. If it keeps happening, give an admin this ID." + await _owner_error_detail(interaction.user, original),
    )


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    # Ignore errors already handled by local command code. For everything else,
    # give the user a useful, compact message and keep the full traceback in logs.
    original = getattr(error, "original", error)
    if isinstance(original, commands.CommandNotFound):
        await _safe_ctx_send(ctx, content="❓ **Unknown command**")
        return
    if isinstance(original, commands.MissingPermissions):
        await _safe_ctx_send(ctx, content=f"🔒 **Permission denied**\nYou need: `{', '.join(original.missing_permissions)}`")
        return
    if isinstance(original, commands.BotMissingPermissions):
        await _safe_ctx_send(ctx, content=f"🔐 **I need more permissions**\nMissing: `{', '.join(original.missing_permissions)}`")
        return
    if isinstance(original, commands.MemberNotFound):
        await _safe_ctx_send(ctx, content="👤 **Member not found**\nUse Discord's member picker or check the name/mention.")
        return
    if isinstance(original, commands.MissingRequiredArgument):
        await _safe_ctx_send(ctx, content=f"📝 **Missing option**\nPlease provide `{original.param.name}` and try again.")
        return
    if isinstance(original, commands.NotOwner):
        await _safe_ctx_send(ctx, content="👑 **Owner only**\nYou don't have access to this command.")
        return
    if isinstance(original, commands.CommandOnCooldown):
        retry = max(0, original.retry_after)
        await _safe_ctx_send(ctx, content=f"⏳ **On cooldown**\nTry again in **{retry:.1f}s**.")
        return
    if isinstance(original, commands.CheckFailure):
        # A check said no. This is expected, not a crash - it used to fall through
        # to the generic "Something went wrong" + Error ID message. The global
        # command cooldown has already told the user "Slow down", so stay quiet
        # for that one; other checks get a short generic message.
        if "global check" not in str(original).lower():
            await _safe_ctx_send(ctx, content="🔒 **You can't use this command here or right now.**")
        return
    if isinstance(original, (commands.BadArgument, commands.UserInputError)):
        await _safe_ctx_send(ctx, content="📝 **Invalid input**\nCheck the command's options and try again.")
        return


    error_id = _command_error_id()
    logging.error("Unhandled command error [%s] in %s", error_id, getattr(ctx.command, "qualified_name", "unknown"), exc_info=(type(original), original, getattr(original, "__traceback__", None)))
    await _safe_ctx_send(ctx, content=f"⚠️ **Something went wrong**\nError ID: `{error_id}`\nPlease try again. If it persists, give an admin this ID." + await _owner_error_detail(ctx.author, original))


# ---------------------------------------------------------------------------
# Economy commands (VHQCoins) — hybrid = usable as !command or /command
# ---------------------------------------------------------------------------

def _format_econ_name(member: discord.Member | discord.User, econ: dict) -> str:
    badge = "🏅 " if "vip_badge" in econ.get("inventory", []) else ""
    return f"{badge}{member.display_name}"


def _theme_color(econ: dict) -> int:
    theme_id = econ.get("active_theme")
    item = get_shop_item(theme_id) if theme_id else None
    return item["color"] if item and item.get("kind") == "theme" else 0x7CF5FF


async def _economy_gate(ctx: commands.Context) -> bool:
    """Checks the global economy on/off switch ("enabled" in economy_config.json)
    before any economy command runs. Sends a heads-up and returns False if
    disabled; returns True (does nothing) if the command should proceed."""
    if not economy_enabled():
        prefix = get_effective_prefix(ctx.guild.id if ctx.guild else None)
        await ctx.send(f"💤 The VHQCoins economy is currently disabled. Ask a bot admin to re-enable it.")
        return False
    return True


# In-memory only, on purpose — this is a rate-limit meant to space out rapid
# repeat command spam within the current session; it doesn't need to survive
# a restart, and skipping a persisted file avoids one more disk write per
# command.
_last_command_at: dict[int, float] = {}


@bot.check
async def _universal_command_cooldown(ctx: commands.Context) -> bool:
    """Configurable ("universal_cooldown_seconds" in economy_config.json) global cooldown: after
    running ANY command — prefix or slash, any category — a member has to
    wait this many seconds before running another one. Set to 0 to
    turn it off. Registered as a global check via @bot.check,
    so it runs before every command, including hybrid commands invoked as
    slash commands (those still build a commands.Context and go through the
    same check pipeline as prefix invocations)."""
    seconds = load_economy_config().get("universal_cooldown_seconds", 0)
    if seconds <= 0:
        return True

    now = time.time()
    last = _last_command_at.get(ctx.author.id, 0.0)
    remaining = seconds - (now - last)
    if remaining > 0:
        await _safe_ctx_send(ctx, content=f"⏳ **Slow down**\nYou can run another command in **{remaining:.1f}s**.")
        return False

    _last_command_at[ctx.author.id] = now
    return True


VIRTUAL_DISCLAIMER = "🪙 VHQCoins are a virtual, in-game currency with no real-world value. They can't be bought, sold, or cashed out."


async def _games_gate(ctx: commands.Context) -> bool:
    """Gate for every coin game: economy must be on, and (by default) the
    channel must be age-restricted. Shows the virtual-currency disclaimer
    when it blocks."""
    if not await _economy_gate(ctx):
        return False
    # Env var GAMES_AGE_RESTRICTED_ONLY ("true"/"false") overrides the config file, so it can be
    # switched in the host's Environment settings without touching economy_config.json.
    env_override = os.getenv("GAMES_AGE_RESTRICTED_ONLY", "").strip().lower()
    if env_override in ("true", "1", "yes"):
        restricted = True
    elif env_override in ("false", "0", "no"):
        restricted = False
    else:
        restricted = load_economy_config().get("games_age_restricted_only", True)
    if restricted:
        channel = ctx.channel
        is_nsfw = bool(ctx.guild) and hasattr(channel, "is_nsfw") and channel.is_nsfw()
        if not is_nsfw:
            await _safe_ctx_send(
                ctx,
                content="🔞 **Coin games are only available in age-restricted channels.**\n"
                        f"Ask an admin to enable them in a channel marked as age-restricted.\n*{VIRTUAL_DISCLAIMER}*",
                ephemeral=True,
            )
            return False
    return True


def _cooldown_status(elapsed: float, cooldown_seconds: float) -> str:
    """Shared formatter for the !stats cooldown fields — 'Ready now' if the
    cooldown has passed, otherwise a human 'Xh Ym' / 'Xm Ys' countdown."""
    if cooldown_seconds <= 0 or elapsed >= cooldown_seconds:
        return "✅ Ready now"
    remaining = int(cooldown_seconds - elapsed)
    if remaining >= 3600:
        return f"⏳ {remaining // 3600}h {(remaining % 3600) // 60}m"
    return f"⏳ {remaining // 60}m {remaining % 60}s"


@bot.hybrid_command(name="stats", aliases=["balance"], description="Your (or someone else's) full VHQCoins stats — balance, luck, boosts, cooldowns, and items")
@app_commands.describe(member="Whose stats to check (defaults to you)")
async def stats(ctx: commands.Context, member: discord.Member = None):
    if not await _economy_gate(ctx):
        return
    target = member or ctx.author
    econ = get_user_economy(target.id)
    cfg = load_economy_config()
    now = time.time()

    embed = discord.Embed(
        title=f"📊 {_format_econ_name(target, econ)}'s stats",
        description=f"**{econ['balance']:,}** 🪙 VHQCoins",
        color=_theme_color(econ),
    )

    if has_active_boost(econ):
        remaining = int(econ["boost_until"] - now)
        embed.add_field(name="⚡ Active boost", value=f"{_earn_rate_multiplier(econ)}x earnings for {remaining // 60}m {remaining % 60}s", inline=False)
    if get_luck_percent(econ):
        remaining = int(econ["luck_until"] - now)
        embed.add_field(
            name="🍀 Luck",
            value=f"+{get_luck_percent(econ)}% edge on crime, cf, and more — {remaining // 60}m {remaining % 60}s left",
            inline=False,
        )

    embed.add_field(name="☀️ Daily", value=_cooldown_status(now - econ.get("last_daily", 0.0), cfg.get("daily_cooldown_hours", 20) * 3600), inline=True)
    embed.add_field(name="🗓️ Weekly", value=_cooldown_status(now - econ.get("last_weekly", 0.0), cfg.get("weekly_cooldown_days", 7) * 86400), inline=True)
    embed.add_field(name="💼 Work", value=_cooldown_status(now - econ.get("last_work", 0.0), cfg.get("work_cooldown_minutes", 30) * 60), inline=True)
    embed.add_field(name="🙏 Beg", value=_cooldown_status(now - econ.get("last_beg", 0.0), cfg.get("beg_cooldown_minutes", 5) * 60), inline=True)
    embed.add_field(name="🎣 Fish", value=_cooldown_status(now - econ.get("last_fish", 0.0), cfg.get("fish_cooldown_minutes", 15) * 60), inline=True)
    embed.add_field(name="🕶️ Crime", value=_cooldown_status(now - econ.get("last_crime", 0.0), cfg.get("crime_cooldown_minutes", 45) * 60), inline=True)
    embed.add_field(name="🍀 Luck roll", value=_cooldown_status(now - econ.get("last_luck", 0.0), cfg.get("luck_cooldown_hours", 24) * 3600), inline=True)

    passes = valid_resubmit_passes(econ)
    if passes:
        embed.add_field(name="🔁 Resubmit Pass", value=f"{len(passes)} active — next expires <t:{int(passes[0])}:R>", inline=False)

    if econ["inventory"]:
        owned_names = [get_shop_item(i)["name"] for i in econ["inventory"] if get_shop_item(i)]
        embed.add_field(name="🎒 Items owned", value=", ".join(owned_names) or "—", inline=False)

    embed.set_thumbnail(url=target.display_avatar.url)
    prefix = get_effective_prefix(ctx.guild.id if ctx.guild else None)
    embed.set_footer(text=f"Try {prefix}shop to spend your coins • Virtual currency, no real-world value")
    await ctx.send(embed=embed)


@bot.hybrid_command(name="daily", description="Claim your daily VHQCoins reward")
async def daily(ctx: commands.Context):
    if not await _economy_gate(ctx):
        return
    cfg = load_economy_config()
    econ = get_user_economy(ctx.author.id)
    now = time.time()
    cooldown = cfg.get("daily_cooldown_hours", 20) * 3600
    elapsed = now - econ["last_daily"]
    if elapsed < cooldown:
        remaining = int(cooldown - elapsed)
        await ctx.send(f"⏳ You already claimed today's reward. Try again in {remaining // 3600}h {(remaining % 3600) // 60}m.")
        return
    reward = random.randint(cfg.get("daily_min", 150), max(cfg.get("daily_min", 150), cfg.get("daily_max", 300))) * _earn_rate_multiplier(econ)
    set_user_economy(ctx.author.id, last_daily=now)
    new_balance = add_coins(ctx.author.id, reward)
    await ctx.send(f"🪙 You claimed your daily reward: **+{reward}** VHQCoins! (Balance: **{new_balance:,}**)")


@bot.hybrid_command(name="weekly", description="Claim your weekly VHQCoins reward — a bigger payday than !daily")
async def weekly(ctx: commands.Context):
    if not await _economy_gate(ctx):
        return
    cfg = load_economy_config()
    econ = get_user_economy(ctx.author.id)
    now = time.time()
    cooldown = cfg.get("weekly_cooldown_days", 7) * 86400
    elapsed = now - econ.get("last_weekly", 0.0)
    if elapsed < cooldown:
        remaining = int(cooldown - elapsed)
        await ctx.send(f"⏳ You already claimed this week's reward. Try again in {remaining // 86400}d {(remaining % 86400) // 3600}h.")
        return
    reward = random.randint(cfg.get("weekly_min", 800), max(cfg.get("weekly_min", 800), cfg.get("weekly_max", 1500))) * _earn_rate_multiplier(econ)
    set_user_economy(ctx.author.id, last_weekly=now)
    new_balance = add_coins(ctx.author.id, reward)
    await ctx.send(f"🗓️ You claimed your weekly reward: **+{reward}** VHQCoins! (Balance: **{new_balance:,}**)")


@bot.hybrid_command(name="work", description="Work a quick shift to earn VHQCoins")
async def work(ctx: commands.Context):
    if not await _economy_gate(ctx):
        return
    cfg = load_economy_config()
    econ = get_user_economy(ctx.author.id)
    now = time.time()
    cooldown = cfg.get("work_cooldown_minutes", 30) * 60
    elapsed = now - econ["last_work"]
    if elapsed < cooldown:
        remaining = int(cooldown - elapsed)
        await ctx.send(f"⏳ You're still on cooldown. Try again in {remaining // 60}m {remaining % 60}s.")
        return
    gigs = [
        "moderated a chaotic server for an hour",
        "helped someone identify a song from a 2-second clip",
        "downloaded a video that was somehow region-locked three times over",
        "answered the same question in #welcome for the fifth time today",
        "kept the bot dashboard from catching fire",
    ]
    reward = random.randint(cfg.get("work_min", 20), max(cfg.get("work_min", 20), cfg.get("work_max", 60))) * _earn_rate_multiplier(econ)
    set_user_economy(ctx.author.id, last_work=now)
    new_balance = add_coins(ctx.author.id, reward)
    await ctx.send(f"💼 You {random.choice(gigs)} and earned **+{reward}** VHQCoins! (Balance: **{new_balance:,}**)")


BEG_FLAVOR_GIVE = [
    "A stranger tossed you some spare change",
    "Someone in #general took pity on you",
    "A passing NPC handed over their pocket change",
    "A kind soul dropped a few coins in your hat",
]
BEG_FLAVOR_NOTHING = [
    "Nobody had anything to spare",
    "You got waved off by everyone who walked by",
    "A stranger just told you to get a job",
    "No luck — everyone else is broke too",
]


@bot.hybrid_command(name="beg", description="Beg for spare VHQCoins — small reward, short cooldown, but can come up empty")
async def beg(ctx: commands.Context):
    if not await _economy_gate(ctx):
        return
    cfg = load_economy_config()
    econ = get_user_economy(ctx.author.id)
    now = time.time()
    cooldown = cfg.get("beg_cooldown_minutes", 5) * 60
    elapsed = now - econ.get("last_beg", 0.0)
    if elapsed < cooldown:
        remaining = int(cooldown - elapsed)
        await ctx.send(f"⏳ Give it a bit. Try again in {remaining // 60}m {remaining % 60}s.")
        return
    set_user_economy(ctx.author.id, last_beg=now)
    if random.random() > cfg.get("beg_success_chance", 0.8):
        await ctx.send(f"🙏 {random.choice(BEG_FLAVOR_NOTHING)}. Nothing this time — try again soon.")
        return
    reward = random.randint(cfg.get("beg_min", 5), max(cfg.get("beg_min", 5), cfg.get("beg_max", 25))) * _earn_rate_multiplier(econ)
    new_balance = add_coins(ctx.author.id, reward)
    await ctx.send(f"🙏 {random.choice(BEG_FLAVOR_GIVE)} — **+{reward}** VHQCoins! (Balance: **{new_balance:,}**)")


FISH_CATCHES = [
    "an old boot",
    "a shiny trout",
    "a handful of soggy coins",
    "a tiny crab",
    "a rusty treasure chest",
    "a suspiciously large tuna",
]


@bot.hybrid_command(name="fish", description="Go fishing for VHQCoins")
async def fish(ctx: commands.Context):
    if not await _economy_gate(ctx):
        return
    cfg = load_economy_config()
    econ = get_user_economy(ctx.author.id)
    now = time.time()
    cooldown = cfg.get("fish_cooldown_minutes", 15) * 60
    elapsed = now - econ.get("last_fish", 0.0)
    if elapsed < cooldown:
        remaining = int(cooldown - elapsed)
        await ctx.send(f"⏳ Your line's still out. Try again in {remaining // 60}m {remaining % 60}s.")
        return
    reward = random.randint(cfg.get("fish_min", 15), max(cfg.get("fish_min", 15), cfg.get("fish_max", 80))) * _earn_rate_multiplier(econ)
    set_user_economy(ctx.author.id, last_fish=now)
    new_balance = add_coins(ctx.author.id, reward)
    await ctx.send(f"🎣 You reeled in {random.choice(FISH_CATCHES)} and sold it for **+{reward}** VHQCoins! (Balance: **{new_balance:,}**)")


@bot.hybrid_command(name="cf", description="Flip a coin and bet VHQCoins — win doubles it, lose loses it")
@app_commands.describe(amount='How much to bet — a number, "all", or "half"', side='Call it: heads or tails')
@app_commands.choices(side=[
    app_commands.Choice(name="Heads", value="heads"),
    app_commands.Choice(name="Tails", value="tails"),
])
async def cf(ctx: commands.Context, amount: str, side: str = "heads"):
    if not await _games_gate(ctx):
        return
    econ = get_user_economy(ctx.author.id)
    balance = econ["balance"]

    normalized = amount.strip().lower()
    if normalized == "all":
        bet = balance
    elif normalized == "half":
        bet = balance // 2
    else:
        try:
            bet = int(normalized.replace(",", ""))
        except ValueError:
            await ctx.send('Give an amount to bet — a number, `all`, or `half`.')
            return

    if bet <= 0:
        await ctx.send("Bet has to be more than 0 VHQCoins.")
        return
    if bet > balance:
        await ctx.send(f"You only have **{balance:,}** VHQCoins — can't bet **{bet:,}**.")
        return

    call = side.strip().lower()
    if call not in ("heads", "tails"):
        await ctx.send('Call it `heads` or `tails`.')
        return

    # Configurable house edge — defaults to a true 50/50 coin.
    # Luck (see the LUCK SYSTEM note near get_luck_bonus) chips away at
    # whatever edge is configured, clamped so it can never fully disappear.
    cf_lose_chance = load_economy_config().get("cf_lose_chance", 0.5)
    lose_chance = max(0.05, cf_lose_chance - get_luck_bonus(econ))
    won = random.random() > lose_chance
    result = call if won else (("tails" if call == "heads" else "heads"))
    if won:
        win_amount = _owner_payout(ctx.author.id, bet, 0)
        new_balance = add_coins(ctx.author.id, win_amount)
        await ctx.send(f"🪙 {result.capitalize()}! You called it — won **+{win_amount:,}** VHQCoins! (Balance: **{new_balance:,}**)")
    else:
        new_balance = add_coins(ctx.author.id, -bet)
        await ctx.send(f"💀 {result.capitalize()}. You called {call} — lost **{bet:,}** VHQCoins. (Balance: **{new_balance:,}**)")


@bot.hybrid_command(name="give", description="Give some of your VHQCoins to another member")
@app_commands.describe(member="Who to give coins to", amount="How many VHQCoins to give")
async def give(ctx: commands.Context, member: discord.Member, amount: int):
    if not await _economy_gate(ctx):
        return
    if member.bot:
        await ctx.send("Bots don't need VHQCoins.")
        return
    if member.id == ctx.author.id:
        await ctx.send("You can't give coins to yourself.")
        return
    if amount <= 0:
        await ctx.send("Give a positive amount.")
        return
    # Owner: no balance needed (coins are created, not deducted). Everyone else: no cap, but the coins come from their own balance.
    if ctx.author.id in OWNER_IDS or await ctx.bot.is_owner(ctx.author):
        add_coins(member.id, amount)
        await ctx.send(f"🤝 {ctx.author.mention} gave **{amount:,}** VHQCoins to {member.mention}!")
        return
    econ = get_user_economy(ctx.author.id)
    if econ["balance"] < amount:
        await ctx.send(f"You only have **{econ['balance']:,}** VHQCoins.")
        return
    add_coins(ctx.author.id, -amount)
    add_coins(member.id, amount)
    await ctx.send(f"🤝 {ctx.author.mention} gave **{amount:,}** VHQCoins to {member.mention}!")


CRIME_FLAVOR = [
    "hacked a vending machine",
    "pickpocketed a rich NPC",
    "ran an elaborate Discord Nitro scam",
    "sold counterfeit VIP badges",
    "raided an abandoned server's vault",
    "swiped someone's unattended !daily claim",
]


@bot.hybrid_command(name="crime", description="Attempt a risky crime — chance to win VHQCoins, chance to lose a % of your balance")
async def crime(ctx: commands.Context):
    if not await _economy_gate(ctx):
        return
    cfg = load_economy_config()
    econ = get_user_economy(ctx.author.id)
    now = time.time()
    cooldown = cfg.get("crime_cooldown_minutes", 45) * 60
    elapsed = now - econ.get("last_crime", 0.0)
    if elapsed < cooldown:
        remaining = int(cooldown - elapsed)
        await ctx.send(f"🚓 You're laying low after your last job. Try another crime in {remaining // 60}m {remaining % 60}s.")
        return

    set_user_economy(ctx.author.id, last_crime=now)

    # Luck nudges the success chance up, clamped so a crime is never a sure
    # thing — see the LUCK SYSTEM note near get_luck_bonus() above.
    base_chance = cfg.get("crime_success_chance", 0.55)
    chance = min(0.9, base_chance + get_luck_bonus(econ))

    if random.random() < chance:
        reward = random.randint(
            cfg.get("crime_min", 50), max(cfg.get("crime_min", 50), cfg.get("crime_max", 200))
        ) * _earn_rate_multiplier(econ)
        new_balance = add_coins(ctx.author.id, reward)
        await ctx.send(
            f"🕶️ You {random.choice(CRIME_FLAVOR)} and got away with **+{reward:,}** VHQCoins! (Balance: **{new_balance:,}**)"
        )
    else:
        balance = econ["balance"]
        pct = random.uniform(
            cfg.get("crime_fail_min_pct", 10), max(cfg.get("crime_fail_min_pct", 10), cfg.get("crime_fail_max_pct", 30))
        )
        loss = int(balance * pct / 100)
        new_balance = add_coins(ctx.author.id, -loss)
        await ctx.send(
            f"🚨 Busted! You got caught and paid **{loss:,}** VHQCoins (**{pct:.0f}%** of your balance) in fines. (Balance: **{new_balance:,}**)"
        )


@bot.hybrid_command(name="luck", description="Once a day: gain 1-10 luck points for 1 hour, each worth a 1% edge on crime, cf, and more")
async def luck(ctx: commands.Context):
    if not await _economy_gate(ctx):
        return
    cfg = load_economy_config()
    econ = get_user_economy(ctx.author.id)
    now = time.time()
    cooldown = cfg.get("luck_cooldown_hours", 24) * 3600
    elapsed = now - econ.get("last_luck", 0.0)
    if elapsed < cooldown:
        remaining = int(cooldown - elapsed)
        await ctx.send(f"🍀 You've already pushed your luck today. Try again in {remaining // 3600}h {(remaining % 3600) // 60}m.")
        return

    gained = random.randint(
        cfg.get("luck_min_points", 1), max(cfg.get("luck_min_points", 1), cfg.get("luck_max_points", 10))
    )
    duration_seconds = cfg.get("luck_duration_minutes", 60) * 60
    luck_until = now + duration_seconds
    set_user_economy(ctx.author.id, luck=gained, luck_until=luck_until, last_luck=now)

    prefix = get_effective_prefix(ctx.guild.id if ctx.guild else None)
    duration_minutes = cfg.get("luck_duration_minutes", 60)
    await ctx.send(
        f"🍀 Luck is on your side — **+{gained}%** edge for the next **{duration_minutes} minutes**! "
        f"That's a small boost on `{prefix}crime`, `{prefix}cf`, and future economy commands too. "
        f"Come back tomorrow for another shot."
    )


@bot.command(name="addmoney")
@owner_only()
async def addmoney(ctx: commands.Context, amount: int):
    """Owner-only. Mints VHQCoins straight into your own balance — no
    economy on/off check, no balance check, no upper limit. Deliberately
    NOT a hybrid command (no slash-command form): this bypasses the normal
    economy rules entirely, so it stays out of the public /command list and
    off the !help panel, reachable only via the prefix by whoever
    bot.is_owner() recognizes — the account listed as the bot application's
    owner (or a team member) in the Discord Developer Portal, same check
    used by !cookiestatus, !potupdate, etc. elsewhere in this file."""
    if amount == 0:
        await ctx.send("Give a non-zero amount.")
        return
    new_balance = add_coins(ctx.author.id, amount)
    verb = "Minted" if amount > 0 else "Removed"
    await ctx.send(f"🪙 {verb} **{abs(amount):,}** VHQCoins. (Balance: **{new_balance:,}**)")


@bot.command(name="giveall")
@owner_only()
@commands.guild_only()
async def giveall(ctx: commands.Context, amount: int):
    """Owner-only, prefix only: gives `amount` VHQCoins to every non-bot member of this
    server. Coins are created (nothing is deducted from you). Usage: !giveall 5000"""
    if amount <= 0:
        await ctx.send("Amount must be above 0.")
        return
    # The bot doesn't cache every member (chunk_guilds_at_startup=False), so ctx.guild.members
    # would only contain a few people. fetch_members asks Discord for the full list.
    count = 0
    async for member in ctx.guild.fetch_members(limit=None):
        if member.bot:
            continue
        add_coins(member.id, amount)
        count += 1
    await ctx.send(f"🎁 Gave **{amount:,}** VHQCoins to **{count:,}** members!")


@bot.command(name="backup")
@owner_only()
async def backup_cmd(ctx: commands.Context):
    """Owner-only: posts economy.json (everyone's coins) as a file. Run this BEFORE every deploy."""
    if not os.path.exists(ECONOMY_FILE):
        await ctx.send("No economy.json yet.")
        return
    # write the live cache first so the backup is current
    await asyncio.get_running_loop().run_in_executor(None, _write_json_file, ECONOMY_FILE, copy.deepcopy(load_economy()))
    await ctx.send("💾 Backup — keep this file safe:", file=discord.File(ECONOMY_FILE, filename="economy.json"))


@bot.command(name="restore")
@owner_only()
async def restore_cmd(ctx: commands.Context):
    """Owner-only: attach a backed-up economy.json to this command to load it back in."""
    if not ctx.message.attachments:
        await ctx.send("Attach your `economy.json` backup to the `!restore` message.")
        return
    try:
        data = json.loads((await ctx.message.attachments[0].read()).decode("utf-8"))
    except Exception:
        await ctx.send("That file isn't valid JSON.")
        return
    if not isinstance(data, dict):
        await ctx.send("That doesn't look like an economy backup.")
        return
    cache = load_economy()
    cache.clear()
    cache.update(data)
    save_economy(cache)
    await ctx.send(f"✅ Restored **{len(data):,}** users' economy data.")


@bot.command(name="ownerboost")
@owner_only()
async def ownerboost(ctx: commands.Context, mult: str = None):
    """Owner-only, prefix only (no slash form, not in !help). Sets a permanent
    multiplier on YOUR earnings only: !daily, !weekly, !work, !beg, !fish, !crime,
    chat coins, and the PROFIT of cf / slots / roulette / highlow / blackjack.
    Usage: !ownerboost 10   |   !ownerboost off   |   !ownerboost (shows current)"""
    econ = get_user_economy(ctx.author.id)
    if mult is None:
        await ctx.send(f"⚡ Owner boost: **{econ.get('owner_mult', 1)}x**")
        return
    if mult.lower() in ("off", "reset", "1"):
        set_user_economy(ctx.author.id, owner_mult=1)
        await ctx.send("⚡ Owner boost **off** (1x).")
        return
    try:
        value = int(mult)
    except ValueError:
        await ctx.send("Usage: `!ownerboost <number>` or `!ownerboost off`")
        return
    if value < 1 or value > 1_000_000:
        await ctx.send("Pick a number between 1 and 1,000,000.")
        return
    set_user_economy(ctx.author.id, owner_mult=value)
    await ctx.send(f"⚡ Owner boost set to **{value}x** — daily, weekly, work, crime, chat coins and all game wins.")


class DuelFight:
    """One in-progress Duel Fight lobby for a single server. Deliberately
    kept in memory only (see active_duel_fights below) — nothing about a
    lobby is written to disk, so a bot restart or code reload just clears
    whatever was pending instead of leaving a duel stuck that can never
    start, which is what happened with the old reaction-based version."""

    def __init__(self, guild_id: int, channel: discord.abc.Messageable, starter: discord.Member, amount: int):
        self.guild_id = guild_id
        self.channel = channel
        self.min_bet = amount  # floor every joiner's bet must match or beat
        self.participants: dict[int, int] = {starter.id: amount}  # user_id -> bet
        self.members: dict[int, discord.Member] = {starter.id: starter}
        self.message: discord.Message | None = None
        self.task: asyncio.Task | None = None


# One active Duel Fight per server at a time, keyed by guild ID.
active_duel_fights: dict[int, DuelFight] = {}


def _duel_fight_lobby_text(fight: DuelFight, seconds_left: int = None) -> str:
    prefix = get_effective_prefix(fight.guild_id)
    roster = "\n".join(
        f"⚔️ {fight.members[uid].mention} — **{bet:,}** 🪙"
        for uid, bet in fight.participants.items()
    )
    text = f"🗡️ **Duel Fight!**\n{roster}\n\n`{prefix}duel join <amount>` to enter — bet **{fight.min_bet:,}** 🪙 or more."
    if seconds_left is not None:
        text += f"\nStarting in **{seconds_left}s**..."
    return text


async def _duel_fight_countdown(fight: DuelFight, join_seconds: int):
    try:
        if join_seconds > 4:
            await asyncio.sleep(join_seconds / 2)
            if active_duel_fights.get(fight.guild_id) is fight and fight.message:
                try:
                    await fight.message.edit(content=_duel_fight_lobby_text(fight, seconds_left=round(join_seconds / 2)))
                except discord.HTTPException:
                    pass
            await asyncio.sleep(join_seconds / 2)
        else:
            await asyncio.sleep(join_seconds)
    except asyncio.CancelledError:
        return

    if active_duel_fights.get(fight.guild_id) is fight:
        await _resolve_duel_fight(fight)


async def _resolve_duel_fight(fight: DuelFight):
    # Pull the lobby the instant the timer ends, whatever happens next —
    # this is what guarantees a Duel Fight can never get stuck open.
    if active_duel_fights.get(fight.guild_id) is fight:
        del active_duel_fights[fight.guild_id]

    # Re-check every balance right before coins move — plenty of time
    # passed since anyone joined for a bet to no longer be covered.
    dropped = [uid for uid, bet in fight.participants.items() if get_user_economy(uid)["balance"] < bet]
    for uid in dropped:
        del fight.participants[uid]

    if len(fight.participants) < 2:
        try:
            await fight.channel.send(
                "🚫 Someone in the Duel Fight no longer has the coins to cover their bet — it's cancelled. No coins moved."
                if dropped else
                "🚫 Nobody else joined in time — the Duel Fight is cancelled."
            )
        except discord.HTTPException:
            pass
        return

    for uid, bet in fight.participants.items():
        add_coins(uid, -bet)

    try:
        msg = await fight.channel.send(f"⚔️ {len(fight.participants)} duelists draw their swords...")
        for line in ("🗡️ Steel meets steel...", "⚔️ *clash!*...", "⚔️ *clash!*..."):
            await asyncio.sleep(1.3)
            try:
                await msg.edit(content=line)
            except discord.HTTPException:
                pass
        await asyncio.sleep(1.3)
    except discord.HTTPException:
        pass

    # Equal chance for every participant, regardless of bet size.
    winner_id = random.choice(list(fight.participants.keys()))
    pot = sum(fight.participants.values())
    new_balance = add_coins(winner_id, pot)

    winner = fight.members.get(winner_id)
    winner_mention = winner.mention if winner else f"<@{winner_id}>"
    loser_mentions = ", ".join(
        (fight.members[uid].mention if uid in fight.members else f"<@{uid}>")
        for uid in fight.participants if uid != winner_id
    )

    try:
        await fight.channel.send(
            f"🏆 **{winner_mention}** wins the Duel Fight and takes the pot — **+{pot:,}** VHQCoins! "
            f"(Balance: **{new_balance:,}**)\n💀 {loser_mentions} fought bravely and lost their bet."
        )
    except discord.HTTPException:
        pass


@bot.hybrid_group(name="duel", invoke_without_command=True, description="Start or join a Duel Fight — a multiplayer sword-duel bet")
async def duel(ctx: commands.Context):
    prefix = get_effective_prefix(ctx.guild.id if ctx.guild else None)
    await ctx.send(f"⚔️ Start a Duel Fight with `{prefix}duel start <amount>`, or join one already running with `{prefix}duel join <amount>`.")


@duel.command(name="start", description="Start a Duel Fight that others can join before the timer runs out")
@app_commands.describe(amount="How many VHQCoins to bet")
async def duel_start(ctx: commands.Context, amount: int):
    if not await _games_gate(ctx):
        return
    if not ctx.guild:
        await ctx.send("Duel Fights only work in a server.")
        return
    if ctx.guild.id in active_duel_fights:
        prefix = get_effective_prefix(ctx.guild.id)
        await ctx.send(f"⚔️ There's already a Duel Fight running in this server — join it with `{prefix}duel join <amount>` instead.")
        return

    min_bet = load_economy_config().get("duel_min_bet", 100)
    if amount < min_bet:
        await ctx.send(f"Minimum duel bet is **{min_bet:,}** VHQCoins.")
        return

    econ = get_user_economy(ctx.author.id)
    if econ["balance"] < amount:
        await ctx.send(f"You only have **{econ['balance']:,}** VHQCoins.")
        return

    join_seconds = load_economy_config().get("duel_join_seconds", 30)
    fight = DuelFight(ctx.guild.id, ctx.channel, ctx.author, amount)
    active_duel_fights[ctx.guild.id] = fight
    fight.message = await ctx.send(_duel_fight_lobby_text(fight, seconds_left=join_seconds))
    fight.task = asyncio.create_task(_duel_fight_countdown(fight, join_seconds))


@duel.command(name="join", description="Join the Duel Fight currently running in this server")
@app_commands.describe(amount="How many VHQCoins to bet — must match or beat the starting bet")
async def duel_join(ctx: commands.Context, amount: int):
    if not await _games_gate(ctx):
        return
    if not ctx.guild:
        await ctx.send("Duel Fights only work in a server.")
        return

    fight = active_duel_fights.get(ctx.guild.id)
    if not fight:
        prefix = get_effective_prefix(ctx.guild.id)
        await ctx.send(f"There's no Duel Fight running in this server right now — start one with `{prefix}duel start <amount>`.")
        return
    if ctx.author.id in fight.participants:
        await ctx.send("You're already in this Duel Fight.")
        return
    if amount < fight.min_bet:
        await ctx.send(f"This Duel Fight needs a bet of at least **{fight.min_bet:,}** VHQCoins.")
        return

    econ = get_user_economy(ctx.author.id)
    if econ["balance"] < amount:
        await ctx.send(f"You only have **{econ['balance']:,}** VHQCoins.")
        return

    fight.participants[ctx.author.id] = amount
    fight.members[ctx.author.id] = ctx.author
    await ctx.send(f"⚔️ {ctx.author.mention} joins the Duel Fight with **{amount:,}** VHQCoins!")
    if fight.message:
        try:
            await fight.message.edit(content=_duel_fight_lobby_text(fight))
        except discord.HTTPException:
            pass


# ---------------------------------------------------------------------------
# !roulette — a shared 30s entry window, buttons instead of typed
# commands (same "table stays open, others can join before it resolves"
# shape as Duel Fight above, but a Modal collects the entry amount since a
# button click alone can't carry a number).
# ---------------------------------------------------------------------------

ROULETTE_COLOR_META = {
    "red": {"emoji": "🔴", "label": "Red"},
    "black": {"emoji": "⚫", "label": "Black"},
    "green": {"emoji": "🟢", "label": "Green"},
}


class RouletteTable:
    """One open table for a single server. In-memory only, same tradeoff
    as DuelFight — a restart just clears whatever was open."""

    def __init__(self, guild_id: int, channel: discord.abc.Messageable):
        self.guild_id = guild_id
        self.channel = channel
        self.entries: dict[int, tuple] = {}  # user_id -> (color, amount, member)
        self.message: discord.Message | None = None
        self.task: asyncio.Task | None = None


active_roulette_tables: dict[int, RouletteTable] = {}


def _roulette_table_text(table: RouletteTable, seconds_left: int = None) -> str:
    if table.entries:
        roster = "\n".join(
            f"{ROULETTE_COLOR_META[color]['emoji']} {member.mention} — **{amount:,}** 🪙 on {ROULETTE_COLOR_META[color]['label']}"
            for color, amount, member in table.entries.values()
        )
    else:
        roster = "*No entries placed yet.*"
    text = f"🎡 **Wheel Game is open!**\n{roster}\n\nTap a button below to place an entry."
    if seconds_left is not None:
        text += f"\nSpinning in **{seconds_left}s**..."
    return text


class RouletteEntryModal(discord.ui.Modal):
    def __init__(self, table: RouletteTable, color: str):
        super().__init__(title=f"Entry: {ROULETTE_COLOR_META[color]['label']}")
        self.table = table
        self.color = color
        self.amount_input = discord.ui.TextInput(
            label="Entry amount (VHQCoins)",
            placeholder='A number, "all", or "half"',
            required=True,
            max_length=12,
        )
        self.add_item(self.amount_input)

    async def on_submit(self, interaction: discord.Interaction):
        if active_roulette_tables.get(self.table.guild_id) is not self.table:
            await interaction.response.send_message("This Wheel Game table already closed.", ephemeral=True)
            return
        if not economy_enabled():
            await interaction.response.send_message("💤 The VHQCoins economy is currently disabled.", ephemeral=True)
            return

        econ = get_user_economy(interaction.user.id)
        balance = econ["balance"]
        normalized = self.amount_input.value.strip().lower()
        if normalized == "all":
            entry = balance
        elif normalized == "half":
            entry = balance // 2
        else:
            try:
                entry = int(normalized.replace(",", ""))
            except ValueError:
                await interaction.response.send_message('Give an amount to enter with — a number, "all", or "half".', ephemeral=True)
                return

        min_entry = load_economy_config().get("roulette_min_entry", 10)
        if entry < min_entry:
            await interaction.response.send_message(f"Minimum wheel game entry is **{min_entry:,}** VHQCoins.", ephemeral=True)
            return
        if entry > balance:
            await interaction.response.send_message(f"You only have **{balance:,}** VHQCoins — can't enter with **{entry:,}**.", ephemeral=True)
            return

        self.table.entries[interaction.user.id] = (self.color, entry, interaction.user)
        await interaction.response.send_message(
            f"{ROULETTE_COLOR_META[self.color]['emoji']} Entry placed: **{entry:,}** 🪙 on {ROULETTE_COLOR_META[self.color]['label']}.",
            ephemeral=True,
        )
        if self.table.message:
            try:
                await self.table.message.edit(content=_roulette_table_text(self.table))
            except discord.HTTPException:
                pass


class RouletteColorButton(discord.ui.Button):
    def __init__(self, color: str):
        meta = ROULETTE_COLOR_META[color]
        style = {"red": discord.ButtonStyle.danger, "black": discord.ButtonStyle.secondary, "green": discord.ButtonStyle.success}[color]
        super().__init__(label=meta["label"], emoji=meta["emoji"], style=style)
        self.color = color

    async def callback(self, interaction: discord.Interaction):
        table = self.view.table
        if active_roulette_tables.get(table.guild_id) is not table:
            await interaction.response.send_message("This Wheel Game table already closed.", ephemeral=True)
            return
        await interaction.response.send_modal(RouletteEntryModal(table, self.color))


class RouletteView(discord.ui.View):
    def __init__(self, table: RouletteTable, seconds: int):
        super().__init__(timeout=seconds + 5)
        self.table = table
        for color in ("red", "black", "green"):
            self.add_item(RouletteColorButton(color))

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        if self.table.message:
            try:
                await self.table.message.edit(view=self)
            except discord.HTTPException:
                pass


async def _roulette_countdown(table: RouletteTable, entry_seconds: int):
    try:
        if entry_seconds > 4:
            await asyncio.sleep(entry_seconds / 2)
            if active_roulette_tables.get(table.guild_id) is table and table.message:
                try:
                    await table.message.edit(content=_roulette_table_text(table, seconds_left=round(entry_seconds / 2)))
                except discord.HTTPException:
                    pass
            await asyncio.sleep(entry_seconds / 2)
        else:
            await asyncio.sleep(entry_seconds)
    except asyncio.CancelledError:
        return

    if active_roulette_tables.get(table.guild_id) is table:
        await _resolve_roulette(table)


async def _resolve_roulette(table: RouletteTable):
    if active_roulette_tables.get(table.guild_id) is table:
        del active_roulette_tables[table.guild_id]

    if not table.entries:
        try:
            await table.channel.send("🎡 Nobody placed an entry — the table closes with no spin.")
        except discord.HTTPException:
            pass
        return

    # Re-check every balance right before coins move — plenty of time
    # passed since anyone placed an entry for it to no longer be covered.
    dropped = [uid for uid, (color, amount, member) in table.entries.items() if get_user_economy(uid)["balance"] < amount]
    for uid in dropped:
        del table.entries[uid]

    if not table.entries:
        try:
            await table.channel.send("🚫 Nobody at the table could still cover their entry — the round is cancelled.")
        except discord.HTTPException:
            pass
        return

    for uid, (color, amount, member) in table.entries.items():
        add_coins(uid, -amount)

    # American-wheel-style odds: 18 red, 18 black, 2 green out of 38 —
    # one shared spin for the whole table, same as a real wheel.
    winning_color = random.choices(["red", "black", "green"], weights=[18, 18, 2], k=1)[0]

    try:
        msg = await table.channel.send("🎡 The wheel is spinning...")
        for line in ("🎡 Spinning...", "🎡 Slowing down...",
                     f"🎡 It lands on... {ROULETTE_COLOR_META[winning_color]['emoji']} **{ROULETTE_COLOR_META[winning_color]['label']}**!"):
            await asyncio.sleep(1.3)
            try:
                await msg.edit(content=line)
            except discord.HTTPException:
                pass
    except discord.HTTPException:
        pass

    cfg = load_economy_config()
    red_black_mult = cfg.get("roulette_red_black_multiplier", 2.0)
    green_mult = cfg.get("roulette_green_multiplier", 14.0)

    lines = []
    for uid, (color, amount, member) in table.entries.items():
        econ = get_user_economy(uid)
        if color == winning_color:
            mult = green_mult if color == "green" else red_black_mult
            payout = _owner_payout(uid, int(amount * mult), amount)
            new_balance = add_coins(uid, payout)
            lines.append(f"🏆 {member.mention} won **+{payout:,}** 🪙 on {ROULETTE_COLOR_META[color]['label']} (x{mult})! (Balance: **{new_balance:,}**)")
        else:
            # Luck (LUCK SYSTEM note): a single shared spin can't nudge
            # each player's own odds of winning individually, so luck is
            # applied here as a personal "insurance" chance instead — a
            # losing entry has a luck-scaled chance to be refunded rather
            # than lost outright.
            if random.random() < get_luck_bonus(econ):
                new_balance = add_coins(uid, amount)
                lines.append(f"🍀 {member.mention}'s entry on {ROULETTE_COLOR_META[color]['label']} lost, but luck refunded **{amount:,}** 🪙. (Balance: **{new_balance:,}**)")
            else:
                lines.append(f"💀 {member.mention} lost **{amount:,}** 🪙 on {ROULETTE_COLOR_META[color]['label']}.")

    try:
        await table.channel.send("\n".join(lines))
    except discord.HTTPException:
        pass


@bot.hybrid_command(name="roulette", description="Open a 30-second Wheel Game table — choose Red, Black, or Green with buttons")
async def roulette(ctx: commands.Context):
    if not await _games_gate(ctx):
        return
    if not ctx.guild:
        await ctx.send("Wheel Game only works in a server.")
        return
    if ctx.guild.id in active_roulette_tables:
        await ctx.send("🎡 There's already a Wheel Game table open in this server — place your entry on it with the buttons above.")
        return

    entry_seconds = load_economy_config().get("roulette_entry_seconds", 30)
    table = RouletteTable(ctx.guild.id, ctx.channel)
    active_roulette_tables[ctx.guild.id] = table
    view = RouletteView(table, entry_seconds)
    table.message = await ctx.send(_roulette_table_text(table, seconds_left=entry_seconds), view=view)
    table.task = asyncio.create_task(_roulette_countdown(table, entry_seconds))


# ---------------------------------------------------------------------------
# !slots — 3 reels drawn from a weighted, configurable symbol table.
# 3-of-a-kind pays that symbol's own multiplier (a real slot machine pays
# different amounts per symbol); 2-of-a-kind pays a small flat consolation.
# ---------------------------------------------------------------------------

def _pick_slot_symbol(symbols: list) -> dict:
    weights = [max(0, s.get("weight", 1)) for s in symbols]
    if not any(weights):
        weights = [1] * len(symbols)
    return random.choices(symbols, weights=weights, k=1)[0]


def _parse_entry_amount(raw: str, balance: int):
    """Shared parser for the "<number>|all|half" amount syntax used by
    !cf, !slots, and !highlow. Returns an int, or None if unparseable."""
    normalized = raw.strip().lower()
    if normalized == "all":
        return balance
    if normalized == "half":
        return balance // 2
    try:
        return int(normalized.replace(",", ""))
    except ValueError:
        return None


@bot.hybrid_command(name="slots", description="Spin the reels and enter with VHQCoins — match symbols for a payout")
@app_commands.describe(amount='How much to enter with — a number, "all", or "half"')
async def slots(ctx: commands.Context, amount: str):
    if not await _games_gate(ctx):
        return
    econ = get_user_economy(ctx.author.id)
    balance = econ["balance"]

    entry = _parse_entry_amount(amount, balance)
    if entry is None:
        await ctx.send('Give an amount to enter with — a number, `all`, or `half`.')
        return

    cfg = load_economy_config()
    min_entry = cfg.get("slots_min_entry", 10)
    if entry < min_entry:
        await ctx.send(f"Minimum reels entry is **{min_entry:,}** VHQCoins.")
        return
    if entry <= 0 or entry > balance:
        await ctx.send(f"You only have **{balance:,}** VHQCoins — can't enter with **{entry:,}**.")
        return

    symbols = cfg.get("slots_symbols") or [{"emoji": "🍒", "weight": 1, "multiplier": 2.0}]
    add_coins(ctx.author.id, -entry)

    # Luck (LUCK SYSTEM note): each reel after the first gets a luck-scaled
    # chance to copy the first reel instead of spinning independently —
    # widens the odds of a match without ever guaranteeing one, since each
    # reel is still its own independent roll.
    luck_bonus = get_luck_bonus(econ)
    reel1 = _pick_slot_symbol(symbols)
    reels = [reel1]
    for _ in range(2):
        reels.append(reel1 if random.random() < luck_bonus else _pick_slot_symbol(symbols))

    display = " | ".join(r["emoji"] for r in reels)

    if reels[0]["emoji"] == reels[1]["emoji"] == reels[2]["emoji"]:
        mult = reel1.get("multiplier", 2.0)
        payout = _owner_payout(ctx.author.id, int(entry * mult), entry)
        new_balance = add_coins(ctx.author.id, payout)
        await ctx.send(f"🎰 [ {display} ] — **PERFECT MATCH!** Three of a kind pays **{mult}x** — **+{payout:,}** 🪙! (Balance: **{new_balance:,}**)")
    elif reels[0]["emoji"] == reels[1]["emoji"] or reels[1]["emoji"] == reels[2]["emoji"] or reels[0]["emoji"] == reels[2]["emoji"]:
        mult = cfg.get("slots_two_match_multiplier", 1.2)
        payout = _owner_payout(ctx.author.id, int(entry * mult), entry)
        new_balance = add_coins(ctx.author.id, payout)
        await ctx.send(f"🎰 [ {display} ] — Two of a kind pays **{mult}x** — **+{payout:,}** 🪙! (Balance: **{new_balance:,}**)")
    else:
        new_balance = get_user_economy(ctx.author.id)["balance"]
        await ctx.send(f"🎰 [ {display} ] — No match. Lost **{entry:,}** 🪙. (Balance: **{new_balance:,}**)")


# ---------------------------------------------------------------------------
# !highlow — bot shows a number (1-highlow_range, configurable in
# economy_config.json as 1-10 or 1-20) and secretly picks another one; you call
# Higher, Lower, or Same on how the secret pick compares to the shown
# number.
#
# Payout curve: a naive 1/probability payout doesn't feel right at either
# end — it's too generous on "safe" calls (e.g. calling Lower on a shown 8
# out of 1-10 is right 70% of the time, but 1/0.7 still pays 1.43x) and,
# once compressed enough to fix that, it ends up crushing the payout on
# genuinely risky calls too. The curve is defined by fixed anchor points in
# probability space, expressed as *fractions* of the configured
# [highlow_min_multiplier, side_hi] range, so the whole shape scales
# automatically if the admin changes the min/max multiplier in
# economy_config.json instead of needing to be re-tuned by hand.
#
# Anchors are (r, fraction) where r = 1 - probability (i.e. how unlikely
# the call is) and fraction is where that call's payout sits between the
# floor (0.0) and the ceiling (1.0). At the default 1.01x-5.0x range these
# land on: p=0.9 -> ~1.06x, p=0.8 -> ~1.2x, p=0.5 (coin flip) -> exactly
# 2.0x, p=0.4 -> 2.29x, p=0.3 -> 2.65x, p=0.2 -> 3.34x, comfortably under
# Same's fixed max. The curve is deliberately steeper between p=0.3 and
# p=0.2 than between p=0.5 and p=0.3 — risky calls pick up payout faster
# the closer they get to the rarest single-value territory Same occupies.
#
# Same is always a fixed 1-in-highlow_range shot, so it always pays exactly
# the configured max multiplier — and _highlow_side_multiplier's result is
# capped a bit below that max (side_hi) so Same can never be tied or beaten.
# ---------------------------------------------------------------------------

_HIGHLOW_CURVE_ANCHORS = [
    (0.0, 0.0),
    (0.1, 0.01433),
    (0.2, 0.05444),
    (0.5, 0.28367),
    (0.6, 0.36676),
    (0.7, 0.46991),
    (0.8, 0.66762),
    (0.9, 0.82521),
    (1.0, 1.0),
]


def _highlow_curve_fraction(r: float) -> float:
    """Piecewise-linear interpolation through the anchor points above.
    r is clamped to [0, 1] first since probability is always in (0, 1]."""
    r = max(0.0, min(1.0, r))
    for (r0, f0), (r1, f1) in zip(_HIGHLOW_CURVE_ANCHORS, _HIGHLOW_CURVE_ANCHORS[1:]):
        if r0 <= r <= r1:
            if r1 == r0:
                return f0
            t = (r - r0) / (r1 - r0)
            return f0 + t * (f1 - f0)
    return 1.0  # unreachable given the anchors above, but a safe fallback


def _highlow_side_multiplier(probability: float, cfg: dict) -> float:
    """Multiplier for a Higher or Lower call. Uses the fixed anchor curve
    (see module note above), scaled to the configured min/max, and capped
    below the configured max so Same always pays strictly more."""
    lo = cfg.get("highlow_min_multiplier", 1.01)
    hi = cfg.get("highlow_max_multiplier", 10.0)
    side_hi = max(lo, hi - max(0.5, hi * 0.1))  # headroom reserved for Same
    if probability <= 0:
        return side_hi
    if probability >= 1:
        return round(lo, 2)
    r = 1 - probability
    frac = _highlow_curve_fraction(r)
    mult = lo + (side_hi - lo) * frac
    return round(max(lo, min(side_hi, mult)), 2)


def _highlow_same_multiplier(cfg: dict) -> float:
    """Same is always the rarest single-value call, so it always pays the
    full configured max multiplier."""
    return round(cfg.get("highlow_max_multiplier", 10.0), 2)


def _highlow_pick_secret(shown: int, relation: str, highlow_range: int) -> int:
    """Pick a secret 1-highlow_range whose relationship to `shown` matches
    `relation` ("higher", "lower", or "same"), so the revealed number always
    agrees with the actual result — never a coincidence, never a
    contradiction."""
    if relation == "higher":
        candidates = list(range(shown + 1, highlow_range + 1))
    elif relation == "lower":
        candidates = list(range(1, shown))
    else:
        candidates = [shown]
    return random.choice(candidates)


class HighLowView(discord.ui.View):
    def __init__(self, author_id: int, shown: int, bet: int, cfg: dict):
        super().__init__(timeout=30)
        self.author_id = author_id
        self.shown = shown
        self.bet = bet
        self.highlow_range = cfg.get("highlow_range", 20)
        self.message: discord.Message | None = None
        self.resolved = False

        # Probabilities assume the secret is an independent uniform
        # 1-highlow_range pick.
        n = self.highlow_range
        self.probs = {
            "higher": (n - shown) / n,
            "lower": (shown - 1) / n,
            "same": 1 / n,
        }
        self.mults = {
            "higher": _highlow_side_multiplier(self.probs["higher"], cfg),
            "lower": _highlow_side_multiplier(self.probs["lower"], cfg),
            "same": _highlow_same_multiplier(cfg),
        }
        # Belt-and-suspenders: whatever the config, Same must always come out
        # strictly ahead of both sides — bump it up if it's ever tied or beaten
        # (e.g. an admin sets min_multiplier == max_multiplier).
        max_side = max(self.mults["higher"], self.mults["lower"])
        if self.mults["same"] <= max_side:
            self.mults["same"] = round(max_side + 0.01, 2)

        higher_btn = discord.ui.Button(label=f"Higher (x{self.mults['higher']})", emoji="⬆️", style=discord.ButtonStyle.success, disabled=self.probs["higher"] <= 0)
        higher_btn.callback = self._make_callback("higher")
        same_btn = discord.ui.Button(label=f"Same (x{self.mults['same']})", emoji="🎯", style=discord.ButtonStyle.primary)
        same_btn.callback = self._make_callback("same")
        lower_btn = discord.ui.Button(label=f"Lower (x{self.mults['lower']})", emoji="⬇️", style=discord.ButtonStyle.danger, disabled=self.probs["lower"] <= 0)
        lower_btn.callback = self._make_callback("lower")
        self.add_item(higher_btn)
        self.add_item(same_btn)
        self.add_item(lower_btn)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("This isn't your Highlow round.", ephemeral=True)
            return False
        return True

    def _make_callback(self, choice: str):
        async def callback(interaction: discord.Interaction):
            if self.resolved:
                await interaction.response.defer()
                return
            self.resolved = True
            for child in self.children:
                child.disabled = True

            econ = get_user_economy(self.author_id)
            probability = self.probs[choice]
            multiplier = self.mults[choice]
            # Luck (LUCK SYSTEM note): added straight to the win chance,
            # clamped so it's never a sure thing. The displayed multiplier
            # is exactly what was promised before the click either way.
            win_chance = min(0.95, probability + get_luck_bonus(econ))
            won = random.random() < win_chance

            # Pick the actual secret number so it's always consistent with
            # this result — no more narrating an outcome that doesn't match
            # the number shown.
            relation = choice if won else random.choice([r for r in ("higher", "lower", "same") if r != choice])
            secret = _highlow_pick_secret(self.shown, relation, self.highlow_range)

            if won:
                payout = _owner_payout(self.author_id, int(self.bet * multiplier), self.bet)
                new_balance = add_coins(self.author_id, payout)
                text = (f"🔢 Shown: **{self.shown}** — secret was **{secret}** — you called **{choice}** and won! "
                        f"**+{payout:,}** 🪙 (x{multiplier}). (Balance: **{new_balance:,}**)")
            else:
                new_balance = get_user_economy(self.author_id)["balance"]
                text = (f"🔢 Shown: **{self.shown}** — secret was **{secret}** — you called **{choice}** and lost your "
                        f"**{self.bet:,}** 🪙 bet. (Balance: **{new_balance:,}**)")

            self.stop()
            await interaction.response.edit_message(content=text, view=self)

        return callback

    async def on_timeout(self):
        if self.resolved:
            return
        self.resolved = True
        add_coins(self.author_id, self.bet)  # no click = refund, nobody wins or loses
        for child in self.children:
            child.disabled = True
        if self.message:
            try:
                await self.message.edit(content=f"🔢 Highlow timed out — no choice made. **{self.bet:,}** 🪙 refunded.", view=self)
            except discord.HTTPException:
                pass


@bot.hybrid_command(name="highlow", description="Bot secretly picks a number — call Higher, Lower, or Same on its pick for a payout")
@app_commands.describe(amount='How much to bet — a number, "all", or "half"')
async def highlow(ctx: commands.Context, amount: str):
    if not await _games_gate(ctx):
        return
    econ = get_user_economy(ctx.author.id)
    balance = econ["balance"]

    bet = _parse_entry_amount(amount, balance)
    if bet is None:
        await ctx.send('Give an amount to bet — a number, `all`, or `half`.')
        return

    cfg = load_economy_config()
    min_bet = cfg.get("highlow_min_bet", 10)
    if bet < min_bet:
        await ctx.send(f"Minimum highlow bet is **{min_bet:,}** VHQCoins.")
        return
    if bet <= 0 or bet > balance:
        await ctx.send(f"You only have **{balance:,}** VHQCoins — can't bet **{bet:,}**.")
        return

    add_coins(ctx.author.id, -bet)

    highlow_range = cfg.get("highlow_range", 20)
    shown = random.randint(1, highlow_range)
    view = HighLowView(ctx.author.id, shown, bet, cfg)
    view.message = await ctx.send(
        f"🔢 The bot secretly picked a number 1-{highlow_range}. Will it be Higher, Lower, or Same as **{shown}**?",
        view=view,
    )


# ---------------------------------------------------------------------------
# !blackjack — standard rules (dealer stands on 17), Hit/Stand/Double Down
# via buttons.
# ---------------------------------------------------------------------------

BLACKJACK_RANKS = ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"]
BLACKJACK_SUITS = ["♠", "♥", "♦", "♣"]


def _bj_draw_card() -> tuple:
    return (random.choice(BLACKJACK_RANKS), random.choice(BLACKJACK_SUITS))


def _bj_card_str(card: tuple) -> str:
    return f"{card[0]}{card[1]}"


def _bj_hand_value(cards: list) -> int:
    total, aces = 0, 0
    for rank, _ in cards:
        if rank == "A":
            total += 11
            aces += 1
        elif rank in ("J", "Q", "K"):
            total += 10
        else:
            total += int(rank)
    while total > 21 and aces:
        total -= 10
        aces -= 1
    return total


def _bj_is_blackjack(cards: list) -> bool:
    return len(cards) == 2 and _bj_hand_value(cards) == 21


def _bj_hand_str(cards: list) -> str:
    return " ".join(_bj_card_str(c) for c in cards)


class BlackjackGame:
    def __init__(self, user_id: int, entry: int):
        self.user_id = user_id
        self.entry = entry
        self.player = [_bj_draw_card(), _bj_draw_card()]
        self.dealer = [_bj_draw_card(), _bj_draw_card()]
        self.done = False


active_blackjack_games: dict[int, BlackjackGame] = {}


def _bj_table_text(game: BlackjackGame) -> str:
    return (f"🃏 **Cards** — entry **{game.entry:,}** 🪙\n"
            f"🧑 Your hand: {_bj_hand_str(game.player)} (**{_bj_hand_value(game.player)}**)\n"
            f"🤖 Dealer shows: {_bj_card_str(game.dealer[0])} 🂠")


class BlackjackView(discord.ui.View):
    def __init__(self, game: BlackjackGame):
        super().__init__(timeout=60)
        self.game = game
        self.message: discord.Message | None = None
        if get_user_economy(game.user_id)["balance"] < game.entry:
            self.double.disabled = True

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.game.user_id:
            await interaction.response.send_message("This isn't your Cards hand.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Hit", emoji="🃏", style=discord.ButtonStyle.success)
    async def hit(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.game.player.append(_bj_draw_card())
        self.double.disabled = True  # doubling is only offered on the first decision
        if _bj_hand_value(self.game.player) > 21:
            text = self._resolve(busted=True)
            await interaction.response.edit_message(content=text, view=self)
            return
        await interaction.response.edit_message(content=_bj_table_text(self.game), view=self)

    @discord.ui.button(label="Stand", emoji="✋", style=discord.ButtonStyle.secondary)
    async def stand(self, interaction: discord.Interaction, button: discord.ui.Button):
        text = self._resolve(busted=False)
        await interaction.response.edit_message(content=text, view=self)

    @discord.ui.button(label="Double Down", emoji="⏫", style=discord.ButtonStyle.primary)
    async def double(self, interaction: discord.Interaction, button: discord.ui.Button):
        if get_user_economy(self.game.user_id)["balance"] < self.game.entry:
            await interaction.response.send_message("You don't have enough VHQCoins left to double down.", ephemeral=True)
            return
        add_coins(self.game.user_id, -self.game.entry)
        self.game.entry *= 2
        self.game.player.append(_bj_draw_card())
        busted = _bj_hand_value(self.game.player) > 21
        text = self._resolve(busted=busted)
        await interaction.response.edit_message(content=text, view=self)

    def _resolve(self, busted: bool) -> str:
        """Settles the hand (dealer draws if needed, coins move) and
        returns the final message text. Disables all buttons — safe to
        call more than once (e.g. from on_timeout), only settles once."""
        game = self.game
        if game.done:
            return "This hand is already settled."
        game.done = True
        active_blackjack_games.pop(game.user_id, None)
        for child in self.children:
            child.disabled = True

        cfg = load_economy_config()
        econ = get_user_economy(game.user_id)
        # Luck (LUCK SYSTEM note): applied while the dealer is still
        # drawing, since there's no lever left to pull once a hand is
        # fully settled — a dealer who stands on a marginal 17-21 gets one
        # small luck-scaled chance at an extra card, tipping close hands
        # toward the player over time without ever guaranteeing a bust.
        luck_bonus = get_luck_bonus(econ)
        if not busted:
            while _bj_hand_value(game.dealer) < 17:
                game.dealer.append(_bj_draw_card())
            if _bj_hand_value(game.dealer) <= 21 and random.random() < luck_bonus:
                game.dealer.append(_bj_draw_card())

        player_total = _bj_hand_value(game.player)
        dealer_total = _bj_hand_value(game.dealer)
        player_bj = _bj_is_blackjack(game.player)
        dealer_bj = _bj_is_blackjack(game.dealer) and len(game.dealer) == 2

        lines = [
            f"🧑 Your hand: {_bj_hand_str(game.player)} (**{player_total}**)",
            f"🤖 Dealer's hand: {_bj_hand_str(game.dealer)} (**{dealer_total}**)",
            "",
        ]

        if busted:
            new_balance = get_user_economy(game.user_id)["balance"]
            lines.append(f"💥 Bust! You lost **{game.entry:,}** 🪙. (Balance: **{new_balance:,}**)")
        elif player_bj and not dealer_bj:
            payout_mult = cfg.get("blackjack_blackjack_payout", 1.5)
            payout = _owner_payout(game.user_id, int(game.entry * (1 + payout_mult)), game.entry)
            new_balance = add_coins(game.user_id, payout)
            lines.append(f"🃏 21! Paid **{payout_mult}x** — **+{payout:,}** 🪙! (Balance: **{new_balance:,}**)")
        elif dealer_bj and not player_bj:
            new_balance = get_user_economy(game.user_id)["balance"]
            lines.append(f"🤖 Dealer has 21. You lost **{game.entry:,}** 🪙. (Balance: **{new_balance:,}**)")
        elif dealer_total > 21 or player_total > dealer_total:
            payout = _owner_payout(game.user_id, game.entry * 2, game.entry)
            new_balance = add_coins(game.user_id, payout)
            reason = "Dealer busts!" if dealer_total > 21 else "You win!"
            lines.append(f"🏆 {reason} **+{payout:,}** 🪙. (Balance: **{new_balance:,}**)")
        elif player_total == dealer_total:
            new_balance = add_coins(game.user_id, game.entry)
            lines.append(f"🤝 Push — entry returned. (Balance: **{new_balance:,}**)")
        else:
            new_balance = get_user_economy(game.user_id)["balance"]
            lines.append(f"💀 Dealer wins. You lost **{game.entry:,}** 🪙. (Balance: **{new_balance:,}**)")

        self.stop()
        return "\n".join(lines)

    async def on_timeout(self):
        if self.game.done:
            return
        text = self._resolve(busted=False)
        if self.message:
            try:
                await self.message.edit(content=f"⏳ Timed out — auto-stood.\n{text}", view=self)
            except discord.HTTPException:
                pass


@bot.hybrid_command(name="blackjack", aliases=["bj"], description="Play a hand of Cards against the dealer")
@app_commands.describe(amount='How much to enter with — a number, "all", or "half"')
async def blackjack(ctx: commands.Context, amount: str):
    if not await _games_gate(ctx):
        return
    if ctx.author.id in active_blackjack_games:
        await ctx.send("🃏 You already have a Cards hand in progress.")
        return

    econ = get_user_economy(ctx.author.id)
    balance = econ["balance"]
    entry = _parse_entry_amount(amount, balance)
    if entry is None:
        await ctx.send('Give an amount to enter with — a number, `all`, or `half`.')
        return

    cfg = load_economy_config()
    min_entry = cfg.get("blackjack_min_entry", 10)
    if entry < min_entry:
        await ctx.send(f"Minimum cards entry is **{min_entry:,}** VHQCoins.")
        return
    if entry <= 0 or entry > balance:
        await ctx.send(f"You only have **{balance:,}** VHQCoins — can't enter with **{entry:,}**.")
        return

    add_coins(ctx.author.id, -entry)
    game = BlackjackGame(ctx.author.id, entry)
    active_blackjack_games[ctx.author.id] = game

    view = BlackjackView(game)
    if _bj_is_blackjack(game.player):
        for child in view.children:
            child.disabled = True
        text = view._resolve(busted=False)
        await ctx.send(text)
        return

    view.message = await ctx.send(_bj_table_text(game), view=view)


@bot.hybrid_command(name="leaderboard", description="See the richest VHQCoins holders")
async def leaderboard(ctx: commands.Context):
    if not await _economy_gate(ctx):
        return
    data = load_economy()
    ranked = sorted(data.items(), key=lambda kv: kv[1].get("balance", 0), reverse=True)[:10]
    if not ranked:
        await ctx.send("Nobody has any VHQCoins yet — be the first with `!daily` or `!work`.")
        return
    lines = []
    for i, (uid, econ) in enumerate(ranked, start=1):
        medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"`#{i}`")
        user = bot.get_user(int(uid))
        if user is None:
            # Not in the bot's member/user cache (e.g. hasn't been seen
            # since last restart) — fall back to an API fetch so the
            # leaderboard shows a real name instead of a raw ID.
            try:
                user = await bot.fetch_user(int(uid))
            except discord.HTTPException:
                user = None
        name = _format_econ_name(user, econ) if user else f"Unknown User ({uid})"
        lines.append(f"{medal} {name} — **{econ.get('balance', 0):,}** 🪙")
    embed = discord.Embed(title="🏆 VHQCoins Leaderboard", description="\n".join(lines), color=0x7CF5FF)
    await ctx.send(embed=embed)


async def _shop_item_autocomplete(interaction: discord.Interaction, current: str):
    query = (current or "").lower().strip()
    results = []
    for item in get_shop_items():
        name = str(item.get("name", ""))
        item_id = str(item.get("id", ""))
        if query and query not in name.lower() and query not in item_id.lower():
            continue
        price = int(item.get("price", 0))
        results.append(app_commands.Choice(name=f"{name} — {price:,} 🪙"[:100], value=name[:100]))
    return results[:25]


@bot.hybrid_command(name="shop", description="Browse the VHQCoins shop, or buy an item: !shop <item>")
@app_commands.autocomplete(item=_shop_item_autocomplete)
@app_commands.describe(item="Item name to buy (leave blank to browse the shop)")
async def shop(ctx: commands.Context, *, item: str = None):
    if not await _economy_gate(ctx):
        return
    prefix = get_effective_prefix(ctx.guild.id if ctx.guild else None)
    catalog = get_shop_items()

    if not item:
        econ = get_user_economy(ctx.author.id)
        embed = discord.Embed(
            title="🛒 VHQCoins Shop",
            description=f"Your balance: **{econ['balance']:,}** 🪙\nBuy with `{prefix}shop <item name>`",
            color=0x7CF5FF,
        )
        embed.set_footer(text="VHQCoins are virtual and have no real-world value.")
        if not catalog:
            embed.description += "\n\n*The shop is currently empty.*"
        for shop_item in catalog:
            owned = (
                shop_item["id"] == econ.get("active_theme")
                if shop_item["kind"] == "theme"
                else shop_item["id"] in econ["inventory"]
            )
            status = "✅ Owned" if owned else f"**{shop_item['price']:,}** 🪙"
            embed.add_field(
                name=f"{shop_item['emoji']} {shop_item['name']}",
                value=f"{shop_item['description']}\n{status}",
                inline=False,
            )
        await ctx.send(embed=embed)
        return

    # Purchase path — match by id or by name, case-insensitively.
    needle = item.strip().lower()
    match = next(
        (i for i in catalog if i["id"] == needle or i["name"].lower() == needle or needle in i["name"].lower()),
        None,
    )
    if not match:
        await ctx.send(f"Couldn't find an item called `{item}`. Run `{prefix}shop` with no arguments to browse.")
        return

    econ = get_user_economy(ctx.author.id)
    if match["kind"] == "theme" and econ.get("active_theme") == match["id"]:
        await ctx.send(f"You already own **{match['name']}** and it's already your active card theme.")
        return
    if match["kind"] == "badge" and match["id"] in econ["inventory"]:
        await ctx.send(f"You already own **{match['name']}**.")
        return
    if match["kind"] == "luck_refill":
        luck_cd = load_economy_config().get("luck_cooldown_hours", 24) * 3600
        if time.time() - econ.get("last_luck", 0.0) >= luck_cd:
            await ctx.send(f"🍀 Your `{prefix}luck` is already ready — no refill needed.")
            return
    if econ["balance"] < match["price"]:
        await ctx.send(f"You need **{match['price']:,}** 🪙 for **{match['name']}** — you have **{econ['balance']:,}**.")
        return

    add_coins(ctx.author.id, -match["price"])
    if match["kind"] == "theme":
        set_user_economy(ctx.author.id, active_theme=match["id"])
        if match["id"] not in econ["inventory"]:
            set_user_economy(ctx.author.id, inventory=econ["inventory"] + [match["id"]])
        await ctx.send(f"{match['emoji']} Bought and equipped **{match['name']}**! Check `{prefix}stats` to see it.")
    elif match["kind"] == "boost":
        current = get_user_economy(ctx.author.id)
        mult = match.get("multiplier", 2)
        base = max(current["boost_until"], time.time())
        keep_mult = current.get("boost_mult", 2) if has_active_boost(current) else 0
        set_user_economy(ctx.author.id, boost_until=base + match["hours"] * 3600, boost_mult=max(mult, keep_mult))
        await ctx.send(f"{match['emoji']} Bought **{match['name']}** — {mult}x earnings active for the next {match['hours']}h.")
    elif match["kind"] == "passive_boost":
        current = get_user_economy(ctx.author.id)
        base = max(current.get("passive_boost_until", 0.0), time.time())
        set_user_economy(ctx.author.id, passive_boost_until=base + match["hours"] * 3600)
        await ctx.send(f"{match['emoji']} Bought **{match['name']}** — chat earnings doubled for the next {match['hours']}h.")
    elif match["kind"] == "resubmit_pass":
        days = match.get("days", 7)
        expires = time.time() + days * 86400
        current = get_user_economy(ctx.author.id)
        set_user_economy(ctx.author.id, resubmit_passes=valid_resubmit_passes(current) + [expires])
        await ctx.send(f"{match['emoji']} Bought **{match['name']}** — valid for {days} days (expires <t:{int(expires)}:R>). "
                       f"The Top Edits bot will let you resubmit once.")
    elif match["kind"] == "luck_refill":
        set_user_economy(ctx.author.id, last_luck=0.0)
        await ctx.send(f"{match['emoji']} Luck refilled! You can use `{prefix}luck` again right now.")
    else:
        # Badges are stored permanently; perks (manual rewards) can be bought repeatedly.
        if match["kind"] != "perk":
            set_user_economy(ctx.author.id, inventory=econ["inventory"] + [match["id"]])
        embed = discord.Embed(
            title=f"Successfully Bought {match['name']}",
            description=match.get("redeem_note") or "Purchase complete — enjoy! (Virtual item, no real-world value.)",
            color=0x7CF5FF,
        )
        embed.add_field(name="Item", value=f"{match['emoji']} {match['name']}", inline=False)
        embed.add_field(name="Buyer", value=f"{ctx.author.mention} ({ctx.author})", inline=False)
        embed.set_thumbnail(url=ctx.author.display_avatar.url)
        await ctx.send(embed=embed)



# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit(
            "DISCORD_TOKEN environment variable is not set. "
            "Set it before running the bot (see .env.example)."
        )

    # Single-instance guard — catches the "two processes both connected
    # with the same token, both answering every command" scenario that
    # happens when a host restarts a new process without fully killing a
    # stale/hung one first. A PID lock file survives that kind of botched
    # restart even though nothing in memory does.
    _LOCK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".voidhq.lock")

    def _pid_is_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        except Exception:
            return True
        return True

    if os.path.exists(_LOCK_FILE):
        try:
            with open(_LOCK_FILE, "r") as f:
                old_pid = int(f.read().strip())
        except (ValueError, OSError):
            old_pid = None
        if old_pid and old_pid != os.getpid() and _pid_is_alive(old_pid):
            raise SystemExit(
                f"Another VoidHQ process (PID {old_pid}) appears to still be running. "
                "Refusing to start a second one against the same bot token — this is "
                "exactly what causes every command to fire twice. Fully STOP the "
                "existing process first (not just Restart), then start again. If you're "
                "certain no other instance is actually running (e.g. the host crashed "
                f"hard and left this file behind), delete {_LOCK_FILE} and retry."
            )

    with open(_LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))

    try:
        bot.run(TOKEN)
    finally:
        try:
            with open(_LOCK_FILE, "r") as f:
                if f.read().strip() == str(os.getpid()):
                    os.remove(_LOCK_FILE)
        except OSError:
            pass
