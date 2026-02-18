#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
VWAP Straddle Strategy — Kite (data) + Stocko (orders)
======================================================

UPDATED (as requested)
----------------------
✅ Track CURRENT week expiry + NEXT week expiry VWAP
✅ VWAP starts from configurable time (default 09:15 IST)
✅ Prelock: build VWAP for ATM(09:15) ± N strikes (configurable, default N=5)
✅ Freeze ATM at configurable time (default 09:30 IST)
✅ At freeze, decide which expiry to TRADE:
    - If CURRENT-week ATM straddle premium > PREMIUM_SWITCH_TH  -> trade CURRENT week
    - Else                                                     -> trade NEXT week
✅ After freeze: maintain ONLY chosen expiry + chosen strike (no other VWAP calculations)
✅ Hard stop if frozen ATM is outside prelock range (do nothing further)

Entry/Exit remains same:
- Entry when straddle_LTP < VWAP  (implemented via: vwap-ENTRY_WINDOW_BELOW <= ltp <= vwap)
- Exit  when straddle_LTP > VWAP  (implemented via: ltp > vwap + EXIT_WINDOW_ABOVE)

LIVE vs PAPER
-------------
- TRADE_MODE=PAPER  -> NO real orders are placed (only logs/prints)
- TRADE_MODE=LIVE   -> Orders go via Stocko API

PRODUCTION ADDITIONS
--------------------
✅ All logs go to Postgres (Railway DATABASE_URL) instead of CSV
✅ Auto-create tables on first run
✅ Kill switch: trade_flag.live_nifty_vwap
   - If FALSE: square-off open position (if any) + exit
"""

import os
import sys
import time
from datetime import datetime, timedelta, time as dt_time

import requests
import pytz
import psycopg2
from kiteconnect import KiteConnect, exceptions as kite_exceptions

# ============================================================
# CONFIGURATION
# ============================================================

# --- TRADE MODE ---
TRADE_MODE = os.getenv("TRADE_MODE", "PAPER").upper()
if TRADE_MODE not in ("PAPER", "LIVE"):
    raise ValueError("TRADE_MODE must be PAPER or LIVE")
print(f"[MODE] Running in {TRADE_MODE} mode")

# --- Kite config (data) ---
# API_KEY = os.getenv("KITE_API_KEY", "").strip()
# ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN", "").strip()

API_KEY = "9qfecm39l1j64xyc"
ACCESS_TOKEN = "w0oFr0nxovmuAmJU0Fv4gG0IHeND661k"

if not API_KEY or not ACCESS_TOKEN:
    raise RuntimeError("Missing KITE_API_KEY or KITE_ACCESS_TOKEN")

# --- Stocko config (orders) ---
STOCKO_BASE_URL     = os.getenv("STOCKO_BASE_URL", "https://api.stocko.in").strip()
STOCKO_ACCESS_TOKEN = os.getenv("STOCKO_ACCESS_TOKEN", "").strip()
STOCKO_CLIENT_ID    = os.getenv("STOCKO_CLIENT_ID", "").strip()

EXCH_OPT     = "NFO"
INDEX_NAME   = "NIFTY"
SPOT_TOKEN   = 256265
STRIKE_STEP  = 50

ENTRY_WINDOW_BELOW      = float(os.getenv("ENTRY_WINDOW_BELOW", "4.0"))
EXIT_WINDOW_ABOVE       = float(os.getenv("EXIT_WINDOW_ABOVE", "2.0"))
LOT_SIZE                = int(os.getenv("LOT_SIZE", "65"))
MAX_LOSS_LIMIT          = float(os.getenv("MAX_LOSS_LIMIT", "-1700.0"))
MAX_ENTRIES_PER_STRIKE  = int(os.getenv("MAX_ENTRIES_PER_STRIKE", "4"))

# Premium switch threshold
PREMIUM_SWITCH_TH       = float(os.getenv("PREMIUM_SWITCH_TH", "215.0"))

# --- TIME CONFIG (configurable) ---
MARKET_TZ = pytz.timezone("Asia/Kolkata")

def parse_hhmm(env_key: str, default_hhmm: str) -> dt_time:
    raw = os.getenv(env_key, default_hhmm).strip()
    try:
        hh, mm = map(int, raw.split(":"))
        return dt_time(hh, mm)
    except Exception:
        raise ValueError(f"{env_key} must be in HH:MM format (e.g. 09:15). Got: {raw}")

VWAP_START    = parse_hhmm("VWAP_COLLECTION_START", "09:49")
ATM_LOCK_TIME = parse_hhmm("ATM_FREEZE_TIME", "09:55")

if VWAP_START >= ATM_LOCK_TIME:
    raise ValueError("VWAP_COLLECTION_START must be earlier than ATM_FREEZE_TIME")

# --- PRELOCK STRIKE RANGE (configurable) ---
PRELOCK_WING_STRIKES = int(os.getenv("PRELOCK_WING_STRIKES", "6"))
if PRELOCK_WING_STRIKES <= 0:
    raise ValueError("PRELOCK_WING_STRIKES must be > 0")

EXIT_CUTOFF = dt_time(15, 25)

print(f"[CONFIG] VWAP_COLLECTION_START={VWAP_START}")
print(f"[CONFIG] ATM_FREEZE_TIME={ATM_LOCK_TIME}")
print(f"[CONFIG] PRELOCK_WING_STRIKES=±{PRELOCK_WING_STRIKES}")

# If LIVE mode, enforce Stocko creds
if TRADE_MODE == "LIVE":
    if not STOCKO_ACCESS_TOKEN:
        raise RuntimeError("LIVE mode requires STOCKO_ACCESS_TOKEN")
    if not STOCKO_CLIENT_ID:
        raise RuntimeError("LIVE mode requires STOCKO_CLIENT_ID")

# ============================================================
# DATABASE (Railway Postgres) + KILL SWITCH
# ============================================================

DB_URL = os.getenv("DATABASE_URL", "").strip()
if not DB_URL:
    raise RuntimeError("DATABASE_URL not set (Railway provides this automatically)")

def get_conn():
    return psycopg2.connect(DB_URL)

def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Create log tables
            cur.execute("""
            CREATE TABLE IF NOT EXISTS nifty_vwap_minute (
                id BIGSERIAL PRIMARY KEY,
                ts TIMESTAMPTZ,
                phase TEXT,
                event TEXT,
                spot DOUBLE PRECISION,
                expiry_tag TEXT,
                expiry_date DATE,
                strike INTEGER,
                straddle_ltp DOUBLE PRECISION,
                vwap DOUBLE PRECISION,
                cum_pv DOUBLE PRECISION,
                cum_vol DOUBLE PRECISION,
                ce_vol BIGINT,
                pe_vol BIGINT,
                ce_ltp DOUBLE PRECISION,
                pe_ltp DOUBLE PRECISION,
                note TEXT
            );
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS nifty_vwap_trade (
                id BIGSERIAL PRIMARY KEY,
                ts TIMESTAMPTZ,
                event TEXT,
                phase TEXT,
                expiry_tag TEXT,
                expiry_date DATE,
                strike INTEGER,
                action_price DOUBLE PRECISION,
                vwap_at_action DOUBLE PRECISION,
                pnl_realized DOUBLE PRECISION,
                pnl_running DOUBLE PRECISION,
                total_pnl DOUBLE PRECISION,
                ce_ltp DOUBLE PRECISION,
                pe_ltp DOUBLE PRECISION,
                status TEXT,
                note TEXT
            );
            """)

            # Ensure kill-switch table/column exists
            cur.execute("""
            CREATE TABLE IF NOT EXISTS trade_flag (
                id BIGSERIAL PRIMARY KEY
            );
            """)
            cur.execute("""
            ALTER TABLE trade_flag
            ADD COLUMN IF NOT EXISTS live_nifty_vwap BOOLEAN DEFAULT TRUE;
            """)

            # Ensure at least one row exists
            cur.execute("SELECT COUNT(*) FROM trade_flag;")
            if cur.fetchone()[0] == 0:
                cur.execute("INSERT INTO trade_flag (live_nifty_vwap) VALUES (TRUE);")

        conn.commit()

def is_kill_switch_on() -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT live_nifty_vwap FROM trade_flag ORDER BY id ASC LIMIT 1;")
            row = cur.fetchone()
            return bool(row[0]) if row and row[0] is not None else True

init_db()
print("[DB] Tables ready (nifty_vwap_minute, nifty_vwap_trade, trade_flag).")

# ============================================================
# BROKER INIT (Kite for data)
# ============================================================
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# ============================================================
# SAFE QUOTE WRAPPER
# ============================================================
def safe_quote(keys, max_retries=5, base_sleep=1.0):
    for attempt in range(1, max_retries + 1):
        try:
            return kite.quote(keys)
        except kite_exceptions.NetworkException as e:
            print(f"[WARN] quote network error (attempt {attempt}/{max_retries}): {e}")
            time.sleep(base_sleep * attempt)
        except kite_exceptions.GeneralException as e:
            print(f"[WARN] quote general error (attempt {attempt}/{max_retries}): {e}")
            time.sleep(base_sleep * attempt)
        except kite_exceptions.InputException as e:
            print(f"[ERROR] quote input error, not retrying: {e}")
            raise
    raise RuntimeError("safe_quote: too many failures calling kite.quote")

# ============================================================
# TIME / UTILS
# ============================================================
def now_ist():
    return datetime.now(MARKET_TZ)

def ts():
    return now_ist().strftime("%Y-%m-%d %H:%M:%S")

def nearest_50(x):
    return int(round(x / STRIKE_STEP) * STRIKE_STEP)

# ============================================================
# EXPIRY LOGIC (NIFTY Tuesday weekly + monthly)
# ============================================================
def get_nifty_expiries(today, holidays=None):
    """
    Returns:
        weekly_expiry (next Tuesday >= today)
        monthly_expiry (last working Tuesday of month)
    """
    if holidays is None:
        holidays = set()

    # Next Tuesday (Mon=0 Tue=1)
    days_ahead = (1 - today.weekday()) % 7
    next_tuesday = today + timedelta(days=days_ahead)
    if next_tuesday < today:
        next_tuesday += timedelta(days=7)

    # Monthly: last working Tuesday
    next_month_first = (today.replace(day=28) + timedelta(days=4)).replace(day=1)
    last_day_this_month = next_month_first - timedelta(days=1)
    offset = (last_day_this_month.weekday() - 1) % 7
    monthly = last_day_this_month - timedelta(days=offset)

    while monthly in holidays:
        monthly -= timedelta(days=1)

    # If next Tuesday equals monthly, weekly becomes previous Tuesday
    if next_tuesday == monthly:
        weekly = next_tuesday - timedelta(days=7)
    else:
        weekly = next_tuesday

    if weekly < today:
        weekly += timedelta(days=7)

    return weekly, monthly

def next_weekly_expiry(curr_weekly, holidays=None):
    """Next weekly = curr_weekly + 7 days, adjusted if holiday list provided."""
    if holidays is None:
        holidays = set()
    nxt = curr_weekly + timedelta(days=7)
    while nxt in holidays:
        nxt -= timedelta(days=1)
    return nxt

# ============================================================
# ✅ HOLIDAY-SAFE EXPIRY PICKER (FROM INSTRUMENT DUMP)
# ============================================================
def pick_curr_next_weekly_from_instruments(instruments, today_date):
    """
    Holiday-safe approach:
    - Uses the actual expiry dates present in Kite instrument dump for NIFTY options
    - Picks the first two expiries >= today
    - Handles Tuesday holiday -> expiry shifts to Monday.
    """
    expiries = sorted({
        ins["expiry"].date() if hasattr(ins.get("expiry"), "date") else ins.get("expiry")
        for ins in instruments
        if ins.get("segment") == "NFO-OPT"
        and str(ins.get("tradingsymbol", "")).startswith("NIFTY")
        and ins.get("expiry") is not None
    })

    future = [e for e in expiries if e >= today_date]
    if len(future) < 2:
        raise RuntimeError(f"Not enough future expiries in instrument dump (found {len(future)})")

    return future[0], future[1]

# ============================================================
# SPOT / INSTRUMENT HELPERS
# ============================================================
def get_spot_ltp():
    q = safe_quote([SPOT_TOKEN])
    return list(q.values())[0]["last_price"]

def load_instruments_nfo():
    return kite.instruments(EXCH_OPT)

def build_opt_index(instruments):
    """
    Build a fast index:
        key = (expiry_date, strike_int, "CE"/"PE") -> (token, tradingsymbol)
    Only for NIFTY options.
    """
    idx = {}
    for ins in instruments:
        if ins.get("segment") != "NFO-OPT":
            continue
        tsym = ins.get("tradingsymbol", "")
        if not tsym.startswith("NIFTY"):
            continue
        try:
            strike = int(ins["strike"])
        except Exception:
            continue

        exp = ins["expiry"].date() if hasattr(ins["expiry"], "date") else ins["expiry"]
        opt_type = ins.get("instrument_type")
        if opt_type not in ("CE", "PE"):
            continue
        idx[(exp, strike, opt_type)] = (ins["instrument_token"], tsym)
    return idx

def get_option_pair_for_strike_fast(strike, expiry, opt_index):
    ce = opt_index.get((expiry, int(strike), "CE"))
    pe = opt_index.get((expiry, int(strike), "PE"))
    if not ce or not pe:
        return None
    ce_token, ce_symbol = ce
    pe_token, pe_symbol = pe
    return (ce_token, pe_token, ce_symbol, pe_symbol)

# ============================================================
# STOCKO HELPERS (LIVE) + EXEC WRAPPER (LIVE/PAPER)
# ============================================================
def _stocko_headers():
    return {
        "Authorization": f"Bearer {STOCKO_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

def stocko_search_token(keyword: str):
    url = f"{STOCKO_BASE_URL}/api/v1/search"
    r = requests.get(url, params={"key": keyword}, headers=_stocko_headers(), timeout=10)
    r.raise_for_status()
    data = r.json()
    result = data.get("result") or data.get("data", {}).get("result", [])
    for rec in result:
        if rec.get("exchange") == "NFO":
            return rec["token"]
    raise ValueError(f"No NFO token found for {keyword}")

def generate_numeric_order_id(offset=0):
    base = int(time.time() * 1000)
    return str(base + offset)[-15:]

def stocko_place_order_token(token: int, side: str, qty: int,
                             exchange="NFO", order_type="MARKET",
                             product="NRML", validity="DAY",
                             price=0, trigger_price=0, offset=0):
    url = f"{STOCKO_BASE_URL}/api/v1/orders"
    order_id = generate_numeric_order_id(offset)
    payload = {
        "exchange": exchange,
        "order_type": order_type,
        "instrument_token": int(token),
        "quantity": int(qty),
        "disclosed_quantity": 0,
        "order_side": side.upper(),
        "price": price,
        "trigger_price": trigger_price,
        "validity": validity,
        "product": product,
        "client_id": STOCKO_CLIENT_ID,
        "user_order_id": order_id,
        "market_protection_percentage": 0,
        "device": "WEB"
    }

    print(f"[{ts()}] 🆔 Placing {side.upper()} (token={token}) | user_order_id={order_id}")
    r = requests.post(url, json=payload, headers=_stocko_headers(), timeout=10)
    if r.status_code != 200:
        print(f"[{ts()}] ❌ Stocko {side.upper()} failed: {r.status_code} → {r.text}")
        return None

    print(f"[{ts()}] ✅ Stocko {side.upper()} placed successfully (user_order_id={order_id})")
    return r.json()

def stocko_open_straddle(ce_keyword, pe_keyword):
    ce_token = stocko_search_token(ce_keyword)
    pe_token = stocko_search_token(pe_keyword)
    print(f"[{ts()}] 💰 Opening straddle (CE={ce_keyword}, PE={pe_keyword})")
    r1 = stocko_place_order_token(ce_token, "SELL", LOT_SIZE, offset=0)
    r2 = stocko_place_order_token(pe_token, "SELL", LOT_SIZE, offset=1)
    return (r1 is not None) and (r2 is not None)

def stocko_close_straddle(ce_keyword, pe_keyword):
    ce_token = stocko_search_token(ce_keyword)
    pe_token = stocko_search_token(pe_keyword)
    print(f"[{ts()}] 💰 Closing straddle (CE={ce_keyword}, PE={pe_keyword})")
    r1 = stocko_place_order_token(ce_token, "BUY", LOT_SIZE, offset=2)
    r2 = stocko_place_order_token(pe_token, "BUY", LOT_SIZE, offset=3)
    return (r1 is not None) and (r2 is not None)

def exec_open_straddle(ce_symbol, pe_symbol):
    """LIVE -> place orders, PAPER -> simulate"""
    if TRADE_MODE == "LIVE":
        ok = stocko_open_straddle(ce_symbol, pe_symbol)
        if not ok:
            raise RuntimeError("Stocko open straddle failed (one or both legs)")
        return "LIVE_ORDER_SENT"
    else:
        print(f"[PAPER] Simulated SELL straddle: CE={ce_symbol}, PE={pe_symbol}, QTY={LOT_SIZE}")
        return "PAPER_ORDER_SIMULATED"

def exec_close_straddle(ce_symbol, pe_symbol):
    """LIVE -> place orders, PAPER -> simulate"""
    if TRADE_MODE == "LIVE":
        ok = stocko_close_straddle(ce_symbol, pe_symbol)
        if not ok:
            raise RuntimeError("Stocko close straddle failed (one or both legs)")
        return "LIVE_ORDER_SENT"
    else:
        print(f"[PAPER] Simulated BUY straddle:  CE={ce_symbol}, PE={pe_symbol}, QTY={LOT_SIZE}")
        return "PAPER_ORDER_SIMULATED"

# ============================================================
# LOGGING (DB)
# ============================================================
def log_trade(event, phase, expiry_tag, expiry_date, strike, action_price, vwap_at_action,
              pnl_realized=None, pnl_running=None, total_pnl=None,
              ce_ltp=None, pe_ltp=None, status="", note=""):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO nifty_vwap_trade (
                    ts, event, phase, expiry_tag, expiry_date, strike,
                    action_price, vwap_at_action,
                    pnl_realized, pnl_running, total_pnl,
                    ce_ltp, pe_ltp, status, note
                )
                VALUES (%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s)
            """, (
                now_ist(), event, phase, expiry_tag, expiry_date, strike,
                action_price, vwap_at_action,
                pnl_realized, pnl_running, total_pnl,
                ce_ltp, pe_ltp, status, note
            ))
        conn.commit()

def log_vwap(event, phase, spot,
             expiry_tag=None, expiry_date=None,
             strike=None, ltp=None, vwap=None, cum_pv=None, cum_vol=None,
             ce_vol=None, pe_vol=None, ce_ltp=None, pe_ltp=None, note=""):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO nifty_vwap_minute (
                    ts, phase, event, spot,
                    expiry_tag, expiry_date,
                    strike, straddle_ltp, vwap,
                    cum_pv, cum_vol,
                    ce_vol, pe_vol,
                    ce_ltp, pe_ltp,
                    note
                )
                VALUES (%s,%s,%s,%s,
                        %s,%s,
                        %s,%s,%s,
                        %s,%s,
                        %s,%s,
                        %s,%s,
                        %s)
            """, (
                now_ist(), phase, event, spot,
                expiry_tag, expiry_date,
                strike, ltp, vwap,
                cum_pv, cum_vol,
                ce_vol, pe_vol,
                ce_ltp, pe_ltp,
                note
            ))
        conn.commit()

# ========================= PART 1 ENDS HERE =========================
# Part 2 continues with:
# - StraddleVWAP class
# - main() with kill-switch check inside the trading loop
# - entry point
# ============================================================
# VWAP OBJECT
# ============================================================
class StraddleVWAP:
    def __init__(self, strike, expiry_date, expiry_tag, ce_token, pe_token, ce_symbol, pe_symbol):
        self.strike = strike
        self.expiry_date = expiry_date
        self.expiry_tag = expiry_tag

        self.ce_token = ce_token
        self.pe_token = pe_token
        self.ce_symbol = ce_symbol
        self.pe_symbol = pe_symbol

        self.last_ce_vol = None
        self.last_pe_vol = None
        self.cum_pv = 0.0
        self.cum_vol = 0.0

        self.in_pos = False
        self.entry = None
        self.realized = 0.0
        self.disabled = False
        self.entries_taken = 0

    def quote(self):
        q = safe_quote([self.ce_token, self.pe_token])
        v = {d["instrument_token"]: d for d in q.values()}
        ce = v[self.ce_token]
        pe = v[self.pe_token]
        return ce["last_price"], pe["last_price"], ce["volume"], pe["volume"]

    def update(self):
        try:
            ce_ltp, pe_ltp, ce_v, pe_v = self.quote()
        except Exception as e:
            print(f"[WARN] {self.expiry_tag} {self.strike} update skipped due to quote error: {e}")
            return (None, None, self.cum_pv, self.cum_vol, None, None, None, None)

        straddle = ce_ltp + pe_ltp

        d_ce = 0 if self.last_ce_vol is None else max(0, ce_v - self.last_ce_vol)
        d_pe = 0 if self.last_pe_vol is None else max(0, pe_v - self.last_pe_vol)
        vol = d_ce + d_pe

        if vol > 0:
            self.cum_pv += straddle * vol
            self.cum_vol += vol

        self.last_ce_vol = ce_v
        self.last_pe_vol = pe_v

        vwap = self.cum_pv / self.cum_vol if self.cum_vol > 0 else None
        return straddle, vwap, self.cum_pv, self.cum_vol, ce_v, pe_v, ce_ltp, pe_ltp

    def status(self):
        if self.disabled:
            return "DISABLED"
        return "IN" if self.in_pos else "OUT"

    def pnl_run(self, ltp):
        if not (self.in_pos and self.entry is not None and ltp is not None):
            return 0.0
        return (self.entry - ltp) * LOT_SIZE

    def try_entry(self, ltp, vwap, phase):
        if self.disabled or self.in_pos or vwap is None or ltp is None:
            return False
        if self.entries_taken >= MAX_ENTRIES_PER_STRIKE:
            return False

        if not ((vwap - ENTRY_WINDOW_BELOW) <= ltp <= vwap):
            return False

        try:
            ce_ltp, pe_ltp, _, _ = self.quote()
        except Exception as e:
            print(f"[ENTRY_SKIP_QUOTE_ERR] {self.expiry_tag} {self.strike} error={e}")
            return False

        try:
            exec_note = exec_open_straddle(self.ce_symbol, self.pe_symbol)

            self.in_pos = True
            self.entry = ltp
            self.entries_taken += 1

            note = (f"{self.expiry_tag} ENTRY {TRADE_MODE} ce={ce_ltp:.2f} pe={pe_ltp:.2f} "
                    f"entries={self.entries_taken} {exec_note}")
            log_trade("ENTRY", phase, self.expiry_tag, self.expiry_date, self.strike, ltp, vwap,
                      ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                      status=self.status(), note=note)
            print(f"[ENTRY {TRADE_MODE}] {ts()} {self.expiry_tag} strike={self.strike} ltp={ltp:.2f} vwap={vwap:.2f} entries={self.entries_taken}")
            return True

        except Exception as e:
            note = f"{self.expiry_tag} ENTRY_FAIL {TRADE_MODE} {type(e).__name__}: {e}"
            log_trade("ENTRY_FAIL", phase, self.expiry_tag, self.expiry_date, self.strike, ltp, vwap,
                      ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                      status=self.status(), note=note)
            print(f"[ENTRY_FAIL] {ts()} {self.expiry_tag} strike={self.strike} error={e}")
            return False

    def try_exit(self, ltp, vwap, phase):
        if not self.in_pos or ltp is None or vwap is None:
            return False

        try:
            ce_ltp, pe_ltp, *_ = self.quote()
        except Exception as e:
            print(f"[EXIT_SKIP_QUOTE_ERR] {self.expiry_tag} {self.strike} error={e}")
            return False

        pnl = self.pnl_run(ltp)
        total = self.realized + pnl

        if total <= MAX_LOSS_LIMIT:
            try:
                exec_note = exec_close_straddle(self.ce_symbol, self.pe_symbol)

                self.realized = total
                self.in_pos = False
                self.disabled = True

                note = (f"{self.expiry_tag} HARD_SL {TRADE_MODE} ce={ce_ltp:.2f} pe={pe_ltp:.2f} "
                        f"entries={self.entries_taken} {exec_note}")
                log_trade("HARD_SL", phase, self.expiry_tag, self.expiry_date, self.strike, ltp, vwap,
                          pnl_realized=pnl, total_pnl=self.realized,
                          ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                          status=self.status(), note=note)
                print(f"[HARD_SL {TRADE_MODE}] {ts()} {self.expiry_tag} strike={self.strike} total={self.realized:.2f}")
                return True
            except Exception as e:
                note = f"{self.expiry_tag} HARD_SL_FAIL {TRADE_MODE} {type(e).__name__}: {e}"
                log_trade("HARD_SL_FAIL", phase, self.expiry_tag, self.expiry_date, self.strike, ltp, vwap,
                          pnl_realized=pnl, total_pnl=total,
                          ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                          status=self.status(), note=note)
                print(f"[HARD_SL_FAIL] {ts()} {self.expiry_tag} strike={self.strike} error={e}")
                return False

        if ltp > (vwap + EXIT_WINDOW_ABOVE):
            try:
                exec_note = exec_close_straddle(self.ce_symbol, self.pe_symbol)

                self.realized = total
                self.in_pos = False
                if self.entries_taken >= MAX_ENTRIES_PER_STRIKE:
                    self.disabled = True

                note = (f"{self.expiry_tag} EXIT {TRADE_MODE} ce={ce_ltp:.2f} pe={pe_ltp:.2f} "
                        f"entries={self.entries_taken} {exec_note}")
                log_trade("EXIT", phase, self.expiry_tag, self.expiry_date, self.strike, ltp, vwap,
                          pnl_realized=pnl, total_pnl=self.realized,
                          ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                          status=self.status(), note=note)
                print(f"[EXIT {TRADE_MODE}] {ts()} {self.expiry_tag} strike={self.strike} total={self.realized:.2f} entries={self.entries_taken}")
                return True
            except Exception as e:
                note = f"{self.expiry_tag} EXIT_FAIL {TRADE_MODE} {type(e).__name__}: {e}"
                log_trade("EXIT_FAIL", phase, self.expiry_tag, self.expiry_date, self.strike, ltp, vwap,
                          pnl_realized=pnl, total_pnl=total,
                          ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                          status=self.status(), note=note)
                print(f"[EXIT_FAIL] {ts()} {self.expiry_tag} strike={self.strike} error={e}")
                return False

        return False

# ============================================================
# MAIN
# ============================================================
def main():
    print("[INIT] Starting VWAP (PRELOCK ATM±N both expiries, then freeze & keep one) …")

    # Wait until VWAP collection start
    while now_ist().time() < VWAP_START:
        time.sleep(0.5)

    instruments = load_instruments_nfo()
    opt_index = build_opt_index(instruments)

    today = now_ist().date()

    # =========================================================
    # ✅ FIX: EXPIRY MUST COME FROM INSTRUMENT DUMP (HOLIDAY SAFE)
    # =========================================================
    curr_weekly, next_weekly = pick_curr_next_weekly_from_instruments(instruments, today)

    print(f"[EXPIRY] Current weekly = {curr_weekly}")
    print(f"[EXPIRY] Next weekly    = {next_weekly}")

    # --------------------------
    # Prelock base ATM is fixed at VWAP_START time (NO rolling)
    # --------------------------
    try:
        spot_start = get_spot_ltp()
    except Exception as e:
        raise RuntimeError(f"Could not read spot at VWAP start {VWAP_START}: {e}")

    base_atm = nearest_50(spot_start)
    strikes = [base_atm + i * STRIKE_STEP for i in range(-PRELOCK_WING_STRIKES, PRELOCK_WING_STRIKES + 1)]
    prelock_strikes = set(strikes)

    print(f"[PRELOCK] Base ATM @ VWAP_START={VWAP_START} is {base_atm}")
    print(f"[PRELOCK] Tracking strikes range {min(strikes)} to {max(strikes)} (±{PRELOCK_WING_STRIKES})")

    # Build VWAP objects for both expiries, all strikes
    prelock_objs = {
        "CURR_WEEK": {},
        "NEXT_WEEK": {}
    }

    for s in strikes:
        meta_c = get_option_pair_for_strike_fast(s, curr_weekly, opt_index)
        meta_n = get_option_pair_for_strike_fast(s, next_weekly, opt_index)

        if not meta_c:
            raise RuntimeError(f"No CE/PE found for CURRENT weekly {curr_weekly} strike={s}")
        if not meta_n:
            raise RuntimeError(f"No CE/PE found for NEXT weekly {next_weekly} strike={s}")

        prelock_objs["CURR_WEEK"][s] = StraddleVWAP(s, curr_weekly, "CURR_WEEK", *meta_c)
        prelock_objs["NEXT_WEEK"][s] = StraddleVWAP(s, next_weekly, "NEXT_WEEK", *meta_n)

    did_lock = False
    phase = "prelock"

    active = None
    active_tag = None
    active_expiry = None
    atm_locked = None

    last_min = None

    while now_ist().time() < EXIT_CUTOFF:
        now = now_ist()
        curr_min = now.minute

        # --------------------------
        # KILL SWITCH CHECK (NEW)
        # --------------------------
        if not is_kill_switch_on():
            print("[KILL SWITCH] live_nifty_vwap=FALSE -> forcing exit & stopping.")

            # If we have an open position, force square-off
            if active is not None and getattr(active, "in_pos", False):
                try:
                    exec_note = exec_close_straddle(active.ce_symbol, active.pe_symbol)
                    # best effort logging
                    try:
                        ce_ltp, pe_ltp, *_ = active.quote()
                    except Exception:
                        ce_ltp, pe_ltp = None, None

                    log_trade(
                        "FLAG_EXIT", phase,
                        active_tag or "NA",
                        active_expiry,
                        atm_locked,
                        None, None,
                        total_pnl=active.realized,
                        ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                        status="FORCED_EXIT",
                        note=f"Exited due to trade_flag.live_nifty_vwap=FALSE | {exec_note}"
                    )
                except Exception as e:
                    print(f"[FLAG_EXIT_FAIL] {e}")
                    log_trade(
                        "FLAG_EXIT_FAIL", phase,
                        active_tag or "NA",
                        active_expiry,
                        atm_locked,
                        None, None,
                        status="FORCED_EXIT_FAIL",
                        note=f"Kill-switch exit failed: {type(e).__name__}: {e}"
                    )
            else:
                # No position open; just log and stop
                log_trade(
                    "FLAG_STOP", phase,
                    active_tag or "NA",
                    active_expiry,
                    atm_locked,
                    None, None,
                    status="STOPPED",
                    note="Stopped due to trade_flag.live_nifty_vwap=FALSE (no open position)"
                )

            sys.exit(0)

        # Spot
        try:
            spot = get_spot_ltp()
        except Exception as e:
            print(f"[WARN] Spot quote failed, skipping tick: {e}")
            time.sleep(1)
            continue

        # --------------------------
        # PRELOCK: update VWAP universe once per minute
        # --------------------------
        if not did_lock:
            if last_min != curr_min:
                last_min = curr_min

                # Update ALL prelock objects (both expiries, all strikes) once per minute
                for tag in ("CURR_WEEK", "NEXT_WEEK"):
                    for s, obj in prelock_objs[tag].items():
                        ltp, vwap, cum_pv, cum_vol, ce_v, pe_v, ce_ltp, pe_ltp = obj.update()
                        log_vwap("PRELOCK_VWAP", phase, spot,
                                 expiry_tag=tag, expiry_date=obj.expiry_date,
                                 strike=s, ltp=ltp, vwap=vwap,
                                 cum_pv=cum_pv, cum_vol=cum_vol,
                                 ce_vol=ce_v, pe_vol=pe_v,
                                 ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                                 note="prelock tracking")
            # Check if freeze time reached
            if now.time() < ATM_LOCK_TIME:
                time.sleep(1)
                continue

        # --------------------------
        # LOCK ATM @ ATM_FREEZE_TIME (first time only)
        # --------------------------
        if (not did_lock) and now.time() >= ATM_LOCK_TIME:
            atm_locked = nearest_50(spot)

            # HARD STOP if outside prelock range
            if atm_locked not in prelock_strikes:
                msg = (f"[FATAL] ATM at freeze time ({ATM_LOCK_TIME}) is {atm_locked}, "
                       f"outside prelock range [{min(prelock_strikes)}..{max(prelock_strikes)}] "
                       f"(base_atm={base_atm}, wing=±{PRELOCK_WING_STRIKES}). Stopping.")
                print(msg)
                log_trade("OUT_OF_RANGE_EXIT", "locked", "NA", today, atm_locked, None, None, note=msg)
                sys.exit(0)

            # Read CURRENT-week premium at locked ATM using the already-tracked object
            obj_curr_lock = prelock_objs["CURR_WEEK"][atm_locked]
            ltp_curr, vwap_curr, *_ = obj_curr_lock.update()

            if ltp_curr is None:
                raise RuntimeError("Could not read CURRENT-week straddle premium at lock time")

            if ltp_curr > PREMIUM_SWITCH_TH:
                active_tag = "CURR_WEEK"
                active_expiry = curr_weekly
            else:
                active_tag = "NEXT_WEEK"
                active_expiry = next_weekly

            active = prelock_objs[active_tag][atm_locked]

            # After lock: discard everything else (stop calculating VWAP for other strikes/expiry)
            prelock_objs = None

            did_lock = True
            phase = "locked"
            last_min = None  # reset minute gate so first locked-minute runs cleanly

            note = (f"ATM_LOCK={atm_locked} curr_prem={ltp_curr:.2f} th={PREMIUM_SWITCH_TH:.2f} "
                    f"-> TRADE={active_tag} expiry={active_expiry} MODE={TRADE_MODE}")
            log_trade("ATM_LOCK", phase, active_tag, active_expiry, atm_locked, ltp_curr, vwap_curr, note=note)
            print(f"[LOCK] {ts()} {note}")

        # --------------------------
        # POST-LOCK: only maintain chosen expiry + chosen strike
        # --------------------------
        ltp_a, vwap_a, cum_pv, cum_vol, ce_v, pe_v, ce_ltp, pe_ltp = active.update()

        # Entry/Exit once per minute (as in your original)
        if last_min != curr_min:
            last_min = curr_min

            entered = active.try_entry(ltp_a, vwap_a, phase)
            if not entered:
                active.try_exit(ltp_a, vwap_a, phase)

            # Heartbeat / VWAP row for active only
            pnl_run = active.pnl_run(ltp_a) if active.in_pos else 0.0
            total = active.realized + pnl_run
            status = active.status()

            log_vwap("VWAP_ROW", phase, spot,
                     expiry_tag=active_tag, expiry_date=active_expiry,
                     strike=atm_locked, ltp=ltp_a, vwap=vwap_a,
                     cum_pv=cum_pv, cum_vol=cum_vol,
                     ce_vol=ce_v, pe_vol=pe_v,
                     ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                     note=f"ACTIVE only | MODE={TRADE_MODE}")

            log_trade("HEARTBEAT", phase, active_tag, active_expiry, atm_locked, ltp_a, vwap_a,
                      pnl_realized=active.realized,
                      pnl_running=pnl_run,
                      total_pnl=total,
                      ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                      status=status,
                      note=f"{active_tag} heartbeat entries={active.entries_taken} MODE={TRADE_MODE}")

        time.sleep(1)

    print("[DONE] Trading day complete. Check DB logs.")

# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    main()
