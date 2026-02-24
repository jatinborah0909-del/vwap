#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
VWAP Straddle Strategy — Kite (data) + Stocko (orders) + Railway Postgres
==========================================================================

v4 CHANGES (on top of v3)
--------------------------
✅ Open Interest (OI) change tracking:
   - At VWAP_START: take a baseline OI snapshot for ±PRELOCK_WING_STRIKES strikes
     of the CURRENT weekly expiry (CE + PE separately).
   - Every minute: compute cumulative OI change vs the baseline snapshot and store
     in two new DB columns:  ce_oi_change  and  pe_oi_change.
   - Pre-lock: tracked for ALL strikes in the prelock range.
   - Post-lock (after ATM_FREEZE_TIME): tracked ONLY for the locked ATM strike.
   - Columns are added to nifty_vwap_minute via ALTER TABLE … ADD COLUMN IF NOT EXISTS
     so no manual schema migration is needed.

v3 CHANGES (on top of v2)
--------------------------
✅ Profit target + trailing stop (per-lot basis):
   - PROFIT_PER_LOT  : profit (₹) per lot at which trailing activates
   - TRAIL_PROFIT    : step size (₹) per lot for the trailing stop

v2 CHANGES (unchanged)
-----------------------
✅ Crossover-based entry
✅ Per-leg exit management (partial exit of loss leg)
✅ Re-entry logic
✅ Railway / Postgres logging
✅ Kill switch: trade_flag.live_nifty_vwap
✅ EOD force-close

PRELOCK (unchanged)
-------------------
✅ Track CURRENT week + NEXT week expiry VWAP from VWAP_START
✅ ATM fixed at VWAP_START spot price; track ±PRELOCK_WING_STRIKES strikes
✅ At ATM_FREEZE_TIME: decide expiry (curr prem > PREMIUM_SWITCH_TH → curr, else next)
✅ Hard stop if frozen ATM outside prelock range

LIVE vs PAPER
-------------
- TRADE_MODE=PAPER  → NO real orders (only logs)
- TRADE_MODE=LIVE   → Orders via Stocko API
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

TRADE_MODE = os.getenv("TRADE_MODE", "PAPER").upper()
if TRADE_MODE not in ("PAPER", "LIVE"):
    raise ValueError("TRADE_MODE must be PAPER or LIVE")
print(f"[MODE] Running in {TRADE_MODE} mode")

# --- Kite config ---
API_KEY      = os.getenv("KITE_API_KEY", "").strip()
ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN", "").strip()

if not API_KEY or not ACCESS_TOKEN:
    raise RuntimeError("Missing KITE_API_KEY or KITE_ACCESS_TOKEN")

# --- Stocko config ---
STOCKO_BASE_URL     = os.getenv("STOCKO_BASE_URL", "https://api.stocko.in").strip()
STOCKO_ACCESS_TOKEN = os.getenv("STOCKO_ACCESS_TOKEN", "").strip()
STOCKO_CLIENT_ID    = os.getenv("STOCKO_CLIENT_ID", "").strip()

EXCH_OPT    = "NFO"
SPOT_TOKEN  = 256265
STRIKE_STEP = 50

EXIT_WINDOW_ABOVE      = float(os.getenv("EXIT_WINDOW_ABOVE",      "2.0"))
LOT_SIZE               = int(os.getenv("LOT_SIZE",                 "65"))
MAX_LOSS_LIMIT         = float(os.getenv("MAX_LOSS_LIMIT",         "-1700.0"))
MAX_ENTRIES_PER_STRIKE = int(os.getenv("MAX_ENTRIES_PER_STRIKE",   "4"))
PREMIUM_SWITCH_TH      = float(os.getenv("PREMIUM_SWITCH_TH",      "215.0"))

SINGLE_LEG_SL          = float(os.getenv("SINGLE_LEG_SL",          "10.0"))

# ---------------------------------------------------------------
# v3: Profit target + trailing stop (per LOT basis)
# ---------------------------------------------------------------
PROFIT_PER_LOT  = float(os.getenv("PROFIT_PER_LOT",  "1000.0"))
TRAIL_PROFIT    = float(os.getenv("TRAIL_PROFIT",      "500.0"))

if PROFIT_PER_LOT < 0:
    raise ValueError("PROFIT_PER_LOT must be >= 0")
if TRAIL_PROFIT <= 0:
    raise ValueError("TRAIL_PROFIT must be > 0")
if TRAIL_PROFIT > PROFIT_PER_LOT:
    raise ValueError("TRAIL_PROFIT must be <= PROFIT_PER_LOT")

print(f"[CONFIG] PROFIT_PER_LOT={PROFIT_PER_LOT}  TRAIL_PROFIT={TRAIL_PROFIT}")

MARKET_TZ = pytz.timezone("Asia/Kolkata")

def parse_hhmm(env_key: str, default_hhmm: str) -> dt_time:
    raw = os.getenv(env_key, default_hhmm).strip()
    try:
        hh, mm = map(int, raw.split(":"))
        return dt_time(hh, mm)
    except Exception:
        raise ValueError(f"{env_key} must be HH:MM. Got: {raw}")

VWAP_START    = parse_hhmm("VWAP_COLLECTION_START", "09:15")
ATM_LOCK_TIME = parse_hhmm("ATM_FREEZE_TIME",       "09:30")

if VWAP_START >= ATM_LOCK_TIME:
    raise ValueError("VWAP_COLLECTION_START must be earlier than ATM_FREEZE_TIME")

PRELOCK_WING_STRIKES = int(os.getenv("PRELOCK_WING_STRIKES", "6"))
if PRELOCK_WING_STRIKES <= 0:
    raise ValueError("PRELOCK_WING_STRIKES must be > 0")

EXIT_CUTOFF = dt_time(15, 25)

print(f"[CONFIG] VWAP_COLLECTION_START={VWAP_START}")
print(f"[CONFIG] ATM_FREEZE_TIME={ATM_LOCK_TIME}")
print(f"[CONFIG] PRELOCK_WING_STRIKES=±{PRELOCK_WING_STRIKES}")

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

            # v4: Add OI change columns if they don't exist yet
            for col, coltype in [
                ("ce_oi_change", "BIGINT"),
                ("pe_oi_change", "BIGINT"),
            ]:
                cur.execute(f"""
                    ALTER TABLE nifty_vwap_minute
                    ADD COLUMN IF NOT EXISTS {col} {coltype};
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
                leg TEXT,
                leg_entry DOUBLE PRECISION,
                leg_exit_price DOUBLE PRECISION,
                pnl_realized DOUBLE PRECISION,
                pnl_running DOUBLE PRECISION,
                total_pnl DOUBLE PRECISION,
                ce_ltp DOUBLE PRECISION,
                pe_ltp DOUBLE PRECISION,
                status TEXT,
                note TEXT
            );
            """)

            for col, coltype in [
                ("leg",            "TEXT"),
                ("leg_entry",      "DOUBLE PRECISION"),
                ("leg_exit_price", "DOUBLE PRECISION"),
            ]:
                cur.execute(f"""
                    ALTER TABLE nifty_vwap_trade
                    ADD COLUMN IF NOT EXISTS {col} {coltype};
                """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS trade_flag (
                id BIGSERIAL PRIMARY KEY
            );
            """)
            cur.execute("""
            ALTER TABLE trade_flag
            ADD COLUMN IF NOT EXISTS live_nifty_vwap BOOLEAN DEFAULT TRUE;
            """)
            cur.execute("SELECT COUNT(*) FROM trade_flag;")
            if cur.fetchone()[0] == 0:
                cur.execute("INSERT INTO trade_flag (live_nifty_vwap) VALUES (TRUE);")

        conn.commit()

def is_kill_switch_on() -> bool:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT live_nifty_vwap FROM trade_flag ORDER BY id ASC LIMIT 1;")
                row = cur.fetchone()
                return bool(row[0]) if row and row[0] is not None else True
    except Exception as e:
        print(f"[WARN] Kill-switch DB read failed: {e} — assuming ON")
        return True

init_db()
print("[DB] Tables ready (ce_oi_change + pe_oi_change columns ensured).")

# ============================================================
# BROKER INIT
# ============================================================
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# ============================================================
# SAFE QUOTE
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
    raise RuntimeError("safe_quote: too many failures")

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
# EXPIRY HELPERS
# ============================================================
def pick_curr_next_weekly_from_instruments(instruments, today_date):
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
    return kite.instruments("NFO")

def build_opt_index(instruments):
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
    return (ce[0], pe[0], ce[1], pe[1])

# ============================================================
# STOCKO HELPERS
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
    return str(int(time.time() * 1000) + offset)[-15:]

def stocko_place_order_token(token: int, side: str, qty: int, offset=0):
    url = f"{STOCKO_BASE_URL}/api/v1/orders"
    order_id = generate_numeric_order_id(offset)
    payload = {
        "exchange": "NFO",
        "order_type": "MARKET",
        "instrument_token": int(token),
        "quantity": int(qty),
        "disclosed_quantity": 0,
        "order_side": side.upper(),
        "price": 0,
        "trigger_price": 0,
        "validity": "DAY",
        "product": "NRML",
        "client_id": STOCKO_CLIENT_ID,
        "user_order_id": order_id,
        "market_protection_percentage": 0,
        "device": "WEB"
    }
    print(f"[{ts()}] 🆔 Placing {side.upper()} token={token} order_id={order_id}")
    r = requests.post(url, json=payload, headers=_stocko_headers(), timeout=10)
    if r.status_code != 200:
        print(f"[{ts()}] ❌ Stocko {side.upper()} failed: {r.status_code} → {r.text}")
        return None
    print(f"[{ts()}] ✅ Stocko {side.upper()} placed (order_id={order_id})")
    return r.json()

def _exec_single_leg(symbol: str, side: str, offset: int) -> str:
    """Place or simulate a single-leg order."""
    if TRADE_MODE == "LIVE":
        token = stocko_search_token(symbol)
        result = stocko_place_order_token(token, side, LOT_SIZE, offset=offset)
        if result is None:
            raise RuntimeError(f"Stocko {side} failed for {symbol}")
        return "LIVE_ORDER_SENT"
    else:
        print(f"[PAPER] Simulated {side} leg: {symbol} QTY={LOT_SIZE}")
        return "PAPER_ORDER_SIMULATED"

# ============================================================
# LOGGING (Postgres)
# ============================================================
def log_trade(event, phase, expiry_tag, expiry_date, strike,
              action_price=None, vwap_at_action=None,
              leg=None, leg_entry=None, leg_exit_price=None,
              pnl_realized=None, pnl_running=None, total_pnl=None,
              ce_ltp=None, pe_ltp=None, status="", note=""):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO nifty_vwap_trade (
                        ts, event, phase, expiry_tag, expiry_date, strike,
                        action_price, vwap_at_action,
                        leg, leg_entry, leg_exit_price,
                        pnl_realized, pnl_running, total_pnl,
                        ce_ltp, pe_ltp, status, note
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,
                        %s,%s,
                        %s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,%s
                    )
                """, (
                    now_ist(), event, phase, expiry_tag, expiry_date, strike,
                    action_price, vwap_at_action,
                    leg, leg_entry, leg_exit_price,
                    pnl_realized, pnl_running, total_pnl,
                    ce_ltp, pe_ltp, status, note
                ))
            conn.commit()
    except Exception as e:
        print(f"[LOG_TRADE_FAIL] {e}")

def log_vwap(event, phase, spot,
             expiry_tag=None, expiry_date=None,
             strike=None, ltp=None, vwap=None, cum_pv=None, cum_vol=None,
             ce_vol=None, pe_vol=None, ce_ltp=None, pe_ltp=None,
             ce_oi_change=None, pe_oi_change=None,
             note=""):
    try:
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
                        ce_oi_change, pe_oi_change,
                        note
                    ) VALUES (
                        %s,%s,%s,%s,
                        %s,%s,
                        %s,%s,%s,
                        %s,%s,
                        %s,%s,
                        %s,%s,
                        %s,%s,
                        %s
                    )
                """, (
                    now_ist(), phase, event, spot,
                    expiry_tag, expiry_date,
                    strike, ltp, vwap,
                    cum_pv, cum_vol,
                    ce_vol, pe_vol,
                    ce_ltp, pe_ltp,
                    ce_oi_change, pe_oi_change,
                    note
                ))
            conn.commit()
    except Exception as e:
        print(f"[LOG_VWAP_FAIL] {e}")

# ============================================================
# v4: OI TRACKER
# ============================================================
class OITracker:
    """
    Tracks cumulative Open Interest change from a baseline snapshot taken at
    VWAP_START for ±PRELOCK_WING_STRIKES strikes of the CURRENT weekly expiry.

    Usage:
        tracker = OITracker()
        tracker.take_baseline(strikes, curr_weekly, opt_index)

        # each minute:
        ce_chg, pe_chg = tracker.get_oi_change(strike, curr_weekly, opt_index)
        # Returns (None, None) if strike not tracked or quote fails.

    The tracker stores:
        _baseline: {(strike, "CE"): baseline_oi, (strike, "PE"): baseline_oi, …}

    OI change = current_oi - baseline_oi   (positive = OI added since open)
    """

    def __init__(self):
        self._baseline: dict = {}   # {(strike, opt_type): int}

    def take_baseline(self, strikes: list, expiry, opt_index: dict):
        """
        Fetch OI for all (strike, CE/PE) pairs and store as baseline.
        Called once at VWAP_START.
        """
        tokens_to_key = {}   # token_int -> (strike, opt_type)
        for s in strikes:
            for opt_type in ("CE", "PE"):
                meta = opt_index.get((expiry, int(s), opt_type))
                if meta:
                    token_int = meta[0]
                    tokens_to_key[token_int] = (s, opt_type)

        if not tokens_to_key:
            print("[OI_TRACKER] No tokens found for baseline snapshot — OI tracking disabled.")
            return

        try:
            q = safe_quote(list(tokens_to_key.keys()))
            for token_int, key in tokens_to_key.items():
                data = q.get(token_int) or q.get(str(token_int))
                if data:
                    oi = data.get("oi", 0) or 0
                    self._baseline[key] = int(oi)
                else:
                    self._baseline[key] = 0
            print(f"[OI_TRACKER] Baseline snapshot taken for {len(self._baseline)//2} strikes "
                  f"({len(self._baseline)} legs). Expiry={expiry}")
        except Exception as e:
            print(f"[OI_TRACKER] Baseline snapshot failed: {e} — OI tracking disabled.")

    def get_oi_change(self, strike: int, expiry, opt_index: dict):
        """
        Fetch current OI for a single strike and return (ce_oi_change, pe_oi_change).
        Returns (None, None) if baseline not available or quote fails.
        """
        if not self._baseline:
            return None, None

        results = {}
        for opt_type in ("CE", "PE"):
            key = (strike, opt_type)
            if key not in self._baseline:
                results[opt_type] = None
                continue
            meta = opt_index.get((expiry, int(strike), opt_type))
            if not meta:
                results[opt_type] = None
                continue
            try:
                q = safe_quote([meta[0]])
                data = list(q.values())[0]
                current_oi = int(data.get("oi", 0) or 0)
                results[opt_type] = current_oi - self._baseline[key]
            except Exception as e:
                print(f"[OI_TRACKER] get_oi_change failed for {strike}{opt_type}: {e}")
                results[opt_type] = None

        return results.get("CE"), results.get("PE")

    def get_oi_change_batch(self, strikes: list, expiry, opt_index: dict):
        """
        Fetch OI for multiple strikes in a single quote call.
        Returns dict: {strike: (ce_oi_change, pe_oi_change)}
        """
        if not self._baseline:
            return {s: (None, None) for s in strikes}

        # Build token → (strike, opt_type) map
        token_map = {}
        for s in strikes:
            for opt_type in ("CE", "PE"):
                key = (s, opt_type)
                if key in self._baseline:
                    meta = opt_index.get((expiry, int(s), opt_type))
                    if meta:
                        token_map[meta[0]] = (s, opt_type)

        if not token_map:
            return {s: (None, None) for s in strikes}

        try:
            q = safe_quote(list(token_map.keys()))
        except Exception as e:
            print(f"[OI_TRACKER] batch quote failed: {e}")
            return {s: (None, None) for s in strikes}

        # Parse results
        current_oi: dict = {}   # {(strike, opt_type): int}
        for token_int, (s, opt_type) in token_map.items():
            data = q.get(token_int) or q.get(str(token_int))
            if data:
                current_oi[(s, opt_type)] = int(data.get("oi", 0) or 0)

        out = {}
        for s in strikes:
            ce_chg = None
            pe_chg = None
            ce_key = (s, "CE")
            pe_key = (s, "PE")
            if ce_key in self._baseline and ce_key in current_oi:
                ce_chg = current_oi[ce_key] - self._baseline[ce_key]
            if pe_key in self._baseline and pe_key in current_oi:
                pe_chg = current_oi[pe_key] - self._baseline[pe_key]
            out[s] = (ce_chg, pe_chg)
        return out


# ============================================================
# PRELOCK VWAP OBJECT (unchanged)
# ============================================================
class StraddleVWAP:
    def __init__(self, strike, expiry_date, expiry_tag, ce_token, pe_token, ce_symbol, pe_symbol):
        self.strike      = strike
        self.expiry_date = expiry_date
        self.expiry_tag  = expiry_tag
        self.ce_token    = ce_token
        self.pe_token    = pe_token
        self.ce_symbol   = ce_symbol
        self.pe_symbol   = pe_symbol

        self.last_ce_vol = None
        self.last_pe_vol = None
        self.cum_pv  = 0.0
        self.cum_vol = 0.0

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
            print(f"[WARN] {self.expiry_tag} {self.strike} update skipped: {e}")
            return (None, None, self.cum_pv, self.cum_vol, None, None, None, None)

        straddle = ce_ltp + pe_ltp
        d_ce = 0 if self.last_ce_vol is None else max(0, ce_v - self.last_ce_vol)
        d_pe = 0 if self.last_pe_vol is None else max(0, pe_v - self.last_pe_vol)
        vol  = d_ce + d_pe

        if vol > 0:
            self.cum_pv  += straddle * vol
            self.cum_vol += vol

        self.last_ce_vol = ce_v
        self.last_pe_vol = pe_v

        vwap = self.cum_pv / self.cum_vol if self.cum_vol > 0 else None
        return straddle, vwap, self.cum_pv, self.cum_vol, ce_v, pe_v, ce_ltp, pe_ltp


# ============================================================
# ACTIVE STRADDLE MANAGER (post-lock — v3 with trailing stop)
# ============================================================
class ActiveStraddleManager:
    """
    Entry State Machine:
        UNARMED  -> waiting for straddle to CLOSE ABOVE vwap
        ARMED    -> saw close above vwap; waiting for CLOSE BELOW vwap
        ENTERED  -> both legs sold; per-leg exit / re-entry active

    Leg management:
        - Partial exit: straddle > vwap + EXIT_WINDOW_ABOVE
          -> close ONLY the loss-making leg (higher LTP vs entry)
          -> profit leg stays open
        - Single remaining leg hard stop: total PnL <= MAX_LOSS_LIMIT -> close it
        - Re-entry (one leg closed): straddle crosses below vwap -> re-add closed leg
        - Re-entry (both legs closed, not hard-stopped): straddle crosses below vwap
          -> re-enter both legs (crossover still required)

    v3 — Trailing profit stop (per-lot basis):
        - trail_activation  = PROFIT_PER_LOT  × num_lots
        - trail_step        = TRAIL_PROFIT     × num_lots
        - Once total PnL >= trail_activation, trailing is armed.
        - On every candle: if trailing is armed and total_pnl drops to/below trail_floor
          → close ALL open legs immediately (TRAIL_STOP exit).
    """

    def __init__(self, strike, expiry_date, expiry_tag,
                 ce_token, pe_token, ce_symbol, pe_symbol):
        self.strike      = strike
        self.expiry_date = expiry_date
        self.expiry_tag  = expiry_tag
        self.ce_token    = ce_token
        self.pe_token    = pe_token
        self.ce_symbol   = ce_symbol
        self.pe_symbol   = pe_symbol

        self.num_lots = max(1, int(os.getenv("NUM_LOTS", "1")))

        self._trail_activation = PROFIT_PER_LOT * self.num_lots
        self._trail_step       = TRAIL_PROFIT    * self.num_lots

        self.last_ce_vol = None
        self.last_pe_vol = None
        self.cum_pv  = 0.0
        self.cum_vol = 0.0

        self.entry_state = "UNARMED"

        self.ce_open        = False
        self.pe_open        = False
        self.ce_entry_price = None
        self.pe_entry_price = None
        self.ce_realized    = 0.0
        self.pe_realized    = 0.0

        self.partial_closed_leg = None

        self.hard_stopped = False

        self.prev_close_above = None

        self.entries_taken = 0

        self.trail_armed  = False
        self.trail_floor  = None
        self.peak_pnl     = None

        print(f"[TRAIL] num_lots={self.num_lots}  "
              f"activation=₹{self._trail_activation:.0f}  "
              f"step=₹{self._trail_step:.0f}")

    def quote(self):
        q = safe_quote([self.ce_token, self.pe_token])
        v = {d["instrument_token"]: d for d in q.values()}
        ce = v[self.ce_token]
        pe = v[self.pe_token]
        return ce["last_price"], pe["last_price"], ce["volume"], pe["volume"]

    def _update_vwap(self, straddle, ce_v, pe_v):
        d_ce = 0 if self.last_ce_vol is None else max(0, ce_v - self.last_ce_vol)
        d_pe = 0 if self.last_pe_vol is None else max(0, pe_v - self.last_pe_vol)
        vol  = d_ce + d_pe

        if vol > 0:
            self.cum_pv  += straddle * vol
            self.cum_vol += vol

        self.last_ce_vol = ce_v
        self.last_pe_vol = pe_v

        return self.cum_pv / self.cum_vol if self.cum_vol > 0 else None

    def _pnl_leg(self, leg: str, ce_ltp: float, pe_ltp: float) -> float:
        if leg == "CE":
            if not self.ce_open or self.ce_entry_price is None:
                return 0.0
            return (self.ce_entry_price - ce_ltp) * LOT_SIZE
        else:
            if not self.pe_open or self.pe_entry_price is None:
                return 0.0
            return (self.pe_entry_price - pe_ltp) * LOT_SIZE

    def pnl_running(self, ce_ltp, pe_ltp):
        return self._pnl_leg("CE", ce_ltp, pe_ltp) + self._pnl_leg("PE", ce_ltp, pe_ltp)

    def pnl_total(self, ce_ltp, pe_ltp):
        return self.ce_realized + self.pe_realized + self.pnl_running(ce_ltp, pe_ltp)

    def status_str(self):
        ce = "OPEN" if self.ce_open else "CLOSED"
        pe = "OPEN" if self.pe_open else "CLOSED"
        trail_info = ""
        if self.trail_armed:
            trail_info = f" TRAIL_FLOOR=₹{self.trail_floor:.0f}"
        return f"{self.entry_state} CE={ce} PE={pe}{trail_info}"

    def _update_trail(self, total_pnl: float) -> bool:
        if not self.trail_armed:
            if total_pnl >= self._trail_activation:
                self.trail_armed = True
                self.peak_pnl   = total_pnl
                self.trail_floor = self._trail_activation - self._trail_step
                print(f"[TRAIL_ARMED] {ts()} total_pnl=₹{total_pnl:.2f} "
                      f"activation=₹{self._trail_activation:.0f} "
                      f"initial_floor=₹{self.trail_floor:.0f}")
                log_trade("TRAIL_ARMED", "locked",
                          self.expiry_tag, self.expiry_date, self.strike,
                          total_pnl=total_pnl,
                          note=f"Trailing activated. floor=₹{self.trail_floor:.0f} "
                               f"step=₹{self._trail_step:.0f}")
            return False

        if total_pnl > self.peak_pnl:
            self.peak_pnl = total_pnl
            steps = int((self.peak_pnl - self._trail_activation) / self._trail_step)
            new_floor = (self._trail_activation - self._trail_step) + steps * self._trail_step
            if new_floor > self.trail_floor:
                old_floor = self.trail_floor
                self.trail_floor = new_floor
                print(f"[TRAIL_RATCHET] {ts()} peak=₹{self.peak_pnl:.2f} "
                      f"floor moved ₹{old_floor:.0f} → ₹{self.trail_floor:.0f}")
                log_trade("TRAIL_RATCHET", "locked",
                          self.expiry_tag, self.expiry_date, self.strike,
                          total_pnl=total_pnl,
                          note=f"Trail floor ₹{old_floor:.0f}→₹{self.trail_floor:.0f} "
                               f"peak=₹{self.peak_pnl:.2f}")

        if total_pnl <= self.trail_floor:
            return True

        return False

    def _open_leg(self, leg: str, ce_ltp: float, pe_ltp: float,
                  vwap: float, phase: str, reason: str) -> bool:
        symbol = self.ce_symbol if leg == "CE" else self.pe_symbol
        offset = 0 if leg == "CE" else 1
        try:
            exec_note = _exec_single_leg(symbol, "SELL", offset)
        except Exception as e:
            print(f"[OPEN_LEG_FAIL] {leg} {symbol}: {e}")
            log_trade("OPEN_LEG_FAIL", phase, self.expiry_tag, self.expiry_date, self.strike,
                      action_price=ce_ltp + pe_ltp, vwap_at_action=vwap,
                      leg=leg, ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                      status=self.status_str(), note=f"{reason} {e}")
            return False

        if leg == "CE":
            self.ce_open        = True
            self.ce_entry_price = ce_ltp
        else:
            self.pe_open        = True
            self.pe_entry_price = pe_ltp

        entry_px = ce_ltp if leg == "CE" else pe_ltp
        log_trade("OPEN_LEG", phase, self.expiry_tag, self.expiry_date, self.strike,
                  action_price=ce_ltp + pe_ltp, vwap_at_action=vwap,
                  leg=leg, leg_entry=entry_px,
                  ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                  status=self.status_str(), note=f"{reason} {exec_note}")
        print(f"[OPEN_LEG {TRADE_MODE}] {ts()} leg={leg} symbol={symbol} "
              f"ce={ce_ltp:.2f} pe={pe_ltp:.2f} reason={reason}")
        return True

    def _close_leg(self, leg: str, ce_ltp: float, pe_ltp: float,
                   vwap: float, phase: str, reason: str) -> bool:
        symbol   = self.ce_symbol if leg == "CE" else self.pe_symbol
        exit_ltp = ce_ltp if leg == "CE" else pe_ltp
        entry_px = self.ce_entry_price if leg == "CE" else self.pe_entry_price
        offset   = 10 if leg == "CE" else 11
        try:
            exec_note = _exec_single_leg(symbol, "BUY", offset)
        except Exception as e:
            print(f"[CLOSE_LEG_FAIL] {leg} {symbol}: {e}")
            log_trade("CLOSE_LEG_FAIL", phase, self.expiry_tag, self.expiry_date, self.strike,
                      action_price=ce_ltp + pe_ltp, vwap_at_action=vwap,
                      leg=leg, leg_entry=entry_px, leg_exit_price=exit_ltp,
                      ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                      status=self.status_str(), note=f"{reason} {e}")
            return False

        leg_pnl = (entry_px - exit_ltp) * LOT_SIZE if entry_px is not None else 0.0

        if leg == "CE":
            self.ce_realized += leg_pnl
            self.ce_open      = False
        else:
            self.pe_realized += leg_pnl
            self.pe_open      = False

        total = self.pnl_total(ce_ltp, pe_ltp)
        log_trade("CLOSE_LEG", phase, self.expiry_tag, self.expiry_date, self.strike,
                  action_price=ce_ltp + pe_ltp, vwap_at_action=vwap,
                  leg=leg, leg_entry=entry_px, leg_exit_price=exit_ltp,
                  pnl_realized=leg_pnl, total_pnl=total,
                  ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                  status=self.status_str(), note=f"{reason} {exec_note}")
        print(f"[CLOSE_LEG {TRADE_MODE}] {ts()} leg={leg} pnl=₹{leg_pnl:.2f} "
              f"total=₹{total:.2f} reason={reason}")
        return True

    def _close_all_open_legs(self, ce_ltp, pe_ltp, vwap, phase, reason):
        if self.ce_open:
            self._close_leg("CE", ce_ltp, pe_ltp, vwap, phase, reason)
        if self.pe_open:
            self._close_leg("PE", ce_ltp, pe_ltp, vwap, phase, reason)

    def on_minute_close(self, phase: str, trade_allowed: bool = True,
                        ce_oi_change=None, pe_oi_change=None):
        """
        Called once per minute candle close.
        ce_oi_change / pe_oi_change: pre-computed OI deltas (passed in from main loop).
        trade_allowed=False  → VWAP still updates, exits/SL/trail still fire,
                               but NO new entries are placed.
        Returns (straddle_ltp, vwap, ce_ltp, pe_ltp) or (None, None, None, None) on error.
        """
        if self.hard_stopped:
            return None, None, None, None

        try:
            ce_ltp, pe_ltp, ce_v, pe_v = self.quote()
        except Exception as e:
            print(f"[WARN] quote error in on_minute_close: {e}")
            return None, None, None, None

        straddle = ce_ltp + pe_ltp
        vwap = self._update_vwap(straddle, ce_v, pe_v)

        if vwap is None:
            return straddle, None, ce_ltp, pe_ltp

        above_vwap = straddle > vwap

        # ========== UNARMED ==========
        if self.entry_state == "UNARMED":
            if above_vwap:
                self.entry_state      = "ARMED"
                self.prev_close_above = True
                print(f"[ARMED] {ts()} straddle={straddle:.2f} vwap={vwap:.2f} — waiting for close below")
                log_trade("ARMED", phase, self.expiry_tag, self.expiry_date, self.strike,
                          action_price=straddle, vwap_at_action=vwap,
                          ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                          status=self.status_str(), note="Straddle closed above vwap — armed")
            else:
                self.prev_close_above = False
            return straddle, vwap, ce_ltp, pe_ltp

        # ========== ARMED ==========
        if self.entry_state == "ARMED":
            if not above_vwap:
                if not trade_allowed:
                    print(f"[ENTRY_BLOCKED] Kill-switch flag=FALSE — crossover detected but entry suppressed")
                elif self.entries_taken >= MAX_ENTRIES_PER_STRIKE:
                    print(f"[ENTRY_SKIP] max entries={MAX_ENTRIES_PER_STRIKE} reached")
                else:
                    ok_ce = self._open_leg("CE", ce_ltp, pe_ltp, vwap, phase, "INITIAL_ENTRY")
                    ok_pe = self._open_leg("PE", ce_ltp, pe_ltp, vwap, phase, "INITIAL_ENTRY")
                    if ok_ce and ok_pe:
                        self.entry_state        = "ENTERED"
                        self.partial_closed_leg = None
                        self.entries_taken     += 1
                        log_trade("ENTRY", phase, self.expiry_tag, self.expiry_date, self.strike,
                                  action_price=straddle, vwap_at_action=vwap,
                                  ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                                  total_pnl=self.pnl_total(ce_ltp, pe_ltp),
                                  status=self.status_str(),
                                  note=f"Both legs SELL entries={self.entries_taken} {TRADE_MODE}")
                        print(f"[ENTRY {TRADE_MODE}] {ts()} strike={self.strike} "
                              f"straddle={straddle:.2f} vwap={vwap:.2f} entries={self.entries_taken}")
            self.prev_close_above = above_vwap
            return straddle, vwap, ce_ltp, pe_ltp

        # ========== ENTERED ==========
        if self.entry_state == "ENTERED":
            total = self.pnl_total(ce_ltp, pe_ltp)

            # PRIORITY 1: HARD STOP
            if total <= MAX_LOSS_LIMIT:
                print(f"[HARD_SL] {ts()} total=₹{total:.2f} <= limit=₹{MAX_LOSS_LIMIT} — closing all")
                self._close_all_open_legs(ce_ltp, pe_ltp, vwap, phase, "HARD_SL")
                self.hard_stopped = True
                log_trade("HARD_SL_DONE", phase, self.expiry_tag, self.expiry_date, self.strike,
                          action_price=straddle, vwap_at_action=vwap,
                          ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                          total_pnl=self.pnl_total(ce_ltp, pe_ltp),
                          status=self.status_str(), note="Hard stop — all legs closed")
                return straddle, vwap, ce_ltp, pe_ltp

            # PRIORITY 2: TRAILING PROFIT STOP
            any_open = self.ce_open or self.pe_open
            if any_open and self._trail_activation > 0:
                trail_hit = self._update_trail(total)
                if trail_hit:
                    print(f"[TRAIL_STOP] {ts()} total=₹{total:.2f} <= "
                          f"floor=₹{self.trail_floor:.0f} — closing all open legs")
                    self._close_all_open_legs(ce_ltp, pe_ltp, vwap, phase, "TRAIL_STOP")
                    self.hard_stopped = True
                    total_after = self.pnl_total(ce_ltp, pe_ltp)
                    log_trade("TRAIL_STOP", phase, self.expiry_tag, self.expiry_date, self.strike,
                              action_price=straddle, vwap_at_action=vwap,
                              ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                              total_pnl=total_after,
                              status=self.status_str(),
                              note=f"Trail floor=₹{self.trail_floor:.0f} hit. "
                                   f"peak=₹{self.peak_pnl:.2f} final=₹{total_after:.2f}")
                    return straddle, vwap, ce_ltp, pe_ltp

            # PRIORITY 3: SOLO LEG SL
            solo_leg = None
            if self.ce_open and not self.pe_open:
                solo_leg = "CE"
            elif self.pe_open and not self.ce_open:
                solo_leg = "PE"

            if solo_leg is not None:
                solo_entry = self.ce_entry_price if solo_leg == "CE" else self.pe_entry_price
                solo_ltp   = ce_ltp             if solo_leg == "CE" else pe_ltp
                if solo_entry is not None:
                    leg_drawdown = solo_ltp - solo_entry
                    if leg_drawdown > SINGLE_LEG_SL:
                        print(f"[SOLO_LEG_SL] {ts()} {solo_leg} drawdown=₹{leg_drawdown:.2f} "
                              f"> SL={SINGLE_LEG_SL} (entry={solo_entry:.2f} ltp={solo_ltp:.2f}) — closing")
                        ok = self._close_leg(solo_leg, ce_ltp, pe_ltp, vwap, phase, "SOLO_LEG_SL")
                        if ok:
                            log_trade("SOLO_LEG_SL", phase, self.expiry_tag, self.expiry_date, self.strike,
                                      action_price=straddle, vwap_at_action=vwap,
                                      leg=solo_leg,
                                      leg_entry=solo_entry, leg_exit_price=solo_ltp,
                                      ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                                      total_pnl=self.pnl_total(ce_ltp, pe_ltp),
                                      status=self.status_str(),
                                      note=f"Solo leg SL hit: drawdown=₹{leg_drawdown:.2f} > {SINGLE_LEG_SL}")
                        self.prev_close_above = above_vwap
                        return straddle, vwap, ce_ltp, pe_ltp

            # PRIORITY 4: PARTIAL EXIT
            if straddle > (vwap + EXIT_WINDOW_ABOVE) and self.ce_open and self.pe_open:
                ce_loss = ce_ltp - (self.ce_entry_price or ce_ltp)
                pe_loss = pe_ltp - (self.pe_entry_price or pe_ltp)
                loss_leg = "CE" if ce_loss >= pe_loss else "PE"

                print(f"[PARTIAL_EXIT] {ts()} straddle={straddle:.2f} vwap={vwap:.2f} "
                      f"ce_loss={ce_loss:.2f} pe_loss={pe_loss:.2f} closing={loss_leg}")
                ok = self._close_leg(loss_leg, ce_ltp, pe_ltp, vwap, phase, "PARTIAL_EXIT")
                if ok:
                    self.partial_closed_leg = loss_leg
                    log_trade("PARTIAL_EXIT", phase, self.expiry_tag, self.expiry_date, self.strike,
                              action_price=straddle, vwap_at_action=vwap,
                              leg=loss_leg,
                              ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                              total_pnl=self.pnl_total(ce_ltp, pe_ltp),
                              status=self.status_str(),
                              note=f"Closed loss leg={loss_leg}, profit leg stays open")

            # PRIORITY 5: RE-ENTRY LOGIC
            elif (not above_vwap) and self.prev_close_above:

                if self.partial_closed_leg is not None:
                    reopen_leg = self.partial_closed_leg
                    if not trade_allowed:
                        print(f"[RE_ENTRY_BLOCKED] Kill-switch — {reopen_leg} re-entry suppressed")
                    elif self.entries_taken < MAX_ENTRIES_PER_STRIKE:
                        print(f"[RE_ENTRY_ONE_LEG] {ts()} re-adding {reopen_leg} "
                              f"straddle={straddle:.2f} crossed below vwap={vwap:.2f}")
                        ok = self._open_leg(reopen_leg, ce_ltp, pe_ltp, vwap, phase, "RE_ENTRY")
                        if ok:
                            self.partial_closed_leg = None
                            self.entries_taken += 1
                            log_trade("RE_ENTRY", phase, self.expiry_tag, self.expiry_date, self.strike,
                                      action_price=straddle, vwap_at_action=vwap,
                                      leg=reopen_leg,
                                      ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                                      total_pnl=self.pnl_total(ce_ltp, pe_ltp),
                                      status=self.status_str(),
                                      note=f"Re-entered {reopen_leg} entries={self.entries_taken} {TRADE_MODE}")
                    else:
                        print(f"[RE_ENTRY_SKIP] max entries={MAX_ENTRIES_PER_STRIKE} reached")

                elif not self.ce_open and not self.pe_open and not self.hard_stopped:
                    if not trade_allowed:
                        print(f"[RE_ENTRY_BOTH_BLOCKED] Kill-switch — both-leg re-entry suppressed")
                    elif self.entries_taken < MAX_ENTRIES_PER_STRIKE:
                        print(f"[RE_ENTRY_BOTH] {ts()} both legs closed, "
                              f"straddle crossed below vwap={vwap:.2f} — re-entering both")
                        ok_ce = self._open_leg("CE", ce_ltp, pe_ltp, vwap, phase, "RE_ENTRY_BOTH")
                        ok_pe = self._open_leg("PE", ce_ltp, pe_ltp, vwap, phase, "RE_ENTRY_BOTH")
                        if ok_ce and ok_pe:
                            self.partial_closed_leg = None
                            self.entries_taken += 1
                            log_trade("RE_ENTRY_BOTH", phase, self.expiry_tag, self.expiry_date, self.strike,
                                      action_price=straddle, vwap_at_action=vwap,
                                      ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                                      total_pnl=self.pnl_total(ce_ltp, pe_ltp),
                                      status=self.status_str(),
                                      note=f"Both legs re-entered entries={self.entries_taken} {TRADE_MODE}")
                    else:
                        print(f"[RE_ENTRY_BOTH_SKIP] max entries={MAX_ENTRIES_PER_STRIKE} reached")

            self.prev_close_above = above_vwap
            return straddle, vwap, ce_ltp, pe_ltp

        return straddle, vwap, ce_ltp, pe_ltp

    def close_all_now(self, phase: str, reason: str = "EOD_CLOSE"):
        """Force-close all open legs immediately."""
        if self.hard_stopped:
            return
        try:
            ce_ltp, pe_ltp, _, _ = self.quote()
        except Exception:
            ce_ltp = pe_ltp = 0.0

        vwap = self.cum_pv / self.cum_vol if self.cum_vol > 0 else None
        self._close_all_open_legs(ce_ltp, pe_ltp, vwap, phase, reason)
        total = self.pnl_total(ce_ltp, pe_ltp)
        print(f"[{reason}] {ts()} ce_realized=₹{self.ce_realized:.2f} "
              f"pe_realized=₹{self.pe_realized:.2f} total=₹{total:.2f}")


# ============================================================
# MAIN
# ============================================================
def main():
    print("[INIT] Starting VWAP Straddle v4 (Railway) — crossover entry + per-leg exit + trail stop + OI tracking …")

    while now_ist().time() < VWAP_START:
        time.sleep(0.5)

    instruments = load_instruments_nfo()
    opt_index   = build_opt_index(instruments)
    today       = now_ist().date()

    curr_weekly, next_weekly = pick_curr_next_weekly_from_instruments(instruments, today)
    print(f"[EXPIRY] Current weekly={curr_weekly}  Next weekly={next_weekly}")

    try:
        spot_start = get_spot_ltp()
    except Exception as e:
        raise RuntimeError(f"Could not read spot at VWAP start: {e}")

    base_atm = nearest_50(spot_start)
    strikes  = [base_atm + i * STRIKE_STEP
                for i in range(-PRELOCK_WING_STRIKES, PRELOCK_WING_STRIKES + 1)]
    prelock_strikes = set(strikes)

    print(f"[PRELOCK] Base ATM={base_atm}  range {min(strikes)}..{max(strikes)}")

    # --------------------------------------------------------
    # v4: Initialise OI tracker and take baseline snapshot
    # --------------------------------------------------------
    oi_tracker = OITracker()
    oi_tracker.take_baseline(strikes, curr_weekly, opt_index)
    # (OI is only tracked for CURRENT week expiry per the spec)

    prelock_objs = {"CURR_WEEK": {}, "NEXT_WEEK": {}}
    for s in strikes:
        meta_c = get_option_pair_for_strike_fast(s, curr_weekly, opt_index)
        meta_n = get_option_pair_for_strike_fast(s, next_weekly, opt_index)
        if not meta_c:
            raise RuntimeError(f"No CE/PE for CURR weekly {curr_weekly} strike={s}")
        if not meta_n:
            raise RuntimeError(f"No CE/PE for NEXT weekly {next_weekly} strike={s}")
        prelock_objs["CURR_WEEK"][s] = StraddleVWAP(s, curr_weekly, "CURR_WEEK", *meta_c)
        prelock_objs["NEXT_WEEK"][s] = StraddleVWAP(s, next_weekly, "NEXT_WEEK", *meta_n)

    did_lock      = False
    phase         = "prelock"
    active        = None        # ActiveStraddleManager (post-lock)
    active_tag    = None
    active_expiry = None
    atm_locked    = None
    last_min      = None

    kill_positions_closed = False

    CANDLE_CLOSE_DELAY_SECS = int(os.getenv("CANDLE_CLOSE_DELAY_SECS", "5"))

    # ============================================================
    # MAIN LOOP
    # ============================================================
    while now_ist().time() < EXIT_CUTOFF:
        now      = now_ist()
        curr_min = now.minute
        curr_sec = now.second

        # ---- KILL SWITCH ----
        trade_allowed = is_kill_switch_on()

        if not trade_allowed:
            if active is not None and not kill_positions_closed:
                any_open = active.ce_open or active.pe_open
                if any_open:
                    print(f"[KILL SWITCH] flag=FALSE — closing all open legs …")
                    try:
                        active.close_all_now(phase, reason="FLAG_EXIT")
                        if not active.ce_open and not active.pe_open:
                            kill_positions_closed = True
                            print("[KILL SWITCH] All positions closed successfully.")
                            log_trade("FLAG_EXIT", phase, active_tag or "NA", active_expiry, atm_locked,
                                      status="FORCED_EXIT",
                                      note="All legs closed — kill-switch flag=FALSE")
                        else:
                            print("[KILL SWITCH] Some legs still open after close attempt — will retry.")
                            log_trade("FLAG_EXIT_PARTIAL", phase, active_tag or "NA", active_expiry, atm_locked,
                                      status="FORCED_EXIT_PARTIAL",
                                      note="Close attempted but some legs still open — retrying next tick")
                    except Exception as e:
                        print(f"[KILL SWITCH] Close attempt failed: {e} — will retry next tick")
                        log_trade("FLAG_EXIT_FAIL", phase, active_tag or "NA", active_expiry, atm_locked,
                                  status="FORCED_EXIT_FAIL",
                                  note=f"Kill-switch close failed: {e} — retrying")
                else:
                    kill_positions_closed = True
                    print("[KILL SWITCH] flag=FALSE — no open positions. Entries blocked.")
                    log_trade("FLAG_BLOCK", phase, active_tag or "NA", active_expiry, atm_locked,
                              status="ENTRY_BLOCKED",
                              note="Kill-switch flag=FALSE — no positions to close, entries blocked")

            elif active is None and not kill_positions_closed:
                kill_positions_closed = True
                print("[KILL SWITCH] flag=FALSE in pre-lock phase — entries blocked.")
                log_trade("FLAG_BLOCK", phase, "NA", None, None,
                          status="ENTRY_BLOCKED",
                          note="Kill-switch flag=FALSE in pre-lock — entries blocked")

            time.sleep(1)
            continue

        if trade_allowed and kill_positions_closed:
            print("[KILL SWITCH] flag restored to TRUE — entries re-enabled.")
            log_trade("FLAG_RESTORED", phase, active_tag or "NA", active_expiry, atm_locked,
                      status="ENTRY_ENABLED",
                      note="Kill-switch flag=TRUE restored — normal operation resumed")
            kill_positions_closed = False

        # ---- SPOT ----
        try:
            spot = get_spot_ltp()
        except Exception as e:
            print(f"[WARN] Spot quote failed: {e}")
            time.sleep(1)
            continue

        # ---- PRELOCK: update once per minute ----
        if not did_lock:
            if last_min != curr_min and curr_sec >= CANDLE_CLOSE_DELAY_SECS:
                last_min = curr_min

                # v4: fetch OI changes for ALL prelock strikes in one batch call
                oi_changes = oi_tracker.get_oi_change_batch(strikes, curr_weekly, opt_index)

                for tag in ("CURR_WEEK", "NEXT_WEEK"):
                    for s, obj in prelock_objs[tag].items():
                        ltp, vwap, cum_pv, cum_vol, ce_v, pe_v, ce_ltp, pe_ltp = obj.update()

                        # OI changes are tracked against CURR_WEEK baseline regardless of tag
                        ce_oi_chg, pe_oi_chg = oi_changes.get(s, (None, None))

                        log_vwap("PRELOCK_VWAP", phase, spot,
                                 expiry_tag=tag, expiry_date=obj.expiry_date,
                                 strike=s, ltp=ltp, vwap=vwap,
                                 cum_pv=cum_pv, cum_vol=cum_vol,
                                 ce_vol=ce_v, pe_vol=pe_v,
                                 ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                                 ce_oi_change=ce_oi_chg, pe_oi_change=pe_oi_chg,
                                 note="prelock tracking")

                        if ce_oi_chg is not None or pe_oi_chg is not None:
                            print(f"[OI] {ts()} {tag} strike={s} "
                                  f"ce_oi_chg={ce_oi_chg} pe_oi_chg={pe_oi_chg}")

            if now.time() < ATM_LOCK_TIME:
                time.sleep(1)
                continue

        # ---- ATM LOCK (first time only) ----
        if (not did_lock) and now.time() >= ATM_LOCK_TIME:
            atm_locked = nearest_50(spot)

            if atm_locked not in prelock_strikes:
                msg = (f"[FATAL] ATM at freeze={atm_locked} outside prelock range "
                       f"[{min(prelock_strikes)}..{max(prelock_strikes)}]. Stopping.")
                print(msg)
                log_trade("OUT_OF_RANGE_EXIT", "locked", "NA", today, atm_locked, note=msg)
                sys.exit(0)

            obj_curr_lock = prelock_objs["CURR_WEEK"][atm_locked]
            ltp_curr, vwap_curr, *_ = obj_curr_lock.update()
            if ltp_curr is None:
                raise RuntimeError("Could not read CURR-week straddle premium at lock time")

            active_tag    = "CURR_WEEK" if ltp_curr > PREMIUM_SWITCH_TH else "NEXT_WEEK"
            active_expiry = curr_weekly  if active_tag == "CURR_WEEK"   else next_weekly

            chosen_prelock = prelock_objs[active_tag][atm_locked]

            active = ActiveStraddleManager(
                strike=atm_locked, expiry_date=active_expiry, expiry_tag=active_tag,
                ce_token=chosen_prelock.ce_token, pe_token=chosen_prelock.pe_token,
                ce_symbol=chosen_prelock.ce_symbol, pe_symbol=chosen_prelock.pe_symbol
            )
            # Seed VWAP state from prelock for continuity
            active.cum_pv      = chosen_prelock.cum_pv
            active.cum_vol     = chosen_prelock.cum_vol
            active.last_ce_vol = chosen_prelock.last_ce_vol
            active.last_pe_vol = chosen_prelock.last_pe_vol

            prelock_objs = None   # free memory

            did_lock = True
            phase    = "locked"
            last_min = None

            note = (f"ATM_LOCK={atm_locked} curr_prem={ltp_curr:.2f} th={PREMIUM_SWITCH_TH:.2f} "
                    f"-> TRADE={active_tag} expiry={active_expiry} MODE={TRADE_MODE} "
                    f"num_lots={active.num_lots} trail_activation=₹{active._trail_activation:.0f} "
                    f"trail_step=₹{active._trail_step:.0f}")
            log_trade("ATM_LOCK", phase, active_tag, active_expiry, atm_locked,
                      action_price=ltp_curr, vwap_at_action=vwap_curr, note=note)
            print(f"[LOCK] {ts()} {note}")

        # ---- POST-LOCK: process once per minute ----
        if did_lock and (last_min != curr_min) and curr_sec >= CANDLE_CLOSE_DELAY_SECS:
            last_min = curr_min

            # v4: fetch OI change for the LOCKED strike only
            ce_oi_chg, pe_oi_chg = oi_tracker.get_oi_change(
                atm_locked, curr_weekly, opt_index
            )

            straddle, vwap, ce_ltp, pe_ltp = active.on_minute_close(
                phase,
                trade_allowed=trade_allowed,
                ce_oi_change=ce_oi_chg,
                pe_oi_change=pe_oi_chg,
            )

            if straddle is not None and vwap is not None:
                run_pnl   = active.pnl_running(ce_ltp, pe_ltp)
                total_pnl = active.pnl_total(ce_ltp, pe_ltp)

                trail_note = ""
                if active.trail_armed:
                    trail_note = (f" | TRAIL armed peak=₹{active.peak_pnl:.2f} "
                                  f"floor=₹{active.trail_floor:.0f}")

                oi_note = ""
                if ce_oi_chg is not None or pe_oi_chg is not None:
                    oi_note = f" | OI_chg CE={ce_oi_chg} PE={pe_oi_chg}"
                    print(f"[OI] {ts()} locked strike={atm_locked} "
                          f"ce_oi_chg={ce_oi_chg} pe_oi_chg={pe_oi_chg}")

                log_vwap("VWAP_ROW", phase, spot,
                         expiry_tag=active_tag, expiry_date=active_expiry,
                         strike=atm_locked, ltp=straddle, vwap=vwap,
                         cum_pv=active.cum_pv, cum_vol=active.cum_vol,
                         ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                         ce_oi_change=ce_oi_chg, pe_oi_change=pe_oi_chg,
                         note=f"state={active.entry_state} {active.status_str()} MODE={TRADE_MODE}{trail_note}{oi_note}")

                log_trade("HEARTBEAT", phase, active_tag, active_expiry, atm_locked,
                          action_price=straddle, vwap_at_action=vwap,
                          pnl_running=run_pnl,
                          total_pnl=total_pnl,
                          ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                          status=active.status_str(),
                          note=f"entries={active.entries_taken} MODE={TRADE_MODE}{trail_note}{oi_note}")

        time.sleep(1)

    # ---- EOD: close any remaining open legs ----
    if active is not None:
        print("[EOD] Closing all open legs …")
        active.close_all_now(phase, reason="EOD_CLOSE")

    print("[DONE] Trading day complete.")


# ============================================================
if __name__ == "__main__":
    main()
