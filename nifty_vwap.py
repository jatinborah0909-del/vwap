#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
VWAP Straddle Strategy — Kite (data) + Stocko (orders) + Railway Postgres
==========================================================================

v2 CHANGES
----------
✅ Crossover-based entry:
   - Straddle must CLOSE ABOVE vwap first  (arms the trigger)
   - Then CLOSE BELOW vwap                 (entry fires — both legs SELL)
   - Simple "below vwap" alone is NOT enough

✅ Per-leg exit management:
   - Exit signal (straddle > vwap + EXIT_WINDOW_ABOVE): close ONLY the loss-making leg
   - Profit leg stays open and continues running
   - If remaining single leg causes total PnL <= MAX_LOSS_LIMIT -> hard stop that leg too

✅ Re-entry logic:
   - One leg closed (partial exit): straddle crosses below vwap -> re-add the closed leg
   - Both legs closed (not hard-stopped): straddle crosses below vwap -> re-enter both legs
   - Re-entry still requires the above->below crossover; bare "below vwap" alone won't trigger

✅ Railway / Postgres logging (unchanged)
✅ Kill switch: trade_flag.live_nifty_vwap — now handles per-leg open positions
✅ EOD force-close of any open legs

PRELOCK (unchanged)
-------------------
✅ Track CURRENT week + NEXT week expiry VWAP from VWAP_START
✅ ATM fixed at VWAP_START spot price; track ±PRELOCK_WING_STRIKES strikes
✅ At ATM_FREEZE_TIME: decide expiry (curr prem > PREMIUM_SWITCH_TH -> curr, else next)
✅ Hard stop if frozen ATM outside prelock range

LIVE vs PAPER
-------------
- TRADE_MODE=PAPER  -> NO real orders (only logs)
- TRADE_MODE=LIVE   -> Orders via Stocko API
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

# Once the loss leg is closed and only one leg is running solo,
# close it if its LTP rises more than this many points above its entry price.
# i.e. surviving leg is now losing: (current_ltp - entry_price) > SINGLE_LEG_SL
# Set to 9999 to effectively disable.
SINGLE_LEG_SL          = float(os.getenv("SINGLE_LEG_SL",           "10.0"))

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

            # v2: added leg, leg_entry, leg_exit_price columns
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

            # Add v2 columns to existing table if upgrading in place
            for col, coltype in [
                ("leg",           "TEXT"),
                ("leg_entry",     "DOUBLE PRECISION"),
                ("leg_exit_price","DOUBLE PRECISION"),
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
print("[DB] Tables ready.")

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
             ce_vol=None, pe_vol=None, ce_ltp=None, pe_ltp=None, note=""):
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
                        ce_ltp, pe_ltp, note
                    ) VALUES (
                        %s,%s,%s,%s,
                        %s,%s,
                        %s,%s,%s,
                        %s,%s,
                        %s,%s,
                        %s,%s,%s
                    )
                """, (
                    now_ist(), phase, event, spot,
                    expiry_tag, expiry_date,
                    strike, ltp, vwap,
                    cum_pv, cum_vol,
                    ce_vol, pe_vol,
                    ce_ltp, pe_ltp, note
                ))
            conn.commit()
    except Exception as e:
        print(f"[LOG_VWAP_FAIL] {e}")

# ============================================================
# PRELOCK VWAP OBJECT (unchanged — used only before ATM lock)
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
# ACTIVE STRADDLE MANAGER (post-lock — v2 logic)
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

        # VWAP (volume-weighted, seeded from prelock object after lock)
        self.last_ce_vol = None
        self.last_pe_vol = None
        self.cum_pv  = 0.0
        self.cum_vol = 0.0

        # Entry state machine
        self.entry_state = "UNARMED"   # UNARMED | ARMED | ENTERED

        # Per-leg state
        self.ce_open        = False
        self.pe_open        = False
        self.ce_entry_price = None   # CE LTP at time of sell
        self.pe_entry_price = None   # PE LTP at time of sell
        self.ce_realized    = 0.0
        self.pe_realized    = 0.0

        # Which leg is currently closed after a partial exit (for re-entry)
        self.partial_closed_leg = None   # "CE" | "PE" | None

        # Hard-stopped: no further action
        self.hard_stopped = False

        # For crossover detection (was previous candle close above vwap?)
        self.prev_close_above = None

        # Total entries taken (both legs count as 1 entry)
        self.entries_taken = 0

    # ---------------------------------------------------------------- quote
    def quote(self):
        q = safe_quote([self.ce_token, self.pe_token])
        v = {d["instrument_token"]: d for d in q.values()}
        ce = v[self.ce_token]
        pe = v[self.pe_token]
        return ce["last_price"], pe["last_price"], ce["volume"], pe["volume"]

    # ---------------------------------------------------------------- vwap
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

    # ---------------------------------------------------------------- pnl helpers
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
        return f"{self.entry_state} CE={ce} PE={pe}"

    # ---------------------------------------------------------------- order helpers
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
        print(f"[CLOSE_LEG {TRADE_MODE}] {ts()} leg={leg} pnl={leg_pnl:.2f} "
              f"total={total:.2f} reason={reason}")
        return True

    def _close_all_open_legs(self, ce_ltp, pe_ltp, vwap, phase, reason):
        if self.ce_open:
            self._close_leg("CE", ce_ltp, pe_ltp, vwap, phase, reason)
        if self.pe_open:
            self._close_leg("PE", ce_ltp, pe_ltp, vwap, phase, reason)

    # ---------------------------------------------------------------- main per-minute handler
    def on_minute_close(self, phase: str, trade_allowed: bool = True):
        """
        Called once per minute candle close.
        trade_allowed=False  -> VWAP still updates, exits/SL still fire,
                                but NO new entries (initial or re-entry) are placed.
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
                # Crossover: above -> below
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

            # --- HARD STOP: total PnL breached (check first, before any other logic) ---
            if total <= MAX_LOSS_LIMIT:
                print(f"[HARD_SL] {ts()} total={total:.2f} <= limit={MAX_LOSS_LIMIT} — closing all")
                self._close_all_open_legs(ce_ltp, pe_ltp, vwap, phase, "HARD_SL")
                self.hard_stopped = True
                log_trade("HARD_SL_DONE", phase, self.expiry_tag, self.expiry_date, self.strike,
                          action_price=straddle, vwap_at_action=vwap,
                          ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                          total_pnl=self.pnl_total(ce_ltp, pe_ltp),
                          status=self.status_str(), note="Hard stop — all legs closed")
                return straddle, vwap, ce_ltp, pe_ltp

            # --- SOLO LEG SL: exactly one leg open (other was partially exited) ---
            # The surviving leg is now naked. Monitor it independently.
            # If its LTP rises more than SINGLE_LEG_SL above its own entry price,
            # the market reversed and it is now losing — close it.
            solo_leg = None
            if self.ce_open and not self.pe_open:
                solo_leg = "CE"
            elif self.pe_open and not self.ce_open:
                solo_leg = "PE"

            if solo_leg is not None:
                solo_entry = self.ce_entry_price if solo_leg == "CE" else self.pe_entry_price
                solo_ltp   = ce_ltp             if solo_leg == "CE" else pe_ltp
                if solo_entry is not None:
                    # Positive value means LTP moved above entry → leg is losing
                    leg_drawdown = solo_ltp - solo_entry
                    if leg_drawdown > SINGLE_LEG_SL:
                        print(f"[SOLO_LEG_SL] {ts()} {solo_leg} solo leg drawdown={leg_drawdown:.2f} "
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
                                      note=f"Solo leg SL hit: drawdown={leg_drawdown:.2f} > {SINGLE_LEG_SL}")
                        self.prev_close_above = above_vwap
                        return straddle, vwap, ce_ltp, pe_ltp

            # --- PARTIAL EXIT: straddle above vwap + buffer, BOTH legs open ---
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

            # --- RE-ENTRY LOGIC ---
            elif (not above_vwap) and self.prev_close_above:

                # Case A: one leg was partially closed — re-add it
                if self.partial_closed_leg is not None:
                    reopen_leg = self.partial_closed_leg
                    if not trade_allowed:
                        print(f"[RE_ENTRY_BLOCKED] Kill-switch flag=FALSE — crossover detected but {reopen_leg} re-entry suppressed")
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

                # Case B: both legs closed (not hard-stopped) — re-enter both
                elif not self.ce_open and not self.pe_open and not self.hard_stopped:
                    if not trade_allowed:
                        print(f"[RE_ENTRY_BOTH_BLOCKED] Kill-switch flag=FALSE — crossover detected but both-leg re-entry suppressed")
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

    # ---------------------------------------------------------------- EOD / kill-switch close
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
        print(f"[{reason}] {ts()} ce_realized={self.ce_realized:.2f} "
              f"pe_realized={self.pe_realized:.2f} total={total:.2f}")


# ============================================================
# MAIN
# ============================================================
def main():
    print("[INIT] Starting VWAP Straddle v2 (Railway) — crossover entry + per-leg exit …")

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
    last_min      = None       # tracks which minute we last processed

    # Kill-switch state — tracked across loop iterations so we don't
    # repeatedly attempt to close already-closed positions.
    kill_positions_closed = False   # True once we've successfully closed all legs due to flag=False

    # How many seconds past the minute boundary to wait before sampling.
    # This ensures we read the settled candle-close LTP, not a spike
    # that happened right at the boundary and already reversed.
    CANDLE_CLOSE_DELAY_SECS = int(os.getenv("CANDLE_CLOSE_DELAY_SECS", "5"))

    # ============================================================
    # MAIN LOOP
    # ============================================================
    while now_ist().time() < EXIT_CUTOFF:
        now      = now_ist()
        curr_min = now.minute
        curr_sec = now.second

        # ---- KILL SWITCH ----
        # Check DB flag every tick (cheap read).
        # Behaviour:
        #   flag=False + open positions  -> close all open legs immediately (retry until done)
        #   flag=False + no open positions -> block all new entries, keep looping + logging
        #   flag=True  -> normal operation; if it was previously False, entries can resume
        trade_allowed = is_kill_switch_on()

        if not trade_allowed:
            # --- Close any open positions (only attempt if not already done) ---
            if active is not None and not kill_positions_closed:
                any_open = active.ce_open or active.pe_open
                if any_open:
                    print(f"[KILL SWITCH] flag=FALSE — closing all open legs …")
                    try:
                        active.close_all_now(phase, reason="FLAG_EXIT")
                        # Check if all legs are actually closed now
                        if not active.ce_open and not active.pe_open:
                            kill_positions_closed = True
                            print("[KILL SWITCH] All positions closed successfully.")
                            log_trade("FLAG_EXIT", phase, active_tag or "NA", active_expiry, atm_locked,
                                      status="FORCED_EXIT",
                                      note="All legs closed — kill-switch flag=FALSE")
                        else:
                            # Partial failure — will retry next tick
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
                    # No open positions — just mark as done
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

            # --- Skip all trading logic this tick ---
            time.sleep(1)
            continue

        # flag is True here — reset closure tracker so re-entry can happen
        # if flag was temporarily toggled and positions were closed
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

        # ---- PRELOCK: update once per minute, after candle close delay ----
        if not did_lock:
            if last_min != curr_min and curr_sec >= CANDLE_CLOSE_DELAY_SECS:
                last_min = curr_min
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
            # Seed VWAP state from prelock so continuity is preserved
            active.cum_pv      = chosen_prelock.cum_pv
            active.cum_vol     = chosen_prelock.cum_vol
            active.last_ce_vol = chosen_prelock.last_ce_vol
            active.last_pe_vol = chosen_prelock.last_pe_vol

            prelock_objs = None   # free memory

            did_lock = True
            phase    = "locked"
            last_min = None

            note = (f"ATM_LOCK={atm_locked} curr_prem={ltp_curr:.2f} th={PREMIUM_SWITCH_TH:.2f} "
                    f"-> TRADE={active_tag} expiry={active_expiry} MODE={TRADE_MODE}")
            log_trade("ATM_LOCK", phase, active_tag, active_expiry, atm_locked,
                      action_price=ltp_curr, vwap_at_action=vwap_curr, note=note)
            print(f"[LOCK] {ts()} {note}")

        # ---- POST-LOCK: process once per minute, after candle close delay ----
        if did_lock and (last_min != curr_min) and curr_sec >= CANDLE_CLOSE_DELAY_SECS:
            last_min = curr_min

            straddle, vwap, ce_ltp, pe_ltp = active.on_minute_close(phase, trade_allowed=trade_allowed)

            if straddle is not None and vwap is not None:
                run_pnl   = active.pnl_running(ce_ltp, pe_ltp)
                total_pnl = active.pnl_total(ce_ltp, pe_ltp)

                log_vwap("VWAP_ROW", phase, spot,
                         expiry_tag=active_tag, expiry_date=active_expiry,
                         strike=atm_locked, ltp=straddle, vwap=vwap,
                         cum_pv=active.cum_pv, cum_vol=active.cum_vol,
                         ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                         note=f"state={active.entry_state} {active.status_str()} MODE={TRADE_MODE}")

                log_trade("HEARTBEAT", phase, active_tag, active_expiry, atm_locked,
                          action_price=straddle, vwap_at_action=vwap,
                          pnl_running=run_pnl,
                          total_pnl=total_pnl,
                          ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                          status=active.status_str(),
                          note=f"entries={active.entries_taken} MODE={TRADE_MODE}")

        time.sleep(1)

    # ---- EOD: close any remaining open legs ----
    if active is not None:
        print("[EOD] Closing all open legs …")
        active.close_all_now(phase, reason="EOD_CLOSE")

    print("[DONE] Trading day complete.")


# ============================================================
if __name__ == "__main__":
    main()
