#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
  RA4 Bullock Cart 17 — Full Strategy Engine v3 (Dhan API + WebSocket v2)
  ─────────────────────────────────────────────────────────────────────────
  Structure:  Bullock Cart = DE1 (Profit Cap) + DE2 (No Cap)
  Indices:    NIFTY + SENSEX (option chart-based entries)
  Entry:      9 EMA > 15 EMA on option chart + ADX(5,5) > 20
  Lots:       4-lot phased exit (+1, +7.5, +15, TSL trail)
  Logics:     Vikalp, EPEL, Exit+1, Vivek, Kuber, Hanuman Tail, Tiered Risk
  Data:       WebSocket v2 (binary ticker) → 1-min candle engine → indicators
  Token:      3-tier → Verify existing → Renew → Generate via TOTP

  v3 fixes:
    - Dhan API rate limiter (Order: 10/s,250/m,1000/h,7000/d | Data: 5/s)
    - Single candle single entry per INDEX (not per DE) — fixes 400+ trades
    - Max trades per day per index safety cap
    - SELL order on exit (was missing — positions never closed in live)
    - 429 backoff handling
    - Lot 4 safety gates (+1 / entry fallback)
    - Consecutive EPEL scratch pause
═══════════════════════════════════════════════════════════════════════════════
  .env:
    DHAN_CLIENT_ID=your_client_id
    DHAN_PIN=your_6_digit_pin
    DHAN_TOTP_SECRET=your_totp_secret
    DHAN_ACCESS_TOKEN=           (auto-written after first generate)
═══════════════════════════════════════════════════════════════════════════════
"""

import os
import sys
import time
import json
import math
import struct
import signal
import threading
import logging
from datetime import datetime, timedelta, timezone
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Tuple, Any
from enum import Enum
from copy import deepcopy

import requests
import pyotp
import websocket
from dotenv import load_dotenv, set_key
from pathlib import Path


# ═══════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"ra4_{datetime.now().strftime('%Y%m%d')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)
log = logging.getLogger("RA4")


# ═══════════════════════════════════════════════════════════════════════════
# ANSI COLORS
# ═══════════════════════════════════════════════════════════════════════════
RESET   = "\033[0m"
BOLD    = "\033[1m"
DIM     = "\033[2m"
GREEN   = "\033[92m"
RED     = "\033[91m"
YELLOW  = "\033[93m"
CYAN    = "\033[96m"
WHITE   = "\033[97m"
MAGENTA = "\033[95m"


# ═══════════════════════════════════════════════════════════════════════════
# DHAN API RATE LIMITER
# ═══════════════════════════════════════════════════════════════════════════

class DhanRateLimiter:
    """
    Enforces Dhan API rate limits with 80% safety margin:
      Order APIs:  10/sec → 8,  250/min → 200,  1000/hr → 900,  7000/day → 6500
      Data APIs:   5/sec → 4,  100000/day → 90000
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._order_ts: List[float] = []
        self._data_ts: List[float] = []

    def _count_in_window(self, timestamps: List[float], window_sec: float) -> int:
        cutoff = time.time() - window_sec
        return sum(1 for t in timestamps if t > cutoff)

    def _cleanup(self, timestamps: List[float], max_age: float = 86400):
        cutoff = time.time() - max_age
        while timestamps and timestamps[0] < cutoff:
            timestamps.pop(0)

    def wait_for_order_slot(self):
        while True:
            with self._lock:
                now = time.time()
                self._cleanup(self._order_ts)
                s1 = self._count_in_window(self._order_ts, 1.0)
                s60 = self._count_in_window(self._order_ts, 60.0)
                s3600 = self._count_in_window(self._order_ts, 3600.0)
                total = len(self._order_ts)
                if s1 < 8 and s60 < 200 and s3600 < 900 and total < 6500:
                    self._order_ts.append(now)
                    return
            time.sleep(0.15)

    def wait_for_data_slot(self):
        while True:
            with self._lock:
                now = time.time()
                self._cleanup(self._data_ts)
                s1 = self._count_in_window(self._data_ts, 1.0)
                total = len(self._data_ts)
                if s1 < 4 and total < 90000:
                    self._data_ts.append(now)
                    return
            time.sleep(0.25)

    def get_order_counts(self) -> dict:
        with self._lock:
            now = time.time()
            return {
                "per_sec": self._count_in_window(self._order_ts, 1.0),
                "per_min": self._count_in_window(self._order_ts, 60.0),
                "per_hr": self._count_in_window(self._order_ts, 3600.0),
                "per_day": len(self._order_ts),
            }

    def can_place_order(self) -> bool:
        """Quick check without waiting."""
        c = self.get_order_counts()
        return c["per_sec"] < 8 and c["per_min"] < 200 and c["per_hr"] < 900 and c["per_day"] < 6500


RATE_LIMITER = DhanRateLimiter()


# ═══════════════════════════════════════════════════════════════════════════
# CONFIG & TOKEN MANAGEMENT (3-Tier: Verify → Renew → Generate TOTP)
# ═══════════════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).parent if "__file__" in globals() else Path.cwd()
ENV_FILE = BASE_DIR / ".env"

# Ensure .env exists
if not ENV_FILE.exists():
    ENV_FILE.write_text(
        "DHAN_CLIENT_ID=\nDHAN_PIN=\nDHAN_TOTP_SECRET=\nDHAN_ACCESS_TOKEN=\n"
    )
load_dotenv(str(ENV_FILE), override=True)

DHAN_CLIENT_ID    = os.getenv("DHAN_CLIENT_ID", "").strip()
DHAN_PIN          = os.getenv("DHAN_PIN", "").strip()
DHAN_TOTP_SECRET  = os.getenv("DHAN_TOTP_SECRET", "").strip()
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "").strip()

# Only enforce when running standalone (not from GUI)
_STANDALONE = False

BASE_URL           = "https://api.dhan.co/v2"
AUTH_GENERATE_URL  = "https://auth.dhan.co/app/generateAccessToken"
AUTH_RENEW_URL     = "https://api.dhan.co/v2/RenewToken"
AUTH_VERIFY_URL    = "https://api.dhan.co/v2/profile"

# These will be set after token resolution
HEADERS: Dict[str, str] = {}
WS_URL: str = ""


class DhanTokenManager:
    """Three-tier token strategy:
       1. Verify existing token in .env → use it
       2. Try Renew on existing token → get new one
       3. Generate fresh via TOTP (auto-waits for clean 30s window)
    """

    def __init__(self):
        self.client_id    = DHAN_CLIENT_ID
        self.pin          = DHAN_PIN
        self.totp_secret  = DHAN_TOTP_SECRET
        self.existing_token = DHAN_ACCESS_TOKEN

    def verify(self, token: str) -> bool:
        if not token:
            return False
        try:
            h = {"access-token": token, "client-id": self.client_id}
            r = requests.get(AUTH_VERIFY_URL, headers=h, timeout=10)
            return r.status_code == 200
        except Exception:
            return False

    def renew(self, token: str) -> Optional[str]:
        try:
            h = {"access-token": token, "dhanClientId": self.client_id,
                 "Content-Type": "application/json"}
            r = requests.get(AUTH_RENEW_URL, headers=h, timeout=15)
            try:
                d = r.json()
            except Exception:
                d = {"errorMessage": r.text[:200]}
            if "accessToken" in d:
                log.info(f"✅ Token renewed (exp: {d.get('expiryTime', '?')})")
                return d["accessToken"]
            err = d.get("errorMessage") or d.get("message") or str(d)
            log.warning(f"Renew failed: {err}")
            return None
        except Exception as e:
            log.warning(f"Renew exception: {e}")
            return None

    def generate(self, max_retries: int = 3) -> Optional[str]:
        for attempt in range(max_retries):
            rem = 30 - (int(time.time()) % 30)
            if attempt > 0 or rem < 10:
                log.info(f"⏳ Waiting {rem + 1}s for fresh TOTP window...")
                time.sleep(rem + 1)
            totp = pyotp.TOTP(self.totp_secret).now()
            log.info(f"Attempt {attempt + 1}: TOTP={totp}")
            try:
                params = {"dhanClientId": self.client_id,
                          "pin": self.pin, "totp": totp}
                r = requests.post(AUTH_GENERATE_URL, params=params, timeout=15)
                try:
                    d = r.json()
                except Exception:
                    d = {"errorMessage": r.text[:200]}
                if "accessToken" in d:
                    log.info(f"✅ Token generated (exp: {d.get('tokenExpiry', '?')})")
                    return d["accessToken"]
                err = str(d.get("errorMessage") or d.get("message") or d.get("remarks") or d)
                log.warning(f"Generate attempt {attempt + 1} failed: {err}")
                if "totp" in err.lower() or "invalid" in err.lower():
                    continue
                return None
            except Exception as e:
                log.warning(f"Generate exception (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return None
        return None

    def ensure_token(self) -> str:
        existing = self.existing_token
        if existing:
            log.info("Found existing token in .env — verifying...")
            if self.verify(existing):
                log.info("✅ Existing token still valid")
                return existing
            log.info("Existing token invalid — trying Renew...")
            renewed = self.renew(existing)
            if renewed:
                self._save_token(renewed)
                return renewed
            log.info("Renew failed — falling back to Generate via TOTP")
        else:
            log.info("No existing token — using Generate via TOTP")

        new_token = self.generate()
        if not new_token:
            log.error("❌ Could not obtain Dhan token by any method. Exiting.")
            sys.exit(1)
        self._save_token(new_token)
        return new_token

    def _save_token(self, token: str):
        try:
            set_key(str(ENV_FILE), "DHAN_ACCESS_TOKEN", token)
            log.info("Token saved to .env")
        except Exception as e:
            log.warning(f"Could not save token to .env: {e}")


def set_credentials(client_id: str, pin: str, totp_secret: str, token: str = ""):
    """Set credentials from GUI (instead of .env)."""
    global DHAN_CLIENT_ID, DHAN_PIN, DHAN_TOTP_SECRET, DHAN_ACCESS_TOKEN
    DHAN_CLIENT_ID = client_id
    DHAN_PIN = pin
    DHAN_TOTP_SECRET = totp_secret
    DHAN_ACCESS_TOKEN = token


def init_credentials(status_cb=None):
    """Resolve token and set global HEADERS + WS_URL."""
    global HEADERS, WS_URL, DHAN_ACCESS_TOKEN

    log.info(f"\n{BOLD}🔐 Authenticating with Dhan...{RESET}")
    if status_cb: status_cb("Authenticating with Dhan...")
    mgr = DhanTokenManager()
    token = mgr.ensure_token()
    DHAN_ACCESS_TOKEN = token

    HEADERS.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
        "access-token": token,
        "client-id": DHAN_CLIENT_ID,
    })
    WS_URL = (
        f"wss://api-feed.dhan.co?version=2"
        f"&token={token}&clientId={DHAN_CLIENT_ID}&authType=2"
    )
    log.info("Credentials initialized. WS URL ready.")


# ═══════════════════════════════════════════════════════════════════════════
# ENUMS & CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════

class Side(Enum):
    CE = "CE"
    PE = "PE"

class DEMode(Enum):
    PROFIT_CAP = "DE1"    # Fixed daily target
    NO_CAP     = "DE2"    # No cap, profit lock + safety line

# Dhan WebSocket v2 constants
REQ_SUB_TICKER  = 15
RESP_TICKER     = 2
RESP_PREV_CLOSE = 6
RESP_DISCONNECT = 50

EXCH_SEG_MAP = {
    0: "IDX_I", 1: "NSE_EQ", 2: "NSE_FNO", 3: "NSE_CURRENCY",
    4: "BSE_EQ", 5: "MCX_COMM", 7: "BSE_CURRENCY", 8: "BSE_FNO"
}

# Index configurations
INDEX_CONFIG = {
    "NIFTY": {
        "security_id": "13",        # IDX_I security_id for spot
        "segment": "NSE_FNO",
        "exchange": "NSE",
        "strike_gap": 50,
        "lot_size": 75,
        "option_chain_symbol": "NIFTY",
        "expiry_offset": 1,          # Current week + 1
    },
    "SENSEX": {
        "security_id": "51",         # IDX_I security_id for SENSEX spot
        "segment": "BSE_FNO",
        "exchange": "BSE",
        "strike_gap": 100,
        "lot_size": 20,
        "option_chain_symbol": "SENSEX",
        "expiry_offset": 1,
    }
}

# Candle interval for strategy (1-minute)
CANDLE_INTERVAL_SEC = 60


# ═══════════════════════════════════════════════════════════════════════════
# TIME HELPERS
# ═══════════════════════════════════════════════════════════════════════════

IST = timezone(timedelta(hours=5, minutes=30))

def _normalize_dhan_epoch(ts: int) -> int:
    """Dhan sometimes sends IST epoch; normalize to UTC epoch."""
    ts = int(ts)
    now_ts = int(time.time())
    diff = ts - now_ts
    if int(4.5 * 3600) <= diff <= int(6.5 * 3600):
        ts -= 19800
    return ts

def epoch_to_ist_str(ts: Optional[int], fmt="%H:%M:%S") -> str:
    if not ts:
        return "-"
    ts = _normalize_dhan_epoch(int(ts))
    dt = datetime.fromtimestamp(ts, tz=IST)
    return dt.strftime(fmt)

def candle_bucket(epoch_sec: int) -> int:
    """Floor epoch to candle boundary."""
    epoch_sec = _normalize_dhan_epoch(int(epoch_sec))
    return epoch_sec - (epoch_sec % CANDLE_INTERVAL_SEC)

def now_ist() -> datetime:
    return datetime.now(IST)


# ═══════════════════════════════════════════════════════════════════════════
# DHAN REST API HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def api_post(endpoint: str, payload: dict, retries: int = 2,
             is_order: bool = False) -> Optional[dict]:
    """POST with Dhan rate limiting. is_order=True for /orders endpoint."""
    url = f"{BASE_URL}{endpoint}"

    # Wait for rate limit slot
    if is_order:
        RATE_LIMITER.wait_for_order_slot()
    else:
        RATE_LIMITER.wait_for_data_slot()

    for attempt in range(retries + 1):
        try:
            r = requests.post(url, headers=HEADERS, json=payload, timeout=15)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                wait = 2 ** (attempt + 1)
                log.warning(f"API {endpoint} → 429 RATE LIMITED. Backoff {wait}s...")
                time.sleep(wait)
                continue
            else:
                log.warning(f"API {endpoint} → {r.status_code}: {r.text[:200]}")
                if attempt < retries:
                    time.sleep(1)
        except Exception as e:
            log.error(f"API {endpoint} error: {e}")
            if attempt < retries:
                time.sleep(1)
    return None


def api_get(endpoint: str, retries: int = 2) -> Optional[dict]:
    url = f"{BASE_URL}{endpoint}"
    RATE_LIMITER.wait_for_data_slot()
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                wait = 2 ** (attempt + 1)
                log.warning(f"API GET {endpoint} → 429 RATE LIMITED. Backoff {wait}s...")
                time.sleep(wait)
                continue
            else:
                log.warning(f"API GET {endpoint} → {r.status_code}: {r.text[:200]}")
                if attempt < retries:
                    time.sleep(1)
        except Exception as e:
            log.error(f"API GET {endpoint} error: {e}")
            if attempt < retries:
                time.sleep(1)
    return None


def fetch_expiry_list(index_name: str) -> List[str]:
    """Get list of expiry dates for the index options."""
    cfg = INDEX_CONFIG[index_name]
    payload = {
        "UnderlyingScrip": int(cfg["security_id"]),
        "UnderlyingSeg": "IDX_I",
    }
    resp = api_post("/optionchain/expirylist", payload)
    if resp and resp.get("status") == "success":
        return resp.get("data", [])
    return []


def get_target_expiry(index_name: str) -> Optional[str]:
    """Find current week + offset expiry."""
    expiries = fetch_expiry_list(index_name)
    if not expiries:
        log.error(f"No expiries found for {index_name}")
        return None

    today = now_ist().date()
    offset = INDEX_CONFIG[index_name]["expiry_offset"]

    expiry_dates = []
    for exp_str in expiries:
        try:
            d = datetime.strptime(exp_str, "%Y-%m-%d").date()
            if d >= today:
                expiry_dates.append((d, exp_str))
        except ValueError:
            continue

    expiry_dates.sort(key=lambda x: x[0])

    # offset=0 → nearest, offset=1 → next week, etc.
    if len(expiry_dates) > offset:
        return expiry_dates[offset][1]
    elif expiry_dates:
        return expiry_dates[-1][1]
    return None


def fetch_option_chain(index_name: str, expiry: str) -> Optional[dict]:
    """Fetch full option chain. Returns {spot_price, oc: {strike: {ce:{...}, pe:{...}}}}"""
    cfg = INDEX_CONFIG[index_name]
    payload = {
        "UnderlyingScrip": int(cfg["security_id"]),
        "UnderlyingSeg": "IDX_I",
        "Expiry": expiry,
    }
    resp = api_post("/optionchain", payload)
    if resp and resp.get("status") == "success":
        data = resp["data"]
        return {
            "spot_price": float(data["last_price"]),
            "oc": data["oc"],
        }
    return None


def fetch_historical_candles(security_id: str, segment: str,
                              interval: str = "1", days_back: int = 5) -> List[dict]:
    """
    Fetch intraday candles from Dhan v2.
    interval: "1" for 1-min, "5" for 5-min, etc.
    """
    to_date = now_ist().strftime("%Y-%m-%d")
    from_date = (now_ist() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    payload = {
        "securityId": str(security_id),
        "exchangeSegment": segment,
        "instrument": "OPTIDX",
        "interval": interval,
        "fromDate": from_date,
        "toDate": to_date,
    }
    resp = api_post("/charts/intraday", payload)
    if not resp or "open" not in resp or not resp["open"]:
        return []

    candles = []
    for i in range(len(resp["open"])):
        candles.append({
            "timestamp": resp["timestamp"][i] if "timestamp" in resp else i,
            "open": float(resp["open"][i]),
            "high": float(resp["high"][i]),
            "low": float(resp["low"][i]),
            "close": float(resp["close"][i]),
            "volume": int(resp["volume"][i]) if "volume" in resp else 0,
        })
    return candles


def place_order(security_id: str, segment: str, side: str,
                qty: int, price: float, order_type: str = "LIMIT",
                validity: str = "IOC", product: str = "MIS") -> dict:
    """Place order via Dhan REST API (rate-limited as Order API)."""
    payload = {
        "transactionType": side,       # BUY / SELL
        "exchangeSegment": segment,
        "productType": product,
        "orderType": order_type,
        "validity": validity,
        "securityId": str(security_id),
        "quantity": qty,
        "price": price,
        "dhanClientId": DHAN_CLIENT_ID,
    }
    result = api_post("/orders", payload, is_order=True)
    return result or {}


# ═══════════════════════════════════════════════════════════════════════════
# DHAN WS BINARY PARSERS
# ═══════════════════════════════════════════════════════════════════════════

def parse_header_8(msg: bytes) -> Optional[dict]:
    if len(msg) < 8:
        return None
    resp_code = msg[0]
    exch_seg_num = msg[3]
    sec_id_i = struct.unpack_from("<I", msg, 4)[0]
    return {
        "resp_code": resp_code,
        "exch_seg_name": EXCH_SEG_MAP.get(exch_seg_num, str(exch_seg_num)),
        "security_id": str(sec_id_i),
        "payload": msg[8:],
    }


def parse_ticker(payload: bytes) -> Optional[dict]:
    if len(payload) < 8:
        return None
    ltp = struct.unpack_from("<f", payload, 0)[0]
    ltt = struct.unpack_from("<I", payload, 4)[0]
    return {"ltp": float(ltp), "ltt_epoch": int(ltt)}


# ═══════════════════════════════════════════════════════════════════════════
# 1-MIN CANDLE ENGINE (from WebSocket ticks)
# ═══════════════════════════════════════════════════════════════════════════

class CandleEngine:
    """Builds 1-min candles from tick data. Fires callback on candle close."""

    def __init__(self, sec_id: str, label: str, interval_sec: int = 60,
                 on_candle_close=None):
        self.sec_id = sec_id
        self.label = label
        self.interval_sec = interval_sec
        self.on_candle_close = on_candle_close

        self.lock = threading.Lock()
        self.current: Optional[dict] = None
        self.last_ltp: Optional[float] = None
        self.last_ltt: Optional[int] = None
        self.tick_count: int = 0

    def on_tick(self, ltp: float, ltt_epoch: int):
        ltp = float(ltp)
        ltt_epoch = _normalize_dhan_epoch(int(ltt_epoch))
        bucket = ltt_epoch - (ltt_epoch % self.interval_sec)

        with self.lock:
            self.last_ltp = ltp
            self.last_ltt = ltt_epoch
            self.tick_count += 1

            if self.current is None:
                self.current = {
                    "bucket": bucket, "open": ltp, "high": ltp,
                    "low": ltp, "close": ltp, "ticks": 1,
                }
                return

            if bucket == self.current["bucket"]:
                self.current["high"] = max(self.current["high"], ltp)
                self.current["low"] = min(self.current["low"], ltp)
                self.current["close"] = ltp
                self.current["ticks"] += 1
                return

            if bucket > self.current["bucket"]:
                completed = dict(self.current)
                self.current = {
                    "bucket": bucket, "open": ltp, "high": ltp,
                    "low": ltp, "close": ltp, "ticks": 1,
                }

                if self.on_candle_close:
                    threading.Thread(
                        target=self.on_candle_close,
                        args=(self.sec_id, completed),
                        daemon=True,
                    ).start()

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "label": self.label,
                "ltp": self.last_ltp,
                "ltt": self.last_ltt,
                "current": dict(self.current) if self.current else None,
                "tick_count": self.tick_count,
            }


# ═══════════════════════════════════════════════════════════════════════════
# INDICATOR ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class IndicatorEngine:
    """
    Maintains EMA(9), EMA(15), ADX(5,5), ATR(14) on 1-min option candles.
    Seeds from historical, updates incrementally from live candle closes.
    Uses Pine Script-matching EMA: bar0 = source, bar1+ = source*k + prev*(1-k)
    """

    def __init__(self, ema_fast: int = 9, ema_slow: int = 15,
                 adx_period: int = 5, adx_smoothing: int = 5,
                 atr_period: int = 14):
        self.ema_fast_period = ema_fast
        self.ema_slow_period = ema_slow
        self.adx_period = adx_period
        self.adx_smoothing = adx_smoothing
        self.atr_period = atr_period

        self.k_fast = 2.0 / (ema_fast + 1.0)
        self.k_slow = 2.0 / (ema_slow + 1.0)

        # EMA state (on close)
        self.ema_fast_val: Optional[float] = None
        self.ema_slow_val: Optional[float] = None

        # ADX state
        self.prev_high: Optional[float] = None
        self.prev_low: Optional[float] = None
        self.prev_close: Optional[float] = None
        self.atr_rma: Optional[float] = None
        self.plus_dm_rma: Optional[float] = None
        self.minus_dm_rma: Optional[float] = None
        self.adx_rma: Optional[float] = None

        # ATR state (separate for SL)
        self.sl_atr_rma: Optional[float] = None

        self.candle_count: int = 0
        self.candles: deque = deque(maxlen=300)

    def seed_from_historical(self, candles: List[dict]):
        """Initialize from historical candle list."""
        self.candle_count = 0
        self.ema_fast_val = None
        self.ema_slow_val = None
        self.prev_high = None
        self.prev_low = None
        self.prev_close = None
        self.atr_rma = None
        self.plus_dm_rma = None
        self.minus_dm_rma = None
        self.adx_rma = None
        self.sl_atr_rma = None
        self.candles.clear()

        for c in candles:
            self._process_candle(c["open"], c["high"], c["low"], c["close"],
                                  c.get("timestamp", 0))

    def update_candle(self, o: float, h: float, l: float, c: float, ts: int = 0):
        """Process a new completed candle."""
        self._process_candle(o, h, l, c, ts)

    def _process_candle(self, o: float, h: float, l: float, c: float, ts: int = 0):
        self.candle_count += 1

        # ── EMA on close ──
        if self.ema_fast_val is None:
            self.ema_fast_val = c
            self.ema_slow_val = c
        else:
            self.ema_fast_val = c * self.k_fast + self.ema_fast_val * (1.0 - self.k_fast)
            self.ema_slow_val = c * self.k_slow + self.ema_slow_val * (1.0 - self.k_slow)

        # ── ADX ──
        if self.prev_high is not None:
            tr = max(h - l,
                     abs(h - self.prev_close),
                     abs(l - self.prev_close))

            up_move = h - self.prev_high
            dn_move = self.prev_low - l
            plus_dm = up_move if (up_move > dn_move and up_move > 0) else 0.0
            minus_dm = dn_move if (dn_move > up_move and dn_move > 0) else 0.0

            # RMA for ATR, +DM, -DM
            p = self.adx_period
            if self.atr_rma is None:
                self.atr_rma = tr
                self.plus_dm_rma = plus_dm
                self.minus_dm_rma = minus_dm
            else:
                self.atr_rma = (self.atr_rma * (p - 1) + tr) / p
                self.plus_dm_rma = (self.plus_dm_rma * (p - 1) + plus_dm) / p
                self.minus_dm_rma = (self.minus_dm_rma * (p - 1) + minus_dm) / p

            # DI+ / DI- / DX
            if self.atr_rma and self.atr_rma > 0:
                plus_di = 100.0 * self.plus_dm_rma / self.atr_rma
                minus_di = 100.0 * self.minus_dm_rma / self.atr_rma
                di_sum = plus_di + minus_di
                dx = 100.0 * abs(plus_di - minus_di) / di_sum if di_sum > 0 else 0.0
            else:
                dx = 0.0

            # ADX = RMA of DX
            s = self.adx_smoothing
            if self.adx_rma is None:
                self.adx_rma = dx
            else:
                self.adx_rma = (self.adx_rma * (s - 1) + dx) / s

            # SL ATR (separate period)
            ap = self.atr_period
            if self.sl_atr_rma is None:
                self.sl_atr_rma = tr
            else:
                self.sl_atr_rma = (self.sl_atr_rma * (ap - 1) + tr) / ap

        self.prev_high = h
        self.prev_low = l
        self.prev_close = c

        self.candles.append({
            "ts": ts, "o": o, "h": h, "l": l, "c": c,
            "ema_fast": self.ema_fast_val,
            "ema_slow": self.ema_slow_val,
            "adx": self.adx_rma or 0.0,
            "atr": self.sl_atr_rma or 0.0,
        })

    def is_ready(self) -> bool:
        return (self.candle_count >= self.ema_slow_period
                and self.ema_fast_val is not None
                and self.adx_rma is not None)

    def get_entry_signal(self, adx_threshold: float = 20.0) -> bool:
        """True if 9EMA > 15EMA + ADX > threshold."""
        if not self.is_ready():
            return False
        return (self.ema_fast_val > self.ema_slow_val
                and (self.adx_rma or 0) > adx_threshold)

    def get_values(self) -> dict:
        return {
            "ema_fast": round(self.ema_fast_val, 2) if self.ema_fast_val else None,
            "ema_slow": round(self.ema_slow_val, 2) if self.ema_slow_val else None,
            "adx": round(self.adx_rma, 2) if self.adx_rma else None,
            "atr": round(self.sl_atr_rma, 2) if self.sl_atr_rma else None,
            "candle_count": self.candle_count,
            "ready": self.is_ready(),
        }


# ═══════════════════════════════════════════════════════════════════════════
# STRATEGY CONFIGURATION (mirrors Web UI settings)
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class LotConfig:
    lot_number: int
    target_points: Optional[float]   # None = no fixed target (trail only)
    tsl_points: Optional[float]      # Trailing SL distance

@dataclass
class StrategyConfig:
    # ── Deployment ──
    paper_mode: bool = True

    # ── Indicators ──
    ema_fast: int = 9
    ema_slow: int = 15
    adx_period: int = 5
    adx_smoothing: int = 5
    adx_threshold: float = 20.0
    atr_period: int = 14
    atr_multiplier: float = 1.5

    # ── Strike Selection ──
    target_premium: float = 50.0
    premium_range_low: float = 40.0
    premium_range_high: float = 65.0
    max_strike_scan: int = 5

    # ── Lot Configuration ──
    num_lots: int = 4
    multiplier: int = 1
    multiplier_enabled: bool = False
    lot_configs: List[LotConfig] = field(default_factory=lambda: [
        LotConfig(lot_number=1, target_points=1.0,  tsl_points=None),
        LotConfig(lot_number=2, target_points=7.5,  tsl_points=3.0),
        LotConfig(lot_number=3, target_points=15.0, tsl_points=3.0),
        LotConfig(lot_number=4, target_points=None,  tsl_points=3.0),
    ])

    # ── DE1 ──
    de1_day_target_per_index: float = 6000.0
    de1_day_sl_per_index: float = 1500.0

    # ── DE2 ──
    de2_day_sl_per_index: float = 1500.0
    de2_safety_line: float = 2000.0
    de2_profit_lock_milestones: List[float] = field(default_factory=lambda: [
        6000, 10000, 15000, 20000, 30000
    ])

    # ── Logic Toggles ──
    logic_vikalp_enabled: bool = True
    logic_epel_enabled: bool = True
    logic_exit_plus1_enabled: bool = True
    logic_vivek_enabled: bool = True
    logic_kuber_enabled: bool = True
    logic_hanuman_tail_enabled: bool = True
    logic_tiered_risk_enabled: bool = True

    # ── Kuber ──
    kuber_lock_pct: float = 0.90
    kuber_buffer_pct: float = 0.10

    # ── Hanuman Tail ──
    hanuman_lock_offset: float = 1000.0
    hanuman_buffer: float = 1000.0

    # ── Tiered Risk ──
    tiered_risk_tiers: List[Dict] = field(default_factory=lambda: [
        {"profit": 12000, "lock": 11000, "buffer": 1000},
        {"profit": 20000, "lock": 18000, "buffer": 2000},
        {"profit": 30000, "lock": 27000, "buffer": 3000},
    ])

    # ── Timing ──
    trade_start_time: str = "09:16"
    trade_end_time: str = "15:15"
    cooldown_minutes: int = 5
    single_candle_single_entry: bool = True

    # ── Execution ──
    max_sl_per_trade: float = 25.0
    candle_interval_sec: int = 60

    # ── Rate Limit Safety ──
    max_trades_per_day_per_index: int = 30       # Safety cap per index


# ═══════════════════════════════════════════════════════════════════════════
# TRADE / LOT STATE
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class LotState:
    lot_number: int
    qty: int
    entry_price: float
    target_points: Optional[float]
    tsl_points: Optional[float]
    is_active: bool = True
    highest_since_entry: float = 0.0
    tsl_active: bool = False
    tsl_trigger_price: float = 0.0
    exit_price: float = 0.0
    exit_reason: str = ""

@dataclass
class ActiveTrade:
    trade_id: str
    index: str
    de_mode: str
    side: Side
    security_id: str
    strike: float
    entry_price: float
    entry_time: str
    lots: List[LotState] = field(default_factory=list)
    is_open: bool = True
    total_pnl: float = 0.0
    epel_gate_active: bool = True
    current_ltp: float = 0.0

@dataclass
class ProfitTracker:
    de_mode: str
    index: str
    total_pnl: float = 0.0
    trade_count: int = 0
    is_stopped: bool = False
    cooldown_until: Optional[datetime] = None
    last_trade_side: Optional[Side] = None
    last_candle_bucket: Optional[int] = None

    # DE2 Profit Lock
    current_lock: float = 0.0
    safety_line_remaining: float = 0.0

    # Kuber
    kuber_lock: float = 0.0
    kuber_buffer: float = 0.0
    kuber_active: bool = False

    # Hanuman Tail
    hanuman_active: bool = False
    hanuman_lock: float = 0.0
    hanuman_trail_high: float = 0.0

    # Tiered Risk
    current_tier_index: int = -1


# ═══════════════════════════════════════════════════════════════════════════
# MAIN STRATEGY ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class BullockCart17Engine:
    """
    Core engine: DE1+DE2 × NIFTY+SENSEX.
    WebSocket v2 for live ticks → CandleEngine → IndicatorEngine → Signals.
    """

    def __init__(self, config: StrategyConfig = None, gui_callback=None):
        self.config = config or StrategyConfig()
        self.gui_cb = gui_callback or (lambda *a: None)
        self.stop_event = threading.Event()

        # ── Per-index state ──
        self.expiry: Dict[str, str] = {}
        self.spot_price: Dict[str, float] = {}

        # ── Option info: key = f"{DE}_{INDEX}_{SIDE}" ──
        self.option_info: Dict[str, dict] = {}    # strike info dicts
        self.indicators: Dict[str, IndicatorEngine] = {}  # sec_id → IndicatorEngine
        self.indicator_labels: Dict[str, str] = {}

        # ── Candle engines (sec_id → CandleEngine) ──
        self.candle_engines: Dict[str, CandleEngine] = {}
        self.spot_engines: Dict[str, CandleEngine] = {}

        # ── WebSocket ──
        self.ws: Optional[websocket.WebSocketApp] = None
        self.ws_connected = threading.Event()
        self.ws_instruments: List[dict] = []

        # ── Profit trackers ──
        self.trackers: Dict[str, ProfitTracker] = {}
        for de in [DEMode.PROFIT_CAP, DEMode.NO_CAP]:
            for idx in ["NIFTY", "SENSEX"]:
                key = f"{de.value}_{idx}"
                self.trackers[key] = ProfitTracker(de_mode=de.value, index=idx)

        # ── Active / Closed trades ──
        self.trades_lock = threading.Lock()
        self.active_trades: Dict[str, ActiveTrade] = {}   # trade_id → Trade
        self.closed_trades: List[ActiveTrade] = []

        # ── Per-INDEX entry tracking (shared across DEs) ──
        # Spec: "One trade/candle/index" — NOT per DE
        self.index_last_entry_bucket: Dict[str, int] = {}   # "NIFTY" → last candle bucket
        self.index_trade_count: Dict[str, int] = {"NIFTY": 0, "SENSEX": 0}

        # ── Stats ──
        self.stats_lock = threading.Lock()
        self.packet_count = 0
        self.last_ws_error: Optional[str] = None

    def _emit(self, event: str, data: dict = None):
        """Thread-safe GUI callback."""
        try: self.gui_cb(event, data or {})
        except Exception: pass

    # ─────────────── INITIALIZATION ───────────────

    def initialize(self):
        """Fetch expiries, option chains, historical data, seed indicators."""
        log.info("=" * 70)
        log.info(f"  {BOLD}{CYAN}RA4 BULLOCK CART 17{RESET}")
        log.info(f"  Mode: {'PAPER' if self.config.paper_mode else 'LIVE'} | "
                 f"DE1 (Cap ₹{self.config.de1_day_target_per_index:.0f}/idx) + "
                 f"DE2 (No Cap, Lock+Safety)")
        log.info(f"  Logics: Vikalp={self.config.logic_vikalp_enabled} | "
                 f"EPEL={self.config.logic_epel_enabled} | "
                 f"Exit+1={self.config.logic_exit_plus1_enabled} | "
                 f"Vivek={self.config.logic_vivek_enabled} | "
                 f"Kuber={self.config.logic_kuber_enabled} | "
                 f"HanumanTail={self.config.logic_hanuman_tail_enabled} | "
                 f"TieredRisk={self.config.logic_tiered_risk_enabled}")
        log.info("=" * 70)

        for idx in ["NIFTY", "SENSEX"]:
            log.info(f"\n  [{idx}] Initializing...")
            self._emit("status", {"msg": f"Initializing {idx}..."})

            # 1. Expiry
            exp = get_target_expiry(idx)
            if not exp:
                log.error(f"  Cannot find expiry for {idx}. Skipping.")
                self._emit("error", {"msg": f"No expiry for {idx}"})
                continue
            self.expiry[idx] = exp
            log.info(f"  Expiry: {exp}")

            # 2. Option chain → find ₹50 premium strikes
            self._setup_index_strikes(idx)

        if not self.ws_instruments:
            log.error("No instruments to subscribe. Exiting.")
            sys.exit(1)

    def _setup_index_strikes(self, index: str):
        """For each DE × Side, find qualifying strikes and seed indicators."""
        exp = self.expiry.get(index)
        if not exp:
            return

        oc_data = fetch_option_chain(index, exp)
        if not oc_data:
            log.error(f"  Option chain fetch failed for {index}")
            return

        spot = oc_data["spot_price"]
        self.spot_price[index] = spot
        log.info(f"  Spot: {spot:.2f}")
        self._emit("spot", {"index": index, "spot": spot, "expiry": exp})

        cfg = self.config
        oc = oc_data["oc"]

        # Find strikes near ₹50 premium for CE and PE
        for side in [Side.CE, Side.PE]:
            candidates = self._find_premium_candidates(oc, side, cfg)
            if not candidates:
                log.warning(f"  No {side.value} strikes in ₹{cfg.premium_range_low}-{cfg.premium_range_high} range")
                continue

            # Take the nearest to ₹50 — we'll do Vikalp scanning at entry time
            best = candidates[0]
            sec_id = best["security_id"]
            strike = best["strike"]
            ltp = best["ltp"]

            label = f"{index} {int(strike)}{side.value}"
            log.info(f"  {GREEN if side == Side.CE else MAGENTA}"
                     f"  {side.value}: Strike={int(strike)} | SecID={sec_id} | LTP=₹{ltp:.2f}{RESET}")

            # Store for both DEs (they share the same option chain data)
            for de in [DEMode.PROFIT_CAP, DEMode.NO_CAP]:
                key = f"{de.value}_{index}_{side.value}"
                self.option_info[key] = {
                    "security_id": sec_id,
                    "strike": strike,
                    "ltp": ltp,
                    "side": side.value,
                    "index": index,
                    "segment": INDEX_CONFIG[index]["segment"],
                    "exchange_ws": INDEX_CONFIG[index]["segment"],
                }

            # Seed indicator (one per security_id, shared across DEs)
            if sec_id not in self.indicators:
                self._setup_indicator(sec_id, label, index)

    def _find_premium_candidates(self, oc: dict, side: Side,
                                  cfg: StrategyConfig) -> List[dict]:
        """Find option strikes within premium range, sorted by distance from target."""
        candidates = []
        opt_key = "ce" if side == Side.CE else "pe"

        for strike_key, strike_data in oc.items():
            if opt_key not in strike_data:
                continue
            opt = strike_data[opt_key]
            ltp = float(opt.get("last_price", 0))
            if cfg.premium_range_low <= ltp <= cfg.premium_range_high:
                try:
                    strike_val = float(strike_key)
                except ValueError:
                    continue
                candidates.append({
                    "security_id": str(opt["security_id"]),
                    "strike": strike_val,
                    "ltp": ltp,
                    "distance": abs(ltp - cfg.target_premium),
                })

        candidates.sort(key=lambda x: x["distance"])
        return candidates

    def _setup_indicator(self, sec_id: str, label: str, index: str):
        """Fetch historical candles, seed indicator, create candle engine."""
        cfg = self.config
        segment = INDEX_CONFIG[index]["segment"]

        log.info(f"    Fetching 1-min history for {label} (secId={sec_id})...")
        candles = fetch_historical_candles(sec_id, segment, interval="1", days_back=5)

        # Drop last candle if it's in current bucket (may be incomplete)
        if candles:
            now_epoch = int(time.time())
            current_bucket = candle_bucket(now_epoch)
            if candles and int(candles[-1].get("timestamp", 0)) >= current_bucket:
                candles = candles[:-1]
                log.info(f"    (dropped last incomplete candle)")

        indicator = IndicatorEngine(
            ema_fast=cfg.ema_fast,
            ema_slow=cfg.ema_slow,
            adx_period=cfg.adx_period,
            adx_smoothing=cfg.adx_smoothing,
            atr_period=cfg.atr_period,
        )

        if candles:
            indicator.seed_from_historical(candles)
            vals = indicator.get_values()
            log.info(f"    → {len(candles)} candles | EMA9={vals['ema_fast']} | "
                     f"EMA15={vals['ema_slow']} | ADX={vals['adx']} | "
                     f"ATR={vals['atr']} | Ready={vals['ready']}")
        else:
            log.warning(f"    → No historical data. Indicator will build from live ticks.")

        self.indicators[sec_id] = indicator
        self.indicator_labels[sec_id] = label

        # Create candle engine
        engine = CandleEngine(
            sec_id, label,
            interval_sec=cfg.candle_interval_sec,
            on_candle_close=self.on_candle_close,
        )
        self.candle_engines[sec_id] = engine

        # Add to WS subscription
        exchange_ws = INDEX_CONFIG[index]["segment"]
        self.ws_instruments.append({
            "name": label,
            "exchange": exchange_ws,
            "security_id": sec_id,
        })

    # ─────────────── CANDLE CLOSE CALLBACK ───────────────

    def on_candle_close(self, sec_id: str, candle: dict):
        """Fired when a 1-min candle completes. Run indicator + strategy logic."""
        indicator = self.indicators.get(sec_id)
        if not indicator:
            return

        o = float(candle["open"])
        h = float(candle["high"])
        l = float(candle["low"])
        c = float(candle["close"])
        ts = int(candle["bucket"])

        # Update indicator with completed candle
        indicator.update_candle(o, h, l, c, ts)

        if not indicator.is_ready():
            return

        vals = indicator.get_values()
        label = self.indicator_labels.get(sec_id, sec_id)
        time_str = epoch_to_ist_str(ts, "%H:%M")

        log.debug(f"[{time_str}] {label}: C={c:.2f} EMA9={vals['ema_fast']} "
                  f"EMA15={vals['ema_slow']} ADX={vals['adx']}")

        self._emit("indicator", {"sec_id": sec_id, "label": label, "ltp": c, **vals})

        # ── Check time window ──
        now = now_ist()
        start_t = datetime.strptime(self.config.trade_start_time, "%H:%M").time()
        end_t = datetime.strptime(self.config.trade_end_time, "%H:%M").time()
        if not (start_t <= now.time() <= end_t):
            return

        # ── Check for new entries across DEs ──
        has_signal = indicator.get_entry_signal(self.config.adx_threshold)

        if has_signal:
            # Find which index/side this sec_id belongs to
            for key, info in self.option_info.items():
                if info["security_id"] != sec_id:
                    continue

                parts = key.split("_")  # DE1_NIFTY_CE
                de_mode = parts[0]
                index = parts[1]
                side = Side(parts[2])

                # Check if entry allowed
                if not self._check_entry_allowed(de_mode, index, side, ts):
                    continue

                # Check if already in trade for this DE+Index+Side
                existing = [t for t in self.active_trades.values()
                            if t.de_mode == de_mode and t.index == index
                            and t.side == side and t.is_open]
                if existing:
                    continue

                self._execute_entry(
                    index=index, de_mode=de_mode, side=side,
                    security_id=sec_id, strike=info["strike"],
                    entry_price=c, candle_bucket=ts, atr=vals["atr"] or 0,
                )

    # ─────────────── ENTRY CHECKS ───────────────

    def _check_entry_allowed(self, de_mode: str, index: str, side: Side,
                              candle_bucket: int) -> bool:
        key = f"{de_mode}_{index}"
        tracker = self.trackers[key]
        cfg = self.config
        now = now_ist()

        if tracker.is_stopped:
            return False

        # ── Max trades per day per index (safety cap) ──
        if self.index_trade_count.get(index, 0) >= cfg.max_trades_per_day_per_index:
            log.info(f"[LIMIT] {index} hit max {cfg.max_trades_per_day_per_index} trades/day")
            return False

        # ── Single candle single entry: per INDEX (not per DE) ──
        # Spec Section 10: "One trade/candle/index"
        if cfg.single_candle_single_entry:
            if self.index_last_entry_bucket.get(index) == candle_bucket:
                return False

        # ── Cooldown: same direction only after SL ──
        if tracker.cooldown_until and now < tracker.cooldown_until:
            if tracker.last_trade_side == side:
                return False

        # ── DE1 target check ──
        if de_mode == DEMode.PROFIT_CAP.value:
            if tracker.total_pnl >= cfg.de1_day_target_per_index:
                if not (cfg.logic_hanuman_tail_enabled and tracker.hanuman_active):
                    tracker.is_stopped = True
                    log.info(f"[DE1] {index} day target ₹{tracker.total_pnl:.0f} reached")
                    return False

        # ── Day SL ──
        day_sl = (cfg.de1_day_sl_per_index if de_mode == DEMode.PROFIT_CAP.value
                  else cfg.de2_day_sl_per_index)
        if tracker.total_pnl <= -day_sl:
            tracker.is_stopped = True
            log.info(f"[{de_mode}] {index} day SL hit: ₹{tracker.total_pnl:.0f}")
            return False

        # ── DE2 profit lock breach ──
        if de_mode == DEMode.NO_CAP.value and tracker.current_lock > 0:
            if tracker.total_pnl <= tracker.current_lock:
                tracker.is_stopped = True
                log.info(f"[DE2] {index} profit dropped to lock ₹{tracker.current_lock:.0f}")
                return False

        # ── Order API budget check ──
        if not RATE_LIMITER.can_place_order():
            log.warning(f"[RATE] Order API budget exhausted. Skipping entry.")
            return False

        return True

    # ─────────────── EXECUTE ENTRY ───────────────

    def _execute_entry(self, index: str, de_mode: str, side: Side,
                        security_id: str, strike: float, entry_price: float,
                        candle_bucket: int, atr: float):
        cfg = self.config
        key = f"{de_mode}_{index}"
        tracker = self.trackers[key]

        lot_size = INDEX_CONFIG[index]["lot_size"]
        trade_id = f"{de_mode}_{index}_{side.value}_{now_ist().strftime('%H%M%S%f')}"

        # Build lots
        lots = []
        for lc in cfg.lot_configs:
            mult = cfg.multiplier if cfg.multiplier_enabled else 1
            lots.append(LotState(
                lot_number=lc.lot_number,
                qty=lot_size * mult,
                entry_price=entry_price,
                target_points=lc.target_points,
                tsl_points=lc.tsl_points,
                highest_since_entry=entry_price,
            ))

        trade = ActiveTrade(
            trade_id=trade_id,
            index=index,
            de_mode=de_mode,
            side=side,
            security_id=security_id,
            strike=strike,
            entry_price=entry_price,
            entry_time=now_ist().strftime("%H:%M:%S"),
            lots=lots,
            current_ltp=entry_price,
        )

        total_qty = sum(l.qty for l in lots)

        if cfg.paper_mode:
            log.info(f"\n  {'▓' * 60}")
            log.info(f"  {GREEN}{BOLD}  ▶ PAPER ENTRY — {trade_id}{RESET}")
            log.info(f"  {GREEN}  {index} {side.value} @ ₹{entry_price:.2f} | "
                     f"Strike: {int(strike)} | Qty: {total_qty} | {de_mode}{RESET}")
            log.info(f"  {GREEN}  ATR SL: ₹{atr:.2f} | Max SL: ₹{cfg.max_sl_per_trade}{RESET}")
            log.info(f"  {'▓' * 60}\n")
        else:
            result = place_order(
                security_id=security_id,
                segment=INDEX_CONFIG[index]["segment"],
                side="BUY",
                qty=total_qty,
                price=entry_price,
                order_type="LIMIT",
                validity="IOC",
            )
            log.info(f"[LIVE ENTRY] {trade_id} | Order: {result}")

        self._emit("entry", {"trade_id": trade_id, "index": index, "de": de_mode,
                              "side": side.value, "strike": int(strike), "price": entry_price,
                              "qty": total_qty, "paper": cfg.paper_mode})

        tracker.trade_count += 1
        tracker.last_trade_side = side
        tracker.last_candle_bucket = candle_bucket

        # Per-index tracking (shared across DEs) — enforces "one trade/candle/index"
        self.index_last_entry_bucket[index] = candle_bucket
        self.index_trade_count[index] = self.index_trade_count.get(index, 0) + 1

        with self.trades_lock:
            self.active_trades[trade_id] = trade

    # ─────────────── EXIT HELPERS ───────────────

    def _exit_lot(self, trade: ActiveTrade, lot: LotState, exit_price: float, reason: str):
        if not lot.is_active:
            return

        lot.is_active = False
        lot.exit_price = exit_price
        lot.exit_reason = reason

        pnl_points = exit_price - lot.entry_price
        pnl_value = pnl_points * lot.qty

        key = f"{trade.de_mode}_{trade.index}"
        tracker = self.trackers[key]
        tracker.total_pnl += pnl_value
        trade.total_pnl += pnl_value

        # ── PLACE SELL ORDER (was missing in v2!) ──
        if not self.config.paper_mode:
            result = place_order(
                security_id=trade.security_id,
                segment=INDEX_CONFIG[trade.index]["segment"],
                side="SELL",
                qty=lot.qty,
                price=exit_price,
                order_type="LIMIT",
                validity="IOC",
            )
            log.info(f"  [SELL] L{lot.lot_number} Qty={lot.qty} @ ₹{exit_price:.2f} → {result}")

        log.info(f"  [EXIT] {trade.trade_id} Lot{lot.lot_number} @ ₹{exit_price:.2f} | "
                 f"{reason} | PnL: ₹{pnl_value:+.0f} | Day: ₹{tracker.total_pnl:+.0f}")

        self._emit("lot_exit", {"trade_id": trade.trade_id, "lot": lot.lot_number,
                                 "price": exit_price, "reason": reason, "pnl": pnl_value,
                                 "day_pnl": tracker.total_pnl})

        # Check if trade fully closed
        if all(not l.is_active for l in trade.lots):
            trade.is_open = False
            log.info(f"  [TRADE CLOSED] {trade.trade_id} | Total: ₹{trade.total_pnl:+.0f}")
            self._emit("trade_closed", {"trade_id": trade.trade_id, "pnl": trade.total_pnl,
                                         "de": trade.de_mode, "index": trade.index})

            # Cooldown on loss
            if trade.total_pnl < 0:
                tracker.cooldown_until = now_ist() + timedelta(minutes=self.config.cooldown_minutes)
                tracker.last_trade_side = trade.side
                log.info(f"  [Cooldown] {key} {trade.side.value} until "
                         f"{tracker.cooldown_until.strftime('%H:%M:%S')}")

    def _exit_all_lots(self, trade: ActiveTrade, exit_price: float, reason: str):
        for lot in trade.lots:
            if lot.is_active:
                self._exit_lot(trade, lot, exit_price, reason)

    # ─────────────── LOGIC: EPEL ───────────────

    def _check_epel(self, trade: ActiveTrade, ltp: float) -> bool:
        if not self.config.logic_epel_enabled or not trade.epel_gate_active:
            return False

        entry = trade.entry_price
        lot1 = trade.lots[0]
        if not lot1.is_active:
            trade.epel_gate_active = False
            return False

        max_move = max(l.highest_since_entry for l in trade.lots if l.is_active)

        # Price barely moved and came back to entry
        if max_move < entry + 1.0 and ltp <= entry:
            log.info(f"  [EPEL] {trade.trade_id} — Scratch exit at ₹{entry:.2f}")
            self._exit_all_lots(trade, entry, "EPEL_SCRATCH")
            return True

        # Touched +1 then reversed to entry
        if max_move >= entry + 1.0 and ltp <= entry:
            log.info(f"  [EPEL] {trade.trade_id} — Gate2 exit at ₹{entry:.2f}")
            self._exit_all_lots(trade, entry, "EPEL_GATE2")
            return True

        return False

    # ─────────────── LOGIC: EXIT+1 ───────────────

    def _check_exit_plus1(self, trade: ActiveTrade, ltp: float) -> bool:
        if not self.config.logic_exit_plus1_enabled:
            return False

        lot1 = trade.lots[0]
        if lot1.is_active:
            return False  # +1 not yet hit

        entry = trade.entry_price
        plus1_price = entry + 1.0

        if ltp <= plus1_price:
            remaining = [l for l in trade.lots if l.is_active]
            if remaining:
                log.info(f"  [Exit+1] {trade.trade_id} — All lots exit at +1")
                for lot in remaining:
                    self._exit_lot(trade, lot, plus1_price, "EXIT_PLUS1")
                return True
        return False

    # ─────────────── LOGIC: VIVEK ───────────────

    def _check_vivek(self, trade: ActiveTrade, ltp: float) -> bool:
        if not self.config.logic_vivek_enabled:
            return False

        entry = trade.entry_price
        acted = False

        for lot in trade.lots:
            if not lot.is_active or lot.lot_number not in [2, 3]:
                continue

            max_move = lot.highest_since_entry - entry
            if max_move < 2.0:
                continue

            if not lot.tsl_active:
                half_exit = entry + (max_move / 2.0)
                if ltp <= half_exit:
                    log.info(f"  [Vivek] {trade.trade_id} Lot{lot.lot_number} — "
                             f"Half-move exit @ ₹{half_exit:.2f}")
                    self._exit_lot(trade, lot, half_exit, "VIVEK_HALF")
                    acted = True

        return acted

    # ─────────────── LOT MONITORING ───────────────

    def _monitor_lots(self, trade: ActiveTrade, ltp: float):
        entry = trade.entry_price
        cfg = self.config

        for lot in trade.lots:
            if not lot.is_active:
                continue

            if ltp > lot.highest_since_entry:
                lot.highest_since_entry = ltp

            move = ltp - entry

            # Fixed target
            if lot.target_points is not None and move >= lot.target_points:
                self._exit_lot(trade, lot, ltp, f"TARGET_+{lot.target_points}")
                continue

            # TSL
            if lot.tsl_points is not None:
                max_move = lot.highest_since_entry - entry
                if max_move >= lot.tsl_points and not lot.tsl_active:
                    lot.tsl_active = True
                    lot.tsl_trigger_price = lot.highest_since_entry - lot.tsl_points

                if lot.tsl_active:
                    new_tsl = lot.highest_since_entry - lot.tsl_points
                    if new_tsl > lot.tsl_trigger_price:
                        lot.tsl_trigger_price = new_tsl
                    if ltp <= lot.tsl_trigger_price:
                        self._exit_lot(trade, lot, lot.tsl_trigger_price, f"TSL_{lot.tsl_points}")
                        continue

            # Hard SL
            if move <= -cfg.max_sl_per_trade:
                self._exit_lot(trade, lot, ltp, f"HARD_SL_{cfg.max_sl_per_trade}")
                continue

            # ── Lot 4 safety gates (spec: "Exit at +1 or Entry point") ──
            if lot.lot_number == 4 and lot.tsl_points is not None and not lot.tsl_active:
                # If price drops back to entry+1 without TSL ever activating
                if lot.highest_since_entry >= entry + 1.0 and ltp <= entry + 1.0:
                    self._exit_lot(trade, lot, entry + 1.0, "LOT4_SAFETY_+1")
                    continue
                # If price drops to entry without any meaningful move
                if ltp <= entry and lot.highest_since_entry < entry + 1.0:
                    self._exit_lot(trade, lot, entry, "LOT4_SAFETY_ENTRY")
                    continue

    # ─────────────── PROFIT MANAGEMENT LOGICS ───────────────

    def _apply_kuber(self, de_mode: str, index: str):
        if not self.config.logic_kuber_enabled:
            return
        key = f"{de_mode}_{index}"
        tracker = self.trackers[key]
        pnl = tracker.total_pnl
        if pnl <= 0:
            tracker.kuber_active = False
            return

        lock_amt = pnl * self.config.kuber_lock_pct
        if lock_amt > tracker.kuber_lock:
            tracker.kuber_lock = lock_amt
            tracker.kuber_buffer = pnl * self.config.kuber_buffer_pct
            tracker.kuber_active = True
            log.info(f"  [Kuber] {key} — Lock ₹{lock_amt:.0f}")

        if tracker.kuber_active and pnl <= tracker.kuber_lock:
            log.info(f"  [Kuber] {key} — Breached lock, stopping")
            tracker.is_stopped = True

    def _apply_hanuman_tail(self, de_mode: str, index: str):
        if not self.config.logic_hanuman_tail_enabled:
            return
        if de_mode != DEMode.PROFIT_CAP.value:
            return

        key = f"{de_mode}_{index}"
        tracker = self.trackers[key]
        pnl = tracker.total_pnl

        if pnl < self.config.de1_day_target_per_index:
            return

        if not tracker.hanuman_active:
            tracker.hanuman_active = True
            tracker.hanuman_trail_high = pnl
            tracker.is_stopped = False
            log.info(f"  [Hanuman] {key} — Activated! Trail from ₹{pnl:.0f}")

        if pnl > tracker.hanuman_trail_high:
            tracker.hanuman_trail_high = pnl

        tracker.hanuman_lock = tracker.hanuman_trail_high - self.config.hanuman_lock_offset

        if pnl <= tracker.hanuman_lock:
            log.info(f"  [Hanuman] {key} — Lock breached ₹{tracker.hanuman_lock:.0f}, stopping")
            tracker.is_stopped = True

    def _apply_tiered_risk(self, de_mode: str, index: str):
        if not self.config.logic_tiered_risk_enabled:
            return
        key = f"{de_mode}_{index}"
        tracker = self.trackers[key]
        pnl = tracker.total_pnl
        tiers = self.config.tiered_risk_tiers

        for i in range(len(tiers) - 1, -1, -1):
            tier = tiers[i]
            if pnl >= tier["profit"] and i > tracker.current_tier_index:
                tracker.current_tier_index = i
                tracker.kuber_lock = tier["lock"]
                tracker.kuber_buffer = tier["buffer"]
                tracker.kuber_active = True
                log.info(f"  [Tiered] {key} — Tier {i+1}: Lock ₹{tier['lock']:.0f}")
                break

        if tracker.kuber_active and pnl <= tracker.kuber_lock:
            tracker.is_stopped = True

    def _apply_de2_profit_lock(self, index: str):
        key = f"{DEMode.NO_CAP.value}_{index}"
        tracker = self.trackers[key]
        pnl = tracker.total_pnl
        cfg = self.config

        if pnl <= 0:
            return

        for milestone in sorted(cfg.de2_profit_lock_milestones, reverse=True):
            if pnl >= milestone:
                new_lock = milestone - cfg.de2_safety_line
                if new_lock > tracker.current_lock:
                    tracker.current_lock = new_lock
                    tracker.safety_line_remaining = cfg.de2_safety_line
                    log.info(f"  [DE2 Lock] {index} — ₹{pnl:.0f} → Lock ₹{new_lock:.0f}")
                break

        if tracker.current_lock > 0 and pnl <= tracker.current_lock:
            tracker.is_stopped = True

    # ─────────────── TICK-LEVEL TRADE MONITOR ───────────────

    def _on_trade_tick(self, sec_id: str, ltp: float):
        """Called on every tick for active trades matching this sec_id."""
        with self.trades_lock:
            for trade_id, trade in list(self.active_trades.items()):
                if trade.security_id != sec_id or not trade.is_open:
                    continue

                trade.current_ltp = ltp

                # 1. EPEL
                if self._check_epel(trade, ltp):
                    continue

                # 2. Per-lot monitoring
                self._monitor_lots(trade, ltp)

                # 3. Exit+1
                if self._check_exit_plus1(trade, ltp):
                    continue

                # 4. Vivek
                self._check_vivek(trade, ltp)

                # 5. Profit management
                self._apply_kuber(trade.de_mode, trade.index)
                self._apply_hanuman_tail(trade.de_mode, trade.index)
                self._apply_tiered_risk(trade.de_mode, trade.index)
                if trade.de_mode == DEMode.NO_CAP.value:
                    self._apply_de2_profit_lock(trade.index)

            # Move closed trades
            closed_ids = [tid for tid, t in self.active_trades.items() if not t.is_open]
            for tid in closed_ids:
                self.closed_trades.append(self.active_trades.pop(tid))

    # ─────────────── WEBSOCKET ───────────────

    def on_ws_open(self, ws):
        self.ws_connected.set()

        # Subscribe index spots
        spot_instruments = []
        for idx in ["NIFTY", "SENSEX"]:
            cfg = INDEX_CONFIG[idx]
            spot_instruments.append({
                "ExchangeSegment": "IDX_I",
                "SecurityId": cfg["security_id"],
            })
            self.spot_engines[idx] = CandleEngine(
                cfg["security_id"], f"{idx} SPOT",
                interval_sec=CANDLE_INTERVAL_SEC,
            )

        # Subscribe option instruments
        opt_instruments = [
            {
                "ExchangeSegment": str(i["exchange"]),
                "SecurityId": str(i["security_id"]),
            }
            for i in self.ws_instruments
        ]

        all_instruments = spot_instruments + opt_instruments
        sub_msg = {
            "RequestCode": REQ_SUB_TICKER,
            "InstrumentCount": len(all_instruments),
            "InstrumentList": all_instruments,
        }
        ws.send(json.dumps(sub_msg))
        log.info(f"{GREEN}WebSocket connected — subscribed to {len(all_instruments)} instruments{RESET}")
        self._emit("ws_connected", {"instruments": len(all_instruments)})

    def on_ws_message(self, ws, message):
        if isinstance(message, str):
            return

        msg = bytes(message)
        hdr = parse_header_8(msg)
        if not hdr:
            return

        code = int(hdr["resp_code"])
        sec_id = str(hdr["security_id"])

        with self.stats_lock:
            self.packet_count += 1

        if code == RESP_TICKER:
            t = parse_ticker(hdr["payload"])
            if not t:
                return

            ltp = float(t["ltp"])
            ltt = int(t["ltt_epoch"])

            # Update spot price
            for idx in ["NIFTY", "SENSEX"]:
                if sec_id == INDEX_CONFIG[idx]["security_id"]:
                    self.spot_price[idx] = ltp
                    self._emit("spot_tick", {"index": idx, "ltp": ltp})

            # Update option candle engines
            if sec_id in self.candle_engines:
                self.candle_engines[sec_id].on_tick(ltp, ltt)

            # Tick-level trade monitoring (for fast exits)
            self._on_trade_tick(sec_id, ltp)

    def on_ws_error(self, ws, error):
        with self.stats_lock:
            self.last_ws_error = str(error)

    def on_ws_close(self, ws, status_code, msg):
        self.ws_connected.clear()
        log.warning(f"WebSocket closed: {status_code} {msg}")
        self._emit("ws_disconnected", {})

    def run_ws(self):
        websocket.enableTrace(False)
        while not self.stop_event.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    WS_URL,
                    on_open=self.on_ws_open,
                    on_message=self.on_ws_message,
                    on_error=self.on_ws_error,
                    on_close=self.on_ws_close,
                )
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                with self.stats_lock:
                    self.last_ws_error = f"WS exception: {e}"
            finally:
                if not self.stop_event.is_set():
                    log.info("WS reconnecting in 2s...")
                    time.sleep(2)

    # ─────────────── STATUS DISPLAY ───────────────

    def print_status(self):
        now = now_ist().strftime("%H:%M:%S")
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.flush()

        print(f"  {BOLD}{CYAN}RA4 BULLOCK CART 17{RESET}  │  {now}  │  "
              f"{'PAPER' if self.config.paper_mode else 'LIVE'}")
        print(f"{'═' * 90}")

        # Spot prices
        for idx in ["NIFTY", "SENSEX"]:
            spot = self.spot_price.get(idx, 0)
            exp = self.expiry.get(idx, "-")
            print(f"  {BOLD}{idx}{RESET}: Spot={spot:.2f}  Expiry={exp}")

        # Tracker summary
        print(f"\n  {BOLD}Daily P&L:{RESET}")
        total_pnl = 0
        for key, tracker in self.trackers.items():
            status = f"{RED}STOPPED{RESET}" if tracker.is_stopped else f"{GREEN}ACTIVE{RESET}"
            col = GREEN if tracker.total_pnl >= 0 else RED
            lock_info = ""
            if tracker.current_lock > 0:
                lock_info = f" | Lock=₹{tracker.current_lock:.0f}"
            if tracker.kuber_active:
                lock_info += f" | Kuber=₹{tracker.kuber_lock:.0f}"
            print(f"    {key:15s} | {col}₹{tracker.total_pnl:>+10,.0f}{RESET} | "
                  f"Trades: {tracker.trade_count} | {status}{lock_info}")
            total_pnl += tracker.total_pnl

        tcol = GREEN if total_pnl >= 0 else RED
        print(f"    {'─' * 60}")
        print(f"    {'TOTAL':15s} | {tcol}{BOLD}₹{total_pnl:>+10,.0f}{RESET}")

        # Option indicators
        print(f"\n  {BOLD}Indicators:{RESET}")
        print(f"    {'Option':25}  {'LTP':>8}  {'EMA9':>8}  {'EMA15':>8}  {'ADX':>6}  {'Ticks':>7}")
        print(f"    {'─' * 70}")
        for sec_id, engine in self.candle_engines.items():
            snap = engine.snapshot()
            ind = self.indicators.get(sec_id)
            vals = ind.get_values() if ind else {}

            ltp_s = f"{snap['ltp']:.2f}" if snap['ltp'] else "-"
            ef = f"{vals.get('ema_fast', '-')}" if vals.get('ema_fast') else "..."
            es = f"{vals.get('ema_slow', '-')}" if vals.get('ema_slow') else "..."
            adx_s = f"{vals.get('adx', '-')}" if vals.get('adx') else "..."

            print(f"    {snap['label']:25}  {ltp_s:>8}  {ef:>8}  {es:>8}  "
                  f"{adx_s:>6}  {snap['tick_count']:>7}")

        # Active trades
        with self.trades_lock:
            if self.active_trades:
                print(f"\n  {BOLD}Active Trades:{RESET}")
                for tid, t in self.active_trades.items():
                    ltp = t.current_ltp or t.entry_price
                    active_lots = [l for l in t.lots if l.is_active]
                    unrealized = sum((ltp - l.entry_price) * l.qty for l in active_lots)
                    col = GREEN if unrealized >= 0 else RED
                    print(f"    {t.de_mode} {t.index} {int(t.strike)}{t.side.value}: "
                          f"Entry=₹{t.entry_price:.2f} LTP=₹{ltp:.2f} "
                          f"Lots={len(active_lots)}/{len(t.lots)} "
                          f"{col}Unrealized=₹{unrealized:+.0f}{RESET}")

        # Recent closed trades
        if self.closed_trades:
            print(f"\n  {BOLD}Recent Trades:{RESET}")
            for t in self.closed_trades[-8:]:
                col = GREEN if t.total_pnl >= 0 else RED
                print(f"    {t.de_mode} {t.index} {int(t.strike)}{t.side.value}: "
                      f"₹{t.entry_price:.2f} → {col}₹{t.total_pnl:+.0f}{RESET} "
                      f"({', '.join(l.exit_reason for l in t.lots if l.exit_reason)})")

        with self.stats_lock:
            rl = RATE_LIMITER.get_order_counts()
            print(f"\n  {DIM}Packets: {self.packet_count} | "
                  f"Orders: {rl['per_min']}/min {rl['per_hr']}/hr {rl['per_day']}/day | "
                  f"WS Error: {self.last_ws_error or 'None'}{RESET}")
            idx_counts = " | ".join(f"{k}={v}" for k, v in self.index_trade_count.items())
            print(f"  {DIM}Trades/index: {idx_counts} | Max: {self.config.max_trades_per_day_per_index}{RESET}")
        print(f"  {DIM}Press Ctrl+C to stop{RESET}")

    # ─────────────── EOD ───────────────

    def _eod_exit_all(self):
        """Exit all open trades at EOD."""
        with self.trades_lock:
            for tid, trade in list(self.active_trades.items()):
                if trade.is_open:
                    ltp = trade.current_ltp or trade.entry_price
                    self._exit_all_lots(trade, ltp, "EOD_EXIT")

            closed_ids = [tid for tid, t in self.active_trades.items() if not t.is_open]
            for tid in closed_ids:
                self.closed_trades.append(self.active_trades.pop(tid))
        self._emit("eod", self.get_summary())

    def get_summary(self) -> dict:
        """Snapshot for GUI and logging."""
        total = 0; rows = []
        for key, t in self.trackers.items():
            rows.append({"key": key, "pnl": t.total_pnl, "trades": t.trade_count,
                          "stopped": t.is_stopped, "lock": t.current_lock,
                          "kuber": t.kuber_lock if t.kuber_active else 0})
            total += t.total_pnl
        return {"trackers": rows, "total_pnl": total,
                "closed_count": len(self.closed_trades),
                "index_trades": dict(self.index_trade_count),
                "rate_limits": RATE_LIMITER.get_order_counts()}

    def _print_day_summary(self):
        log.info("\n" + "=" * 60)
        log.info("DAY SUMMARY — RA4 BULLOCK CART 17")
        log.info("=" * 60)
        total = 0
        for key, tracker in self.trackers.items():
            log.info(f"  {key:15s} | PnL: ₹{tracker.total_pnl:>+10,.0f} | Trades: {tracker.trade_count}")
            total += tracker.total_pnl
        log.info("-" * 60)
        log.info(f"  {'TOTAL':15s} | PnL: ₹{total:>+10,.0f}")
        log.info("=" * 60)

    # ─────────────── MAIN RUN ───────────────

    def run(self):
        """Main entry point."""
        self.initialize()

        # Start WebSocket in background
        ws_thread = threading.Thread(target=self.run_ws, daemon=True)
        ws_thread.start()

        log.info(f"\n{BOLD}Waiting for WebSocket connection...{RESET}")
        self.ws_connected.wait(timeout=15)

        if not self.ws_connected.is_set():
            log.error("WebSocket failed to connect. Check credentials.")
            return

        log.info(f"{GREEN}Live! Monitoring for signals...{RESET}")
        time.sleep(2)

        # Main display loop
        while not self.stop_event.is_set():
            try:
                now = now_ist()
                end_t = datetime.strptime(self.config.trade_end_time, "%H:%M").time()

                # EOD check
                if now.time() > end_t:
                    eod_buffer = (datetime.combine(now.date(), end_t)
                                  + timedelta(minutes=2)).time()
                    if now.time() > eod_buffer:
                        log.info("🔔 Trade window closed.")
                        self._eod_exit_all()
                        self._print_day_summary()
                        break

                # Console display only when no GUI callback
                has_gui = self.gui_cb is not None and not (
                    hasattr(self.gui_cb, '__name__') and self.gui_cb.__name__ == '<lambda>')
                if not has_gui:
                    self.print_status()

                self._emit("tick_update", {
                    "spots": dict(self.spot_price),
                    "packets": self.packet_count,
                    "active_count": len(self.active_trades),
                    **self.get_summary(),
                })
                time.sleep(1)

            except KeyboardInterrupt:
                break
            except Exception as e:
                log.error(f"Main loop error: {e}")
                time.sleep(1)

    def stop(self):
        self.stop_event.set()
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def main():
    # ── Step 1: Authenticate ──
    if not DHAN_CLIENT_ID or not DHAN_PIN or not DHAN_TOTP_SECRET:
        raise SystemExit("Missing credentials in .env (DHAN_CLIENT_ID, DHAN_PIN, DHAN_TOTP_SECRET)")
    init_credentials()

    # ── Step 2: Build config ──
    config = StrategyConfig(
        paper_mode=True,

        # Indicators
        ema_fast=9,
        ema_slow=15,
        adx_period=5,
        adx_smoothing=5,
        adx_threshold=20.0,
        atr_period=14,

        # Strike selection
        target_premium=50.0,
        premium_range_low=40.0,
        premium_range_high=65.0,

        # Lots
        num_lots=4,
        multiplier_enabled=False,

        # DE1
        de1_day_target_per_index=6000.0,
        de1_day_sl_per_index=1500.0,

        # DE2
        de2_day_sl_per_index=1500.0,
        de2_safety_line=2000.0,

        # Logics — all ON
        logic_vikalp_enabled=True,
        logic_epel_enabled=True,
        logic_exit_plus1_enabled=True,
        logic_vivek_enabled=True,
        logic_kuber_enabled=True,
        logic_hanuman_tail_enabled=True,
        logic_tiered_risk_enabled=True,

        # Timing
        trade_start_time="09:16",
        trade_end_time="15:15",
        cooldown_minutes=5,

        # Execution
        max_sl_per_trade=25.0,
        candle_interval_sec=60,

        # Rate limit safety
        max_trades_per_day_per_index=30,
    )

    # ── Step 3: Run ──
    engine = BullockCart17Engine(config)

    def _sig_handler(sig, frame):
        log.info(f"\n{YELLOW}Shutting down...{RESET}")
        engine.stop()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    engine.run()


if __name__ == "__main__":
    main()
