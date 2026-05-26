"""
RA4 Bullock Cart 17 — Core Strategy Engine
═══════════════════════════════════════════
DE1 (Profit Cap) + DE2 (No Cap) × NIFTY + SENSEX
7 Smart Logics | 4-Lot Phased Exit | WebSocket v2
"""

import os
import sys
import time
import json
import struct
import threading
import logging
from datetime import datetime, timedelta, timezone
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Tuple, Callable
from enum import Enum

import requests
import pyotp
import websocket
from dotenv import load_dotenv, set_key
from pathlib import Path

log = logging.getLogger("RA4")

# ═══════════════════════════════════════════════════════════════════════════
# ANSI (for console fallback)
# ═══════════════════════════════════════════════════════════════════════════
RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"
GREEN = "\033[92m"; RED = "\033[91m"; YELLOW = "\033[93m"
CYAN = "\033[96m"; MAGENTA = "\033[95m"

# ═══════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════

class Side(Enum):
    CE = "CE"
    PE = "PE"

class DEMode(Enum):
    PROFIT_CAP = "DE1"
    NO_CAP     = "DE2"

BASE_URL           = "https://api.dhan.co/v2"
AUTH_GENERATE_URL  = "https://auth.dhan.co/app/generateAccessToken"
AUTH_RENEW_URL     = "https://api.dhan.co/v2/RenewToken"
AUTH_VERIFY_URL    = "https://api.dhan.co/v2/profile"

REQ_SUB_TICKER  = 15
RESP_TICKER     = 2
RESP_PREV_CLOSE = 6
RESP_DISCONNECT = 50

EXCH_SEG_MAP = {
    0: "IDX_I", 1: "NSE_EQ", 2: "NSE_FNO", 3: "NSE_CURRENCY",
    4: "BSE_EQ", 5: "MCX_COMM", 7: "BSE_CURRENCY", 8: "BSE_FNO"
}

INDEX_CONFIG = {
    "NIFTY": {
        "security_id": "13", "segment": "NSE_FNO", "exchange": "NSE",
        "strike_gap": 50, "lot_size": 75, "expiry_offset": 1,
    },
    "SENSEX": {
        "security_id": "51", "segment": "BSE_FNO", "exchange": "BSE",
        "strike_gap": 100, "lot_size": 20, "expiry_offset": 1,
    }
}

IST = timezone(timedelta(hours=5, minutes=30))
CANDLE_INTERVAL_SEC = 60


# ═══════════════════════════════════════════════════════════════════════════
# TIME HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _normalize_dhan_epoch(ts: int) -> int:
    ts = int(ts)
    now_ts = int(time.time())
    diff = ts - now_ts
    if int(4.5 * 3600) <= diff <= int(6.5 * 3600):
        ts -= 19800
    return ts

def epoch_to_ist_str(ts: Optional[int], fmt="%H:%M:%S") -> str:
    if not ts: return "-"
    ts = _normalize_dhan_epoch(int(ts))
    return datetime.fromtimestamp(ts, tz=IST).strftime(fmt)

def candle_bucket(epoch_sec: int) -> int:
    epoch_sec = _normalize_dhan_epoch(int(epoch_sec))
    return epoch_sec - (epoch_sec % CANDLE_INTERVAL_SEC)

def now_ist() -> datetime:
    return datetime.now(IST)


# ═══════════════════════════════════════════════════════════════════════════
# TOKEN MANAGER (3-Tier: Verify → Renew → Generate TOTP)
# ═══════════════════════════════════════════════════════════════════════════

HEADERS: Dict[str, str] = {}
WS_URL: str = ""

class DhanTokenManager:
    def __init__(self, client_id: str, pin: str, totp_secret: str,
                 existing_token: str = "", env_file: str = ".env"):
        self.client_id = client_id
        self.pin = pin
        self.totp_secret = totp_secret
        self.existing_token = existing_token
        self.env_file = env_file

    def verify(self, token: str) -> bool:
        if not token: return False
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
            try: d = r.json()
            except: d = {"errorMessage": r.text[:200]}
            if "accessToken" in d:
                log.info(f"✅ Token renewed (exp: {d.get('expiryTime', '?')})")
                return d["accessToken"]
            log.warning(f"Renew failed: {d.get('errorMessage') or d.get('message') or d}")
            return None
        except Exception as e:
            log.warning(f"Renew exception: {e}")
            return None

    def generate(self, max_retries: int = 3, status_cb: Callable = None) -> Optional[str]:
        for attempt in range(max_retries):
            rem = 30 - (int(time.time()) % 30)
            if attempt > 0 or rem < 10:
                if status_cb:
                    status_cb(f"Waiting {rem+1}s for fresh TOTP window...")
                time.sleep(rem + 1)
            totp = pyotp.TOTP(self.totp_secret).now()
            log.info(f"TOTP attempt {attempt+1}: {totp}")
            try:
                params = {"dhanClientId": self.client_id, "pin": self.pin, "totp": totp}
                r = requests.post(AUTH_GENERATE_URL, params=params, timeout=15)
                try: d = r.json()
                except: d = {"errorMessage": r.text[:200]}
                if "accessToken" in d:
                    log.info(f"✅ Token generated")
                    return d["accessToken"]
                err = str(d.get("errorMessage") or d.get("message") or d.get("remarks") or d)
                log.warning(f"Generate attempt {attempt+1} failed: {err}")
                if "totp" in err.lower() or "invalid" in err.lower():
                    continue
                return None
            except Exception as e:
                log.warning(f"Generate exception: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
        return None

    def ensure_token(self, status_cb: Callable = None) -> str:
        if self.existing_token:
            if status_cb: status_cb("Verifying existing token...")
            if self.verify(self.existing_token):
                log.info("✅ Existing token valid")
                return self.existing_token
            if status_cb: status_cb("Token invalid, trying renew...")
            renewed = self.renew(self.existing_token)
            if renewed:
                self._save_token(renewed)
                return renewed
            if status_cb: status_cb("Renew failed, generating via TOTP...")
        else:
            if status_cb: status_cb("No token found, generating via TOTP...")

        new_token = self.generate(status_cb=status_cb)
        if not new_token:
            raise RuntimeError("Could not obtain Dhan token by any method")
        self._save_token(new_token)
        return new_token

    def _save_token(self, token: str):
        try:
            set_key(self.env_file, "DHAN_ACCESS_TOKEN", token)
        except Exception as e:
            log.warning(f"Could not save token: {e}")


def init_credentials(client_id: str, pin: str, totp_secret: str,
                     existing_token: str = "", env_file: str = ".env",
                     status_cb: Callable = None) -> str:
    global HEADERS, WS_URL
    mgr = DhanTokenManager(client_id, pin, totp_secret, existing_token, env_file)
    token = mgr.ensure_token(status_cb=status_cb)
    HEADERS.update({
        "Content-Type": "application/json", "Accept": "application/json",
        "access-token": token, "client-id": client_id,
    })
    WS_URL = (f"wss://api-feed.dhan.co?version=2"
              f"&token={token}&clientId={client_id}&authType=2")
    return token


# ═══════════════════════════════════════════════════════════════════════════
# REST API HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def api_post(endpoint: str, payload: dict, retries: int = 2) -> Optional[dict]:
    url = f"{BASE_URL}{endpoint}"
    for attempt in range(retries + 1):
        try:
            r = requests.post(url, headers=HEADERS, json=payload, timeout=15)
            if r.status_code == 200: return r.json()
            log.warning(f"API {endpoint} → {r.status_code}: {r.text[:200]}")
            if attempt < retries: time.sleep(1)
        except Exception as e:
            log.error(f"API {endpoint} error: {e}")
            if attempt < retries: time.sleep(1)
    return None

def fetch_expiry_list(index_name: str) -> List[str]:
    cfg = INDEX_CONFIG[index_name]
    payload = {"UnderlyingScrip": int(cfg["security_id"]), "UnderlyingSeg": "IDX_I"}
    resp = api_post("/optionchain/expirylist", payload)
    if resp and resp.get("status") == "success":
        return resp.get("data", [])
    return []

def get_target_expiry(index_name: str) -> Optional[str]:
    expiries = fetch_expiry_list(index_name)
    if not expiries: return None
    today = now_ist().date()
    offset = INDEX_CONFIG[index_name]["expiry_offset"]
    expiry_dates = []
    for exp_str in expiries:
        try:
            d = datetime.strptime(exp_str, "%Y-%m-%d").date()
            if d >= today: expiry_dates.append((d, exp_str))
        except ValueError: continue
    expiry_dates.sort(key=lambda x: x[0])
    if len(expiry_dates) > offset: return expiry_dates[offset][1]
    elif expiry_dates: return expiry_dates[-1][1]
    return None

def fetch_option_chain(index_name: str, expiry: str) -> Optional[dict]:
    cfg = INDEX_CONFIG[index_name]
    payload = {"UnderlyingScrip": int(cfg["security_id"]),
               "UnderlyingSeg": "IDX_I", "Expiry": expiry}
    resp = api_post("/optionchain", payload)
    if resp and resp.get("status") == "success":
        data = resp["data"]
        return {"spot_price": float(data["last_price"]), "oc": data["oc"]}
    return None

def fetch_historical_candles(security_id: str, segment: str,
                              interval: str = "1", days_back: int = 5) -> List[dict]:
    to_date = now_ist().strftime("%Y-%m-%d")
    from_date = (now_ist() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    payload = {"securityId": str(security_id), "exchangeSegment": segment,
               "instrument": "OPTIDX", "interval": interval,
               "fromDate": from_date, "toDate": to_date}
    resp = api_post("/charts/intraday", payload)
    if not resp or "open" not in resp or not resp["open"]: return []
    candles = []
    for i in range(len(resp["open"])):
        candles.append({
            "timestamp": resp["timestamp"][i] if "timestamp" in resp else i,
            "open": float(resp["open"][i]), "high": float(resp["high"][i]),
            "low": float(resp["low"][i]), "close": float(resp["close"][i]),
            "volume": int(resp["volume"][i]) if "volume" in resp else 0,
        })
    return candles

def place_order(security_id: str, segment: str, side: str, qty: int,
                price: float, order_type: str = "LIMIT",
                validity: str = "IOC", product: str = "MIS") -> dict:
    payload = {"transactionType": side, "exchangeSegment": segment,
               "productType": product, "orderType": order_type,
               "validity": validity, "securityId": str(security_id),
               "quantity": qty, "price": price, "dhanClientId": HEADERS.get("client-id", "")}
    return api_post("/orders", payload) or {}


# ═══════════════════════════════════════════════════════════════════════════
# WS BINARY PARSERS
# ═══════════════════════════════════════════════════════════════════════════

def parse_header_8(msg: bytes) -> Optional[dict]:
    if len(msg) < 8: return None
    return {
        "resp_code": msg[0],
        "exch_seg_name": EXCH_SEG_MAP.get(msg[3], str(msg[3])),
        "security_id": str(struct.unpack_from("<I", msg, 4)[0]),
        "payload": msg[8:],
    }

def parse_ticker(payload: bytes) -> Optional[dict]:
    if len(payload) < 8: return None
    return {"ltp": float(struct.unpack_from("<f", payload, 0)[0]),
            "ltt_epoch": int(struct.unpack_from("<I", payload, 4)[0])}


# ═══════════════════════════════════════════════════════════════════════════
# CANDLE ENGINE (WS ticks → 1-min candles)
# ═══════════════════════════════════════════════════════════════════════════

class CandleEngine:
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
            self.last_ltp = ltp; self.last_ltt = ltt_epoch; self.tick_count += 1
            if self.current is None:
                self.current = {"bucket": bucket, "open": ltp, "high": ltp,
                                "low": ltp, "close": ltp, "ticks": 1}
                return
            if bucket == self.current["bucket"]:
                self.current["high"] = max(self.current["high"], ltp)
                self.current["low"] = min(self.current["low"], ltp)
                self.current["close"] = ltp; self.current["ticks"] += 1
                return
            if bucket > self.current["bucket"]:
                completed = dict(self.current)
                self.current = {"bucket": bucket, "open": ltp, "high": ltp,
                                "low": ltp, "close": ltp, "ticks": 1}
                if self.on_candle_close:
                    threading.Thread(target=self.on_candle_close,
                                     args=(self.sec_id, completed), daemon=True).start()

    def snapshot(self) -> dict:
        with self.lock:
            return {"label": self.label, "ltp": self.last_ltp, "ltt": self.last_ltt,
                    "current": dict(self.current) if self.current else None,
                    "tick_count": self.tick_count}


# ═══════════════════════════════════════════════════════════════════════════
# INDICATOR ENGINE (EMA + ADX + ATR — incremental)
# ═══════════════════════════════════════════════════════════════════════════

class IndicatorEngine:
    def __init__(self, ema_fast=9, ema_slow=15, adx_period=5,
                 adx_smoothing=5, atr_period=14):
        self.ema_fast_period = ema_fast; self.ema_slow_period = ema_slow
        self.adx_period = adx_period; self.adx_smoothing = adx_smoothing
        self.atr_period = atr_period
        self.k_fast = 2.0 / (ema_fast + 1.0); self.k_slow = 2.0 / (ema_slow + 1.0)
        self.ema_fast_val = None; self.ema_slow_val = None
        self.prev_high = None; self.prev_low = None; self.prev_close = None
        self.atr_rma = None; self.plus_dm_rma = None; self.minus_dm_rma = None
        self.adx_rma = None; self.sl_atr_rma = None; self.candle_count = 0
        self.candles: deque = deque(maxlen=300)

    def seed_from_historical(self, candles: List[dict]):
        self.candle_count = 0; self.ema_fast_val = None; self.ema_slow_val = None
        self.prev_high = None; self.prev_low = None; self.prev_close = None
        self.atr_rma = None; self.plus_dm_rma = None; self.minus_dm_rma = None
        self.adx_rma = None; self.sl_atr_rma = None; self.candles.clear()
        for c in candles:
            self._process(c["open"], c["high"], c["low"], c["close"], c.get("timestamp", 0))

    def update_candle(self, o, h, l, c, ts=0):
        self._process(o, h, l, c, ts)

    def _process(self, o, h, l, c, ts=0):
        self.candle_count += 1
        if self.ema_fast_val is None:
            self.ema_fast_val = c; self.ema_slow_val = c
        else:
            self.ema_fast_val = c * self.k_fast + self.ema_fast_val * (1.0 - self.k_fast)
            self.ema_slow_val = c * self.k_slow + self.ema_slow_val * (1.0 - self.k_slow)

        if self.prev_high is not None:
            tr = max(h - l, abs(h - self.prev_close), abs(l - self.prev_close))
            up = h - self.prev_high; dn = self.prev_low - l
            pdm = up if (up > dn and up > 0) else 0.0
            mdm = dn if (dn > up and dn > 0) else 0.0
            p = self.adx_period
            if self.atr_rma is None:
                self.atr_rma = tr; self.plus_dm_rma = pdm; self.minus_dm_rma = mdm
            else:
                self.atr_rma = (self.atr_rma * (p-1) + tr) / p
                self.plus_dm_rma = (self.plus_dm_rma * (p-1) + pdm) / p
                self.minus_dm_rma = (self.minus_dm_rma * (p-1) + mdm) / p
            if self.atr_rma and self.atr_rma > 0:
                pdi = 100.0 * self.plus_dm_rma / self.atr_rma
                mdi = 100.0 * self.minus_dm_rma / self.atr_rma
                ds = pdi + mdi
                dx = 100.0 * abs(pdi - mdi) / ds if ds > 0 else 0.0
            else: dx = 0.0
            s = self.adx_smoothing
            self.adx_rma = dx if self.adx_rma is None else (self.adx_rma * (s-1) + dx) / s
            ap = self.atr_period
            self.sl_atr_rma = tr if self.sl_atr_rma is None else (self.sl_atr_rma * (ap-1) + tr) / ap

        self.prev_high = h; self.prev_low = l; self.prev_close = c
        self.candles.append({"ts": ts, "o": o, "h": h, "l": l, "c": c,
                             "ema_f": self.ema_fast_val, "ema_s": self.ema_slow_val,
                             "adx": self.adx_rma or 0, "atr": self.sl_atr_rma or 0})

    def is_ready(self):
        return self.candle_count >= self.ema_slow_period and self.ema_fast_val is not None and self.adx_rma is not None

    def get_entry_signal(self, adx_threshold=20.0):
        if not self.is_ready(): return False
        return self.ema_fast_val > self.ema_slow_val and (self.adx_rma or 0) > adx_threshold

    def get_values(self):
        return {"ema_fast": round(self.ema_fast_val, 2) if self.ema_fast_val else None,
                "ema_slow": round(self.ema_slow_val, 2) if self.ema_slow_val else None,
                "adx": round(self.adx_rma, 2) if self.adx_rma else None,
                "atr": round(self.sl_atr_rma, 2) if self.sl_atr_rma else None,
                "candles": self.candle_count, "ready": self.is_ready()}


# ═══════════════════════════════════════════════════════════════════════════
# STRATEGY CONFIG
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class LotConfig:
    lot_number: int
    target_points: Optional[float]
    tsl_points: Optional[float]

@dataclass
class StrategyConfig:
    paper_mode: bool = True
    ema_fast: int = 9; ema_slow: int = 15
    adx_period: int = 5; adx_smoothing: int = 5; adx_threshold: float = 20.0
    atr_period: int = 14; atr_multiplier: float = 1.5
    target_premium: float = 50.0
    premium_range_low: float = 40.0; premium_range_high: float = 65.0
    max_strike_scan: int = 5
    num_lots: int = 4; multiplier: int = 1; multiplier_enabled: bool = False
    lot_configs: List[LotConfig] = field(default_factory=lambda: [
        LotConfig(1, 1.0, None), LotConfig(2, 7.5, 3.0),
        LotConfig(3, 15.0, 3.0), LotConfig(4, None, 3.0)])
    de1_day_target: float = 6000.0; de1_day_sl: float = 1500.0
    de2_day_sl: float = 1500.0; de2_safety_line: float = 2000.0
    de2_milestones: List[float] = field(default_factory=lambda: [6000,10000,15000,20000,30000])
    logic_vikalp: bool = True; logic_epel: bool = True
    logic_exit_plus1: bool = True; logic_vivek: bool = True
    logic_kuber: bool = True; logic_hanuman_tail: bool = True; logic_tiered_risk: bool = True
    kuber_lock_pct: float = 0.90; kuber_buffer_pct: float = 0.10
    hanuman_lock_offset: float = 1000.0; hanuman_buffer: float = 1000.0
    tiered_tiers: List[Dict] = field(default_factory=lambda: [
        {"profit": 12000, "lock": 11000, "buffer": 1000},
        {"profit": 20000, "lock": 18000, "buffer": 2000},
        {"profit": 30000, "lock": 27000, "buffer": 3000}])
    trade_start: str = "09:16"; trade_end: str = "15:15"
    cooldown_min: int = 5; single_candle_entry: bool = True
    max_sl: float = 25.0; candle_sec: int = 60


# ═══════════════════════════════════════════════════════════════════════════
# TRADE STATE
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class LotState:
    lot_number: int; qty: int; entry_price: float
    target_points: Optional[float]; tsl_points: Optional[float]
    is_active: bool = True; highest: float = 0.0
    tsl_active: bool = False; tsl_trigger: float = 0.0
    exit_price: float = 0.0; exit_reason: str = ""

@dataclass
class ActiveTrade:
    trade_id: str; index: str; de_mode: str; side: Side
    security_id: str; strike: float; entry_price: float; entry_time: str
    lots: List[LotState] = field(default_factory=list)
    is_open: bool = True; total_pnl: float = 0.0
    epel_gate: bool = True; current_ltp: float = 0.0

@dataclass
class ProfitTracker:
    de_mode: str; index: str; total_pnl: float = 0.0; trade_count: int = 0
    is_stopped: bool = False; cooldown_until: Optional[datetime] = None
    last_side: Optional[Side] = None; last_bucket: Optional[int] = None
    current_lock: float = 0.0; safety_remaining: float = 0.0
    kuber_lock: float = 0.0; kuber_buffer: float = 0.0; kuber_active: bool = False
    hanuman_active: bool = False; hanuman_lock: float = 0.0; hanuman_high: float = 0.0
    tier_index: int = -1


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class BullockCart17Engine:
    def __init__(self, config: StrategyConfig = None, gui_callback: Callable = None):
        """
        gui_callback: function(event_type, data_dict) called on every state change
                      so the GUI can update in real-time.
        """
        self.config = config or StrategyConfig()
        self.gui_cb = gui_callback or (lambda *a: None)
        self.stop_event = threading.Event()

        self.expiry: Dict[str, str] = {}
        self.spot_price: Dict[str, float] = {}
        self.option_info: Dict[str, dict] = {}
        self.indicators: Dict[str, IndicatorEngine] = {}
        self.indicator_labels: Dict[str, str] = {}
        self.candle_engines: Dict[str, CandleEngine] = {}
        self.spot_engines: Dict[str, CandleEngine] = {}

        self.ws: Optional[websocket.WebSocketApp] = None
        self.ws_connected = threading.Event()
        self.ws_instruments: List[dict] = []

        self.trackers: Dict[str, ProfitTracker] = {}
        for de in [DEMode.PROFIT_CAP, DEMode.NO_CAP]:
            for idx in ["NIFTY", "SENSEX"]:
                key = f"{de.value}_{idx}"
                self.trackers[key] = ProfitTracker(de_mode=de.value, index=idx)

        self.trades_lock = threading.Lock()
        self.active_trades: Dict[str, ActiveTrade] = {}
        self.closed_trades: List[ActiveTrade] = []

        self.stats_lock = threading.Lock()
        self.packet_count = 0
        self.last_ws_error: Optional[str] = None

    # ─── EMIT GUI EVENT ───
    def _emit(self, event: str, data: dict = None):
        try: self.gui_cb(event, data or {})
        except Exception: pass

    # ─── INITIALIZE ───
    def initialize(self):
        self._emit("status", {"msg": "Initializing strategy..."})
        for idx in ["NIFTY", "SENSEX"]:
            self._emit("status", {"msg": f"Fetching {idx} expiry..."})
            exp = get_target_expiry(idx)
            if not exp:
                self._emit("error", {"msg": f"No expiry for {idx}"})
                continue
            self.expiry[idx] = exp
            self._setup_index_strikes(idx)

        if not self.ws_instruments:
            self._emit("error", {"msg": "No instruments found. Check market hours."})
            return False
        return True

    def _setup_index_strikes(self, index):
        exp = self.expiry.get(index)
        if not exp: return
        oc_data = fetch_option_chain(index, exp)
        if not oc_data:
            self._emit("error", {"msg": f"Option chain failed for {index}"})
            return
        spot = oc_data["spot_price"]
        self.spot_price[index] = spot
        self._emit("spot", {"index": index, "spot": spot, "expiry": exp})

        cfg = self.config; oc = oc_data["oc"]
        for side in [Side.CE, Side.PE]:
            candidates = self._find_candidates(oc, side, cfg)
            if not candidates: continue
            best = candidates[0]
            sec_id = best["security_id"]; strike = best["strike"]
            label = f"{index} {int(strike)}{side.value}"
            for de in [DEMode.PROFIT_CAP, DEMode.NO_CAP]:
                key = f"{de.value}_{index}_{side.value}"
                self.option_info[key] = {
                    "security_id": sec_id, "strike": strike, "ltp": best["ltp"],
                    "side": side.value, "index": index,
                    "segment": INDEX_CONFIG[index]["segment"],
                }
            if sec_id not in self.indicators:
                self._setup_indicator(sec_id, label, index)
            self._emit("strike", {"index": index, "side": side.value,
                                   "strike": int(strike), "sec_id": sec_id,
                                   "ltp": best["ltp"]})

    def _find_candidates(self, oc, side, cfg):
        candidates = []
        opt_key = "ce" if side == Side.CE else "pe"
        for sk, sd in oc.items():
            if opt_key not in sd: continue
            opt = sd[opt_key]; ltp = float(opt.get("last_price", 0))
            if cfg.premium_range_low <= ltp <= cfg.premium_range_high:
                try: sv = float(sk)
                except: continue
                candidates.append({"security_id": str(opt["security_id"]),
                                    "strike": sv, "ltp": ltp,
                                    "distance": abs(ltp - cfg.target_premium)})
        candidates.sort(key=lambda x: x["distance"])
        return candidates

    def _setup_indicator(self, sec_id, label, index):
        cfg = self.config; segment = INDEX_CONFIG[index]["segment"]
        self._emit("status", {"msg": f"Loading history: {label}..."})
        candles = fetch_historical_candles(sec_id, segment, "1", 5)
        if candles:
            now_ep = int(time.time()); cb = candle_bucket(now_ep)
            if candles and int(candles[-1].get("timestamp", 0)) >= cb:
                candles = candles[:-1]
        ind = IndicatorEngine(cfg.ema_fast, cfg.ema_slow, cfg.adx_period,
                               cfg.adx_smoothing, cfg.atr_period)
        if candles: ind.seed_from_historical(candles)
        self.indicators[sec_id] = ind
        self.indicator_labels[sec_id] = label
        engine = CandleEngine(sec_id, label, cfg.candle_sec, self.on_candle_close)
        self.candle_engines[sec_id] = engine
        self.ws_instruments.append({"name": label,
                                     "exchange": INDEX_CONFIG[index]["segment"],
                                     "security_id": sec_id})

    # ─── CANDLE CLOSE ───
    def on_candle_close(self, sec_id, candle):
        ind = self.indicators.get(sec_id)
        if not ind: return
        o, h, l, c = float(candle["open"]), float(candle["high"]), float(candle["low"]), float(candle["close"])
        ts = int(candle["bucket"])
        ind.update_candle(o, h, l, c, ts)

        vals = ind.get_values()
        self._emit("indicator", {"sec_id": sec_id, "label": self.indicator_labels.get(sec_id, ""),
                                  **vals, "ltp": c, "time": epoch_to_ist_str(ts, "%H:%M")})

        if not ind.is_ready(): return
        now = now_ist()
        try:
            st = datetime.strptime(self.config.trade_start, "%H:%M").time()
            et = datetime.strptime(self.config.trade_end, "%H:%M").time()
        except: return
        if not (st <= now.time() <= et): return

        if ind.get_entry_signal(self.config.adx_threshold):
            for key, info in self.option_info.items():
                if info["security_id"] != sec_id: continue
                parts = key.split("_"); de_mode, index, side_str = parts[0], parts[1], parts[2]
                side = Side(side_str)
                if not self._check_entry(de_mode, index, side, ts): continue
                existing = [t for t in self.active_trades.values()
                            if t.de_mode == de_mode and t.index == index
                            and t.side == side and t.is_open]
                if existing: continue
                self._execute_entry(index, de_mode, side, sec_id, info["strike"],
                                     c, ts, vals.get("atr") or 0)

    # ─── ENTRY CHECKS ───
    def _check_entry(self, de_mode, index, side, bucket):
        key = f"{de_mode}_{index}"; t = self.trackers[key]; cfg = self.config; now = now_ist()
        if t.is_stopped: return False
        if t.cooldown_until and now < t.cooldown_until and t.last_side == side: return False
        if cfg.single_candle_entry and t.last_bucket == bucket: return False
        if de_mode == DEMode.PROFIT_CAP.value:
            if t.total_pnl >= cfg.de1_day_target:
                if not (cfg.logic_hanuman_tail and t.hanuman_active):
                    t.is_stopped = True; return False
        day_sl = cfg.de1_day_sl if de_mode == DEMode.PROFIT_CAP.value else cfg.de2_day_sl
        if t.total_pnl <= -day_sl: t.is_stopped = True; return False
        if de_mode == DEMode.NO_CAP.value and t.current_lock > 0:
            if t.total_pnl <= t.current_lock: t.is_stopped = True; return False
        return True

    # ─── EXECUTE ENTRY ───
    def _execute_entry(self, index, de_mode, side, sec_id, strike, price, bucket, atr):
        cfg = self.config; key = f"{de_mode}_{index}"; tracker = self.trackers[key]
        lot_size = INDEX_CONFIG[index]["lot_size"]
        tid = f"{de_mode}_{index}_{side.value}_{now_ist().strftime('%H%M%S%f')}"
        mult = cfg.multiplier if cfg.multiplier_enabled else 1
        lots = [LotState(lc.lot_number, lot_size * mult, price,
                          lc.target_points, lc.tsl_points, highest=price)
                for lc in cfg.lot_configs]
        trade = ActiveTrade(tid, index, de_mode, side, sec_id, strike, price,
                             now_ist().strftime("%H:%M:%S"), lots, current_ltp=price)
        total_qty = sum(l.qty for l in lots)

        if not cfg.paper_mode:
            place_order(sec_id, INDEX_CONFIG[index]["segment"], "BUY",
                        total_qty, price, "LIMIT", "IOC")

        tracker.trade_count += 1; tracker.last_side = side; tracker.last_bucket = bucket
        with self.trades_lock: self.active_trades[tid] = trade

        self._emit("entry", {"trade_id": tid, "index": index, "de": de_mode,
                              "side": side.value, "strike": int(strike), "price": price,
                              "qty": total_qty, "atr": atr,
                              "paper": cfg.paper_mode})
        log.info(f"{'PAPER ' if cfg.paper_mode else ''}ENTRY {tid} {index} "
                 f"{side.value} @ ₹{price:.2f} Strike={int(strike)} Qty={total_qty}")

    # ─── EXIT LOT ───
    def _exit_lot(self, trade, lot, exit_price, reason):
        if not lot.is_active: return
        lot.is_active = False; lot.exit_price = exit_price; lot.exit_reason = reason
        pnl = (exit_price - lot.entry_price) * lot.qty
        key = f"{trade.de_mode}_{trade.index}"; tracker = self.trackers[key]
        tracker.total_pnl += pnl; trade.total_pnl += pnl

        self._emit("lot_exit", {"trade_id": trade.trade_id, "lot": lot.lot_number,
                                 "price": exit_price, "reason": reason, "pnl": pnl,
                                 "day_pnl": tracker.total_pnl})
        log.info(f"EXIT {trade.trade_id} L{lot.lot_number} @ ₹{exit_price:.2f} "
                 f"{reason} PnL=₹{pnl:+.0f} Day=₹{tracker.total_pnl:+.0f}")

        if all(not l.is_active for l in trade.lots):
            trade.is_open = False
            if trade.total_pnl < 0:
                tracker.cooldown_until = now_ist() + timedelta(minutes=self.config.cooldown_min)
                tracker.last_side = trade.side
            self._emit("trade_closed", {"trade_id": trade.trade_id,
                                         "pnl": trade.total_pnl,
                                         "de": trade.de_mode, "index": trade.index})

    def _exit_all(self, trade, price, reason):
        for lot in trade.lots:
            if lot.is_active: self._exit_lot(trade, lot, price, reason)

    # ─── LOGIC: EPEL ───
    def _check_epel(self, trade, ltp):
        if not self.config.logic_epel or not trade.epel_gate: return False
        if not trade.lots[0].is_active:
            trade.epel_gate = False; return False
        entry = trade.entry_price
        mx = max(l.highest for l in trade.lots if l.is_active)
        if mx < entry + 1.0 and ltp <= entry:
            self._exit_all(trade, entry, "EPEL_SCRATCH"); return True
        if mx >= entry + 1.0 and ltp <= entry:
            self._exit_all(trade, entry, "EPEL_GATE2"); return True
        return False

    # ─── LOGIC: EXIT+1 ───
    def _check_exit_plus1(self, trade, ltp):
        if not self.config.logic_exit_plus1: return False
        if trade.lots[0].is_active: return False
        p1 = trade.entry_price + 1.0
        if ltp <= p1:
            rem = [l for l in trade.lots if l.is_active]
            if rem:
                for l in rem: self._exit_lot(trade, l, p1, "EXIT+1")
                return True
        return False

    # ─── LOGIC: VIVEK ───
    def _check_vivek(self, trade, ltp):
        if not self.config.logic_vivek: return False
        entry = trade.entry_price; acted = False
        for lot in trade.lots:
            if not lot.is_active or lot.lot_number not in [2, 3]: continue
            mx = lot.highest - entry
            if mx < 2.0 or lot.tsl_active: continue
            he = entry + mx / 2.0
            if ltp <= he:
                self._exit_lot(trade, lot, he, "VIVEK_HALF"); acted = True
        return acted

    # ─── LOT MONITOR ───
    def _monitor_lots(self, trade, ltp):
        entry = trade.entry_price; cfg = self.config
        for lot in trade.lots:
            if not lot.is_active: continue
            if ltp > lot.highest: lot.highest = ltp
            move = ltp - entry
            if lot.target_points is not None and move >= lot.target_points:
                self._exit_lot(trade, lot, ltp, f"TGT+{lot.target_points}"); continue
            if lot.tsl_points is not None:
                mm = lot.highest - entry
                if mm >= lot.tsl_points and not lot.tsl_active:
                    lot.tsl_active = True; lot.tsl_trigger = lot.highest - lot.tsl_points
                if lot.tsl_active:
                    nt = lot.highest - lot.tsl_points
                    if nt > lot.tsl_trigger: lot.tsl_trigger = nt
                    if ltp <= lot.tsl_trigger:
                        self._exit_lot(trade, lot, lot.tsl_trigger, f"TSL{lot.tsl_points}"); continue
            if move <= -cfg.max_sl:
                self._exit_lot(trade, lot, ltp, f"HARDSL{cfg.max_sl}")

    # ─── PROFIT MGMT ───
    def _apply_kuber(self, dm, idx):
        if not self.config.logic_kuber: return
        t = self.trackers[f"{dm}_{idx}"]
        if t.total_pnl <= 0: t.kuber_active = False; return
        lk = t.total_pnl * self.config.kuber_lock_pct
        if lk > t.kuber_lock:
            t.kuber_lock = lk; t.kuber_buffer = t.total_pnl * self.config.kuber_buffer_pct
            t.kuber_active = True
            self._emit("kuber", {"key": f"{dm}_{idx}", "lock": lk})
        if t.kuber_active and t.total_pnl <= t.kuber_lock: t.is_stopped = True

    def _apply_hanuman(self, dm, idx):
        if not self.config.logic_hanuman_tail or dm != DEMode.PROFIT_CAP.value: return
        t = self.trackers[f"{dm}_{idx}"]
        if t.total_pnl < self.config.de1_day_target: return
        if not t.hanuman_active:
            t.hanuman_active = True; t.hanuman_high = t.total_pnl; t.is_stopped = False
            self._emit("hanuman", {"key": f"{dm}_{idx}", "activated": True})
        if t.total_pnl > t.hanuman_high: t.hanuman_high = t.total_pnl
        t.hanuman_lock = t.hanuman_high - self.config.hanuman_lock_offset
        if t.total_pnl <= t.hanuman_lock: t.is_stopped = True

    def _apply_tiered(self, dm, idx):
        if not self.config.logic_tiered_risk: return
        t = self.trackers[f"{dm}_{idx}"]
        for i in range(len(self.config.tiered_tiers) - 1, -1, -1):
            tier = self.config.tiered_tiers[i]
            if t.total_pnl >= tier["profit"] and i > t.tier_index:
                t.tier_index = i; t.kuber_lock = tier["lock"]
                t.kuber_buffer = tier["buffer"]; t.kuber_active = True
                self._emit("tiered", {"key": f"{dm}_{idx}", "tier": i+1, "lock": tier["lock"]})
                break
        if t.kuber_active and t.total_pnl <= t.kuber_lock: t.is_stopped = True

    def _apply_de2_lock(self, idx):
        t = self.trackers[f"{DEMode.NO_CAP.value}_{idx}"]
        if t.total_pnl <= 0: return
        cfg = self.config
        for ms in sorted(cfg.de2_milestones, reverse=True):
            if t.total_pnl >= ms:
                nl = ms - cfg.de2_safety_line
                if nl > t.current_lock:
                    t.current_lock = nl; t.safety_remaining = cfg.de2_safety_line
                    self._emit("de2_lock", {"index": idx, "lock": nl, "profit": t.total_pnl})
                break
        if t.current_lock > 0 and t.total_pnl <= t.current_lock: t.is_stopped = True

    # ─── TICK MONITOR ───
    def _on_trade_tick(self, sec_id, ltp):
        with self.trades_lock:
            for tid, trade in list(self.active_trades.items()):
                if trade.security_id != sec_id or not trade.is_open: continue
                trade.current_ltp = ltp
                if self._check_epel(trade, ltp): continue
                self._monitor_lots(trade, ltp)
                if self._check_exit_plus1(trade, ltp): continue
                self._check_vivek(trade, ltp)
                self._apply_kuber(trade.de_mode, trade.index)
                self._apply_hanuman(trade.de_mode, trade.index)
                self._apply_tiered(trade.de_mode, trade.index)
                if trade.de_mode == DEMode.NO_CAP.value:
                    self._apply_de2_lock(trade.index)
            closed = [t for t, tr in self.active_trades.items() if not tr.is_open]
            for t in closed: self.closed_trades.append(self.active_trades.pop(t))

    # ─── WEBSOCKET ───
    def on_ws_open(self, ws):
        self.ws_connected.set()
        spot_instr = [{"ExchangeSegment": "IDX_I", "SecurityId": INDEX_CONFIG[idx]["security_id"]}
                      for idx in ["NIFTY", "SENSEX"]]
        opt_instr = [{"ExchangeSegment": str(i["exchange"]), "SecurityId": str(i["security_id"])}
                     for i in self.ws_instruments]
        all_instr = spot_instr + opt_instr
        ws.send(json.dumps({"RequestCode": REQ_SUB_TICKER,
                             "InstrumentCount": len(all_instr),
                             "InstrumentList": all_instr}))
        self._emit("ws_connected", {"instruments": len(all_instr)})
        log.info(f"WS connected — {len(all_instr)} instruments")

    def on_ws_message(self, ws, message):
        if isinstance(message, str): return
        hdr = parse_header_8(bytes(message))
        if not hdr: return
        code = int(hdr["resp_code"]); sec_id = str(hdr["security_id"])
        with self.stats_lock: self.packet_count += 1
        if code == RESP_TICKER:
            t = parse_ticker(hdr["payload"])
            if not t: return
            ltp = float(t["ltp"]); ltt = int(t["ltt_epoch"])
            for idx in ["NIFTY", "SENSEX"]:
                if sec_id == INDEX_CONFIG[idx]["security_id"]:
                    self.spot_price[idx] = ltp
                    self._emit("spot_tick", {"index": idx, "ltp": ltp})
            if sec_id in self.candle_engines:
                self.candle_engines[sec_id].on_tick(ltp, ltt)
            self._on_trade_tick(sec_id, ltp)

    def on_ws_error(self, ws, error):
        with self.stats_lock: self.last_ws_error = str(error)
        self._emit("ws_error", {"error": str(error)})

    def on_ws_close(self, ws, status_code, msg):
        self.ws_connected.clear()
        self._emit("ws_disconnected", {})

    def run_ws(self):
        websocket.enableTrace(False)
        while not self.stop_event.is_set():
            try:
                self.ws = websocket.WebSocketApp(WS_URL,
                    on_open=self.on_ws_open, on_message=self.on_ws_message,
                    on_error=self.on_ws_error, on_close=self.on_ws_close)
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                with self.stats_lock: self.last_ws_error = str(e)
            finally:
                if not self.stop_event.is_set(): time.sleep(2)

    # ─── EOD ───
    def eod_exit_all(self):
        with self.trades_lock:
            for tid, trade in list(self.active_trades.items()):
                if trade.is_open:
                    self._exit_all(trade, trade.current_ltp or trade.entry_price, "EOD")
            closed = [t for t, tr in self.active_trades.items() if not tr.is_open]
            for t in closed: self.closed_trades.append(self.active_trades.pop(t))

    def get_summary(self) -> dict:
        total = 0; rows = []
        for key, t in self.trackers.items():
            rows.append({"key": key, "pnl": t.total_pnl, "trades": t.trade_count,
                          "stopped": t.is_stopped, "lock": t.current_lock,
                          "kuber": t.kuber_lock if t.kuber_active else 0})
            total += t.total_pnl
        return {"trackers": rows, "total_pnl": total,
                "closed_count": len(self.closed_trades)}

    # ─── MAIN RUN (called from GUI thread) ───
    def start(self):
        if not self.initialize(): return
        ws_thread = threading.Thread(target=self.run_ws, daemon=True)
        ws_thread.start()
        self._emit("status", {"msg": "Waiting for WebSocket..."})
        self.ws_connected.wait(timeout=15)
        if not self.ws_connected.is_set():
            self._emit("error", {"msg": "WebSocket failed to connect"})
            return
        self._emit("status", {"msg": "LIVE — Monitoring for signals"})

        # Monitor loop in background
        def _monitor():
            while not self.stop_event.is_set():
                try:
                    now = now_ist()
                    et = datetime.strptime(self.config.trade_end, "%H:%M").time()
                    buf = (datetime.combine(now.date(), et) + timedelta(minutes=2)).time()
                    if now.time() > buf:
                        self.eod_exit_all()
                        self._emit("eod", self.get_summary())
                        break
                    self._emit("tick_update", {
                        "spots": dict(self.spot_price),
                        "packets": self.packet_count,
                        "ws_error": self.last_ws_error,
                        "active_count": len(self.active_trades),
                        **self.get_summary(),
                    })
                    time.sleep(1)
                except Exception as e:
                    log.error(f"Monitor error: {e}")
                    time.sleep(1)

        threading.Thread(target=_monitor, daemon=True).start()

    def stop(self):
        self.stop_event.set()
        if self.ws:
            try: self.ws.close()
            except: pass
