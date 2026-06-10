"""
RA4 Bullock Cart 17 — GUI Application v3
═════════════════════════════════════════
Light theme | Bold fonts | Balfund Trading Pvt Ltd
"""

import os, sys, json, time, logging, threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import customtkinter as ctk
from dotenv import load_dotenv, set_key

from engine import (
    BullockCart17Engine, StrategyConfig, LotConfig, init_credentials,
    set_credentials, DEMode, Side, now_ist, RATE_LIMITER, ENV_FILE
)

# ═══════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"ra4_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()]
)
log = logging.getLogger("RA4")

# ═══════════════════════════════════════════════════════════════════════════
# LIGHT THEME (matches Renko system)
# ═══════════════════════════════════════════════════════════════════════════

ctk.set_appearance_mode("light")
ctk.set_default_color_theme("blue")

BG   = "#f0f2f5"
CARD = "#ffffff"
ACC  = "#0369a1"
GRN  = "#16a34a"
RED  = "#dc2626"
YEL  = "#ca8a04"
TXT  = "#111827"
DIM  = "#6b7280"
BD   = "#d1d5db"

FT = ("Segoe UI", 20, "bold")   # Title
FH = ("Segoe UI", 16, "bold")   # Section header
FM = ("Segoe UI", 14, "bold")   # Medium bold
FS = ("Segoe UI", 13)           # Body
FX = ("Consolas", 12)           # Mono
FL = ("Segoe UI", 12)           # Label
FB = ("Segoe UI", 30, "bold")   # Big PnL
FC = ("Segoe UI", 16, "bold")   # Card PnL
FN = ("Segoe UI", 11)           # Small


# ═══════════════════════════════════════════════════════════════════════════
# APP
# ═══════════════════════════════════════════════════════════════════════════

class BullockCartApp(ctk.CTk):
    VERSION = "3.0"

    def __init__(self):
        super().__init__()
        load_dotenv(str(ENV_FILE), override=True)
        self.title(f"RA4 Bullock Cart 17  v{self.VERSION}  —  Balfund Trading")
        self.geometry("1480x920")
        self.minsize(1200, 750)
        self.configure(fg_color=BG)

        self.engine: Optional[BullockCart17Engine] = None
        self.is_running = False

        self._build_ui()
        self._load_env()
        self._tick_clock()

    # ═════════════════════════════════════════════════════════════
    # BUILD
    # ═════════════════════════════════════════════════════════════

    def _build_ui(self):
        # ── TOP BAR ──
        top = ctk.CTkFrame(self, fg_color=CARD, height=56, corner_radius=0,
                            border_width=1, border_color=BD)
        top.pack(fill="x"); top.pack_propagate(False)

        ctk.CTkLabel(top, text="⚡ RA4 BULLOCK CART 17", font=FT,
                     text_color=ACC).pack(side="left", padx=18)
        ctk.CTkLabel(top, text=f"v{self.VERSION}", font=FN,
                     text_color=DIM).pack(side="left", padx=4, pady=(6, 0))

        self.lbl_clock = ctk.CTkLabel(top, text="--:--:--", font=FM, text_color=TXT)
        self.lbl_clock.pack(side="right", padx=18)

        self.lbl_status = ctk.CTkLabel(top, text="● IDLE", font=FM, text_color=YEL)
        self.lbl_status.pack(side="right", padx=14)

        self.lbl_ws = ctk.CTkLabel(top, text="WS ─", font=FL, text_color=DIM)
        self.lbl_ws.pack(side="right", padx=10)

        # ── BODY 3-PANEL ──
        body = ctk.CTkFrame(self, fg_color=BG)
        body.pack(fill="both", expand=True, padx=8, pady=(4, 0))

        # LEFT
        self.left = ctk.CTkScrollableFrame(body, fg_color=CARD, width=340,
                                            corner_radius=8, border_width=1, border_color=BD)
        self.left.pack(side="left", fill="y", padx=(0, 4))

        # CENTER
        self.center = ctk.CTkFrame(body, fg_color=BG)
        self.center.pack(side="left", fill="both", expand=True, padx=4)

        # RIGHT
        self.right = ctk.CTkFrame(body, fg_color=CARD, width=340,
                                   corner_radius=8, border_width=1, border_color=BD)
        self.right.pack(side="right", fill="y", padx=(4, 0))
        self.right.pack_propagate(False)

        self._build_settings()
        self._build_dashboard()
        self._build_trade_log()
        self._build_bottom()

    # ─── SETTINGS ───

    def _build_settings(self):
        p = self.left

        self._sec(p, "🔐 CREDENTIALS")
        self.ent_cid  = self._entry(p, "Client ID")
        self.ent_pin  = self._entry(p, "PIN", show="•")
        self.ent_totp = self._entry(p, "TOTP Secret", show="•")

        self._sec(p, "⚙️ MODE")
        self.var_paper = ctk.BooleanVar(value=True)
        ctk.CTkSwitch(p, text="Paper Trading", font=FM, variable=self.var_paper,
                       progress_color=YEL, text_color=TXT).pack(anchor="w", padx=14, pady=5)

        self._sec(p, "📊 INDICATORS")
        g1 = ctk.CTkFrame(p, fg_color="transparent"); g1.pack(fill="x", padx=14, pady=2)
        self.ent_ef  = self._gentry(g1, "EMA Fast", "9", 0)
        self.ent_es  = self._gentry(g1, "EMA Slow", "15", 1)
        self.ent_adp = self._gentry(g1, "ADX Period", "5", 2)
        self.ent_adt = self._gentry(g1, "ADX Thresh", "20", 3)
        self.ent_atr = self._gentry(g1, "ATR Period", "14", 4)

        self._sec(p, "🎯 STRIKE SELECTION")
        g2 = ctk.CTkFrame(p, fg_color="transparent"); g2.pack(fill="x", padx=14, pady=2)
        self.ent_tp  = self._gentry(g2, "Target ₹", "50", 0)
        self.ent_prl = self._gentry(g2, "Range Low", "40", 1)
        self.ent_prh = self._gentry(g2, "Range High", "65", 2)

        self._sec(p, "🏎 DE1 — PROFIT CAP")
        g3 = ctk.CTkFrame(p, fg_color="transparent"); g3.pack(fill="x", padx=14, pady=2)
        self.ent_d1t = self._gentry(g3, "Day Target ₹", "6000", 0)
        self.ent_d1s = self._gentry(g3, "Day SL ₹", "1500", 1)

        self._sec(p, "🚀 DE2 — NO CAP")
        g4 = ctk.CTkFrame(p, fg_color="transparent"); g4.pack(fill="x", padx=14, pady=2)
        self.ent_d2s = self._gentry(g4, "Day SL ₹", "1500", 0)
        self.ent_d2l = self._gentry(g4, "Safety Line ₹", "2000", 1)

        self._sec(p, "⏰ TIMING & LIMITS")
        g5 = ctk.CTkFrame(p, fg_color="transparent"); g5.pack(fill="x", padx=14, pady=2)
        self.ent_ts  = self._gentry(g5, "Start", "09:16", 0)
        self.ent_te  = self._gentry(g5, "End", "15:15", 1)
        self.ent_cd  = self._gentry(g5, "Cooldown min", "5", 2)
        self.ent_msl = self._gentry(g5, "Max SL pts", "25", 3)
        self.ent_mxt = self._gentry(g5, "Max Trades/idx", "30", 4)

        self._sec(p, "🧠 7 SMART LOGICS")
        self.logic_vars = {}
        for key, txt in [
            ("logic_vikalp_enabled",      "Vikalp — Strike Scanner"),
            ("logic_epel_enabled",        "EPEL — Scratch Exit"),
            ("logic_exit_plus1_enabled",  "Exit+1 — Safety Exit"),
            ("logic_vivek_enabled",       "Vivek — Half-Move"),
            ("logic_kuber_enabled",       "Kuber — Profit Vault"),
            ("logic_hanuman_tail_enabled","Hanuman Tail — Trail Cap"),
            ("logic_tiered_risk_enabled", "Tiered Risk — Scaling"),
        ]:
            v = ctk.BooleanVar(value=True); self.logic_vars[key] = v
            ctk.CTkSwitch(p, text=txt, font=FL, variable=v,
                           progress_color=ACC, text_color=TXT, width=36
                           ).pack(anchor="w", padx=14, pady=3)

        ctk.CTkFrame(p, fg_color="transparent", height=10).pack()

        self.btn_start = ctk.CTkButton(p, text="▶  START ENGINE", font=FM,
                                        fg_color=GRN, hover_color="#15803d",
                                        text_color="white", height=46, corner_radius=8,
                                        command=self._on_start)
        self.btn_start.pack(fill="x", padx=14, pady=4)

        self.btn_stop = ctk.CTkButton(p, text="■  STOP ENGINE", font=FM,
                                       fg_color=RED, hover_color="#b91c1c",
                                       text_color="white", height=46, corner_radius=8,
                                       command=self._on_stop, state="disabled")
        self.btn_stop.pack(fill="x", padx=14, pady=(0, 10))

    # ─── DASHBOARD ───

    def _build_dashboard(self):
        c = self.center

        # Hero PnL
        pnl_card = ctk.CTkFrame(c, fg_color=CARD, height=100, corner_radius=10,
                                 border_width=1, border_color=BD)
        pnl_card.pack(fill="x", pady=(0, 4)); pnl_card.pack_propagate(False)
        ctk.CTkLabel(pnl_card, text="TOTAL DAY P&L", font=FL, text_color=DIM).pack(pady=(10, 0))
        self.lbl_pnl = ctk.CTkLabel(pnl_card, text="₹ 0", font=FB, text_color=TXT)
        self.lbl_pnl.pack()

        # Spot row
        row_s = ctk.CTkFrame(c, fg_color="transparent"); row_s.pack(fill="x", pady=4)
        self.spot_cards = {}
        for idx in ["NIFTY", "SENSEX"]:
            cd = ctk.CTkFrame(row_s, fg_color=CARD, corner_radius=8,
                               border_width=1, border_color=BD)
            cd.pack(side="left", fill="x", expand=True,
                    padx=(0 if idx == "NIFTY" else 3, 3 if idx == "NIFTY" else 0))
            ctk.CTkLabel(cd, text=idx, font=FH, text_color=ACC).pack(pady=(6, 0))
            ls = ctk.CTkLabel(cd, text="—", font=(F_FAMILY, 20, "bold") if False else ("Segoe UI", 20, "bold"),
                               text_color=TXT)
            ls.pack()
            le = ctk.CTkLabel(cd, text="Exp: —", font=FN, text_color=DIM)
            le.pack(pady=(0, 6))
            self.spot_cards[idx] = {"spot": ls, "expiry": le}

        # DE tracker cards
        row_d = ctk.CTkFrame(c, fg_color="transparent"); row_d.pack(fill="x", pady=4)
        self.de_cards = {}
        for key in ["DE1_NIFTY", "DE1_SENSEX", "DE2_NIFTY", "DE2_SENSEX"]:
            cd = ctk.CTkFrame(row_d, fg_color=CARD, corner_radius=8,
                               border_width=1, border_color=BD)
            cd.pack(side="left", fill="x", expand=True, padx=2)
            ctk.CTkLabel(cd, text=key.replace("_", " "), font=FN,
                         text_color=DIM).pack(pady=(5, 0))
            lp = ctk.CTkLabel(cd, text="₹ 0", font=FC, text_color=TXT)
            lp.pack()
            li = ctk.CTkLabel(cd, text="0 trades | Active", font=FN, text_color=DIM)
            li.pack(pady=(0, 5))
            self.de_cards[key] = {"pnl": lp, "info": li}

        # Indicators
        self._sec(c, "📈 LIVE INDICATORS")
        self.ind_frame = ctk.CTkFrame(c, fg_color=CARD, corner_radius=8,
                                        border_width=1, border_color=BD)
        self.ind_frame.pack(fill="x", pady=2)
        hdr = ctk.CTkFrame(self.ind_frame, fg_color="#e8ecf1", corner_radius=0)
        hdr.pack(fill="x")
        for col, w in [("Option", 180), ("LTP", 70), ("EMA 9", 70), ("EMA 15", 70),
                        ("ADX", 55), ("ATR", 55), ("Signal", 65)]:
            ctk.CTkLabel(hdr, text=col, font=(F_FAMILY, 11, "bold") if False else ("Segoe UI", 11, "bold"),
                         text_color=TXT, width=w, anchor="w").pack(side="left", padx=4, pady=3)
        self.ind_rows: Dict[str, dict] = {}

        # Active trades
        self._sec(c, "🔥 ACTIVE TRADES")
        self.active_frame = ctk.CTkFrame(c, fg_color=CARD, corner_radius=8,
                                           border_width=1, border_color=BD, height=100)
        self.active_frame.pack(fill="x", pady=2)
        self.active_frame.pack_propagate(False)
        self.lbl_no_trades = ctk.CTkLabel(self.active_frame, text="No active trades",
                                           font=FL, text_color=DIM)
        self.lbl_no_trades.pack(expand=True)

        # API usage
        self._sec(c, "📡 API USAGE")
        self.rate_frame = ctk.CTkFrame(c, fg_color=CARD, corner_radius=8,
                                         border_width=1, border_color=BD)
        self.rate_frame.pack(fill="x", pady=2)
        rf = ctk.CTkFrame(self.rate_frame, fg_color="transparent")
        rf.pack(fill="x", padx=8, pady=5)
        self.lbl_rates = {}
        for label in ["Orders/sec", "Orders/min", "Orders/hr", "Orders/day",
                       "NIFTY trades", "SENSEX trades"]:
            f = ctk.CTkFrame(rf, fg_color="transparent"); f.pack(side="left", expand=True)
            ctk.CTkLabel(f, text=label, font=FN, text_color=DIM).pack()
            lbl = ctk.CTkLabel(f, text="0", font=FM, text_color=TXT)
            lbl.pack()
            self.lbl_rates[label] = lbl

    # ─── TRADE LOG ───

    def _build_trade_log(self):
        r = self.right
        ctk.CTkLabel(r, text="📋 TRADE LOG", font=FH,
                     text_color=ACC).pack(pady=(8, 4), padx=10, anchor="w")
        self.trade_log = ctk.CTkTextbox(r, fg_color="#fafbfc", font=FX, text_color=TXT,
                                          border_width=1, border_color=BD, corner_radius=6,
                                          wrap="word", state="disabled")
        self.trade_log.pack(fill="both", expand=True, padx=6, pady=(0, 6))

    # ─── BOTTOM ───

    def _build_bottom(self):
        bot = ctk.CTkFrame(self, fg_color=CARD, height=28, corner_radius=0,
                            border_width=1, border_color=BD)
        bot.pack(fill="x", side="bottom"); bot.pack_propagate(False)
        self.lbl_pkts = ctk.CTkLabel(bot, text="Packets: 0", font=FN, text_color=DIM)
        self.lbl_pkts.pack(side="left", padx=12)
        ctk.CTkLabel(bot, text="Balfund Trading Pvt Ltd  •  www.balfund.com",
                     font=FN, text_color=DIM).pack(side="right", padx=12)
        self.lbl_logf = ctk.CTkLabel(bot, text=f"Log: {log_file}", font=FN, text_color=DIM)
        self.lbl_logf.pack(side="right", padx=12)

    # ═════════════════════════════════════════════════════════════
    # HELPERS
    # ═════════════════════════════════════════════════════════════

    def _sec(self, parent, text):
        f = ctk.CTkFrame(parent, fg_color="transparent")
        f.pack(fill="x", padx=10, pady=(10, 2))
        ctk.CTkLabel(f, text=text, font=FH, text_color=ACC).pack(anchor="w")
        ctk.CTkFrame(f, fg_color=BD, height=1).pack(fill="x", pady=(2, 0))

    def _entry(self, parent, label, show=None):
        f = ctk.CTkFrame(parent, fg_color="transparent")
        f.pack(fill="x", padx=14, pady=3)
        ctk.CTkLabel(f, text=label, font=FM, text_color=TXT,
                     width=95, anchor="w").pack(side="left")
        e = ctk.CTkEntry(f, font=FS, fg_color="#fafbfc", border_color=BD,
                          text_color=TXT, height=30, show=show or "")
        e.pack(side="left", fill="x", expand=True)
        return e

    def _gentry(self, parent, label, default, row):
        ctk.CTkLabel(parent, text=label, font=FM, text_color=TXT,
                     width=110, anchor="w").grid(row=row, column=0, padx=2, pady=2, sticky="w")
        e = ctk.CTkEntry(parent, font=FS, fg_color="#fafbfc", border_color=BD,
                          text_color=TXT, height=28, width=80)
        e.grid(row=row, column=1, padx=2, pady=2, sticky="e")
        e.insert(0, default)
        return e

    def _load_env(self):
        self.ent_cid.insert(0, os.getenv("DHAN_CLIENT_ID", ""))
        self.ent_pin.insert(0, os.getenv("DHAN_PIN", ""))
        self.ent_totp.insert(0, os.getenv("DHAN_TOTP_SECRET", ""))

    def _tick_clock(self):
        self.lbl_clock.configure(text=now_ist().strftime("%H:%M:%S"))
        self.after(1000, self._tick_clock)

    def _log(self, msg):
        ts = now_ist().strftime("%H:%M:%S")
        self.trade_log.configure(state="normal")
        self.trade_log.insert("end", f"[{ts}] {msg}\n")
        self.trade_log.see("end")
        self.trade_log.configure(state="disabled")

    # ═════════════════════════════════════════════════════════════
    # ENGINE EVENTS
    # ═════════════════════════════════════════════════════════════

    def _on_engine_event(self, event, data):
        self.after(0, self._handle, event, data)

    def _handle(self, ev, d):
        if ev == "status":
            self.lbl_status.configure(text=f"● {d.get('msg','')}", text_color=YEL)
            self._log(d.get("msg", ""))

        elif ev == "error":
            self.lbl_status.configure(text="● ERROR", text_color=RED)
            self._log(f"ERROR  {d.get('msg','')}")

        elif ev == "ws_connected":
            n = d.get("instruments", 0)
            self.lbl_ws.configure(text=f"WS ● {n}", text_color=GRN)
            self.lbl_status.configure(text="● LIVE", text_color=GRN)
            self._log(f"WebSocket connected — {n} instruments")

        elif ev == "ws_disconnected":
            self.lbl_ws.configure(text="WS ✗", text_color=RED)

        elif ev == "spot":
            idx = d.get("index", "")
            if idx in self.spot_cards:
                self.spot_cards[idx]["spot"].configure(text=f"{d.get('spot',0):.2f}")
                self.spot_cards[idx]["expiry"].configure(text=f"Exp: {d.get('expiry','—')}")

        elif ev == "spot_tick":
            idx = d.get("index", "")
            if idx in self.spot_cards:
                self.spot_cards[idx]["spot"].configure(text=f"{d.get('ltp',0):.2f}")

        elif ev == "indicator":
            sid = d.get("sec_id", "")
            label = d.get("label", "")
            if sid not in self.ind_rows:
                row = ctk.CTkFrame(self.ind_frame, fg_color="transparent")
                row.pack(fill="x")
                lbls = {}
                for k, w in [("name", 180), ("ltp", 70), ("ef", 70),
                              ("es", 70), ("adx", 55), ("atr", 55), ("sig", 65)]:
                    l = ctk.CTkLabel(row, text="—", font=FL, text_color=TXT, width=w, anchor="w")
                    l.pack(side="left", padx=4, pady=2)
                    lbls[k] = l
                self.ind_rows[sid] = lbls
            r = self.ind_rows[sid]
            r["name"].configure(text=label)
            r["ltp"].configure(text=f"₹{d.get('ltp',0):.2f}")
            ef = d.get("ema_fast"); es = d.get("ema_slow")
            r["ef"].configure(text=f"{ef}" if ef else "...")
            r["es"].configure(text=f"{es}" if es else "...")
            r["adx"].configure(text=f"{d.get('adx','...')}")
            r["atr"].configure(text=f"{d.get('atr','...')}")
            is_sig = (ef and es and ef > es and (d.get("adx") or 0) > 20)
            r["sig"].configure(text="✓ SIGNAL" if is_sig else "—",
                                text_color=GRN if is_sig else DIM)

        elif ev == "entry":
            icon = "📝" if d.get("paper") else "🔴"
            self._log(f"{icon} ENTRY  {d.get('de','')} {d.get('index','')} "
                      f"{d.get('side','')}  Strike={d.get('strike','')}  "
                      f"@ ₹{d.get('price',0):.2f}  Qty={d.get('qty','')}")

        elif ev == "lot_exit":
            self._log(f"   L{d.get('lot','')} EXIT  @ ₹{d.get('price',0):.2f}  "
                      f"({d.get('reason','')})  PnL=₹{d.get('pnl',0):+.0f}")

        elif ev == "trade_closed":
            pnl = d.get("pnl", 0)
            icon = "✅" if pnl >= 0 else "❌"
            self._log(f"{icon} CLOSED  {d.get('de','')} {d.get('index','')}  PnL=₹{pnl:+.0f}")

        elif ev == "tick_update":
            for row in d.get("trackers", []):
                key = row["key"]
                if key in self.de_cards:
                    pnl = row["pnl"]
                    col = GRN if pnl >= 0 else RED
                    self.de_cards[key]["pnl"].configure(text=f"₹{pnl:+,.0f}", text_color=col)
                    st = "Stopped" if row["stopped"] else "Active"
                    lk = f"  Lock=₹{row['lock']:.0f}" if row.get("lock", 0) > 0 else ""
                    self.de_cards[key]["info"].configure(text=f"{row['trades']} trades | {st}{lk}")

            total = d.get("total_pnl", 0)
            self.lbl_pnl.configure(text=f"₹{total:+,.0f}",
                                    text_color=GRN if total >= 0 else RED)

            self.lbl_pkts.configure(text=f"Packets: {d.get('packets', 0):,}")

            rl = d.get("rate_limits", {})
            it = d.get("index_trades", {})
            for k, v in {
                "Orders/sec": rl.get("per_sec", 0), "Orders/min": rl.get("per_min", 0),
                "Orders/hr": rl.get("per_hr", 0), "Orders/day": rl.get("per_day", 0),
                "NIFTY trades": it.get("NIFTY", 0), "SENSEX trades": it.get("SENSEX", 0),
            }.items():
                if k in self.lbl_rates:
                    self.lbl_rates[k].configure(text=str(v))

        elif ev == "eod":
            self._log("═" * 45)
            self._log(f"EOD SUMMARY — Total: ₹{d.get('total_pnl',0):+,.0f}")
            self._log("═" * 45)

    # ═════════════════════════════════════════════════════════════
    # START / STOP
    # ═════════════════════════════════════════════════════════════

    def _on_start(self):
        if self.is_running: return
        cid  = self.ent_cid.get().strip()
        pin  = self.ent_pin.get().strip()
        totp = self.ent_totp.get().strip()
        if not cid or not pin or not totp:
            self._log("ERROR  Fill in all credentials"); return

        set_key(str(ENV_FILE), "DHAN_CLIENT_ID", cid)
        set_key(str(ENV_FILE), "DHAN_PIN", pin)
        set_key(str(ENV_FILE), "DHAN_TOTP_SECRET", totp)

        self.btn_start.configure(state="disabled")
        self.btn_stop.configure(state="normal")
        self.lbl_status.configure(text="● CONNECTING...", text_color=YEL)

        def _run():
            try:
                set_credentials(cid, pin, totp, os.getenv("DHAN_ACCESS_TOKEN", ""))
                self.after(0, self._log, "Authenticating...")
                init_credentials(
                    status_cb=lambda msg: self.after(0, self._log, f"  {msg}")
                )
                self.after(0, self._log, "Token ready")
                cfg = self._build_config()
                self.engine = BullockCart17Engine(cfg, gui_callback=self._on_engine_event)
                self.is_running = True
                self.engine.run()
            except Exception as e:
                self.after(0, self._log, f"ERROR  Start failed: {e}")
                self.after(0, lambda: self.btn_start.configure(state="normal"))
                self.after(0, lambda: self.btn_stop.configure(state="disabled"))
                self.after(0, lambda: self.lbl_status.configure(text="● ERROR", text_color=RED))

        threading.Thread(target=_run, daemon=True).start()

    def _on_stop(self):
        if self.engine: self.engine.stop()
        self.is_running = False
        self.btn_start.configure(state="normal")
        self.btn_stop.configure(state="disabled")
        self.lbl_status.configure(text="● STOPPED", text_color=RED)
        self._log("Engine stopped")

    def _build_config(self) -> StrategyConfig:
        def _f(e, d=0.0):
            try: return float(e.get().strip())
            except: return d
        def _i(e, d=0):
            try: return int(e.get().strip())
            except: return d

        return StrategyConfig(
            paper_mode=self.var_paper.get(),
            ema_fast=_i(self.ent_ef, 9), ema_slow=_i(self.ent_es, 15),
            adx_period=_i(self.ent_adp, 5), adx_smoothing=_i(self.ent_adp, 5),
            adx_threshold=_f(self.ent_adt, 20.0), atr_period=_i(self.ent_atr, 14),
            target_premium=_f(self.ent_tp, 50.0),
            premium_range_low=_f(self.ent_prl, 40.0),
            premium_range_high=_f(self.ent_prh, 65.0),
            de1_day_target_per_index=_f(self.ent_d1t, 6000.0),
            de1_day_sl_per_index=_f(self.ent_d1s, 1500.0),
            de2_day_sl_per_index=_f(self.ent_d2s, 1500.0),
            de2_safety_line=_f(self.ent_d2l, 2000.0),
            logic_vikalp_enabled=self.logic_vars["logic_vikalp_enabled"].get(),
            logic_epel_enabled=self.logic_vars["logic_epel_enabled"].get(),
            logic_exit_plus1_enabled=self.logic_vars["logic_exit_plus1_enabled"].get(),
            logic_vivek_enabled=self.logic_vars["logic_vivek_enabled"].get(),
            logic_kuber_enabled=self.logic_vars["logic_kuber_enabled"].get(),
            logic_hanuman_tail_enabled=self.logic_vars["logic_hanuman_tail_enabled"].get(),
            logic_tiered_risk_enabled=self.logic_vars["logic_tiered_risk_enabled"].get(),
            trade_start_time=self.ent_ts.get().strip() or "09:16",
            trade_end_time=self.ent_te.get().strip() or "15:15",
            cooldown_minutes=_i(self.ent_cd, 5),
            max_sl_per_trade=_f(self.ent_msl, 25.0),
            max_trades_per_day_per_index=_i(self.ent_mxt, 30),
        )


if __name__ == "__main__":
    app = BullockCartApp()
    app.mainloop()
