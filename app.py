"""
RA4 Bullock Cart 17 — CustomTkinter GUI
════════════════════════════════════════
Dark scalper theme | Live dashboard | Real-time trade log
"""

import os
import sys
import json
import time
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import customtkinter as ctk
from dotenv import load_dotenv, set_key

# Local import
from engine import (
    BullockCart17Engine, StrategyConfig, LotConfig, init_credentials,
    DEMode, Side, now_ist
)

# ═══════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
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
# THEME CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════

BG_DARK       = "#0a0e17"       # Deep navy
BG_CARD       = "#111827"       # Card background
BG_CARD_ALT   = "#0f1629"       # Alternate card
BORDER_COLOR  = "#1e293b"       # Subtle borders
ACCENT_GREEN  = "#00e676"       # Profit / buy green
ACCENT_RED    = "#ff1744"       # Loss / sell red
ACCENT_GOLD   = "#ffd740"       # Highlights / warnings
ACCENT_CYAN   = "#00bcd4"       # Info / headers
ACCENT_BLUE   = "#2962ff"       # Primary buttons
TEXT_PRIMARY   = "#e2e8f0"       # Main text
TEXT_SECONDARY = "#64748b"       # Dim text
TEXT_MUTED     = "#334155"       # Very dim

FONT_FAMILY   = "Consolas"
FONT_MONO     = ("Consolas", 11)
FONT_SMALL    = ("Consolas", 10)
FONT_HEADER   = ("Consolas", 13, "bold")
FONT_TITLE    = ("Consolas", 18, "bold")
FONT_BIG      = ("Consolas", 28, "bold")
FONT_LABEL    = ("Consolas", 11)

# ═══════════════════════════════════════════════════════════════════════════
# ENV FILE MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).parent if "__file__" in globals() else Path.cwd()
ENV_FILE = BASE_DIR / ".env"

def ensure_env():
    if not ENV_FILE.exists():
        ENV_FILE.write_text(
            "DHAN_CLIENT_ID=\nDHAN_PIN=\nDHAN_TOTP_SECRET=\nDHAN_ACCESS_TOKEN=\n"
        )
    load_dotenv(str(ENV_FILE), override=True)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN APPLICATION
# ═══════════════════════════════════════════════════════════════════════════

class BullockCartApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        ensure_env()

        self.title("RA4 Bullock Cart 17 — Balfund Trading")
        self.geometry("1440x900")
        self.minsize(1200, 750)
        self.configure(fg_color=BG_DARK)

        ctk.set_appearance_mode("dark")

        self.engine: Optional[BullockCart17Engine] = None
        self.is_running = False

        self._build_ui()
        self._load_env_to_fields()

    # ─────────────── BUILD UI ───────────────

    def _build_ui(self):
        # ── TOP BAR ──
        self.top_bar = ctk.CTkFrame(self, fg_color=BG_CARD, height=60, corner_radius=0)
        self.top_bar.pack(fill="x", padx=0, pady=0)
        self.top_bar.pack_propagate(False)

        ctk.CTkLabel(self.top_bar, text="⚡ RA4 BULLOCK CART 17",
                     font=FONT_TITLE, text_color=ACCENT_CYAN).pack(side="left", padx=20)

        self.lbl_clock = ctk.CTkLabel(self.top_bar, text="--:--:--",
                                       font=("Consolas", 14), text_color=TEXT_SECONDARY)
        self.lbl_clock.pack(side="right", padx=20)

        self.lbl_status = ctk.CTkLabel(self.top_bar, text="● IDLE",
                                        font=("Consolas", 12, "bold"), text_color=ACCENT_GOLD)
        self.lbl_status.pack(side="right", padx=20)

        self.lbl_ws = ctk.CTkLabel(self.top_bar, text="WS: —",
                                    font=FONT_SMALL, text_color=TEXT_SECONDARY)
        self.lbl_ws.pack(side="right", padx=15)

        # ── MAIN BODY ──
        self.body = ctk.CTkFrame(self, fg_color=BG_DARK)
        self.body.pack(fill="both", expand=True, padx=10, pady=5)

        # LEFT PANEL (Settings)
        self.left_panel = ctk.CTkScrollableFrame(self.body, fg_color=BG_CARD,
                                                   width=340, corner_radius=8,
                                                   border_width=1, border_color=BORDER_COLOR)
        self.left_panel.pack(side="left", fill="y", padx=(0, 5), pady=0)

        # CENTER PANEL (Dashboard)
        self.center_panel = ctk.CTkFrame(self.body, fg_color=BG_DARK)
        self.center_panel.pack(side="left", fill="both", expand=True, padx=5, pady=0)

        # RIGHT PANEL (Trade Log)
        self.right_panel = ctk.CTkFrame(self.body, fg_color=BG_CARD,
                                         width=320, corner_radius=8,
                                         border_width=1, border_color=BORDER_COLOR)
        self.right_panel.pack(side="right", fill="y", padx=(5, 0), pady=0)
        self.right_panel.pack_propagate(False)

        self._build_settings_panel()
        self._build_dashboard()
        self._build_trade_log()
        self._build_bottom_bar()

        # Clock updater
        self._update_clock()

    # ─── SETTINGS PANEL ───

    def _build_settings_panel(self):
        p = self.left_panel

        # ── Credentials ──
        self._section_header(p, "🔐 CREDENTIALS")

        self.ent_client_id = self._labeled_entry(p, "Client ID")
        self.ent_pin = self._labeled_entry(p, "PIN", show="•")
        self.ent_totp = self._labeled_entry(p, "TOTP Secret", show="•")

        # ── Mode ──
        self._section_header(p, "⚙️ MODE")

        self.var_paper = ctk.BooleanVar(value=True)
        self.chk_paper = ctk.CTkSwitch(p, text="Paper Trading", font=FONT_LABEL,
                                         variable=self.var_paper,
                                         fg_color=TEXT_MUTED, progress_color=ACCENT_GOLD,
                                         text_color=TEXT_PRIMARY)
        self.chk_paper.pack(anchor="w", padx=15, pady=5)

        # ── Indicators ──
        self._section_header(p, "📊 INDICATORS")
        frm_ind = ctk.CTkFrame(p, fg_color="transparent")
        frm_ind.pack(fill="x", padx=15, pady=2)

        self.ent_ema_fast = self._mini_entry(frm_ind, "EMA Fast", "9", row=0)
        self.ent_ema_slow = self._mini_entry(frm_ind, "EMA Slow", "15", row=1)
        self.ent_adx_period = self._mini_entry(frm_ind, "ADX Period", "5", row=2)
        self.ent_adx_thresh = self._mini_entry(frm_ind, "ADX Thresh", "20", row=3)
        self.ent_atr_period = self._mini_entry(frm_ind, "ATR Period", "14", row=4)

        # ── Strike Selection ──
        self._section_header(p, "🎯 STRIKE SELECTION")
        frm_strike = ctk.CTkFrame(p, fg_color="transparent")
        frm_strike.pack(fill="x", padx=15, pady=2)

        self.ent_target_prem = self._mini_entry(frm_strike, "Target ₹", "50", row=0)
        self.ent_prem_low = self._mini_entry(frm_strike, "Range Low", "40", row=1)
        self.ent_prem_high = self._mini_entry(frm_strike, "Range High", "65", row=2)

        # ── DE1 / DE2 ──
        self._section_header(p, "🏎 DE1 (PROFIT CAP)")
        frm_de1 = ctk.CTkFrame(p, fg_color="transparent")
        frm_de1.pack(fill="x", padx=15, pady=2)
        self.ent_de1_target = self._mini_entry(frm_de1, "Day Target ₹", "6000", row=0)
        self.ent_de1_sl = self._mini_entry(frm_de1, "Day SL ₹", "1500", row=1)

        self._section_header(p, "🚀 DE2 (NO CAP)")
        frm_de2 = ctk.CTkFrame(p, fg_color="transparent")
        frm_de2.pack(fill="x", padx=15, pady=2)
        self.ent_de2_sl = self._mini_entry(frm_de2, "Day SL ₹", "1500", row=0)
        self.ent_de2_safety = self._mini_entry(frm_de2, "Safety Line ₹", "2000", row=1)

        # ── Timing ──
        self._section_header(p, "⏰ TIMING")
        frm_time = ctk.CTkFrame(p, fg_color="transparent")
        frm_time.pack(fill="x", padx=15, pady=2)
        self.ent_start = self._mini_entry(frm_time, "Start", "09:16", row=0)
        self.ent_end = self._mini_entry(frm_time, "End", "15:15", row=1)
        self.ent_cooldown = self._mini_entry(frm_time, "Cooldown min", "5", row=2)
        self.ent_max_sl = self._mini_entry(frm_time, "Max SL pts", "25", row=3)

        # ── Logic Toggles ──
        self._section_header(p, "🧠 SMART LOGICS")

        self.logic_vars = {}
        logic_names = [
            ("logic_vikalp", "Logic Vikalp (Strike Scanner)"),
            ("logic_epel", "Logic EPEL (Scratch Exit)"),
            ("logic_exit_plus1", "Logic Exit+1 (Safety Exit)"),
            ("logic_vivek", "Logic Vivek (Half-Move)"),
            ("logic_kuber", "Logic Kuber (Profit Vault)"),
            ("logic_hanuman_tail", "Logic Hanuman Tail (Trail Cap)"),
            ("logic_tiered_risk", "Logic Tiered Risk (Scaling)"),
        ]
        for key, label in logic_names:
            var = ctk.BooleanVar(value=True)
            self.logic_vars[key] = var
            sw = ctk.CTkSwitch(p, text=label, font=FONT_SMALL, variable=var,
                                fg_color=TEXT_MUTED, progress_color=ACCENT_CYAN,
                                text_color=TEXT_PRIMARY, width=40)
            sw.pack(anchor="w", padx=15, pady=3)

        # ── Buttons ──
        ctk.CTkFrame(p, fg_color="transparent", height=15).pack()

        self.btn_start = ctk.CTkButton(
            p, text="▶  START ENGINE", font=("Consolas", 14, "bold"),
            fg_color=ACCENT_BLUE, hover_color="#1e40af", height=45,
            corner_radius=6, command=self._on_start)
        self.btn_start.pack(fill="x", padx=15, pady=5)

        self.btn_stop = ctk.CTkButton(
            p, text="■  STOP ENGINE", font=("Consolas", 14, "bold"),
            fg_color=ACCENT_RED, hover_color="#b71c1c", height=45,
            corner_radius=6, command=self._on_stop, state="disabled")
        self.btn_stop.pack(fill="x", padx=15, pady=(0, 10))

    # ─── DASHBOARD ───

    def _build_dashboard(self):
        c = self.center_panel

        # ── TOTAL PNL ──
        self.pnl_frame = ctk.CTkFrame(c, fg_color=BG_CARD, height=100,
                                        corner_radius=8, border_width=1,
                                        border_color=BORDER_COLOR)
        self.pnl_frame.pack(fill="x", pady=(0, 5))
        self.pnl_frame.pack_propagate(False)

        ctk.CTkLabel(self.pnl_frame, text="TOTAL DAY P&L",
                     font=FONT_SMALL, text_color=TEXT_SECONDARY).pack(pady=(10, 0))
        self.lbl_total_pnl = ctk.CTkLabel(self.pnl_frame, text="₹ 0",
                                            font=FONT_BIG, text_color=TEXT_PRIMARY)
        self.lbl_total_pnl.pack()

        # ── SPOTS ROW ──
        spots_frame = ctk.CTkFrame(c, fg_color="transparent")
        spots_frame.pack(fill="x", pady=5)

        self.spot_cards = {}
        for idx in ["NIFTY", "SENSEX"]:
            card = ctk.CTkFrame(spots_frame, fg_color=BG_CARD, corner_radius=8,
                                 border_width=1, border_color=BORDER_COLOR)
            card.pack(side="left", fill="x", expand=True, padx=(0 if idx == "NIFTY" else 3, 3 if idx == "NIFTY" else 0))

            ctk.CTkLabel(card, text=idx, font=FONT_HEADER,
                         text_color=ACCENT_CYAN).pack(pady=(8, 0))

            lbl_spot = ctk.CTkLabel(card, text="—", font=("Consolas", 20, "bold"),
                                     text_color=TEXT_PRIMARY)
            lbl_spot.pack()

            lbl_exp = ctk.CTkLabel(card, text="Expiry: —", font=FONT_SMALL,
                                    text_color=TEXT_SECONDARY)
            lbl_exp.pack(pady=(0, 8))

            self.spot_cards[idx] = {"spot": lbl_spot, "expiry": lbl_exp}

        # ── DE TRACKER CARDS ──
        de_frame = ctk.CTkFrame(c, fg_color="transparent")
        de_frame.pack(fill="x", pady=5)

        self.de_cards = {}
        for key in ["DE1_NIFTY", "DE1_SENSEX", "DE2_NIFTY", "DE2_SENSEX"]:
            card = ctk.CTkFrame(de_frame, fg_color=BG_CARD, corner_radius=8,
                                 border_width=1, border_color=BORDER_COLOR)
            card.pack(side="left", fill="x", expand=True, padx=2)

            de_label = key.replace("_", " ")
            ctk.CTkLabel(card, text=de_label, font=FONT_SMALL,
                         text_color=TEXT_SECONDARY).pack(pady=(6, 0))

            lbl_pnl = ctk.CTkLabel(card, text="₹ 0", font=("Consolas", 16, "bold"),
                                    text_color=TEXT_PRIMARY)
            lbl_pnl.pack()

            lbl_info = ctk.CTkLabel(card, text="0 trades | Active",
                                     font=("Consolas", 9), text_color=TEXT_SECONDARY)
            lbl_info.pack(pady=(0, 6))

            self.de_cards[key] = {"pnl": lbl_pnl, "info": lbl_info}

        # ── INDICATOR TABLE ──
        self._section_header(c, "📈 LIVE INDICATORS")

        self.ind_frame = ctk.CTkFrame(c, fg_color=BG_CARD, corner_radius=8,
                                        border_width=1, border_color=BORDER_COLOR)
        self.ind_frame.pack(fill="x", pady=3)

        # Table header
        hdr = ctk.CTkFrame(self.ind_frame, fg_color=BG_CARD_ALT, corner_radius=0)
        hdr.pack(fill="x")
        cols = ["Option", "LTP", "EMA 9", "EMA 15", "ADX", "Signal"]
        widths = [200, 80, 80, 80, 65, 70]
        for col, w in zip(cols, widths):
            ctk.CTkLabel(hdr, text=col, font=FONT_SMALL, text_color=TEXT_SECONDARY,
                         width=w, anchor="w").pack(side="left", padx=5, pady=4)

        self.ind_rows: Dict[str, dict] = {}

        # ── ACTIVE TRADES ──
        self._section_header(c, "🔥 ACTIVE TRADES")

        self.active_frame = ctk.CTkFrame(c, fg_color=BG_CARD, corner_radius=8,
                                           border_width=1, border_color=BORDER_COLOR,
                                           height=120)
        self.active_frame.pack(fill="x", pady=3)
        self.active_frame.pack_propagate(False)

        self.lbl_no_trades = ctk.CTkLabel(self.active_frame,
                                           text="No active trades",
                                           font=FONT_SMALL, text_color=TEXT_MUTED)
        self.lbl_no_trades.pack(expand=True)

    # ─── TRADE LOG ───

    def _build_trade_log(self):
        r = self.right_panel

        ctk.CTkLabel(r, text="📋 TRADE LOG", font=FONT_HEADER,
                     text_color=ACCENT_CYAN).pack(pady=(10, 5), padx=10, anchor="w")

        self.trade_log = ctk.CTkTextbox(r, fg_color=BG_CARD_ALT, font=FONT_SMALL,
                                          text_color=TEXT_PRIMARY,
                                          border_width=1, border_color=BORDER_COLOR,
                                          corner_radius=6, wrap="word",
                                          state="disabled")
        self.trade_log.pack(fill="both", expand=True, padx=8, pady=(0, 8))

    # ─── BOTTOM BAR ───

    def _build_bottom_bar(self):
        self.bottom_bar = ctk.CTkFrame(self, fg_color=BG_CARD, height=30, corner_radius=0)
        self.bottom_bar.pack(fill="x", side="bottom")
        self.bottom_bar.pack_propagate(False)

        self.lbl_packets = ctk.CTkLabel(self.bottom_bar, text="Packets: 0",
                                         font=("Consolas", 9), text_color=TEXT_SECONDARY)
        self.lbl_packets.pack(side="left", padx=15)

        ctk.CTkLabel(self.bottom_bar, text="Balfund Trading Pvt Ltd | www.balfund.com",
                     font=("Consolas", 9), text_color=TEXT_MUTED).pack(side="right", padx=15)

        self.lbl_info = ctk.CTkLabel(self.bottom_bar, text="",
                                      font=("Consolas", 9), text_color=TEXT_SECONDARY)
        self.lbl_info.pack(side="right", padx=15)

    # ─────────────── HELPER WIDGETS ───────────────

    def _section_header(self, parent, text):
        frm = ctk.CTkFrame(parent, fg_color="transparent")
        frm.pack(fill="x", padx=10, pady=(12, 3))
        ctk.CTkLabel(frm, text=text, font=FONT_HEADER,
                     text_color=ACCENT_CYAN).pack(anchor="w")
        ctk.CTkFrame(frm, fg_color=BORDER_COLOR, height=1).pack(fill="x", pady=(2, 0))

    def _labeled_entry(self, parent, label, show=None):
        frm = ctk.CTkFrame(parent, fg_color="transparent")
        frm.pack(fill="x", padx=15, pady=3)
        ctk.CTkLabel(frm, text=label, font=FONT_SMALL,
                     text_color=TEXT_SECONDARY, width=90, anchor="w").pack(side="left")
        ent = ctk.CTkEntry(frm, font=FONT_MONO, fg_color=BG_CARD_ALT,
                            border_color=BORDER_COLOR, text_color=TEXT_PRIMARY,
                            height=30, show=show or "")
        ent.pack(side="left", fill="x", expand=True)
        return ent

    def _mini_entry(self, parent, label, default, row):
        ctk.CTkLabel(parent, text=label, font=FONT_SMALL,
                     text_color=TEXT_SECONDARY, width=100, anchor="w"
                     ).grid(row=row, column=0, padx=2, pady=2, sticky="w")
        ent = ctk.CTkEntry(parent, font=FONT_MONO, fg_color=BG_CARD_ALT,
                            border_color=BORDER_COLOR, text_color=TEXT_PRIMARY,
                            height=28, width=80)
        ent.grid(row=row, column=1, padx=2, pady=2, sticky="e")
        ent.insert(0, default)
        return ent

    def _load_env_to_fields(self):
        self.ent_client_id.insert(0, os.getenv("DHAN_CLIENT_ID", ""))
        self.ent_pin.insert(0, os.getenv("DHAN_PIN", ""))
        self.ent_totp.insert(0, os.getenv("DHAN_TOTP_SECRET", ""))

    def _update_clock(self):
        self.lbl_clock.configure(text=now_ist().strftime("%H:%M:%S"))
        self.after(1000, self._update_clock)

    # ─────────────── LOG TO GUI ───────────────

    def _log(self, msg: str, color: str = TEXT_PRIMARY):
        ts = now_ist().strftime("%H:%M:%S")
        self.trade_log.configure(state="normal")
        self.trade_log.insert("end", f"[{ts}] {msg}\n")
        self.trade_log.see("end")
        self.trade_log.configure(state="disabled")

    # ─────────────── ENGINE CALLBACKS (thread-safe) ───────────────

    def _on_engine_event(self, event: str, data: dict):
        """Called from engine threads — schedule on main thread."""
        self.after(0, self._handle_event, event, data)

    def _handle_event(self, event: str, data: dict):
        if event == "status":
            self.lbl_status.configure(text=f"● {data.get('msg', '')}")
            self._log(data.get("msg", ""))

        elif event == "error":
            self.lbl_status.configure(text=f"● ERROR", text_color=ACCENT_RED)
            self._log(f"❌ {data.get('msg', '')}")

        elif event == "ws_connected":
            self.lbl_ws.configure(text=f"WS: ✓ {data.get('instruments', 0)} instr",
                                   text_color=ACCENT_GREEN)
            self.lbl_status.configure(text="● LIVE", text_color=ACCENT_GREEN)
            self._log(f"WebSocket connected — {data.get('instruments', 0)} instruments")

        elif event == "ws_disconnected":
            self.lbl_ws.configure(text="WS: ✗ Disconnected", text_color=ACCENT_RED)

        elif event == "spot":
            idx = data.get("index", "")
            if idx in self.spot_cards:
                self.spot_cards[idx]["spot"].configure(text=f"{data.get('spot', 0):.2f}")
                self.spot_cards[idx]["expiry"].configure(text=f"Expiry: {data.get('expiry', '—')}")

        elif event == "spot_tick":
            idx = data.get("index", "")
            if idx in self.spot_cards:
                self.spot_cards[idx]["spot"].configure(text=f"{data.get('ltp', 0):.2f}")

        elif event == "strike":
            self._log(f"Strike: {data.get('index','')} {data.get('side','')} "
                      f"= {data.get('strike','')} (₹{data.get('ltp', 0):.2f})")

        elif event == "indicator":
            sec_id = data.get("sec_id", "")
            label = data.get("label", "")
            if sec_id not in self.ind_rows:
                row = ctk.CTkFrame(self.ind_frame, fg_color="transparent")
                row.pack(fill="x")
                lbls = {}
                cols_w = [("name", 200), ("ltp", 80), ("ef", 80),
                          ("es", 80), ("adx", 65), ("sig", 70)]
                for k, w in cols_w:
                    l = ctk.CTkLabel(row, text="—", font=FONT_SMALL,
                                      text_color=TEXT_PRIMARY, width=w, anchor="w")
                    l.pack(side="left", padx=5, pady=2)
                    lbls[k] = l
                self.ind_rows[sec_id] = lbls

            r = self.ind_rows[sec_id]
            r["name"].configure(text=label)
            r["ltp"].configure(text=f"₹{data.get('ltp', 0):.2f}")
            ef = data.get("ema_fast")
            es = data.get("ema_slow")
            r["ef"].configure(text=f"{ef}" if ef else "...")
            r["es"].configure(text=f"{es}" if es else "...")
            r["adx"].configure(text=f"{data.get('adx', '...')}")
            is_signal = (ef and es and ef > es and (data.get("adx") or 0) > 20)
            r["sig"].configure(text="✓ SIGNAL" if is_signal else "—",
                                text_color=ACCENT_GREEN if is_signal else TEXT_MUTED)

        elif event == "entry":
            paper = "📝" if data.get("paper") else "🔴"
            self._log(f"{paper} ENTRY {data.get('de','')} {data.get('index','')} "
                      f"{data.get('side','')} @ ₹{data.get('price', 0):.2f} "
                      f"Strike={data.get('strike','')} Qty={data.get('qty','')}")

        elif event == "lot_exit":
            self._log(f"  ↳ L{data.get('lot','')} exit @ ₹{data.get('price', 0):.2f} "
                      f"({data.get('reason','')}) PnL=₹{data.get('pnl', 0):+.0f}")

        elif event == "trade_closed":
            pnl = data.get("pnl", 0)
            col_txt = "✅" if pnl >= 0 else "❌"
            self._log(f"{col_txt} CLOSED {data.get('de','')} {data.get('index','')} "
                      f"PnL=₹{pnl:+.0f}")

        elif event == "tick_update":
            # Update DE cards
            for row in data.get("trackers", []):
                key = row["key"]
                if key in self.de_cards:
                    pnl = row["pnl"]
                    col = ACCENT_GREEN if pnl >= 0 else ACCENT_RED
                    self.de_cards[key]["pnl"].configure(
                        text=f"₹{pnl:+,.0f}", text_color=col)
                    status = "Stopped" if row["stopped"] else "Active"
                    lock_info = f" Lock=₹{row['lock']:.0f}" if row.get("lock", 0) > 0 else ""
                    self.de_cards[key]["info"].configure(
                        text=f"{row['trades']} trades | {status}{lock_info}")

            # Total PnL
            total = data.get("total_pnl", 0)
            col = ACCENT_GREEN if total >= 0 else ACCENT_RED
            self.lbl_total_pnl.configure(text=f"₹{total:+,.0f}", text_color=col)

            # Packets
            self.lbl_packets.configure(text=f"Packets: {data.get('packets', 0):,}")

        elif event in ("kuber", "hanuman", "tiered", "de2_lock"):
            self._log(f"  🔒 {event.upper()}: {json.dumps(data)}")

        elif event == "eod":
            self._log("═" * 40)
            self._log(f"EOD SUMMARY — Total: ₹{data.get('total_pnl', 0):+,.0f}")
            self._log("═" * 40)

    # ─────────────── START / STOP ───────────────

    def _on_start(self):
        if self.is_running:
            return

        # Save credentials to .env
        client_id = self.ent_client_id.get().strip()
        pin = self.ent_pin.get().strip()
        totp_secret = self.ent_totp.get().strip()

        if not client_id or not pin or not totp_secret:
            self._log("❌ Please fill in all credentials")
            return

        set_key(str(ENV_FILE), "DHAN_CLIENT_ID", client_id)
        set_key(str(ENV_FILE), "DHAN_PIN", pin)
        set_key(str(ENV_FILE), "DHAN_TOTP_SECRET", totp_secret)

        self.btn_start.configure(state="disabled")
        self.btn_stop.configure(state="normal")
        self.lbl_status.configure(text="● CONNECTING...", text_color=ACCENT_GOLD)

        def _run():
            try:
                # Authenticate
                existing_token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
                self.after(0, self._log, "🔐 Authenticating...")
                token = init_credentials(
                    client_id, pin, totp_secret, existing_token, str(ENV_FILE),
                    status_cb=lambda msg: self.after(0, self._log, f"  {msg}")
                )
                self.after(0, self._log, "✅ Token ready")

                # Build config from UI
                config = self._build_config()

                # Create and start engine
                self.engine = BullockCart17Engine(config, gui_callback=self._on_engine_event)
                self.is_running = True
                self.engine.start()

            except Exception as e:
                self.after(0, self._log, f"❌ Start failed: {e}")
                self.after(0, self.btn_start.configure, {"state": "normal"})
                self.after(0, self.btn_stop.configure, {"state": "disabled"})
                self.after(0, self.lbl_status.configure,
                           {"text": "● ERROR", "text_color": ACCENT_RED})

        threading.Thread(target=_run, daemon=True).start()

    def _on_stop(self):
        if self.engine:
            self.engine.stop()
        self.is_running = False
        self.btn_start.configure(state="normal")
        self.btn_stop.configure(state="disabled")
        self.lbl_status.configure(text="● STOPPED", text_color=ACCENT_RED)
        self._log("■ Engine stopped")

    def _build_config(self) -> StrategyConfig:
        def _f(ent, default=0.0):
            try: return float(ent.get().strip())
            except: return default
        def _i(ent, default=0):
            try: return int(ent.get().strip())
            except: return default

        return StrategyConfig(
            paper_mode=self.var_paper.get(),
            ema_fast=_i(self.ent_ema_fast, 9),
            ema_slow=_i(self.ent_ema_slow, 15),
            adx_period=_i(self.ent_adx_period, 5),
            adx_smoothing=_i(self.ent_adx_period, 5),
            adx_threshold=_f(self.ent_adx_thresh, 20.0),
            atr_period=_i(self.ent_atr_period, 14),
            target_premium=_f(self.ent_target_prem, 50.0),
            premium_range_low=_f(self.ent_prem_low, 40.0),
            premium_range_high=_f(self.ent_prem_high, 65.0),
            de1_day_target=_f(self.ent_de1_target, 6000.0),
            de1_day_sl=_f(self.ent_de1_sl, 1500.0),
            de2_day_sl=_f(self.ent_de2_sl, 1500.0),
            de2_safety_line=_f(self.ent_de2_safety, 2000.0),
            logic_vikalp=self.logic_vars["logic_vikalp"].get(),
            logic_epel=self.logic_vars["logic_epel"].get(),
            logic_exit_plus1=self.logic_vars["logic_exit_plus1"].get(),
            logic_vivek=self.logic_vars["logic_vivek"].get(),
            logic_kuber=self.logic_vars["logic_kuber"].get(),
            logic_hanuman_tail=self.logic_vars["logic_hanuman_tail"].get(),
            logic_tiered_risk=self.logic_vars["logic_tiered_risk"].get(),
            trade_start=self.ent_start.get().strip() or "09:16",
            trade_end=self.ent_end.get().strip() or "15:15",
            cooldown_min=_i(self.ent_cooldown, 5),
            max_sl=_f(self.ent_max_sl, 25.0),
        )


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    app = BullockCartApp()
    app.mainloop()
