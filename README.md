# ⚡ RA4 Bullock Cart 17

**Balfund Trading Pvt Ltd** — NIFTY + SENSEX Options Scalper

Dual-engine strategy with 7 smart logics, 4-lot phased exits, and real-time WebSocket v2 execution via Dhan API.

---

## Architecture

```
DE1 (Profit Cap ₹6k/index/day) ──┐
                                  ├── NIFTY + SENSEX × CE + PE
DE2 (No Cap, Profit Lock)  ──────┘
```

**Entry:** 9 EMA > 15 EMA on option chart + ADX(5,5) > 20  
**Exit:** 4-lot phased — L1=+1pt, L2=+7.5pt/TSL3, L3=+15pt/TSL3, L4=trail TSL3  
**SL:** ATR(14) × 1.5  

### 7 Smart Logics

| Logic | Description |
|-------|-------------|
| Vikalp | Scans 5 nearest strikes for qualifying entry |
| EPEL | Scratch exit at entry if no move |
| Exit+1 | All lots exit at +1 on reversal |
| Vivek | Half-move exit for L2/L3 |
| Kuber | Profit vault — independent of day target |
| Hanuman Tail | Post-target trailing flex cap |
| Tiered Risk | Scaling locks at ₹12k/₹20k/₹30k |

---

## Setup

### 1. Clone & Install

```bash
git clone https://github.com/balfundalgo/ra4-bullockcart17.git
cd ra4-bullockcart17
pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your Dhan credentials
```

### 3. Run

```bash
python app.py
```

### 4. EXE Build (local)

```bash
pip install pyinstaller
pyinstaller --noconfirm --onefile --windowed --name "RA4_BullockCart17" --hidden-import customtkinter --hidden-import pyotp --hidden-import websocket --collect-all customtkinter app.py
```

The EXE also builds automatically via GitHub Actions on every push to `main`.

---

## Project Structure

```
ra4-bullockcart17/
├── app.py              # CustomTkinter GUI
├── engine.py           # Core strategy engine
├── requirements.txt    # Python dependencies
├── .env.example        # Credential template
├── .gitignore
├── assets/             # Icons, images
├── logs/               # Daily rotating logs
└── .github/workflows/
    └── build.yml       # PyInstaller CI/CD
```

---

## Token Management

3-tier automatic authentication:
1. **Verify** existing token from `.env`
2. **Renew** if expired but renewable
3. **Generate** fresh token via TOTP if both fail

---

## Data Flow

```
Dhan WebSocket v2 (binary)
  → tick (LTP + LTT)
    → CandleEngine (1-min buckets)
      → IndicatorEngine (EMA/ADX/ATR)
        → Entry signal check
    → Tick-level exit monitoring (EPEL, TSL, Vivek, Exit+1)
      → Profit management (Kuber, Hanuman, Tiered, DE2 Lock)
```

---

**Balfund Trading Pvt Ltd** | info@balfund.com | www.balfund.com
