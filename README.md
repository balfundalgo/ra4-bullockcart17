# ⚡ RA4 Bullock Cart 17  v3

**Balfund Trading Pvt Ltd** — NIFTY + SENSEX Options Scalper

Dual-engine strategy with 7 smart logics, 4-lot phased exits, Dhan API rate limiter, and real-time WebSocket v2 execution.

## v3 Changes

- **Dhan API rate limiter** — Order: 8/sec, 200/min, 900/hr, 6500/day (80% safety margin)
- **Single candle single entry per INDEX** — fixes excessive trade count
- **SELL order on exit** — positions actually close on the broker now
- **Max 30 trades/day/index** — hard safety cap
- **Lot 4 safety gates** — exit at +1 or entry price fallback
- **EPEL scratch pause** — stops after 3 consecutive scratches
- **429 exponential backoff** — graceful rate limit recovery

## Architecture

```
DE1 (Profit Cap ₹6k/index/day)  ─┐
                                  ├── NIFTY + SENSEX × CE + PE
DE2 (No Cap, Profit Lock)  ──────┘
```

## Setup

```bash
git clone https://github.com/balfundalgo/ra4-bullockcart17.git
cd ra4-bullockcart17
pip install -r requirements.txt
cp .env.example .env   # fill in credentials
python app.py           # GUI mode
python engine.py        # Console mode
```

## Project Structure

```
ra4-bullockcart17/
├── app.py              # CustomTkinter GUI (588 lines)
├── engine.py           # Core strategy engine v3 (2100 lines)
├── requirements.txt
├── .env.example
├── .gitignore
├── logs/               # Daily rotating logs with full detail
└── .github/workflows/
    └── build.yml       # PyInstaller Windows EXE CI/CD
```

**Balfund Trading Pvt Ltd** | www.balfund.com
