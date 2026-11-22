# Weekly R4 Breakout Scanner + Journal

Strategy: weekly close above yearly Camarilla R4 -> stop at weekly 20 SMA -> add a unit whenever the
weekly 20 SMA rises to the last entry price. Universe: NSE EQ stocks with market cap >= 5000 Cr.

## Setup
1. Create a GitHub repo and upload everything in this folder (keep the `.github/workflows` path).
2. Settings -> Actions -> General -> Workflow permissions -> **Read and write permissions**.
3. Settings -> Pages -> Source: *Deploy from a branch* -> `main` / `/docs`.
4. Actions tab -> run **Weekly R4 scan** manually once (first run builds the universe, ~10-20 min).
5. Dashboard: `https://<your-user>.github.io/<repo>/`

## Schedules (IST)
- Weekly scan: Saturday 10:00 -> new entries, pyramid adds, weekly-close exits
- Daily update: Mon-Fri 17:00 -> CMP, stoploss (live weekly SMA20), P/L, stoploss-hit log

## Optional Telegram alerts
Add repo secrets `TELEGRAM_TOKEN` and `TELEGRAM_CHAT_ID`.

## Files written by the bot
- `data/journal.csv`  all trades (open and closed)
- `data/events.csv`   ENTRY / PYRAMID / SL_HIT log
- `docs/index.html`   dashboard, `docs/alerts.md` today's alerts
