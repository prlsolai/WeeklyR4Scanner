#!/usr/bin/env python3
"""
Weekly > Yearly Camarilla R4 scanner with trade journal (for GitHub Actions)

  python scanner.py weekly   # Saturday: full scan -> new entries, pyramid adds, weekly-close exits
  python scanner.py daily    # Mon-Fri evening: update CMP / SL / P&L, log stoploss hits

Strategy
  ENTRY    weekly close crosses above yearly R4  (R4 = prevC + (prevH - prevL) * 1.1 / 2)
  STOP     weekly 20 SMA (trailing)
  PYRAMID  when weekly 20 SMA rises to the last entry/add price -> add 1 unit at that weekly close
  EXIT     weekly 20 SMA breached: daily close below the (live) weekly SMA20 -> SL HIT
           (the weekly run also exits if a completed weekly close is below SMA20)
"""
import html
import io
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
import yfinance as yf

IST = timezone(timedelta(hours=5, minutes=30))
SMA_LEN = 20
MIN_MCAP_CR = float(os.getenv("MIN_MCAP_CR", "5000"))
UNIVERSE_MAX_AGE_DAYS = 30
NSE_LIST_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"

DATA, DOCS = "data", "docs"
JOURNAL = f"{DATA}/journal.csv"
EVENTS = f"{DATA}/events.csv"
UNIVERSE = f"{DATA}/universe.csv"
JCOLS = ["id", "symbol", "status", "r4", "entry_date", "legs", "units", "avg_price",
         "last_add", "last_week", "sl", "cmp", "open_pnl_pct", "exit_date",
         "exit_price", "exit_reason", "realized_pnl_pct", "updated"]
ECOLS = ["date", "type", "symbol", "price", "details"]


# ----------------------------------------------------------------- helpers
def now_ist():
    return datetime.now(IST)


def today():
    return now_ist().strftime("%Y-%m-%d")


def fl(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def parse_legs(s):
    out = []
    for p in str(s).split("|"):
        if "@" in p:
            d, pr = p.split("@")
            out.append((d, float(pr)))
    return out


def pnl_pct(legs, price):
    cost = sum(p for _, p in legs)
    return (price * len(legs) - cost) / cost * 100 if cost else 0.0


def load_journal():
    if not os.path.exists(JOURNAL):
        return []
    return pd.read_csv(JOURNAL, dtype=str, keep_default_na=False).to_dict("records")


def save_journal(rows):
    os.makedirs(DATA, exist_ok=True)
    pd.DataFrame(rows, columns=JCOLS).to_csv(JOURNAL, index=False)


def load_events():
    if not os.path.exists(EVENTS):
        return []
    return pd.read_csv(EVENTS, dtype=str, keep_default_na=False).to_dict("records")


def save_events(rows):
    pd.DataFrame(rows, columns=ECOLS).to_csv(EVENTS, index=False)


def event(etype, symbol, price, details):
    return {"date": today(), "type": etype, "symbol": symbol,
            "price": f"{price:.2f}", "details": details}


# ----------------------------------------------------------------- data
def fetch_weekly(symbol, complete_only, period="3y"):
    for attempt in range(3):
        try:
            df = yf.download(f"{symbol}.NS", period=period, interval="1wk",
                             auto_adjust=False, progress=False, threads=False)
            if df is not None and not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                df = df[["Open", "High", "Low", "Close"]].dropna()
                df.index = pd.to_datetime(df.index).tz_localize(None)
                # weekly bars start Monday: keep the last bar only once Saturday has arrived
                if complete_only and len(df) and (now_ist().date() - df.index[-1].date()).days < 5:
                    df = df.iloc[:-1]
                return df
        except Exception:
            pass
        time.sleep(1 + attempt)
    return None


def prep(wk):
    wk = wk.copy()
    wk["SMA"] = wk["Close"].rolling(SMA_LEN).mean()
    y = wk.groupby(wk.index.year).agg(H=("High", "max"), L=("Low", "min"), C=("Close", "last"))
    r4 = y["C"] + (y["H"] - y["L"]) * 1.1 / 2
    r4.index = r4.index + 1  # level of year N is used in year N+1
    wk["R4"] = wk.index.year.map(r4)
    return wk


def get_mcap_cr(symbol):
    t = yf.Ticker(f"{symbol}.NS")
    for attempt in range(3):
        try:
            fi = t.fast_info
            mc = None
            try:
                mc = fi["market_cap"]
            except Exception:
                pass
            if not mc:
                mc = float(fi["last_price"]) * float(fi["shares"])
            if mc and mc > 0:
                return mc / 1e7
        except Exception:
            pass
        time.sleep(1 + attempt)
    return 0.0


def list_symbols():
    """All NSE EQ-series symbols. Uses data/EQUITY_L.csv if you uploaded one, else downloads it."""
    local = f"{DATA}/EQUITY_L.csv"
    if os.path.exists(local):
        text = open(local, encoding="utf-8", errors="ignore").read()
    else:
        try:
            r = requests.get(NSE_LIST_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
            r.raise_for_status()
            text = r.text
        except Exception as ex:
            raise SystemExit(f"Could not download the NSE list ({ex}). Download EQUITY_L.csv from "
                             "NSE and upload it into the data folder of the repo.")
    nse = pd.read_csv(io.StringIO(text))
    nse.columns = [c.strip() for c in nse.columns]
    return sorted(nse[nse["SERIES"].str.strip() == "EQ"]["SYMBOL"].str.strip().unique())


def download_batch(symbols, complete_only=True, chunk=80):
    """Weekly data for many symbols with few requests (80 tickers per call)."""
    out = {}
    for i in range(0, len(symbols), chunk):
        part = symbols[i:i + chunk]
        df = None
        for attempt in range(3):
            try:
                df = yf.download([f"{x}.NS" for x in part], period="3y", interval="1wk",
                                 group_by="ticker", auto_adjust=False, progress=False, threads=True)
                if df is not None and not df.empty:
                    break
            except Exception:
                pass
            time.sleep(3 * (attempt + 1))
        if df is None or df.empty:
            continue
        for x in part:
            try:
                d = df[f"{x}.NS"][["Open", "High", "Low", "Close"]].dropna()
            except Exception:
                continue
            if d.empty:
                continue
            d.index = pd.to_datetime(d.index).tz_localize(None)
            if complete_only and (now_ist().date() - d.index[-1].date()).days < 5:
                d = d.iloc[:-1]
            out[x] = d
        print(f"Downloaded {min(i + chunk, len(symbols))}/{len(symbols)}")
    return out


# ----------------------------------------------------------------- journal ops
def close_trade(row, price, reason, events, etype="SL_HIT"):
    legs = parse_legs(row["legs"])
    row.update(status="CLOSED", exit_date=today(), exit_price=f"{price:.2f}",
               exit_reason=reason, realized_pnl_pct=f"{pnl_pct(legs, price):.2f}",
               cmp=f"{price:.2f}", open_pnl_pct="", updated=today())
    events.append(event(etype, row["symbol"], price,
                        f"{reason}. SL(SMA20)={fl(row['sl']):.2f}, units={row['units']}, "
                        f"avg={fl(row['avg_price']):.2f}, P/L={row['realized_pnl_pct']}%"))


def refresh_row(row, price, sl):
    legs = parse_legs(row["legs"])
    row.update(cmp=f"{price:.2f}", sl=f"{sl:.2f}",
               open_pnl_pct=f"{pnl_pct(legs, price) + 0.0:.2f}".replace("-0.00", "0.00"), updated=today())


def new_trade(symbol, date, price, sma, r4):
    return {"id": f"{symbol}-{date}", "symbol": symbol, "status": "OPEN", "r4": f"{r4:.2f}",
            "entry_date": date, "legs": f"{date}@{price:.2f}", "units": "1",
            "avg_price": f"{price:.2f}", "last_add": f"{price:.2f}", "last_week": date,
            "sl": f"{sma:.2f}", "cmp": f"{price:.2f}", "open_pnl_pct": "0.00",
            "exit_date": "", "exit_price": "", "exit_reason": "", "realized_pnl_pct": "",
            "updated": today()}


# ----------------------------------------------------------------- modes
def run_weekly(force_universe=False):
    journal = load_journal()
    events = []
    open_rows = {r["symbol"]: r for r in journal if r["status"] == "OPEN"}
    symbols = sorted(set(list_symbols()) | set(open_rows))
    print(f"Downloading weekly data for {len(symbols)} symbols ...")
    data = download_batch(symbols)
    for sym in open_rows:                      # make sure open positions always have data
        if sym not in data:
            wk = fetch_weekly(sym, complete_only=True)
            if wk is not None:
                data[sym] = wk

    candidates = []
    for sym, raw in data.items():
        if len(raw) < 60:
            continue
        wk = prep(raw).dropna(subset=["SMA", "R4"])
        if len(wk) < 2:
            continue
        last, prev = wk.iloc[-1], wk.iloc[-2]
        wdate = wk.index[-1].strftime("%Y-%m-%d")
        c, sma, r4 = float(last["Close"]), float(last["SMA"]), float(last["R4"])
        row = open_rows.get(sym)
        if row is None:
            if float(prev["Close"]) <= r4 < c:  # fresh weekly close above yearly R4
                candidates.append((sym, wdate, c, sma, r4))
        elif wdate > row["last_week"]:
            row["last_week"] = wdate
            if c < sma:
                close_trade(row, c, "Weekly close below SMA20", events)
                continue
            if sma >= fl(row["last_add"]):  # SMA touched last entry price -> pyramid
                legs = parse_legs(row["legs"]) + [(wdate, c)]
                row.update(legs="|".join(f"{d}@{p:.2f}" for d, p in legs), units=str(len(legs)),
                           avg_price=f"{sum(p for _, p in legs) / len(legs):.2f}",
                           last_add=f"{c:.2f}")
                events.append(event("PYRAMID", sym, c,
                                    f"Weekly SMA20 {sma:.2f} touched last entry. Added unit #{len(legs)} "
                                    f"at {c:.2f}. Stop now {sma:.2f}"))
            refresh_row(row, c, sma)

    print(f"{len(candidates)} R4 crossovers found - checking market caps ...")
    for sym, wdate, c, sma, r4 in candidates:   # market cap is checked only for actual signals
        mc = get_mcap_cr(sym)
        if 0 < mc < MIN_MCAP_CR:
            print(f"  skip {sym}: market cap {mc:,.0f} Cr")
            continue
        note = f"Market cap {mc:,.0f} Cr" if mc > 0 else "Market cap unverified"
        journal.append(new_trade(sym, wdate, c, sma, r4))
        events.append(event("ENTRY", sym, c,
                            f"Weekly close crossed above yearly R4 {r4:.2f}. "
                            f"SL (weekly SMA20) = {sma:.2f}. {note}"))
    finish(journal, events)


def run_daily():
    journal = load_journal()
    events = []
    for row in [r for r in journal if r["status"] == "OPEN"]:
        wk = fetch_weekly(row["symbol"], complete_only=False, period="2y")
        if wk is None or len(wk) < SMA_LEN:
            print("No data for", row["symbol"])
            continue
        price = float(wk["Close"].iloc[-1])           # latest price (running week's close so far)
        sl = float(wk["Close"].tail(SMA_LEN).mean())  # live weekly SMA20
        if price < sl:
            close_trade(row, price, "Daily close below weekly SMA20", events)
        else:
            refresh_row(row, price, sl)
    finish(journal, events)


# ----------------------------------------------------------------- output
def finish(journal, events):
    save_journal(journal)
    all_events = load_events() + events
    save_events(all_events)
    build_dashboard(journal, all_events, events)
    send_telegram(events)
    print(f"Done. New events: {len(events)}")
    for e in events:
        print(f"  {e['type']:8s} {e['symbol']:12s} {e['price']}  {e['details']}")


def send_telegram(events):
    tok, chat = os.getenv("TELEGRAM_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if not (tok and chat and events):
        return
    icon = {"ENTRY": "🟢", "PYRAMID": "➕", "SL_HIT": "🔴"}
    msg = "\n".join(f"{icon.get(e['type'], '')} {e['type']} {e['symbol']} @ {e['price']}\n{e['details']}"
                    for e in events)
    try:
        requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                      data={"chat_id": chat, "text": msg[:4000]}, timeout=20)
    except Exception as ex:
        print("Telegram failed:", ex)


def table(headers, rows):
    if not rows:
        return "<p class='muted'>None</p>"
    th = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f"<div class='scroll'><table><tr>{th}</tr>{body}</table></div>"


def pct(x):
    v = fl(x, None)
    if v is None:
        return ""
    return f"<span class='{'up' if v >= 0 else 'dn'}'>{v:+.2f}%</span>"


def build_dashboard(journal, all_events, new_events):
    os.makedirs(DOCS, exist_ok=True)
    open_rows = [r for r in journal if r["status"] == "OPEN"]
    closed = [r for r in journal if r["status"] == "CLOSED"]
    cutoff = (now_ist() - timedelta(days=14)).strftime("%Y-%m-%d")
    recent = [e for e in all_events if e["date"] >= cutoff][::-1]

    o_rows = []
    for r in sorted(open_rows, key=lambda r: -fl(r["open_pnl_pct"])):
        cmp_, sl = fl(r["cmp"]), fl(r["sl"])
        dist = (cmp_ - sl) / cmp_ * 100 if cmp_ else 0
        o_rows.append([f"<b>{html.escape(r['symbol'])}</b>", r["entry_date"], r["units"],
                       f"{fl(r['avg_price']):.2f}", f"{cmp_:.2f}", f"{sl:.2f}", f"{dist:.1f}%",
                       f"{fl(r['last_add']):.2f}", f"{fl(r['r4']):.2f}", pct(r["open_pnl_pct"])])
    c_rows = [[f"<b>{html.escape(r['symbol'])}</b>", r["entry_date"], r["exit_date"], r["units"],
               f"{fl(r['avg_price']):.2f}", f"{fl(r['exit_price']):.2f}",
               html.escape(r["exit_reason"]), pct(r["realized_pnl_pct"])]
              for r in sorted(closed, key=lambda r: r["exit_date"], reverse=True)]
    sl_rows = [[e["date"], f"<b>{html.escape(e['symbol'])}</b>", e["price"], html.escape(e["details"])]
               for e in all_events[::-1] if e["type"] == "SL_HIT"]
    e_rows = [[e["date"], e["type"], f"<b>{html.escape(e['symbol'])}</b>", e["price"],
               html.escape(e["details"])] for e in recent]
    todays = [e for e in all_events if e["date"] == today()]
    alert_html = "".join(
        f"<div class='alert {e['type'].lower()}'><b>{e['type']}</b> {html.escape(e['symbol'])} "
        f"@ {e['price']}<br><small>{html.escape(e['details'])}</small></div>" for e in todays
    ) or "<p class='muted'>No alerts today.</p>"

    wins = [r for r in closed if fl(r["realized_pnl_pct"]) > 0]
    winrate = f"{len(wins) / len(closed) * 100:.0f}%" if closed else "-"
    avg_ret = f"{sum(fl(r['realized_pnl_pct']) for r in closed) / len(closed):+.2f}%" if closed else "-"

    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Weekly R4 Breakout Journal</title>
<style>
:root{{--bg:#fff;--fg:#1a1a1a;--mut:#777;--line:#ddd;--card:#f6f7f9;--up:#0a7d3b;--dn:#c62828}}
@media (prefers-color-scheme:dark){{:root{{--bg:#121212;--fg:#eee;--mut:#999;--line:#333;--card:#1c1c1e;--up:#4cd47f;--dn:#ff6b6b}}}}
body{{font-family:system-ui,sans-serif;background:var(--bg);color:var(--fg);margin:0;padding:12px;max-width:1100px;margin:auto}}
h1{{font-size:1.3rem}}h2{{font-size:1.05rem;margin-top:26px;border-bottom:1px solid var(--line);padding-bottom:4px}}
.scroll{{overflow-x:auto}}table{{border-collapse:collapse;width:100%;font-size:.85rem}}
th,td{{padding:6px 8px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}}
th{{color:var(--mut);font-weight:600}}.up{{color:var(--up)}}.dn{{color:var(--dn)}}.muted{{color:var(--mut)}}
.stats span{{display:inline-block;background:var(--card);padding:6px 12px;border-radius:8px;margin:0 6px 6px 0}}
.alert{{background:var(--card);border-left:4px solid var(--mut);padding:8px 10px;margin:6px 0;border-radius:4px}}
.alert.entry{{border-color:var(--up)}}.alert.sl_hit{{border-color:var(--dn)}}.alert.pyramid{{border-color:#e69500}}
</style></head><body>
<h1>Weekly &gt; Yearly R4 Breakout Journal</h1>
<p class="muted">Updated {now_ist().strftime('%d %b %Y %H:%M IST')} · Entry: weekly close above yearly R4 · Stop: weekly 20 SMA · Pyramid: SMA20 touches last entry</p>
<div class="stats"><span>Open: <b>{len(open_rows)}</b></span><span>Closed: <b>{len(closed)}</b></span>
<span>Win rate: <b>{winrate}</b></span><span>Avg closed return: <b>{avg_ret}</b></span></div>
<h2>Today's alerts</h2>{alert_html}
<h2>Open positions</h2>{table(["Symbol", "Entry", "Units", "Avg", "CMP", "SL (SMA20)", "SL dist", "Next pyramid when SMA ≥", "R4", "P/L"], o_rows)}
<h2>Stoploss hits</h2>{table(["Date", "Symbol", "Exit price", "Details"], sl_rows)}
<h2>Closed trades</h2>{table(["Symbol", "Entry", "Exit", "Units", "Avg", "Exit px", "Reason", "P/L"], c_rows)}
<h2>Alert log (last 14 days)</h2>{table(["Date", "Type", "Symbol", "Price", "Details"], e_rows)}
</body></html>"""
    with open(f"{DOCS}/index.html", "w", encoding="utf-8") as f:
        f.write(page)

    with open(f"{DOCS}/alerts.md", "w", encoding="utf-8") as f:
        f.write(f"# Alerts - {today()}\n\n")
        f.write("\n".join(f"- **{e['type']}** {e['symbol']} @ {e['price']} - {e['details']}"
                          for e in todays) or "No alerts today.")
        f.write("\n")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "weekly"
    if mode == "weekly":
        run_weekly(force_universe="--refresh-universe" in sys.argv)
    elif mode == "daily":
        run_daily()
    else:
        raise SystemExit("usage: python scanner.py [weekly|daily] [--refresh-universe]")
