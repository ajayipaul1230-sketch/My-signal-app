import asyncio
import json
import sqlite3
import uuid
import os
from datetime import datetime, timezone, timedelta
import pandas as pd
import httpx
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
import uvicorn

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="super-secret-random-key-123")
APP_PASSWORD = "secret123" 
subscribers = set()

# --- API SETTINGS ---
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY", "").strip()
FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()

ACCOUNT_BALANCE = 10000.0   
RISK_PER_TRADE = 0.0025     
MAX_OPEN_TRADES = 2         
MAX_TRADES_PER_DAY = 4
MAX_DAILY_LOSS_R = -2.0 
MIN_SCORE = 7
SWING_STRENGTH_H1 = 3
SWING_STRENGTH_H4 = 5
MIN_SWING_ATR_MULT = 0.6
FIB_TOLERANCE = 0.02
MIN_IMPULSE_ATR = 1.2  # Lowered from 1.5 to catch more institutional moves

# --- DB FUNCTIONS ---
def init_db():
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS signals (
            id TEXT PRIMARY KEY, ts TEXT, symbol TEXT, strategy TEXT, 
            dir TEXT, score INTEGER, entry REAL, sl REAL, tp REAL,
            status TEXT DEFAULT 'OPEN')''')
    conn.commit(); conn.close()

def save_signal(sig):
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("INSERT INTO signals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
              (sig["id"], sig["ts"], sig["symbol"], sig["strategy"], sig["dir"], 
               sig["score"], sig["entry"], sig["sl"], sig["tp"], "OPEN"))
    conn.commit(); conn.close()

def update_trade_status(trade_id, status):
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("UPDATE signals SET status = ? WHERE id = ?", (status, trade_id))
    conn.commit(); conn.close()

def get_open_trades():
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("SELECT * FROM signals WHERE status = 'OPEN'")
    rows = c.fetchall()
    conn.close()
    return [{"id": r[0], "symbol": r[2], "dir": r[4], "entry": r[6], "sl": r[7], "tp": r[8]} for r in rows]

def get_daily_stats():
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("SELECT COUNT(*), status FROM signals WHERE ts >= date('now') GROUP BY status")
    rows = c.fetchall()
    conn.close()
    trades_today = 0
    daily_R = 0.0
    for count, status in rows:
        trades_today += count
        if status == "WIN": daily_R += 2.0 
        elif status == "LOSS": daily_R -= 1.0 
    return trades_today, daily_R

def get_recent_signals():
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("SELECT * FROM signals ORDER BY ts DESC LIMIT 20")
    rows = c.fetchall()
    conn.close()
    return [{"id": r[0], "ts": r[1], "symbol": r[2], "strategy": r[3], "dir": r[4], "score": r[5], "entry": r[6], "sl": r[7], "tp": r[8], "status": r[9]} for r in rows]

async def publish(event: str, data: dict):
    for q in list(subscribers):
        await q.put({"event": event, "data": data})

async def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: 
        print("Telegram config missing!")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(url, json=payload)
            print(f"Telegram response: {r.status_code}")
        except Exception as e:
            print(f"Telegram error: {e}")

# --- ECONOMIC NEWS FILTER ---
async def check_news_blackout():
    if not FINNHUB_API_KEY: return False
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    url = f"https://finnhub.io/api/v1/calendar/economic?from={today}&to={today}&token={FINNHUB_API_KEY}"
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url)
            data = r.json()
            if "economicCalendar" not in data: return False
            for event in data["economicCalendar"]:
                if event.get("country") == "US" and str(event.get("impact", "")).lower() == "high":
                    time_str = event.get("time")
                    if not time_str: continue
                    try:
                        event_time = datetime.fromisoformat(time_str.replace(" ", "T")).replace(tzinfo=timezone.utc)
                        diff = (event_time - now).total_seconds() / 60.0
                        if -45 <= diff <= 30:
                            return True
                    except: continue
        except: pass
    return False

# --- OPTIMIZED DATA FETCHER ---
async def fetch_h1_data(symbol):
    if not TWELVEDATA_API_KEY: return pd.DataFrame()
    url = "https://api.twelvedata.com/time_series"
    params = {"symbol": symbol, "interval": "1h", "outputsize": 100, "apikey": TWELVEDATA_API_KEY, "format": "JSON"}
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, params=params)
            data = r.json()
            if "values" not in data: 
                print(f"API Error {symbol}: {data}")
                return pd.DataFrame()
            df = pd.DataFrame(data["values"])
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
            df = df.iloc[::-1] 
            for col in ['open', 'high', 'low', 'close']:
                df[col] = df[col].astype(float)
            df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close'}, inplace=True)
            return df
        except Exception as e: 
            print(f"Fetch Error {symbol}: {e}")
            return pd.DataFrame()

# --- V2.1 HARMONIC MATH ---
def get_confirmed_swings(df, strength, atr_val):
    highs = df['High'].values
    lows = df['Low'].values
    raw_swings = []
    for i in range(strength, len(df) - strength):
        if highs[i] == max(highs[i-strength:i+strength+1]):
            if (highs[i] - min(lows[i-strength:i+strength+1])) >= (MIN_SWING_ATR_MULT * atr_val):
                raw_swings.append({"type": "H", "price": highs[i], "idx": i})
        if lows[i] == min(lows[i-strength:i+strength+1]):
            if (max(highs[i-strength:i+strength+1]) - lows[i]) >= (MIN_SWING_ATR_MULT * atr_val):
                raw_swings.append({"type": "L", "price": lows[i], "idx": i})
    clean_swings = []
    for s in raw_swings:
        if not clean_swings: clean_swings.append(s)
        else:
            last = clean_swings[-1]
            if s["type"] == last["type"]:
                if (s["type"] == "H" and s["price"] > last["price"]) or (s["type"] == "L" and s["price"] < last["price"]):
                    clean_swings[-1] = s
            else: clean_swings.append(s)
    return clean_swings[-5:] if len(clean_swings) >= 5 else []

def check_htf_bias(df_h4, current_price, atr_h4):
    swings = get_confirmed_swings(df_h4, SWING_STRENGTH_H4, atr_h4)
    if not swings: return "neutral"
    highs = [s["price"] for s in swings if s["type"] == "H"]
    lows = [s["price"] for s in swings if s["type"] == "L"]
    if not highs or not lows: return "neutral"
    long_allowed = current_price > lows[-1]
    short_allowed = current_price < highs[-1]
    if long_allowed and not short_allowed: return "long_only"
    elif short_allowed and not long_allowed: return "short_only"
    else: return "both"

def check_c1c2_rejection(df):
    if len(df) < 3: return None
    c1, c2 = df.iloc[-2], df.iloc[-1]
    c1_range = c1['High'] - c1['Low']
    if c1_range == 0: return None
    
    # LOOSENED TO 0.60 for higher frequency
    c1_long_valid = ((c1['Close'] - c1['Low']) / c1_range >= 0.60) and (c1['Close'] > c1['Open'])
    if c1_long_valid and (c2['Close'] > c1['High']): return "long"
    
    c1_short_valid = ((c1['High'] - c1['Close']) / c1_range >= 0.60) and (c1['Close'] < c1['Open'])
    if c1_short_valid and (c2['Close'] < c1['Low']): return "short"
    return None

def detect_harmonic(df, direction, atr, current_price):
    swings = get_confirmed_swings(df, 3, atr)
    if len(swings) < 4: return None
    X, A, B, C = swings
    if direction == "long" and C["type"] != "H": return None
    if direction == "short" and C["type"] != "L": return None
    XA = abs(A["price"] - X["price"])
    AB = abs(B["price"] - A["price"])
    AD = abs(current_price - X["price"])
    if XA == 0: return None
    ab_xa, ad_xa = AB / XA, AD / XA
    if 0.35 <= ab_xa <= 0.55 and abs(ad_xa - 0.886) <= FIB_TOLERANCE: return "Bat"
    if 0.60 <= ab_xa <= 0.80 and abs(ad_xa - 0.786) <= FIB_TOLERANCE: return "Gartley"
    return None

def calculate_score(df_h1, direction, bias, atr, current_price):
    score = 0
    reasons = []
    delta = df_h1['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    current_rsi = float((100 - (100 / (1 + rs))).iloc[-1])
    if (direction == "long" and current_rsi < 20) or (direction == "short" and current_rsi > 80):
        score += 1; reasons.append("RSI")
    if (direction == "long" and bias in ["long_only", "both"]) or \
       (direction == "short" and bias in ["short_only", "both"]):
        score += 2; reasons.append("Struct")
    harmonic = detect_harmonic(df_h1, direction, atr, current_price)
    if harmonic:
        score += 3; reasons.append(harmonic)
    else:
        swings = get_confirmed_swings(df_h1, 3, atr)
        if len(swings) == 4:
            ab_leg = abs(swings[1]["price"] - swings[0]["price"])
            cd_leg = abs(swings[3]["price"] - swings[2]["price"])
            if ab_leg > 0 and 0.9 <= (cd_leg / ab_leg) <= 1.1:
                score += 2; reasons.append("AB=CD")
    return score, "+".join(reasons)

# --- INSTITUTIONAL SUPPLY & DEMAND ---
def check_supply_demand(df_h1, direction, current_price, current_atr):
    if len(df_h1) < 40: return None
    # Look back up to 50 candles (increased from 30)
    for i in range(len(df_h1) - 5, len(df_h1) - 50, -1):
        c_impulse = df_h1.iloc[i]
        c_base = df_h1.iloc[i-1]
        
        impulse_body = abs(c_impulse['Close'] - c_impulse['Open'])
        
        if impulse_body >= (MIN_IMPULSE_ATR * current_atr):
            zone_top = c_base['High']
            zone_bottom = c_base['Low']
            
            # Add a buffer to the zone so we don't miss the entry by a fraction of a pip
            buffer = 0.1 * current_atr
            
            if direction == "long" and c_impulse['Close'] > c_impulse['Open']:
                if (zone_bottom - buffer) <= current_price <= (zone_top + buffer):
                    return {"zone_type": "Demand", "top": round(zone_top, 4), "bottom": round(zone_bottom, 4)}
                    
            if direction == "short" and c_impulse['Close'] < c_impulse['Open']:
                if (zone_bottom - buffer) <= current_price <= (zone_top + buffer):
                    return {"zone_type": "Supply", "top": round(zone_top, 4), "bottom": round(zone_bottom, 4)}
    return None

async def check_open_trades(tickers):
    open_trades = get_open_trades()
    if not open_trades: return
    for trade in open_trades:
        try:
            ticker = tickers.get(trade["symbol"])
            if not ticker: continue
            df = await fetch_h1_data(ticker) 
            if df.empty: continue
            current_price = float(df['Close'].iloc[-1])
            
            hit_status = None
            if trade["dir"] == "long":
                if current_price >= trade["tp"]: hit_status = "WIN"
                elif current_price <= trade["sl"]: hit_status = "LOSS"
            else:
                if current_price <= trade["tp"]: hit_status = "WIN"
                elif current_price >= trade["sl"]: hit_status = "LOSS"

            if hit_status:
                update_trade_status(trade["id"], hit_status)
                await publish("trade_update", {"id": trade["id"], "status": hit_status})
                await send_telegram(f"🏁 <b>TRADE CLOSED: {trade['symbol']}</b>\nResult: {hit_status}")
        except Exception: pass 

async def check_market(symbol, api_ticker):
    try:
        df_h1 = await fetch_h1_data(api_ticker)
        if df_h1.empty or len(df_h1) < 50: 
            print(f"[{symbol}] Empty or insufficient data.")
            return None
        
        df_h4 = df_h1.resample('4h').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}).dropna()
        
        df_h1['TR'] = df_h1['High'] - df_h1['Low']
        df_h1['ATR'] = df_h1['TR'].rolling(window=14).mean()
        current_atr = float(df_h1['ATR'].iloc[-1])
        current_price = float(df_h1['Close'].iloc[-1])
        
        df_h4['TR'] = df_h4['High'] - df_h4['Low']
        df_h4['ATR'] = df_h4['TR'].rolling(window=14).mean()
        atr_h4 = float(df_h4['ATR'].iloc[-1])

        bias = check_htf_bias(df_h4, current_price, atr_h4)
        rejection_dir = check_c1c2_rejection(df_h1)
        
        if not rejection_dir: 
            print(f"[{symbol}] Rejected: No C1/C2 rejection candle.")
            return None

        if rejection_dir == "long" and bias not in ["long_only", "both"]: 
            print(f"[{symbol}] Rejected: C1/C2 is Long, but H4 Bias is {bias}.")
            return None
        if rejection_dir == "short" and bias not in ["short_only", "both"]: 
            print(f"[{symbol}] Rejected: C1/C2 is Short, but H4 Bias is {bias}.")
            return None

        # Check S&D
        sd_zone = check_supply_demand(df_h1, rejection_dir, current_price, current_atr)
        if sd_zone:
            if rejection_dir == "long":
                sl = round(sd_zone["bottom"] - (0.2 * current_atr), 4) 
                tp = round(current_price + (2.5 * current_atr), 4)     
            else:
                sl = round(sd_zone["top"] + (0.2 * current_atr), 4)    
                tp = round(current_price - (2.5 * current_atr), 4)
                
            return {
                "id": str(uuid.uuid4())[:6], "ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "symbol": symbol, "strategy": f"S&D {sd_zone['zone_type']} Zone", "dir": rejection_dir,
                "score": 9, "entry": round(current_price, 4), "sl": sl, "tp": tp
            }
        else:
            print(f"[{symbol}] Rejected: No S&D zone found at current price.")

        score, reason_string = calculate_score(df_h1, rejection_dir, bias, current_atr, current_price)
        if score < MIN_SCORE: 
            print(f"[{symbol}] Rejected: V2.1 Score {score}/12 (Need 7)")
            return None

        if rejection_dir == "long":
            sl = round(current_price - (1.5 * current_atr), 4)
            tp = round(current_price + (2.0 * current_atr), 4)
        else:
            sl = round(current_price + (1.5 * current_atr), 4)
            tp = round(current_price - (2.0 * current_atr), 4)

        return {
            "id": str(uuid.uuid4())[:6], "ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol, "strategy": f"V2.1 Score {score}/12 ({reason_string})", "dir": rejection_dir,
            "score": score, "entry": round(current_price, 4), "sl": sl, "tp": tp
        }
    except Exception as e: 
        print(f"Error checking {symbol}: {e}")
        return None

async def worker():
    tickers = {
        "EURUSD": "EUR/USD", "GBPUSD": "GBP/USD", "XAUUSD": "XAU/USD",
        "AUDUSD": "AUD/USD", "USDJPY": "USD/JPY"
    }
    
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        await send_telegram("🤖 <b>Bot Online!</b>\nWide Open Mode Active. Verbose Logging ON. Manual trigger available.")

    while True:
        await check_open_trades(tickers)
        trades_today, daily_R = get_daily_stats()
        
        open_trades = get_open_trades()
        if len(open_trades) >= MAX_OPEN_TRADES or daily_R <= MAX_DAILY_LOSS_R or trades_today >= MAX_TRADES_PER_DAY:
            await asyncio.sleep(300)
            continue

        is_blackout = await check_news_blackout()
        if is_blackout:
            print("NEWS BLACKOUT ACTIVE.")
            await asyncio.sleep(300)
            continue

        print(f"\n--- Scanning (Trades: {trades_today}/4 | R: {daily_R} | Open: {len(open_trades)}) ---")
        
        for symbol, api_ticker in tickers.items():
            sig = await check_market(symbol, api_ticker)
            if sig:
                save_signal(sig)
                await publish("signal", sig)
                emoji = "🟢" if sig['dir'] == 'long' else "🔴"
                risk_usd = ACCOUNT_BALANCE * RISK_PER_TRADE
                msg = (f"{emoji} <b>NEW SIGNAL: {sig['symbol']}</b>\n"
                       f"Strategy: {sig['strategy']}\n"
                       f"Direction: {sig['dir'].upper()}\n"
                       f"Entry: {sig['entry']}\n"
                       f"SL: {sig['sl']}\n"
                       f"TP: {sig['tp']}\n"
                       f"Risk: ${risk_usd:.2f} (0.25%)")
                await send_telegram(msg)
                
                if get_daily_stats()[0] >= MAX_TRADES_PER_DAY or len(get_open_trades()) >= MAX_OPEN_TRADES: 
                    break
        
        await asyncio.sleep(60)

@app.on_event("startup")
async def startup():
    init_db()
    asyncio.create_task(worker())

# --- WEB ROUTES ---
@app.get("/healthz")
def healthz(): return {"status": "ok"}

@app.get("/test-signal")
async def test_signal():
    """MANUAL TRIGGER: Bypasses all math and sends a test signal to Telegram"""
    dummy_sig = {
        "id": str(uuid.uuid4())[:6], "ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": "TEST", "strategy": "MANUAL TEST", "dir": "long",
        "score": 10, "entry": 1.0000, "sl": 0.9900, "tp": 1.0200
    }
    save_signal(dummy_sig)
    await publish("signal", dummy_sig)
    await send_telegram("🚨 <b>MANUAL TEST SIGNAL</b>\nIf you see this, the Telegram pipeline is working perfectly.")
    return {"status": "test signal sent"}

@app.get("/login", response_class=HTMLResponse)
def login_page():
    return """
    <body style="background:#0f1216; color:#fff; font-family:monospace; text-align:center; padding-top:100px;">
        <h2>Enter Dashboard Password</h2>
        <form action="/login" method="post">
            <input type="password" name="password" placeholder="Password" style="padding:10px; font-size:16px;">
            <button type="submit" style="padding:10px 20px; font-size:16px;">Enter</button>
        </form>
    </body>
    """

@app.post("/login")
async def do_login(request: Request, password: str = Form(...)):
    if password == APP_PASSWORD:
        request.session["logged_in"] = True
        return RedirectResponse(url="/", status_code=303)
    return HTMLResponse("Wrong password. <a href='/login'>Try again</a>", status_code=401)

@app.get("/stream")
async def stream(request: Request):
    if not request.session.get("logged_in"): return RedirectResponse(url="/login")
    q = asyncio.Queue(); subscribers.add(q)
    async def gen():
        try:
            while True:
                if await request.is_disconnected(): break
                msg = await q.get()
                yield f"event: {msg['event']}\ndata: {json.dumps(msg['data'])}\n\n"
        finally:
            subscribers.remove(q)
    return StreamingResponse(gen(), media_type="text/event-stream")

@app.get("/api/history")
def history(request: Request):
    if not request.session.get("logged_in"): return []
    return get_recent_signals()

@app.get("/")
def index(request: Request):
    if not request.session.get("logged_in"): return RedirectResponse(url="/login")
    return HTMLResponse("""
    <html>
    <head><title>Signals</title>
    <style>
        body { background: #0f1216; color: #e6edf3; font-family: monospace; padding: 20px; }
        .card { border: 1px solid #26303b; padding: 15px; margin-bottom: 10px; border-radius: 6px; background: #171c23; }
        .long { color: #3fb950; font-weight: bold; } .short { color: #f85149; font-weight: bold; }
        .row { display: flex; justify-content: space-between; margin-top: 5px; font-size: 12px; color: #8b98a5;}
        .badge { padding: 2px 6px; border-radius: 4px; font-size: 10px; margin-left: 10px; font-weight: bold; }
        .bg-open { background: #1f6feb; color: white; }
        .bg-win { background: #238636; color: white; }
        .bg-loss { background: #da3633; color: white; }
    </style></head>
    <body>
        <h2>Live Signals</h2>
        <button onclick="fetch('/test-signal').then(()=>alert('Test sent to Telegram!'))" style="background:#1f6feb; color:white; border:none; padding:10px; margin-bottom:20px; cursor:pointer; border-radius:4px;">
            🚨 Trigger Manual Test Signal
        </button>
        <div id="list">Loading history...</div>
        <script>
            const list = document.getElementById('list');
            function addCard(d) {
                const div = document.createElement('div');
                div.id = 'trade-' + d.id;
                div.className = 'card';
                const timeOnly = d.ts.includes(' ') ? d.ts.split(' ')[1] : d.ts;
                const badgeClass = d.status === 'WIN' ? 'bg-win' : (d.status === 'LOSS' ? 'bg-loss' : 'bg-open');
                
                div.innerHTML = `<div><b>[${timeOnly}]</b> ${d.symbol} <span class="${d.dir}">${d.dir.toUpperCase()}</span> | ${d.strategy} 
                                 <span class="badge ${badgeClass}">${d.status}</span></div>
                                 <div class="row"><span>Entry: ${d.entry}</span><span>SL: ${d.sl}</span><span>TP: ${d.tp}</span></div>`;
                return div;
            }
            
            fetch('/api/history').then(r => r.json()).then(data => {
                list.innerHTML = '';
                if(data.length === 0) list.innerHTML = "Scanning...";
                data.forEach(d => list.appendChild(addCard(d)));
            });
            
            const es = new EventSource('/stream');
            es.addEventListener('signal', e => {
                const d = JSON.parse(e.data);
                if(list.innerText.includes("Scanning")) list.innerHTML = "";
                list.prepend(addCard(d));
            });
            
            es.addEventListener('trade_update', e => {
                const d = JSON.parse(e.data);
                const card = document.getElementById('trade-' + d.id);
                if (card) {
                    const badge = card.querySelector('.badge');
                    badge.className = 'badge ' + (d.status === 'WIN' ? 'bg-win' : 'bg-loss');
                    badge.innerText = d.status;
                }
            });
        </script>
    </body>
    </html>
    """)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)