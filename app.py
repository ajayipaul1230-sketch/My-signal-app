import asyncio
import json
import sqlite3
import uuid
import os
import traceback
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

# --- PASTE YOUR KEYS HERE ---
TELEGRAM_TOKEN = "9096846772:AAEF5MKRdUeBnIrx1WwJqadHz2wfqMGTMvI"
TELEGRAM_CHAT_ID = "7185853274"
TWELVEDATA_API_KEY = "8cbd61450fb54efc8de484c2281ce149"
FINNHUB_API_KEY = "dc5ast1r01qochf1766gdc5ast1r01qochf17670"
# ----------------------------

ACCOUNT_BALANCE = 10000.0   
RISK_PER_TRADE = 0.0025     
MAX_OPEN_TRADES = 2         
MAX_TRADES_PER_DAY = 4
MAX_DAILY_LOSS_R = -2.0 

# --- DB FUNCTIONS (With Memory Fallback) ---
def get_db():
    try:
        return sqlite3.connect("signals.db")
    except:
        return sqlite3.connect(":memory:")

def init_db():
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS signals (
                id TEXT PRIMARY KEY, ts TEXT, symbol TEXT, strategy TEXT, 
                dir TEXT, score INTEGER, entry REAL, sl REAL, tp REAL,
                status TEXT DEFAULT 'OPEN')''')
        conn.commit(); conn.close()
    except Exception as e:
        print(f"DB Init Error: {e}")

def save_signal(sig):
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("INSERT INTO signals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                  (sig["id"], sig["ts"], sig["symbol"], sig["strategy"], sig["dir"], 
                   sig["score"], sig["entry"], sig["sl"], sig["tp"], "OPEN"))
        conn.commit(); conn.close()
    except Exception as e:
        print(f"Save DB Error: {e}")

def update_trade_status(trade_id, status):
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("UPDATE signals SET status = ? WHERE id = ?", (status, trade_id))
        conn.commit(); conn.close()
    except: pass

def get_open_trades():
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT * FROM signals WHERE status = 'OPEN'")
        rows = c.fetchall()
        conn.close()
        return [{"id": r[0], "symbol": r[2], "dir": r[4], "entry": r[6], "sl": r[7], "tp": r[8]} for r in rows]
    except: return []

def get_daily_stats():
    try:
        conn = get_db()
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
    except: return 0, 0.0

def get_recent_signals():
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT * FROM signals ORDER BY ts DESC LIMIT 20")
        rows = c.fetchall()
        conn.close()
        return [{"id": r[0], "ts": r[1], "symbol": r[2], "strategy": r[3], "dir": r[4], "score": r[5], "entry": r[6], "sl": r[7], "tp": r[8], "status": r[9]} for r in rows]
    except: return []

async def publish(event: str, data: dict):
    for q in list(subscribers):
        await q.put({"event": event, "data": data})

async def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    async with httpx.AsyncClient() as client:
        try:
            await client.post(url, json=payload)
        except Exception as e:
            print(f"Telegram send error: {e}")

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
        except: return pd.DataFrame()

# --- STRATEGY ENGINE (EMA CROSS + S&D) ---
def check_market(symbol, df_h1):
    try:
        if df_h1.empty or len(df_h1) < 50: return None
        
        df_h1['EMA20'] = df_h1['Close'].ewm(span=20, adjust=False).mean()
        df_h1['EMA50'] = df_h1['Close'].ewm(span=50, adjust=False).mean()
        df_h1['TR'] = df_h1['High'] - df_h1['Low']
        df_h1['ATR'] = df_h1['TR'].rolling(window=14).mean()
        
        c1 = df_h1.iloc[-2]
        c2 = df_h1.iloc[-1]
        
        current_price = float(c2['Close'])
        current_atr = float(c2['ATR'])
        
        ema20_c2 = float(c2['EMA20'])
        ema50_c2 = float(c2['EMA50'])
        ema20_c1 = float(c1['EMA20'])
        ema50_c1 = float(c1['EMA50'])

        direction = None
        strategy = None

        # 1. EMA CROSS (High Frequency)
        if ema20_c1 < ema50_c1 and ema20_c2 > ema50_c2:
            direction = "long"
            strategy = "EMA 20/50 Bullish Cross"
        elif ema20_c1 > ema50_c1 and ema20_c2 < ema50_c2:
            direction = "short"
            strategy = "EMA 20/50 Bearish Cross"

        # 2. S&D ZONE (If no cross)
        if not direction:
            for i in range(len(df_h1) - 5, len(df_h1) - 50, -1):
                c_impulse = df_h1.iloc[i]
                c_base = df_h1.iloc[i-1]
                impulse_body = abs(c_impulse['Close'] - c_impulse['Open'])
                
                if impulse_body >= (1.0 * current_atr):
                    zone_top = c_base['High']
                    zone_bottom = c_base['Low']
                    buffer = 0.1 * current_atr
                    
                    if c_impulse['Close'] > c_impulse['Open']: 
                        if (zone_bottom - buffer) <= current_price <= (zone_top + buffer):
                            direction = "long"
                            strategy = "S&D Demand Zone"
                            break
                    if c_impulse['Close'] < c_impulse['Open']: 
                        if (zone_bottom - buffer) <= current_price <= (zone_top + buffer):
                            direction = "short"
                            strategy = "S&D Supply Zone"
                            break

        if not direction: return None

        if direction == "long":
            sl = round(current_price - (1.5 * current_atr), 4)
            tp = round(current_price + (2.5 * current_atr), 4)
        else:
            sl = round(current_price + (1.5 * current_atr), 4)
            tp = round(current_price - (2.5 * current_atr), 4)

        return {
            "id": str(uuid.uuid4())[:6], "ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol, "strategy": strategy, "dir": direction,
            "score": 8, "entry": round(current_price, 4), "sl": sl, "tp": tp
        }
    except Exception as e: 
        print(f"Math Error {symbol}: {e}")
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

async def worker():
    tickers = {"EURUSD": "EUR/USD", "GBPUSD": "GBP/USD", "XAUUSD": "XAU/USD", "AUDUSD": "AUD/USD", "USDJPY": "USD/JPY"}
    
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        await send_telegram("🤖 <b>Bot Online!</b>\nHunting for EMA Crosses and S&D Zones.")

    while True:
        try:
            await check_open_trades(tickers)
            trades_today, daily_R = get_daily_stats()
            open_trades = get_open_trades()
            
            if len(open_trades) >= MAX_OPEN_TRADES or daily_R <= MAX_DAILY_LOSS_R or trades_today >= MAX_TRADES_PER_DAY:
                await asyncio.sleep(300)
                continue

            print(f"\n--- Scanning (Trades: {trades_today}/4 | Open: {len(open_trades)}) ---")
            
            for symbol, api_ticker in tickers.items():
                df_h1 = await fetch_h1_data(api_ticker)
                sig = check_market(symbol, df_h1)
                
                if sig:
                    print(f"!!! SIGNAL FOUND: {symbol} {sig['dir']} !!!")
                    save_signal(sig)
                    await publish("signal", sig)
                    emoji = "🟢" if sig['dir'] == 'long' else "🔴"
                    msg = (f"{emoji} <b>NEW SIGNAL: {sig['symbol']}</b>\n"
                           f"Strategy: {sig['strategy']}\n"
                           f"Direction: {sig['dir'].upper()}\n"
                           f"Entry: {sig['entry']}\n"
                           f"SL: {sig['sl']}\n"
                           f"TP: {sig['tp']}")
                    await send_telegram(msg)
                    
                    if get_daily_stats()[0] >= MAX_TRADES_PER_DAY or len(get_open_trades()) >= MAX_OPEN_TRADES: 
                        break
                else:
                    print(f"[{symbol}] No setup found.")
            
            await asyncio.sleep(60)
        except Exception:
            await send_telegram(f"❌ <b>BOT CRASHED</b>\n{traceback.format_exc()}")
            await asyncio.sleep(60)

@app.on_event("startup")
async def startup():
    init_db()
    asyncio.create_task(worker())

@app.get("/healthz")
def healthz(): return {"status": "ok"}

@app.get("/force-real-signal")
async def force_real_signal():
    df = await fetch_h1_data("EUR/USD")
    if df.empty:
        await send_telegram("❌ <b>FORCE TEST FAILED:</b> TwelveData key is wrong or limit reached.")
        return {"error": "no data"}
    current_price = float(df['Close'].iloc[-1])
    await send_telegram(f"🚨 <b>FORCED LIVE TEST</b>\nLive Price: {current_price}")
    return {"status": "sent"}

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
        button { background:#d29922; color:white; border:none; padding:10px; margin-bottom:20px; cursor:pointer; border-radius:4px; font-weight:bold;}
    </style></head>
    <body>
        <h2>Live Signals</h2>
        <button onclick="fetch('/force-real-signal').then(()=>alert('Real test sent to Telegram!'))">
            ⚡ Trigger REAL Data Test Signal
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