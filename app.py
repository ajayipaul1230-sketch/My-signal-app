import asyncio
import json
import sqlite3
import uuid
import os
from datetime import datetime, timezone
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
import uvicorn
import yfinance as yf
import pandas as pd

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="super-secret-random-key-123")
APP_PASSWORD = "secret123" 
subscribers = set()

# --- RISK SETTINGS ---
MAX_TRADES_PER_DAY = 4

def init_db():
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS signals (
            id TEXT PRIMARY KEY, ts TEXT, symbol TEXT, strategy TEXT, 
            dir TEXT, score INTEGER, entry REAL, sl REAL, tp REAL)''')
    conn.commit()
    conn.close()

def save_signal(sig):
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("INSERT INTO signals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", 
              (sig["id"], sig["ts"], sig["symbol"], sig["strategy"], sig["dir"], sig["score"], sig["entry"], sig["sl"], sig["tp"]))
    conn.commit()
    conn.close()

def get_trades_today():
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM signals WHERE ts >= date('now')")
    count = c.fetchone()[0]
    conn.close()
    return count

def get_recent_signals():
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("SELECT * FROM signals ORDER BY ts DESC LIMIT 15")
    rows = c.fetchall()
    conn.close()
    return [{"id": r[0], "ts": r[1], "symbol": r[2], "strategy": r[3], "dir": r[4], "score": r[5], "entry": r[6], "sl": r[7], "tp": r[8]} for r in rows]

async def publish(event: str, data: dict):
    for q in list(subscribers):
        await q.put({"event": event, "data": data})

# --- REAL YAHOO FINANCE LOGIC ---
def check_market(symbol, yf_ticker):
    try:
        # Fetch 5 days of 1-hour candles
        df = yf.download(tickers=yf_ticker, period="5d", interval="1h", progress=False, multi_level_index=False)
        
        if df.empty:
            print(f"[{symbol}] No data returned from Yahoo.")
            return None

        # Safely extract the latest price as a float
        current_price = float(df['Close'].iloc[-1])

        # Calculate RSI (14 period)
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        current_rsi = float(df['RSI'].iloc[-1])

        # Calculate ATR (14 period)
        df['TR'] = df['High'] - df['Low']
        df['ATR'] = df['TR'].rolling(window=14).mean()
        current_atr = float(df['ATR'].iloc[-1])
        
        # Prevent division by zero if the market is completely flat
        if current_atr == 0:
            return None

        # --- STRATEGY TRIGGER ---
        # For now, we use a basic RSI trigger (Oversold < 30, Overbought > 70)
        # We will replace this with your v2.1 Gartley/AB=CD logic later.
        direction = None
        strategy_name = None
        
        if current_rsi < 30:
            direction = "long"
            strategy_name = "RSI Oversold"
        elif current_rsi > 70:
            direction = "short"
            strategy_name = "RSI Overbought"
            
        if not direction:
            return None # No setup found

        # Calculate Stop Loss (1.5 ATR) and Take Profit (2.0 ATR)
        if direction == "long":
            sl = round(current_price - (1.5 * current_atr), 4)
            tp = round(current_price + (2.0 * current_atr), 4)
        else:
            sl = round(current_price + (1.5 * current_atr), 4)
            tp = round(current_price - (2.0 * current_atr), 4)

        return {
            "id": str(uuid.uuid4())[:6],
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "strategy": strategy_name,
            "dir": direction,
            "score": 7,
            "entry": round(current_price, 4),
            "sl": sl,
            "tp": tp
        }

    except Exception as e:
        # If Yahoo blocks the request or data is malformed, log it and return None (app keeps running)
        print(f"Error checking {symbol}: {e}")
        return None

async def worker():
    # Map your symbols to Yahoo Finance tickers
    tickers = {
        "EURUSD": "EURUSD=X", 
        "XAUUSD": "GC=F",   # Gold Futures
        "GBPUSD": "GBPUSD=X"
    }
    
    while True:
        # 1. Check trade limit
        trades_today = get_trades_today()
        if trades_today >= MAX_TRADES_PER_DAY:
            print(f"Daily limit reached ({trades_today}/{MAX_TRADES_PER_DAY}). Sleeping...")
            await asyncio.sleep(300) # Sleep for 5 minutes to save API calls
            continue

        # 2. Scan markets
        print("Scanning markets...")
        for symbol, ticker in tickers.items():
            sig = check_market(symbol, ticker)
            if sig:
                print(f"!!! SIGNAL GENERATED: {symbol} {sig['dir']} !!!")
                save_signal(sig)
                await publish("signal", sig)
                
                # Check limit again right after a signal, in case we hit 4
                if get_trades_today() >= MAX_TRADES_PER_DAY:
                    break
        
        # Wait 60 seconds before scanning again
        await asyncio.sleep(60)

@app.on_event("startup")
async def startup():
    init_db()
    asyncio.create_task(worker())

# --- WEB ROUTES ---
@app.get("/healthz")
def healthz():
    return {"status": "ok"}

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
    q = asyncio.Queue()
    subscribers.add(q)
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
    <head><title>Real Market Signals</title>
    <style>
        body { background: #0f1216; color: #e6edf3; font-family: monospace; padding: 20px; }
        .card { border: 1px solid #26303b; padding: 15px; margin-bottom: 10px; border-radius: 6px; background: #171c23; }
        .long { color: #3fb950; font-weight: bold; } .short { color: #f85149; font-weight: bold; }
        .row { display: flex; justify-content: space-between; margin-top: 5px; font-size: 12px; color: #8b98a5;}
    </style></head>
    <body>
        <h2>Live Real Market Signals</h2>
        <p style="color:#8b98a5; font-size:12px;">Scanning EURUSD, XAUUSD, GBPUSD (Max 4 per day).</p>
        <div id="list">Loading history...</div>
        <script>
            const list = document.getElementById('list');
            function addCard(d) {
                const div = document.createElement('div');
                div.className = 'card';
                const timeOnly = d.ts.includes(' ') ? d.ts.split(' ')[1] : d.ts;
                div.innerHTML = `<div><b>[${timeOnly}]</b> ${d.symbol} <span class="${d.dir}">${d.dir.toUpperCase()}</span> | ${d.strategy}</div>
                                 <div class="row"><span>Entry: ${d.entry}</span><span>SL: ${d.sl}</span><span>TP: ${d.tp}</span></div>`;
                return div;
            }
            fetch('/api/history').then(r => r.json()).then(data => {
                list.innerHTML = '';
                if(data.length === 0) list.innerHTML = "Waiting for market setups (RSI < 30 or > 70)...";
                data.forEach(d => list.appendChild(addCard(d)));
            });
            const es = new EventSource('/stream');
            es.addEventListener('signal', e => {
                const d = JSON.parse(e.data);
                if(list.innerText.includes("Waiting")) list.innerHTML = "";
                list.prepend(addCard(d));
            });
        </script>
    </body>
    </html>
    """)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)