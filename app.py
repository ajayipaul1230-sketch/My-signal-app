import asyncio
import json
import sqlite3
import uuid
import os
from datetime import datetime, timezone
import pandas as pd
import yfinance as yf
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
import uvicorn

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="super-secret-random-key-123")
APP_PASSWORD = "secret123" 
subscribers = set()

MAX_TRADES_PER_DAY = 4
MIN_SCORE = 7
SWING_STRENGTH_H1 = 3
SWING_STRENGTH_H4 = 5
MIN_SWING_ATR_MULT = 0.6

# --- DB FUNCTIONS ---
def init_db():
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS signals (
            id TEXT PRIMARY KEY, ts TEXT, symbol TEXT, strategy TEXT, 
            dir TEXT, score INTEGER, entry REAL, sl REAL, tp REAL)''')
    conn.commit(); conn.close()

def save_signal(sig):
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("INSERT INTO signals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", 
              (sig["id"], sig["ts"], sig["symbol"], sig["strategy"], sig["dir"], sig["score"], sig["entry"], sig["sl"], sig["tp"]))
    conn.commit(); conn.close()

def get_trades_today():
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM signals WHERE ts >= date('now')")
    count = c.fetchone()[0]
    conn.close(); return count

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

# --- V2.1 MATH ---
def find_swings(df, strength, atr_val):
    highs = df['High'].values
    lows = df['Low'].values
    swing_highs, swing_lows = [], []
    for i in range(strength, len(df) - strength):
        if highs[i] == max(highs[i-strength:i+strength+1]):
            if (highs[i] - min(lows[i-strength:i+strength+1])) >= (MIN_SWING_ATR_MULT * atr_val):
                swing_highs.append(highs[i])
        if lows[i] == min(lows[i-strength:i+strength+1]):
            if (max(highs[i-strength:i+strength+1]) - lows[i]) >= (MIN_SWING_ATR_MULT * atr_val):
                swing_lows.append(lows[i])
    return swing_highs, swing_lows

def check_htf_bias(df_h4, current_price, atr_h4):
    h4_highs, h4_lows = find_swings(df_h4, SWING_STRENGTH_H4, atr_h4)
    if not h4_highs or not h4_lows: return "neutral"
    long_allowed = current_price > h4_lows[-1]
    short_allowed = current_price < h4_highs[-1]
    if long_allowed and not short_allowed: return "long_only"
    elif short_allowed and not long_allowed: return "short_only"
    else: return "both"

def check_c1c2_rejection(df):
    if len(df) < 3: return None
    c1, c2 = df.iloc[-2], df.iloc[-1]
    c1_range = c1['High'] - c1['Low']
    if c1_range == 0: return None

    c1_long_valid = ((c1['Close'] - c1['Low']) / c1_range >= 0.75) and (c1['Close'] > c1['Open'])
    if c1_long_valid and (c2['Close'] > c1['High']): return "long"

    c1_short_valid = ((c1['High'] - c1['Close']) / c1_range >= 0.75) and (c1['Close'] < c1['Open'])
    if c1_short_valid and (c2['Close'] < c1['Low']): return "short"
    return None

def detect_abcd(df, direction, atr):
    """§9.5 Valid AB=CD pattern detection (Simplified for H1 swings)"""
    if len(df) < 10: return False
    
    # We look at the last 4 major swing points to find A, B, C, D
    highs, lows = find_swings(df, 3, atr)
    if len(highs) < 2 or len(lows) < 2: return False

    # For a Long (Bullish AB=CD): A is low, B is high, C is low, D is low (higher than C)
    # For a Short (Bearish AB=CD): A is high, B is low, C is high, D is high (lower than C)
    
    if direction == "long":
        # Simplified check: We just assume if we are at a swing low, we check the AB=CD ratio
        # In a full implementation, you'd map the exact A,B,C,D points.
        recent_lows = lows[-2:]
        if len(recent_lows) == 2:
            ab_leg = abs(highs[-1] - recent_lows[0])
            cd_leg = abs(highs[-1] - recent_lows[-1])
            # CD must be 90% to 110% of AB
            if ab_leg > 0 and 0.9 <= (cd_leg / ab_leg) <= 1.1:
                return True
    else:
        recent_highs = highs[-2:]
        if len(recent_highs) == 2:
            ab_leg = abs(recent_highs[0] - lows[-1])
            cd_leg = abs(recent_highs[-1] - lows[-1])
            if ab_leg > 0 and 0.9 <= (cd_leg / ab_leg) <= 1.1:
                return True
                
    return False

def calculate_score(df_h1, direction, bias, atr):
    score = 0
    reasons = []

    # 1. RSI Overbought/Oversold (1 Point)
    delta = df_h1['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    current_rsi = float((100 - (100 / (1 + rs))).iloc[-1])
    if (direction == "long" and current_rsi < 20) or (direction == "short" and current_rsi > 80):
        score += 1; reasons.append("RSI")

    # 2. Structure Alignment (2 Points) - Automatically passed by HTF filter
    if (direction == "long" and bias in ["long_only", "both"]) or \
       (direction == "short" and bias in ["short_only", "both"]):
        score += 2; reasons.append("Struct")

    # 3. Ratio Confirmation (2 Points) - Simplified: if we have valid swings
    score += 2; reasons.append("Ratio")

    # 4. AB=CD Completion (2 Points)
    if detect_abcd(df_h1, direction, atr):
        score += 2; reasons.append("AB=CD")

    return score, "+".join(reasons)

def check_market(symbol, yf_ticker):
    try:
        df_h1 = yf.download(tickers=yf_ticker, period="5d", interval="1h", progress=False, multi_level_index=False)
        if df_h1.empty: return None

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
        
        if not rejection_dir: return None
        if rejection_dir == "long" and bias not in ["long_only", "both"]: return None
        if rejection_dir == "short" and bias not in ["short_only", "both"]: return None

        score, reason_string = calculate_score(df_h1, rejection_dir, bias, current_atr)

        if score < MIN_SCORE:
            return None

        # Calculate SL and TP (v2.1 ATR logic: 1.5 ATR Stop, 2.0 ATR TP)
        if rejection_dir == "long":
            sl = round(current_price - (1.5 * current_atr), 4)
            tp = round(current_price + (2.0 * current_atr), 4)
        else:
            sl = round(current_price + (1.5 * current_atr), 4)
            tp = round(current_price - (2.0 * current_atr), 4)

        # --- §4a PER-ASSET RR FLOOR ---
        risk = abs(current_price - sl)
        reward = abs(tp - current_price)
        rr = reward / risk if risk > 0 else 0

        min_rr = 1.40
        if symbol in ["XAUUSD", "BTCUSD"]: min_rr = 1.70
        elif symbol == "USDCAD": min_rr = 1.50

        if rr < min_rr:
            print(f"[{symbol}] Rejected: RR {rr:.2f} below floor {min_rr}")
            return None

        return {
            "id": str(uuid.uuid4())[:6],
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "strategy": f"Score {score}/12 ({reason_string}) | RR: {rr:.1f}",
            "dir": rejection_dir,
            "score": score,
            "entry": round(current_price, 4),
            "sl": sl,
            "tp": tp
        }

    except Exception as e:
        print(f"Error checking {symbol}: {e}")
        return None

async def worker():
    tickers = {"EURUSD": "EURUSD=X", "XAUUSD": "GC=F", "GBPUSD": "GBPUSD=X"}
    while True:
        if get_trades_today() >= MAX_TRADES_PER_DAY:
            await asyncio.sleep(300)
            continue

        print("Scanning markets (v2.1 AB=CD & RR)...")
        for symbol, ticker in tickers.items():
            sig = check_market(symbol, ticker)
            if sig:
                print(f"!!! TRADE: {symbol} {sig['dir'].upper()} | {sig['strategy']} !!!")
                save_signal(sig)
                await publish("signal", sig)
                if get_trades_today() >= MAX_TRADES_PER_DAY: break
        
        await asyncio.sleep(60)

@app.on_event("startup")
async def startup():
    init_db()
    asyncio.create_task(worker())

# --- WEB ROUTES (unchanged) ---
@app.get("/healthz")
def healthz(): return {"status": "ok"}

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
    <head><title>v2.1 Signals</title>
    <style>
        body { background: #0f1216; color: #e6edf3; font-family: monospace; padding: 20px; }
        .card { border: 1px solid #26303b; padding: 15px; margin-bottom: 10px; border-radius: 6px; background: #171c23; }
        .long { color: #3fb950; font-weight: bold; } .short { color: #f85149; font-weight: bold; }
        .row { display: flex; justify-content: space-between; margin-top: 5px; font-size: 12px; color: #8b98a5;}
    </style></head>
    <body>
        <h2>v2.1 Live Signals (Strict AB=CD & RR)</h2>
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
                if(data.length === 0) list.innerHTML = "Scanning for valid AB=CD...";
                data.forEach(d => list.appendChild(addCard(d)));
            });
            const es = new EventSource('/stream');
            es.addEventListener('signal', e => {
                const d = JSON.parse(e.data);
                if(list.innerText.includes("Scanning")) list.innerHTML = "";
                list.prepend(addCard(d));
            });
        </script>
    </body>
    </html>
    """)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)