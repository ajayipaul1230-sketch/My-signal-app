import asyncio
import json
import sqlite3
import uuid
import random
import os
from datetime import datetime, timezone
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
import uvicorn

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="super-secret-random-key-123")
APP_PASSWORD = "secret123" 
subscribers = set()

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

async def worker():
    while True:
        await asyncio.sleep(10) # Fast 10-second timer for testing
        direction = random.choice(["long", "short"])
        entry = round(random.uniform(1.05, 1.12), 4)
        sig = {
            "id": str(uuid.uuid4())[:6],
            "ts": datetime.now(timezone.utc).strftime("%H:%M:%S"),
            "symbol": random.choice(["EURUSD", "XAUUSD", "GBPUSD"]),
            "strategy": "Test Mode",
            "dir": direction,
            "score": 7,
            "entry": entry,
            "sl": round(entry - 0.002, 4) if direction=="long" else round(entry + 0.002, 4),
            "tp": round(entry + 0.004, 4) if direction=="long" else round(entry - 0.004, 4),
        }
        save_signal(sig)
        await publish("signal", sig)

@app.on_event("startup")
async def startup():
    init_db()
    asyncio.create_task(worker())

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
    <head><title>Signals</title>
    <style>
        body { background: #0f1216; color: #e6edf3; font-family: monospace; padding: 20px; }
        .card { border: 1px solid #26303b; padding: 15px; margin-bottom: 10px; border-radius: 6px; background: #171c23; }
        .long { color: #3fb950; font-weight: bold; } .short { color: #f85149; font-weight: bold; }
        .row { display: flex; justify-content: space-between; margin-top: 5px; font-size: 12px; color: #8b98a5;}
    </style></head>
    <body>
        <h2>Live Signals (Test)</h2>
        <div id="list">Loading history...</div>
        <script>
            const list = document.getElementById('list');
            function addCard(d) {
                const div = document.createElement('div');
                div.className = 'card';
                div.innerHTML = `<div><b>[${d.ts}]</b> ${d.symbol} <span class="${d.dir}">${d.dir.toUpperCase()}</span> | ${d.strategy}</div>
                                 <div class="row"><span>Entry: ${d.entry}</span><span>SL: ${d.sl}</span><span>TP: ${d.tp}</span></div>`;
                return div;
            }
            fetch('/api/history').then(r => r.json()).then(data => {
                list.innerHTML = '';
                if(data.length === 0) list.innerHTML = "Waiting for data...";
                data.forEach(d => list.appendChild(addCard(d)));
            });
            const es = new EventSource('/stream');
            es.addEventListener('signal', e => {
                const d = JSON.parse(e.data);
                if(list.innerText === "Waiting for data...") list.innerHTML = "";
                list.prepend(addCard(d));
            });
        </script>
    </body>
    </html>
    """)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)