import asyncio
import json
import random
import sqlite3
import uuid
from datetime import datetime, timezone
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
import uvicorn

app = FastAPI()
subscribers = set()

# --- 1. DATABASE SETUP ---
def init_db():
    # This creates a file named 'signals.db' in your folder
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS signals (
            id TEXT PRIMARY KEY,
            ts TEXT,
            symbol TEXT,
            strategy TEXT,
            dir TEXT,
            score INTEGER,
            entry REAL
        )
    ''')
    conn.commit()
    conn.close()

def save_signal(sig):
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("INSERT INTO signals VALUES (?, ?, ?, ?, ?, ?, ?)", 
              (sig["id"], sig["ts"], sig["symbol"], sig["strategy"], sig["dir"], sig["score"], sig["entry"]))
    conn.commit()
    conn.close()

def get_recent_signals():
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("SELECT * FROM signals ORDER BY ts DESC LIMIT 10")
    rows = c.fetchall()
    conn.close()
    # Convert to list of dicts
    return [{"id": r[0], "ts": r[1], "symbol": r[2], "strategy": r[3], "dir": r[4], "score": r[5], "entry": r[6]} for r in rows]

# --- 2. EVENT BROKER & WORKER ---
async def publish(event: str, data: dict):
    for q in list(subscribers):
        await q.put({"event": event, "data": data})

async def worker():
    while True:
        await asyncio.sleep(5)
        direction = random.choice(["long", "short"])
        sig = {
            "id": str(uuid.uuid4())[:6],
            "ts": datetime.now(timezone.utc).strftime("%H:%M:%S"),
            "symbol": random.choice(["EURUSD", "XAUUSD", "GBPUSD"]),
            "strategy": random.choice(["Bat", "Gartley", "Main"]),
            "dir": direction,
            "score": random.randint(6, 9),
            "entry": round(random.uniform(1.05, 1.12), 4),
        }
        save_signal(sig)         # Save to DB first
        await publish("signal", sig) # Then broadcast to web

@app.on_event("startup")
async def startup():
    init_db()                    # Create table if it doesn't exist
    asyncio.create_task(worker())

# --- 3. WEB ROUTES ---
@app.get("/stream")
async def stream(request: Request):
    q = asyncio.Queue()
    subscribers.add(q)
    async def gen():
        try:
            while True:
                if await request.is_disconnected():
                    break
                msg = await q.get()
                yield f"event: {msg['event']}\ndata: {json.dumps(msg['data'])}\n\n"
        finally:
            subscribers.remove(q)
    return StreamingResponse(gen(), media_type="text/event-stream")

@app.get("/api/history")
def history():
    # Serves the last 10 signals from the DB
    return get_recent_signals()

@app.get("/")
def index():
    return HTMLResponse("""
    <html>
    <head>
        <title>Signals</title>
        <style>
            body { background: #0f1216; color: #e6edf3; font-family: monospace; padding: 20px; }
            .card { border: 1px solid #26303b; padding: 10px; margin-bottom: 10px; border-radius: 6px; background: #171c23; }
            .long { color: #3fb950; font-weight: bold; } .short { color: #f85149; font-weight: bold; }
        </style>
    </head>
    <body>
        <h2>Live Signals (DB Enabled)</h2>
        <div id="list">Loading history...</div>
        
        <script>
            const list = document.getElementById('list');

            function addCard(d) {
                const div = document.createElement('div');
                div.className = 'card';
                div.innerHTML = `<b>[${d.ts}]</b> ${d.symbol} <span class="${d.dir}">${d.dir.toUpperCase()}</span> 
                                 | ${d.strategy} | Score: ${d.score}/12 | Entry: ${d.entry}`;
                return div;
            }

            // 1. Load History from Database on page load
            fetch('/api/history')
                .then(res => res.json())
                .then(data => {
                    list.innerHTML = ''; // Clear "Loading..."
                    data.forEach(d => list.appendChild(addCard(d)));
                });

            // 2. Connect to Live Stream for new signals
            const es = new EventSource('/stream');
            es.addEventListener('signal', e => {
                const d = JSON.parse(e.data);
                list.prepend(addCard(d)); // Put new signal at the top
            });
        </script>
    </body>
    </html>
    """)

import os 

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)