import os, random, time, threading, requests
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://jnhpnsfdnckkpnyrmpkc.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_publishable_t52nYcH5sW43P48ZSUE4eg_GWwyg_ma")
TOTAL, KEEP, INTERVAL = 500, 10, 30

H = {"apikey": SUPABASE_KEY, "Content-Type": "application/json"}
URL = f"{SUPABASE_URL}/rest/v1/signals"

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

lock = threading.Lock()
store = {i: {"name": f"SIGNAL{i:03d}", "data": []} for i in range(1, TOTAL + 1)}
status = {"last_save": None, "error": None}


def load_db():
    try:
        r = requests.get(URL + "?select=id,name,data&order=id&limit=1000", headers=H, timeout=20)
        rows = r.json()
        if isinstance(rows, list):
            with lock:
                for row in rows:
                    if row["id"] in store:
                        store[row["id"]]["data"] = row.get("data") or []
        else:
            status["error"] = str(rows)
    except Exception as e:
        status["error"] = str(e)


def tick():
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    with lock:
        for i, s in store.items():
            s["data"] = ([{"p": random.choice(["BIG", "SMALL"]), "t": now}] + s["data"])[:KEEP]
            rows.append({"id": i, "name": s["name"], "data": s["data"]})
    try:
        h = dict(H, Prefer="resolution=merge-duplicates,return=minimal")
        r = requests.post(URL + "?on_conflict=id", headers=h, json=rows, timeout=30)
        if r.status_code >= 300:
            status["error"] = r.text[:300]
        else:
            status["error"] = None
            status["last_save"] = now
    except Exception as e:
        status["error"] = str(e)


def loop():
    load_db()
    nxt = time.time()
    while True:
        tick()
        nxt += INTERVAL
        time.sleep(max(0, nxt - time.time()))


@app.on_event("startup")
def start():
    threading.Thread(target=loop, daemon=True).start()


def fmt(i):
    s = store[i]
    return {
        "signal": s["name"],
        "id": i,
        "signals": [
            {"no": n + 1, "prediction": d["p"], "ago_seconds": (n + 1) * INTERVAL}
            for n, d in enumerate(s["data"])
        ],
    }


@app.get("/")
def home():
    return {"total": TOTAL, "all": "/api/all", "single": "/api/signal/1",
            "list": "/api/list", "status": "/health"}


@app.get("/health")
def health():
    return {"ok": True, **status}


@app.get("/api/list")
def lst():
    return [{"id": i, "name": s["name"], "url": f"/api/signal/{i}"} for i, s in store.items()]


@app.get("/api/signal/{key}")
def one(key: str):
    k = key.upper().replace("SIGNAL", "")
    if not k.isdigit() or not (1 <= int(k) <= TOTAL):
        raise HTTPException(404, "1-500 er moddhe dao")
    with lock:
        return fmt(int(k))


@app.get("/api/all")
def all_():
    with lock:
        return [fmt(i) for i in store]
