import os, random, time, threading, requests
from datetime import datetime, timezone
from flask import Flask, jsonify

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://jnhpnsfdnckkpnyrmpkc.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_publishable_t52nYcH5sW43P48ZSUE4eg_GWwyg_ma")
TOTAL, KEEP, INTERVAL = 500, 10, 30

H = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
     "Content-Type": "application/json"}
URL = f"{SUPABASE_URL}/rest/v1/signals"

app = Flask(__name__)
lock = threading.Lock()
store = {i: {"name": f"SIGNAL{i:03d}", "data": []} for i in range(1, TOTAL + 1)}


def load_db():
    try:
        r = requests.get(URL + "?select=id,name,data&order=id&limit=1000", headers=H, timeout=20)
        for row in r.json():
            store[row["id"]] = {"name": row["name"], "data": row["data"] or []}
    except Exception as e:
        print("load error", e)


def tick():
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    with lock:
        for i, s in store.items():
            s["data"] = ([{"p": random.choice(["BIG", "SMALL"]), "t": now}] + s["data"])[:KEEP]
            rows.append({"id": i, "name": s["name"], "data": s["data"]})
    try:
        h = dict(H, Prefer="resolution=merge-duplicates")
        requests.post(URL + "?on_conflict=id", headers=h, json=rows, timeout=30)
    except Exception as e:
        print("save error", e)


def loop():
    load_db()
    nxt = time.time()
    while True:
        tick()
        nxt += INTERVAL
        time.sleep(max(0, nxt - time.time()))


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


@app.after_request
def cors(r):
    r.headers["Access-Control-Allow-Origin"] = "*"
    return r


@app.route("/")
def home():
    return jsonify({"total": TOTAL, "all": "/api/all", "single": "/api/signal/1",
                    "list": "/api/list"})


@app.route("/health")
def health():
    return "ok"


@app.route("/api/list")
def lst():
    return jsonify([{"id": i, "name": s["name"], "url": f"/api/signal/{i}"}
                    for i, s in store.items()])


@app.route("/api/signal/<key>")
def one(key):
    k = key.upper().replace("SIGNAL", "")
    if not k.isdigit() or not (1 <= int(k) <= TOTAL):
        return jsonify({"error": "1-500 er moddhe dao"}), 404
    return jsonify(fmt(int(k)))


@app.route("/api/all")
def all_():
    with lock:
        return jsonify([fmt(i) for i in store])
