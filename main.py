import asyncio
import json
import os
import random
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from supabase import create_client, Client

# ---- config ----
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://jnhpnsfdnckkpnyrmpkc.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "sb_publishable_t52nYcH5sW43P48ZSUE4eg_GWwyg_ma")

MAX_SIGNALS            = 500
INTERVAL_SECONDS       = 30
PREDICTIONS_PER_SIGNAL = 10

supabase: Client | None = None
generator_task: asyncio.Task | None = None


def get_client() -> Client:
    global supabase
    if supabase is None:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    return supabase


def get_count() -> int:
    res = get_client().table("signals").select("id", count="exact").execute()
    return res.count if res.count is not None else len(res.data or [])


def get_last_created_at() -> datetime | None:
    res = (
        get_client()
        .table("signals")
        .select("created_at")
        .order("id", desc=True)
        .limit(1)
        .execute()
    )
    if not res.data:
        return None
    raw = res.data[0]["created_at"]
    return datetime.fromisoformat(raw.replace("Z", "+00:00")) if isinstance(raw, str) else raw


def generate_one() -> bool:
    count = get_count()
    if count >= MAX_SIGNALS:
        return False

    idx  = count + 1
    name = f"SIGNAL {str(idx).zfill(3)}"
    predictions = [random.choice(["BIG", "SMALL"]) for _ in range(PREDICTIONS_PER_SIGNAL)]

    try:
        get_client().table("signals").insert(
            {"name": name, "predictions": predictions}
        ).execute()
        print(f"[{datetime.now(timezone.utc).isoformat()}] Inserted {name}: {predictions}")
        return True
    except Exception as e:
        print(f"Insert error: {e}")
        return False


async def generator_loop():
    # restart-safe: last signal থেকে 30s হয়নি হলে বাকি সময় wait
    last = get_last_created_at()
    if last:
        elapsed = (datetime.now(timezone.utc) - last).total_seconds()
        wait = INTERVAL_SECONDS - elapsed
        if wait > 0:
            print(f"Waiting {wait:.1f}s before next signal...")
            await asyncio.sleep(wait)

    while True:
        if get_count() >= MAX_SIGNALS:
            print("✅ Reached 500 signals. Generator stopped.")
            return
        generate_one()
        await asyncio.sleep(INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global generator_task
    generator_task = asyncio.create_task(generator_loop())
    yield
    if generator_task:
        generator_task.cancel()
        try:
            await generator_task
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)


@app.get("/")
def root():
    return {
        "api": "Big/Small Signal API",
        "endpoints": ["/api/latest", "/api/ping"],
        "max_signals": MAX_SIGNALS,
        "interval_seconds": INTERVAL_SECONDS,
    }


@app.get("/api/ping")
def ping():
    return {"pong": True, "now": datetime.now(timezone.utc).isoformat()}


@app.get("/api/latest")
def latest():
    total = get_count()
    rows = (
        get_client()
        .table("signals")
        .select("*")
        .order("id", desc=True)
        .limit(10)
        .execute()
    ).data or []

    latest_list = []
    for i, row in enumerate(rows, start=1):
        preds = row["predictions"]
        if isinstance(preds, str):
            try:
                preds = json.loads(preds)
            except Exception:
                preds = []
        latest_list.append({
            "index": i,
            "name": row["name"],
            "predictions": [{"no": j + 1, "result": p} for j, p in enumerate(preds)],
            "created_at": row["created_at"],
        })

    return {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "total_signals": total,
        "max_signals": MAX_SIGNALS,
        "interval_seconds": INTERVAL_SECONDS,
        "latest_10": latest_list,
    }
