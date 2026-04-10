from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import uuid
import time
import os
import asyncio
import random

app = FastAPI()

LOGGING_URLS = os.getenv("LOGGING_URLS", "http://localhost:8001").split(",")
COUNTER_URL = os.getenv("COUNTER_URL", "http://localhost:8002")

time_metrics = {"logging_time": 0.0, "counter_time": 0.0}

class ClientRequest(BaseModel):
    user_Id: str
    amount: float

async def call_logging_with_failover(client, endpoint, payload=None, is_post=True):
    urls = list(LOGGING_URLS)
    random.shuffle(urls)

    for url in urls:
        target = f"{url}{endpoint}"
        try:
            if is_post:
                resp = await client.post(target, json=payload, timeout=2.0)
            else:
                resp = await client.get(target, timeout=2.0)
            resp.raise_for_status()
            return resp
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            print(f"Warning: Failed to connect to {url}, trying next... Error: {e}")
            continue

    raise HTTPException(status_code=503, detail="All logging service instances are unreachable.")

@app.post("/")
async def process_transaction(req: ClientRequest):
    transaction_id = str(uuid.uuid4())
    log_payload = {"transaction_ID": transaction_id, "user_Id": req.user_Id, "amount": req.amount}
    counter_payload = {"user_Id": req.user_Id, "amount": req.amount}

    async with httpx.AsyncClient() as client:
        t0 = time.time()
        log_resp = await call_logging_with_failover(client, "/log", payload=log_payload, is_post=True)
        t1 = time.time()

        counter_resp = await client.post(f"{COUNTER_URL}/update", json=counter_payload)
        t2 = time.time()

        time_metrics["logging_time"] += (t1 - t0)
        time_metrics["counter_time"] += (t2 - t1)

    balance = counter_resp.json().get("balance")
    return {"transaction_ID": transaction_id, "balance": balance, "logger_used": log_resp.json().get("instance")}

@app.get("/user/{user_Id}")
async def get_user_info(user_Id: str):
    async with httpx.AsyncClient() as client:
        bal_resp = await client.get(f"{COUNTER_URL}/balance/{user_Id}")
        log_resp = await call_logging_with_failover(client, f"/log/{user_Id}", is_post=False)

    return {
        "balance": bal_resp.json().get("balance"),
        "transactions": log_resp.json().get("transactions"),
        "logger_used": log_resp.json().get("instance")
    }

@app.get("/metrics")
async def get_metrics(reset: bool = False):
    metrics = dict(time_metrics)
    if reset:
        time_metrics["logging_time"] = 0.0
        time_metrics["counter_time"] = 0.0
    return metrics