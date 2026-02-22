from fastapi import FastAPI
from pydantic import BaseModel
import httpx
import uuid
import time
import os
import asyncio

app = FastAPI()

LOGGING_URL = os.getenv("LOGGING_URL", "http://localhost:8001")
COUNTER_URL = os.getenv("COUNTER_URL", "http://localhost:8002")

time_metrics = {"logging_time": 0.0, "counter_time": 0.0}

class ClientRequest(BaseModel):
    user_Id: str
    amount: float

@app.post("/")
async def process_transaction(req: ClientRequest):
    transaction_id = str(uuid.uuid4())

    # Prepare payloads
    log_payload = {"transaction_ID": transaction_id, "user_Id": req.user_Id, "amount": req.amount}
    counter_payload = {"user_Id": req.user_Id, "amount": req.amount}

    async with httpx.AsyncClient() as client:
        t0 = time.time()
        log_task = client.post(f"{LOGGING_URL}/log", json=log_payload)

        t1 = time.time()
        counter_task = client.post(f"{COUNTER_URL}/update", json=counter_payload)

        log_resp, counter_resp = await asyncio.gather(log_task, counter_task)

        t2 = time.time()

        time_metrics["logging_time"] += (t2 - t0)
        time_metrics["counter_time"] += (t2 - t1)

    balance = counter_resp.json().get("balance")
    return {"transaction_ID": transaction_id, "balance": balance}

@app.get("/user/{user_Id}")
async def get_user_info(user_Id: str):
    async with httpx.AsyncClient() as client:
        bal_resp, log_resp = await asyncio.gather(
            client.get(f"{COUNTER_URL}/balance/{user_Id}"),
            client.get(f"{LOGGING_URL}/log/{user_Id}")
        )
    return {
        "balance": bal_resp.json().get("balance"),
        "transactions": log_resp.json().get("transactions")
    }

@app.get("/accounts")
async def get_all_accounts():
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{COUNTER_URL}/balances")
    return resp.json()

@app.get("/metrics")
async def get_metrics(reset: bool = False):
    metrics = dict(time_metrics)
    if reset:
        time_metrics["logging_time"] = 0.0
        time_metrics["counter_time"] = 0.0
    return metrics