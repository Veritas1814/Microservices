from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import hazelcast
import uuid
import os
import asyncio
import random
import json

app = FastAPI()

CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8000")
HZ_MEMBERS = os.getenv("HZ_MEMBERS", "hz1,hz2,hz3").split(",")
MY_SERVICE_NAME = os.getenv("MY_SERVICE_NAME", "facade-service")
MY_SERVICE_URL = os.getenv("MY_SERVICE_URL", "http://facade-service:8000")

hz_client = None
counter_queue = None



@app.on_event("startup")
async def startup():
    global hz_client, counter_queue

    for attempt in range(15):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_members=HZ_MEMBERS,
                cluster_name="dev",
            )
            counter_queue = hz_client.get_queue("counter-queue")
            print("[facade-service] Connected to Hazelcast ✓")
            break
        except Exception as e:
            print(f"[facade-service] Hazelcast not ready (attempt {attempt + 1}): {e}")
            await asyncio.sleep(3)

    await register_with_config_server()
    print("[facade-service] Startup complete.")


async def register_with_config_server():
    for attempt in range(15):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{CONFIG_SERVER_URL}/register",
                    json={"service_name": MY_SERVICE_NAME, "url": MY_SERVICE_URL},
                    timeout=5.0,
                )
                print(f"[facade-service] Registered with config-server: {resp.json()}")
                return
        except Exception as e:
            print(f"[facade-service] Config-server not ready (attempt {attempt + 1}): {e}")
            await asyncio.sleep(2)



async def get_service_urls(service_name: str):
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{CONFIG_SERVER_URL}/services/{service_name}", timeout=5.0
        )
        urls = resp.json().get("urls", [])
    if not urls:
        raise HTTPException(
            status_code=503,
            detail=f"No instances of '{service_name}' registered in config-server",
        )
    return urls


async def call_random_logging(client, endpoint, payload=None, is_post=True):
    """Pick a random logging-service instance (from config-server) and call it with failover."""
    urls = await get_service_urls("logging-service")
    random.shuffle(urls)
    for url in urls:
        target = f"{url}{endpoint}"
        try:
            if is_post:
                resp = await client.post(target, json=payload, timeout=2.0)
            else:
                resp = await client.get(target, timeout=2.0)
            resp.raise_for_status()
            print(f"[facade-service] Used logging instance: {url}")
            return resp
        except Exception as e:
            print(f"[facade-service] Logging instance {url} failed: {e}")
    raise HTTPException(status_code=503, detail="All logging service instances are unreachable.")



class ClientRequest(BaseModel):
    user_Id: str
    amount: float


@app.post("/")
async def process_transaction(req: ClientRequest):
    transaction_id = str(uuid.uuid4())
    log_payload = {
        "transaction_ID": transaction_id,
        "user_Id": req.user_Id,
        "amount": req.amount,
    }

    async with httpx.AsyncClient() as client:
        log_resp = await call_random_logging(client, "/log", payload=log_payload, is_post=True)

    msg = json.dumps({"user_Id": req.user_Id, "amount": req.amount})
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: counter_queue.put(msg).result())
    print(f"[facade-service] Enqueued counter update: {msg}")

    return {
        "transaction_ID": transaction_id,
        "status": "queued",
        "logger_used": log_resp.json().get("instance"),
    }


@app.get("/user/{user_Id}")
async def get_user_info(user_Id: str):
    counter_urls = await get_service_urls("counter-service")
    counter_url = random.choice(counter_urls)
    print(f"[facade-service] Using counter-service at {counter_url}")

    async with httpx.AsyncClient() as client:
        try:
            bal_resp = await client.get(f"{counter_url}/balance/{user_Id}", timeout=3.0)
            balance = bal_resp.json().get("balance")
        except Exception as e:
            print(f"[facade-service] Counter-service unreachable: {e}")
            balance = None

        log_resp = await call_random_logging(client, f"/log/{user_Id}", is_post=False)

    return {
        "balance": balance,
        "transactions": log_resp.json().get("transactions"),
        "logger_used": log_resp.json().get("instance"),
    }


@app.get("/balances")
async def get_all_balances():
    counter_urls = await get_service_urls("counter-service")
    counter_url = random.choice(counter_urls)
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{counter_url}/balances", timeout=3.0)
            return resp.json()
        except Exception as e:
            return {"error": str(e)}
