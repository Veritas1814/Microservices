from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import hazelcast
import uuid
import os
import asyncio
import random
import json
import time

app = FastAPI()

CONSUL_URL     = os.getenv("CONSUL_URL",     "http://consul:8500")
MY_SERVICE_NAME = os.getenv("MY_SERVICE_NAME", "facade-service")
MY_SERVICE_ID   = os.getenv("MY_SERVICE_ID",   "facade-service-1")
MY_ADDRESS      = os.getenv("MY_ADDRESS",      "facade-service")
MY_PORT         = int(os.getenv("MY_PORT",     "8000"))

hz_client     = None
counter_queue = None

_metrics = {"logging_time": 0.0, "counter_time": 0.0, "request_count": 0}



async def wait_for_consul():
    for attempt in range(30):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(f"{CONSUL_URL}/v1/status/leader", timeout=3.0)
                if resp.status_code == 200 and resp.text.strip('"'):
                    print("[facade-service] Consul is ready ✓")
                    return
        except Exception as e:
            print(f"[facade-service] Waiting for Consul (attempt {attempt + 1}): {e}")
        await asyncio.sleep(2)
    raise RuntimeError("Consul never became ready")


async def register_with_consul():
    payload = {
        "ID":      MY_SERVICE_ID,
        "Name":    MY_SERVICE_NAME,
        "Address": MY_ADDRESS,
        "Port":    MY_PORT,
        "Check": {
            "HTTP":                          f"http://{MY_ADDRESS}:{MY_PORT}/health",
            "Interval":                      "10s",
            "Timeout":                       "2s",
            "DeregisterCriticalServiceAfter": "30s",
        },
    }
    for attempt in range(15):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.put(
                    f"{CONSUL_URL}/v1/agent/service/register",
                    json=payload, timeout=5.0,
                )
            print(f"[facade-service] Registered with Consul (HTTP {resp.status_code})")
            return
        except Exception as e:
            print(f"[facade-service] Consul registration failed (attempt {attempt + 1}): {e}")
            await asyncio.sleep(2)


async def get_kv(key: str) -> str:
    for attempt in range(30):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(f"{CONSUL_URL}/v1/kv/{key}?raw=true", timeout=5.0)
                if resp.status_code == 200:
                    return resp.text.strip()
                print(f"[facade-service] KV '{key}' not ready (HTTP {resp.status_code}), retrying...")
        except Exception as e:
            print(f"[facade-service] KV error for '{key}' (attempt {attempt+1}): {e}")
        await asyncio.sleep(2)
    raise RuntimeError(f"Consul KV key '{key}' never became available")


async def discover_services(service_name: str):
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{CONSUL_URL}/v1/health/service/{service_name}?passing=true",
            timeout=5.0,
        )
    services = resp.json()
    urls = [
        f"http://{svc['Service']['Address']}:{svc['Service']['Port']}"
        for svc in services
    ]
    if not urls:
        raise HTTPException(
            status_code=503,
            detail=f"No healthy instances of '{service_name}' found in Consul",
        )
    return urls



@app.on_event("startup")
async def startup():
    global hz_client, counter_queue

    await wait_for_consul()
    await register_with_consul()

    queue_name   = await get_kv("config/mq/queue_name")
    hz_members_s = await get_kv("config/hazelcast/members")
    cluster_name = await get_kv("config/hazelcast/cluster_name")
    hz_members   = [m.strip() for m in hz_members_s.split(",")]

    print(f"[facade-service] Config from Consul → queue={queue_name}  "
          f"hz_members={hz_members}  cluster={cluster_name}")

    for attempt in range(15):
        try:
            hz_client     = hazelcast.HazelcastClient(
                cluster_members=hz_members,
                cluster_name=cluster_name,
            )
            counter_queue = hz_client.get_queue(queue_name)
            print("[facade-service] Connected to Hazelcast ✓")
            break
        except Exception as e:
            print(f"[facade-service] Hazelcast not ready (attempt {attempt + 1}): {e}")
            await asyncio.sleep(3)

    print("[facade-service] Startup complete.")


@app.on_event("shutdown")
async def shutdown():
    try:
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{CONSUL_URL}/v1/agent/service/deregister/{MY_SERVICE_ID}",
                timeout=5.0,
            )
        print("[facade-service] Deregistered from Consul")
    except Exception as e:
        print(f"[facade-service] Deregister error: {e}")



@app.get("/health")
async def health():
    return {"status": "healthy", "service": MY_SERVICE_NAME, "id": MY_SERVICE_ID}


@app.get("/metrics")
async def get_metrics(reset: bool = False):
    data = dict(_metrics)
    if reset:
        _metrics["logging_time"]  = 0.0
        _metrics["counter_time"]  = 0.0
        _metrics["request_count"] = 0
    return data



async def call_random_logging(client, endpoint, payload=None, is_post=True):
    urls = await discover_services("logging-service")
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
    log_payload    = {
        "transaction_ID": transaction_id,
        "user_Id":        req.user_Id,
        "amount":         req.amount,
    }

    async with httpx.AsyncClient() as client:
        t0       = time.time()
        log_resp = await call_random_logging(client, "/log", payload=log_payload, is_post=True)
        _metrics["logging_time"] += time.time() - t0

    t0  = time.time()
    msg = json.dumps({"user_Id": req.user_Id, "amount": req.amount})
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: counter_queue.put(msg).result())
    _metrics["counter_time"]  += time.time() - t0
    _metrics["request_count"] += 1

    print(f"[facade-service] Enqueued counter update: {msg}")
    return {
        "transaction_ID": transaction_id,
        "status":         "queued",
        "logger_used":    log_resp.json().get("instance"),
    }


@app.get("/user/{user_Id}")
async def get_user_info(user_Id: str):
    counter_urls = await discover_services("counter-service")
    counter_url  = random.choice(counter_urls)
    print(f"[facade-service] Using counter-service at {counter_url}")

    async with httpx.AsyncClient() as client:
        t0 = time.time()
        try:
            bal_resp = await client.get(f"{counter_url}/balance/{user_Id}", timeout=3.0)
            balance  = bal_resp.json().get("balance")
        except Exception as e:
            print(f"[facade-service] Counter-service unreachable: {e}")
            balance = None
        _metrics["counter_time"] += time.time() - t0

        t0       = time.time()
        log_resp = await call_random_logging(client, f"/log/{user_Id}", is_post=False)
        _metrics["logging_time"] += time.time() - t0

    return {
        "balance":      balance,
        "transactions": log_resp.json().get("transactions"),
        "logger_used":  log_resp.json().get("instance"),
    }


@app.get("/balances")
async def get_all_balances():
    counter_urls = await discover_services("counter-service")
    counter_url  = random.choice(counter_urls)
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{counter_url}/balances", timeout=3.0)
            return resp.json()
        except Exception as e:
            return {"error": str(e)}