from fastapi import FastAPI
from pydantic import BaseModel
import hazelcast
import httpx
import os
import asyncio

app = FastAPI()

CONSUL_URL      = os.getenv("CONSUL_URL",      "http://consul:8500")
INSTANCE_NAME   = os.getenv("INSTANCE_NAME",   "logging_unknown")
MY_SERVICE_NAME = os.getenv("MY_SERVICE_NAME", "logging-service")
MY_SERVICE_ID   = os.getenv("MY_SERVICE_ID",   "logging-service-1")
MY_ADDRESS      = os.getenv("MY_ADDRESS",      "logging1")
MY_PORT         = int(os.getenv("MY_PORT",     "8000"))

hz_map = None



async def wait_for_consul():
    for attempt in range(30):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(f"{CONSUL_URL}/v1/status/leader", timeout=3.0)
                if resp.status_code == 200 and resp.text.strip('"'):
                    print(f"[{INSTANCE_NAME}] Consul is ready ✓")
                    return
        except Exception as e:
            print(f"[{INSTANCE_NAME}] Waiting for Consul (attempt {attempt + 1}): {e}")
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
            print(f"[{INSTANCE_NAME}] Registered with Consul (HTTP {resp.status_code})")
            return
        except Exception as e:
            print(f"[{INSTANCE_NAME}] Consul registration failed (attempt {attempt + 1}): {e}")
            await asyncio.sleep(2)


async def get_kv(key: str) -> str:
    for attempt in range(30):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(f"{CONSUL_URL}/v1/kv/{key}?raw=true", timeout=5.0)
                if resp.status_code == 200:
                    return resp.text.strip()
                print(f"[{INSTANCE_NAME}] KV '{key}' not ready (HTTP {resp.status_code}), retrying...")
        except Exception as e:
            print(f"[{INSTANCE_NAME}] KV error for '{key}' (attempt {attempt+1}): {e}")
        await asyncio.sleep(2)
    raise RuntimeError(f"Consul KV key '{key}' never became available")



@app.on_event("startup")
async def startup():
    global hz_map

    await wait_for_consul()
    await register_with_consul()

    hz_members_s = await get_kv("config/hazelcast/members")
    cluster_name = await get_kv("config/hazelcast/cluster_name")
    hz_members   = [m.strip() for m in hz_members_s.split(",")]

    print(f"[{INSTANCE_NAME}] Hazelcast config from Consul → "
          f"members={hz_members}  cluster={cluster_name}")

    for attempt in range(15):
        try:
            client = hazelcast.HazelcastClient(
                cluster_members=hz_members,
                cluster_name=cluster_name,
            )
            hz_map = client.get_map("transactions_map")
            print(f"[{INSTANCE_NAME}] Connected to Hazelcast ✓")
            break
        except Exception as e:
            print(f"[{INSTANCE_NAME}] Hazelcast not ready (attempt {attempt + 1}): {e}")
            await asyncio.sleep(3)

    print(f"[{INSTANCE_NAME}] Startup complete.")


@app.on_event("shutdown")
async def shutdown():
    try:
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{CONSUL_URL}/v1/agent/service/deregister/{MY_SERVICE_ID}",
                timeout=5.0,
            )
        print(f"[{INSTANCE_NAME}] Deregistered from Consul")
    except Exception as e:
        print(f"[{INSTANCE_NAME}] Deregister error: {e}")



@app.get("/health")
async def health():
    return {"status": "healthy", "instance": INSTANCE_NAME}



class Transaction(BaseModel):
    transaction_ID: str
    user_Id: str
    amount: float


@app.post("/log")
async def log_transaction(tx: Transaction):
    print(f"[{INSTANCE_NAME}] Logging transaction: {tx.dict()}")
    hz_map.set(tx.transaction_ID, tx.dict()).result()
    return {"status": "logged", "instance": INSTANCE_NAME}


@app.get("/log/{user_Id}")
async def get_user_logs(user_Id: str):
    print(f"[{INSTANCE_NAME}] Fetching logs for user={user_Id}")
    entries  = hz_map.entry_set().result()
    user_txs = [val for _, val in entries if val["user_Id"] == user_Id]
    return {"transactions": user_txs, "instance": INSTANCE_NAME}