from fastapi import FastAPI
from pydantic import BaseModel
import hazelcast
import httpx
import os
import asyncio

app = FastAPI()

HZ_MEMBERS = os.getenv("HZ_MEMBERS", "hz1,hz2,hz3").split(",")
INSTANCE_NAME = os.getenv("INSTANCE_NAME", "logging_unknown")
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8000")
MY_SERVICE_NAME = os.getenv("MY_SERVICE_NAME", "logging-service")
MY_SERVICE_URL = os.getenv("MY_SERVICE_URL", "http://logging1:8000")

hz_map = None


async def register_with_config_server():
    for attempt in range(15):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{CONFIG_SERVER_URL}/register",
                    json={"service_name": MY_SERVICE_NAME, "url": MY_SERVICE_URL},
                    timeout=5.0,
                )
                print(f"[{INSTANCE_NAME}] Registered with config-server: {resp.json()}")
                return
        except Exception as e:
            print(f"[{INSTANCE_NAME}] Config-server not ready (attempt {attempt + 1}): {e}")
            await asyncio.sleep(2)


@app.on_event("startup")
async def startup():
    global hz_map

    for attempt in range(15):
        try:
            client = hazelcast.HazelcastClient(
                cluster_members=HZ_MEMBERS,
                cluster_name="dev",
            )
            hz_map = client.get_map("transactions_map")
            print(f"[{INSTANCE_NAME}] Connected to Hazelcast ✓")
            break
        except Exception as e:
            print(f"[{INSTANCE_NAME}] Hazelcast not ready (attempt {attempt + 1}): {e}")
            await asyncio.sleep(3)

    await register_with_config_server()
    print(f"[{INSTANCE_NAME}] Startup complete.")


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
    entries = hz_map.entry_set().result()
    user_txs = [val for _, val in entries if val["user_Id"] == user_Id]
    return {"transactions": user_txs, "instance": INSTANCE_NAME}
