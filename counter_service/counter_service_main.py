from fastapi import FastAPI
from pydantic import BaseModel
import asyncpg
import hazelcast
import os
import asyncio
import json
import httpx

app = FastAPI()

CONSUL_URL      = os.getenv("CONSUL_URL",      "http://consul:8500")
DB_URL          = os.getenv("DB_URL",          "postgresql://admin:password@localhost:5432/counter_db")
MY_SERVICE_NAME = os.getenv("MY_SERVICE_NAME", "counter-service")
MY_SERVICE_ID   = os.getenv("MY_SERVICE_ID",   "counter-service-1")
MY_ADDRESS      = os.getenv("MY_ADDRESS",      "counter-service")
MY_PORT         = int(os.getenv("MY_PORT",     "8000"))

pool      = None
hz_client = None
hz_queue  = None


class Transaction(BaseModel):
    user_Id: str
    amount: float



async def wait_for_consul():
    for attempt in range(30):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(f"{CONSUL_URL}/v1/status/leader", timeout=3.0)
                if resp.status_code == 200 and resp.text.strip('"'):
                    print("[counter-service] Consul is ready ✓")
                    return
        except Exception as e:
            print(f"[counter-service] Waiting for Consul (attempt {attempt + 1}): {e}")
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
            print(f"[counter-service] Registered with Consul (HTTP {resp.status_code})")
            return
        except Exception as e:
            print(f"[counter-service] Consul registration failed (attempt {attempt + 1}): {e}")
            await asyncio.sleep(2)


async def get_kv(key: str) -> str:
    for attempt in range(30):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(f"{CONSUL_URL}/v1/kv/{key}?raw=true", timeout=5.0)
                if resp.status_code == 200:
                    return resp.text.strip()
                print(f"[counter-service] KV '{key}' not ready (HTTP {resp.status_code}), retrying...")
        except Exception as e:
            print(f"[counter-service] KV error for '{key}' (attempt {attempt+1}): {e}")
        await asyncio.sleep(2)
    raise RuntimeError(f"Consul KV key '{key}' never became available")



async def update_balance_internal(user_id: str, amount: float):
    async with pool.acquire() as conn:
        new_balance = await conn.fetchval(
            """
            INSERT INTO balances (user_id, balance)
            VALUES ($1, $2)
                ON CONFLICT (user_id)
            DO UPDATE SET balance = balances.balance + $2
                                   RETURNING balance
            """,
            user_id, amount,
        )
    print(f"[counter-service] Balance updated → user={user_id}  "
          f"amount={amount}  new_balance={new_balance}")
    return new_balance



async def consume_queue(queue_name: str):
    print(f"[counter-service] Consumer started on queue '{queue_name}' ...")
    loop = asyncio.get_event_loop()
    while True:
        try:
            msg_str = await loop.run_in_executor(None, lambda: hz_queue.take().result())
            tx      = json.loads(msg_str)
            print(f"[counter-service] Dequeued: {tx}")
            await update_balance_internal(tx["user_Id"], tx["amount"])
        except Exception as e:
            print(f"[counter-service] Consumer error: {e}")
            await asyncio.sleep(1)


@app.on_event("startup")
async def startup():
    global pool, hz_client, hz_queue

    await wait_for_consul()
    await register_with_consul()

    queue_name   = await get_kv("config/mq/queue_name")
    hz_members_s = await get_kv("config/hazelcast/members")
    cluster_name = await get_kv("config/hazelcast/cluster_name")
    hz_members   = [m.strip() for m in hz_members_s.split(",")]

    print(f"[counter-service] Config from Consul → queue={queue_name}  "
          f"hz_members={hz_members}  cluster={cluster_name}")

    for attempt in range(15):
        try:
            pool = await asyncpg.create_pool(DB_URL)
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS balances (
                                                            user_id VARCHAR(50) PRIMARY KEY,
                        balance FLOAT NOT NULL
                        )
                    """
                )
            print("[counter-service] Connected to PostgreSQL ✓")
            break
        except Exception as e:
            print(f"[counter-service] DB not ready (attempt {attempt + 1}): {e}")
            await asyncio.sleep(3)

    for attempt in range(15):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_members=hz_members,
                cluster_name=cluster_name,
            )
            hz_queue  = hz_client.get_queue(queue_name)
            print("[counter-service] Connected to Hazelcast ✓")
            break
        except Exception as e:
            print(f"[counter-service] Hazelcast not ready (attempt {attempt + 1}): {e}")
            await asyncio.sleep(3)

    asyncio.create_task(consume_queue(queue_name))
    print("[counter-service] Startup complete.")


@app.on_event("shutdown")
async def shutdown():
    try:
        async with httpx.AsyncClient() as client:
            await client.put(
                f"{CONSUL_URL}/v1/agent/service/deregister/{MY_SERVICE_ID}",
                timeout=5.0,
            )
        print("[counter-service] Deregistered from Consul")
    except Exception as e:
        print(f"[counter-service] Deregister error: {e}")



@app.get("/health")
async def health():
    return {"status": "healthy", "service": MY_SERVICE_NAME}


@app.get("/balance/{user_Id}")
async def get_balance(user_Id: str):
    async with pool.acquire() as conn:
        balance = await conn.fetchval(
            "SELECT balance FROM balances WHERE user_id = $1", user_Id
        )
    return {"user_Id": user_Id, "balance": balance or 0.0}


@app.get("/balances")
async def get_all_balances():
    async with pool.acquire() as conn:
        records = await conn.fetch("SELECT * FROM balances")
    return {rec["user_id"]: rec["balance"] for rec in records}


@app.post("/update")
async def update_balance(tx: Transaction):
    new_balance = await update_balance_internal(tx.user_Id, tx.amount)
    return {"user_Id": tx.user_Id, "balance": new_balance}