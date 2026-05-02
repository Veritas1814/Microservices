from fastapi import FastAPI
from pydantic import BaseModel
import asyncpg
import hazelcast
import os
import asyncio
import json
import httpx

app = FastAPI()

DB_URL = os.getenv("DB_URL", "postgresql://admin:password@localhost:5432/counter_db")
HZ_MEMBERS = os.getenv("HZ_MEMBERS", "hz1,hz2,hz3").split(",")
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8000")
MY_SERVICE_NAME = os.getenv("MY_SERVICE_NAME", "counter-service")
MY_SERVICE_URL = os.getenv("MY_SERVICE_URL", "http://counter-service:8000")

pool = None
hz_client = None
hz_queue = None


class Transaction(BaseModel):
    user_Id: str
    amount: float


async def register_with_config_server():
    for attempt in range(15):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{CONFIG_SERVER_URL}/register",
                    json={"service_name": MY_SERVICE_NAME, "url": MY_SERVICE_URL},
                    timeout=5.0,
                )
                print(f"[counter-service] Registered with config-server: {resp.json()}")
                return
        except Exception as e:
            print(f"[counter-service] Config-server not ready (attempt {attempt + 1}): {e}")
            await asyncio.sleep(2)


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
            user_id,
            amount,
        )
    print(f"[counter-service] Balance updated -> user={user_id}, amount={amount}, new_balance={new_balance}")
    return new_balance


async def consume_queue():
    """Background task: reads from Hazelcast 'counter-queue' and updates the DB."""
    print("[counter-service] Queue consumer started, waiting for messages...")
    loop = asyncio.get_event_loop()
    while True:
        try:
            msg_str = await loop.run_in_executor(None, lambda: hz_queue.take().result())
            tx = json.loads(msg_str)
            print(f"[counter-service] Dequeued message: {tx}")
            await update_balance_internal(tx["user_Id"], tx["amount"])
        except Exception as e:
            print(f"[counter-service] Queue consumer error: {e}")
            await asyncio.sleep(1)


@app.on_event("startup")
async def startup():
    global pool, hz_client, hz_queue

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
                cluster_members=HZ_MEMBERS,
                cluster_name="dev",
            )
            hz_queue = hz_client.get_queue("counter-queue")
            print("[counter-service] Connected to Hazelcast ✓")
            break
        except Exception as e:
            print(f"[counter-service] Hazelcast not ready (attempt {attempt + 1}): {e}")
            await asyncio.sleep(3)

    await register_with_config_server()

    asyncio.create_task(consume_queue())
    print("[counter-service] Startup complete.")


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
