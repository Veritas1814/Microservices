from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncpg
import os
import asyncio

app = FastAPI()
DB_URL = os.getenv("DB_URL", "postgresql://admin:password@localhost:5432/counter_db")
pool = None

class Transaction(BaseModel):
    user_Id: str
    amount: float

@app.on_event("startup")
async def startup():
    global pool
    for _ in range(10):
        try:
            pool = await asyncpg.create_pool(DB_URL)
            async with pool.acquire() as conn:
                await conn.execute('''
                                   CREATE TABLE IF NOT EXISTS balances (
                                                                           user_id VARCHAR(50) PRIMARY KEY,
                                       balance FLOAT NOT NULL
                                       )
                                   ''')
            print("Successfully connected to the database!")
            break
        except Exception as e:
            print(f"Database not ready yet, retrying in 3 seconds... Error: {e}")
            await asyncio.sleep(3)

@app.post("/update")
async def update_balance(tx: Transaction):
    async with pool.acquire() as conn:
        new_balance = await conn.fetchval('''
                                          INSERT INTO balances (user_id, balance)
                                          VALUES ($1, $2)
                                              ON CONFLICT (user_id) 
            DO UPDATE SET balance = balances.balance + $2
                                                                 RETURNING balance
                                          ''', tx.user_Id, tx.amount)
    return {"user_Id": tx.user_Id, "balance": new_balance}

@app.get("/balance/{user_Id}")
async def get_balance(user_Id: str):
    async with pool.acquire() as conn:
        balance = await conn.fetchval('SELECT balance FROM balances WHERE user_id = $1', user_Id)
    return {"balance": balance or 0.0}

@app.get("/balances")
async def get_all_balances():
    async with pool.acquire() as conn:
        records = await conn.fetch('SELECT * FROM balances')
    return {rec['user_id']: rec['balance'] for rec in records}