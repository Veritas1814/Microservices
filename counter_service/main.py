from fastapi import FastAPI
from pydantic import BaseModel
from collections import defaultdict
import asyncio

app = FastAPI()

balances_db = defaultdict(float)
lock = asyncio.Lock()

class Transaction(BaseModel):
    user_Id: str
    amount: float

@app.post("/update")
async def update_balance(tx: Transaction):
    async with lock:
        balances_db[tx.user_Id] += tx.amount
        current_balance = balances_db[tx.user_Id]
    return {"user_Id": tx.user_Id, "balance": current_balance}

@app.get("/balance/{user_Id}")
async def get_balance(user_Id: str):
    return {"balance": balances_db.get(user_Id, 0.0)}

@app.get("/balances")
async def get_all_balances():
    return dict(balances_db)