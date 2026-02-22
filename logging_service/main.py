from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict

app = FastAPI()

transactions_db: Dict[str, dict] = {}

class Transaction(BaseModel):
    transaction_ID: str
    user_Id: str
    amount: float

@app.post("/log")
async def log_transaction(tx: Transaction):
    print(f"Received transaction: {tx.dict()}")
    transactions_db[tx.transaction_ID] = tx.dict()
    return {"status": "logged"}

@app.get("/log/{user_Id}")
async def get_user_logs(user_Id: str):
    user_txs = [tx for tx in transactions_db.values() if tx["user_Id"] == user_Id]
    return {"transactions": user_txs}