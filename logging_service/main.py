from fastapi import FastAPI
from pydantic import BaseModel
import hazelcast
import os

app = FastAPI()

HZ_MEMBERS = os.getenv("HZ_MEMBERS", "localhost").split(",")
INSTANCE_NAME = os.getenv("INSTANCE_NAME", "logging_unknown")

client = hazelcast.HazelcastClient(
    cluster_members=HZ_MEMBERS,
    cluster_name="dev"
)
hz_map = client.get_map("transactions_map")

class Transaction(BaseModel):
    transaction_ID: str
    user_Id: str
    amount: float

@app.post("/log")
async def log_transaction(tx: Transaction):
    print(f"[{INSTANCE_NAME}] Received transaction: {tx.dict()}")
    hz_map.set(tx.transaction_ID, tx.dict()).result()
    return {"status": "logged", "instance": INSTANCE_NAME}

@app.get("/log/{user_Id}")
async def get_user_logs(user_Id: str):
    print(f"[{INSTANCE_NAME}] Fetching logs for {user_Id}")
    entries = hz_map.entry_set().result()
    user_txs = [val for key, val in entries if val["user_Id"] == user_Id]
    return {"transactions": user_txs, "instance": INSTANCE_NAME}