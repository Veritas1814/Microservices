import asyncio
import aiohttp
import json

FACADE_URL = "http://localhost:8000"
CONFIG_URL  = "http://localhost:8085"


async def post_transaction(session, user_id: str, amount: float, label: str):
    async with session.post(
        f"{FACADE_URL}/",
        json={"user_Id": user_id, "amount": amount},
    ) as resp:
        data = await resp.json()
        print(f"  POST [{label}] -> {json.dumps(data)}")
        return data


async def get_user(session, user_id: str):
    async with session.get(f"{FACADE_URL}/user/{user_id}") as resp:
        data = await resp.json()
        print(f"  GET  user={user_id} -> balance={data.get('balance')}, "
              f"txs={len(data.get('transactions', []))}, "
              f"logger={data.get('logger_used')}")
        return data


async def get_all_balances(session):
    async with session.get(f"{FACADE_URL}/balances") as resp:
        data = await resp.json()
        print(f"  GET  /balances -> {json.dumps(data, indent=2)}")
        return data


async def get_registry(session):
    async with session.get(f"{CONFIG_URL}/registry") as resp:
        data = await resp.json()
        print(f"  Config-server registry:\n{json.dumps(data, indent=2)}")
        return data


async def main():
    async with aiohttp.ClientSession() as session:

        await get_registry(session)

        print("\nSending 10 transactions (msg1-msg10)")
        for i in range(1, 11):
            await post_transaction(session, user_id="userA", amount=10.0, label=f"msg{i}")
            await asyncio.sleep(0.1)

        await asyncio.sleep(3)

        await get_user(session, "userA")
        await get_all_balances(session)



if __name__ == "__main__":
    asyncio.run(main())
