import asyncio
import aiohttp
import json

FACADE_URL = "http://localhost:8000"
CONSUL_URL = "http://localhost:8500"


async def post_transaction(session, user_id: str, amount: float, label: str):
    async with session.post(
        f"{FACADE_URL}/",
        json={"user_Id": user_id, "amount": amount},
    ) as resp:
        data = await resp.json()
        print(f"  POST [{label}] → {json.dumps(data)}")
        return data


async def get_user(session, user_id: str):
    async with session.get(f"{FACADE_URL}/user/{user_id}") as resp:
        data = await resp.json()
        print(
            f"  GET  user={user_id} → balance={data.get('balance')}, "
            f"txs={len(data.get('transactions', []))}, "
            f"logger={data.get('logger_used')}"
        )
        return data


async def get_all_balances(session):
    async with session.get(f"{FACADE_URL}/balances") as resp:
        data = await resp.json()
        print(f"  GET  /balances → {json.dumps(data, indent=2)}")
        return data


async def get_consul_services(session):
    async with session.get(
        f"{CONSUL_URL}/v1/health/service/logging-service?passing=true"
    ) as resp:
        data = await resp.json()
        print("\n  [Consul] Healthy logging-service instances:")
        for svc in data:
            s = svc["Service"]
            print(f"    ID={s['ID']}  Address={s['Address']}:{s['Port']}")
        return data


async def get_consul_kv(session, key: str):
    async with session.get(f"{CONSUL_URL}/v1/kv/{key}?raw=true") as resp:
        value = await resp.text()
        print(f"  [Consul KV] {key} = {value.strip()}")
        return value


async def main():
    async with aiohttp.ClientSession() as session:

        await get_consul_kv(session, "config/hazelcast/members")
        await get_consul_kv(session, "config/hazelcast/cluster_name")
        await get_consul_kv(session, "config/mq/queue_name")

        await get_consul_services(session)

        print("\nSending 10 Transactions (userA)")
        for i in range(1, 11):
            await post_transaction(session, user_id="userA", amount=10.0, label=f"tx{i}")
            await asyncio.sleep(0.1)

        await asyncio.sleep(3)

        await get_user(session, "userA")
        await get_all_balances(session)

if __name__ == "__main__":
    asyncio.run(main())
