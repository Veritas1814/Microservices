import asyncio
import aiohttp
import time

FACADE_URL = "http://localhost:8000/"

async def send_requests(session, user_id, amount, count):
    payload = {"user_Id": user_id, "amount": amount}
    for _ in range(count):
        async with session.post(FACADE_URL, json=payload) as response:
            await response.read()

async def run_scenario(scenario_name, users, tx_per_user):
    print(f"\n--- Running {scenario_name} ---")

    async with aiohttp.ClientSession() as session:
        await session.get(f"{FACADE_URL}metrics?reset=true")

        start_time = time.time()

        tasks = []
        for user_id in users:
            tasks.append(send_requests(session, user_id, 1.0, tx_per_user))

        await asyncio.gather(*tasks)

        total_time = time.time() - start_time
        total_requests = len(users) * tx_per_user
        rps = total_requests / total_time

        print(f"Total Time: {total_time:.2f} seconds")
        print(f"Requests per Second: {rps:.2f}")

        # Fetch metrics
        async with session.get(f"{FACADE_URL}metrics") as resp:
            metrics = await resp.json()
            print(f"Time spent on Logging Service calls: {metrics['logging_time']:.2f}s")
            print(f"Time spent on Counter Service calls: {metrics['counter_time']:.2f}s")

async def main():
    # Scenario 1: 10 clients, 10K txs each on THEIR OWN account [cite: 102, 103]
    users_scen_1 = [f"user_{i}" for i in range(1, 11)]
    await run_scenario("Scenario 1 (Distinct Accounts)", users_scen_1, 10000)

    # Scenario 2: 10 clients, 10K txs each on THE SAME account [cite: 104, 105]
    users_scen_2 = ["shared_user" for _ in range(10)]
    await run_scenario("Scenario 2 (Shared Account)", users_scen_2, 10000)

if __name__ == "__main__":
    asyncio.run(main())