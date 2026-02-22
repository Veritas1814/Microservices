import asyncio
import aiohttp
import time

FACADE_URL = "http://localhost:8000"
NUM_CLIENTS = 10
REQUESTS_PER_CLIENT = 10000

async def make_requests(session, user_id, amount):
    for _ in range(REQUESTS_PER_CLIENT):
        async with session.post(f"{FACADE_URL}/", json={"user_Id": user_id, "amount": amount}) as response:
            await response.json()

async def run_scenario(scenario_name, same_account=False):
    print(f"\n--- Starting {scenario_name} ---")
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(NUM_CLIENTS):
            user_id = "user_shared" if same_account else f"user_{i}"
            tasks.append(make_requests(session, user_id, 1.0))

        await asyncio.gather(*tasks)

    total_time = time.time() - start_time
    total_requests = NUM_CLIENTS * REQUESTS_PER_CLIENT
    rps = total_requests / total_time

    print(f"Total time: {total_time:.2f} seconds")
    print(f"Requests per second (RPS): {rps:.2f}")

    # Check stats
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{FACADE_URL}/stats") as response:
            stats = await response.json()
            print(f"Time spent in Logging Service: {stats['logging_time']:.2f}s")
            print(f"Time spent in Counter Service: {stats['counter_time']:.2f}s")

async def main():
    # Scenario 1: 10 clients, 10K transactions each to their OWN accounts [cite: 102]
    await run_scenario("Scenario 1 (Different Accounts)", same_account=False)

    # Scenario 2: 10 clients, 10K transactions each to the SAME account [cite: 104]
    await run_scenario("Scenario 2 (Same Account)", same_account=True)

if __name__ == "__main__":
    asyncio.run(main())