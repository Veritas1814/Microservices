
import asyncio
import aiohttp
import time

FACADE_URL = "http://localhost:8000"
TX_PER_USER = 1000


async def send_requests(session, user_id, amount, count):
    payload = {"user_Id": user_id, "amount": amount}
    for _ in range(count):
        async with session.post(FACADE_URL + "/", json=payload) as resp:
            await resp.read()


async def run_scenario(scenario_name: str, users: list, tx_per_user: int):
    print(f"  {scenario_name}")
    print(f"  Users: {len(set(users))} distinct  |  TX/user: {tx_per_user}  |  "
          f"Total TX: {len(users) * tx_per_user}")

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{FACADE_URL}/metrics?reset=true"):
            pass

        start = time.time()

        tasks = [send_requests(session, uid, 1.0, tx_per_user) for uid in users]
        await asyncio.gather(*tasks)

        elapsed   = time.time() - start
        total_tx  = len(users) * tx_per_user
        rps       = total_tx / elapsed

        async with session.get(f"{FACADE_URL}/metrics") as resp:
            m = await resp.json()

        print(f"\n  Total wall-clock time :  {elapsed:.2f} s")
        print(f"  Throughput            :  {rps:.1f} req/s")
        print(f"  logging-service time  :  {m['logging_time']:.2f} s  "
              f"({100*m['logging_time']/elapsed:.1f}% of wall time)")
        print(f"  counter-service time  :  {m['counter_time']:.2f} s  "
              f"({100*m['counter_time']/elapsed:.1f}% of wall time)")
        print(f"  Requests counted      :  {m['request_count']}")

    return {
        "total_time":      round(elapsed, 2),
        "rps":             round(rps, 1),
        "logging_time":    round(m["logging_time"], 2),
        "counter_time":    round(m["counter_time"], 2),
        "request_count":   m["request_count"],
    }


async def main():
    users_s1 = [f"perf_user_{i}" for i in range(1, 11)]
    r1 = await run_scenario(
        "Scenario 1 – 10 accounts (distinct)", users_s1, TX_PER_USER
    )

    users_s2 = ["shared_perf_user"] * 10
    r2 = await run_scenario(
        "Scenario 2 – 1 account (shared, high contention)", users_s2, TX_PER_USER
    )

    print(f"  {'Metric':<35} {'Scenario 1':>12} {'Scenario 2':>12}")
    print(f"  {'-'*35} {'-'*12} {'-'*12}")
    print(f"  {'Total time (s)':<35} {r1['total_time']:>12} {r2['total_time']:>12}")
    print(f"  {'logging-service contribution (s)':<35} {r1['logging_time']:>12} "
          f"{r2['logging_time']:>12}")
    print(f"  {'counter-service contribution (s)':<35} {r1['counter_time']:>12} "
          f"{r2['counter_time']:>12}")
    print(f"  {'Throughput (req/s)':<35} {r1['rps']:>12} {r2['rps']:>12}")


if __name__ == "__main__":
    asyncio.run(main())
