import hazelcast
import time
import multiprocessing

def increment_without_lock():
    client = hazelcast.HazelcastClient()
    my_map = client.get_map("concurrency-map").blocking()
    my_map.put_if_absent("key_no_lock", 0)

    for _ in range(10000):
        val = my_map.get("key_no_lock")
        val += 1
        my_map.put("key_no_lock", val)
    client.shutdown()

def increment_pessimistic_lock():
    client = hazelcast.HazelcastClient()
    my_map = client.get_map("concurrency-map").blocking()
    my_map.put_if_absent("key_pessimistic", 0)

    for _ in range(10000):
        my_map.lock("key_pessimistic")
        try:
            val = my_map.get("key_pessimistic")
            val += 1
            my_map.put("key_pessimistic", val)
        finally:
            my_map.unlock("key_pessimistic")
    client.shutdown()

def increment_optimistic_lock():
    client = hazelcast.HazelcastClient()
    my_map = client.get_map("concurrency-map").blocking()
    my_map.put_if_absent("key_optimistic", 0)

    for _ in range(10000):
        while True:
            old_val = my_map.get("key_optimistic")
            new_val = old_val + 1
            if my_map.replace_if_same("key_optimistic", old_val, new_val):
                break
    client.shutdown()

def run_test(target_func, name):
    print(f"Starting test: {name}")
    start_time = time.time()
    processes = [multiprocessing.Process(target=target_func) for _ in range(3)]

    for p in processes: p.start()
    for p in processes: p.join()

    print(f"{name} finished in {time.time() - start_time:.2f} seconds.")

if __name__ == "__main__":
    client = hazelcast.HazelcastClient()
    main_map = client.get_map("concurrency-map").blocking()
    main_map.clear()

    # Task 4: No Lock
    run_test(increment_without_lock, "No Lock")
    print("Final value (No Lock):", main_map.get("key_no_lock")) # Will NOT be 30,000

    # Task 5: Pessimistic Lock
    run_test(increment_pessimistic_lock, "Pessimistic Lock")
    print("Final value (Pessimistic):", main_map.get("key_pessimistic")) # Will be 30,000

    # Task 6: Optimistic Lock
    run_test(increment_optimistic_lock, "Optimistic Lock")
    print("Final value (Optimistic):", main_map.get("key_optimistic")) # Will be 30,000

    client.shutdown()