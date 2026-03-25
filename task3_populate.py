import hazelcast

if __name__ == "__main__":
    client = hazelcast.HazelcastClient(
        cluster_members=["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"]
    )

    my_map = client.get_map("test-map").blocking()

    for i in range(1001):
        my_map.put(i, f"value_{i}")

    print("Successfully inserted 1000 elements.")
    client.shutdown()