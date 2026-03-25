import hazelcast
import multiprocessing
import time

def producer():
    client = hazelcast.HazelcastClient()
    queue = client.get_queue("bounded-queue").blocking()

    queue.clear()

    for i in range(1, 101):
        queue.put(i)
        print(f"Produced: {i}")
    client.shutdown()

def consumer(consumer_id):
    client = hazelcast.HazelcastClient()
    queue = client.get_queue("bounded-queue").blocking()

    while True:
        item = queue.take()
        print(f"Consumer {consumer_id} read: {item}")
        time.sleep(0.1)
    client.shutdown()

if __name__ == "__main__":
    c1 = multiprocessing.Process(target=consumer, args=(1,))
    c2 = multiprocessing.Process(target=consumer, args=(2,))
    # c1.start()
    # c2.start()

    time.sleep(1)
    p = multiprocessing.Process(target=producer)
    p.start()

    p.join()
    c1.terminate()
    c2.terminate()