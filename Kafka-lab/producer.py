import os
import random
import signal
import time

from confluent_kafka import Producer

BOOTSTRAP = os.getenv("BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "orders")
RATE = float(os.getenv("RATE", "2"))  # messages per second

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "client.id": os.getenv("HOSTNAME", "producer"),
    "acks": "all",                    # wait for every in-sync replica
    "enable.idempotence": True,       # no duplicates from retries (off by default in Python!)
    "partitioner": "murmur2_random",  # same key -> same partition as the Java client
    "delivery.timeout.ms": 120000,    # give up on a record after 2 min
})

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGTERM, stop)  # sent by Kubernetes when the pod is deleted
signal.signal(signal.SIGINT, stop)   # Ctrl+C


def on_delivery(err, msg):
    key = msg.key().decode()
    if err:
        print(f"FAILED  key={key} error={err}")
    else:
        print(f"ok      key={key} -> partition {msg.partition()} @ offset {msg.offset()}")


keys = ["user-1", "user-2", "user-3", "user-4", "user-5"]
n = 0
while running:
    key = random.choice(keys)
    try:
        producer.produce(TOPIC, key=key, value=f"event-{n}", on_delivery=on_delivery)
        n += 1
    except BufferError:
        print("local queue full (broker not acking?), waiting...")
    producer.poll(0)  # runs delivery callbacks for finished sends
    time.sleep(30)

print("shutting down, flushing pending messages...")
producer.flush(10)