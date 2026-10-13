import os
import signal
import time

from confluent_kafka import Consumer

BOOTSTRAP = os.getenv("BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "orders")
GROUP = os.getenv("GROUP", "scoring")
PROCESS_MS = int(os.getenv("PROCESS_MS", "0"))  # fake work per message (drill 8: raise it to build lag)
NAME = os.getenv("HOSTNAME", "consumer")

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": GROUP,
    "client.id": NAME,
    "enable.auto.commit": False,     # we commit ourselves, AFTER processing = at-least-once
    "auto.offset.reset": "earliest", # no saved offset yet? start from the beginning
})

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)


def on_assign(c, partitions):
    print(f"[{NAME}] ASSIGNED {[p.partition for p in partitions]}")


def on_revoke(c, partitions):
    print(f"[{NAME}] REVOKED  {[p.partition for p in partitions]}")


consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)

while running:
    msg = consumer.poll(1.0)  # also sends heartbeats and runs rebalance callbacks
    if msg is None:
        continue
    if msg.error():
        print(f"[{NAME}] error: {msg.error()}")
        continue

    time.sleep(PROCESS_MS / 1000)  # "process" the record
    print(f"[{NAME}] processed key={msg.key().decode()} partition {msg.partition()} @ offset {msg.offset()}")

    consumer.commit(message=msg, asynchronous=False)  # commits offset+1 for us

print(f"[{NAME}] leaving group cleanly")
consumer.close()  # tells the coordinator we left -> instant rebalance