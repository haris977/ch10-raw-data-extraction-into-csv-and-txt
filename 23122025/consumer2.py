import threading
import queue
from kafka import KafkaConsumer

HEADER_SIZE = 24

packet_queue = queue.Queue(maxsize=1000)

consumer = KafkaConsumer(
    "ch10.raw.frames",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="ch10-dump-consumer",
    value_deserializer=lambda v: v
)

def poll_kafka():
    for msg in consumer:
        packet_queue.put(msg.value)

def dump_worker():
    packet_no = 1
    with open("all_packets.txt", "w") as out:
        while True:
            packet = packet_queue.get()

            out.write(f"PACKET {packet_no}\n")

            for i in range(0, len(packet), 4):
                word = packet[i:i+4]
                if len(word) < 4:
                    break
                out.write(word.hex().upper() + " ")

            out.write("\n\n")
            out.flush()
            packet_no += 1

t_poll = threading.Thread(target=poll_kafka, daemon=True)
t_dump = threading.Thread(target=dump_worker, daemon=True)

t_poll.start()
t_dump.start()

t_poll.join()
