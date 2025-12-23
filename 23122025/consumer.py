import struct
import re
import threading
import queue
from kafka import KafkaConsumer

HEADER_SIZE = 24

major_queue = queue.Queue(maxsize=1000)

consumer = KafkaConsumer(
    "ch10.raw.frames",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="ch10-consumer",
    value_deserializer=lambda v: v
)

def extract_pcm_structure(tmats_bytes):
    text = tmats_bytes.decode(errors="ignore")

    f1  = int(re.search(r'P-\d+\\F1:(\d+);', text).group(1))
    mf1 = int(re.search(r'P-\d+\\MF1:(\d+);', text).group(1))
    mf2 = int(re.search(r'P-\d+\\MF2:(\d+);', text).group(1))

    word_bytes = f1 // 8
    major_frame_bytes = word_bytes * mf1 * mf2

    return word_bytes, major_frame_bytes


def poll_kafka():
    for msg in consumer:
        major_queue.put(msg.value)


def major_frame_worker():
    pcm_buffer = b''
    tmats_buffer = b''
    word_bytes = None
    major_frame_size = None
    tmats_parsed = False
    frame_no = 1

    with open("major_frames.txt", "w") as out:
        while True:
            packet = major_queue.get()

            payload = packet[HEADER_SIZE:]

            # STEP 1: Accumulate TMATS until parsed (ONCE)
            if not tmats_parsed:
                tmats_buffer += payload
                try:
                    word_bytes, major_frame_size = extract_pcm_structure(tmats_buffer)
                    tmats_parsed = True
                    print("TMATS parsed")
                except:
                    continue
                continue   # IMPORTANT: wait for next packet

            # STEP 2: Accumulate PCM stream
            pcm_buffer += payload

            # STEP 3: Slice MAJOR frames by size
            while len(pcm_buffer) >= major_frame_size:
                frame = pcm_buffer[:major_frame_size]
                pcm_buffer = pcm_buffer[major_frame_size:]

                out.write(f"MAJOR FRAME {frame_no}\n")

                for i in range(0, major_frame_size, word_bytes):
                    out.write(frame[i:i+word_bytes].hex() + " ")

                out.write("\n\n")
                frame_no += 1


t_poll = threading.Thread(target=poll_kafka, daemon=True)
t_major = threading.Thread(target=major_frame_worker, daemon=True)

t_poll.start()
t_major.start()

t_poll.join()
