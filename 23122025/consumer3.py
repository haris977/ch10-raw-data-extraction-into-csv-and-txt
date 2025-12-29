import struct
import re
import threading
import queue
from kafka import KafkaConsumer

HEADER_SIZE = 24

major_q = queue.Queue(maxsize=1000)
time_q  = queue.Queue(maxsize=1000)

consumer = KafkaConsumer(
    "ch10.raw.frames",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="ch10-consumer",
    value_deserializer=lambda v: v
)

def extract_pcm_structure_from_tmats(tmats_bytes):
    text = tmats_bytes.decode(errors="ignore")

    f1  = int(re.search(r'P-\d+\\F1:(\d+);', text).group(1))
    mf1 = int(re.search(r'P-\d+\\MF1:(\d+);', text).group(1))
    mf2 = int(re.search(r'P-\d+\\MF2:(\d+);', text).group(1))

    word_bytes = f1 // 8
    major_frame_size = word_bytes * mf1 * mf2

    return word_bytes, major_frame_size

def poll_kafka():
    for msg in consumer:
        packet = msg.value
        payload = packet[HEADER_SIZE:]

        major_q.put(payload)
        time_q.put(payload)

def major_frame_worker():
    pcm_buffer = b''
    tmats_buffer = b''
    word_bytes = None
    major_frame_size = None
    tmats_parsed = False
    idx = 1

    with open("major_frames_3.txt", "w") as out:
        while True:
            payload = major_q.get()

            if not tmats_parsed:
                tmats_buffer += payload
                try:
                    word_bytes, major_frame_size = extract_pcm_structure_from_tmats(tmats_buffer)
                    tmats_parsed = True
                    print("TMATS parsed (major worker)")
                except:
                    continue
                continue

            pcm_buffer += payload

            while len(pcm_buffer) >= major_frame_size:
                frame = pcm_buffer[:major_frame_size]
                pcm_buffer = pcm_buffer[major_frame_size:]

                out.write(f"MAJOR FRAME {idx}\n")
                for i in range(0, major_frame_size, word_bytes):
                    out.write(frame[i:i+word_bytes].hex() + " ")
                out.write("\n\n")
                out.flush()
                idx += 1

# def time_slice_worker(slice_packets=50):
#     buf = []
#     slice_id = 1

#     with open("time_slices_3.txt", "w") as out:
#         while True:
#             payload = time_q.get()
#             buf.append(payload)

#             if len(buf) >= slice_packets:
#                 out.write(f"TIME SLICE {slice_id} ({len(buf)} packets)\n")
#                 for p in buf:
#                     out.write(p.hex() + " ")
#                 out.write("\n\n")
#                 out.flush()

#                 buf.clear()
#                 slice_id += 1

def time_slice_worker(slice_us=100_000):  # 100 ms
    current_slice_start = None
    slice_packets = []
    slice_id = 1

    with open("time_slices.txt", "w") as out:
        while True:
            payload = time_q.get()
            packet = payload  # payload came from poll thread

            # extract timestamp (example offset)
            ts = struct.unpack("<Q", packet[8:16])[0]

            if current_slice_start is None:
                current_slice_start = ts

            if ts - current_slice_start <= slice_us:
                slice_packets.append(packet)
            else:
                # flush old slice
                out.write(f"TIME SLICE {slice_id} ({len(slice_packets)} packets)\n")
                for p in slice_packets:
                    out.write(p.hex() + " ")
                out.write("\n\n")
                out.flush()

                # start new slice
                slice_packets = [packet]
                current_slice_start = ts
                slice_id += 1


t_poll  = threading.Thread(target=poll_kafka, daemon=True)
t_major = threading.Thread(target=major_frame_worker, daemon=True)
t_time  = threading.Thread(target=time_slice_worker, daemon=True)

t_poll.start()
t_major.start()
t_time.start()

t_poll.join()

