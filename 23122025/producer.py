import struct
from kafka import KafkaProducer

CH10_SYNC = b'\x25\xEB'
HEADER_SIZE = 24

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: v
)

def produce_ch10_frames(ch10_file):
    frame_index = 1

    with open(ch10_file, "rb") as f:
        while True:
            b = f.read(1)
            if not b:
                break

            if b != CH10_SYNC[:1]:
                continue

            if f.read(1) != CH10_SYNC[1:]:
                f.seek(-1, 1)
                continue

            header = CH10_SYNC + f.read(HEADER_SIZE - 2)
            packet_len = struct.unpack("<I", header[4:8])[0]
            packet = header + f.read(packet_len - HEADER_SIZE)

            producer.send(
                topic="ch10.raw.frames",
                key=frame_index.to_bytes(4, "little"),
                value=packet
            )

            frame_index += 1

    producer.flush()
    print("DONE producing frames")

file_name = "../ch10_pcm_decoder/Calculex-PCM20_12052009_212115/data.ch10"

produce_ch10_frames(file_name)
