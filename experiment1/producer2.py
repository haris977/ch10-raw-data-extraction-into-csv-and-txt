import struct
import re

CH10_SYNC = b'\x25\xEB'   # little-endian
HEADER_SIZE = 24

def extract_pcm_layout(filename):
    with open(filename, "rb") as f:
        text = f.read(80000).decode(errors="ignore")

    f1 = int(re.search(r'P-\d+\\F1:(\d+);', text).group(1))
    mf2 = int(re.search(r'P-\d+\\MF2:(\d+);', text).group(1))

    word_bytes = f1 // 8
    frame_bytes = word_bytes * mf2

    return word_bytes, frame_bytes


def extract_frames(ch10_file, out_file):
    word_size, frame_size = extract_pcm_layout(ch10_file)

    print(f"[INFO] Word size   = {word_size} bytes")
    print(f"[INFO] Frame size  = {frame_size} bytes")

    frame_index = 1

    with open(ch10_file, "rb") as f, open(out_file, "w") as out:
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

            payload = f.read(packet_len - HEADER_SIZE)

            offset = 0
            while offset + frame_size <= len(payload):
                frame = payload[offset:offset + frame_size]

                out.write(f"FRAME {frame_index}\n")

                words = [
                    frame[i:i+word_size].hex()
                    for i in range(0, frame_size, word_size)
                ]

                out.write(" ".join(words) + "\n\n")

                frame_index += 1
                offset += frame_size

    print(f"[DONE] Total frames written: {frame_index - 1}")


extract_frames("data.ch10", "frames.txt")
