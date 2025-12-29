import struct
import re

CH10_SYNC = b'\x25\xEB'   # little-endian sync
HEADER_SIZE = 24

def extract_pcm_layout(filename):
    with open(filename, "rb") as f:
        text = f.read(80000).decode(errors="ignore")

    f1 = int(re.search(r'P-\d+\\F1:(\d+);', text).group(1))
    mf2 = int(re.search(r'P-\d+\\MF2:(\d+);', text).group(1))

    word_bytes = f1 // 8
    frame_bytes = word_bytes * mf2

    return word_bytes, mf2, frame_bytes


def extract_frames(ch10_file, out_file):
    word_size, words_per_frame, frame_size = extract_pcm_layout(ch10_file)

    print(f"[INFO] Word size        = {word_size} bytes")
    print(f"[INFO] Words per frame  = {words_per_frame}")
    print(f"[INFO] Frame size       = {frame_size} bytes")

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

        # ✅ Summary block at the end
        total_frames = frame_index - 1

        out.write("================================\n")
        out.write(f"TOTAL_FRAMES = {total_frames}\n")
        out.write(f"WORD_SIZE_BYTES = {word_size}\n")
        out.write(f"WORDS_PER_FRAME = {words_per_frame}\n")
        out.write(f"FRAME_SIZE_BYTES = {frame_size}\n")
        out.write("================================\n")

    print(f"[DONE] Total frames written: {total_frames}")

file_name1 = "C:\Haris\python\ch10_pcm_decoder\Calculex-PCM20_12052009_212115/"
file_name = "../../ch10_pcm_decoder/Calculex-PCM20_12052009_212115/data.ch10"
extract_frames(file_name, "real.txt")
