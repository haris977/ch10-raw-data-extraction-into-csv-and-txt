import struct
import re

CH10_SYNC = b'\x25\xEB'
HEADER_SIZE = 24

def extract_pcm_structure(filename):
    with open(filename, "rb") as f:
        text = f.read(100000).decode(errors="ignore")

    f1  = int(re.search(r'P-\d+\\F1:(\d+);', text).group(1))
    mf2 = int(re.search(r'P-\d+\\MF2:(\d+);', text).group(1))
    mf1 = int(re.search(r'P-\d+\\MF1:(\d+);', text).group(1))

    word_bytes = f1 // 8
    major_frame_bytes = word_bytes * mf2 * mf1

    return word_bytes, major_frame_bytes


def dump_major_frames(ch10_file, out_file):
    word_size, major_frame_size = extract_pcm_structure(ch10_file)

    print(f"[INFO] Major frame size = {major_frame_size} bytes")

    buffer = b''
    major_index = 1

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

            # accumulate PCM stream
            buffer += payload

            # extract ONLY major frames
            while len(buffer) >= major_frame_size:
                frame = buffer[:major_frame_size]
                buffer = buffer[major_frame_size:]

                out.write(f"MAJOR FRAME {major_index}\n")

                words = [
                    frame[i:i+word_size].hex()
                    for i in range(0, major_frame_size, word_size)
                ]

                out.write(" ".join(words) + "\n\n")
                major_index += 1

    print(f"[DONE] Major frames dumped: {major_index - 1}")

file_name = "../../ch10_pcm_decoder/Calculex-PCM20_12052009_212115/data.ch10"
dump_major_frames(file_name, "major_frames.txt")
