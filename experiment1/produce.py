import struct
import re

CH10_SYNC = b'\xEB\x25'
HEADER_SIZE = 24

def extract_frame_size_from_tmats(filename):
    with open(filename, "rb") as f:
        text = f.read(50000).decode(errors="ignore")

    match = re.search(r'P-\d+\\F1:(\d+);', text)
    if not match:
        raise RuntimeError("Frame size not found in TMATS")

    return int(match.group(1)) // 8


def extract_frames(ch10_file, out_file):
    frame_size = extract_frame_size_from_tmats(ch10_file)
    print(f"[INFO] Frame size = {frame_size} bytes")

    frame_index = 0

    with open(ch10_file, "rb") as f, open(out_file, "w") as out:
        out.write("frame_index,hex_data\n")

        while True:
            byte = f.read(1)
            if not byte:
                break

            # 🔍 Scan for sync
            if byte != CH10_SYNC[:1]:
                continue

            next_byte = f.read(1)
            if next_byte != CH10_SYNC[1:]:
                f.seek(-1, 1)
                continue

            # ✅ Sync found — read rest of header
            rest = f.read(HEADER_SIZE - 2)
            if len(rest) < HEADER_SIZE - 2:
                break

            header = CH10_SYNC + rest

            packet_len = struct.unpack("<I", header[4:8])[0]

            # Sanity check
            if packet_len < HEADER_SIZE or packet_len > 50_000_000:
                continue

            payload_len = packet_len - HEADER_SIZE
            payload = f.read(payload_len)

            if payload_len < frame_size:
                continue

            # 🔥 Extract frames
            for i in range(0, payload_len - frame_size + 1, frame_size):
                frame = payload[i:i+frame_size]
                out.write(f"{frame_index},{frame.hex()}\n")
                frame_index += 1

    print(f"[DONE] Frames extracted: {frame_index}")


extract_frames("data.ch10", "frames.csv")
