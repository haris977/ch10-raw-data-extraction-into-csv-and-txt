import struct

CH10_SYNC = b'\x25\xEB'
HEADER_SIZE = 24

def dump_ch10_like_friend(ch10_file, out_file):
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

            packet = header + f.read(packet_len - HEADER_SIZE)

            out.write(f"Frame {frame_index}\n")

            for i in range(0, len(packet), 4):
                word = packet[i:i+4]
                if len(word) < 4:
                    break

                # 🔥 KEY LINE — endian fix
                out.write(word[::-1].hex().upper() + " ")

            out.write("\n\n")
            frame_index += 1

    print(f"[DONE] Total packets dumped: {frame_index - 1}")

file_name = "../../ch10_pcm_decoder/Calculex-PCM20_12052009_212115/data.ch10"
dump_ch10_like_friend(file_name, "matched_friend_output.txt")
