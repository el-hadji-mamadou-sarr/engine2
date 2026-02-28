import struct

format = "> I 20s d" # 4 + 8 + 20 = 32 bytes
record_size = struct.calcsize(format)
print(record_size)

with open("data.bin", "wb") as f:
    records = [
        (1, b"Alice", 95.5),
        (2, b"Bob", 87.5),
        (3, b"Carol", 92.3)
    ]
    for r in records:
        f.write(struct.pack(format, r[0], r[1].ljust(20), r[2]))

# read third
with open("data.bin", "rb") as f:
    f.seek(2 * record_size)
    raw = f.read(record_size)
    id, name, score = struct.unpack(format, raw)
    print(name.rstrip(b'\x00').decode('utf-8'))
    