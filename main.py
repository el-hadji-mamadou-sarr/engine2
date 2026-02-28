import struct
from typing import Literal

class RecordSerializer:
    def __init__(self, schema: list[tuple[str, str]]):
        self.schema = schema
        self.format = self._get_format()
        self.record_size = struct.calcsize(self.format)
    
    def _get_format(self):
        format = "> "
        for item in self.schema:
            _, type = item
            elt = type.split(':')
            match elt[0]:
                case "int32":
                    format += "i "
                case "string":
                    format += f"{elt[-1]}s "
                case "float64":
                    format += "d "
                case _:
                    raise ValueError("This type is not recognized")
        return format
                
        
    def pack(self, record: dict) -> bytes:
        print(record["score"])
        args = []
        for item in self.schema:
            key, value = item

            if "string" in value:
                length = int(value.split(':')[-1])
                args.append(record[key].encode('utf-8').ljust(length, b'\x00'))
            else:
                args.append(record[key])
        return struct.pack(self.format, *args)

    def unpack(self, data: bytes) -> dict:
        values = struct.unpack(self.format, data)
        result = {}
        for (item, value) in zip(self.schema, values):
            key, type = item
            if "string" in type:
                value = value.rstrip(b'\x00').decode('utf-8')
                result[key] = value
            else:
                result[key] = value
        return result
                

schema = [
    ("id",    "int32"),
    ("name",  "string:20"),
    ("score", "float64"),
    ("active","int32"),
]

s = RecordSerializer(schema)
print(s.record_size) 

data = s.pack({"id": 1, "name": "Alice", "score": 95.5, "active": 1})
print(len(data))

record = s.unpack(data)
print(record)
    
    