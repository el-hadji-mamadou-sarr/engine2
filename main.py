import struct
import os
from dataclasses import dataclass

PAGE_SIZE = 4096
HEADER_FORMAT = ">I H H" #8bytes
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

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
    
class Page:
    def __init__(self, page_id: int, record_size: int, data: bytes = None):
        self.page_id = page_id
        self.record_size = record_size
        self.capacity = (PAGE_SIZE - HEADER_SIZE) // record_size
        self.dirty = False # page modified since last read
        
        if data is not None:
            self._data = bytearray(data)
            self._read_header()
        else:
            self._data = bytearray(PAGE_SIZE)
            self.num_records = 0
            self._write_header()
    
    def _write_header(self):
        header = struct.pack(HEADER_FORMAT, self.page_id, self.num_records, self.record_size)
        self._data[:HEADER_SIZE] = header
    
    def _read_header(self):
        page_id, num_records, record_size = struct.unpack(HEADER_FORMAT, bytes(self._data[:HEADER_SIZE]))
        self.page_id = page_id
        self.num_records = num_records
        self.record_size = record_size
        
        self.capacity = (PAGE_SIZE - HEADER_SIZE) // record_size
    
    def _slot_offset(self, slot_id) -> int:
        return HEADER_SIZE + (slot_id * self.record_size)
    
    def insert(self, record_bytes: bytes) -> int:
        if self.num_records > self.capacity:
            raise OverflowError(f"Page {self.page_id} is full")
        if len(record_bytes) != self.record_size:
            raise ValueError("the record size is different")
        
        slot_id = self.num_records
        offset = self._slot_offset(slot_id)
        self._data[offset: offset + self.record_size] = record_bytes
        self.num_records +=1
        self._write_header()
        self.dirty = True
        return slot_id
    
    def get(self, slot_id) -> bytes:
        if slot_id >= self.num_records:
            raise ValueError("Slot invalid")
        offset = self._slot_offset(slot_id)
        return bytes(self._data[offset:offset+self.record_size])

    def delete(self, slot_id):
        """tombstone: replace with 0"""
        if slot_id >= self.num_records:
            raise ValueError("slot_id is greater than num records")
        
        offset = self._slot_offset(slot_id)
        self._data[offset:offset+self.record_size] = bytes(self.record_size)
    
    def is_full(self) -> bool:
        return self.num_records >= self.capacity
    
    def free_space(self) -> int:
        return (self.capacity - self.num_records) * self.record_size
    
    def to_bytes(self) -> bytes:
        return bytes(self._data)
    
    @classmethod
    def from_bytes(cls, data: bytes, record_size: int) -> "Page":
        return cls(page_id=0, record_size=record_size, data=data)

class DiskManager:
    def __init__(self, filepath: str, record_size: int):
        self.filepath = filepath
        self.record_size = record_size
        
        if not os.path.exists(filepath):
            open(filepath, 'wb').close()
        
    def _file_size(self) -> int:
        return os.path.getsize(self.filepath)
    
    def num_pages(self) -> int:
        return self._file_size() // PAGE_SIZE
    
    def write_page(self, page: Page):
        with open(self.filepath, 'r+b') as f:
            f.seek(page.page_id * PAGE_SIZE)
            f.write(page.to_bytes())
    
    def read_page(self, page_id) -> Page:
        with open(self.filepath, 'r') as f:
            f.seek(page_id * PAGE_SIZE)
            page_data = f.read(PAGE_SIZE)
            return Page.from_bytes(page_data, self.record_size)
    
    def allocate_page(self, record_size) -> Page:
        page_id = self.num_pages()
        page = Page(page_id, record_size)
        self.write_page(page)
        return page
        

@dataclass(frozen=True)
class RID:
    page_id: int
    slot_id: int
    
    def __repr__(self):
        return f"RID({self.page_id}:{self.slot_id})"
        