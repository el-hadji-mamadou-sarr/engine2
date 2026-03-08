import struct
import os
from dataclasses import dataclass
from typing import Optional
from collections import OrderedDict
from contextlib import contextmanager

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
        self.dirty = True
    
    def update(self, slot_id, record_bytes: bytes):
        if slot_id >= self.num_records:
            raise ValueError("Invalid slot")
        
        if len(record_bytes) != self.record_size:
            raise ValueError("Invalid record bytes")
        
        offset = self._slot_offset(slot_id)
        self._data[offset : offset + self.record_size] = record_bytes
        self.dirty = True
        
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
        with open(self.filepath, 'rb') as f:
            f.seek(page_id * PAGE_SIZE)
            page_data = f.read(PAGE_SIZE)
            return Page.from_bytes(page_data, self.record_size)
    
    def allocate_page(self) -> Page:
        page_id = self.num_pages()
        page = Page(page_id, self.record_size)
        self.write_page(page)
        return page
  
@dataclass(frozen=True)
class RID:
    page_id: int
    slot_id: int
    
    def __repr__(self):
        return f"RID({self.page_id}:{self.slot_id})"
        
class HeapTable:
    def __init__(self, filepath: str, schema: list[tuple[str, str]], pool_size: int):
        self.schema = schema
        self.record_serializer = RecordSerializer(schema)
        self.disk_manager = DiskManager(filepath, self.record_serializer.record_size)
        self.pool = BufferPool(self.disk_manager, pool_size)
    
    @contextmanager
    def fetch_page(self,page_id: int, is_dirty: bool = False):
        page = self.pool.fetch_page(page_id)
        try:
            yield page
        finally:
            self.pool.unpin_page(page_id, is_dirty=is_dirty)
    
    def insert(self, record: dict) -> RID:
        data = self.record_serializer.pack(record)
        num_pages = self.disk_manager.num_pages()
        if num_pages == 0 :
            page, _ = self.pool.new_page()
        else:
            last_page = self.pool.fetch_page(num_pages - 1)
            if last_page.is_full():
                page, _ = self.pool.new_page()
            else:
                page = last_page
        
        slot_id = page.insert(data)
        self.pool.unpin_page(page.page_id, is_dirty=True)
        
        return RID(page.page_id, slot_id)
    
    def get(self, rid: RID) -> dict:
        with self.fetch_page(rid.page_id) as page:
            data = page.get(rid.slot_id)
            return self.record_serializer.unpack(data)
        
    def update(self, rid: RID, record: dict):
        with self.fetch_page(rid.page_id, is_dirty=True) as page:
            data = self.record_serializer.pack(record)
            page.update(rid.slot_id, data)
    
    def scan(self) ->list[dict]:
        page_nums = self.disk_manager.num_pages()
        records = []
        null_record = bytes(self.disk_manager.record_size)
        for page_id in range(page_nums):
            with self.fetch_page(page_id) as page:
                for slot_id in range(page.num_records):
                    data = page.get(slot_id)
                    if data == null_record:
                        continue
                    record = self.record_serializer.unpack(data)
                    records.append(record)
        return records
    
    def delete(self, rid: RID):
        with self.fetch_page(rid.page_id, is_dirty=False) as page:
            page.delete(rid.slot_id)
        
    
    def close(self):
        self.pool.flush_all()
    
@dataclass
class FrameMeta:
    page_id: Optional[int] = None #
    pin_count: int = 0 # opération qui utilise cette page
    dirty: bool = False

class BufferPool:
    def __init__(self, disk_manager: DiskManager, pool_size: int = 10):
        self.disk = disk_manager
        self.pool_size = pool_size
    
        self.frames: list[Optional[object]] = [None] * pool_size
        self.meta: list[FrameMeta] = [FrameMeta() for _ in range(pool_size)]
        self.page_table: dict[int, int] = {}
        self.lru: OrderedDict[int, bool] = OrderedDict() # pour la suppression
        self.free_frames: list[int] = list(range(pool_size))
        self.cache_hit_count: int = 0
        self.cache_miss_count: int = 0
        
    
    def _get_free_frame(self) -> Optional[int]:
        if self.free_frames:
            return self.free_frames.pop()
        
        # s'il y'a rien dans lru => pas possible de supprimer ,tout est pinné
        if not self.lru:
            return None
        
        victim_frame = next(iter(self.lru))
        del self.lru[victim_frame]
        
        victim_meta = self.meta[victim_frame]
        
        if victim_meta.dirty:
            self.disk.write_page(self.frames[victim_frame])
        
        del self.page_table[victim_meta.page_id]
        self.frames[victim_frame] = None
        self.meta[victim_frame] = FrameMeta()
        return victim_frame
    
    def fetch_page(self, page_id: int) -> Page:
        # 1. get page from pool first
        if page_id in self.page_table:
            self.cache_hit_count += 1
            frame_id = self.page_table[page_id]
            meta = self.meta[frame_id]
            meta.pin_count+=1
            if frame_id in self.lru:
                del self.lru[frame_id]
            page = self.frames[frame_id]
            return page
        self.cache_miss_count += 1
        frame_id = self._get_free_frame()
        if frame_id is None:
            raise MemoryError("Buffer full, no frame evictable")
        
        page = self.disk.read_page(page_id)
        
        self.frames[frame_id] = page
        self.meta[frame_id] = FrameMeta(page_id=page_id, pin_count=1, dirty=False)
        self.page_table[page_id] = frame_id
        
        return page                
    
    def unpin_page(self, page_id: int, is_dirty: bool = False):
        if page_id not in self.page_table:
            return
        
        frame_id = self.page_table[page_id]
        meta = self.meta[frame_id]
        
        if meta.pin_count <=0:
            return

        meta.pin_count -= 1
        if is_dirty:
            meta.dirty = True
        
        if meta.pin_count == 0:
            self.lru[frame_id] = True
    
    def flush_page(self, page_id: int):
        if page_id not in self.page_table:
            return
        
        frame_id = self.page_table[page_id]
        
        meta = self.meta[frame_id]
        if meta.dirty:
            page = self.frames[frame_id]
            self.disk.write_page(page)
            meta.dirty = False
    
    def stats(self):
        stats = {}
        stats["pool_size"] = self.pool_size
        stats["pages_in_pool"] = len([page for page in self.frames if page is not None])
        stats["dirty_pages"] = len([meta for meta in self.meta if meta.dirty == True])
        stats["pinned_pages"] = len([meta for meta in self.meta if meta.pin_count > 0])
        
        cache_req_count = self.cache_hit_count + self.cache_miss_count
        stats["hit_rate"] = f"{round(100 * self.cache_hit_count/(self.cache_hit_count + self.cache_miss_count), 1)}%" if cache_req_count > 0 else "N/A"
        
        return stats
    
    def flush_all(self):
        for page_id in list(self.page_table.keys()):
            self.flush_page(page_id)
    
    def new_page(self):
        
        frame_id = self._get_free_frame()
        
        if frame_id is None:
            raise MemoryError("Buffer full")
        
        page = self.disk.allocate_page()
        page_id = page.page_id
        self.frames[frame_id] = page
        self.meta[frame_id] = FrameMeta(page_id=page_id, pin_count=1, dirty=True)
        self.page_table[page_id] = frame_id
        
        return page, page_id
        

schema = [
    ("id",    "int32"),
    ("name",  "string:20"),
    ("score", "float64"),
    ("active","int32"),
]

table = HeapTable("students.db", schema, pool_size=10)

rid1 = table.insert({"id": 1, "name": "Alice", "score": 95.5, "active": 1})
rid2 = table.insert({"id": 2, "name": "Bob",   "score": 87.0, "active": 1})
rid3 = table.insert({"id": 3, "name": "Carol", "score": 92.0, "active": 0})

_ = table.get(rid1)
_ = table.get(rid1)  # 2ème accès → cache hit
_ = table.get(rid2)

table.update(rid3, {"id": 3, "name": "Carol", "score": 99.0, "active": 1})
print(rid3)

print(table.pool.stats())
# {
#   "pool_size": 10,
#   "pages_in_pool": 1,
#   "dirty_pages": 1,
#   "pinned_pages": 0,
#   "hit_rate": "X.X%"
# }

table.close()
                

            
            
            
            
