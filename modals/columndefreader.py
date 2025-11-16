# modals/columndefreader.py
from configs import constants
import struct
import modals
import modals.reader

class ColumnDefReader:
    reader:modals.reader.Reader
    def __init__(self, reader):
        self.reader = reader  # This is a modals.reader.Reader instance

    def read(self, data: bytearray) -> int:
        # Peek to check if we have at least 8 bytes (type + length)
        peek = self.reader.reader.peek(8)[:8]
        if len(peek) < 8:
            return 0

        # outer_type = int.from_bytes(peek[:4], 'little')
        outer_type_bytes = peek[0:4]
        outer_type = struct.unpack("<i", outer_type_bytes)[0]   # ← unpack properly

        if outer_type != constants.ColumnType:
            raise ValueError(f"Expected column definition ({constants.ColumnType}), got {outer_type}")
        
        # Read outer type (4 bytes)
        data[0:4] = self.reader.read(4)

        # Read length (4 bytes)
        len_bytes = self.reader.read(4)
        length = int.from_bytes(len_bytes, 'little')
        data[4:8] = len_bytes

        # Read full payload
        pos = 8
        remaining = length
        while remaining > 0:
            chunk = self.reader.read(remaining)
            if not chunk:
                raise EOFError("Unexpected end of file in column definition")
            chunk_len = len(chunk)
            data[pos:pos + chunk_len] = chunk
            pos += chunk_len
            remaining -= chunk_len

        return 8 + length