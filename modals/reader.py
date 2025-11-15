import io
import struct
from configs import constants

class Reader:
    reader: io.BufferedReader
    data: bytes
    buffer_value : any 

    def __init__(self, reader: io.BufferedReader = None):
        '''reader takes stream of bytes kinda thing 
        stream = io.BytesIO(b'\x01\x00\x00\x00\xff\xff\xff\xffHello!')

        r = Reader(stream)'''
        if reader is None:
            raise ValueError("Reader: None reader given")
        self.reader = reader
        self.data = b""

    def read(self, size=None):
        if size is None:
            return self.reader.read()

        buf = self.reader.read(size)
        if buf is None or len(buf) != size:
            raise IOError(f"expected {size} bytes, got {0 if buf is None else len(buf)}")
        return buf

    def read_byte(self):
        return self.read(1)

    def read_int(self):
        return int.from_bytes(self.read(4), "little")


    def read_tlv(self):
        buffer = io.BytesIO()
        datatype = self.read_byte()
        buffer.write(datatype)

        length = self.read_int()
        buffer.write(length.to_bytes(constants.LenInt32))

        value = self.read(length)
        buffer.write(value)
        self.buffer_value =  buffer.getvalue()
        return self.buffer_value


