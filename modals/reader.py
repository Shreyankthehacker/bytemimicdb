import io
import struct
from configs import constants

class Reader:
    reader: io.BufferedReader
    data: bytes
    buffer_value : any 

    def __init__(self, reader: io.BufferedReader = None):
        if reader is None:
            raise ValueError("Reader: None reader given")
        self.reader = reader
        self.data = b""

    def read(self,size:int = None):
        if self.reader is None:
            raise ValueError("Plz init the reader jeezzz")
        
        self.data = self.reader.read(size)
        return self.data 

    def read_int(self):
        return int.from_bytes(self.read(size = constants.LenInt32 ))
    
    def read_byte(self):
        return self.read(size = constants.LenInt32 )

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


