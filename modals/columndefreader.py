import modals
import modals.reader
import io 


class ColumnDefReader:
    reader:modals.reader.Reader

    def __init__(self,reader):
        self.reader = reader

    def read(self,data : bytearray):
        '''returns the length of the reader's dtype , length , column value being taken as bytes '''
        buffer = io.BytesIO()
        
        datatype = self.reader.read_byte()
        length = self.reader.read_int()
        col = self.reader.read(length)
        buffer.write(datatype)
        buffer.write(length.to_bytes(4, "little"))
        buffer.write(col)
        out = buffer.getvalue()
        data[:len(out)] = out 
        return len(out)



