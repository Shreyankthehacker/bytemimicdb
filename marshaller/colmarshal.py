from marshaller.valmarshal import TLVMarshaler, TLVUnmarshaller, Marshaler
import io
from configs import constants
import struct


class Column:
    name:str
    datatype:int
    allow_null:bool




class ColumnMarshaler(TLVMarshaler):

    column : Column
    column_tlv_encoded:bytes
    

    def __init__(self, value=None,name = None , datatype = None,allowNull = None  ):
        super().__init__(value)
        self.column = Column()
        self.column.name = name 
        self.column.datatype=datatype
        self.column.allow_null = allowNull

    def get_length(self):
        name_tlv = TLVMarshaler(self.column.name).tlv_marshal()
        dtype_tlv = TLVMarshaler(self.column.datatype).tlv_marshal()
        null_tlv = TLVMarshaler(self.column.allow_null).tlv_marshal()
        
        inner_length = len(name_tlv) + len(dtype_tlv) + len(null_tlv)
        total_length = inner_length  # only payload, header is added outside
        return total_length
    
    def marshal_column(self):
        name_tlv = TLVMarshaler(self.column.name).tlv_marshal()
        dtype_tlv = TLVMarshaler(self.column.datatype).tlv_marshal()
        allow_tlv = TLVMarshaler(self.column.allow_null).tlv_marshal()

        payload = name_tlv + dtype_tlv + allow_tlv
        total_length = len(payload)

        buffer = io.BytesIO()
        buffer.write(constants.TypeCOLUMNDEF)                  # b'c\x00\x00\x00'
        buffer.write(struct.pack("<i", total_length))          # correct length
        buffer.write(payload)

        result = buffer.getvalue()
        print(f"Column TLV: {list(result)} (len={len(result)})")
        return result
        
    def print_bytes(self):
        print(f"the byte value is {list(self.column_tlv_encoded)}")
        

class ColumnUnmarshaler(TLVUnmarshaller):
    column_name : str 
    column_datatype : int 
    column_allownull : bool

    def __init__(self, data):
        super().__init__(data)
    

class ColumnUnmarshaler(TLVUnmarshaller):
    column_name: str
    column_datatype: int
    column_allownull: bool

    def __init__(self, data):
        super().__init__(data)
        self.column_name = ""
        self.column_datatype = 0
        self.column_allownull = False

    def unmarshal_column(self):
        outer_type = struct.unpack("<i", self.read(4))[0]
        print("Outer type is ", outer_type)
        col_length = struct.unpack("<i", self.read(4))[0]
        print("Column length is ", col_length)

        nametype, namelength, namevalue = self.tlv_unmarshal()
        self.column_name = namevalue
        print(f"Name field decoded at offset {self.offset}")

        dtypename, dtypelength, dtypevalue = self.tlv_unmarshal()
        self.column_datatype = dtypevalue
        print(f"Datatype field decoded at offset {self.offset}")

        allownulltype, allownulllength, allownullvalue = self.tlv_unmarshal()
        self.column_allownull = allownullvalue
        print(f"AllowNull field decoded at offset {self.offset}")

        c = Column()
        c.name = self.column_name
        c.datatype = self.column_datatype
        c.allow_null = self.column_allownull
        return c

    
    def print_col(self):
        print(f"the column name is {self.column_name} with the dtype {constants.typemap[self.column_datatype]} and has allownull {self.column_allownull}")






