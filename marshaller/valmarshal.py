import struct
from io import BytesIO
from configs import constants

# ==================================================
# BASE MARSHALER
# ==================================================

class Marshaler:
    FORMATS = {
        int:  ("<i", constants.TypeINT32),
        float:("<f", constants.TypeFLOAT),
        bool: ("<?", constants.TypeBOOL),
        str:  (None, constants.TypeSTRING),  # dynamic length
        bytes:(None, constants.TypeBYTE)
    }

    def marshal(self, value):
        if isinstance(value, str):
            encoded = value.encode("utf-8")
        elif isinstance(value, (bytes, bytearray)):
            encoded = bytes(value)
        elif type(value) in self.FORMATS:
            fmt, _ = self.FORMATS[type(value)]
            encoded = struct.pack(fmt, value)
        else:
            raise TypeError(f"Unsupported type: {type(value)}")
        return encoded

    def unmarshal(self, data, dtype:type):
        if dtype == int:
            return int.from_bytes(data[:4], byteorder='little', signed=True)
        elif dtype == float:
            return struct.unpack("<f", data[:4])[0]
        elif dtype == bool:
            return struct.unpack("<?", data[:1])[0]
        elif dtype == bytes:
            return data
        elif dtype == str:
            return data.decode("utf-8")
        raise TypeError(f"Unsupported dtype: {dtype}")


# ==================================================
# TLV MARSHALER
# ==================================================

class TLVMarshaler(Marshaler):
    def __init__(self, value=None):
        super().__init__()
        self.value = value
        

    def tlv_marshal(self, value=None):
        if value is not None:
            self.value = value

        if self.value is None:
            raise ValueError("Cannot encode None")

        # Determine type and encode
        encoded_value = self.marshal(self.value)
        type_flag = self.FORMATS.get(type(self.value), (None, None))[1]
        if type_flag is None:
            raise TypeError(f"Unsupported value type: {type(self.value)}")

        # Determine length dynamically
        length_bytes = struct.pack("<i", len(encoded_value))

        tlv = type_flag + length_bytes + encoded_value
        print(f"Encoded TLV: {list(tlv)} for value {self.value}")
        self.tlv = tlv
        return tlv
    
    def tlv_length(self):
        if isinstance(self.value,bytes):
            return constants.LenMeta+constants.LenByte
        if isinstance(self.value,int):
            return constants.LenMeta+constants.LenInt32
        if isinstance(self.value,bool):
            return constants.LenMeta+constants.LenByte
        if isinstance(self.value,str):
            return constants.LenMeta+len(self.value)


    def print_tlv(self):
        print(f"The value of the encoded bytes is {self.tlv} for the value {self.value}")

# ==================================================
# TLV UNMARSHALER
# ==================================================

class TLVUnmarshaller(Marshaler):
    def __init__(self, data):
        super().__init__()
        self.data = data
        self.offset = 0

    def read(self, n):
        chunk = self.data[self.offset:self.offset + n]
        self.offset += n
        return chunk

    def tlv_unmarshal(self,data = None):
        if data is not None:
            self.data = data 
        type_flag = self.read(constants.LenByte)[0]
        length = struct.unpack("<i", self.read(constants.LenInt32))[0]
        value_data = self.read(length)

        dtype = constants.typemap.get(type_flag)
        if dtype is None:
            raise ValueError(f"Unknown type flag {type_flag}")

        value = self.unmarshal(value_data, dtype)
        print(f"Decoded TLV: type={type_flag}, length={length}, value={value}")
        return type_flag, length, value
