
ColumnNameLength = b'\x40'

TypeINT32 = b'\x05'
TypeSTRING = b'\x02'
TypeBYTE = b'\x03'
TypeBOOL = b'\x04'
TypeFLOAT = b'\x06'
TypeCOLUMNDEF = b'c\x00\x00\x00'
TypeRECORD = b'd\x00\x00\x00'
LenByte = 1
LenInt32 = 4
LenInt64 = 8
typemap = {
    5: int,
    2: str,
    3: bytes,
    4: bool,
    6: float,
}

Table_FileExtension = ".bin"
BASEDir = './data'