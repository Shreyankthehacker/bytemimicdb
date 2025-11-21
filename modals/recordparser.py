import typing
import io 
from configs import constants
import modals
import modals.reader
import modals.tlvparser 

class RawRecord:
    size : int 
    fullSize : int
    record : dict  
    
    def __init__(self,size , reocrd ):
        self.size = size 
        self.record = reocrd
        self.fullSize = constants.LenMeta


class RecordParser:
    file :  io.TextIOWrapper | io.BufferedReader | typing.Any  
    columnsNames: list 
    value : RawRecord
    reader : modals.reader.Reader

    def __init__(self,file,columns,reader  ):
        self.file = file 
        self.columnsNames = columns
        self.reader = reader or modals.reader.Reader(file)


    def skipDeletedRecords(self):
        while(True):
            t = self.reader.read_byte()
            if t==constants.TypeDeleteRecrod:
                l= self.reader.read_int()
                self.file.seek(l,io.SEEK_CUR)
            elif t==constants.TypeRECORD:
                return




    def parse(self):
        try:
            b = self.reader.read_byte()
        except EOFError:
            return None   # ← termination signal

        if b not in (constants.TypeDeleteRecrod, constants.TypeRECORD):
            raise ValueError("maybe file offset isn't set properly")

        if b == constants.TypeDeleteRecrod:
            self.file.seek(-constants.LenByte, io.SEEK_CUR)
            self.skipDeletedRecords()

        record = {}
        lenrecord = self.reader.read_int()

        for i, col in enumerate(self.columnsNames):
            tlvparser = modals.tlvparser.TLVParser(self.reader)
            value = tlvparser.parse()
            record[self.columnsNames[i]] = value

        self.value = RawRecord(size=lenrecord, record=record)
        return self.value
