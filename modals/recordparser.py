import modals
import io 
from typing import Any
from configs import constants
import modals.reader
import modals.tlvparser


class RawRecord :
    size : int 
    fullSize : int 
    record : dict 

    def __init__(self,size,record):
        self.size = size
        self.record = record
        self.fullSize = self.size+8
    



class RecordParser:
    file: io.TextIOWrapper | io.BufferedReader | Any  
    columns : list 
    value : RawRecord
    reader = modals.reader.Reader


    def __init__(self,file,columns):
        self.file = file 
        self.columns = columns

    # def skipDeletedRecords(self):
    #     try:
    #         while True:
    #             t = self.reader.read_byte()
    #             if t ==constants.TypeDeletedRecord:
    #                 length = self.reader.read_int()
    #                 self.file.seek(length,io.SEEK_CUR)
    #             if t==constants.TypeRECORD:
    #                 return 
    #     except IOError as e:
    #         print("no more records ")
    #         return -1 

    # def parse(self):
    #     reader = modals.reader.Reader(self.file)
    #     self.reader = reader

    #     t = reader.read_byte()
    #     if t!= constants.TypeDeletedRecord and t!=constants.TypeRECORD:
    #         return -1
    #     if t==constants.TypeDeletedRecord:
    #         self.file.seek(-1*constants.LenByte,io.SEEK_CUR)
    #         self.skipDeletedRecords()
    #     lenrecord = self.reader.read_int()
    #     record = {}
    #     for col in self.columns:
    #         tlvparser = modals.tlvparser.TLVParser(self.reader)
    #         t,l,v = tlvparser.parse()
    #         self.value = RawRecord(size=lenrecord,record =record )
    #         record[col] = self.value
    #     self.value = RawRecord(size = lenrecord,record = record)
    #     return 1


    def skipDeletedRecords(self):
            try:
                while True:
                    t = self.reader.read_int()  
                    if t == constants.TypeDeletedRecord:
                        length = self.reader.read_int()
                        self.file.seek(length, io.SEEK_CUR)
                    elif t == constants.TypeRECORD:
                        self.file.seek(-4, io.SEEK_CUR)  
                        return
                    else:
                        self.file.seek(-4, io.SEEK_CUR)
                        return -1
            except IOError as e:
                print("no more records")
                return -1
   

    # def parse(self):
    #     reader = modals.reader.Reader(self.file)
    #     self.reader = reader

    #     t = reader.read_int()  # 4-byte type
    #     if t != constants.TypeDeletedRecord and t != constants.TypeRECORD:
    #         self.file.seek(-4, io.SEEK_CUR)  
    #         return -1
    #     if t == constants.TypeDeletedRecord:
    #         self.file.seek(-4, io.SEEK_CUR)
    #         self.skipDeletedRecords()
    #     lenrecord = self.reader.read_int()
    #     record = {}
    #     for col in self.columns:
    #         tlvparser = modals.tlvparser.TLVParser(self.reader)
    #         _, _, value = tlvparser.parse()  
    #         record[col] = value
    #     self.value = RawRecord(size=lenrecord, record=record)
    #     return 1

    def parse(self):
        reader = modals.reader.Reader(self.file)
        self.reader = reader

        t = reader.read_int()  # 4-byte type
        if t != constants.TypeDeletedRecord and t != constants.TypeRECORD:
            self.file.seek(-4, io.SEEK_CUR)
            return -1
        if t == constants.TypeDeletedRecord:
            self.file.seek(-4, io.SEEK_CUR)
            self.skipDeletedRecords()
            t = self.reader.read_int()
            if t != constants.TypeRECORD:
                self.file.seek(-4, io.SEEK_CUR)
                return -1
        lenrecord = self.reader.read_int()
        record = {}
        for col in self.columns:
            tlvparser = modals.tlvparser.TLVParser(self.reader)
            _, _, value = tlvparser.parse()
            record[col] = value
        self.value = RawRecord(size=lenrecord, record=record)
        return 1



    def parse_for_update(self):
        t = self.parse()
        if t == -1:
            # check actual EOF
            pos = self.file.tell()
            b = self.file.read(1)
            if b == b"":   # true EOF
                return False
            self.file.seek(pos)
            return -1
        return True

        
