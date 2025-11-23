from configs import constants
from typing import List
import marshaller
import marshaller.colmarshal
import marshaller.valmarshal
import modals
import io 
from typing import Any,Annotated
import re

import modals.columndefreader
import modals.columndefwriter
import modals.columns
import modals.reader 

import traceback

import modals.recordparser
import struct
from pathlib import Path



class DeletableRecord :
    offset : int 
    length  : int 

    def __init__(self,offset= None  , length= None ):
        self.offset = offset
        self.length = length
        



class Table:
    name: str
    file: io.TextIOWrapper | io.BufferedReader | Any  
    columnNames: Annotated[list[str],"has List of string where each of them is the column name "] # list of the column names
    columns: Annotated[dict[str, modals.columns.Column],"Map or dict which has column name as key and value is the column object itself "]  # map[column name] = modals.column object
    reader: modals.reader.Reader          #  modals.reader class
    recordParser: modals.recordparser.RecordParser
    columnDefReader: modals.columndefreader.ColumnDefReader
    wal: Any
    _file_path:str 


    def __init__(self, file_path=None, reader=None, wal=None, columns={}, columnNames=[], recordParser=None):
        self._file_path = file_path
        self.file = None  
        self.reader = None
        if file_path:
            p = Path(file_path)
            mode = "w+b" if not p.exists() else "r+b"
            self.file = open(file_path, mode)
        if reader:
            self.reader = reader
        elif self.file:
            self.reader = modals.reader.Reader(self.file)
        self.name = self.getTableName() if self.file else "unknown"
        self.columns = columns
        self.columnNames = columnNames
        self.columnDefReader = modals.columndefreader.ColumnDefReader(self.reader) if self.reader else None
        self.wal = wal
    
    def setRecordParse(self,recordParser:modals.recordparser.RecordParser):
        self.recordParser= recordParser

    
    def getTableName(self) -> str:

        filename = getattr(self.file, "name", getattr(self.file, "filename", "unknown"))
        parts = re.split(r'[_ ]+', filename)
        base = parts[0].split('.')[0]
        return base.split("/")[-1]
    
    def getColumnNames(self):
        return self.columnNames
    
    def WriteColumnDefinition(self):
        for col in self.columnNames:
            coltlv = self.columns[col].column_marshaler.marshal_column()
            colwriter = modals.columndefwriter.ColumnDefinitionWriter(self.file).write(coltlv)
            print(f"The value {coltlv} was wrote to the colwriter of length {colwriter} bytes with total data being {len(coltlv)}" )


    def ReadColumnDefinitions(self):
        self.file.seek(0,io.SEEK_SET)
        self.columnNames = []
        self.columns = {}
        buffer = bytearray(1024 * 20)  
        while True:
            
            try:
                buffer[:] = b'\x00' * len(buffer)
                length_read = self.columnDefReader.read(buffer)
                if length_read ==0:  
                    break
                print(buffer)
                self.buffer = buffer
                col_unmarshaler = marshaller.colmarshal.ColumnUnmarshaler(buffer[:length_read])
                col = col_unmarshaler.unmarshal_column()
                self.columnNames.append(col.name)
                self.columns[col.name] = col
                print(f"Loaded column: {col.name} (type: {constants.typemap.get(col.datatype, 'unknown')}, null: {col.allow_null})")
            except Exception as e:
                print(e)
                traceback.print_exc()
                return
                    
            print(f"Total columns loaded: {len(self.columnNames)}")




    def print_table(self):
        print(f"the table has the name {self.name} with columns {self.columnNames} and file path mm {self._file_path}")

        
    def validate_columns(self,columns):
        '''expects column name : value kind of dictionary'''
        for col,value in columns.items():
            try:
                self.columns[col]
            except KeyError as e:
                print("No such column found",e)
            
            if self.columns[col].allow_null==False and value ==None:
                raise IndexError("Column value cant be null")
        return None
    

    # def insert(self,record):
    #     '''record be dictionary'''
    #     self.file.seek(0,2) # move the file pointer to the end
    #     sizeOfRecord = 0
    #     for col in self.columnNames:  # maybe make primary key and stuff
    #         if record.get(col) is None:
    #             raise ValueError(f"The record doesnt have value for the given column {col}")
    #         val = record[col]
    #         tlv = marshaller.valmarshal.TLVMarshaler(val)
    #         tlv.tlv_marshal()
    #         length = tlv.tlv_length()
    #         sizeOfRecord+=length
        
    #     buffer = io.BytesIO()
    #     buffer.write(struct.pack("<i", constants.TypeRECORD))

    #     buffer.write(struct.pack("<i", sizeOfRecord))

    #     for col in self.columnNames:
    #         val = record[col]
    #         tlv = marshaller.valmarshal.TLVMarshaler(val)
    #         t = tlv.tlv_marshal()
    #         buffer.write(t)

    #     n = self.file.write(buffer.getvalue())

    #     if n==len(buffer.getvalue()):
    #         print("Record registered successfullly to the table")
    #     else:
    #         raise Exception("Couldnt write to the table")

    def insert(self, record):
        # ... existing validation ...
        self.file.seek(0, 2)
        sizeOfRecord = 0
        for col in self.columnNames:
            if record.get(col) is None:
                raise ValueError(f"The record doesnt have value for the given column {col}")
            val = record[col]
            tlv = marshaller.valmarshal.TLVMarshaler(val)
            tlv.tlv_marshal()
            length = tlv.tlv_length()
            sizeOfRecord += length

        buffer = io.BytesIO()
        buffer.write(struct.pack("<i", constants.TypeRECORD))  # 4-byte type
        buffer.write(struct.pack("<i", sizeOfRecord))  # 4-byte length

        for col in self.columnNames:
            val = record[col]
            tlv = marshaller.valmarshal.TLVMarshaler(val)  # New instance per col
            t = tlv.tlv_marshal()
            buffer.write(t)  # Full TLV bytes

        n = self.file.write(buffer.getvalue())
        if n == len(buffer.getvalue()):
            pass
            #print("Record registered successfully to the table")
        else:
            raise Exception("Couldnt write to the table")
    # insertion is fucking fine hhahahahajqa

    def seekuntil(self, target):
        while True:
            try:
                dtype = self.reader.read_int()   # read 4-byte type
                if dtype == target:
                    # rewind exactly 4 bytes to land at the start of the record
                    self.file.seek(-4, io.SEEK_CUR)
                    return 1

                length = self.reader.read_int()  # the length field
                self.file.seek(length, io.SEEK_CUR)
            except IOError:
                return None

    def ensureFilePointer(self):
        self.file.seek(0,io.SEEK_SET)
        return  self.seekuntil(constants.TypeRECORD)
        
    def validateWhereStatement(self,wherestmt):
        print(self.columnNames)
        # actually my wherestatemnt accepts 
        wherestmt['_union_type'] = wherestmt.get('_union_type','or')
        for col,_ in wherestmt.items():
            if col=='_union_type':
                continue
            if col not in self.columnNames:
                print(col)
                return -1
        
            
            
    
    def _or_implementation_of_records(self,records , wherestmt):
        return [rec for rec in records if any(rec[col] in lov for col,lov in wherestmt.items()) ]
    
    def _and_implementation_of_records(self,records,wherestmt):
        return [rec for rec in records if all(rec[col] in lov for col, lov in wherestmt.items())]

    def fileter_wherestatement(self,records , wherestmt):
        results = []
        select_type = wherestmt.pop('_union_type', 'or')
        if  select_type== "and":

            return self._and_implementation_of_records(records,wherestmt)
        else:
            return self._or_implementation_of_records(records,wherestmt)




    def select(self, wherestmt=None):
        '''
        wherestatement pattern be 
        {
        col1 : [possible values to be selected],
        col2 : same 

        _union_type : or/and

        }
        '''
        if wherestmt is not None:
            r = self.validateWhereStatement(wherestmt=wherestmt)
            if r==-1:
                print("Please check the format of the where statement")
                return
        
        
        i = self.ensureFilePointer()
        if i is None:
            print("No records found")
            return []
        results = []
        try:
            while True:
                self.recordParser.parse()
                rawrecord = self.recordParser.value
                results.append(rawrecord.record)  
        except IOError as e:
            print("End of the file reached returning the records")
            if wherestmt is None:
                return results
            return self.fileter_wherestatement(results , wherestmt)
# just wanna run a rough check first then will throw the wherestmt
    
    def markDeleted(self,records:List[DeletableRecord]):
        for rec in records:
            self.file.seek(rec.offset,io.SEEK_SET)
            self.file.write(struct.pack("<i",constants.TypeDeletedRecord))
            length = self.reader.read_int()
            zerobytes = bytearray(length)
            self.file.write(zerobytes)
        print(len(records) , " - total records were deleted")
        return len(records)
            


    def update(self,wherestmt , values : dict):
        self.validate_columns(values)
        self.ensureFilePointer()
        deletablerecords = []
        rawrecords = []
        while True:
            try:
                t = self.recordParser.parse_for_update()
                if t is False:
                    break     
                if t == -1:
                    continue   
                rawrecord = self.recordParser.value
                size = self.recordParser.value.fullSize
                rawrecords.append(rawrecord.record)
                print("1 to the rawrecords")
                pos = self.file.seek(0,io.SEEK_CUR)
                deletablerecords.append(DeletableRecord(pos-size))
            except OSError as e:
                print("mmmm breaking the loop")
                break

        self.markDeleted(deletablerecords)
        
        if wherestmt is not None:
            rawrecords = self.fileter_wherestatement(rawrecords, wherestmt)


        for record in rawrecords:
            updatedrecord = {}
            for col,value in record.items():
                updatedrecord[col] = values.get(col,value)
            self.insert(updatedrecord)
            print("inserting >>>>",updatedrecord)
        print(len(rawrecords))
        return 1



            





