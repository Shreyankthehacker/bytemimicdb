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



class Table:
    name: str
    file: io.TextIOWrapper | io.BufferedReader | Any  
    columnNames: Annotated[list[str],"has List of string where each of them is the column name "] # list of the column names
    columns: Annotated[dict[str, modals.columns.Column],"Map or dict which has column name as key and value is the column object itself "]  # map[column name] = modals.column object
    reader: modals.reader.Reader          #  modals.reader class
    recordParser:  modals.recordparser.RecordParser
    columnDefReader: modals.columndefreader.ColumnDefReader
    wal: Any
    _file_path:str 

    def __init__(self, file_path= None ,  wal= None ,columns = None , columnNames= None,recordParser= None  ):
        ''''Dont pass col def reader coz its done when we pass reader'''
        self._file_path = file_path
        self.file = open(file_path, "r+b")
        self.name = self.getTableName()
        self.columns = {}
        self.reader = modals.reader.Reader(self.file)
        self.columnDefReader = modals.columndefreader.ColumnDefReader(self.reader)
        self.wal = wal
        self.columns = columns if columns is not None else {}
        self.columnNames = columnNames if columnNames is not None else []
        
        self.recordParser = None
    
    def getTableName(self) -> str:
        # Get filename attribute safely
        filename = getattr(self.file, "name", getattr(self.file, "filename", "unknown"))
        # Split on both underscores and spaces
        parts = re.split(r'[_ ]+', filename)
        # Take first chunk, remove extension
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
        buffer = bytearray(1024 * 20)  # Larger for full col defs
        print("cmonnnnn mannnn")
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
                print("tututudududu")
                print(e)
                traceback.print_exc()
                return
                    
            print(f"Total columns loaded: {len(self.columnNames)}")
        self.recordParser = modals.recordparser.RecordParser(file = self.file ,columns=self.columnNames,reader = self.reader)        
        # self.ensureFilePointer()



    def print_table(self):
        print(f"the table has the name {self.name} with columns {self.columnNames} and file path mm {self._file_path}")

        
    def validate_columns(self,columns):
        '''expects column name : value kind of dictionary'''
        for col,value in columns:
            try:
                self.columns[col]
            except KeyError as e:
                print("No such column found")
            
            if self.columns[col].allow_null==False and value ==None:
                raise IndexError("Column value cant be null")
        return None
    

    def seek_until(self,targetType):
        print((type(self.reader)))
        while(True):
            try:
                dtype = self.reader.read_byte()
            except IOError:
                return
            if dtype==targetType:
                self.file.seek(-1*constants.LenByte,io.SEEK_CUR)
                return
            length = self.reader.read_int()
            self.file.seek(length,io.SEEK_CUR)



    def ensureFilePointer(self):
        self.file.seek(0,io.SEEK_SET)
        self.seek_until(constants.TypeRECORD)
        return 



    def insert(self, record):
            self.file.seek(0, 2)  # end of file

            total_size = 0
            field_tlvs = []

            for col in self.columnNames:
                if col not in record:
                    raise ValueError(f"Missing value for column: {col}")
                val = record[col]
                marshaler = marshaller.valmarshal.TLVMarshaler(val)
                tlv_bytes = marshaler.tlv_marshal()
                field_tlvs.append(tlv_bytes)
                total_size += len(tlv_bytes)

            buffer = io.BytesIO()
            buffer.write(constants.TypeRECORD)      # Type
            buffer.write(total_size.to_bytes(4, "little"))                # Length
            for tlv_bytes in field_tlvs:
                buffer.write(tlv_bytes)

            written = self.file.write(buffer.getvalue())
            self.file.flush()

            if written == len(buffer.getvalue()):
                print("Record inserted successfully")
            else:
                raise Exception("Partial write during insert")
    
    def validateWhereStatement(self,wherestmt):
        for col,val in wherestmt.items():
            if col not in self.columns:
                raise ValueError("UNkwon column in the where statement")
        return 

    def ensureColumnLength(self,record):
        if len(record.keys())!=len(self.columnNames):
            raise ValueError("Column lengths mismatch couldnt retrieve all the column from the table")

    def evaluateWhereStatement(self,wherestmt,record):
        for key,value in wherestmt.items():
            if record[key]!=value:
                return False
        return True

    def select(self,wherestmt):
        self.ensureFilePointer()
        self.validateWhereStatement(wherestmt)
        results = []
        while(True):
            raw = self.recordParser.parse()
            if raw is None:
                return results
            rawrecord = self.recordParser.value
            self.ensureColumnLength(rawrecord.record)
            if not self.evaluateWhereStatement(wherestmt,rawrecord.record):
                continue
            results.append(rawrecord.record)







# from Writing Column Definitions is remaining 

class DeletetableRecord :
    offset : int 
    length  : int 

    def __init__(self,offset= None  , length= None ):
        self.offset = offset
        self.length = length
        
