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



class Table:
    name: str
    file: io.TextIOWrapper | io.BufferedReader | Any  
    columnNames: Annotated[list[str],"has List of string where each of them is the column name "] # list of the column names
    columns: Annotated[dict[str, modals.columns.Column],"Map or dict which has column name as key and value is the column object itself "]  # map[column name] = modals.column object
    reader: modals.reader.Reader          #  modals.reader class
    recordParser: Any
    columnDefReader: modals.columndefreader.ColumnDefReader
    wal: Any
    _file_path:str 

    def __init__(self, file_path= None , reader= None  , wal= None ,columns = None , columnNames= None,recordParser= None  ):
        ''''Dont pass col def reader coz its done when we pass reader'''
        self._file_path = file_path
        self.file = open(file_path, "r+b")
        self.name = self.getTableName()
        self.columns = {}
        self.reader = reader
        self.columnDefReader = modals.columndefreader.ColumnDefReader(self.reader)
        self.wal = wal
        self.columns = columns 
        self.columnNames = columnNames # list of string 
        self.recordParser = recordParser

    
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
        self.columnNames = []
        self.columns = {}
        buffer = bytearray(1024 * 20)  # Larger for full col defs
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
        for col,value in columns:
            try:
                self.columns[col]
            except KeyError as e:
                print("No such column found")
            
            if self.columns[col].allow_null==False and value ==None:
                raise IndexError("Column value cant be null")
        return None
    

    def insert(self,record):
        '''record be dictionary'''
        self.file.seek(0,2) # move the file pointer to the end
        sizeOfRecord = 0
        for col in self.columnNames:  # maybe make primary key and stuff
            if record.get(col) is None:
                raise ValueError(f"The record doesnt have value for the given column {col}")
            val = record[col]
            tlv = marshaller.valmarshal.TLVMarshaler(val)
            tlv.tlv_marshal()
            length = tlv.tlv_length()
            sizeOfRecord+=length
        
        buffer = io.BytesIO()
        typebuffer = tlv.marshal(constants.TypeRECORD)
        buffer.write(typebuffer)

        size_buffer = tlv.marshal(sizeOfRecord)
        buffer.write(size_buffer)

        for col in self.columnNames:
            val= record[col]
            tlv_record = tlv.tlv_marshal(val)
            buffer.write(tlv_record)

        n = self.file.write(buffer.getvalue())

        if n==len(buffer.getvalue()):
            print("Record registered successfullly to the table")
        else:
            raise Exception("Couldnt write to the table")



# from Writing Column Definitions is remaining 

class DeletetableRecord :
    offset : int 
    length  : int 

    def __init__(self,offset= None  , length= None ):
        self.offset = offset
        self.length = length
        
