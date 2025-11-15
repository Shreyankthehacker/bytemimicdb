from configs import constants
from typing import List
import marshaller
import marshaller.colmarshal
import modals
import io 
from typing import Any,Annotated
import re

import modals.columndefreader
import modals.columndefwriter
import modals.columns
import modals.reader 





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
        return base
    
    def getColumnNames(self):
        return self.columnNames
    
    def WriteColumnDefinition(self):
        for col in self.columnNames:
            coltlv = self.columns[col].column_marshaler.marshal_column()
            colwriter = modals.columndefwriter.ColumnDefinitionWriter(self.file).write(coltlv)
            print(f"The value {coltlv} was wrote to the colwriter of length {colwriter} bytes with total data being {len(coltlv)}" )


    def ReadColumnDefinitions(self):
        buffer = bytearray(1024)
        length_read = self.columnDefReader.read(buffer)
        # col = modals.columns.Column()
        col_tlv_decoded = marshaller.colmarshal.ColumnUnmarshaler(buffer[:length_read]).tlv_unmarshal()
        col = modals.columns.Column(col_tlv_decoded[0],col_tlv_decoded[1],col_tlv_decoded[2])
        self.columnNames.append(col.name)
        self.columns[col.name] = col 
        print("Column has been successfully extracted")
        return 






        


    

# from Writing Column Definitions is remaining 

class DeletetableRecord :
    offset : int 
    length  : int 

    def __init__(self,offset= None  , length= None ):
        self.offset = offset
        self.length = length
        
