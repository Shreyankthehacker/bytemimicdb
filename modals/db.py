import modals
from pathlib import Path
from configs import constants
import os

import modals.columndefreader
import modals.reader
import modals.tables 

class DataBase:
    name : str 
    path : str
    tables : dict # key is the table name and value is the modals.table object  
    

    def __init__(self,name):
        self.name = name 
        self.path = Path(constants.BaseDir).joinpath(name)
        self.tables = {}
        if self.path.exists():
            print("Path already exists!")
        else:
            os.makedirs(self.path)
            
        self.tables = self.read_tables()
        # for name,table_object in self.tables:
        #     table_object.restoreWal()



    def create_tables(self,name:str,columns_list,columns_map):
        table_path = (self.path / name).with_suffix(constants.Table_FileExtension)

        try:
            table_path.open("rb").close()
            raise FileExistsError("Table name already exists")
        except FileNotFoundError:
            pass

        try:
            f = table_path.open("wb")
        except Exception as e :
            raise KeyError(f"cant create table file {e}")
        
        table = modals.tables.Table(file_path=table_path,
                                    columnNames=columns_list,
                                    columns=columns_map,
                                    reader=f)
        table.WriteColumnDefinition()
        self.tables[name] = table
        return table 
    

    def read_tables(self):
        tables = {}

        for file in self.path.iterdir():     
            if file.name.endswith('_idx') or file.name.endswith('.wal'):
                continue

            # Fix: Open the file as BufferedReader
            opened_file = file.open('rb')
            r = modals.reader.Reader(opened_file)
            t = modals.tables.Table(file_path=str(file), reader=r)
            t.ReadColumnDefinitions()

            tables[t.name] = t

        print("total tables in the db are", list(tables.keys()))
        return tables

if __name__=='main':
    db = DataBase("db")
    columns_map = {"id": modals.columns.Column("id", 1, False),"name":modals.columns.Column("name", 2, False)} 
    columns_list = ["id","name"]
    table = db.create_tables("mytable", columns_list, columns_map)

