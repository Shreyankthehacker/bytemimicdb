import modals
from pathlib import Path
from configs import constants
import os 

class DataBase:
    name : str 
    path : str
    tables : dict # key is the table name and value is the modals.table object  
    

    def __init__(self,name):
        self.name = name 
        self.path = Path(constants.BaseDir).joinpath(name)
        if self.path.exists():
            print("Path already exists!")
        else:
            os.makedirs(self.path)
            
        self.tables = self.read_tables()
        for name,table_object in self.tables:
            table_object.restoreWal()


    def create_tables(self):
        pass

    def read_tables(self):
        pass