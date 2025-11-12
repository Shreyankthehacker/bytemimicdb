from configs import constants
from typing import List
import modals

class Table:
    name : str 
    file : any # basically io.textiowrapper or smthng 
    columnNames : list[str]
    columns : dict # basically column name and Column object 
    reader : any # to be developed
    recordParser : any # tbd
    columnDefReader : any # tbd 
    wal : any # tbd

class DeletetableRecord :
    offset : int 
    length  : int 
