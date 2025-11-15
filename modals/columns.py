from configs import constants
from marshaller import colmarshal

class Column():
    name : str 
    datatype : int
    allow_null : bool
    column_marshaler:colmarshal.ColumnMarshaler


    def __init__(self,name : str , datatype:int , allow_null:bool):
        self.name = name 
        self.datatype = datatype
        self.allow_null = allow_null
        self.column_marshaler = colmarshal.ColumnMarshaler(name = name , datatype= datatype , allowNull=allow_null)

    #column_marshaler = colmarshal.ColumnMarshaler()
    #column_unmarshaler:colmarshal.ColumnUnmarshaler() # ig this isnt needed will remove this if not used

    

    


# column on encoding looks ike 2 64 0 0 0 105 100 
# 2 is for the strig , then 4 for length , then ascii representation of id 


