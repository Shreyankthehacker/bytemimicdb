from configs import constants


class Column:
    name : str 
    datatype : int
    allow_null : bool

    


# column on encoding looks ike 2 64 0 0 0 105 100 
# 2 is for the strig , then 4 for length , then ascii representation of id 


