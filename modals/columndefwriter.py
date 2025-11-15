from logger import logger 

class ColumnDefinitionWriter:
    def __init__(self, file_obj):
        self.file = file_obj


    def write(self, data: bytes):
        n = self.file.write(data)
        self.file.flush()
        return n