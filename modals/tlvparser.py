import marshaller.valmarshal
import modals
import modals.reader 
from configs import constants
import marshaller

class TLVParser(marshaller.valmarshal.TLVUnmarshaller):
    reader : modals.reader.Reader

    def __init__(self,reader):
        self.reader = reader

    
    def parse(self):
        data = self.reader.read_tlv()


        return self.unmarshal(data[0],type(data))
