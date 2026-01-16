import os,binascii
from .msc import MSC
from .hlr_auc import HLR, HLR_data
from .phone import Phone

class Network:
   
    def __init__(self, mcc, mnc, cc, ndc) -> None:
        self.mcc = mcc # mobile country code
        self.mnc = mnc # mobile network code
        self.cc = cc # country code
        self.ndc = ndc # national destination
        self.hlr = HLR()
        self.msc = []
        self.ms_count = 0
        
    def add_msc(self):
        msc = MSC(hlr=self.hlr)
        
    def create_new_ms(self):
        self.ms_count+=1
        phone_number = format(self.ms_count,"09d")
        imsi = self.mcc + self.mnc + format(self.ms_count,"010d")
        Ki = binascii.b2a_hex(os.urandom(32))
        self.hlr.add_ms(phone_number=phone_number, data=HLR_data(imsi=imsi, Ki=Ki))
        return Phone(
                    number=phone_number,
                    imsi=imsi,
                    Ki=Ki)
        
        
        