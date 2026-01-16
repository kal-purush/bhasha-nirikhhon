import os, sys
from random import randrange
from Crypto.Cipher import Blowfish

class BFCipher:
    def __init__(self,key):
        self.cipher = Blowfish.new(key)

    def encrypt(self,data = ''):
        ciphertext = self.cipher.encrypt(self.pad_data(data))
        return ciphertext

    def decrypt(self,data = ''):
        return self.unpad_data(self.cipher.decrypt(data))

    def pad_data(self,data):
        pad_bytes = 8 - (len(data) % 8)
        for i in range(pad_bytes - 1):
            data += chr(randrange(0,256))
        bflag = randrange(6, 248); bflag -= bflag % 8 - pad_bytes
        data += chr(bflag)
        return data

    def unpad_data(self,data):
        pad_bytes = ord(data[-1])%8
        if not pad_bytes:
            pad_bytes = 8
        return data[:-pad_bytes]


if __name__ == '__main__':
    data = 'sekharkaredla'
    # key of 32 bit
    key32 = '\xfa\x7d\xe6\x23\xf1\x57\xc0\xb6'
    # key of 448 bit
    key448 = '\x69\x82\x17\x61\xb6\x88\xcd\x7d\x5b\xea\xcc\x0b\xd5\x5f\x2d\xea\x91\xbd\xdf\x7d\x0d\x6f\xcb\xbe\xc5\x9c\x27\xe0\x37\xe1\x95\x20\xe3\x90\x89\x40\x2f\xd1\x13\xd7\xed\x1f\x88\xae\x87\x2f\xab\x9b\x82\x64\xab\x3e\xf0\x0c\x1c\x68'
    bfc = BFCipher(key32)
    encrypt = bfc.encrypt(data)
    print encrypt
    decrypt = bfc.decrypt(encrypt)
    print decrypt