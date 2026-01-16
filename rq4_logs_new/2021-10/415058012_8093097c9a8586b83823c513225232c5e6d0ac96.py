#CREATION DU SEED

import random
import hashlib
rand = "".join( [str( random.randint (0, 1)) for v in range(128)] )
core = (hashlib.sha256(bytes([int(i) for i in rand]))).hexdigest()
concat = bin(int(core[0], 16))[2:].zfill(4)
seed = rand+str(concat)
print("Seed:\n", seed)
print("\n\n")

#DECOUPAGE DU SEED ET ASSOCIATION DES MOTS

string = seed
with open("wordlistEng.txt", "r") as f:
    lines = f.readlines()
lot = [ (int (string[i:i+11]), lines[int( string[i:i+11], 2)].strip()) for i in range(0, 128, 11)]
print("Association:\n", lot)
mnemonic = " ".join([lot[i][1] for i in range(12)])
print("Mnemonic:\n", mnemonic)
print("\n\n")


#EXTRACTION DU MPK & MCC

from backports.pbkdf2 import pbkdf2_hmac
import binascii 
key = pbkdf2_hmac("sha512", mnemonic.encode("utf8"), "mnemonic".encode("utf8"), 2048, 256)
print("key:\n", str(binascii.hexlify(key))[2:514])
MasterPrivatekey = str(binascii.hexlify(key))[2:258]
print("MasterPrivatekey:\n", MasterPrivatekey, len(MasterPrivatekey))
MasterChainCode = str(binascii.hexlify(key))[258:514]
print("MasterChainCode:\n", MasterChainCode, len(MasterChainCode))
print("\n\n")
