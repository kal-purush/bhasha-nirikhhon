from Crypto.Cipher import Blowfish
from Crypto import Random
from struct import pack

bs = Blowfish.block_size
key = b'An arbitrarily long key'

iv = Random.new().read(bs)
cipher = Blowfish.new(key, Blowfish.MODE_CBC, iv)
plaintext = b'kalyan'
plen = bs - divmod(len(plaintext),bs)[1]
padding = [plen]*plen
padding = pack('b'*plen, *padding)
msg = iv + cipher.encrypt(plaintext + padding)
print "Encrypted msg: " + msg


de = cipher.decrypt(msg)

last_byte = de[-1]
de = de[:- (last_byte if type(last_byte) is int else ord(last_byte))]
print(repr(de))
