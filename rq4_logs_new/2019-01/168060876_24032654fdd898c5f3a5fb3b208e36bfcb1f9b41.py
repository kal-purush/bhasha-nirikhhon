import hashlib
import os

fp = open('./samples/eicar.txt', 'rb')
fb = fp.read()
fp.close()

# if fb[0:3] == b'X5O':
#     print('Virus')
#     os.remove('./samples/2eicar.txt')
# else:
#     print('No Virus')

m = hashlib.md5()
# m.update('hello'.encode('utf-8'))
m.update(fb)
fmd5 = m.hexdigest()
print(fmd5)

if fmd5 == '44d88612fea8a8f36de82e1278abb02f':
    print('Virus')
    os.remove('./samples/eicar.txt')
else:
    print('No Virus')