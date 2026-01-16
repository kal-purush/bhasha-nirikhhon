import paramiko
from paramiko import SSHClient
from scp import SCPClient

class SendData(object):
    _sshclient = None
    _scpclient = None
    #Example:
    #sender = SendData ('11.22.33.44','username')
    def __init__(self,serverip,rusername):
        self._sshclient = SSHClient()
        self._sshclient.set_missing_host_key_policy (paramiko.AutoAddPolicy())
        # Server needs to have your key
        self._sshclient.load_system_host_keys()
        self._sshclient.connect (hostname=serverip,username=rusername)
        self._scpclient = SCPClient (self._sshclient.get_transport ())
        #Example: send (pathtofile)
    def send (self,file):
        self._scpclient.put (file)

    def end (self):
        self._scpclient.close()

def main ():
    sender = SendData ('159.203.103.181','aeolus')
    sender.send ('/home/pedro/TESTFILE')
    sender.end()

if __name__ == "__main__":
    main()