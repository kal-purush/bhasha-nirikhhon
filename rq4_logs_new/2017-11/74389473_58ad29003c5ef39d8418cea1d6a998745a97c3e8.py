import gmail

uname= ''
password=''

class Creds(object):
    def __init__(self):
        self.gm = gmail.GMail(uname,password)
        self.gm.connect()

    def gmailSend(self,msg, address):
        self.gm = gmail.GMail(uname,password)
        self.gm.connect()
        self.gm.send(msg,[address])