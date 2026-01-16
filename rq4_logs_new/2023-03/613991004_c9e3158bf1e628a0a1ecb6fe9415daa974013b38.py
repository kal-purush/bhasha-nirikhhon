import logging
from fernet_gui import FernetGUI
from cryptography.fernet import Fernet
from cryptography.fernet import InvalidToken
import time
import base64

TTL = 30
class TimeFernetGUI(FernetGUI):
    def encrypt(self, message):
        '''
        chiffre le message avec Fernet
        '''
        encr= Fernet(self.key)
        new_message= bytes(message,'utf-8')
        Temps = int(time.time())
        #Temps = int(time.time())-45
        encryp = encr.encrypt_at_time(new_message,current_time= Temps)
        return encryp

    def decrypt(self, message: bytes):
        '''
        déchiffre le message avec Fernet
 
        '''
        
        
        try:
            decryp = Fernet(self.key)
            message = base64.b64decode(message['data'])
            decr = Fernet(self.key)
            Temps= int(time.time())
            text_bytes = decr.decrypt_at_time(token=message,ttl = TTL,current_time=Temps).decode('utf-8') 
            return text_bytes
        
        
        except InvalidToken:
            return "message expiré"
        
    
if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)

    # instanciate the class, create context and related stuff, run the main loop
    client = TimeFernetGUI()
    client.create()
    client.loop()