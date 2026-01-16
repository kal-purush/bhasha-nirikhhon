# Creates a class called SLAAC_Message
import logging

class S3Log:
    # Initialize when created. Self tells its from this class and the others are your created attributes
    def __init__(self):
        # Self is the new object
        self.name = "Hello"

    def writelog(self,message,logfile):
        logging.basicConfig(filename=logfile,format='%(levelname)s:%(asctime)s: %(message)s',level=logging.INFO)
        logging.info(message)

