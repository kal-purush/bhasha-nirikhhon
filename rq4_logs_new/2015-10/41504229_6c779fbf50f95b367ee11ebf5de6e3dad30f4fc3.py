"""
    Configuration module using simply ".py" format
    For diverse configurations, maintain multiple files.

    If maintain multiple files get messy, Saltar team will look for another
    approach.
"""

#************************ General debugging flags ************************
DEBUG_MODE = True
MAKE_JSON_FILE = False
MAKE_SHRED_JSON_FILE = False

#************************ Path and Files ************************
# Directory where the .exe.pcap files are located
DIR_PCAP = "/home/luti/Saltar/python/"
#The extensions of files to be processed
PCAP_EXTENSION = ".exe.pcap"
JSON_EXTENSION = ".json"

#************************ MongoDB ************************
# MongoDB can be
HOST_NAME = "localhost"
IP_ADDRESS = "198.162.40.12"
#Port of comunication
PORT = 27017

#The file needs be particioned, the mongoDB do not acept
#files bigger then 16MB == 16777216 bytes
MAX_LENGTH_BSON = 16777216

#************************ Parallelism configuration ************************
#Determine the number of cores that should be adjusted due to file size and memory consumption
MAX_CORES = 1
MODERATED_CORES = 1
LOW_CORES = 1