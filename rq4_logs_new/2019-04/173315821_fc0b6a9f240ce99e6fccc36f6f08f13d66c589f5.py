#!/usr/bin/env python
# -*- coding: utf8 -*-

import RPi.GPIO as GPIO
import MFRC522       
import signal
import time

continue_reading = True
# the uid of the last recognized card
lastcarduid = None
# the time a card uid was last seen
lastcardtime = 0.0

UsersIn = [] 

# this long after a card has been noticed, it can be noticed again
# This works because the reader generates repeated notifications of the card
# as it is held agains the reader - faster than this interval.
# The timer is restarted every time the reader generates a uid.
# If you sometimes get repeated new card IDs when holding a card against the
# reader, increase this a little bit.
CARDTIMEOUT = 1.0

# Capture SIGINT for cleanup when the script is aborted
def end_read(signal,frame):
    global continue_reading
    print "Ctrl+C captured, ending read."
    continue_reading = False
    GPIO.cleanup()

# Hook the SIGINT
signal.signal(signal.SIGINT, end_read)

# Create an object of the class MFRC522
MIFAREReader = MFRC522.MFRC522()

## Welcome message
#print "Welcome to the MFRC522 data read example"
#print "Press Ctrl-C to stop."

def getUser():
    if uid == "d0289125":
        return "A"
    elif uid == "99d6cb55":
        return "B"
    elif uid == "6956cd55":
        return "C"
    elif uid == "b90dc755":
        return "D"
    else:
        return "Invalid"

def checkTime():
    seconds = time.time()
    local_time = time.ctime(seconds)
    return local_time
    
def gettypename( typecode ):
    typecode &= 0x7F;
    if typecode == 0x00:
        return "MIFARE Ultralight or Ultralight C"
    elif typecode == 0x01:
        return "MIFARE TNP3XXX"
    elif typecode == 0x04:
        return "SAK indicates UID is not complete"
    elif typecode == 0x08:
        return "MIFARE 1KB"
    elif typecode == 0x09:    
        return "MIFARE Mini, 320 bytes"
    elif typecode == 0x10 or typecode == 0x11:
        return "MIFARE Plus"
    elif typecode == 0x18:
        return "MIFARE 4KB"
    elif typecode == 0x20:
        return "PICC compliant with ISO/IEC 14443-4"
    elif typecode == 0x40:
        return "PICC compliant with ISO/IEC 18092 (NFC)"
    return "Unknown type";

# This loop keeps checking for chips. If one is near it will get the UID and authenticate
while continue_reading:
    # Scan for cards    
    (status,TagType) = MIFAREReader.MFRC522_Request(MIFAREReader.PICC_REQIDL)
        
    # Get the UID of the card
    (status,rawuid) = MIFAREReader.MFRC522_Anticoll()
    
    
    # If we have a good read of the UID, process it
    if status == MIFAREReader.MI_OK:
        uid = "{0[0]:02x}{0[1]:02x}{0[2]:02x}{0[3]:02x}".format(rawuid)
#        # Print UID
#        print "Card read UID: "+uid
        newcard = False
        if lastcarduid:
            if lastcarduid != uid or (lastcardtime and time.clock() - lastcardtime >= CARDTIMEOUT):
                newcard = True
        else:
            newcard = True
        
        if newcard:
#            print "New Card read UID: "+uid
            # String payload = "{\"d\":{\"cardUID\":\"";
            #  payload += UID_HEX;
            #  payload += "\",\"cardtype\":\"";
            #  payload += mfrc522.PICC_GetTypeName(piccType);
            #  payload += "\"}}"; 
            rawcardtype = MIFAREReader.MFRC522_SelectTag(rawuid)
            cardtypename = gettypename( rawcardtype )
            User = getUser()
            if User in UsersIn:
                timeout = checkTime()
                print '{ "User":"'+User+'","Check out":"'+timeout+'" }'
                UsersIn.remove(User)
            elif User != "Invalid": 
                UsersIn.append(User)
                timein = checkTime()
                print '{ "User":"'+User+'","Check in":"'+timein+'" }'
            
        # remember the last card uid recognized
        lastcarduid = uid
        # remember when it was seen
        lastcardtime = time.clock()