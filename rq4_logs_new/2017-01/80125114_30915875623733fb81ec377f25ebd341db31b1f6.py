#!/usr/bin/env python

import uhal 
import sys 
import argparse 
import time
from time import sleep
import struct
from CommonTools import uInt32HexStr, uInt32HexListStr
import array
import binascii
from itertools import izip, izip_longest
import argparse
import datetime 
from datetime import datetime
import ROOT
from ROOT import TFile
from ROOT import TH1
from ROOT import TTree
import array
from ROOT import TDatime

branch_form = 'ch1/D:ch2/D:ch3/D:ch4/D:ch5/D:ch6/D:ch7/D'
dcunames = [('dcu1',0x10,15),('dcu2',0x20,15),('dcu3',0x40,15),
            ('vfeDCU9',0x10,9),('vfeDCU10',0x10,10),('vfeDCU11',0x10,11),('vfeDCU12',0x10,12)]

num_points = 30

class dcu:
    def __init__(self, name, addr, port, branch_form):
        self.name = name
        self.addr = addr
        self.port = port
        self.ch = []
        self.brch = branch_form

dcus = []
dcuAddress = {}
for dcuname in dcunames:
    d = dcu(dcuname[0], dcuname[1], dcuname[2], branch_form)
    dcus.append(d)
    dcuAddress[d.name] = array.array('d',[0,0,0,0,0,0,0,0])

rootFile = ROOT.TFile("DCUdata_2.root", "recreate")
tt = ROOT.TTree("DCUdata", "DCUdata")
for dcu in dcus:
    tt.Branch(dcu.name,dcuAddress[dcu.name], dcu.brch)

## Simple converter for full 32 bit strings stored as in int
def regToString(reg): 
    register = [0,0,0,0]
    register[0] = (0xFF000000 & reg) >> 24
    register[1] = (0x00FF0000 & reg) >> 16
    register[2] = (0x0000FF00 & reg) >> 8
    register[3] = 0x000000FF & reg
    return ''.join(map(chr, register))

## Defining a function for recording time for repeated measurements
#def time_stepper(num_points,delay):
 #   count=0
  #  while count<num_points:
   #     time.sleep(delay)
    #    count+=1
     #   time=count*delay
	


## We're supposed to have the ability to call the Manager's Dispatch function
## but it doesn't work, so as a work around I directly call the IPBus Client. 
def readReg(client, node): 
    address = node.getAddress()
    reg = client.read(address)
    client.dispatch()
    return reg
    
def to8BitStr(char):
    # convert passed in character to an 8-bit string of 1's & 0's that is
    # reversed so that index [0] refers to the rightmost bit in byte
    return bin(ord(char))[2:].zfill(8)[::-1]

def toHexStr(char):
    # convert passed in character to a uppercase hex byte without '0x'
    return hex(ord(char)).split('x')[1].zfill(2).upper()

########
## From: http://code.activestate.com/recipes/510399-byte-to-hex-and-hex-to-byte-string-conversion/
#
def int2byteBE(val, width=32):
    """
    Convert a signed integer value of specified width into a byte string.
    """
    if val < 0:
        val = val | (1 << width)
    return ''.join([chr((val >> 8*n) & 255) for n in reversed(range(width/8))])

def int2byteLE(val, width=32):
    """
    Convert a signed integer value of specified width into a byte string.
    """
    if val < 0:
        val = val | (1 << width)
    return ''.join([chr((val >> 8*n) & 255) for n in (range(width/8))])

def byte2intBE(bstr, width=32):
    """
    Convert a byte string into a signed integer value of specified width.
    """
    val = sum(ord(b) << 8*n for (n, b) in enumerate(reversed(bstr)))
    if val >= (1 << (width - 1)):
        val = val - (1 << width)
    return val

def byte2intLE(bstr, width=32):
    """
    Convert a byte string into a signed integer value of specified width.
    """
    val = sum(ord(b) << 8*n for (n, b) in enumerate(bstr))
    if val >= (1 << (width - 1)):
        val = val - (1 << width)
    return val
########

#based on: https://mail.python.org/pipermail/tutor/2009-September/071393.html
def round_figures(x, n):
    if x == 0:
        return 0
    else:
        return round(x, int(n - math.ceil(math.log10(abs(x)))))
########


parser = argparse.ArgumentParser(description="Write a test message to the Token Ring FIFOs and send it then read out the received message. They should be the same")
parser.add_argument('-a', '--connectionFile', type=str, default='glibConnectECAL.xml',
                    help='Connect Address File with connections to the glib_v3 Card')
parser.add_argument('-f', '--file', type=str, default=None)

args = parser.parse_args()

outputFile = None
if args.file:
    outputFile = args.file
    print 'Outputting to {0}'.format(outputFile)


#if not args.connectionFile:
#    print 'Please provide a connections file'
#    exit()

#if not args.connectionFile:
#    connectFile = 'glibConnectECAL.xml'
#else:
connectFile = args.connectionFile
    
connFile = 'file://{0};'.format(connectFile)
print 'Using {0} as connection file'.format(connFile)


#Create uHal Manager and attempt to connect to the glib

# Only show log messages with levels Warning and above
uhal.setLogLevelTo( uhal.LogLevel.WARNING )
# Don't show any log messages
#uhal.disableLogging()

manager = uhal.ConnectionManager(connFile)
hw = manager.getDevice('glib_v3')

#print hw.getNodes()

#read Board ID, Sys ID, Firmware ID

client = hw.getClient()

userID = hw.getNode("user_ipb.stat.id").read()
userVersionMajor = hw.getNode("user_ipb.ver_major").read()
userVersionMinor = hw.getNode("user_ipb.ver_minor").read()
userVersionBuild = hw.getNode("user_ipb.ver_build").read()
user_ver_yyyy  = hw.getNode("user_ipb.firmware_yy").read()
user_ver_mm  = hw.getNode("user_ipb.firmware_mm").read()
user_ver_dd  = hw.getNode("user_ipb.firmware_dd").read()

hw.dispatch() # Send IPbus transactions

# Modify any register values after dispatch but before use
user_ver_yyyy += 2000

print '\nFirmware Information'
print '\nUser Type: {0}'.format(regToString(userID))
print 'User Version: {0}.{1}.{2} ({3})'.format(userVersionMajor, userVersionMinor, userVersionBuild, '/'.join([str(user_ver_dd),str(user_ver_mm),str(user_ver_yyyy)]))

#######################################################

def composeTRmsg(dest, source, ch, data):
    msgB = array.array('B')
        
    msgB.append(dest)                     # Dest Token Ring node
    msgB.append(source)                   # Source Token Ring node (us)

    lenData = len(data)+2                 # adding two bytes to data payload
    if lenData <= 127:
        msgB.append(lenData)
    else:
        # Need 2-byte encoding of length. The msb of first byte gets set to a '1'.
        msgB.append((lenData & 0x007F) | 0x0080)
        msgB.append((lenData & 0x07F80) >> 7)

    msgB.append(ch)
    msgB.append(composeTRmsg.transnum)
    msgB.extend(data)

    composeTRmsg.transnumLast = composeTRmsg.transnum # save the last used transaction number

    composeTRmsg.transnum += 1            # increment transaction number so it is unique
    if (composeTRmsg.transnum > 255):
        composeTRmsg.transnum = 1         # wrap around byte and skip 0
    
    ## Make length of msgB evenly divisible by 4
    len4 = len(msgB) % 4
    if (len4 > 0):
        msgB.extend([0] * (4 - len4))
        
    #return msgB

    msgU32 = array.array('I')
    for sb0,sb1,sb2,sb3 in zip(*[iter(msgB)]*4):
        #print("%08X" % ((sb3 << 24) | (sb2 << 16) | (sb1 << 8) | sb0))
        msgU32.append((sb0 << 24) | (sb1 << 16) | (sb2 << 8) | sb3)

    ## For debug - 
    #msgU32.append(0xDEADBEEF)
    #msgU32.append(0xDEADCABB)
            
    #return struct.unpack('<I', msgB)
    
    ## Now convert from a byte array to a uInt32 array
    #msgU32 = array.array('l')
    #struct.pack_into("<I", msgU32, 0, msgB)

    return msgU32

composeTRmsg.transnum = 1 # init transaction number (do NOT use 0)
composeTRmsg.transnumLast = composeTRmsg.transnum

def receiveTRmsgOLD():
    msg = []
    maxReads = 1023  ## The receive FIFO is limited to 1023 words

    # First check if FIFO has anything
    recMT = hw.getNode("user_ipb.tr.stat0.rec_empty").read()
    hw.dispatch() # Send IPbus transactions
    
    ## Read 32-bits words from FIFO until it is empty
    while ((recMT == 0) and (maxReads > 0)):
        rxWord = hw.getNode("user_ipb.tr.rec_fifo").read()
        recMT = hw.getNode("user_ipb.tr.stat0.rec_empty").read()
        hw.dispatch() # Send IPbus transactions
        msg.append(rxWord)
        maxReads -= 1
        
    return msg

def decomposeTRmsg(msg):
    # First, convert from 32-bit list to byte array
    msgB = array.array('B')
    for dat in msg:
        msgB.append((dat & 0xFF000000) >> 24)
        msgB.append((dat & 0x00FF0000) >> 16)
        msgB.append((dat & 0x0000FF00) >> 8)
        msgB.append((dat & 0x000000FF) >> 0)
    
    ret = {}
    ret['dst'] = msgB[0]
    ret['src'] = msgB[1]

    msglen = msgB[2]        
    msgdat = 3
    if (msglen & 0x0080):
        # 2-byte length (mFEC firmware does not seem to handle this correctly)
        msglen = (msgB[3] << 7) | (msglen & 0x007F)
        msgdat = 4

    ret['len'] = msglen                     # data length
    ret['chn'] = msgB[msgdat]               # channel number
    ret['trn'] = msgB[msgdat+1]             # transaction number
    ret['dat'] = msgB[msgdat+2:msgdat+msglen] # The data
    ret['sta'] = msgB[msgdat+msglen]        # status byte

    return ret
    
def word2byte(word):
    return (((word & 0xFF000000) >> 24), ((word & 0x00FF0000) >> 16), ((word & 0x0000FF00) >> 8), ((word & 0x000000FF) >> 0))

def receiveTRmsg(rxTimeout=10):
    msg = []
    msgB = []
    maxReads = 1023  ## The receive FIFO is limited to 1023 words

    # First check if FIFO has anything
    recMT = 1
    rxTO = rxTimeout
    while ((recMT == 1) and (rxTO > 0)):
        recMT = hw.getNode("user_ipb.tr.stat0.rec_empty").read()
        hw.dispatch() # Send IPbus transactions
        rxTO -= 1

    if (recMT == 1):
        sys.exit("ERROR: Timeout waiting for Token Ring message!\n")
        
    # Read the first word to determine the length of the message
    rxWord = hw.getNode("user_ipb.tr.rec_fifo").read()
    recMT = hw.getNode("user_ipb.tr.stat0.rec_empty").read()
    hw.dispatch() # Send IPbus transactions

    msg.append(rxWord)                    # save original word for debug
    msgB.extend(word2byte(rxWord))

    ret = {}
    ret['dst'] = msgB[0]
    ret['src'] = msgB[1]

    msglen = msgB[2]        
    msgdat = 3
    if (msglen & 0x0080):
        # 2-byte length (mFEC firmware does not seem to handle this correctly)
        msglen = (msgB[3] << 7) | (msglen & 0x007F)
        msgdat = 4

    ret['len'] = msglen                     # data length

    # Compute number of 32-bit words left to read out
    bytes2read = (ret['len'] - (4 - msgdat)) + 1   # read the 1 byte status after the data
    words2read = (bytes2read / 4)
    if (bytes2read % 4):
        words2read += 1

    # read any remaining words
    while (words2read > 0):
        rxTO = rxTimeout
        while ((recMT == 1) and (rxTO > 0)):
            recMT = hw.getNode("user_ipb.tr.stat0.rec_empty").read()
            hw.dispatch() # Send IPbus transactions
            rxTO -= 1

        if (recMT == 1):
            sys.exit("ERROR: Timeout waiting for Token Ring message!\n")        
            
        # Read the next word
        rxWord = hw.getNode("user_ipb.tr.rec_fifo").read()
        recMT = hw.getNode("user_ipb.tr.stat0.rec_empty").read()
        hw.dispatch() # Send IPbus transactions

        msg.append(rxWord)                    # save original word for debug
        msgB.extend(word2byte(rxWord))
        words2read -= 1
        
    ret['chn'] = msgB[msgdat]               # channel number
    ret['trn'] = msgB[msgdat+1]             # transaction number
    ret['dat'] = msgB[msgdat+2:msgdat+msglen] # The data
    ret['sta'] = msgB[msgdat+msglen]        # status byte
    ret['msg'] = msg                        # the entire message in words (for debug)
    ret['mgB'] = msgB                       # the entire message in bytes (for debug)

    return ret


def TRstatus():
    ctrl0 = range(0, 10)
    ctrl0[0] = hw.getNode("user_ipb.tr.ctrl0.en_fec").read()
    ctrl0[1] = hw.getNode("user_ipb.tr.ctrl0.send" ).read()
    ctrl0[2] = hw.getNode("user_ipb.tr.ctrl0.sel_ser_out").read()
    ctrl0[3] = hw.getNode("user_ipb.tr.ctrl0.sel_ser_in" ).read()
    ctrl0[4] = hw.getNode("user_ipb.tr.ctrl0.rec_clk_pol").read()
    ctrl0[5] = hw.getNode("user_ipb.tr.ctrl0.disable_rec").read()
    ctrl0[6] = hw.getNode("user_ipb.tr.ctrl0.loopback").read()
    ctrl0[7] = hw.getNode("user_ipb.tr.ctrl0.soft_reset" ).read()
    ctrl0[8] = hw.getNode("user_ipb.tr.ctrl0.reset_link_b").read()
    ctrl0[9] = hw.getNode("user_ipb.tr.ctrl0.reset_link_a").read()
    stat0 = range(0, 12)
    stat0[0]  = hw.getNode("user_ipb.tr.stat0.tra_run" ).read()
    stat0[1]  = hw.getNode("user_ipb.tr.stat0.rec_run"  ).read()
    stat0[2]  = hw.getNode("user_ipb.tr.stat0.ill_send").read()
    stat0[3]  = hw.getNode("user_ipb.tr.stat0.rec_full" ).read()
    stat0[4]  = hw.getNode("user_ipb.tr.stat0.rec_empty").read()
    stat0[5]  = hw.getNode("user_ipb.tr.stat0.ret_full" ).read()
    stat0[6]  = hw.getNode("user_ipb.tr.stat0.ret_empty").read()
    stat0[7]  = hw.getNode("user_ipb.tr.stat0.tra_full"  ).read()
    stat0[8]  = hw.getNode("user_ipb.tr.stat0.tra_empty").read()
    stat0[9]  = hw.getNode("user_ipb.tr.stat0.link_inited").read()
    stat0[10] = hw.getNode("user_ipb.tr.stat0.irq_pend" ).read()
    stat0[11] = hw.getNode("user_ipb.tr.stat0.data_to_fec").read()
    stat1 = range(0, 8)
    stat1[0]  = hw.getNode("user_ipb.tr.stat1.ill_dat"  ).read()
    stat1[1]  = hw.getNode("user_ipb.tr.stat1.ill_seq"   ).read()
    stat1[2]  = hw.getNode("user_ipb.tr.stat1.crc_err" ).read()
    stat1[3]  = hw.getNode("user_ipb.tr.stat1.data_copied" ).read()
    stat1[4]  = hw.getNode("user_ipb.tr.stat1.addr_seen").read()
    stat1[5]  = hw.getNode("user_ipb.tr.stat1.err" ).read()
    stat1[6]  = hw.getNode("user_ipb.tr.stat1.time_out").read()
    stat1[7]  = hw.getNode("user_ipb.tr.stat1.clk_err"   ).read()

    ds = hw.getNode("user_ipb.tr.dipstick").read()
    hw.dispatch() # Send IPbus transactions

    print 'Ctrl0: ResetLinkA:{9} ResetLinkB:{8} SoftReset:{7} Loopback:{6} DisableRX:{5} RXClkPol:{4} SelSerIn:{3} SelSerOut:{2}    Send:{1}    EnFEC:{0}'.format(*ctrl0)
    print 'Stat0:  DataToFEC:{11}        IRQ:{10}  LinkInit:{9} TRAempty:{8}   TRAfull:{7} RETempty:{6}  RETfull:{5}  RECempty:{4} RECfull:{3}  ILLsend:{2} RECrun:{1} TRArun:{0}'.format(*stat0)
    print 'DS: {0:>5}'.format(ds), '                  Stat1:  ClkErr:{7}  TimeOut:{6}       Err:{5} AddrSeen:{4} DataCopy:{3}    CRCerr:{2}  ILLseq:{1}  ILLdata:{0}\n'.format(*stat1)


def postMsg(FE_CCU_addr, FECaddr, Channel, Data, Verbose=0):
    Msg = composeTRmsg(FE_CCU_addr, FECaddr, Channel, Data)
    #print "Received Msg: ", [ toHexStr(dat) for dat in Msg ]
    #print "Received Msg: ", binascii.hexlify(Msg)
    #print [hex(x) for x in Msg]

    if Verbose: print "Msg to Transmit: ", ' '.join( [ "%08X" % x for x in Msg ] )
    ##print ''

    ## Write the message words to the transmit fifo
    hw.getNode("user_ipb.tr.tra_fifo").writeBlock(list(Msg))
    hw.dispatch() # Send IPbus transactions

    ## Send the message (toggle the send bit)
    hw.getNode("user_ipb.tr.ctrl0.send").write(1)
    hw.dispatch() # Send IPbus transactions

    ## clear the Send bit (may be able to do this immediately after set it - not sure)
    hw.getNode("user_ipb.tr.ctrl0.send").write(0)
    hw.dispatch() # Send IPbus transactions

def sendMsg(FE_CCU_addr, FECaddr, Channel, Data, Verbose=0):
    # post the message to the TX FIFO and send it
    postMsg(FE_CCU_addr, FECaddr, Channel, Data, Verbose)

    # Wait and get the returned message and check it
    retMsg = recvMsg(10, Verbose)

    # check the returned message
    if (retMsg['dst'] != FE_CCU_addr or
        retMsg['src'] != FECaddr or
        retMsg['chn'] != Channel or
        retMsg['trn'] != composeTRmsg.transnumLast or
        retMsg['dat'] != Data):
        err = 1
        print "ERROR: Returned message mismatch (FEAddr: {0}/CCUchan: {1:02x})\n".format(FE_CCU_addr, Channel)
    elif(retMsg['sta'] != 0xB0):
        err = 2
        print "ERROR: Unexpected message status 0x{0:02X} (FEAddr: {1}/CCUchan: {2:02x})\n".format(retMsg['sta'], FE_CCU_addr, Channel)
    else:
        err = 0
        if Verbose: print "SUCCESS: Message Sent and received by destination\n"

    return err


def recvMsgOLD(IRQtimeout=10):
    IRQ = 0
    while (IRQ == 0 and IRQtimeout > 0):
        sleep(0.25)                            # wait a little
        IRQ = hw.getNode("user_ipb.tr.stat0.irq_pend").read()
        hw.dispatch()
        IRQtimeout -= 1

    if (IRQ == 0 and IRQtimeout == 0):
        sys.exit("ERROR: Timeout waiting for Token Ring message!\n")
    else:
        ## Attempt to read the Receive FIFO and print what was found
        rxMsg = receiveTRmsg()
        print "Msg Received:    ", ' '.join( [ "%08X" % x for x in rxMsg ] ) ##, '\n'
        # Then clear the IRQ
        hw.getNode("user_ipb.tr.ctrl1.clr_irq").write(1)
        hw.dispatch()        

        rxMsgD = decomposeTRmsg(rxMsg)
        print "Msg Dest:{0} Src:{1} Len:{2} Status:{3:#x} Channel:{4} Transaction:{5}".format(rxMsgD['dst'], rxMsgD['src'], rxMsgD['len'], rxMsgD['sta'], rxMsgD['chn'], rxMsgD['trn']) 
        print "Msg Data: ", ' '.join( [ "%02X" % x for x in rxMsgD['dat'] ] )
        print ""

    return rxMsgD
    
def recvMsg(RXtimeout=10, Verbose=0):
    ## Could look at IRQ, but sinc ehave to poll, might as well just poll for RECempty.

    ## Read the Receive FIFO and print what was found
    rxMsg = receiveTRmsg(RXtimeout)
    if Verbose: print "Msg Received:    ", ' '.join( [ "%08X" % x for x in rxMsg['msg'] ] ) ##, '\n'

    # Then clear any IRQ
    hw.getNode("user_ipb.tr.ctrl1.clr_irq").write(1)
    hw.dispatch()        

    if Verbose: 
        print "Msg Dest:{0} Src:{1} Len:{2} Status:{3:#x} Channel:{4} Transaction:{5}".format(rxMsg['dst'], rxMsg['src'], rxMsg['len'], rxMsg['sta'], rxMsg['chn'], rxMsg['trn']) 
        print "Msg Data: ", ' '.join( [ "%02X" % x for x in rxMsg['dat'] ] )

    return rxMsg
    
def replyMsg(FE_CCU_addr, FECaddr, Channel, RXtimeout=10, Verbose=0):
    rxmsg = recvMsg(RXtimeout, Verbose)

    # Check that the expected source, destination and channel are
    # correct. Remember that for the reply, the source and destination
    # are expected to be swapped since the reply is orginating on the
    # other end.
    if (rxmsg['src'] != FE_CCU_addr or
        rxmsg['dst'] != FECaddr or
        rxmsg['chn'] != Channel or
        rxmsg['trn'] != composeTRmsg.transnumLast):
        err = 1
        print "ERROR: Reply message mismatch\n"
    elif(rxmsg['sta'] != 0x80):
        err = 2
        print "ERROR: Unexpected message status 0x{0:02X}\n".format(retMsg['sta'])
    else:
        err = 0
        if Verbose: print "SUCCESS: Reply Message received from destination\n"

    rxmsg['err'] = err
    return rxmsg

## Check the returned ACK from I2C access
def checkI2CAck(CCU_addr, I2C_Port, I2C_Addr, ACK):
    if (ACK != 0x04):
        sys.exit("ERROR: I2C (FEAddr: {0} / I2C Port {1} / I2CAddr: 0x{2:02x}) Write failed with status: 0x{3:02x}".format(CCU_addr, I2C_Port, I2C_Addr, ACK))

# Initialize the front end
def initFE(FE_CCU_addr, FECaddr):

    ## According to the CCU ASIC specification, in order to write to
    ## CCU Node Control Reg. E, must write and read all 5 control
    ## registers.

    # All accesses here are to Channel 0
    Channel = 0

    # Write Control Reg. A (resets)
    sendMsg(FE_CCU_addr, FECaddr, Channel, [0, 0xC0])   # Clear error and Reset All Channels

    # Wait for resets to complete
    sleep (0.1)
    
    # Write Control Reg. A (resets)
    sendMsg(FE_CCU_addr, FECaddr, Channel, [0, 0])

    # Write Control Reg. B (alarms)
    sendMsg(FE_CCU_addr, FECaddr, Channel, [1, 0]) 

    # Write Control Reg. C (redundancy)
    #
    # Not sure why, but writing 0 returns a status os 0x80 which
    # implies that the FE did not accept the write request. So setting
    # one of the reserved bits high seems to give us a 0xB0 status, as
    # expected. Hopefully the reserved bit is not a special test bit.
    sendMsg(FE_CCU_addr, FECaddr, Channel, [2, 0x80])

    # Write Control Reg. D (broadcast)
    sendMsg(FE_CCU_addr, FECaddr, Channel, [3, 0])

    # Write Control Reg. E (Enable Channels)
    # Enabling All I2C Channels (reduce this, if needed) and All PIO Channels
    # Left Disabled: JTAG, trigger and memory controllers
    sendMsg(FE_CCU_addr, FECaddr, Channel, [4, 0xFF, 0xFF, 0x0F])

    # Read Ctrl Reg. A
    sendMsg(FE_CCU_addr, FECaddr, Channel, [0x10])
    rxMsg = replyMsg(FE_CCU_addr, FECaddr, Channel)
    ctrlA = rxMsg['dat'][0]
    
    # Read Ctrl Reg. B
    sendMsg(FE_CCU_addr, FECaddr, Channel, [0x11])
    rxMsg = replyMsg(FE_CCU_addr, FECaddr, Channel)
    ctrlB = rxMsg['dat'][0]
    
    # Read Ctrl Reg. C
    sendMsg(FE_CCU_addr, FECaddr, Channel, [0x12])
    rxMsg = replyMsg(FE_CCU_addr, FECaddr, Channel)
    ctrlC = rxMsg['dat'][0]
    
    # Read Ctrl Reg. D
    sendMsg(FE_CCU_addr, FECaddr, Channel, [0x13])
    rxMsg = replyMsg(FE_CCU_addr, FECaddr, Channel)
    ctrlD = rxMsg['dat'][0]
    
    # Read Ctrl Reg. E
    sendMsg(FE_CCU_addr, FECaddr, Channel, [0x14])
    rxMsg = replyMsg(FE_CCU_addr, FECaddr, Channel)
    ctrlE = rxMsg['dat'][2] << 16 | rxMsg['dat'][1] << 8 | rxMsg['dat'][0]

    print "Node Ctrl Reg: A:0x{0:02x} B:0x{1:02x} C:0x{2:02x} D:0x{3:02x} E:0x{4:06x}\n".format(ctrlA, ctrlB,  ctrlC,  ctrlD,  ctrlE)

    ## Now, set up all I2C channels to always return the ACK even if
    ## successful. This makes coding easier if you always know to
    ## expect the response.
    ##
    ## Also setting the clock frequency to 100 KHz (plenty fast for us)
    for chan in range(0x10, 0x20):
        sendMsg(FE_CCU_addr, FECaddr, chan, [0xF0, 0x40])
    


def writeTRI2Cbyte(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr, Data):
    Channel = I2C_Port + 15 # convert I2C_Port number to CCU Channel
    sendMsg(FE_CCU_addr, FECaddr, Channel, [0x00, I2C_Addr, Data])
    rxMsg = replyMsg(FE_CCU_addr, FECaddr, Channel)
    checkI2CAck(FE_CCU_addr, I2C_Port, I2C_Addr, rxMsg['dat'][0])
    return rxMsg['dat'][0]                # return ACK in case it is useful

def readTRI2Cbyte(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr):
    Channel = I2C_Port + 15 # convert I2C_Port number to CCU Channel
    sendMsg(FE_CCU_addr, FECaddr, Channel, [0x01, I2C_Addr | 0])
    rxMsg = replyMsg(FE_CCU_addr, FECaddr, Channel)
    checkI2CAck(FE_CCU_addr, I2C_Port, I2C_Addr, rxMsg['dat'][1])
    return rxMsg['dat'][0]

def readTRDCU(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr, DCU_Ch):
    # Make sure that (TREG) is 0x10 to turn the bandgap on
    #
    # NOTE: Only need to do this once after power on so if can handle
    # the bookeeping, could keep from having to write this over and
    # over.
    writeTRI2Cbyte(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr | 4, 0x10)

    # Make sure that (AREG) is 0x00 - it appears to always be after power-on
    writeTRI2Cbyte(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr | 2, 0x00)
    
    # Write 0x88+DCU_Ch to CREG (0) to start acquition on temperature reading
    writeTRI2Cbyte(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr | 0, (0x88 | DCU_Ch))

    sleep(0.1)                               # give time for conversion

    # Read DCU Control Reg (CREG)
    #creg = readTRI2Cbyte(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr | 0)

    # Read DCU Status/High Data Reg (SHREG)
    shreg = readTRI2Cbyte(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr | 1)

    # Read DCU Aux Reg (AREG)
    #areg = readTRI2Cbyte(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr | 2)

    # Read DCU Data Low Reg (LREG)
    lreg = readTRI2Cbyte(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr | 3)

    # Read DCU Test Reg (TREG)
    #treg = readTRI2Cbyte(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr | 4)

    # Read DCU ID Low, Medium and High Reg
    # idlreg = readTRI2Cbyte(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr | 5)
    # idmreg = readTRI2Cbyte(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr | 6)
    # idhreg = readTRI2Cbyte(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr | 7)
    # idres = idhreg
    # idres <<= 8
    # idres += idmreg
    # idres <<= 8
    # idres += idlreg
    # idstr = 'id: h:{0},m:{1},l:{2}, combo:{3}'.format(idhreg, idmreg, idlreg, hex(idres))
    # print idstr

    if (not(shreg & 0x80)):
        sys.exit("ERROR: DCU is not is IDLE state when data is read. Extend wait time before read.")
    
    return ((shreg & 0x0F) << 8 | lreg)
    
### Check mFEC Version
trVersion = hw.getNode("user_ipb.tr.version").read()

### Issue a reset to FE
hw.getNode("user_ipb.ctrl.tr_reset_oe").write(0)
hw.getNode("user_ipb.ctrl.tr_reset_o").write(0)
hw.dispatch() # Send IPbus transactions

hw.getNode("user_ipb.ctrl.tr_reset_o").write(1)
hw.dispatch() # Send IPbus transactions

hw.getNode("user_ipb.ctrl.tr_reset_o").write(0)
hw.dispatch() # Send IPbus transactions

### Issue a soft reset
hw.getNode("user_ipb.tr.ctrl0.soft_reset").write(1)
hw.dispatch() # Send IPbus transactions

### Issue a reset_link_a
#hw.getNode("user_ipb.tr.ctrl0.reset_link_a").write(1)
#hw.dispatch() # Send IPbus transactions

### Setup Source Register to be 0
#hw.getNode("user_ipb.tr.source").write(0)

### Make sure ctrl0 is 0
hw.getNode("user_ipb.tr.ctrl0").write(0)

### Clear any errors or IRQs
hw.getNode("user_ipb.tr.ctrl1.clr_irq").write(1)
hw.getNode("user_ipb.tr.ctrl1.clr_errors").write(1)

### Select A for serial input and output
hw.getNode("user_ipb.tr.ctrl0.sel_ser_out").write(0)
hw.getNode("user_ipb.tr.ctrl0.sel_ser_in").write(0)

### Set received clock polarity to '1' (need to experiment to see which works best)
### Right now - cannot get Link Initialized if set to '0' even with the long cable
hw.getNode("user_ipb.tr.ctrl0.rec_clk_pol").write(1)

### Enable Token Ring
hw.getNode("user_ipb.tr.ctrl0.en_fec").write(1)

hw.dispatch() # Send IPbus transactions

print "mFEC Firmware Version: %02x" % trVersion
print ''

# Check that the link is initialized and that no pending operation is active.
tra_run = hw.getNode("user_ipb.tr.stat0.tra_run").read()
rec_run = hw.getNode("user_ipb.tr.stat0.rec_run").read()
link_inited = hw.getNode("user_ipb.tr.stat0.link_inited").read()
RECempty = hw.getNode("user_ipb.tr.stat0.rec_empty").read()

ctrl0 = hw.getNode("user_ipb.tr.ctrl0").read()
stat0 = hw.getNode("user_ipb.tr.stat0").read()
stat1 = hw.getNode("user_ipb.tr.stat1").read()
ds = hw.getNode("user_ipb.tr.dipstick").read()
hw.dispatch() # Send IPbus transactions

print "Before start:  TX Run: %d RX Run: %d Link Init: %d  ctrl0: %08x  stat0: %08x  stat1: %08x  dipstick: %08x\n" % (tra_run, rec_run, link_inited, ctrl0, stat0, stat1, ds)
TRstatus()

if (not link_inited):
    sys.exit("ERROR: Token Ring Link is NOT Initialized. Link may be broken.")

## If Receive FIFO is not empty, then read it out - likely truncated message from power on/reset
if (not RECempty):
    print "Some data in FIFO. Flushing it."
    rxMsg = receiveTRmsg()
    print "Msg Received (?): ", ' '.join( [ "%08X" % x for x in rxMsg ] ), '\n'

# Also clear any IRQ or errors
hw.getNode("user_ipb.tr.ctrl1.clr_irq").write(1)
hw.getNode("user_ipb.tr.ctrl1.clr_errors").write(1)

FECaddr = 0
FE_CCU_addr = 8

# Initialize the front end
initFE(FE_CCU_addr, FECaddr)

## Old Test
if (0):
        # Read Stat Reg. A
        Channel = 0
        sendMsg(FE_CCU_addr, FECaddr, Channel, [0x20])
        rxMsg = replyMsg(FE_CCU_addr, FECaddr, Channel)                         # capture the reply
        #@@@#print "Actual Length of RX Data: ", len(rxMsg['dat']), "\n"
        #@@@#print "Send Data: ", ' '.join( [ "%02X" % x for x in Data ] )
        print "Stat Reg. A: 0x{0:02X}\n".format(rxMsg['dat'][0])
        TRstatus()

        # Read Stat Reg. E
        Channel = 0
        sendMsg(FE_CCU_addr, FECaddr, Channel, [0x24])
        rxMsg = replyMsg(FE_CCU_addr, FECaddr, Channel)                         # capture the reply
        #@@@#print "Actual Length of RX Data: ", len(rxMsg['dat']), "\n"
        #@@@#print "Send Data: ", ' '.join( [ "%02X" % x for x in Data ] )
        print "Stat Reg. E: {0:02x} {1:02x} {2:02x}\n".format(*rxMsg['dat'])
        TRstatus()

## Old Test
if (0):
        # I2C Ports start at channel 0x10, so select port 15:
        I2C_Port = 15 - 1                              # LVRB (not sure if working)
        #@@@#I2C_Port = 10                             # VFE #2
        Channel = 0x10 + I2C_Port                      # For LVR DCU's

        # Read I2C Control Reg. A
        sendMsg(FE_CCU_addr, FECaddr, Channel, [0xF1])
        rxMsg = replyMsg(FE_CCU_addr, FECaddr, Channel)

        print "I2C Port {0}: CtrlA {1:#x}".format(I2C_Port, rxMsg['dat'][0])
        TRstatus()

        # Write I2C Control Reg. A (set to 2 for 200 kHz - just a test)
        sendMsg(FE_CCU_addr, FECaddr, Channel, [0xF0, 2])
        print "Wrote 2 to I2C CtrlA"

        # Read I2C Control Reg. A
        sendMsg(FE_CCU_addr, FECaddr, Channel, [0xF1])
        rxMsg = replyMsg(FE_CCU_addr, FECaddr, Channel)
        print "I2C Port {0}: CtrlA {1:#x}".format(I2C_Port, rxMsg['dat'][0])

        # Write I2C Control Reg. A (set back to 0 for 100 kHz and set FACKW)
        sendMsg(FE_CCU_addr, FECaddr, Channel, [0xF0, 0x40])
        print "Wrote 0 to I2C CtrlA"

        # Read I2C Control Reg. A
        sendMsg(FE_CCU_addr, FECaddr, Channel, [0xF1])
        rxMsg = replyMsg(FE_CCU_addr, FECaddr, Channel)
        print "I2C Port {0}: CtrlA {1:#x}".format(I2C_Port, rxMsg['dat'][0])

        print "\n\n\n"

        
### Finally, can now try reading temperature from LVRB / SLVRB DCU 0x10

I2C_Port = 15
I2C_Addr = 0x40
DCU_Ch = 7


idlreg = readTRI2Cbyte(FE_CCU_addr, FECaddr, I2C_Port, 0x10 | 5)
idmreg = readTRI2Cbyte(FE_CCU_addr, FECaddr, I2C_Port, 0x10 | 6)
idhreg = readTRI2Cbyte(FE_CCU_addr, FECaddr, I2C_Port, 0x10 | 7)

idres = idhreg
idres <<= 8
idres += idmreg
idres <<= 8
idres += idlreg

temp40 = readTRDCU(FE_CCU_addr, FECaddr, I2C_Port, 0x40, DCU_Ch)
temp20 = readTRDCU(FE_CCU_addr, FECaddr, I2C_Port, 0x20, DCU_Ch)
temp10 = readTRDCU(FE_CCU_addr, FECaddr, I2C_Port, 0x10, DCU_Ch)

print "DCU I2CAddr Temp(0x10) 0x{0:06x}, {1:04x} / Temp(0x20) {2:04x} / Temp(0x40) {3:04x}\n\n".format(idres, temp10, temp20, temp40)

I2C_Port = 11
I2C_Addr = 0x10
VFE2dcu10 = []
for dcu_chan in range(0, 8):
    pass
#    VFE2dcu10.append(readTRDCU(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr, dcu_chan))
#    print "VFE DCU I2CPort {0} I2CAddr 0x{1:02x} Channel {2}: (ADC:{3:04x})".format(I2C_Port, I2C_Addr, dcu_chan, VFE2dcu10[dcu_chan])    
#print

csvHeader = 'VFE DCU I2CPort, I2CAddr, DCU Channel, ADC Value (hex), ADC Value(dec)\n'
csvLine = '{0},0x{1:02x},{2},{3:04x}, {4}\n'
now = datetime.now().strftime('%Y-%m-%d-%H:%M:%S')



VFEData = {}
f = open(outputFile, 'w+')
f.write(csvHeader)
for port in [9, 10, 11, 12]:
    for dcu_chan in range(0,8):
        key = '{0}:{1}'.format(port, dcu_chan)
        VFEData[key] = readTRDCU(FE_CCU_addr, FECaddr, port, I2C_Addr, dcu_chan)
        f.write(csvLine.format(port, I2C_Addr, dcu_chan, VFEData[key], VFEData[key])) 
        print "key: {0}, VFE DCU I2CPort {1} I2CAddr 0x{2:02x} Channel {3}: (ADC:{4:04x})".format(key, I2C_Port, I2C_Addr, dcu_chan, VFEData[key])    


#I2C_Port = 11
#I2C_Addr = 0x10
#VFE2dcu10 = []
#for dcu_chan in range(0, 8):
#    VFE2dcu10.append(readTRDCU(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr, dcu_chan))
#    print "VFE DCU I2CPort {0} I2CAddr 0x{1:02x} Channel {2}: (ADC:{3:04x})".format(I2C_Port, I2C_Addr, dcu_chan, VFE2dcu10[dcu_chan])    

try:
    I2C_Port = 12
    I2C_Addr = 0x10
    VFE2dcu10 = []
    for dcu_chan in range(0, 8):
        VFE2dcu10.append(readTRDCU(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr, dcu_chan))
        print "VFE DCU I2CPort {0} I2CAddr 0x{1:02x} Channel {2}: (ADC:{3:04x})".format(I2C_Port, I2C_Addr, dcu_chan, VFE2dcu10[dcu_chan])    
    print

    # I2C_Port = 10
    # I2C_Addr = 0x10
    # VFE2dcu10 = []
    # for dcu_chan in range(0, 8):
    #     VFE2dcu10.append(readTRDCU(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr, dcu_chan))
    #     print "VFE DCU I2CPort {0} I2CAddr 0x{1:02x} Channel {2}: (ADC:{3:04x})".format(I2C_Port, I2C_Addr, dcu_chan, VFE2dcu10[dcu_chan])    
    # print

    I2C_Port = 9
    I2C_Addr = 0x10
    VFE2dcu10 = []
    for dcu_chan in range(0, 8):
        VFE2dcu10.append(readTRDCU(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr, dcu_chan))
        print "VFE DCU I2CPort {0} I2CAddr 0x{1:02x} Channel {2}: (ADC:{3:04x})".format(I2C_Port, I2C_Addr, dcu_chan, VFE2dcu10[dcu_chan])    
    print

except Exception as e:
    print 'Ran into an error reading VFES...{0}'.format(e)
#I2C_Port = 13
#I2C_Addr = 0x10
#VFE2dcu10 = []
#for dcu_chan in range(0, 8):
#    VFE2dcu10.append(readTRDCU(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr, dcu_chan))
#    print "VFE DCU I2CPort {0} I2CAddr 0x{1:02x} Channel {2}: (ADC:{3:04x})".format(I2C_Port, I2C_Addr, dcu_chan, VFE2dcu10[dcu_chan])    
#print


# Dictionary indexed by I2C Address in hexadecimal string. The channel
# order is IA0, IA1, IA2, IA3, IA4, IA5. The data is the bottom
# resistor followed by the top resistor in the voltage divider. If no
# voltage divider, make top tiny compared to bottom.
ADCconv = {'0x40': [[1000000,0], [10000,100000], [10000,100000], [10000,27000],  [1000000,0],    [10000,100000], [1000000,0], [1000000,0]],
           '0x20': [[1000000,0], [10000,27000],  [10000,27000],  [10000,27000],  [10000,27000],  [10000,27000],  [1000000,0], [1000000,0]],
           '0x10': [[1000000,0], [10000,100000], [10000,100000], [10000,100000], [10000,100000], [10000,100000], [1000000,0], [1000000,0]]}

I2C_Port = 15   
#DCUHeader =  '\nI2CAddr, DCU Channel, ADC Value(hex), ADC Value(Dec), time\n'
#DCULine = '{0}, {1}, {2}, {3} mV, {4} s\n'
#f.write(DCUHeader)
#num_points=500
#delay=1
count=0

#while count<num_points:
#    for I2C_Addr in (0x10, 0x20, 0x40):
#        ADCdata = []
#        for dcu_chan in range(0, 8):
#            ADCdata.append(readTRDCU(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr, dcu_chan))
#        for dcu_chan in range(0, 8):
#            VREFdata = ((ADCdata[dcu_chan]) / (2.0**12)) * 1250
#            Rbot = ADCconv[hex(I2C_Addr)][dcu_chan][0]
#            Rtop = ADCconv[hex(I2C_Addr)][dcu_chan][1]
#            Vdata = VREFdata * ((Rtop+Rbot)/(Rbot*1.0))
#            print "DCU I2CAddr {0:02x} Channel {1}: {2:14.5}mV {3:6.4}mV (ADC:{4:04x}) Time {4}s".format(I2C_Addr, dcu_chan, Vdata, VREFdata, ADCdata[dcu_chan], time)
#	    f.write(DCULine.format(I2C_Addr, dcu_chan, ADCdata[dcu_chan], Vdata, time)) 
#    count+=1
#    time=delay*count
#        print

timeStamp = TDatime()
dT = 1

#Setup Time Branch, move this to the top of the script after testing
b = tt.Branch('timestamp', timeStamp)

while count<num_points:
    timeStamp.Set() # This will set time to the current time
    print "Starting capture at {0}".format(timeStamp.AsString())
    for dcu in dcus:
	I2C_Addr = dcu.addr
        I2C_Port = dcu.port
        ADCdata = []
        for dcu_chan in range(1,8):
       	    ADCdata.append(readTRDCU(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr,dcu_chan))
        for dcu_chan in range(0, 7):
            VREFdata = ((ADCdata[dcu_chan]) / (2.0**12)) * 1250
            Rbot = ADCconv[hex(I2C_Addr)][dcu_chan][0]
            Rtop = ADCconv[hex(I2C_Addr)][dcu_chan][1]
            Vdata = VREFdata * ((Rtop+Rbot)/(Rbot*1.0))
            arr = dcuAddress[dcu.name]
#            arr[dcu_chan] = Vdata
	    arr[dcu_chan] = ADCdata[dcu_chan]
    count+=1
    tt.Fill()
    timeStamp.Set(timeStamp.Convert()+dT)
    #Replace busy loop with a Timer thread or something more useful
    while(TDatime().Convert() < timeStamp.Convert()):
        time.sleep(0.01)


#while count<num_points:
#    for I2C_Addr in (0x10, 0x20, 0x40):
#        ADCdata = []
#        for dcu_chan in range(1, 8):
#            ADCdata.append(readTRDCU(FE_CCU_addr, FECaddr, I2C_Port, I2C_Addr,dcu_chan))
#        for dcu_chan in range(1, 8):
#            VREFdata = ((ADCdata[dcu_chan]) / (2.0**12)) * 1250
#            Rbot = ADCconv[hex(I2C_Addr)][dcu_chan][0]
#            Rtop = ADCconv[hex(I2C_Addr)][dcu_chan][1]
#            Vdata = VREFdata * ((Rtop+Rbot)/(Rbot*1.0))
            
            



#if I2C_Addr == 0x10:
#                dcu1[dcu_chan]=Vdata
#            if I2C_Addr == 0x20:
#                dcu2[dcu_chan]=Vdata
#            if I2C_Addr == 0x40:
#                dcu3[dcu_chan]=Vdata
#    t=delay*count
#    count+=1
#    sleep(delay)
#    tt.Fill()

rootFile.Write()
rootFile.Close()
print "\nDone!"            
TRstatus()
