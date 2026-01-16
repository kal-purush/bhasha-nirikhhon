#!/usr/bin/python

import crcmod
import serial
import threading
import Queue
import struct
import time

crc8 = crcmod.mkCrcFun(0x1D5,initCrc=0,rev=False)

header_format = 'BBcBI'
header_fields = ['start','length','type','flags','sequence']

data_formats = {
  'C': {
    'format': 'ff',
    'fields': ['a0','a1']},
  'S': {
    'format': 'Iffffffff',
    'fields': ['time','p0','p1','v0','v1','c0','c1','w0','w1']},
}

MODE_NONE = 0
MODE_HOME = 1
MODE_PWM  = 2
MODE_POS  = 3
MODE_VEL  = 4
MODE_ERR  = 5
MODE_NO_CHANGE = 6

def parse_packet(packet):
  packet_type = packet[2]
  data_format = data_formats[packet_type]['format']
  data_fields = data_formats[packet_type]['fields']
  packet_format = header_format + data_format + 'B'
  packet_fields = header_fields + data_fields + ['crc']
  packet_values = struct.unpack(packet_format,packet)
  return dict(zip(packet_fields,packet_values))

def format_packet(packet):
  data_format = data_formats[packet['type']]['format']
  data_fields = data_formats[packet['type']]['fields']
  packet_format = header_format + data_format
  packet_fields = header_fields + data_fields
  packet.update({'start':0, 'length':struct.calcsize(packet_format)+1})
  packet_values = [packet[field] for field in packet_fields]
  packet = struct.pack(packet_format, *packet_values)
  packet += chr(crc8(packet))
  return packet
  
class PacketParser(threading.Thread):
  def __init__(self, device='/dev/ttyACM0',baud=230400):
    threading.Thread.__init__(self)
    self.daemon = True

    self.serial = serial.Serial(device,baudrate=baud)
    self.packet = ''
    self.queue = Queue.Queue()
    self.sequence = 0

  def __del__(self):
    self.flush()
    self.serial.close()

  def run(self):
    while(self.serial.isOpen()):
      self.packet = self.serial.read(1)
      if self.packet == '\x00':
        self.packet += self.serial.read(1)
        length = ord(self.packet[-1])
        if length >= 9:
          self.packet += self.serial.read(length-2)
          if crc8(self.packet) == 0:
            self.queue.put(self.packet)
  
  def get(self,block=False,timeout=None):
    try:
      return parse_packet(self.queue.get(block,timeout))
    except:
      return None

  def flush(self):
    rp = self.get()
    pkts = []
    while rp is not None:
      pkts.append(rp)
      rp = self.get()
    return pkts

  def put(self, packet):
    if type(packet) is dict:
      packet.update({'sequence':self.sequence})
      packet = format_packet(packet)

    if (self.serial.isOpen()):
      self.sequence += 1
      self.serial.write(packet)

def get_command_packet(a0=0.0, a1=0.0, m0=MODE_NO_CHANGE, m1=MODE_NO_CHANGE):
  return {
    'type': 'C',
    'flags': (m1 << 4) | m0,
    'a0': a0,
    'a1': a1,
    'sequence': 0
  }

if __name__ == '__main__':
  pp = PacketParser()
  pp.start()
  while(True):
    time.sleep(0.01)
    packet = pp.get(True,timeout=0.001)
    if packet:
      print packet