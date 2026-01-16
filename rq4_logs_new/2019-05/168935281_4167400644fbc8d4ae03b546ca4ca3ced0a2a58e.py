import json
import pprint


pkt = dict()

def packet_to_json(packet):

  """ This module fetches each fields in available in each layer and creates a dictionary"""

  l2 = dict()
  l3 = dict()
  l4 = dict()
  for layer in packet.layers:
    if layer.layer_name == 'eth':
      for field in layer.field_names:
        f = "l2_"+str(field)
        l2[f] = layer.get_field_value(field) 
    pkt["L2"] = l2

    if layer.layer_name == 'ip':
      for field in layer.field_names:
        f = "l3_"+str(field)
        l3[f] = layer.get_field_value(field) 
    pkt["L3"] = l3

    if layer.layer_name == 'udp':
      for field in layer.field_names:
        f = "l4_"+str(field)
        l4[f] = layer.get_field_value(field) 
    pkt["L4"] = l4

    if layer.layer_name == 'icmp':
      for field in layer.field_names:
        f = "l4_"+str(field)
        l4[f] = layer.get_field_value(field) 
    pkt["L4"] = l4

  return pkt
