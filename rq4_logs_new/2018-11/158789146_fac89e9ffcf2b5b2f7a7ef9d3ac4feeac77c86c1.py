import xml.etree.ElementTree as ET

socket_data = open("test", "r")
data = socket_data.readlines()

def PlayerInfo(xml_element):
  pass

for message in data:
  message_xml = ET.ElementTree(ET.fromstring(message))
  root = message_xml.getroot()

  # TableDetails
  if root.tag == "TableDetails":


  # PlayerInfo
  if root.tag == "PlayerInfo":


  # Message
  if root.tag == "Message":
    d






<PlayerInfo avatar="1-260" casino-bonus="false" level="0" nickname="Muffin33" poker-bonus="false" preferred-wallet="USD" rank="0" remote-casino-bonus="false" remote-club-bonus="false" uuid="fe96dd1a-b5f6-439e-8d49-a143f9e8db01">
