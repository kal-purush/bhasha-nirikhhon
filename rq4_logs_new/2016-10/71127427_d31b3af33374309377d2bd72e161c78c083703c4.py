#!/usr/bin/env python
from ciscoconfparse import CiscoConfParse


crypto_map = 'crypto map CRYPTO'
file = 'cisco_ipsec.txt'

conf_file = CiscoConfParse(file)

crypto = conf_file.find_objects(crypto_map)

child_group = conf_file.find_objects_wo_child(
    parentspec=crypto_map,
    childspec=r"set transform-set AES-SHA")


for c in child_group:
    print(c.text)
    for child in c.children:
        print(child.text)
