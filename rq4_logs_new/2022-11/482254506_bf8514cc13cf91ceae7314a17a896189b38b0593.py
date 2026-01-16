#!/usr/bin/env python3
import binascii

def octal_to_ascii(octal_str):
    '''
    It takes an octal string and return a string
        :octal_str: octal str like "110 145 154"
    '''
    str_converted = ""
    for octal_char in octal_str.split(" "):
        str_converted += chr(int(octal_char,base=8))
    return str_converted

binary_input = input("Enter the  Binary number: ").replace(" ","")
n = int(binary_input,2)
ascii_version = binascii.unhexlify('%x' % n)
data = str(ascii_version, 'UTF-8')
print(f"BINARY: {binary_input} ==> ASCII: {data}")

octal_input = str(input("Enter the Octal number: ")).strip(" ")
data = octal_to_ascii(octal_input)
print(f"Octal: {octal_input} ==> ASCII: {data}")

hax_input = str(input("Enter the Hax number: ")).strip(" ")
data = binascii.unhexlify(hax_input).decode('utf-8')
print(f"Hax: {hax_input} ==> ASCII: {data}")