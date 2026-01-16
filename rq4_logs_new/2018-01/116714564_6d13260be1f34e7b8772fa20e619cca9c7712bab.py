#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
title          : MonoalphabeticEncryption.py
description    : Get key K as column splitting for given encrypted string
author         : Sebastian Pflaum (240646)
usage          : python MonoalphabeticEncryption.py 'de-/enrypted string' 'modus <decypt or encyrpt>' 'key'
example        : python MonoalphabeticEncryption.py 'ATTACK' 'encrypt' '7'
                 python MonoalphabeticEncryption.py 'HAAHJR' 'decrypt' '7'
python_version :3.6.3
"""

import sys
import platform

def main(edString, modus, key):
    system = platform.system()
    alphabet = {0:'A', 1:'B', 2:'C', 3:'D', 4:'E', 5:'F', 6:'G', 7:'H', 8:'I', 9:'J', 10:'K', 11:'L', 12:'M', 13:'N', 14:'O', 15:'P', 16:'Q', 17:'R', 18:'S', 19:'T', 20:'U', 21:'V', 22:'W', 23:'X', 24:'Y', 25:'Z'}
    
    print('MonoalphabeticEncryption.py running on {0}'.format(system))
    print('--------------------------------------')
    print('Passed string to de-/encrypt (edString): {0}'.format(edString))
    print('Modus (decrypt or encrypt): {0}'.format(modus))
    print('Using given key: {0}'.format(key))
    
    lengthAlp = int(len(alphabet)) - 1
    
    if(modus == "encrypt"):
        print('--------------------------------------')
        print('')
        print('Running encryption...')
        print('')
        print('--------------------------------------')
    
        decString = []
        encString = []
        encStringPlain = ""
        
        for x in edString:
            # Get key for letter
            keyAlp = int(list(alphabet.keys())[list(alphabet.values()).index('{0}'.format(x))])
            
            keyNew = keyAlp + key
            if(keyNew > lengthAlp):
                keyNew = abs(lengthAlp - keyNew) - 1
                
            xNew = alphabet.get(keyNew)
            
            decString.append({keyAlp:x})
            encString.append({keyNew:xNew})
            encStringPlain = encStringPlain + xNew
            
        print('List of decrypted string:')
        print(decString)
        print('')
        print('List of encrypted string:')
        print(encString)
        
        print('')
        print('encString for K={0}: {1}'.format(key,encStringPlain))
        print('--------------------------------------')
    
    elif(modus == "decrypt"):
        print('--------------------------------------')
        print('')
        print('Running decryption...')
        print('')
        print('--------------------------------------')
        
        encString = []
        decString = []
        decStringPlain = ""
        
        for x in edString:
            # Get key for letter
            keyAlp = int(list(alphabet.keys())[list(alphabet.values()).index('{0}'.format(x))])
        
            keyNew = keyAlp - key
            
            if(keyNew < 0):
                keyNew = lengthAlp - abs(keyNew) + 1
            
            xNew = alphabet.get(keyNew)
            
            encString.append({keyAlp:x})
            decString.append({keyNew:xNew})
            decStringPlain = decStringPlain + xNew
            
        print('List of encrypted string:')
        print(encString)
        print('')
        print('List of decrypted string:')
        print(decString)
        
        print('')
        print('decString for K={0}: {1}'.format(key,decStringPlain))
        print('--------------------------------------')
        
    print('')
    print('...finished!')
    
if __name__ == "__main__":
    edString = sys.argv[1]
    modus = sys.argv[2]
    key = int(sys.argv[3])
    main(edString, modus, key)