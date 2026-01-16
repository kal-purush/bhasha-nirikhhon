#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
title          : CaesarEncryption.py
description    : Default description
author         : Sebastian Pflaum (240646)
usage          : python CaesarEncryption.py
python_version :3.6.3
"""

import sys
import platform

def main(encString):
    system = platform.system()
    alphabet = {0:'A', 1:'B', 2:'C', 3:'D', 4:'E', 5:'F', 6:'G', 7:'H', 8:'I', 9:'J', 10:'K', 11:'L', 12:'M', 13:'N', 14:'O', 15:'P', 16:'Q', 17:'R', 18:'S', 19:'T', 20:'U', 21:'V', 22:'W', 23:'X', 24:'Y', 25:'Z', 26:' '}
    
    print('CaesarEncryption.py running on {0}'.format(system))
    print('--------------------------------------')
    print('Passed string to decrypt (encString): {0}'.format(encString))
    
    encString = encString.upper()
    
    print('Changed string to uppercase: {0}'.format(encString))
    print('--------------------------------------')
    print('')
    print('Running decryption...')
    print('')
    print('--------------------------------------')
    lengthAlp = int(len(alphabet)) - 1
    
    for i in range(0, lengthAlp):
        decString = ""
        
        print('encString for K={0}: {1}'.format(i,encString))
        
        for x in encString:
            # Get key for letter
            key = int(list(alphabet.keys())[list(alphabet.values()).index('{0}'.format(x))])
            
            if(key == 26):
                decString = decString + " " 
                continue
            else:
                keyNew = key - i
                
                if(keyNew < 0):
                    keyNew = lengthAlp - abs(keyNew)
   
                oldValue = alphabet.get(key)
                newValue = alphabet.get(keyNew)
                
                #print('{0}:{1} > {2}:{3}'.format(key,oldValue,keyNew,newValue))
                decString = decString + newValue
                
        print('decString for K={0}: {1}'.format(i,decString))
        print('--------------------------------------')
    
    print('')
    print('...finished!')
    
if __name__ == "__main__":
    encString = sys.argv[1]
    main(encString)