#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
title          : TranspositionEncryption.py
description    : Get key K as column splitting for given encrypted string
author         : Simon Ludwig (initial version); Modified by Patrick Thoma; Modified by Sebastian Pflaum (240646)
usage          : python TranspositionEncryption.py 'encrypted string'
example        : python TranspositionEncryption.py 'DtBeelaia elrtntsei  esi iseSnpoinpiptons inars!'
python_version :3.6.3
"""

import sys
import platform

def transposition(encString, columns):

    decString = "" 
    i = 0
    x = len(encString)
    
    for j in range (0, columns):
        i = j
        while i < x:
            decString += encString[i]
            i += columns;
        
    return decString

def main(encString):
    system = platform.system()
    
    print('TranspositionEncryption.py running on {0}'.format(system))
    print('--------------------------------------')
    print('Passed string to decrypt (encString): {0}'.format(encString))
    
    lengthEnc = len(encString)
    
    print('Length of encrypted string: {0}'.format(lengthEnc))
    print('--------------------------------------')
    print('')
    print('Running decryption...')
    print('')
    print('--------------------------------------')
    
    columns = 1
    for i in range (1, lengthEnc):
    
        while columns != lengthEnc:
            decString = transposition(encString, columns)
            
            print('decString for K={0}: {1}'.format(columns,decString))
            print('--------------------------------------')
            
            columns = columns + 1
            i = i + 1;
  
    print('')
    print('...finished!')      

if __name__ == "__main__":
    encString = sys.argv[1]
    main(encString)