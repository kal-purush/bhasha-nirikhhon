#!/usr/bin/env python3

import math
from docopt import docopt

args = docopt(__doc__)

sampleBits = int(args['<sampleBits>'])
waveBits = int(args['<waveBits>'])

samples = 2 ** sampleBits
amplitude = 2 ** (waveBits - 1)

print('-- Signals')
print('signal table_counter : std_logic_vector(' + str(sampleBits - 1) +
      ' downto 0) := "' + '0' * sampleBits + '";')
print('signal table_value : std_logic_vector(' + str(waveBits - 1) +
      ' downto 0) := "' + '0' * waveBits + '";\n')

print('-- Table')
print('table_value <=  ', end='')
for i in range(samples):
    temp = int(amplitude + (amplitude - 1) *
                     math.sin(2 * math.pi * i / samples))
    print('"' + bin(temp)[2:].zfill(waveBits) +
          '" when table_counter =', i, end='') 
    if i == samples - 1:
        print(";")
    else:
        print(" else")