# for x in range(1, 11):
#     print(repr(x).center(2), repr(x * x).center(3), end=' ')
#     # Note use of 'end' on previous line
#     print(repr(x * x * x).center(4))

# print('This {food} is {adjective}.'.format(
#     food = 'spam', adjective = 'absolutely horrible'
# ))

# import math
# print('The value of PO is approximately %.3f.' % math.pi)

# JSON(JavaScript Object Notation)
import json

# print(json.dumps(['foo', {'bar': ('baz', None, 1.0, 2)}]))

# Pretty format printing
# print(json.dumps({'4': 5,'6': 7}, sort_keys=True, indent=4))

# Decoding JSON
# print(json.loads('["foo", {"bar":["baz", null, 1.0, 2]}]'))

# from io import StringIO
# io = StringIO('["streaming API"]')
# print(json.load(io))

def as_complex(dct):
    if '__complex__' in dct:
        return complex(dct['real'], dct['imag'])
    return dct

json.loads('{"__complex__": true, "real": 1, "imag": 2}',
           object_hook=as_complex)