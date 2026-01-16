# -*- coding: utf-8 -*-

import signal

from pyMagician import pyMagician

signal.signal(signal.SIGINT, signal.SIG_DFL)

magician = pyMagician(port='/dev/cu.usbmodem0121')

if magician.connect():
    print('- success connect')

    print('- read_version')
    print(magician.read_version())

    for n in range(3):
        print('-',n,'led')
        magician.led_on(wait=0.5)
        magician.led_off(wait=0.5)

    print('- capture')
    received_size = magician.capture()
    print(received_size)

    print('- read_ir')
    received_data = magician.read_ir()
    if len(received_data) > 10:
        data_str = str(received_data[0])
        for d in received_data[1:10]:
            data_str += ', ' + str(d)
        print('len:',len(received_data),'data: [' + data_str, '...]')
    else:
        print('len:', len(received_data), 'data:', received_data)

    print('- write_ir')
    magician.write_ir([1,2,3,4,5,6,7,8,9,10])
    read_data = magician.read_ir()
    print('check:',read_data)

    print('- send_ir')
    magician.send_ir()

    magician.close()

else:
    print('failed connct')