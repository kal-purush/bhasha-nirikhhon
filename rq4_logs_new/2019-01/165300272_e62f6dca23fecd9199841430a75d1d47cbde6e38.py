# test Coms made for Calvin Mars Rover 2018-19
# by Jason Pruim and Joel Muyskens based on digi.xBee github

from digi.xbee.devices import XBeeDevice


def test_receive1(my_port, my_baud):
    # Start Test
    print("Start Receive Test 1 (looping)")
    device = XBeeDevice(my_port, my_baud)
    try:
        device.open()

        def data_receive_callback(xbee_message):
            print("From %s >> %s" % (xbee_message.remote_device.get_64bit_addr(),
                                     xbee_message.data.decode()))
        device.add_data_received_callback(data_receive_callback)

        print("Waiting for data...\n")
        input()

    finally:
        if device is not None and device.is_open():
            device.close()
            print("Receive Test 1 Complete")


if __name__ == '__main__':
    # values to be changed on different computers
    port = "COM3"
    baud = 19200
    test_receive1(port, baud)