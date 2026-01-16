import canopen
import time

# construct a CAN network
network = canopen.Network()

# connect to the CAN network
network.connect(bustype='socketcan', channel='vcan0', bitrate=1000000)
#network.connect(bustype='socketcan', channel='can0', bitrate=1000000)

# create a slaver node with id 2(need to match with master.py) and Object Dictionary "Slaver.eds"
slaver_node_66 = network.create_node(66, 'Jetson_exo_66.eds')

# Node send the boot-up message to the CAN network COB-ID:0x700+node-ID detail: "0"
slaver_node_66.nmt.send_command(0)

# With the heartbeat configuration in object Dictionary, use the function by following command every 1s(1000ms)
slaver_node_66.nmt.start_heartbeat(1000)  

# send pdo message
slaver_node_66.rpdo.read()
slaver_node_66.tpdo.read()

print("Test reading PDO to read input of Exo")
try:
    while True:
        timestamp_1 = slaver_node_66.rpdo[1].wait_for_reception(timeout=0.1)
        timestamp_2 = slaver_node_66.rpdo[2].wait_for_reception(timeout=0.1)
        timestamp_3 = slaver_node_66.rpdo[3].wait_for_reception(timeout=0.1)
        timestamp_4 = slaver_node_66.rpdo[4].wait_for_reception(timeout=0.1)
        timestamp_5 = slaver_node_66.rpdo[5].wait_for_reception(timeout=0.1)
        timestamp_6 = slaver_node_66.rpdo[6].wait_for_reception(timeout=0.1)
        timestamp_7 = slaver_node_66.rpdo[7].wait_for_reception(timeout=0.1)
        timestamp_8 = slaver_node_66.rpdo[8].wait_for_reception(timeout=0.1)
        LH_position = slaver_node_66.rpdo[1][0x200F].phys
        LH_velocity = slaver_node_66.rpdo[1][0x2010].phys
        LH_torque = slaver_node_66.rpdo[2][0x2011].phys
        LK_position = slaver_node_66.rpdo[3][0x2012].phys
        LK_velocity = slaver_node_66.rpdo[3][0x2013].phys
        LK_torque = slaver_node_66.rpdo[4][0x2014].phys
        RH_position = slaver_node_66.rpdo[5][0x2015].phys
        RH_velocity = slaver_node_66.rpdo[5][0x2016].phys
        RH_torque = slaver_node_66.rpdo[6][0x2017].phys
        RK_position = slaver_node_66.rpdo[7][0x2018].phys
        RK_velocity = slaver_node_66.rpdo[7][0x2019].phys
        RK_torque = slaver_node_66.rpdo[8][0x201A].phys
        print("LH Actual Position = {}, t={}".format(slaver_node_66.rpdo[1][0x200F].raw, timestamp_1))
        print("LH Actual Velocity = {}, t={}".format(slaver_node_66.rpdo[1][0x2010].raw, timestamp_1))
        print("LH Actual Torque   = {}, t={}".format(slaver_node_66.rpdo[2][0x2011].raw, timestamp_2))
        print("LK Actual Position = {}, t={}".format(slaver_node_66.rpdo[3][0x2012].raw, timestamp_3))
        print("LK Actual Velocity = {}, t={}".format(slaver_node_66.rpdo[3][0x2013].raw, timestamp_3))
        print("LK Actual Torque   = {}, t={}".format(slaver_node_66.rpdo[4][0x2014].raw, timestamp_4))
        print("RH Actual Position = {}, t={}".format(slaver_node_66.rpdo[5][0x2015].raw, timestamp_5))
        print("RH Actual Velocity = {}, t={}".format(slaver_node_66.rpdo[5][0x2016].raw, timestamp_5))
        print("RH Actual Torque   = {}, t={}".format(slaver_node_66.rpdo[6][0x2017].raw, timestamp_6))
        print("RK Actual Position = {}, t={}".format(slaver_node_66.rpdo[7][0x2018].raw, timestamp_7))
        print("RK Actual Velocity = {}, t={}".format(slaver_node_66.rpdo[7][0x2019].raw, timestamp_7))
        print("RK Actual Torque   = {}, t={}".format(slaver_node_66.rpdo[8][0x201A].raw, timestamp_8))

    
except KeyboardInterrupt:
    print("Exit from reading PDO to Jetson")