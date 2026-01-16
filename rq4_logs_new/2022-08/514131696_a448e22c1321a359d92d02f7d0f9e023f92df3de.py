import canopen
import time
import INetwork.py

class CANNetwork(Network):
    def __init__(self):
        self.network = None
        self.node = None
    
    def Setup(self):
    ## Setup the connection object
    # construct a CAN network
        self.network = canopen.Network()

        # connect to the CAN network
        self.network.connect(bustype='socketcan', channel='vcan0', bitrate=1000000)
        #self.network.connect(bustype='socketcan', channel='can0', bitrate=1000000)

        # create a slaver node with id 2(need to match with master.py) and Object Dictionary "Slaver.eds"
        self.node = network.create_node(66, 'Jetson_exo_66.eds')

        # Node send the boot-up message to the CAN network COB-ID:0x700+node-ID detail: "0"
        self.node.nmt.send_command(0)

        # With the heartbeat configuration in object Dictionary, use the function by following command every 1s(1000ms)
        self.node.nmt.start_heartbeat(1000)  

        # send pdo message
        self.node.rpdo.read()
        self.node.tpdo.read()

    def Update(self): 
    ## Polling goes here 
        try:
            timestamp_1 = self.node.rpdo[1].wait_for_reception(timeout=0.1)
            timestamp_2 = self.node.rpdo[2].wait_for_reception(timeout=0.1)
            timestamp_3 = self.node.rpdo[3].wait_for_reception(timeout=0.1)
            timestamp_4 = self.node.rpdo[4].wait_for_reception(timeout=0.1)
            timestamp_5 = self.node.rpdo[5].wait_for_reception(timeout=0.1)
            timestamp_6 = self.node.rpdo[6].wait_for_reception(timeout=0.1)
            timestamp_7 = self.node.rpdo[7].wait_for_reception(timeout=0.1)
            timestamp_8 = self.node.rpdo[8].wait_for_reception(timeout=0.1)
            LH_position = self.node.rpdo[1][0x200F].phys
            LH_velocity = self.node.rpdo[1][0x2010].phys
            LH_torque = self.node.rpdo[2][0x2011].phys
            LK_position = self.node.rpdo[3][0x2012].phys
            LK_velocity = self.node.rpdo[3][0x2013].phys
            LK_torque = self.node.rpdo[4][0x2014].phys
            RH_position = self.node.rpdo[5][0x2015].phys
            RH_velocity = self.node.rpdo[5][0x2016].phys
            RH_torque = self.node.rpdo[6][0x2017].phys
            RK_position = self.node.rpdo[7][0x2018].phys
            RK_velocity = self.node.rpdo[7][0x2019].phys
            RK_torque = self.node.rpdo[8][0x201A].phys
            print("LH Actual Position = {}, t={}".format(self.node.rpdo[1][0x200F].raw, timestamp_1))
            print("LH Actual Velocity = {}, t={}".format(self.node.rpdo[1][0x2010].raw, timestamp_1))
            print("LH Actual Torque   = {}, t={}".format(self.node.rpdo[2][0x2011].raw, timestamp_2))
            print("LK Actual Position = {}, t={}".format(self.node.rpdo[3][0x2012].raw, timestamp_3))
            print("LK Actual Velocity = {}, t={}".format(self.node.rpdo[3][0x2013].raw, timestamp_3))
            print("LK Actual Torque   = {}, t={}".format(self.node.rpdo[4][0x2014].raw, timestamp_4))
            print("RH Actual Position = {}, t={}".format(self.node.rpdo[5][0x2015].raw, timestamp_5))
            print("RH Actual Velocity = {}, t={}".format(self.node.rpdo[5][0x2016].raw, timestamp_5))
            print("RH Actual Torque   = {}, t={}".format(self.node.rpdo[6][0x2017].raw, timestamp_6))
            print("RK Actual Position = {}, t={}".format(self.node.rpdo[7][0x2018].raw, timestamp_7))
            print("RK Actual Velocity = {}, t={}".format(self.node.rpdo[7][0x2019].raw, timestamp_7))
            print("RK Actual Torque   = {}, t={}".format(self.node.rpdo[8][0x201A].raw, timestamp_8))

        
        except KeyboardInterrupt:
            print("Exit from reading PDO to Jetson")

    def SetupHardware(self):
    ## For setting up hardware (might not be in use
        pass




print("Test reading PDO to read input of Exo")