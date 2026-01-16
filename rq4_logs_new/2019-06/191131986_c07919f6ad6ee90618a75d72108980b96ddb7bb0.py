import smbus
import math
import time

while(True):

    powerm_1 = 0x6b
    powerm_2 = 0x6c

    def read_byte(adr):
        return bus.read_byte_data (address, adr)

    def read_word(adr):
        high = bus.read_byte_data(address, adr)
        low = bus.read_byte_data(address, adr+1)
        val = (high<<8) + low
        return val

    def read_word_2c(adr):
        val = read_word(adr)
        if(val >= 0x8000):
            return -((65535-val) + 1)
        else :
            return val

    def dist(a,b):
        return math.sqrt((a*a)+(b*b))

    def get_y_rotation(x,y,z): 
        radians = math.atan2(x,dist(y,z))
        return -math.degrees(radians)

    def get_x_rotation(x,y,z):
        radians = math.atan2(y,dist(x,z))
        return math.degrees (radians)

    bus = smbus.SMBus(1)
    address = 0x68
    bus.write_byte_data(address,powerm_1,0)

    print("Data of the Gyroscope")
    print("_____________________________________________________")

    gyro_x = read_word_2c(0x43)
    gyro_y = read_word_2c(0x45)
    gyro_z = read_word_2c(0x47)

    print ("gyro_x :", gyro_x,"scaled :",(gyro_x/131))
    print ("gyro_y :", gyro_y,"scaled :",(gyro_y/131))
    print ("gyrp_z :", gyro_z,"scaled :",(gyro_z/131))
    print("\n")

    print ("Accelerometer data")
    print ("____________________________________________________")

    acc_x = read_word_2c(0x3b)
    acc_y = read_word_2c(0x3d)
    acc_z = read_word_2c(0x3f)

    acc_x_scale = acc_x/16384.0
    acc_y_scale = acc_y/16384.0
    acc_z_scale = acc_z/16384.0

    print ("acc_x :", acc_x,"scaled :",acc_x_scale)
    print ("acc_y :", acc_y,"scaled :",acc_y_scale)
    print ("acc_z :", acc_z,"scaled :",acc_z_scale)

    print ("x Rotation" , get_x_rotation(acc_x_scale, acc_y_scale, acc_z_scale))
    print ("y Rotation" , get_x_rotation(acc_x_scale, acc_y_scale, acc_z_scale))
    print ("++++++++++++++++++++++++++++++++++++++++++++++++\n")
    time.sleep(2)