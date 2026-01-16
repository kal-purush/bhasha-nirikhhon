"this is config pcm5122 dac module"
import math
import wiringpi as wpi
import ctypes

i2s_mod = ctypes.CDLL("./libpycall.so")
reg_name = (
'page'  ,'reg1'  ,'reg2'  ,'reg3'  , 'reg4' ,
'reg5'  ,'reg6'  ,'reg7'  ,'reg8'  , 'reg9' ,
'reg10' ,'reg12' ,'reg13' ,'reg14' , 'reg18',
'reg19' ,'reg20' ,'reg21' ,'reg22' , 'reg23',
'reg24' ,'reg27' ,'reg28' ,'reg29' , 'reg30',
'reg32' ,'reg33' ,'reg34' ,'reg35' , 'reg36',
'reg37' ,'reg40' ,'reg41' ,'reg42' , 'reg43',
'reg44' ,'reg59' ,'reg60' ,'reg61' , 'reg62',
'reg63' ,'reg64' ,'reg65' ,'reg80' , 'reg81',
'reg82' ,'reg83' ,'reg84' ,'reg85' , 'reg86',
'reg87' ,'reg90' ,'reg91' ,'reg92' , 'reg93',
'reg94' ,'reg95' ,'reg108','reg109','reg114',
'reg115','reg118','reg119','reg120','reg121',
'reg122','reg123','reg124','reg125'
)
reg_dict = {
#reg         adress - data          PAGE0                                PAGE1
reg_name[0]: [0x00, 0x00],        #0  PAGE_SELECT
reg_name[1]: [0x01, 0x00 , 0x00], #1  PCM512X_RESET             , PCM512X_OUTPUT_AMPLITUDE
reg_name[2]: [0x02, 0x00 , 0x00], #2  PCM512X_POWER             , PCM512X_ANALOG_GAIN_CTRL
reg_name[3]: [0x03, 0x00],        #3  PCM512X_MUTE
reg_name[4]: [0x04, 0x00],        #4  PCM512X_PLL_EN
reg_name[5]: [0x05, 0X00],        #5  /                         , PCM512X_UNDERVOLTAGE_PORT
reg_name[6]: [0x06, 0x00 , 0x00], #6  PCM512X_SPI_MISO_FUNCTION , PCM512X_ANALOG_MUTE_CTRL
reg_name[7]: [0x07, 0x00 , 0x00], #7  PCM512X_DSP               , PCM512X_ANALOG_GAIN_BOOST
reg_name[8]: [0x08, 0x00 , 0x00], #8  PCM512X_GPIO_EN           , PCM512X_VCOM_CTRL_1
reg_name[9]: [0x09, 0x00 , 0x01], #9  PCM512X_BCLK_LRCLK_CFG    , PCM512X_VCOM_CTRL_2
reg_name[10]:[0x0A, 0x00],        #10 PCM512X_DSP_GPIO_INPUT
reg_name[11]:[0x0C, 0x7c],        #12 PCM512X_MASTER_MODE
reg_name[12]:[0x0D, 0x00],        #13 PCM512X_PLL_REF
reg_name[13]:[0x0E, 0x00],        #14 PCM512X_GPIO_DACIN
reg_name[14]:[0x12, 0x00],        #18 PCM512X_GPIO_PLLIN
reg_name[15]:[0x13, 0x10],        #19 PCM512X_SYNCHRONIZF
reg_name[16]:[0x14, 0x00],        #20 PCM512X_PLL_COEFF_0
reg_name[17]:[0x15, 0x00],        #21 PCM512X_PLL_COEFF_1
reg_name[18]:[0x16, 0x00],        #22 PCM512X_PLL_COEFF_2
reg_name[19]:[0x17, 0x00],        #23 PCM512X_PLL_COEFF_3
reg_name[20]:[0x18, 0x00],        #24 PCM512X_PLL_COEFF_4 
reg_name[21]:[0x1b, 0x00],        #27 PCM512X_DSP_CLKDIV
reg_name[22]:[0x1c, 0x00],        #28 PCM512X_DAC_CLKDIV
reg_name[23]:[0x1d, 0x00],        #29 PCM512X_NCP_CLKDIV
reg_name[24]:[0x1e, 0x00],        #30 PCM512X_OSR_CLKDIV
reg_name[25]:[0x20, 0x00],        #32 PCM512X_MASTER_CLKDIV_1
reg_name[26]:[0x21, 0x00],        #33 PCM512X_MASTER_CLKDIV_2
reg_name[27]:[0x22, 0x00],        #34 PCM512X_FS_SPEED_MODE
reg_name[28]:[0x23, 0x01],        #35 PCM512X_IDAC_1
reg_name[29]:[0x24, 0x00],        #36 PCM512X_IDAC_2
reg_name[30]:[0x25, 0x00],        #37 PCM512X_ERROR_DETECT
reg_name[31]:[0x28, 0x00],        #40 PCM512X_I2S_1
reg_name[32]:[0x29, 0x00],        #41 PCM512X_I2S_2
reg_name[33]:[0x2a, 0x11],        #42 PCM512X_DAC_ROTING
reg_name[34]:[0x2b, 0x01],        #43 PCM512X_DSP_PROGRAM
reg_name[35]:[0x2c, 0x00],        #44 PCM512X_CLKDET
reg_name[36]:[0x3b, 0x00],        #59 PCM512X_AUTO_MUTE
reg_name[37]:[0x3c, 0x00],        #60 PCM512X_DIGITAL_VOLUME_1
reg_name[38]:[0x3d, 0x30],        #61 PCM512X_DIGITAL_VOLUME_2
reg_name[39]:[0x3e, 0x30],        #62 PCM512X_DIGITAL_VOLUME_3
reg_name[40]:[0x3f, 0x22],        #63 PCM512X_DIGITAL_MUTE_1
reg_name[41]:[0x40, 0x00],        #64 PCM512X_DIGITAL_MUTE_2
reg_name[42]:[0x41, 0x07],        #65 PCM512X_DIGITAL_MUTE_3
reg_name[43]:[0x50, 0x00],        #80 PCM512X_GPIO_OUTPUT_1
reg_name[44]:[0x51, 0x00],        #81 PCM512X_GPIO_OUTPUT_2
reg_name[45]:[0x52, 0x00],        #82 PCM512X_GPIO_OUTPUT_3
reg_name[46]:[0x53, 0x00],        #83 PCM512X_GPIO_OUTPUT_4
reg_name[47]:[0x54, 0x00],        #84 PCM512X_GPIO_OUTPUT_5
reg_name[48]:[0x55, 0x00],        #85 PCM512X_GPIO_OUTPUT_6
reg_name[49]:[0x56, 0x00],        #86 PCM512X_GPIO_CONTROL_1
reg_name[50]:[0x57, 0x00],        #87 PCM512X_GPIO_CONTROL_2
reg_name[51]:[0x5a, 0x00],        #90 PCM512X_OVERFLOW
reg_name[52]:[0x5b, 0x00],        #91 PCM512X_RATE_DET_1
reg_name[53]:[0x5c, 0x00],        #92 PCM512X_RATE_DET_2
reg_name[54]:[0x5d, 0x00],        #93 PCM512X_RATE_DET_3
reg_name[55]:[0x5e, 0x00],        #94 PCM512X_RATE_DET_4
reg_name[56]:[0x5f, 0x00],        #95 PCM512X_CLOCK_STATUS
reg_name[57]:[0x6c, 0x00],        #108PCM512X_ANALOG_MUTE_DET
reg_name[58]:[0x6d, 0x00],        #109PCM512X_
reg_name[59]:[0x72, 0x00],        #114PCM512X_
reg_name[60]:[0x73, 0x00],        #115PCM512X_
reg_name[61]:[0x76, 0x00],        #118PCM512X_
reg_name[62]:[0x77, 0x00],        #119PCM512X_GPIN
reg_name[63]:[0x78, 0x00],        #120PCM512X_DIGITAL_MUTE_DET
reg_name[64]:[0x79, 0x00],        #121PCM512X_
reg_name[65]:[0x7a, 0x00],        #122PCM512X_
reg_name[66]:[0x7b, 0x00],        #123PCM512X_
reg_name[67]:[0x7c, 0x00],        #124PCM512X_
reg_name[68]:[0x7d, 0x00]         #125PCM512X_
}
Ch_L = 0
Ch_R = 1
mute = 1
wpi.wiringPiSetup()
fb = wpi.wiringPiI2CSetup(0x4d)

def i2c_write(reg_adress,reg_data):
    wpi.wiringPiI2CWriteReg8(fb,reg_adress,reg_data)

def set_clock_ref():
    'set clock tree'
    #select PLLCKIN source(BCK 2.8224M)
    i2c_write(0x0d,1<<4)

    #generate PLLCK = PLLCKIN * J.D * R/P = 90.3168MHz
    #J = 16
    i2c_write(21,16)
    #D = 0
    i2c_write(22,0)
    i2c_write(23,0)
    #R = 2
    i2c_write(24,2-1)
    #P = 1
    i2c_write(20,1-1)
   
    #enable PLL(select master clk is PLLCK)
    i2c_write(0x04,0x01)

    #generate DSPCK = PLLCK / div = 45.1584MHz
    #div(DDSP) = 2
    i2c_write(0x1b,2-1)

    #select DACCLK source(PLLCK)
    i2c_write(0x0e,1<<4)

    #generate DACCK = DACCLK / div = 5644.8KHz
    #div(DDAC) = 16
    i2c_write(0x1c,16-1)

    #generate CPCK = DACCK / div = 1411.2KHz
    #div(DNCP) = 4
    i2c_write(0x1d,4-1)

    #generate OSRCK = DACCK / div = 705.1KHz
    #div(DOSR) = 8
    i2c_write(0x1e,8-1)
#    i2c_write(0x22,)
    print set_clock_ref.__doc__

def init_dac():
    'init pcm5122 reg'
    i2c_write(0x00,0x00)
    #set i2s format and word length is 32bit
    i2c_write(0x28,0x03)
    #set clock ref
    set_clock_ref()    

def set_volume(Ch,volume):
    'set channel volume'
    if Ch == Ch_L:
#        i2c_write(0x3d,volume)
        print 'xixi'
    else:
        print 'haha'
#        i2c_write(0x3e,volume)

def set_mute(Ch,state):
    'set channel mute'
    if Ch == Ch_L:
        if state == mute:
            print 'channel left is mute'
        else:
            print 'channel left not mute'
    else:
        if state == mute:
            print 'channel right is mute'
        else:
            print 'channel right not mute'
    print set_mute.__doc__
def print_reg():
    'show reg data'
    for i in range(len(reg_name)):
        print reg_name[i],reg_dict[reg_name[i]]
    print len(reg_dict)
if __name__ == '__main__':
#if this module is main
    print __doc__
#    init_dac()
#    print '%.2f'%math.sin(3.14/2)
    i2s_mod.main()
    print 'dac config complete'