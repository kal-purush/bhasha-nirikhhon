from LED import *
import time
    
def logic(tmp):
    if tmp <= 36:
        print("Error: Suhu terlalu rendah")
    elif tmp > 36 and tmp <= 37.5:
        print("Normal")
        LEDGreen_On()
    elif tmp > 37.5 and tmp <= 38.5:
        print("Sakit ringan")
        LEDRed_On()
    else:
        print('Sakit parah')
        for k in range(1,11):
            LEDRed_On()
            time.sleep(1)
            LEDRed_Off()
            time.sleep(1)


time.sleep(1)

settem = float(input('Masukkan temperature: '))
logic(settem)

time.sleep(5)

LEDRed_Off()
LEDGreen_Off()