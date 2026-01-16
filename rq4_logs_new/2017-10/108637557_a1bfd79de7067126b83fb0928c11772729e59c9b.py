import machine
import time
a34 = machine.ADC(machine.Pin(34))
a34.atten(machine.ADC.ATTN_11DB)
m5 = machine.PWM(machine.Pin(5))
alow, ahigh = 1500, 1900
import time
for i in range(100000):
    x = a34.read()
    x = x - alow
    x = x/(ahigh-alow)
    if x < 0:
        x = 0
    if x > 1:
        x = 1
    x = 1 - x
    m5.duty(int(x*1023))
    time.sleep(0.05)