from gpiozero import PWMLED, LED
from time import sleep


class Motor:
    def __init__(self, en, in1, in2):
        self.enable = PWMLED(en)
        self.in1 = LED(in1)
        self.in2 = LED(in2)

    def move_cw(self):
        self.in1.on()
        self.in2.off()

    def move_ccw(self):
        self.in1.off()
        self.in2.on()

    def speed(self, v):
        self.enable.value = v


left = Motor(6, 13, 12)
right = Motor(26, 20, 21)
out_pin = [LED(18)]


def move(spd, dirs):
    speed_det = 0.5
    spd = spd * speed_det
    if spd > 0:
        left.move_cw()
        right.move_ccw()
    else:
        left.move_ccw()
        right.move_cw()
        spd = -spd
    if dirs > 0:
        left.speed(spd)
        right.speed(spd * (1 - dirs))
    else:
        right.speed(spd)
        left.speed(spd * (1 + dirs))


def io(i, std):
    if i < len(out_pin):
        if std == 'high':
            out_pin[i].on()
        elif std == 'low':
            out_pin[i].off()