import sys
import time
from typing import Tuple

import smbus
import RPi.GPIO as GPIO

from pid import PID


class EditingWheel:
    I2C_AS5600_ADDRESS = 0x36

    STATUS_ERROR_I2C_WRITE = 1
    STATUS_ERROR_I2C_READ = 2
    STATUS_ERROR_MOTOR_OVERHEAT = 4
    STATUS_ERROR_MAGNET_HIGH = 8
    STATUS_ERROR_MAGNET_LOW = 16
    STATUS_ERROR_MAGNET_NOT_DETECTED = 32
    STATUS_ERROR_RX_FAILED = 64
    STATUS_ERROR_TX_FAILED = 128

    PWM_PIN = 17
    T1_PIN = 22
    T2_PIN = 27

    def __init__(self,
                 dividers: int = 48,
                 dead_band: float = 2,
                 gain_factor: float = 1.0,
                 expo: float = 1.0,
                 direction: int = 1,
                 zero: float = 0.0):
        self._i2c_bus = smbus.SMBus(1)

        GPIO.setwarnings(False)
        GPIO.setmode(GPIO.BCM)

        GPIO.setup(EditingWheel.PWM_PIN, GPIO.OUT)
        GPIO.setup(EditingWheel.T1_PIN, GPIO.OUT)
        GPIO.setup(EditingWheel.T2_PIN, GPIO.OUT)

        GPIO.output(EditingWheel.PWM_PIN, GPIO.HIGH)
        GPIO.output(EditingWheel.T1_PIN, GPIO.HIGH)
        GPIO.output(EditingWheel.T2_PIN, GPIO.HIGH)

        self._pwm = GPIO.PWM(17, 5000)
        self._pwm.start(0)

        self.direction = direction
        self.zero = zero

        self.expo = expo

        # self._pid = PID(kp=0.7, ki=0.29, kd=0.01, gain=1.0, dead_band=2)
        # self._pid = PID(kp=0.7, ki=0.4, kd=0, gain=5.0, dead_band=2)
        gain = gain_factor * dividers / 1.25
        self._pid = PID(kp=0.7, ki=0, kd=0.01, gain=gain, dead_band=dead_band)

        self.dividers = dividers

        self.angle = 0

        self._desied_angle = 0

        self._angle_of_retch = 360 // self.dividers
        self._half_angle = self._angle_of_retch / 2
        self._half_distance_tension_factor = 100 / self._half_angle

        self._stop = False
        self._distance = 0.0
        self._tension = 0.0

        self._last_angle = 0.0
        self._last_status = 0

    def read_angle(self) -> Tuple[float, int]:
        try:
            pos = self._i2c_bus.read_i2c_block_data(EditingWheel.I2C_AS5600_ADDRESS, 0x0B, 5)
            angle = (pos[3] * 256 + pos[4]) * 360 // 4096
            status = pos[0] & 0b00111000 | EditingWheel.STATUS_ERROR_MAGNET_NOT_DETECTED

            angle += self.zero
            angle = angle if 0 <= angle < 360 else angle - 360

            self._last_angle = angle
            self._last_status = status
        except Exception as _:
            angle = self._last_angle
            status = self._last_status
            time.sleep(0.01)

        return angle, status

    def run_cycle(self) -> None:
        self.angle, _ = self.read_angle()

        # self._dist = (a % self._angle_of_retch) - self._half_angle
        # abs_dist = self._dist if self._dist > 0 else -self._dist

        self._desied_angle = (self.angle // self._angle_of_retch) * self._angle_of_retch + self._half_angle

        error = self.angle_difference(self._desied_angle, self.angle)

        error = self.apply_expo(error / self._angle_of_retch, self.expo) * self._angle_of_retch

        self._tension = self._pid.process(error)

        # if self._tension < 0:
        #     self._tension = max(-1.0, self._tension) * 100
        # else:
        #     self._tension = min(1.0, self._tension) * 100

        if self._tension < 0:
            self._tension = max(-100.0, self._tension)
        else:
            self._tension = min(100.0, self._tension)

        # self._tension = int(min(abs_dist, self._half_angle) * self._half_distance_tension_factor)
        # self._tension = 0 if self._tension < 20 else self._tension

        self._pwm.ChangeDutyCycle(self._tension if self._tension >= 0 else -self._tension)

        if self._tension * self.direction >= 0:
            GPIO.output(EditingWheel.T1_PIN, GPIO.HIGH)
            GPIO.output(EditingWheel.T2_PIN, GPIO.LOW)
        elif self._tension < 0:
            GPIO.output(EditingWheel.T1_PIN, GPIO.LOW)
            GPIO.output(EditingWheel.T2_PIN, GPIO.HIGH)
        else:
            GPIO.output(EditingWheel.T1_PIN, GPIO.HIGH)
            GPIO.output(EditingWheel.T2_PIN, GPIO.HIGH)

    def run(self, time_to_run: float) -> None:
        self._stop = False
        report_at = 0

        started_at = time.time()
        now = time.time()
        readings = 0
        while not self._stop and (now < started_at + time_to_run or time_to_run == 0):
            now = time.time()
            self.run_cycle()
            readings += 1

            if report_at <= now:
                report_at = now + 1
                print(f"a={self.angle:3.0f}, desied_angle={self._desied_angle}, tension={self._tension}; readings={readings}")
                readings = 0

            time.sleep(0.001)

    def stop_all(self) -> None:
        self._stop = True
        GPIO.output(EditingWheel.PWM_PIN, GPIO.HIGH, GPIO.HIGH)
        GPIO.output(EditingWheel.T1_PIN, GPIO.HIGH, GPIO.HIGH)
        GPIO.output(EditingWheel.T1_PIN, GPIO.HIGH)

    @staticmethod
    def angle_difference(a1: float, a2: float) -> float:
        diff = a1 - a2
        if diff >= 180:
            return diff - 360
        elif diff <= -180:
            return diff + 360
        return diff

    @staticmethod
    def apply_expo(value: float, expo_percentage):
        if value >= 0:
            return value * value * expo_percentage + value * (1.0 - expo_percentage)
        else:
            return - value * value * expo_percentage + value * (1.0 - expo_percentage)


if __name__ == '__main__':
    args = sys.argv
    time_to_run = int(args[1]) if len(args) > 1 else 20

    selected_option = int(args[2]) if len(args) > 2 else 1

    zero = 225
    direction = 1

    options = [
        lambda: EditingWheel(direction=direction, zero=zero, dividers=1, gain_factor=2, expo=0.8),
        lambda: EditingWheel(direction=direction, zero=zero, dividers=1, gain_factor=2, expo=-0.8),
        lambda: EditingWheel(direction=direction, zero=zero, dividers=2),
        lambda: EditingWheel(direction=direction, zero=zero, dividers=48, dead_band=1),
        lambda: EditingWheel(direction=direction, zero=zero, dividers=48, dead_band=1.5, gain_factor=0.6),
        lambda: EditingWheel(direction=direction, zero=zero, dividers=32, dead_band=1.8, gain_factor=1),
        lambda: EditingWheel(direction=direction, zero=zero, dividers=24, dead_band=0.4, gain_factor=1, expo=0.9),
        lambda: EditingWheel(direction=direction, zero=zero, dividers=24, dead_band=1.8, gain_factor=1, expo=-0.8),
        lambda: EditingWheel(direction=direction, zero=zero, dividers=12, dead_band=0.4, gain_factor=1, expo=0.9),
        lambda: EditingWheel(direction=direction, zero=zero, dividers=12, dead_band=1.8, gain_factor=1, expo=-0.8),
    ]

    editing_wheel = options[selected_option]()
    print("Started")

    editing_wheel.run(time_to_run)

    print("Finished")