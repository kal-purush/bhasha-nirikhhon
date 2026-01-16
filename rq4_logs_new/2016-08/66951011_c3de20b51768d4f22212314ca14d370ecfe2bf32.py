import pyaudio
import array
import audioop
import RPi.GPIO as GPIO
from struct import unpack
from colorsys import hsv_to_rgb
from time import time
import numpy as np

CHUNK = 1024
FORMAT = pyaudio.paInt16
CHANNELS = 1
RATE = 44100
RECORD_SECONDS = 20

class Letter:
  def __init__(self, rpin, gpin, bpin):
    self.rpin = rpin
    self.gpin = gpin
    self.bpin = bpin
    GPIO.setup(self.rpin, GPIO.OUT)
    GPIO.setup(self.gpin, GPIO.OUT)
    GPIO.setup(self.bpin, GPIO.OUT)
    self.rpwm = GPIO.PWM(self.rpin, 50)
    self.gpwm = GPIO.PWM(self.gpin, 50)
    self.bpwm = GPIO.PWM(self.bpin, 50)
    self.rpwm.start(0)
    self.gpwm.start(0)
    self.bpwm.start(0)

  def rgb(self, r, g, b):
    self.rpwm.ChangeDutyCycle((1.0 - r) * 100.0)
    self.gpwm.ChangeDutyCycle((1.0 - g) * 100.0)
    self.bpwm.ChangeDutyCycle((1.0 - b) * 100.0)

  def hsv(self, h, s, v):
    h %= 1
    self.rgb(*hsv_to_rgb(h, s, v))

GPIO.setmode(GPIO.BCM)
z = Letter(24, 25, 23)
b = Letter(20, 21, 16)
t = Letter(19, 26, 13)

"""
def calculate_levels(data, chunk, sample_rate):
  # convert raw data to nmpy array
  data = unpack("%dh" % (len(data)/2), data)
  data = np.array(data, dtype='h')
  # apply FFT - real data so rfft
  fourier = np.fft.rfft(data)
  # remove last element in array to make it same size as chunk
  fourier = np.delete(fourier, len(fourier) - 1)
  # find amplitude
  power = np.log10(np.abs(fourier)) ** 2
  # arrange array into 3 rows
  power = np.reshape(power, (8, chunk/16))
  matrix = np.average(power, axis=1)
  return matrix

p = pyaudio.PyAudio()

stream = p.open(format=FORMAT, channels=CHANNELS, rate=RATE, input=True, frames_per_buffer=CHUNK)
"""

amplitudes = []

start = time()
while True:
  try:
    
    data = stream.read(CHUNK)
    stream.stop_stream()
"""
    matrix = calculate_levels(data, CHUNK, RATE)
    rms = audioop.rms(data, 2) / 3500.0
    rms = min(1.0, rms)
    amplitudes.append(matrix[3])
    matrix = [ (x / 32.0) % 1 for x in matrix ]
    dt = time() - start
    hz = (dt / 5.0) % 1
    hb = (dt / 5.0 + 0.3) % 1
    ht = (dt / 5.0 + 0.6) % 1
    avg = sum(matrix) / len(matrix)
    wavg = matrix[1] * 0.3 + matrix[2] * 0.3 + matrix[3] * 0.1 + matrix[4] * 0.05 + matrix[5] * 0.05 + matrix[6] * 0.05
    z.hsv(hz + matrix[1], 1.0, rms)
    b.hsv(hz + matrix[3], 1.0, rms)
    t.hsv(hz + matrix[6], 1.0, rms)
    """

    z.hsv(0, 100, 100)
    b.hsv(0, 100, 100)
    t.hsv(0, 100, 100)

    stream.start_stream()

  except KeyboardInterrupt:
    break

stream.stop_stream()
stream.close()
"""

p.terminate()

print
print 'Min:', min(amplitudes)
print 'Max:', max(amplitudes)
print 'Avg:', sum(amplitudes) / len(amplitudes)
"""

print "Exit"