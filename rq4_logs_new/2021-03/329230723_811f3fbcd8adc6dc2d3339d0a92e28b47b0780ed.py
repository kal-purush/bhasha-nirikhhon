import pygame
import serial
import threading
import time

DIMENSIONS: (int, int) = (800, 600)
BACKGROUND: (int, int, int) = (0, 0, 0)

pygame.init()

screen = pygame.display.set_mode(DIMENSIONS)
screen.fill(BACKGROUND)

active = True

PORT_NAME = "/dev/ttyUSB0"
BAUD_RATE = 9600

ser = serial.Serial(port=PORT_NAME, baudrate=BAUD_RATE)
ser.close()
ser.open()

def read_serial(ser):
    while True: 
        print(ser.read_line())
        time.sleep(0.01)

t = threading.Thread(target=read_serial, args=(ser))
t.start()

def write_msg(msg):
    pass
    ser.write(msg.encode("latin"))

def move_forward(val):
    msg1 = "1\n" + str(val) + "\n"
    msg2 = "3\n" + str(val) + "\n"
    print(msg1, end="")
    print(msg2, end="")
    write_msg(msg1)
    write_msg(msg2)

def move_up(val):
    msg = "2\n" + str(val) + "\n"
    print(msg, end="")
    write_msg(msg)

def right_push(val):
    msg = "3\n" + str(val) + "\n"
    print(msg, end="")
    write_msg(msg)

def left_push(val):
    msg = "1\n" + str(val) + "\n"
    print(msg, end="")
    write_msg(msg)

while active:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            active = False
        if event.type == pygame.KEYDOWN:
            if event.key == pygame.K_w:
                move_forward(250)
            if event.key == pygame.K_s:
                move_forward(-250)
            if event.key == pygame.K_SPACE:
                move_up(300)
            if event.key == pygame.K_LSHIFT:
                move_up(-300)
        if event.type == pygame.KEYUP:
            if event.key == pygame.K_w:
                move_forward(0)
            if event.key == pygame.K_s:
                move_forward(0)
            if event.key == pygame.K_SPACE:
                move_up(0)
            if event.key == pygame.K_LSHIFT:
                move_up(0)

t.join()
ser.close()