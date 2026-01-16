#!/usr/bin/env python3

from PyQt5.QtCore import *
from PyQt5.QtSerialPort import *


class Terminal:
    def __init__(self, serial_port):
        assert(isinstance(serial_port, QSerialPort))
        self.port = serial_port
        self.port.readyRead.connect(self.read_serial)
        self.set_style('normal')
        self.clear_screen()
        self.set_cursor_pos(1, 1)

    def read_serial(self):
        data = self.port.read(self.port.bytesAvailable())
        self.port.write(data)

    def clear_screen(self):
        sequence = bytearray()
        sequence.append(27)
        sequence += b'[2J'          # Clear Screen
        self.port.write(sequence)

    def set_cursor_pos(self, x, y):
        sequence = bytearray()
        sequence.append(27)
        sequence += b'['
        sequence += self.int_to_bytearray(y)
        sequence += b';'
        sequence += self.int_to_bytearray(x)
        sequence += b'H'
        self.port.write(sequence)

    def get_cursor_pos(self):
        sequence = bytearray()
        sequence.append(27)
        sequence += b'[6n'
        self.port.write(sequence)
        self.port.readyr

    def write_big(self, x, y, text):
        sequence = bytearray()
        self.set_cursor_pos(x, y)
        sequence.append(27)
        sequence += b'#3'           # Top half of double height font
        sequence += text
        self.port.write(sequence)
        self.set_cursor_pos(x, y + 1)
        sequence.clear()
        sequence.append(27)
        sequence += b'#4'           # Bottom half of double height font
        sequence += text
        self.port.write(sequence)

    def write(self, text):
        self.port.write(text)

    def set_style(self, style):
        sequence = bytearray()
        sequence.append(27)
        sequence += b'['
        if style == 'bold':
            sequence += b'1'
        elif style == 'underscore':
            sequence += b'4'
        elif style == 'blink':
            sequence += b'5'
        elif style == 'invert':
            sequence += b'7'
        elif style == 'normal':
            sequence += b'0'
        else:
            return
        sequence += b'm'
        self.port.write(sequence)

    def int_to_bytearray(self, number):
        assert isinstance(number, int)
        string = str(number)
        text = bytearray()
        for character in string:
            text.append(ord(character))
        return text