#! /ux/Tools/python2.7.10/bin/python2.7
import PIL
from PIL import Image
import sys
from compiler.ast import flatten
from itertools import *

def box_gen(size, color):
    box_block = []
    for i in range(size):
        box_line  = []
        for j in range(size):
            box_line.append(color)
        box_block.append(box_line)
    return box_block

def list2chr(dataIn):
    tmp = map(chr, dataIn)
    return ''.join(tmp)

def display(dataIn, Hsize, Vsize, colorMode='RGB'):
    im = Image.frombytes(colorMode, (Hsize, Vsize), dataIn)
    im.show()


def test1():
    box_pic = box_gen(200, (0,0,0))
    box_arr = [k for i in box_pic for j in i for k in j]
    box_str = list2chr(box_arr)
    display(box_str, 200, 200)

def test2(Hsize=100, Vsize=100, BoxSize=10):
    # each box has 10x10 pixels
    black_box = box_gen(BoxSize, (0,0,0))
    white_box = box_gen(BoxSize, (255,255,255))
    # each line has 100 boxes
    for i in range(Hsize):
        if i == 0:
            block_line = black_box
        elif i%2 == 1:
            block_line = zip(block_line, white_box)
        else:
            block_line = zip(block_line, black_box)
    block_line = flatten(block_line)

    block_line_reverse = []
    for i in block_line:
        if i == 0:
            block_line_reverse.append(255)
        else:
            block_line_reverse.append(0)

    block = []
    for i in range(Vsize):
        if i%2 == 1:
            block.append(block_line)
        else:
            block.append(block_line_reverse)
    block = flatten(block)

    # each row has 100 boxes
    block_str = list2chr(block)
    display(block_str, Hsize*BoxSize,Vsize*BoxSize)

def test3(Hsize=64, Vsize=64, BoxSize=10):    
    colorState = product(range(0,255,16), range(0,255,16), range(0,255,16))

    block = []
    for i, colorSpace in enumerate(colorState):
        tmp = box_gen(BoxSize, colorSpace)
        if i%64 ==0:
            block_line = tmp
        else:
            block_line = zip(block, tmp)
        if i%64 == 63:
            block.append(block_line)
            
    for i,j in enumerate(block):
        block[i] = flatten(block[i])

    block = flatten(block)

    for i,j in enumerate(block):
        if j > 127:
            block[i] = 255
        else:
            block[i] = 0

    block_str = list2chr(block)
    display(block_str, 640, 640)
