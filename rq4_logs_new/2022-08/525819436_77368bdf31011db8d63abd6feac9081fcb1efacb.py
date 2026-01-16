# from typing import final
import numpy as np
import math
from math import *
# rotate_arr = np.array(
#     [
#         [1, 0, 0],
#         [0, 0, -1],
#         [0, 1, 0]
#     ]
# )
x, y, z = input("x, y, z 회전 할 각도를 입력하세요 : ").split()

z = math.radians(int(z))
y = math.radians(int(y))
x = math.radians(int(x))


rotate_arr = np.array(
    [
        [cos(z) * cos(y), cos(z) * sin(y) * sin(x) - sin(z) * cos(x), cos(z)*sin(y)*cos(x) + sin(z) * sin(x)],
        [sin(z) * cos(y), sin(z)*sin(y)*sin(x) + cos(z) * cos(x), sin(z)*sin(y)*cos(x) - cos(z) * sin(x)],
        [-1*sin(y), cos(y) * sin(x), cos(y) * cos(x)]
    ]
)
print(rotate_arr)
file = open('test.obj')
to_file = open('to.obj', 'w')
line = file.readline()
count = 0
while line :
    if count == 0:
        to_file.write(line)

    line = file.readline()

    if 'v ' in line:
        split_line = line.split('v ')[1]
        if ' ' in split_line:
            x_str = split_line.split(' ')[0]
            y_str = split_line.split(' ')[1]
            z_str = split_line.split(' ')[2]
            x_num = int(x_str)
            y_num = int(y_str)
            z_num = int(z_str)

            before_array = np.array(
            [
                [x_num, y_num, z_num]
            ]
            )
            final_array = np.dot(before_array, rotate_arr)

            final_str = str(final_array)
            final_str = final_str.replace('[','')
            final_str = final_str.replace(']','')

            write_final_str = 'v ' + final_str + '\n'
            write_final_str = write_final_str.replace('  ',' ')
            print(write_final_str)
            to_file.write(write_final_str)
    else:
        to_file.write(line)
    count = count+1