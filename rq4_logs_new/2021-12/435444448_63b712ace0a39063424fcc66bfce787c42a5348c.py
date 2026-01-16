import PySimpleGUI as sg
from random import randint
from PIL import Image
import numpy as np
import matplotlib.pyplot as plt

MAX_ROWS = 20
MAX_COL = 50
box_size = 1
# board = [[randint(0, 1) for j in range(MAX_COL)] for i in range(MAX_ROWS)]
img_list = np.zeros((MAX_ROWS, MAX_COL), dtype=np.uint8)
layout = [[[
    sg.Button('',
              size=(box_size, box_size),
              button_color=('white'),
              key=(i, j),
              pad=(0, 0)) for j in range(MAX_COL)
] for i in range(MAX_ROWS)], [sg.Button('変換', key='check', expand_x=True)]]

window = sg.Window('Minesweeper', layout)

b_c = 'white'
while True:
    event, values = window.read()
    if event in (sg.WIN_CLOSED, 'Exit'):
        break
    # window[(row, col)].update('New text')   # To change a button's text, use this pattern
    # For this example, change the text of the button to the board's value and turn color black

    if event == 'check':
        for i in range(MAX_ROWS):
            for j in range(MAX_COL):
                if img_list[i][j] == 0:
                    img_list[i][j] = 255
                else:
                    img_list[i][j] = 0
        print(img_list)
        # plt.imshow(img_list,
        #            cmap='gray',
        #            vmin=0,
        #            vmax=255,
        #            interpolation='none')
        # plt.show()
        plt.imsave('C:\\Users\\Atsushi\\Pictures\\map\\buf.png',
                   img_list)  #拡張子を.pngとかに変えてもちゃんと保存してくれる。

        im_gray = np.array(
            Image.open('C:\\Users\\Atsushi\\Pictures\\map\\buf.png').convert(
                'L'))
        pil_img_gray = Image.fromarray(im_gray)
        pil_img_gray.save('C:\\Users\\Atsushi\\Pictures\\map\\map.pgm')

        # img_list = np.array(save_list)
        # print("OK")
        # pil_img_gray = Image.fromarray(img_list)
        # print(pil_img_gray.mode)
        # # L
        # pil_img_gray.save('C:\\Users\\Atsushi\\Pictures\\map\\a.pgm')
        break

    position = list(event)
    print(position)
    if img_list[position[0]][position[1]] == 1:
        b_c = 'white'
        img_list[position[0]][position[1]] = 0
    else:
        b_c = 'black'
        img_list[position[0]][position[1]] = 1
    window[event].update('', button_color=(b_c))

window.close()