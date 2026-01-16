import cv2
import numpy as np
import imutils
from imutils import contours
picture = cv2.VideoCapture(0)

while True:
  part, picture_video = picture.read()
  lines = picture_lines("/Users/nityaarora/Downloads/picture.png")
  lines_video = picture_lines(picture_video)
  region_place_part = region_place(lines, lines_video)
  draw_picture("/Users/nityaarora/Downloads/picture.png", region_place_part[0], region_place_part[1], region_place_part[2], region_place_part[3])

  
  
def picture_lines(pictures):
    img = cv2.imread(pictures)
    picture = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)

    hsv_dark_red = np.uint8([[[321, 45, 33]]])

    hsv_light_red = np.uint8([[[330, 36, 76]]])
    kernel = np.ones((5,5), np.float32)/25

    picture_picture = cv2.filter2D(picture, -1, kernel)
    mask_picture = cv2.inRange(picture_picture, hsv_light_red, hsv_dark_red)

    picture_show = cv2.bitwise_and(picture_picture, picture_picture, mask_picture)
    picture_picture_show = cv2.cvtColor(picture_show, cv2.COLOR_BGR2GRAY)

    ret, red_part = cv2.threshold(picture_picture_show, 127,255,0)
    ret_white, white_part = cv2.threshold(picture_picture_show, 255-160, 255, 0)
    white_white_white = cv2.subtract(white_part, red_part)
    full_part, hierarchy_full_part = cv2.findContours(white_white_white, cv2.RETR_TREE,cv2.CHAIN_APPROX_SIMPLE)
    reds_part, hierarchy_red_part = cv2.findContours(red_part, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
    full = full_part
    for i in range(0, len(full_part)):
        for j in range(0, len(full_part[i])):
            for k in range(0, len(reds_part)):
                for m in range(0, len(reds_part[k])):
                    if(full_part[i][j][0][0] == reds_part[k][m][0][0] and full_part[i][j][0][1] == reds_part[k][m][0][1]):
                        full.remove(full_part[i][j])
    return ([full_part, reds_part, full])
def draw_picture(picture, lines_repair, lines_unhealthy, lines_growth, lines_damage):
    picture_lines_repair = cv2.drawContours(picture, lines_repair, -1, (255,0,0), 3)
    picture_lines_unhealthy = cv2.drawContours(picture, lines_unhealthy, -1, (0,255,0), 3)
    picture_lines_growth = cv2.drawContours(picture, lines_growth, -1, (0,0,255), 3)
    picture_lines_damage = cv2.drawContours(picture, lines_damage, -1, (127,127,127), 3)
    cv2.imshow('picture_show', picture)

def region_place(lines, lines_shown):
    repair = []
    unhealthy = []
    growth = []
    damage = []
    for i in range(0, len(lines[0])):
        for j in range(0, len(lines[0][i])):
            for k in range(0, len(lines_shown[1])):
                for m in range(0, len(lines_shown[1][k])):
                    if(lines[0][i][j][0][0] == lines_shown[1][k][m][0][0] and lines[0][i][j][0][1] == lines_shown[1][k][m][0][1]):
                        repair.append(lines[0][i][j][0])

    for i in range(0, len(lines[1])):
        for j in range(0, len(lines[1][i])):
            for k in range(0, len(lines_shown[0])):
                for m in range(0, len(lines_shown[0][k])):
                    if(lines[1][i][j][0][0] == lines_shown[0][k][m][0][0] and lines[1][i][j][0][1] == lines_shown[0][k][m][0][1]):
                        unhealthy.append(lines_shown[0][k][m][0])
    
    for i in range(0, len(lines[2])):
        for j in range(0, len(lines[2][i])):
            for k in range(0, len(lines_shown[2])):
                for m in range(0, len(lines_shown[2][k])):
                    if(lines_shown[2][k][m][0][0] not in lines[2][i][j][0][0] and lines_shown[2][k][m][0][1] == lines[2][i][j][0][1]):
                        growth.append(lines_shown)
                    if(lines[2][i][j][0][0] not in lines_shown[2][k][m][0][0] and lines[2][i][j][0][1] not in lines_shown[2][k][m][0][1]):
                        damage.append(lines[2][i][j][0])
    return([repair, unhealthy, growth, damage])

                        
                        
     
































































