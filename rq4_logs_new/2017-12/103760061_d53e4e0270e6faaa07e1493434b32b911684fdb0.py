import cv2
import numpy as np

'''
cv2.rectangle(the_image, (10, 10), (120, 120), (0, 0, 0), 1)
cv2.rectangle(the_image, (10, 130), (120, 250), (0, 0, 0), 1)
cv2.rectangle(the_image, (10, 260), (120, 380), (0, 0, 0), 1)
cv2.rectangle(the_image, (10, 390), (120, 510), (0, 0, 0), 1)
cv2.rectangle(the_image, (10, 520), (120, 640), (0, 0, 0), 1)
'''

color_list = []
for i in range(256):
    for j in range(256):
        for k in range(256):
            if i % 16 == 0 and j % 16 == 0 and k % 16 == 0:
                color_list.append([i, j, k])
print(len(color_list))
#print(color_list)

for k in range((len(color_list) // 300)+1):
    img_name = "the_image_"+str(k)+".png"
    save_path = "C:\\Users\\Mig\\PycharmProjects\\Practice\\gen_image\\"
    height, width = 2480, 3508  # A4 paper 300 PPI

    the_image = np.zeros((height, width, 3), np.uint8)
    the_image[:, 0:width] = (255, 255, 255)  # (B, G, R)
    count = 0
    for i in range(20):
        for j in range(15):
            #print((300*k) + count)
            index = (300*k) + count
            if index >= len(color_list):
                index = len(color_list) - 1

            R = color_list[index][0]
            G = color_list[index][1]
            B = color_list[index][2]
            print(R, G, B)

            cv2.rectangle(the_image,  (10+(120*i), 10+(120*j)), (120+(120*i), 120+(120*j)), (B, G, R), -1)
            count += 1
    cv2.imwrite(save_path+img_name, the_image)
    #print("write---------------")

print("Finish")

