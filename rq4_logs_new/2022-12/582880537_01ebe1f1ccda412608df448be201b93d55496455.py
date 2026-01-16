
# 待识别图片路径
test_img_path = ["test_face_detection.png","test_face_detection_2.png","test_face_detection_3.jpg"]

import matplotlib.pyplot as plt
import matplotlib.image as mpimg

# 展示待识别图片
for picture in test_img_path:
    print(picture)
    img = mpimg.imread(picture,0)
    plt.figure(figsize=(10,10))
    plt.imshow(img)
    plt.axis('off')
    plt.show()

# 加载预训练模型
import paddlehub as hub
module = hub.Module(name="ultra_light_fast_generic_face_detector_1mb_640")
# module = hub.Module(name="ultra_light_fast_generic_face_detector_1mb_320")

input_dict = {"image": test_img_path}
result_img_path = []

results = module.face_detection(data=input_dict, visualization=True)    #获取识别结果
print(results)
for result in results:
    if ('save_path' in result): result_img_path.append(result['save_path']) #提取识别后的图像的存储路径
    else: result_img_path.append("")

# 初始化掌控板
import time
from pinpong.board import Board, LED
Board("handpy").begin()
esp32 = LED()

# 识别结果展示
for each_path in result_img_path:
    if (each_path == ""):
        print("face not detected")
        esp32.set_rgb_color(-1, 255, 0, 0)  # 设置LED灯颜色为红色
        time.sleep(1)
    else:
        img = mpimg.imread(each_path)   # 显示识别后的图像
        plt.figure(figsize=(10, 10))
        plt.imshow(img)
        plt.axis('off')
        plt.show()
        esp32.set_rgb_color(-1, 255, 255, 255)  # 设置LED灯的颜色为白色
        time.sleep(1)