import csv
from ultralytics import YOLO

# 加载预训练的YOLOv8模型
model = YOLO('/root/autodl-tmp/yolov8/runs/detect/train40/weights/best.pt')  # 选择合适的模型权重文件

# 进行推理
results = model('/root/autodl-tmp/yolov8/datasets/UA-DETRAC5/val/images', save=True)  # 替换为实际测试图像的路径

# 定义CSV文件的列名
header = ['image', 'class', 'confidence', 'xmin', 'ymin', 'xmax', 'ymax']

# 打开一个新的CSV文件以写入结果
with open('yolov8_results.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(header)
    
    # 遍历结果
    for result in results:
        boxes = result.boxes
        for box in boxes:
            xmin, ymin, xmax, ymax = box.xyxy[0]  # 获取坐标
            confidence = box.conf[0]  # 获取置信度
            cls = box.cls[0]  # 获取类别
            image_name = result.path.split('/')[-1]  # 获取图像名称
            row = [image_name, int(cls), float(confidence), float(xmin), float(ymin), float(xmax), float(ymax)]
            writer.writerow(row)

print("Results have been saved to yolov8_results.csv")