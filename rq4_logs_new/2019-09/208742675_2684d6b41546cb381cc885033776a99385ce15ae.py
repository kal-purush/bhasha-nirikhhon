# 图片缩放：
from PIL import Image
from PIL import ImageFilter
from PIL import ImageEnhance

img = Image.open("dog.jpg")
print(img.format)  # 输出图片基本信息
print(img.mode)
print(img.size)
img_resize = img.resize((350, 350))  # 调整尺寸
img_resize.save("dogresize.jpg")

