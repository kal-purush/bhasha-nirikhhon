import matplotlib.pyplot as plt
from keras.preprocessing import image
from keras.preprocessing.image import ImageDataGenerator
import cv2
import numpy as np
import os
from PIL import Image
import scipy

def pca_color_augmentation_modify(image_array_input):
    assert image_array_input.ndim == 3 and image_array_input.shape[2] == 3
#    assert image_array_input.dtype == np.uint8

    img = image_array_input.reshape(-1, 3).astype(np.float32)
    # 分散を計算
    ch_var = np.var(img, axis=0)
    # 分散の合計が3になるようにスケーリング
    scaling_factor = np.sqrt(3.0 / sum(ch_var))
    # 平均で引いてスケーリング
    img = (img - np.mean(img, axis=0)) * scaling_factor

    cov = np.cov(img, rowvar=False)
    lambd_eigen_value, p_eigen_vector = np.linalg.eig(cov)

    rand = np.random.randn(3) * 0.1
    delta = np.dot(p_eigen_vector, rand*lambd_eigen_value)
    delta = (delta * 255.0).astype(np.int32)[np.newaxis, np.newaxis, :]

    img_out = np.clip(image_array_input + delta, 0, 255).astype(np.uint8)
    return img_out

img_path = '0ex/'
save_path = '0ex-aug/'

files = os.listdir(img_path)
files.remove('.DS_Store')

index = 1

for file in files:

    img = Image.open(os.path.join(img_path, file))
    img_resize = img.resize((160,120))
    # 画像ファイルをPIL形式でオープン
    # img_pil = image.load_img(img_resize)
    # PIL形式をnumpyのndarray形式に変換
    x = image.img_to_array(img_resize)
    # (height, width, 3) -> (1, height, width, 3)
    x = x.reshape((1,) + x.shape)

    datagen = ImageDataGenerator(
            rotation_range=2,
            width_shift_range=0.1,
            height_shift_range=0.1,
            shear_range=0,
            zoom_range=[0.9,1.1],
            horizontal_flip=True,
            vertical_flip=True)

    max_img_num = 1
    i = 1

    for d in datagen.flow(x, batch_size=1):
        # このあと画像を表示するためにndarrayをPIL形式に変換して保存する
        temp = image.img_to_array(d[0])
        temp = pca_color_augmentation_modify(temp)
        cv2.imwrite(save_path + "Aug_{0}_{1}.jpg".format(index,i), np.asarray(temp)[..., ::-1])
        # datagen.flowは無限ループするため必要な枚数取得できたらループを抜ける
        if (i % max_img_num) == 0:
            print("image %d done" %(index))
            index += 1
            break
        i += 1