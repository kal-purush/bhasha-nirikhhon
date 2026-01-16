from PIL import Image
import os

folder = 'images'
step = 5

dir_list = os.listdir(folder)
 
for Q in range(1, 100, step):
    current_folder = f'{folder}_{Q}'
    os.mkdir(current_folder)

    for im_name in dir_list:
        im = Image.open(f'{folder}//{im_name}')
        im.save(f'{current_folder}//{im_name}', quality=Q)