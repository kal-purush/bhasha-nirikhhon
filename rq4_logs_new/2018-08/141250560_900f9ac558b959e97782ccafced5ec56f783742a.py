from tqdm import tqdm
import numpy as np
import os

from urllib.request import urlretrieve
from os.path import isfile, isdir
import tarfile


file_path = ''

cat_list = ['Abyssinian', 'Bengal', 'Birman', 'Bombay', 'British_Shorthair', 'Egyptian_Mau', 'Maine_Coon','Persian','Ragdoll','Russian_Blue','Siamese','Sphynx']


def file_name_sub(file, str):
    return file.rfind(str)

def trange_file_name(file_path):

    image_dir = os.listdir(file_path)
	
    for i, file in tqdm(enumerate(image_dir)):
        
        #file_name = ''
        #file_ps = ''

        file_name_su = file_name_sub(file, '_')
        file_name = file[:file_name_su]
        file_name_end = file[file_name_su+1:]
        file_ps_su = file_name_sub(file_name_end, '.')
        file_ps = file_name_end[file_ps_su:]

        if file_ps == '.jpg' or file_ps == '.png':
            if file_name in cat_list:
                new_name = 'cat' + '_{}'.format(i) + file_ps
            else:
                new_name = 'dog' + '_{}'.format(i) + file_ps
		
            os.rename(file_path+file, file_path+ new_name)
        else:
            os.remove(file_path+file)

def get_labels(file_path):

    global labels

    image_dir = os.listdir(file_path)
	
    labels = np.zeros((len(image_dir), 2), dtype=int)
	
    for i, file in tqdm(enumerate(image_dir)):
        if 'dog' in file:
            labels[i+25000][0] = 1
        else:
            labels[i+25000][1] = 1
			
    return labels
	
def save_labels(file_name, lables):
    with open(file_name, 'w') as f:
        f.write('id,dog,cat\n')
	
    with open(file_name, 'a') as f:
        num = len(labels)
        for i in tqdm(range(0,num)):
            dog = labels[i][0]
            cat = labels[i][1]
            f.write('{},{},{}\n'.format(i+25001,dog,cat))
    f.close()
    print('file closed!')
	
def test_folder_path(image_supply_path):
    assert image_supply_path is not None,\
        'Cifar-10 data folder not set.'
    assert image_supply_path[-1] != '/',\
        'The "/" shouldn\'t be added to the end of the path.'
    assert os.path.exists(image_supply_path),\
        'Path not found.'
    assert os.path.isdir(image_supply_path),\
        '{} is not a folder.'.format(os.path.basename(image_supply_path))

    print('All files found!')