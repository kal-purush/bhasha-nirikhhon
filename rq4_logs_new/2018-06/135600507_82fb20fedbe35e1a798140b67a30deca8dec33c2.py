import cv2
import numpy as np
import os

def callback(x):
    pass


# Create default image
# with shape 128x128
# and put our image in center
#
def create_default_image(source_image):
    sizze = 128
    default_image = np.empty((sizze, sizze))
    default_image.fill(0)

    if source_image.shape[0] < source_image.shape[1]:
        resize_ratio = sizze / float(source_image.shape[1])
    else:
        resize_ratio = sizze / float(source_image.shape[0])

    image_res_to_def = cv2.resize(source_image, (int(source_image.shape[1] * resize_ratio), int(source_image.shape[0] * resize_ratio)))

    # default_image
    pos_st_y = int((sizze - image_res_to_def.shape[0]) / 2.)
    pos_st_x = int((sizze - image_res_to_def.shape[1]) / 2.)
    default_image[
        pos_st_y:pos_st_y + image_res_to_def.shape[0],
        pos_st_x:pos_st_x + image_res_to_def.shape[1]
    ] = image_res_to_def

    return default_image
#
# ===== ###
#

def read_image(path):
    image = cv2.imread(path)

    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (7, 7), 0)
    th_ret, edged = cv2.threshold(gray, 0, 255, cv2.THRESH_OTSU)
    edged = cv2.bitwise_not(edged)  # invert
    default_image = create_default_image(edged)

    return default_image, image
#=====


source_folder = 'dataset'
target_folder_images = 'dataset_grouped'
target_folder = 'dataset_np'
images_count = 0
dataset_train_count = 0
dataset_test_count = 0

dataset_train = []
dataset_test = []

for (root, dirs, files) in os.walk(source_folder):
    label_images = []
    label_parts = root.split('/')
    if len(label_parts) > 1:
        label = label_parts[1]

        for f in files:
            if f.endswith('.jpg'):
                images_count += 1
                label_images.append(f)

        dataset_part_n = int(len(label_images) / 3)

        dataset_train.append((label, label_images[:dataset_part_n*2]))
        dataset_train_count += len(label_images[:dataset_part_n*2])

        dataset_test.append((label, label_images[:dataset_part_n]))
        dataset_test_count += len(label_images[:dataset_part_n])
        #print " > Label: ", label, " Files: ", label_images

labels_count = len(dataset_train)
print "Labels count: ", labels_count
print "Images count: ", images_count
print "Train count: ", dataset_train_count
print "Test count: ", dataset_test_count

data_train = np.empty([dataset_train_count, 128, 128]).astype('uint8')
data_test = np.empty([dataset_test_count, 128, 128]).astype('uint8')
labels_train = np.zeros((dataset_train_count,)).astype('uint8')
labels_test = np.zeros((dataset_test_count,)).astype('uint8')


##
## Train set
##
files_train_index = 0
label_index = 1
print " == TRAIN"
for group in dataset_train:
    for img_name in group[1]:
        label = group[0]
        print ">> ("+str(label_index)+"/"+str(labels_count)+")",  label, img_name#, image
        image, original = read_image(source_folder + "/" + label + "/" + img_name)
        data_train[files_train_index, :, :] = image
        labels_train[files_train_index] = ord(label)

        files_train_index += 1
    label_index += 1
##
## Test set
##
files_test_index = 0
label_index = 1
print " == TEST"
for group in dataset_test:
    for img_name in group[1]:
        label = group[0]
        print ">> ("+str(label_index)+"/"+str(labels_count)+")",  label, img_name#, image
        image, original = read_image(source_folder + "/" + label + "/" + img_name)
        data_test[files_test_index, :, :] = image
        labels_test[files_test_index] = ord(label)

        # save files
        img_folder = target_folder_images + '/test/' + label
        if not os.path.exists(img_folder):
            os.mkdir(img_folder)

        cv2.imwrite(img_folder + '/' + img_name, original)

        files_test_index += 1
    label_index += 1

print 'Saving to ' + target_folder

np.save(target_folder + '/x_train', data_train)
print 'Train data saved in ' + target_folder + '/x_train.npy'
np.save(target_folder + '/y_train', labels_train)
print 'Train labels saved in ' + target_folder + '/y_train.npy'

np.save(target_folder + '/x_test', data_test)
print 'Test data saved in ' + target_folder + '/x_test.npy'
np.save(target_folder + '/y_test', labels_test)
print 'Test labels saved in ' + target_folder + '/y_test.npy'