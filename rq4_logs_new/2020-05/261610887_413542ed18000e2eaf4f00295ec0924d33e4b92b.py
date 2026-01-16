import os
import cv2
import imgaug.augmenters as iaa
from os import walk
import imgaug.parameters as iap

f = []
for (dirpath, dirnames, filenames) in walk("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/3/"):
    f.extend(filenames)
def get_image(row_id, root="C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/3/"):
    filename = "{}".format(row_id)
    file_path = os.path.join(root, filename)
    img = cv2.imread(file_path)
    return img


for files in f:
    img = get_image(files)
    
    gaussian_noise = iaa.AdditiveGaussianNoise(10, 20)
    noise_image = gaussian_noise.augment_image(img)
    
    flip_hr = iaa.Fliplr(p=1.0)
    flip_hr_image = flip_hr.augment_image(img)

    contrast = iaa.GammaContrast(gamma=2.0)
    contrast_image = contrast.augment_image(img)
    
    aug = iaa.SigmoidContrast(gain=(3, 10), cutoff=(0.4, 0.6))
    sigmoid_image = aug.augment_image(img)

    aug = iaa.LogContrast(gain=(0.6, 1.4))
    log_image = aug.augment_image(img)
		
    aug = iaa.MultiplySaturation((0.5, 1.5))
    channel = aug.augment_image(img)

	
    aug = iaa.Rotate((-5, 5))
    rotate1 = aug.augment_image(img)

    aug = iaa.ShearX((-20, 20))
    shear = aug.augment_image(img)

    aug = iaa.PerspectiveTransform(scale=(0.01, 0.15))
    perspec = aug.augment_image(img)
    cv2.imshow("3",perspec)
    cv2.waitKey(0)
    aaa
	
    rotate = iaa.Affine(rotate=(-50, 30))
    polar = rotate.augment_image(img)
    image_name = files.split("_")[0]
    label = "_"+files.split("_")[1]
    cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/2/"+image_name+str(0)+label,noise_image)
    cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/2/"+image_name+str(5)+label,flip_hr_image)
    cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/2/"+image_name+str(6)+label,contrast_image)
    cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/2/"+image_name+str(7)+label,sigmoid_image)
    cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/2/"+image_name+str(8)+label,log_image)
    if label =="_1.jpg":
    	
    	cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/2/" + image_name + str(12) + label, channel)
    if label =="_2.jpg":
    	
    	cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/2/" + image_name + str(12) + label, channel)
    	cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/2/" + image_name + str(13) + label, rotate1)
    	cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/2/" + image_name + str(14) + label, shear)
    	cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/2/" + image_name + str(15) + label, perspec)
    	cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/augmented/2/" + image_name + str(0) + label, contrast_image)
    	cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/augmented/2/" + image_name + str(2) + label, noise_image)

    if label=="_3.jpg":
    	
    	cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/2/" + image_name + str(12) + label, channel)
    	cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/2/" + image_name + str(13) + label, rotate1)
    	cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/2/" + image_name + str(14) + label, shear)
    	cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/2/" + image_name + str(15) + label, perspec)
    	cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/augmented/3/" + image_name + str(0) + label, contrast_image)
    	cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/augmented/3/" + image_name + str(2) + label, noise_image)

    if label=="_4.jpg":
    	
    	cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/2/" + image_name + str(13) + label, rotate1)
    	cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/2/" + image_name + str(14) + label, shear)
    	cv2.imwrite("C:/Users/Kenil/Downloads/Competitive/Dataset/Combined Dataset/final_training_data/2/" + image_name + str(15) + label, perspec)
    print(files)














