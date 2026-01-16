import cv2
import os

from utils import read_json, image_resize, image_tf_resize

DATASET = "../dataset/img"

ANNOTATION = "../dataset/image_ann"

images = os.listdir(DATASET)
frame_rate = 30

if not images:
    print("No images found in the folder")
    exit()
    
cv2.namedWindow('Video Stream', cv2.WINDOW_NORMAL)

height = 432
width = 768

resize_w = 400
resize_h = 200

w_coef = resize_w / width
h_coef = resize_h / height

while True:
    for image_name in images:
        image_path = os.path.join(DATASET, image_name)
        image_annotation_path = os.path.join(ANNOTATION, image_name.replace(".jpg", ".json"))
        image_json = read_json(image_annotation_path)
        image = cv2.imread(image_path)
        
        image = image_tf_resize(image, resize_h, resize_w)

        if image is None:
            continue
        
        # print("asd: ", image_json["objects"][0]["points"])
        
        left_eye_points = image_json["objects"][0]["points"]
        
        left_eye_top_left = (int(left_eye_points["x"] * w_coef), int(left_eye_points["y"] * h_coef)) 
        left_eye_bottom_right = (int((left_eye_points["x"] + left_eye_points["w"]) * w_coef), int((left_eye_points["y"] + left_eye_points["h"]) * h_coef))
        
        right_eye_points = image_json["objects"][1]["points"]
        
        right_eye_top_left = (int(right_eye_points["x"] * w_coef), int(right_eye_points["y"] * h_coef)) 
        right_eye_bottom_right = (int((right_eye_points["x"] + right_eye_points["w"]) * w_coef), int((right_eye_points["y"] + right_eye_points["h"]) * h_coef))
        
        red_color = (255, 0, 0) 
        green_color = (0, 255, 0)
        thickness = 2
        
        cv2.rectangle(image, left_eye_top_left, left_eye_bottom_right, red_color, thickness)
        cv2.rectangle(image, right_eye_top_left, right_eye_bottom_right, green_color, thickness)
        
        cv2.imshow('Video Stream', image)
        

        if cv2.waitKey(int(1000 / frame_rate)) & 0xFF == ord('q'):
            break
    
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

    cv2.destroyAllWindows()
    
# -------------------------------------------------------

# image_name = images[0]

# image_path = os.path.join(DATASET, image_name)
# image_annotation_path = os.path.join(ANNOTATION, image_name.replace(".jpg", ".json"))

# image_json = read_json(image_annotation_path)
# print("image_annotation: ", image_json)
# image = cv2.imread(image_path)

# cv2.imshow('Video Stream', image)
