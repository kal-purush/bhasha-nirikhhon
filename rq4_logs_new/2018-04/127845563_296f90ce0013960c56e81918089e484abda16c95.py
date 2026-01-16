import tensorflow as tf
import cv2
import copy
import random
from math import exp,sqrt
from pycocotools.coco import COCO
import os
import numpy as np
from scipy.spatial.distance import cdist


def create_data_info(coco,filename,img_dir):
    img_name = filename.rstrip(".jpg")
    img_id = (filename.rstrip(".jpg")).lstrip("0")    
    # print("before*****")
    ann_ids = coco.getAnnIds(imgIds=int(img_id))
    print(ann_ids)
    img_anns = coco.loadAnns(ann_ids)
    print(img_anns)
    # print("after*****")
    numPeople = len(img_anns)
    # image = coco.imgs[img_id]
    # print("imageeee")
    img_path = os.path.join(img_dir, '%s.jpg' % img_name)
    print(img_path)
    img = cv2.imread(img_path)
    h, w = img.shape[0],img.shape[1]
    
    dataset_type = "COCO"

    print("Image ID ", img_id)
    print(numPeople)
    persons = []
    prev_center = []
    joint_all = {}

    for p in range(numPeople):

        # skip this person if parts number is too low or if
        # segmentation area is too small
        if img_anns[p]["num_keypoints"] < 5 or img_anns[p]["area"] < 32 * 32:
            continue

        anno = img_anns[p]["keypoints"]

        pers = {}

        person_center = [img_anns[p]["bbox"][0] + img_anns[p]["bbox"][2] / 2,
                            img_anns[p]["bbox"][1] + img_anns[p]["bbox"][3] / 2]

        # skip this person if the distance to exiting person is too small
        flag = 0
        for pc in prev_center:
            a = np.expand_dims(pc[:2], axis=0)
            b = np.expand_dims(person_center, axis=0)
            dist = cdist(a, b)[0]
            if dist < pc[2]*0.3:
                flag = 1
                continue

        if flag == 1:
            continue

        pers["objpos"] = person_center
        pers["bbox"] = img_anns[p]["bbox"]
        pers["segment_area"] = img_anns[p]["area"]
        pers["num_keypoints"] = img_anns[p]["num_keypoints"]

        pers["joint"] = np.zeros((17, 3))
        for part in range(17):
            pers["joint"][part, 0] = anno[part * 3]
            pers["joint"][part, 1] = anno[part * 3 + 1]

            if anno[part * 3 + 2] == 2:
                pers["joint"][part, 2] = 1
            elif anno[part * 3 + 2] == 1:
                pers["joint"][part, 2] = 0
            else:
                pers["joint"][part, 2] = 2

        pers["scale_provided"] = img_anns[p]["bbox"][3] / 368

        persons.append(pers)
        prev_center.append(np.append(person_center, max(img_anns[p]["bbox"][2], img_anns[p]["bbox"][3])))


    if len(persons) > 0:
        print("adding joints")
        joint_all["dataset"] = dataset_type

        joint_all["img_width"] = w
        joint_all["img_height"] = h
        joint_all["image_id"] = img_name
        joint_all["annolist_index"] = 0 # never used, take out at some point 

        # set image path
        joint_all["img_path"] = os.path.join(img_dir, '%s.jpg' % img_name)

        # set the main person
        joint_all["objpos"] = persons[0]["objpos"]
        joint_all["bbox"] = persons[0]["bbox"]
        joint_all["segment_area"] = persons[0]["segment_area"]
        joint_all["num_keypoints"] = persons[0]["num_keypoints"]
        joint_all["joint_self"] = persons[0]["joint"]
        joint_all["scale_provided"] = persons[0]["scale_provided"]

        # set other persons
        joint_all["joint_others"] = []
        joint_all["scale_provided_other"] = []
        joint_all["objpos_other"] = []
        joint_all["bbox_other"] = []
        joint_all["segment_area_other"] = []
        joint_all["num_keypoints_other"] = []

        for ot in range(1, len(persons)):
            joint_all["joint_others"].append(persons[ot]["joint"])
            joint_all["scale_provided_other"].append(persons[ot]["scale_provided"])
            joint_all["objpos_other"].append(persons[ot]["objpos"])
            joint_all["bbox_other"].append(persons[ot]["bbox"])
            joint_all["segment_area_other"].append(persons[ot]["segment_area"])
            joint_all["num_keypoints_other"].append(persons[ot]["num_keypoints"])

        joint_all["people_index"] = 0
        lenOthers = len(persons) - 1

        joint_all["numOtherPeople"] = lenOthers
        
        mask_all,mask_miss = create_masks(coco,img_anns,img.shape)

        height = img.shape[0]
        width = img.shape[1]

        if (width < 64):
            img = cv2.copyMakeBorder(img, 0, 0, 0, 64 - width, cv2.BORDER_CONSTANT,
                                        value=(128, 128, 128))
            print('saving padded image!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            cv2.imwrite('padded_img.jpg', img)
            width = 64
    else:
        print("no people")
        return None,None,None,None 
    
    return img, joint_all, mask_miss[...,None], mask_all[...,None] if ("dataset" in joint_all) and ("COCO" in joint_all["dataset"]) else None
    
def create_masks(coco,img_anns,img_shape):
    h, w, c = img_shape

    mask_all = np.zeros((h, w), dtype=np.uint8)
    mask_miss = np.zeros((h, w), dtype=np.uint8)
    flag = 0
    for p in img_anns:
        seg = p["segmentation"]

        if p["iscrowd"] == 1:
            mask_crowd = coco.annToMask(p)
            temp = np.bitwise_and(mask_all, mask_crowd)
            mask_crowd = mask_crowd - temp
            flag += 1
            continue
        else:
            mask = coco.annToMask(p)

        mask_all = np.bitwise_or(mask, mask_all)

        if p["num_keypoints"] <= 0:
            mask_miss = np.bitwise_or(mask, mask_miss)

    if flag<1:
        mask_miss = np.logical_not(mask_miss)
    elif flag == 1:
        mask_miss = np.logical_not(np.bitwise_or(mask_miss, mask_crowd))
        mask_all = np.bitwise_or(mask_all, mask_crowd)
    else:
        raise Exception("crowd segments > 1")

    return mask_all * 255, mask_miss * 255        