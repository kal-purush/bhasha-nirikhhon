# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
import glob
import logging
import os
from collections import OrderedDict

import numpy as np
import pandas as pd
from tqdm import tqdm
import torch
import cv2
import random

import detectron2.utils.comm as comm
from detectron2.checkpoint import DetectionCheckpointer
from detectron2.config import get_cfg
from detectron2.data import DatasetCatalog, MetadataCatalog
from detectron2.engine import DefaultPredictor
from detectron2.utils.visualizer import Visualizer

from detectron2.engine import DefaultTrainer, default_argument_parser, default_setup, hooks, launch
from detectron2.evaluation import (
    DatasetEvaluators,
    PascalVOCDetectionEvaluator,
    verify_results,
)
from detectron2.modeling import GeneralizedRCNNWithTTA
from detectron2.data.datasets import register_coco_instances
from detectron2.data.datasets.pascal_voc import register_pascal_voc

from dataset_tools.config import dataset_path
from dataset_tools.loaders import get_camera_names
from dataset_tools.view.videos import png2video, combine_videos

_datasets_root = "datasets"
for d in ["trainval", "test"]:
    register_pascal_voc(name=f'100DOH_hand_{d}', dirname=_datasets_root, split=d, year=2007, class_names=["hand"])
    MetadataCatalog.get(f'100DOH_hand_{d}').set(evaluator_type='pascal_voc')


def test_imgs(img_paths, save_dir, visualize=True):
    os.makedirs(save_dir, exist_ok=True)

    # load cfg and model
    cfg = get_cfg()
    cfg.merge_from_file("./modules/hand_pose/hand_detector/faster_rcnn_X_101_32x8d_FPN_3x_100DOH.yaml")
    cfg.MODEL.WEIGHTS = './modules/hand_pose/hand_detector/models/model_0529999.pth'  # add model weight here
    cfg.MODEL.ROI_HEADS.SCORE_THRESH_TEST = 0.7  # 0.5 , set the testing threshold for this model

    predictor = DefaultPredictor(cfg)

    results = []

    for img_path in tqdm(img_paths):
        # output
        im = cv2.imread(img_path)
        outputs = predictor(im)

        if visualize:
            v = Visualizer(im[:, :, ::-1], MetadataCatalog.get(cfg.DATASETS.TRAIN[0]), scale=1.2)
            v = v.draw_instance_predictions(outputs["instances"].to("cpu"))
            cv2.imwrite(f'{save_dir}/{os.path.basename(img_path)}', v.get_image()[:, :, ::-1])

        bbox = outputs["instances"].pred_boxes.tensor.cpu().numpy()
        if len(bbox) > 0:
            results.append((img_path, bbox))

    return results


def test_on_scene(scene_name):
    scene_path = f'{dataset_path}/{scene_name}'
    for camera_name in get_camera_names(scene_path):
        print(camera_name)
        img_paths = glob.glob(f'{scene_path}/{camera_name}/rgb/*.png')
        results = test_imgs(img_paths, f'{scene_path}/{camera_name}/hand_pose/d2', visualize=True)

        table = np.empty(0, dtype=[('scene_name', 'U20'),
                                 ('camera_name', 'U22'),
                                 ('frame', 'i4'),
                                 ('bbox', 'O')])

        for (img_path, bbox) in results:
            frame = int(os.path.basename(img_path)[:-4])
            table = np.append(table, np.array([(scene_name, camera_name, frame, bbox)], dtype=table.dtype))

        df = pd.DataFrame.from_records(table)
        df['bbox'] = df['bbox'].apply(np.ndarray.tolist)
        df = df.sort_values(by=['frame'])
        file_path = f'{scene_path}/{camera_name}/hand_pose/d2/detections.csv'
        df.to_csv(file_path, index=False)

        png2video(f'{scene_path}/{camera_name}/hand_pose/d2', frame_rate=30)

    combine_videos(sorted(glob.glob(f'{scene_path}/camera*/hand_pose/d2/video.mp4')),
                   f'{scene_path}/hand_pose/d2/video.mp4')


if __name__ == '__main__':
    scene_name = 'scene_2210232307_01'
    test_on_scene(scene_name)