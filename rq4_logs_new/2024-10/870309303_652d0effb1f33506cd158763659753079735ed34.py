import json
import cv2
import matplotlib.pyplot as plt
from pathlib import Path
import os
from collections import defaultdict
import random

import torch
from detectron2.engine import DefaultPredictor
from detectron2.config import get_cfg
from detectron2 import model_zoo
from detectron2.utils.visualizer import Visualizer, ColorMode
from detectron2.data import MetadataCatalog  # Ensure this is imported
from detectron2.data.datasets import register_coco_instances  # Ensure this is imported


def register_datasets():
    """
    Register the training and validation datasets.
    """
    register_coco_instances(
        "custom_dataset_train",
        {},
        "dataset/annotations/train_annotations.json",
        "dataset/images"
    )
    register_coco_instances(
        "custom_dataset_val",
        {},
        "dataset/annotations/val_annotations.json",
        "dataset/images"
    )


def visualize_annotations_and_predictions(
    annotations_json,
    images_dir,
    output_gt_dir,
    output_pred_dir,
    model_weights=None,
    score_threshold=0.5,
    filter_file=None
):
    """
    Visualize ground truth annotations and model predictions on sample images.
    
    Args:
        annotations_json (str): Path to the COCO annotations JSON file.
        images_dir (str): Path to the directory containing images.
        output_gt_dir (str): Directory to save images with ground truth bounding boxes.
        output_pred_dir (str): Directory to save images with predicted bounding boxes.
        model_weights (str): Path to the trained Detectron2 model weights. If None, predictions are skipped.
        score_threshold (float): Minimum score for the predicted bounding boxes to be visualized.
        filter_file (str): Path to a text file containing folder names to filter image filenames.
    """
    # Ensure output directories exist
    Path(output_gt_dir).mkdir(parents=True, exist_ok=True)
    Path(output_pred_dir).mkdir(parents=True, exist_ok=True)
    
    # Load annotations
    with open(annotations_json, 'r') as f:
        coco = json.load(f)
    
    images = coco['images']
    annotations = coco['annotations']
    categories = {cat['id']: cat['name'] for cat in coco['categories']}
    
    # Create a mapping from image_id to annotations
    img_id_to_ann = defaultdict(list)
    for ann in annotations:
        img_id_to_ann[ann['image_id']].append(ann)
    
    # Load filter strings if filter_file is provided
    folder_names = []
    if filter_file:
        with open(filter_file, 'r') as f:
            folder_names = [line.strip() for line in f if line.strip()]
    
    # Filter images based on folder_names
    selected_images = []
    for folder in folder_names:
        folder_images = [img for img in images if folder in img['file_name']]
        if folder_images:
            selected_images.append(random.choice(folder_images))
    
    # If no folder names provided, select one random image
    if not selected_images:
        selected_images = [random.choice(images)]

    # Register the datasets
    register_datasets()
    dataset_name = "custom_dataset_val"  # Ensure consistency
    
    # Initialize Detectron2 predictor if model weights are provided
    if model_weights:
        cfg = get_cfg()
        # Merge with the config file used during training
        cfg.merge_from_file(model_zoo.get_config_file("COCO-Detection/faster_rcnn_R_101_FPN_3x.yaml"))
        
        # Update the number of classes to match your custom dataset
        cfg.MODEL.ROI_HEADS.NUM_CLASSES = 1  # Change to your number of classes
        
        # Set the path to your trained model weights
        cfg.MODEL.WEIGHTS = model_weights  # e.g., "output/model_final.pth"
        
        # Set the computation device
        cfg.MODEL.DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
        
        # Set threshold for this model
        cfg.MODEL.ROI_HEADS.SCORE_THRESH_TEST = score_threshold
        
        predictor = DefaultPredictor(cfg)
    else:
        predictor = None
        print("No model weights provided. Skipping predictions.")
    
    for img in selected_images:
        img_path = Path(images_dir) / img['file_name']
        image = cv2.imread(str(img_path))
        if image is None:
            print(f"Failed to load image {img_path}")
            continue
        image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        
        # === Visualize Ground Truth with OpenCV ===
        for ann in img_id_to_ann[img['id']]:
            x, y, w, h = ann['bbox']
            top_left = (int(x), int(y))
            bottom_right = (int(x + w), int(y + h))
            cv2.rectangle(image_rgb, top_left, bottom_right, color=(0, 255, 0), thickness=2)  # Green box

            # Draw label
            label = categories.get(ann['category_id'], 'object')
            font = cv2.FONT_HERSHEY_SIMPLEX
            font_scale = 0.5
            thickness = 1
            text_size, _ = cv2.getTextSize(label, font, font_scale, thickness)
            text_origin = (int(x), int(y) - 5 if int(y) - 5 > 5 else int(y) + 5)

            # Draw background rectangle for text
            cv2.rectangle(image_rgb, 
                          (text_origin[0], text_origin[1] - text_size[1] - 2),
                          (text_origin[0] + text_size[0] + 2, text_origin[1] + 2),
                          color=(255, 255, 0),  # Yellow background
                          thickness=-1)  # Filled rectangle

            # Put text
            cv2.putText(image_rgb, label, text_origin, font, font_scale, 
                        color=(0, 0, 0), thickness=thickness, lineType=cv2.LINE_AA)

        gt_output_path = Path(output_gt_dir) / f"gt_{img['file_name']}"
        cv2.imwrite(str(gt_output_path), cv2.cvtColor(image_rgb, cv2.COLOR_RGB2BGR))
        print(f"Saved ground truth image to {gt_output_path}")
        
        # === Visualize Predictions ===
        if predictor:
            outputs = predictor(image)
            # Use the correct metadata
            v = Visualizer(image_rgb[:, :, ::-1], MetadataCatalog.get(dataset_name), scale=1.2, instance_mode=ColorMode.IMAGE)
            instances = outputs["instances"].to("cpu")
            
            # Optionally filter predictions by score_threshold
            instances = instances[instances.scores >= score_threshold]
            
            try:
                out = v.draw_instance_predictions(instances)
                pred_output_path = Path(output_pred_dir) / f"pred_{img['file_name']}"
                cv2.imwrite(str(pred_output_path), out.get_image()[:, :, ::-1])
                print(f"Saved prediction image to {pred_output_path}")
            except ValueError as e:
                print(f"Error processing or saving prediction image: {e}")
                print(f"Image file: {img['file_name']}")
                print(f"Image dimensions: {image.shape}")
                # You might want to implement a resizing strategy here
                # For example:
                # max_dim = 2**16 - 1
                # h, w = image.shape[:2]
                # if h > max_dim or w > max_dim:
                #     scale = min(max_dim / h, max_dim / w)
                #     new_h, new_w = int(h * scale), int(w * scale)
                #     resized_image = cv2.resize(image, (new_w, new_h))
                #     # Rerun prediction and visualization on resized image
                # ...


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Visualize Ground Truth and Model Predictions.")
    parser.add_argument(
        "--annotations_json", 
        type=str, 
        default="data/v2-datasets-playwright/live-playwright-coco/annotations/val_annotations.json",
        help="Path to the COCO annotations JSON file."
    )
    parser.add_argument(
        "--images_dir", 
        type=str,  
        default="data/v2-datasets-playwright/live-playwright-coco/images",
        help="Path to the directory containing images."
    )
    parser.add_argument(
        "--output_gt_dir", 
        type=str, 
        default="v2-eval-final/trash", 
        help="Directory to save images with ground truth bounding boxes."
    )
    parser.add_argument(
        "--output_pred_dir", 
        type=str, 
        default="v2-eval-final/eval", 
        help="Directory to save images with predicted bounding boxes."
    )
    parser.add_argument(
        "--model_weights", 
        type=str, 
        default="data/v4-coco-finetune/model_final.pth", 
        help="Path to the trained Detectron2 model weights. If not provided, predictions are skipped."
    )
    parser.add_argument(
        "--score_threshold", 
        type=float, 
        default=0.20, 
        help="Minimum score for the predicted bounding boxes to be visualized."
    )
    parser.add_argument(
        "--filter_file", 
        type=str, 
        required=True, 
        help="Path to a text file containing folder names to filter image filenames."
    )

    args = parser.parse_args()

    visualize_annotations_and_predictions(
        annotations_json=args.annotations_json,
        images_dir=args.images_dir,
        output_gt_dir=args.output_gt_dir,
        output_pred_dir=args.output_pred_dir,
        model_weights=args.model_weights,
        score_threshold=args.score_threshold,
        filter_file=args.filter_file
    )