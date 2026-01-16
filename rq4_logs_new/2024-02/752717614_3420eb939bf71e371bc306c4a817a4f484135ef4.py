import deeplake
import numpy as np
import math
import sys
import time
import torchvision
import albumentations as A
from albumentations.pytorch import ToTensorV2
import torch
from torchvision.models.detection.faster_rcnn import FastRCNNPredictor
from torchvision.models.detection.mask_rcnn import MaskRCNNPredictor
from torchvision.models.detection import fasterrcnn_resnet50_fpn
from torchvision.models.detection.faster_rcnn import FastRCNNPredictor
import torchvision.models.detection.mask_rcnn
import torch
import torch.nn as nn
import torch.optim as optim

# Creating custom train dataset
ds_train = deeplake.ingest_coco(
    "/data/MOT17/gt/mot_challenge/SVIN_train_coco",
    ["data/MOT17/gt/mot_challenge/annotations/SVIN_train_coco.json"],
    dest="./dataset/train",                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
    key_to_tensor_mapping={"category_id": "labels", "bbox": "boxes"},
    ignore_keys=["area", "image_id", "id","segmentation","iscrowd","visibility","seq"],
    num_workers=4,
    overwrite=True
)

# Create custom new val dataset
ds_val = deeplake.ingest_coco(
    "data/MOT17/gt/mot_challenge/SVIN_val_coco",
    ["data/MOT17/gt/mot_challenge/annotations/SVIN_val_coco.json"],
    dest="./dataset/val",                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
    key_to_tensor_mapping={"category_id": "labels", "bbox": "boxes"},
    ignore_keys=["area", "image_id", "id","segmentation","iscrowd","visibility","seq"],
    num_workers=4,
    overwrite=True
)


print("Datasets created")
print(ds_val.summary())

# Augmentation pipeline using Albumentations
tform_train = A.Compose([
    A.RandomSizedBBoxSafeCrop(width=128, height=128, erosion_rate = 0.2),
    A.HorizontalFlip(p=0.5),
    A.Normalize(mean=(0.485, 0.456, 0.406), std=(0.229, 0.224, 0.225)),
    ToTensorV2(), # transpose_mask = True
], bbox_params=A.BboxParams(format='pascal_voc', label_fields=['class_labels', 'bbox_ids'], min_area=25, min_visibility=0.6)) # 'label_fields' and 'box_ids' are all the fields that will be cut when a bounding box is cut.


# Transformation function for pre-processing the Deep Lake sample before sending it to the model
# outputs image and target labels and boxes
def transform(sample_in):
    # Convert boxes to Pascal VOC format
    boxes = coco_2_pascal(sample_in['boxes'])
    # Convert any grayscale images to RGB
    images = sample_in['images']
    transformed = tform_train(image = images,
                                bboxes = boxes,
                                bbox_ids = np.arange(boxes.shape[0]),
                                class_labels = sample_in['labels'],
                                )
    # Convert boxes and labels from lists to torch tensors, because Albumentations does not do that automatically.
    # Be very careful with rounding and casting to integers, becuase that can create bounding boxes with invalid dimensions
    labels_torch = torch.tensor(transformed['class_labels'], dtype = torch.int64)
    boxes_torch = torch.zeros((len(transformed['bboxes']), 4), dtype = torch.int64)
    for b, box in enumerate(transformed['bboxes']):
        boxes_torch[b,:] = torch.tensor(np.round(box))

    # Put annotations in a separate object
    target = {'labels': labels_torch, 'boxes': boxes_torch}

    return transformed['image'], target


# Conversion script for bounding boxes from coco to Pascal VOC format
def coco_2_pascal(boxes):
    # Convert bounding boxes to Pascal VOC format and clip bounding boxes to make sure they have non-negative width and height
    return np.stack((boxes[:,0], boxes[:,1], boxes[:,0]+np.clip(boxes[:,2], 1, None), boxes[:,1]+np.clip(boxes[:,3], 1, None)), axis = 1)

def collate_fn(batch):
    return tuple(zip(*batch))

# Create the dataloaders    
batch_size = 8

train_loader = ds_train.pytorch(num_workers = 2, shuffle = False,
    tensors = ['images', 'labels', 'boxes'], # Specify the tensors that are needed, so we don't load unused data
    transform = transform,
    batch_size = batch_size,
    collate_fn = collate_fn,
    
    )

val_loader = ds_val.pytorch(num_workers = 2, shuffle = False,
    tensors = ['images', 'labels', 'boxes'], # Specify the tensors that are needed, so we don't load unused data
    transform = transform,
    batch_size = batch_size,
    collate_fn = collate_fn,
    )


# Create a FasterRCNN for object detection 
def get_model_object_detection(num_classes):
    # Load a detection model pre-trained on COCO
    model = fasterrcnn_resnet50_fpn(pretrained=True)

    # Get number of input features for the classifier
    in_features = model.roi_heads.box_predictor.cls_score.in_features
    
    # Replace the pre-trained head with a new one
    model.roi_heads.box_predictor = FastRCNNPredictor(in_features, num_classes)

    return model

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

model = get_model_object_detection(num_classes = 1)

model.to(device)



# Specity the optimizer
params = [p for p in model.parameters() if p.requires_grad]
optimizer = torch.optim.SGD(params, lr=0.005,
                            momentum=0.9, weight_decay=0.0005)


def train_one_epoch(model, optimizer, data_loader, device):
    model.train()

    start_time = time.time()
    for i, data in enumerate(data_loader):

        images = list(image.to(device) for image in data[0])
        targets = [{k: v.to(device) for k, v in t.items()} for t in data[1]]

        print("Passing to model")
        loss_dict = model(images, targets)
        losses = sum(loss for loss in loss_dict.values())
        loss_value = losses.item()

        # Print performance statistics
        batch_time = time.time()
        speed = (i+1)/(batch_time-start_time)
        print('[%5d] loss: %.3f, speed: %.2f' %
              (i, loss_value, speed))

        if not math.isfinite(loss_value):
            print(f"Loss is {loss_value}, stopping training")
            print(loss_dict)
            break

        optimizer.zero_grad()

        losses.backward()
        optimizer.step()


# Train the model for 1 epoch
num_epochs = 4
for epoch in range(num_epochs):  # loop over the dataset multiple times
    print("------------------ Training Epoch {} ------------------".format(epoch+1))
    train_one_epoch(model, optimizer, train_loader, device)