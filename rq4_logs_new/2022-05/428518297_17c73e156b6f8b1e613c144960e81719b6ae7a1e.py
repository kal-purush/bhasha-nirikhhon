import numpy as np
import cv2
import os
import torch
import pandas as pd
import torch.nn.functional as F
import torch.optim as optim
from torch.utils.data import DataLoader
import torch.nn as nn

from autoencoder_models.nets import AutoEncoderNet, ContrastiveLoss, EmbddingDataset

VOC_PATH = "/home/kevin/data/VOC2012/"
EXTRACTED_DATA_PATH = os.path.join(VOC_PATH, "extracted")
TRAIN_PATH = "/home/kevin/train/"

device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
print(device)
dataset = EmbddingDataset(os.path.join(VOC_PATH, "box_features.csv"), EXTRACTED_DATA_PATH, split_name="test", get_diff_scale=True)

dataloader = DataLoader(dataset, batch_size=1,
                        shuffle=True, num_workers=0)
fm_output_sizes= [
    576,
    1280,
    512,
    256,
    256,
    64
]

# distance_fn = F.pairwise_distance
distance_fn = nn.CosineSimilarity()

fm_model_dict = {}
for i in range(0, 6):
    fm_model_dict[i] = AutoEncoderNet(fm_output_sizes[i])
    fm_model_dict[i].to(device)
    fm_model_dict[i].load_state_dict(torch.load(os.path.join(TRAIN_PATH, "fm_{}.pt".format(i))))
    fm_model_dict[i].eval()

with torch.no_grad():
    for i, data, in enumerate(dataloader):
        if len(data) == 1:
            continue

        def forward_input(input_list):
            fv = input_list[0].to(device)
            fm_id = input_list[1].numpy()[0]
            # print(fv.shape, fm_id)
            return fm_model_dict[fm_id](fv)

        curr_embed = forward_input(data['curr'])
        same_embed = forward_input(data['same'])
        diff_embed = forward_input(data['diff'])

        same_dist = distance_fn(curr_embed, same_embed).to('cpu')
        print("same: {}".format(same_dist.numpy()[0]))
        diff_dist = distance_fn(curr_embed, diff_embed).to('cpu')
        print("diff: {}".format(diff_dist.numpy()[0]))



