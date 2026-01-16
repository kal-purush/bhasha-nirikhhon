from __future__ import print_function, division
import torch
import matplotlib.pyplot as plt
import argparse,os
import pandas as pd
import cv2
import numpy as np
import random
import math
from torch.utils.data import Dataset, DataLoader
from torchvision import transforms
import time
import tqdm
from models.CDCNs import Conv2d_cd, CDCN, CDCNpp
from dataloader.Load_test_private_data import Spoofing_valtest, Normaliztion_valtest, ToTensor_valtest

import torch.nn.functional as F
from torch.autograd import Variable
import torch.nn as nn
import torch.optim as optim
import torch.onnx

from torchsummary import summary
import copy
import pdb
from datetime import datetime
from utils import AvgrageMeter, accuracy, performances, test_threshold_based, performances_wild_img, performances_SiWM_EER

# Dataset root

# main function
def train_test():
    # GPU  & log file  -->   if use DataParallel, please comment this command
    os.environ["CUDA_VISIBLE_DEVICES"] = "%d" % (args.gpu)

    temp = datetime.now()
    year = temp.year
    month = temp.month
    day = temp.day

    args.log = args.log+'_{:04d}_{:02d}_{:02d}'.format(year, month, day)
    isExists = os.path.exists(args.log)

    print("Private Data:\n ")

    # load the network, load the pre-trained model in UCF101?
    finetune = args.finetune
    print('finetune!\n')

    device = args.gpu
    t_start = time.time()
    model = CDCNpp(basic_conv=Conv2d_cd, theta=0.7)
    t_load_model = time.time()
    model = model.cuda()

    if torch.cuda.is_available():
        map_location=lambda storage, loc: storage.cuda()
    else:
        map_location='cpu'

    model.load_state_dict(torch.load('./zip/v11/256/CDCNpp_train_pd_256s_v11.4.2_2022_6_17/CDCNpp_train_pd_256s_v11.4.2_2022_6_17_6.pkl', map_location=map_location))

    size = 256
    t_load_weight = time.time()
    output =  "./zip/onnx/{}/FAS_{}s_v11.4.onnx".format(size, size)
    model.eval()

    time_process_filename = args.log+'/'+args.log+'_time_process.txt'
    time_lst = []
    inputs = torch.zeros(1, 3, size, size).cuda()
    torch.onnx.export(model,
                      inputs,
                      output,
                       export_params = True,
                       verbose=False,
                       opset_version = 11,
                       do_constant_folding = True,
                       input_names =  ['input'],
                       output_names = ['output'],
                       dynamic_axes =  {'input' : {0 : 'batch_size', 2 : 'width', 3 : 'height'},
                                       'output' : {0 : 'batch_size', 2 : 'width', 3 : 'height'}}
                            )

    import onnx
    
    onnx_model = onnx.load(output)
    onnx.checker.check_model(onnx_model, full_check=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="save quality using landmarkpose model")
    parser.add_argument('--gpu', type=int, default=1, help='the gpu id used for predict')
    parser.add_argument('--batchsize', type=int, default=1, help='initial batchsize')  
    parser.add_argument('--gamma', type=float, default=0.5, help='gamma of optim.lr_scheduler.StepLR, decay of lr')
    parser.add_argument('--echo_batches', type=int, default=50, help='how many batches display once')  # 50
    parser.add_argument('--log', type=str, default="CDCNpp_Private_data_xiaomi_gmcr_v6.5", help='log and save model name')
    parser.add_argument('--finetune', action='store_true', default=False, help='whether finetune other models')

    args = parser.parse_args()
    train_test()