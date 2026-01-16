import torch
from glob import glob
from PIL import Image, ImageOps
import cv2 as cv
from lib.opts import opts
from lib.models.model import create_model, load_model
from lib.datasets.dataset_factory import get_dataset
import os
from torchvision import transforms
import matplotlib.pyplot as plt
import numpy as np
from lib.trains.train_factory import train_factory

def val_img(path: str, opt) -> None:
    save_img_path = './dataset/output/'
      
    Dataset = get_dataset(opt.dataset, opt.task)
    opt = opts().update_dataset_info_and_set_heads(opt, Dataset)
    os.environ['CUDA_VISIBLE_DEVICES'] = opt.gpus_str
    opt.device = torch.device('cuda' if opt.gpus[0] >= 0 else 'cpu')
    
    model = create_model(opt.model_name)
    model = model.to(opt.device)
    model.eval() 

    val_loader = torch.utils.data.DataLoader(
        Dataset(opt, 'test'),
        batch_size=1,
        shuffle=False,
        num_workers=0,
        pin_memory=True
    )
    result=np.zeros([1024, 512], np.uint8)
    for i, inputs in enumerate(val_loader):
        with torch.no_grad():  
            output = model(inputs['input'].to(opt.device))
            pred = output.squeeze(0).squeeze(0)
            pred = pred.cpu()
            pred = pred.numpy()
            plt.imshow(pred, cmap='gray')
            pred = cv.resize(pred, (512, 795))
            pred = cv.copyMakeBorder(pred, 4, 225, 0, 0, 
                borderType=cv.BORDER_CONSTANT, 
                value=[0, 0, 0]  # 颜色为黑色 (BGR)
            )
            plt.imsave(save_img_path + str(i+17) + '_test.png', pred, cmap='gray')
    plt.show()

if __name__ == "__main__":
    opt = opts().parse()
    val_Img_dir = "./dataset/DRIVE/humanseg/val/images/*.tif"
    val_img(path=val_Img_dir, opt=opt)