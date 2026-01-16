"""
Python class to load and transform the data.
"""

import torch
torch.manual_seed(0)

import torch.nn as nn
from torchvision import datasets, transforms
from torch.utils.data import DataLoader
import numpy as np
import scipy
import os


class Loader(nn.Module):
    """
    Class to load the CIFAR-100 data from torchvision and return a tensor 
    """

    def __init__(self, PATH, num_batches):
        super().__init__()
        self.path = PATH
        self.num_batches = num_batches

    def __str__(self):
        return f"Trainloader length = {len(self.getdata()[0])} | Testloader length = {len(self.getdata()[1])}"



    def getdata(self):
        """
        Function to download and store the data into dataloaders. 
        """

        transformer = transforms.Compose([
            transforms.ToTensor(),
            transforms.Normalize( (0, 0,0) , (1, 1, 1))
        ])

        traindata = datasets.CIFAR100(self.path, train = True, download = True, transform = transformer)

        testdata = datasets.CIFAR100(self.path, train = False, download = True, transform = transformer)

        trainloader = DataLoader(traindata, batch_size=self.num_batches, shuffle=True)
        testloader = DataLoader(testdata,  batch_size=self.num_batches, shuffle=False)


        return trainloader, testloader


if __name__ == "__main__":
    """
        Testing the class defined above.
    """        
    loader = Loader(PATH="/home/agastya123/PycharmProjects/CIFAR-100/data", num_batches = 64)

    
    
    





 








