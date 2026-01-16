import shutil
import os
import numpy as np
import argparse
from google_drive_downloader import GoogleDriveDownloader as gdd
from torchvision import datasets, transforms
from torch.utils import data
import torchvision.models as models
import torch
from torchsummary import summary

from PIL import Image
import torch.optim as optim
from collections import Counter
import matplotlib.pyplot as plt
from sklearn.utils import shuffle

%matplotlib inline



def train(net, criterion, optimizer, scheduler, data_loader, batch_print, device):
  running_loss = 0.0

  net.train()
  for i, data in enumerate(data_loader):
    # get the inputs; data is a list of [inputs, labels]
    inputs, labels = data
    inputs = inputs.to(device)
    labels = labels.to(device)
    
    # zero the parameter gradients
    optimizer.zero_grad()

    # forward + backward + optimize
    outputs = net(inputs)
    loss = criterion(outputs, labels)
    loss.backward()
    optimizer.step()
    # print statistics
    running_loss += loss.item()

    if ((i + 1) % batch_print == 0):    # print every 2000 mini-batches
        print('[%5d] loss: %.3f' %
              (i + 1, running_loss / batch_print))
        running_loss = 0.0
  
  scheduler.step()
  return net


def test(net, criterion, data_loader, device):
  total_loss = 0.0
  correct = 0
  net.eval()
  with torch.no_grad():
    for i, data in enumerate(data_loader):
      # get the inputs; data is a list of [inputs, labels]
      inputs, labels = data
      inputs = inputs.to(device)
      labels = labels.to(device)

      # forward + backward + optimize
      outputs = net(inputs)
      loss = criterion(outputs, labels)
      total_loss += loss.item() * outputs.shape[0]
      _, predicted = torch.max(outputs.data, 1)
      correct += (predicted == labels).sum().item()


    loss = total_loss / len(data_loader.dataset)
    accuracy = correct / len(data_loader.dataset)
  return loss, accuracy