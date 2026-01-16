#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=missing-docstring
import torch
import torchvision
from torch.utils.tensorboard import SummaryWriter


def test_graph():
  writer = SummaryWriter('runs/graph')

  net = torchvision.models.resnet18()

  images = torch.randn(size=(1, 3, 224, 224))

  writer.add_graph(net, images)
  writer.close()


if __name__ == '__main__':
  test_graph()


# pip install tensorboard
# tensorboard --logdir=runs