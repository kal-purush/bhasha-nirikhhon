import torch
from torchvision import models


networks_name = {0: "seresnet", 1: "vgg", 2: "alexnet", 3: "resnet18", 4: "resnet34", 5: "resnet50", 6: "resnet101",
                 7: "resnet152", 8: "inception", 9: "googlenet", 10: "mnasnet", 11: "squeezenet", 12: "mobilenet",
                 13: "shufflenet", 14: "densenet121", 15: "densenet201"}

network = [12]
network_ls = [v for k, v in networks_name.items() if k in network]

lr = 0.1
optimizer = "momentum"
num_workers = 2
dest_name = "mobilenet_0422"

batch_sizes = {"seresnet": 128, "vgg": 128, "alexnet": 128, "resnet18": 128, "resnet34": 128, "resnet50": 128, "resnet101": 128,
              "resnet152": 128, "inception": 128, "googlenet": 128, "mnasnet": 128, "squeezenet": 256, "mobilenet": 256,
              "shufflenet": 256, "densenet121": 128, "densenet201": 128}
epochs = {"seresnet": 2, "vgg": 2, "alexnet": 2, "resnet18": 2, "resnet34": 2, "resnet50": 2, "resnet101": 2,
              "resnet152": 2, "inception": 2, "googlenet": 2, "mnasnet": 2, "squeezenet": 2, "mobilenet": 160,
              "shufflenet": 2, "densenet121": 2, "densenet201": 2}
dataset = ["cifar10", "cifar100"]
sparse = {"seresnet": [False], "vgg": [False], "alexnet": [False], "resnet18": [False],
          "resnet34": [False], "resnet50": [False], "resnet101": [False], "resnet152": [False],
          "inception": [False], "googlenet": [False], "mnasnet": [False], "squeezenet": [False],
          "mobilenet": [False], "shufflenet": [False], "densenet121": [False], "densenet201": [False]}



# For mobilenet training

