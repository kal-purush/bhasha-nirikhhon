from torchvision.datasets import ImageFolder
from torchvision.transforms import ToTensor
import torchvision
import numpy as np
import matplotlib.pyplot as plt

plt.ion() 

attributefile = open("Anno/list_attr_cloth.txt", 'r')
attributes = [line.split() for line in attributefile.readlines()] #attributes starts from index 3
del attributes[0]

attributeimage = open("Anno/test.txt", 'r')
for lines in range(1):
    sample = [line.split() for line in attributeimage.readline()] 

i = 1
j = 0
for j in range (len(sample)):
    print(sample[0][0])
    for i in range (len(sample[j])):
        if sample[j][i] == '1':
            print(attributes[i])

#print(attributes) #debug

data = ImageFolder(root='img', transform=ToTensor())

from torch.utils.data import DataLoader
loader = DataLoader(data,shuffle=True, num_workers=4)

class_names = data.classes

def imshow(inp, title=None):
    """Imshow for Tensor."""
    inp = inp.numpy().transpose((1, 2, 0))
    mean = np.array([0.485, 0.456, 0.406])
    std = np.array([0.229, 0.224, 0.225])
    inp = std * inp + mean
    inp = np.clip(inp, 0, 1)
    plt.imshow(inp)
    if title is not None:
        plt.title(title)
    plt.suptitle(title, fontsize=14, fontweight='bold')
    plt.pause(0.001)  # pause a bit so that plots are updated

# Get a batch of training data
inputs, classes = next(iter(loader))

# Make a grid from batch
out = torchvision.utils.make_grid(inputs)

imshow(out, title=[class_names[x] for x in classes])

title=[class_names[x] for x in classes]

"""with open('Anno/list_attr_img.txt', 'r') as searchfile:
    for line in searchfile:
        if title[0] in line:
            sample = [line.split() for line in searchfile.readlines()] 

            i = 1
            j = 0
            for j in range (len(sample)):
                print(sample[0][0])
                for i in range (len(sample[j])):
                    if sample[j][i] == '1':
                        print(attributes[i])"""

plt.ioff()
plt.show()