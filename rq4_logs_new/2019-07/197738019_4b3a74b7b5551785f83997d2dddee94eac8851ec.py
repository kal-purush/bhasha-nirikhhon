import math
import torch
import torch.utils.data as Data
import torch.nn as nn
import numpy as np
import torchvision.transforms as transforms
import matplotlib.pyplot as plt
from e_util import offer_data
from MyDataset import CustomDataset

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

#Hyper Parameters
LR = 0.0025
NUM_EPOCHS = 10
BATCH_SIZE = 32

#首先需要获得数据
url = r"D:\010010 1987-2018.txt"
train_dataset = CustomDataset(url)

train_loader = torch.utils.data.DataLoader(dataset=train_dataset,
                                           batch_size=BATCH_SIZE,
                                           shuffle=True)

test_dataset = CustomDataset(url,train=False)

test_loader = torch.utils.data.DataLoader(dataset=test_dataset,
                                           batch_size=BATCH_SIZE,
                                           shuffle=True)


class RNN(nn.Module):
    def __init__(self):
        super(RNN,self).__init__()
        self.rnn = nn.RNN(13,32,2,batch_first=True)
        self.dropout = nn.Dropout(p=0.5)
        self.fc = nn.Linear(32,1)

    def forward(self, x):
        out, h_state = self.rnn(x, None)
        out = self.dropout(out)
        #batch_size , seq ,
        out = self.fc(out[:, -1, :])
        return out

model = RNN().to(device)
print(model)

optimizer = torch.optim.Adam(model.parameters(), lr=LR)   # optimize all cnn parameters
loss_func = nn.MSELoss()

# Train the model
total_step = len(train_loader)
for epoch in range(NUM_EPOCHS):
    for i, (images, labels) in enumerate(train_loader):
        images = images.reshape(-1, 7, 13).to(device)
        labels = labels.to(device)

        # Forward pass
        outputs = model(images)
        loss = loss_func(outputs, labels)
        # print(outputs)
        # print("\n")
        # print(labels)
        # exit()

        # Backward and optimize
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        if (i + 1) % 10 == 0:
            print('Epoch [{}/{}], Step [{}/{}], Loss: {:.4f}'
                  .format(epoch + 1, NUM_EPOCHS, i + 1, total_step, loss.item()))

# Test the model
with torch.no_grad():
    sum_error = 0
    total = 2000
    for images, labels in test_loader:

        images = images.reshape(-1, 7, 13).to(device)
        labels = labels.to(device)
        outputs = model(images)

        predicted = outputs.data.cpu().numpy().flatten()


        sum_error += np.sum(np.square(predicted-labels.cpu().numpy().flatten()))



    print('RMSE: {} '.format( math.sqrt(sum_error/ total)))