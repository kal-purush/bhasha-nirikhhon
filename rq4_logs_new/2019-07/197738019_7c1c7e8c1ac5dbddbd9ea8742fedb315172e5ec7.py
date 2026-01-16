import torch
import torch.utils.data as Data
import torch.nn as nn
import numpy as np
import torchvision.datasets as dsets
import torchvision.transforms as transforms
import matplotlib.pyplot as plt
from e_util import offer_data

print(torch.cuda.is_available())


#Hyper Parameters
EPOCH = 1
LR = 0.01

#首先需要获得数据
url = r"D:\010010 1987-2018.txt"
X,y = offer_data(url,1,0,1)
X_train = np.array(X[:7000],dtype=np.float32)
y_train = np.array(y[:7000],dtype=np.float32)
X_test = np.array(X[7000:9000],dtype=np.float32)
y_test = np.array(y[7000:9000],dtype=np.float32)



class RNN(nn.Module):
    def __init__(self):
        super(RNN,self).__init__()
        self.rnn = nn.RNN(13,20,1,batch_first=True)
        self.out = nn.Linear(20,1)

    def forward(self, x,h_state):
        output, h_state = self.rnn(x, h_state)

        input_linear = []

        for time_step in range(output.size(1)):
            input_linear.append(self.out(output[:,time_step,:]))

        return torch.stack(input_linear,dim=1),h_state

rnn = RNN()
print(rnn)

rnn.train()

optimizer = torch.optim.Adam(rnn.parameters(), lr=LR)   # optimize all cnn parameters
loss_func = nn.MSELoss()

h_state = None      # for initial hidden state

for step in range(1):
    # use sin predicts cos

    x = torch.from_numpy(X_train)    # shape (batch, time_step, input_size)
    y = torch.from_numpy(y_train)

    x.view((-1,20,13))
    x.cuda()
    y.cuda()

    prediction, h_state = rnn(x, h_state)   # rnn output
    # !! next step is important !!
    h_state = h_state.data
    # repack the hidden state, break the connection from last iteration

    loss = loss_func(prediction, y)         # calculate loss
    optimizer.zero_grad()                   # clear gradients for this training step
    loss.backward()                         # backpropagation, compute gradients
    optimizer.step()                        # apply gradients


rnn.eval()


for step in range(1):
    # use sin predicts cos

    x = torch.from_numpy(X_test)    # shape (batch, time_step, input_size)
    y = torch.from_numpy(y_test)

    x.view((-1,20,13))
    x.cuda()
    y.cuda()

    prediction, h_state = rnn(x, None)   # rnn output
    # !! next step is important !!
    h_state = h_state.data
    # repack the hidden state, break the connection from last iteration

    loss = loss_func(prediction, y)         # calculate loss
    print("loss:   ",torch.sum(loss).detach().numpy()/len(y_test))