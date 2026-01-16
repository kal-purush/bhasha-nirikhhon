import torch
import torch.nn as nn
import torch.nn.functional as F

class QNetwork(nn.Module):
    def __init__(self, state_size, action_size, random_seed, num_units):
        super(QNetwork, self).__init__()
        self.random_seed = torch.manual_seed(random_seed)
        
        self.fc1 = nn.Linear(state_size, num_units)
        nn.init.xavier_uniform_(self.fc1.weight)
        self.fc2 = nn.Linear(num_units, num_units)
        nn.init.xavier_uniform_(self.fc2.weight)
        self.fc3 = nn.Linear(num_units, num_units)
        nn.init.xavier_uniform_(self.fc3.weight)
        self.fc4 = nn.Linear(num_units, num_units)
        nn.init.xavier_uniform_(self.fc4.weight)
        self.out = nn.Linear(num_units, action_size)
        nn.init.xavier_uniform_(self.out.weight)
        
        # self.sigmoid = nn.Sigmoid()
        
    def forward(self, state):
        x = self.fc1(state)
        x = F.relu(x)
        x = self.fc2(x)
        x = F.relu(x)
        x = self.fc3(x)
        x = F.relu(x)
        x = self.fc4(x)
        x = F.relu(x)
        x = self.out(x)
        
        return x
        
		
		
		
class ConvQNetwork(nn.Module):
    def __init__(self, state_size, action_size, random_seed):
        super(QNetwork, self).__init__()
        self.random_seed = torch.manual_seed(random_seed)
        
        self.fc1 = nn.Linear(state_size, 64)
        nn.init.xavier_uniform_(self.fc1.weight)
        self.fc2 = nn.Linear(64, 64)
        nn.init.xavier_uniform_(self.fc2.weight)
        self.fc3 = nn.Linear(64, 64)
        nn.init.xavier_uniform_(self.fc3.weight)
        self.fc4 = nn.Linear(64, 64)
        nn.init.xavier_uniform_(self.fc4.weight)
        self.out = nn.Linear(64, action_size)
        nn.init.xavier_uniform_(self.out.weight)
        
        # self.sigmoid = nn.Sigmoid()
        
    def forward(self, state):
        x = self.fc1(state)
        x = F.relu(x)
        x = self.fc2(x)
        x = F.relu(x)
        x = self.fc3(x)
        x = F.relu(x)
        x = self.fc4(x)
        x = F.relu(x)
        x = self.out(x)
        
        return x
        