import torch
import torch.nn as nn
import torch.distributions as dist
import torch.nn.functional as F


class LSTMEncoder(nn.Module):
    def __init__(self, input_size, num_layers, hidden_size=128, latent_dimension=512):
        super().__init__()
        self.num_layers = num_layers
        self.hidden_size = hidden_size
        self.latent_dimension = latent_dimension

        self.lstm = nn.LSTM(input_size=input_size, hidden_size=hidden_size, num_layers=num_layers, bidirectional=True, batch_first=True)
        self.fc_mean = nn.Linear(in_features=hidden_size*2, out_features=latent_dimension)
        self.fc_std = nn.Linear(in_features=hidden_size*2, out_features=latent_dimension)

    def forward(self, x):
        # tensor shape of initial hidden state: (num_directions∗num_layers, batch_size, hidden_size)
        # ref: https://pytorch.org/docs/stable/generated/torch.nn.LSTM.html

        _, (h_t, _) = self.lstm(x)
        h_t = h_t.view(self.num_layers, 2, -1, self.hidden_size)
        h_t_forward, h_t_backward = h_t[-1, :, :, :]
        h_t = torch.cat((h_t_forward, h_t_backward), dim=1)
        mean, std = self.fc_mean(h_t), F.softplus(self.fc_std(h_t))
        z_dist = dist.Normal(loc=mean, scale=std)
        return z_dist

        
class Conductor(nn.Module):
    def __init__(self, latent_dimension, u_size, hidden_size=256, conductor_input_size=1, num_layers=2) -> None:
        super().__init__()
        self.num_layers = num_layers
        self.hidden_size = hidden_size
        self.u_size = u_size
        self.conductor_input_size = conductor_input_size

        self.fc = nn.Linear(in_features=latent_dimension, out_features=hidden_size*num_layers*2) # h, c -> 2
        self.lstm = nn.LSTM(input_size=conductor_input_size, hidden_size=hidden_size, num_layers=num_layers, batch_first=True)

    def forward(self, z):
        t = torch.tanh(self.fc(z))

        hidden_states, cell_states = t[None, :, :self.hidden_size*self.num_layers], t[None, :, self.hidden_size*self.num_layers:]
        hidden_states = hidden_states.reshape([self.num_layers, -1, self.hidden_size])
        cell_states = cell_states.reshape([self.num_layers, -1, self.hidden_size])

        conductor_input = torch.zeros(size=(z.shape[0], self.u_size, self.conductor_input_size))
        embeddings, _ = self.lstm(conductor_input, (hidden_states, cell_states))
        embeddings = torch.unbind(embeddings, dim=1) # embedding: (B, u_size, conductor_hidden_size) -> u_size * (B, hidden_size)
        return embeddings