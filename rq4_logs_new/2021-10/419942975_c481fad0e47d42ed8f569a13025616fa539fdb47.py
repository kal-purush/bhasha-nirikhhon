import gym
import numpy as np


import torch
import torch.nn as nn
env = gym.make("CartPole-v1")

# Set device
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(device)

########## Load trained model ##############
class policy_estimator(nn.Module):
    def __init__(self):
        super(policy_estimator, self).__init__()
        self.n_inputs = env.observation_space.shape[0]
        self.n_outputs = env.action_space.n

        # Define network
        self.network = nn.Sequential(
            nn.Linear(self.n_inputs, 16),
            nn.ReLU(),
            nn.Linear(16, self.n_outputs),
            nn.Softmax(dim=-1)
        )

    def predict(self, state):
        action_probs = self.network(torch.FloatTensor(state).to(device=device))

        return action_probs

model = torch.load('cartPole_model.pt')
model.to(device=device)
model.eval()
############################################

def sample_action(action_space_n, probs):
    ran_num = np.random.random()

    p = 0.0
    for i in range(action_space_n):
        p += probs[i].item()

        if ran_num < p:
            return i


observation = env.reset()
for _ in range(5000):
  env.render()
  #action = env.action_space.sample() # your agent here (this takes random actions)
  action_probs = model.predict(observation)

  action = sample_action(env.action_space.n, action_probs)
  observation, reward, done, info = env.step(action)

  if done:
    observation = env.reset()
env.close()