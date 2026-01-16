import gym


class RandomAgent:

    def __init__(self, env_name):
        self.env = gym.make(env_name)

    def predict(self):
        return self.env.action_space.sample()