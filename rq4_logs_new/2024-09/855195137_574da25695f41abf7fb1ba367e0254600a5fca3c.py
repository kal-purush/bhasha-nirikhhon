import numpy as np
import gymnasium as gym


class InventoryEnv(gym.Env):
    def __init__(self):
        super(InventoryEnv, self).__init__()

        # Define as ações possíveis. 0: não reabastece, 1: reabastece
        self.action_space = gym.spaces.Discrete(2)

        # Define os estados possíveis. # Estados: 0, 1, 2 (estoque)
        self.observation_space = gym.spaces.Discrete(3)

        # Parâmetros do ambiente
        self.p_sale = 0.3
        self.custo_manutencao_1 = 1
        self.custo_manutencao_2 = 2 * self.custo_manutencao_1
        self.custo_entrega = 5
        self.state = 2  # Estado inicial (estoque cheio)
        self.steps = 0
        self.steps_limit = 100

    def reset(self, seed=None, options=None):
        """
        Reseta o ambiente e define a seed
        """
        super().reset(seed=seed)
        self.state = 2  # Estado inicial
        self.steps = 0
        return self.state, {}

    def step(self, action):
        # Ação de reabastecimento
        if action == 1 and self.state < 2:
            self.state += 1
            reward = -self.custo_entrega  # Custo de entrega
        else:
            reward = 0

        # Simular venda
        venda = np.random.choice(
            [True, False], p=[self.p_sale, 1 - self.p_sale])
        if venda and self.state > 0:
            reward += 10  # Ganha pela venda
            self.state -= 1  # Reduz o estoque

        # Custo de manutenção
        if self.state == 2:
            reward -= self.custo_manutencao_2
        else:
            reward -= self.custo_manutencao_1

        self.steps += 1

        # Finaliza o episódio após 100 passos
        done = self.steps >= self.steps_limit
        return self.state, reward, done, False, {}

    def render(self, mode="human"):
        # Mostra o estado atual
        print(f"Estoque atual: {self.state}")