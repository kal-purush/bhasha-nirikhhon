from typing import Tuple
import torch
import torch.nn as nn
from ..networks.representation import AutoEncoder
from ..common.replay import Dataset


class AgentModel(nn.Module):
    def __init__(self, config: dict, device: str) -> None:
        super().__init__()
        self.device = device

        self.autoencoder = AutoEncoder(config["representation"], self.device)

        self.prev_state_shape = config["transition"]["gru_dim"]
        self.prev_state = torch.zeros(self.prev_state_shape).to(self.device)

    def reset_hidden_state(self):
        self.prev_state = torch.zeros(self.prev_state_shape).to(self.device)

    def set_hidden_state(self, state: torch.tensor):
        self.prev_state = state

    def embed_observation(self, obs: torch.tensor) -> torch.tensor:
        return self.autoencoder.embed(obs)

    def decode_observation(self, obs: torch.tensor) -> torch.tensor:
        return self.autoencoder.decoder(obs)

    def train(self, dataset: Dataset):
        raise NotImplementedError(
            "train is not implemented for abstract class 'AgentModel'"
        )

    def predict(
        self, states: torch.tensor, h: torch.tensor
    ) -> Tuple[torch.tensor, torch.tensor, torch.tensor, torch.tensor]:
        raise NotImplementedError(
            "predict is not implemented for abstract class 'AgentModel'"
        )