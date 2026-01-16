from ..agent.agent_model import AgentModel
from ..common.replay import Dataset
from ..common.utils import create_state_action_batch
from ..networks.lp_bnn_transition import LPBNNTransitionModel
from ..networks.terminal import Network as TerminalNetwork
from ..training.transition import TransitionModelTrainer
from numpy.random import RandomState

import torch


from typing import Tuple


class LPBNNAgentModel(AgentModel):
    def __init__(
        self,
        config: dict,
        device: str,
        actions: torch.tensor,
        random_state: RandomState,
    ) -> None:
        super().__init__(config, device)

        self.terminal_network = TerminalNetwork(
            config["representation"]["embed_dim"], config["terminal"], self.device
        )

        self.transition_network = LPBNNTransitionModel(
            config["representation"]["embed_dim"],
            len(actions),
            config["transition"],
            self.device,
        )

        self.transition_trainer = TransitionModelTrainer(
            config["transition"],
            self.transition_network,
            self.autoencoder,
            self.terminal_network,
            config["replay"]["batch_size"],
            len(actions),
            self.device,
        )

        self.ensemble_size = config["transition"]["ensemble_size"]
        self.random_state = random_state
        self.actions = actions
        self.num_actions = len(actions)

    def train(self, dataset: Dataset):
        self.representation_trainer.train_(dataset)
        self.transition_trainer.train_(dataset)

    def state_action_batch(self, states, hidden_state):
        state_actions, hidden_state = create_state_action_batch(
            states, self.actions, hidden_state, self.num_actions, self.device
        )

        state_actions = torch.concat(
            [state_actions for _ in range(self.ensemble_size)], 0
        ).to(self.device)

        hidden_state = torch.concat(
            [hidden_state for _ in range(self.ensemble_size)], 0
        ).to(self.device)

        return state_actions, hidden_state

    def sample_from_ensembles(self, features, hidden_states):
        def sample(output):
            output = output.view((self.ensemble_size, -1, *output.shape[1:]))
            return self.random_state.choice(output)

        ensemble_states = features[:, :-1]
        ensemble_rewards = features[:, -1]

        states = sample(ensemble_states)
        rewards = sample(ensemble_rewards)
        hidden_states = sample(hidden_states)

        return states, rewards, hidden_states

    def predict(
        self, states: torch.tensor, h: torch.tensor
    ) -> Tuple[torch.tensor, torch.tensor, torch.tensor, torch.tensor]:
        with torch.no_grad():
            state_actions, h = self.state_action_batch(states, h)
            features, h = self.transition_network(state_actions, h)

            states, rewards, h = self.sample_from_ensembles(features, h)
            terminals = self.terminal_network.predict(states)

            return states, rewards.reshape(-1, 1), terminals, h

    def resample_model(self):
        # nothing to do here, lp-bnn samples new weights in the VAE for each forward pass
        pass