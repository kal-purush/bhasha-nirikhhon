import numpy as np
import torch
from torch import optim
from torch.nn.utils import clip_grad_norm_


class CNetAgentMy:
    def __init__(self, opt, game, model, target, index):
        self.opt = opt
        self.game = game
        self.model = model
        self.model_target = target

        for p in self.model_target.parameters():
            p.requires_grad = False

        self.episodes_seen = 0
        self.id = index
        self.optimizer = optim.RMSprop(params=model.get_params(), lr=opt["learningrate"], momentum=opt["momentum"])

    def reset(self):
        self.model.reset_parameters()
        self.model_target.reset_parameters()
        self.episodes_seen = 0

    def _eps_flip(self, eps):
        return np.random.rand(1) < eps

    def _random_choice(self, items):
        return torch.from_numpy(np.random.choice(items, 1)).item()

    def select_action_and_comm(self, step, q, eps=0, target=False, train_mode=False):

        if not train_mode:
            eps = 0
        opt = self.opt
        action_range, comm_range = self.game.get_action_range(step, self.id)
        comm_action = 0
        comm_vector = torch.zeros(opt["game_comm_bits"])
        comm_value = 0

        should_select_random_a = self._eps_flip(eps)
        should_select_random_comm = self._eps_flip(eps)

        a_range = range(action_range[0]-1, action_range[1])
        if should_select_random_a:
            action = self._random_choice(a_range)
            action_value = q[action]
        else:
            # print("Printing q : ", q, a_range, q[a_range])
            action_value, action = q[a_range].max(0)

        action = action + 1

        if comm_range[1] > 0:
            c_range = range(comm_range[0]-1, comm_range[1])
            if should_select_random_comm:
                comm_action = self._random_choice(c_range)
                comm_value = q[comm_action]
                comm_action = comm_action - opt["game_action_space"]
            else:
                comm_value, comm_action = q[c_range].max(0)
            # print("Comm vector : ", comm_vector)
            # print("Comm action : ", comm_action)
            comm_vector[comm_action] = 1
            comm_action = comm_action + 1
        else:
            q_a_range = range(0, opt["game_action_space"])
            comm_value,_ = q[q_a_range].max(0)

        return (action, action_value), (comm_vector, comm_action, comm_value)

    def episode_loss(self, episode):

        opt = self.opt
        total_loss = 0
        steps = episode["steps"]
        for step in range(steps):
            record = episode["step_records"][step]
            for i in range(opt["game_nagents"]):
                td_action = 0
                td_comm = 0
                # print(record["r_t"])
                r_t = record["r_t"]
                q_a_t = record["q_a_t"][i]
                q_comm_t = 0

                if record["a_t"][i] > 0:
                    if record["terminal"] > 0:
                        td_action = r_t - q_a_t
                    else:
                        next_record = episode["step_records"][step+1]
                        q_next_max = next_record["q_a_max_t"][i]
                        q_next_max = (q_next_max + next_record["q_comm_max_t"][i])/2
                        td_action = r_t + opt["gamma"] * q_next_max - q_a_t

                if record["a_comm_t"][i] > 0:
                    q_comm_t = record["q_comm_t"][i]
                    if record["terminal"] > 0:
                        td_comm = r_t - q_comm_t
                    else:
                        next_record = episode["step_records"][step+1]
                        q_next_max = next_record["q_comm_max_t"][i]
                        q_next_max = (q_next_max + next_record["q_a_max_t"][i])/2
                        td_comm = r_t + opt["gamma"]*(q_next_max - q_comm_t)

                loss_t = td_action ** 2 + td_comm ** 2

                total_loss = total_loss + loss_t

        loss = total_loss.sum() / opt["game_nagents"]
        return loss

    def learn_from_episode(self, episode):
        self.optimizer.zero_grad()
        loss = self.episode_loss(episode)
        # print("Loss")
        # print(loss)
        loss.backward(retain_graph=not self.opt["model_know_share"])
        clip_grad_norm_(parameters=self.model.get_params(), max_norm=10)
        self.optimizer.step()

        self.episodes_seen = self.episodes_seen + 1
        if self.episodes_seen % self.opt["step_target"] == 0:
            self.model_target.load_state_dict(self.model.state_dict())




