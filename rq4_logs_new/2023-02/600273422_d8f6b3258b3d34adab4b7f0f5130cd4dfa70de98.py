# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
from collections import deque
import torch
import numpy as np
import gym
from gym.wrappers import TimeLimit
import mj_envs.envs.arms

class FrankaWrapper(gym.Wrapper):
    def __init__(self, env, cfg):
        super().__init__(env)
        self.env = env
        self.cfg = cfg
        self._num_frames = cfg.get("frame_stack", 1)
        self._frames = deque([], maxlen=self._num_frames)
        img_size = max(cfg.img_size, 10)
        self.observation_space = gym.spaces.Box(
            low=0,
            high=255,
            shape=(self._num_frames * 3, img_size, img_size),
            dtype=np.uint8,
        )
        act_low = -np.ones(7)
        act_high = np.ones(7)
        self.action_space = gym.spaces.Box(act_low, act_high, dtype=np.float32)
        self.camera_name = cfg.get("camera_view", "view_1")

    @property
    def state(self):
        return self._state_obs.astype(np.float32)

    def _get_state_obs(self, obs):
      if self.cfg.task.startswith('franka-FrankaPickPlaceRandom'):
        qp = self.env.sim.data.qpos.ravel()
        qv = self.env.sim.data.qvel.ravel()
        grasp_pos = self.env.sim.data.site_xpos[self.env.grasp_sid].ravel()
        obj_err = self.env.sim.data.site_xpos[self.env.object_sid]-self.env.sim.data.site_xpos[self.env.grasp_sid]
        tar_err = self.env.sim.data.site_xpos[self.env.target_sid]-self.env.sim.data.site_xpos[self.env.object_sid]

        if self.cfg.img_size <= 0:
            manual = np.concatenate([qp,qv,grasp_pos,obj_err,tar_err])
            assert(np.isclose(obs[:manual.shape[0]], manual).all())
        else:
            manual = np.concatenate([qp[:9], qv[:9], grasp_pos])
            assert(np.isclose(obs[:9], qp[:9]).all())
            assert(np.isclose(obs[qp.shape[0]:qp.shape[0]+9], qv[:9]).all())
            assert(np.isclose(obs[qp.shape[0]+qv.shape[0]:qp.shape[0]+qv.shape[0]+3], grasp_pos).all())

        return manual
      raise NotImplementedError()

    def _get_pixel_obs(self):
        if self.cfg.img_size <= 0:
            return np.zeros((3,self.observation_space.shape[1],self.observation_space.shape[2]), dtype=np.uint8)

        vis_obs_dict = self.env.get_visual_obs_dict(self.env.sim_obsd)
        rgb_key = 'rgb:'+self.camera_name+':'+str(self.cfg.img_size)+'x'+str(self.cfg.img_size)+':2d'
        assert rgb_key in vis_obs_dict, 'Missing viz key {}, available {}'.format(rgb_key, vis_obs_dict.keys())
        rgb_img = vis_obs_dict[rgb_key].squeeze().transpose(2,0,1) # cxhxw
        return rgb_img       

    def _stacked_obs(self):
        assert len(self._frames) == self._num_frames
        return np.concatenate(list(self._frames), axis=0)

    def reset(self):
        obs = self.env.reset()
        self._state_obs = self._get_state_obs(obs)
        obs = self._get_pixel_obs()
        for _ in range(self._num_frames):
            self._frames.append(obs)
        return self._stacked_obs()

    def step(self, action):
        reward = 0

        aug_action = np.zeros(action.shape[0]+1, dtype=action.dtype)
        aug_action[:3] = action[:3]
        aug_action[3] = 1.0
        aug_action[4] = -1.0
        aug_action[5] = np.arctan2(action[4], action[3])/3.14
        aug_action[6] = 2*(int(action[5]>0.0)-0.5)
        aug_action[7] = action[6]
        
        for _ in range(self.cfg.action_repeat):
            obs, r, _, info = self.env.step(aug_action)
            reward += r
        self._state_obs = self._get_state_obs(obs)
        obs = self._get_pixel_obs()
        self._frames.append(obs)
        info["success"] = info["solved"]
        reward = float(info["success"]) - 1.0
        return self._stacked_obs(), reward, False, info

    def render(self, mode="rgb_array", width=None, height=None, camera_id=None):
        return np.flip(
            self.env.env.sim.render(
                mode="offscreen",
                width=width,
                height=height,
                camera_name=self.camera_name,
            ),
            axis=0,
        )

    def observation_spec(self):
        return self.observation_space

    def action_spec(self):
        return self.action_space

    def __getattr__(self, name):
        return getattr(self._env, name)


def make_franka_env(cfg):
    env_id = cfg.task.split("-", 1)[-1] + "-v0"
    env = gym.make(env_id, **{'reward_mode': 'sparse'})
    env = FrankaWrapper(env, cfg)
    env = TimeLimit(env, max_episode_steps=cfg.episode_length)
    env.reset()
    cfg.state_dim = env.state.shape[0]
    return env