from stable_baselines3 import PPO
from stable_baselines3.common.evaluation import evaluate_policy
from stable_baselines3.common.monitor import Monitor
from stable_baselines3.common.vec_env import DummyVecEnv
from stable_baselines3.common.env_checker import check_env
from stable_baselines3.common.callbacks import EvalCallback
from dataclasses import dataclass
import tyro
from typing import Tuple

import wandb
from wandb.integration.sb3 import WandbCallback
import yaml

import argparse
import gymnasium as gym
from gymnasium.envs.registration import register
import numpy as np
import os
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.sys.path.insert(0, parent_dir)
from common.logger import get_logger
from typing import Literal
from datetime import datetime
log = get_logger(__name__)

ENV_NAME = 'PccNs-v0'


#Use Tyro for CLI arguments
@dataclass
class Args:
    exp_name: str = os.path.basename(__file__)[: -len(".py")]
    """the name of this experiment"""
    wandb_project_name: str = "test_model"
    """the name of the wandb project"""
    tracking: bool = True
    """whether to use wandb for tracking"""
    # Algorithm specific arguments
    env_name: str = "PccNs-v1"
    """the id of the environment"""
    total_timesteps: int = 400000
    """total timesteps of the experiments"""
    learning_rate: float = 3e-4
    """the learning rate of the optimizer"""
    num_envs: int = 1
    """the number of parallel game environments"""
    batch_size: int = 1024
    """size of the batch"""
    learning_rate: float = 3e-4
    """the learning rate of the optimizer"""
    reward_fun: str = "default"
    """Reward function to use"""
    # TODO: restrict options reward_fun: Literal["default", "alternative1", "alternative2"] = "default"
    # """Reward function to use. Must be one of: 'default', 'alternative1', 'alternative2'"""
    history_len: int = 10
    """How many historical steps to take"""
    action_scale: int = 1
    """The action space of the environment [+/-(action_space)]"""
    device: str = "auto"
    """The device to use for training"""
    max_steps: int = 400
    """The maximum number of steps to take in the environment"""
    a: int = 10
    """The throughput reward coefficient"""
    b: int = -1000
    """The latency reward coefficient"""
    c: int = -2000
    """The loss reward coefficient"""
    # reward_coefficients: tuple[float, float, float] = (10, -1000, -2000)
    # """Coefficients for reward calculation (throughput, latency, loss)"""
    use_pretrained: bool = False
    """Whether to use a pretrained model"""
    mode: str = "training"
    """The mode to run the model in (training, testing)"""
    bw: int = 500
    """The fixed bandwidth of the network link in packets per second (100-500)"""
    latency: int = 50
    """The fixed delay of the network link in milliseconds (0.05, 0.5)"""
    queue_size: int = 2
    """The natural log of fixed queue size in packets of the network link (0-8)"""
    loss: float = 0.01  
    """The fixed loss rate of the network link (0.0, 0.05)"""
    


args = tyro.cli(Args)
experiment_name = args.exp_name
config = {  # default config. 
    "exp_name": experiment_name+"r="+args.reward_fun,
    "project": args.wandb_project_name,
    "policy_type": "MlpPolicy",
    "total_timesteps": args.total_timesteps,
    "env_name": args.env_name,
    "batch_size": args.batch_size,
    "learning_rate": args.learning_rate,
    "reward_fun": args.reward_fun,
    "history-len": args.history_len,
    "action_scale": args.action_scale,
    "device": args.device,
    "max_steps": args.max_steps,
    "a": args.a,
    "b": args.b,
    "c": args.c,
    # "reward_coefficients": args.reward_coefficients
    "mode": args.mode,
    "bw": args.bw,
    "latency": args.latency,
    "queue_size": args.queue_size,
    "loss": args.loss
}


register(id="PccNs-v0", entry_point='network_sim:SimulatedNetworkEnv')
register(id="PccNs-v1", entry_point='network_sim_v1:SimulatedNetworkEnv')

# env = gym.make(config["env_name"])

def make_env():
    env = gym.make(config["env_name"], reward_fun = config["reward_fun"], 
                   history_len=args.history_len, ACTION_SCALE=args.action_scale,
                   max_steps=args.max_steps, reward_coefficients=(args.a,args.b,args.c),
                   mode=args.mode, network_values=(args.bw, args.latency, args.queue_size, args.loss))
    env = Monitor(env)
    return env

env = DummyVecEnv([make_env])
env = make_env()
check_env(env)

# Passes. 

# Initialize wandb
if args.tracking:
    wandb.login()
    run = wandb.init(
        project=args.wandb_project_name,
        config=config,
        sync_tensorboard=True,  # auto-upload sb3's tensorboard metrics
        # monitor_gym=True,  # auto-upload the videos of agents playing the game
        save_code=True,  # optional
        # name="default_batch"
    )

# Use PPO to learn a policy & evaluate it

model = PPO(config["policy_type"], env, batch_size=config["batch_size"], verbose=1, tensorboard_log="logs/PCCNs_SB3", device=args.device) # TODO: Use absolute sending rate instead of delta scaling
TIMESTEPS = config["total_timesteps"]
N_EVAL_EPISODES = 10

# Evaluate the untrained agent

mean_reward, std_reward = evaluate_policy(model, env, n_eval_episodes=N_EVAL_EPISODES)
log.debug(f"Untrained {experiment_name} -> mean_reward:{mean_reward:.2f} +/- {std_reward:.2f} over {N_EVAL_EPISODES} episodes")


models_dir = "models/"
# Add path if it doesn't exist
if not os.path.exists(models_dir):
    os.makedirs(models_dir)
    
if args.tracking:
    model_path = models_dir + experiment_name + "_" + args.reward_fun + "_" + str(TIMESTEPS) + "_" + run.id
else:
    model_path = models_dir + experiment_name + "_" + args.reward_fun + "_" + str(TIMESTEPS)+ datetime.now().strftime("%Y%m%d_%H%M%S")
    #TODO: Add a check to see if the model already exists and if so, add a suffix to the name
    #TODO: Change model path format to model_{TIMESTEPS}_{args.reward_fun}-a{args.reward_coefficients[0]} E.g.: model_4000000_custom_a5

print(f"Model path: {model_path} ")

if args.use_pretrained:
    try:
        trained_model = PPO.load(model_path)
        log.debug(f"Loaded model from {model_path}")
    except:
        log.debug(f"Failed to load model from {model_path}")
        args.use_pretrained = False
if not args.use_pretrained:   
    log.debug(f"Starting training for {config['total_timesteps']} timesteps")
    # print(f"Starting training for {args.timesteps} timesteps")
    if args.tracking:
        model.learn(
            total_timesteps=config["total_timesteps"],
            tb_log_name=experiment_name,
            progress_bar=True,
            callback=WandbCallback(
                gradient_save_freq=100,
                model_save_path=f"models/{run.id}",
                verbose=2
            )
            # callback=EvalCallback(
            #     env,
            #     n_eval_episodes=N_EVAL_EPISODES,
            #     eval_freq=10000,
            #     # deterministic=True,
            #     best_model_save_path=f"models/{experiment_name}_best"
            # )
        )
    else:
        model.learn(
            total_timesteps=config["total_timesteps"],
            tb_log_name=experiment_name,
            progress_bar=True,
        )
    # Save the model.
    model.save(model_path)
    log.debug(f"Saved model to {model_path}")

#     # Load the model.
#     trained_model = PPO.load(model_path)
# # Check if the best model was saved correctly
# best_model = PPO.load(f"{model_path}_best/best_model.zip")

# Evaluate the trained agent
trained_model = PPO.load(model_path)
mean_reward, std_reward = evaluate_policy(trained_model, env, n_eval_episodes=N_EVAL_EPISODES)
log.debug(f"Trained {experiment_name} -> mean_reward:{mean_reward:.2f} +/- {std_reward:.2f} over {N_EVAL_EPISODES} episodes")
# mean_reward, std_reward = evaluate_policy(best_model, env, n_eval_episodes=N_EVAL_EPISODES)
# log.debug(f"Best {experiment_name} -> mean_reward:{mean_reward:.2f} +/- {std_reward:.2f} over {N_EVAL_EPISODES} episodes")

#A quick eval of model to see range of values produced.
env = gym.make(args.env_name, reward_fun = args.reward_fun, reward_coefficients=(args.a,args.b,args.c))
env = Monitor(env)

# Examine the range of outputs from the model to see if they are reasonable
obs, _ = env.reset()
LEN = 4000 # 10 episodes
actions = np.zeros(LEN, dtype=np.float16)
for step in range(LEN):
    action, _states = trained_model.predict(obs, deterministic=True)
    obs, rewards, dones, truncated, info = env.step(action)
    actions[step] = action.item()
    if dones:
        obs, _ = env.reset()
log.debug(f"Actions: Min: {np.min(actions)}, Max: {np.max(actions)}")
log.debug(f"Mean: {np.mean(actions)}, Std: {np.std(actions)}")

# 
# Close the environment
env.close()
if args.tracking:
    run.finish()

log.debug("Experiment finished.")