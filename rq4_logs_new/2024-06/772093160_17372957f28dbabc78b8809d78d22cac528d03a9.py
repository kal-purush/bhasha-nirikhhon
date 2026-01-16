from NEAT.neat import NEAT
import gymnasium as gym
import numpy as np

model = NEAT(
        inputSize=128,
        outputSize=6,
        populationSize=10,
        C1=1.2,
        C2=2.3,
        C3=3.5,
)

model.load_genomes("save_ep_50")
print(model.best_genome.fitness)

env = gym.make("SpaceInvaders-ramDeterministic-v4",render_mode="human")

state, info = env.reset()
done = False
truncated = False  # Inicializar la variable truncated
score = 0

while not done and not truncated:
    state = {i: state[i] for i in range(len(state))}
    actions = model.test(state)
    final_action = np.argmax(actions)
    #print(final_action)
    n_state, reward, done, truncated, info = env.step(final_action)
    score += reward
    state = n_state