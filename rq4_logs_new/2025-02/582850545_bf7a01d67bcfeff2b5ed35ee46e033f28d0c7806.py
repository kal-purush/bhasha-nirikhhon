# tests/test_multiagent_env.py
import pytest
import numpy as np
from zombpyg.multiagent_env import MultiagentZombpygEnv


@pytest.fixture(scope="function", name="env1p")
def env1p_fixture():
    env = MultiagentZombpygEnv(
        map_id="demo",
        rules_id="extermination",
        initial_zombies=1,
        minimum_zombies=0, 
        agent_ids = [0],
        agent_weapons="rifle",
        player_specs="",
        enable_rendering=False,
        verbose=False
    )
    return env


@pytest.fixture(scope="function", name="env2p")
def env2p_fixture():
    env = MultiagentZombpygEnv(
        map_id="demo",
        rules_id="extermination",
        initial_zombies=1,
        minimum_zombies=0, 
        agent_ids = [0, 1],
        agent_weapons="random",
        player_specs="",
        enable_rendering=False,
        verbose=False
    )
    return env


def test_multiagent_env_shape(env1p):
    observations, _ = env1p.reset()
    feedback_size = env1p.game.agent_builder.get_feedback_size()
    for agent_id in observations:
        observation = observations[agent_id]
        assert observation.shape == (1,feedback_size,1)
        assert np.all(observation <= 2.0)


def test_multiagent_1pgame(env2p):
    _, _ = env2p.reset()
    stepcount = 0
    while True:
        _, _, done, truncated, _ = env2p.step({
            agent_id: env2p.action_spaces[agent_id].sample()
            for agent_id in env2p.agents
        })

        if done or truncated or (stepcount >=10):
            break

        stepcount += 1

    assert True
