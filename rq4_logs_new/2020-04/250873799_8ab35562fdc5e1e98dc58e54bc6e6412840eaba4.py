import config
from Agent import Agent
def GeneticRecombination(Agents):
    print("GeneticRecombination")

    male_agents = list(filter(lambda obj: obj.gender == "male", Agents))
    male_agents.sort(key=lambda obj: sum(obj.rewards), reverse=True)

    female_agents = list(filter(lambda obj: obj.gender == "female", Agents))
    female_agents.sort(key=lambda obj: sum(obj.rewards), reverse=True)


    male_minimun_reward = sum(male_agents[-1].rewards)
    female_minimun_reward = sum(female_agents[-1].rewards)

    return Agents[-1].DNAPolicy