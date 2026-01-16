import numpy
from zombpyg.core.things import CircularThing
from zombpyg.core.resources import MedicalSupplyResource, AmmoSupplyResource


class Checkpoint(CircularThing):
    def __init__(
        self, x, y, r, 
    ):
        # Note the life for CircularThing is set to 0
        super(Checkpoint, self).__init__(
            x, y, r,
            'checkpoint', 'white', 1,
        )
        self.fighters_checked_in = []

    def reset(self):
        self.life = 1
        self.fighters_checked_in.clear()
    
    def all_agents_checked_in(self, agents):
        return all([agent.agent_id in self.fighters_checked_in for agent in agents])

    def check_in(self, agent):
        if agent.agent_id not in self.fighters_checked_in:
            self.fighters_checked_in.append(agent.agent_id)
            agent.checkpoints_reached += 1
            return True
        return False

    def disable(self):
        self.life = 0
