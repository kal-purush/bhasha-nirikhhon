from .habitat import IContainsAnimals
from .habitat import IContainsPlants


class Biome:

    def __init__(self, name, max_animals, max_plants):
        self.name = name
        self.max_animals = max_animals
        self.max_plants = max_plants

    def exceed_max(self, what_organism):
        try:
            if what_organism.food != "":
                if self.animals == self.max_animals:
                    self.animals.append(what_organism)
            else:
                if self.plants == self.max_plants:
                    self.plants.append(what_organism)
        except AttributeError:
            raise AttributeError("Cannot add organism to Biome")