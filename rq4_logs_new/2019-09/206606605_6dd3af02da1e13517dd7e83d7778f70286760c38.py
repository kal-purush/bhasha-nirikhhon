# Its an gecko

from animals import Animal
from organism_type import Forestful
from organism_type import Identifiable

class GoldGecko(Animal, Forestful, Crawling, Identifiable):

    def __init__(self):
        Animal.__init__(self, "Gold dust day gecko", 3, 2, {"Fly", "Mosquito", "Cricket", "Mealworm"})
        Forestful.__init__(self)
        Crawling.__init__(self)
        Identifiable.__init__(self)

    def feed(self, food):
        if food in self.__food:
            print(f'The gold dust day gecko ate a {food} for a meal')
        # TODO: Ask if we should keep or remove
        # else:
        #     print(f'The dolphin rejects the {prey}')

    def __str__(self):
        return f'Gold dust day gecko {self.id} was created!'