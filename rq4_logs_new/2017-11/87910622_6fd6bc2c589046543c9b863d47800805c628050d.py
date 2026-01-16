from numpy.random import choice


class Dice(object):

    def __init__(self, faces=[1, 2, 3, 4, 5, 6], weight=None):
        if weight is None:
            weight = [1 for _ in range(len(faces))]
        self.faces = faces
        if len(weight) != len(faces):
            raise ValueError("Length of weight must be the same as the amount of faces!")
        # save given weight, since that is more readable than the normalised weight
        self.readable_weight = weight
        self.weight = weight
        self.is_fair = all([x == self.weight[0] for x in self.weight])
        # normalise the weights
        sum_weight = sum(self.weight)
        for i, val in enumerate(self.weight):
            self.weight[i] = val / sum_weight

    def roll(self, times=1):
        rolls = list()
        for _ in range(times):
            rolls.append(choice(self.faces, p=self.weight))
        return rolls

    def is_fair(self):
        return all([x == self.weight[0] for x in self.weight])

    def get_weight(self):
        return self.weight

    def change_weight(self, weight=None):
        if len(weight) == len(self.faces):
            self.readable_weight = weight
            sum_weight = sum(weight)
            for i, val in enumerate(weight):
                self.weight[i] = val / sum_weight
        else:
            raise ValueError("Length of weight must be the same as the amount of faces!")

    def __str__(self):
        return "Dice, faces: {}.".format(self.faces)

    def __repr__(self):
        return "Dice, faces: {}, weight: {}".format(self.faces, self.weight)