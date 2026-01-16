class equipable_items:
    def __init__shortsword(attack,speed,defence,magic_power,special_ability,printout):
        self.attack = 5
        self.speed = 3
        self.defence = 0
        self.magic_power = 0
        self.special_ability = 0
        self.printout = shortsword_printout()

    def __init__longsword(attack,speed,defence,magic_power,special_ability,printout):
        self.attack = 10
        self.speed = 2
        self.defence = 0
        self.magic_power = 0
        self.special_ability = 0
        self.printout = longsword_printout()

    def __init__wooden_staff(attack,speed,defence,magic_power,special_ability,printout):
        self.attack = 2
        self.speed = 1
        self.magic_power = 2
        self.special_ability = 0
        self.printout = wooden_staff_printout()

    def __init__dagger(attack,speed,defence,magic_power,special_ability,printout):
        self.attack = 3
        self.speed = 5
        self.magic_power = 0
        self.special_ability = 0
        self.printout = dagger_printout()