class Animal:
    def __init__(self):
        self.can_run = False
        self.can_flay = False


    def print_adilities(self):
        print(type(self).__name__ + ':')
        print('Can run:', self.can_run)
        print('Clan fly:', self.can_flay)
        print()


class Horse(Animal):
    def __init__(self):
        super().__init__()
        self.can_run = True


class Bird(Animal):
    def __init__(self):
        super().__init__()
        self.can_flay = True


class Pegasus(Horse, Bird):
    pass


class main:
    horse = Horse()
    horse.print_adilities()

    bird = Bird()
    bird.print_adilities()

    pegasus = Pegasus()
    pegasus.print_adilities()


if __name__ == '__main__':
    main()    
    