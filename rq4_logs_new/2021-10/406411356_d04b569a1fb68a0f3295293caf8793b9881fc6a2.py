class Transport:
    def __init__(self) -> None:
        self.type = None
        self.can_drive = False
        self.can_flay = False
        self.can_swim = False

    def print_info(self):
        return type(self).__name__ + ': Type - {}'.format(self.type)


class Car(Transport):
    def __init__(self):
        self.type = 'Car'
        self.can_drive = True
        self.number_of_wheels = 4
        
    def print_info(self):
        return super().print_info() + ' Can drive - {}, Have {} wheels'.format(self.can_drive, self.number_of_wheels)


class Airplan(Transport):
    def __init__(self):
        self.type = 'Airplan'
        self.can_flay = True
        self.length_of_wings = 25

    def print_info(self):
        return super().print_info() + ' Can fly - {}, Length of wings = {} '.format(self.can_flay, self.length_of_wings)


class Ship(Transport):
    def __init__(self):
        self.type = 'Ship'
        self.can_swim = True
        self.displacement = 1500

    def print_info(self):
        return super().print_info() + ' Can swim - {}, Displacement = {} '.format(self.can_swim, self.displacement)


class main:
    car_audi = Car()
    airplan_boing = Airplan()
    ship_fregat = Ship()

    print(car_audi.print_info())
    print(airplan_boing.print_info())
    print(ship_fregat.print_info())


if __name__ == '__main__':
    main()
    