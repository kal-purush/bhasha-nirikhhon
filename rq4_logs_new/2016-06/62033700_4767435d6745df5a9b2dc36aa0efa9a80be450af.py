class Rocket(object):
    def __init__(self, rocket_type, fuel_lev, launch_num):
        self.rocket_type = rocket_type
        self.fuel_lev = fuel_lev
        self.launch_num = launch_num
    def launch(self):
        if rocket_type == 'falcon1':
            self.fuel_lev -= 1
        elif rocket_type == 'falcon9':
            self.fuel -= 1
            self.launches += 1
    def refill(self):
        if self.rocket_type == 'falcon1':
            used_fuel = 5 - self.fuel_lev
            self.fuel_lev = 5
            return used_fuel
        elif self.rocket_type == 'falcon9':
            used_fuel = 20 - self.fuel_lev
            self.fuel_lev = 20
            return used_fuel
    def getStats(self):
        return 'Name: {}, Fuel: {}'.format(self.rocket_type, self.fuel_lev)

class SpaceX(object):
    def __init__(self, stored_fuel):
        self.stored_fuel = stored_fuel
        self.stored_rockets = 0
        self.launches = 1
    def addRocket(self, rocket):
        self.stored_rockets += 1
    def refill_all(self):
        self.stored_fuel = self.stored_fuel - falcon1.refill() - falcon9.refill() - returned_falcon9.refill()
    def launch_all(self):
        self.launches += self.stored_rockets
    def buy_fuel(self, amount):
        self.stored_fuel += amount
    def getStats(self):
         return 'Rocket: {}, Fuel: {}, launches: {}'.format(self.stored_rockets, self.stored_fuel, self.launches)

#
# space_x = SpaceX(100)
# falcon1 = Rocket('falcon1', 0, 0)
# falcon9 = Rocket('falcon9', 0, 0)
# returned_falcon9 = Rocket('falcon9', 11, 1)
#
# print(returned_falcon9.getStats()) # name: falcon9, fuel: 11
#
# space_x.addRocket(falcon1)
# space_x.addRocket(falcon9)
# space_x.addRocket(returned_falcon9)
#
# print(space_x.getStats()) # rockets: 3, fuel: 100, launches: 1
# space_x.refill_all()
# print(space_x.getStats()) # rockets: 3, fuel: 66, launches: 1
# space_x.launch_all()
# print(space_x.getStats()) # rockets: 3, fuel: 66, launches: 4
# space_x.buy_fuel(50)
# print(space_x.getStats()) # rockets: 3, fuel: 116, launches: 4