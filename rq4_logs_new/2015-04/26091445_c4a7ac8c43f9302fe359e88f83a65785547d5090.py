__author__ = 'Kaike'

class HouseState(object):
    def __init__(self, room, hour, users, light, temperature, curtain):
        self.room = room
        self.hour = hour
        self.users = users
        self.light = light
        self.temperature = temperature
        self.curtain = curtain

class HouseStateRule(object):
    def __init__(self, room, hour, user, light, temperature, curtain):
        self.room = room
        self.hour = hour
        self.user = user
        self.light = light
        self.temperature = temperature
        self.curtain = curtain



# rooms
#
#  office = 0
#  bedroom = 1

# users
#
#  nobody = 0
#  silveira = 1
#  carlos = 2
#  cesar = 3
#  guest = 4

#light
#  off = 0
#  on = 1

#temperature
#  off = 0

#curtain
#  off = 0
#  on = 1