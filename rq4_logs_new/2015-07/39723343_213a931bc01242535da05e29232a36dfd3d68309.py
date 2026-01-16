#!/usr/bin/python3

import array
import sys


# Schedules are implemented as arrays of bit fields.
# Every traffic light has its own bit, with a 1 indicating the light being
# green. Thus, a bit field represents the state of all traffic lights at one
# point in time.
# (Note that "yellow" is just the end of the green phase for our purposes.)
# The schedule is thus an arrow of these fields, one for each point in time the
# schedule covers.


# time resolution
TR = 1 # schedule entries per second


# Bit shifts for each of the traffic lights
# (TLC_* .. traffic light for cars; TLP_* .. traffic light for pedestrians)
# (*_L .. left; *_SR .. straight/right)

TLC_EAST_SR     =  0
TLC_NORTH_SR    =  1
TLC_WEST_SR     =  2
TLC_SOUTH_SR    =  3

TLC_EAST_L  =  4
TLC_NORTH_L =  5
TLC_WEST_L  =  6
TLC_SOUTH_L =  7

TLP_EAST    =  8
TLP_NORTH   =  9
TLP_WEST    = 10
TLP_SOUTH   = 11

TLP_MIN_GREEN_TIME = [
    round(21.45 * TR + 0.5), # east:  21.45 s
    round(13.50 * TR + 0.5), # north: 13.50 s
    round(19.20 * TR + 0.5), # west:  19.20 s
    round(16.80 * TR + 0.5)  # south: 16.80 s
]


tl_conflict_mask = None
tl_config_valid  = None


def build_conflict_mask():
    global tl_conflict_mask
    tl_conflict_mask = 12 * [0]

    # Every straight/right TL:
    # - conflicts with every straight/right, except the opposite
    # - conflicts with every left turn, except its own
    # - conflicts with the opposite road's pedestrian TL
    # Every left turn TL:
    # - conflicts with every straight/right, except its own
    # - conflicts with every left turn, except the opposite
    # - conflicts with the target road's pedestrian TL

    # Straight/right car traffic lights
    for i in range(4):
        # conflict masks
        straight    = 0xf
        left        = 0xf
        pedestrian  = 0xf

        # Cannot conflict with itself
        straight &= ~(1 << i)
        # Does not conflict with opposite
        straight &= ~(1 << ((i + 2) % 4))

        # Does not conflict with own left turn
        left &= ~(1 << i)

        # Conflicts with opposite road's pedestrian TL
        pedestrian &= ~(1 << ((i + 2) % 4))

        tl_conflict_mask[i] =  straight \
                            | (left << 4) \
                            | (pedestrian << 8)

    # Left turn car traffic lights
    for i in range(4):
        straight    = 0xf
        left        = 0xf
        pedestrian  = 0xf

        # Does not conflict with own straight/right
        straight &= ~(1 << i)

        # Cannot conflict with itself
        left &= ~(1 << i)
        # Does not conflict with opposite
        left &= ~(1 << ((i + 2) % 4))

        # Does not conflict with the target road
        pedestrian &= ~(1 << ((i + 3) % 4))

        tl_conflict_mask[i + 4] =  straight \
                                | (left << 4) \
                                | (pedestrian << 8)

    # Pedestrian traffic lights
    for i in range(4):
        # They don't conflict with each other; so just collect the car TL
        # conflicts from what we've built already
        for j in range(8):
            if tl_conflict_mask[j] & (1 << (i + 8)):
                tl_conflict_mask[i + 8] |= 1 << j

    # Sanity check: Check symmetry
    for i in range(12):
        for j in range(12):
            if ((tl_conflict_mask[i] >> j) & 1) != ((tl_conflict_mask[j] >> i) & 1):
                print("Error: Asymmetric conflict mask,", i, "and", j, file=sys.stderr)
                print(tl_conflict_mask, file=sys.stderr)
                print(bin(tl_conflict_mask[i]), "vs.", bin(tl_conflict_mask[j]), file=sys.stderr)
                exit(1)

    # Build tl_config_valid
    global tl_config_valid
    tl_config_valid = array.array('B')

    for i in range(2 ** 12):
        valid = True
        for j in range(12):
            if i & (1 << j) and i & tl_conflict_mask[j]:
                valid = False
                break
        tl_config_valid.append(valid)


class Schedule:
    def __init__(self, seconds):
        self.data = array.array('h')
        for i in range(seconds * TR):
            self.data.append((1 << TLP_EAST) | (1 << TLP_NORTH)
                           | (1 << TLP_WEST) | (1 << TLP_SOUTH))

    def length(self):
        return self.data.buffer_info()[1]

    def initial_constraints(self):
        # c) Between switching of green phases consider 3 sec of red phase for
        #    all directions.
        # d) Consider a maximum waiting time of 2 min always

        # TODO: Start with the bus/tram schedule. Maybe that's enough.

        # XXX: This assumes the best strategy is having the following basic
        # schedule:
        # - east+west straight/right
        # - east+west left
        # - north+south straight/right
        # - north+south left
        # XXX: Also, it's really bad.
        for i in range(0, self.length(), 120 * TR):
            for offset in range(0, 120 * TR, 60 * TR):
                tl = offset // (60 * TR)
                # d)
                self.make_green(i + offset, tl)
                self.make_green(i + offset, tl + 2)
                self.make_green(i + offset + 30 * TR, tl + 4)
                self.make_green(i + offset + 30 * TR, tl + 6)
                # c) -- switch off pedestrian lights
                for j in range(1, 3 * TR + 1):
                    self.data[i + offset + j] &= 0xff
                    self.data[i + offset + 30 * TR - j] &= 0xff
                    self.data[i + offset + 30 * TR + j] &= 0xff
                    self.data[i + offset + 60 * TR - j] &= 0xff

        if not self.satisfied(True):
            print("Error: Initial schedule does not satisfy contraints", file=sys.stderr)
            exit(1)

    def make_green(self, second, traffic_light):
        # Make it green
        self.data[second] |= (1 << traffic_light)
        # Make all conflicting lights red
        self.data[second] &= ~tl_conflict_mask[traffic_light]

    def satisfied(self, verbose=False):
        # a) Pedestrian lights need a minimal time of green phase
        # Note: We don't have to care about the red phase for pedestrian TLs;
        # just make the final ten seconds of their green phase red. It's the
        # same thing.
        green_time = 4 * [0]
        for i in range(self.length()):
            configuration = self.data[i]
            for j in range(4):
                if configuration & (1 << (j + 8)):
                    green_time[j] += 1
                elif green_time[j] > 0 and green_time[j] < TLP_MIN_GREEN_TIME[j]:
                    if verbose:
                        print("Error: Schedule does not satisfy constraint A", file=sys.stderr)
                    return False

        # 0) Two conflicting directions cannot be green at the same time
        # b) It is not allowed to have green lights for pedestrians when the
        #    cars can cross their way, except if both go in the same direction.
        for i in range(self.length()):
            if not tl_config_valid[self.data[i]]:
                if verbose:
                    print("Error: Schedule does not satisfy contraints 0/B", file=sys.stderr)
                return False

        # c) Between switching of green phases consider 3 sec of red phase for
        #    all directions.
        # (Note: This breaks condition "there needs to be a green light
        #  anywhere", so we'll just ignore that one)
        last_configuration = None
        first_totally_red = None
        for i in range(self.length()):
            configuration = self.data[i]
            if last_configuration is not None and configuration != last_configuration:
                if last_configuration:
                    if configuration:
                        if verbose:
                            print("Error: Schedule does not satisfy constraint C", file=sys.stderr)
                            print("(Immediate green to green switch)", file=sys.stderr)
                        return False
                    first_totally_red = i
                else:
                    if i - first_totally_red < 3 * TR:
                        if verbose:
                            print("Error: Schedule does not satisfy constraint C", file=sys.stderr)
                            print("(Intermediate red phase too short)", file=sys.stderr)
                        return False
            last_configuration = configuration

        # d) Consider a maximum waiting time of 2 min always
        red_time = 12 * [0]
        for i in range(self.length()):
            configuration = self.data[i]
            for j in range(8):
                if configuration & (1 << j):
                    red_time[j] = 0
                else:
                    red_time[j] += 1
                    if red_time[j] > 120 * TR:
                        if verbose:
                            print("Error: Schedule does not satisfy contraint D", file=sys.stderr)
                        return False

        # e) The Buses and Trams should always pass when they are scheduled
        # (TODO)

        return True


build_conflict_mask()


rush = Schedule(7200)

rush.initial_constraints()