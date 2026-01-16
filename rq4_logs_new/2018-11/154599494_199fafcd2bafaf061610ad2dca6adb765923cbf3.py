#!/usr/bin/env python3
# Python 3.6
import hlt
from hlt import constants
from hlt.positionals import Direction
import logging
from modes_v10 import Modes
from navigation_v10 import Navigation

# This game object contains the initial game state.
game = hlt.Game()

# initialize constants
MAX_HALITE = constants.MAX_HALITE
MAX_TURNS = constants.MAX_TURNS
HALITE_THRESHOLD = MAX_HALITE * 0.1
SHIP_FULL = MAX_HALITE * 0.95
directions = [
    Direction.North, Direction.South, Direction.East, Direction.West,
    Direction.Still
]

# official game start
game.ready("Snowcola_v10")
# log bot id to log
logging.info("Successfully created bot! My Player ID is {}.".format(
    game.my_id))

# initialize map and starting cluster map
nav = Navigation(game)

#                 #
# Start Game Loop #
#                 #

while True:
    # get latest gamestate info from the halite engine
    game.update_frame()
    game_map = game.game_map
    me = game.me
    ships = me.get_ships()

    # update bot data
    nav.update(game)

    for ship in ships:
        state = nav.state(ship)

        #                   #
        # set destinations  #
        #                   #
        if nav.game_mode is Modes.ENDGAME:
            # ensure ships are going to dropoff in engame stage
            nav.go_home(ship)

        elif state.mode is Modes.COLLECTING:
            # TODO: need method detect and unstick to get ship unstuck
            if ship.halite_amount >= SHIP_FULL and state.destination:
                # go to nearest dropoff point
                nav.go_home(ship)

            elif not state.destination or nav.dest_halite(
                    ship) < HALITE_THRESHOLD:
                # go to nearest high value halite cluster
                nav.select_rich_destination(ship)

        elif state.mode is Modes.DEPOSITING:
            if nav.deposit_complete(ship):
                # go to nearest high value halite cluster and
                # switch to collection mode
                nav.select_rich_destination(ship)
                nav.set_mode(ship, Modes.COLLECTING)

            elif not nav.going_home(ship) or not state.destination:
                nav.go_home(ship)

        #                #
        # calculate move #
        #                #

        # special case to ignore collisions at end game
        if nav.game_mode is Modes.ENDGAME:
            if nav.adjacent_dest(ship):
                nav.kamikaze(ship)
            else:
                nav.navigate_bline(ship)
            # TODO: only invovke this when right next to the dropoff

        # handout nav instructions
        elif state.mode is Modes.COLLECTING:
            if nav.on_dropoff(ship):
                nav.leave_dropoff(ship)

            elif nav.should_move(ship):
                nav.navigate_max_halite(ship)

            else:
                nav.stay_still(ship)

        elif state.mode is Modes.DEPOSITING:
            nav.navigate_bline(ship)
        else:
            nav.stay_still(ship)

    if nav.can_produce():
        #add class method for this
        nav.command(me.shipyard, me.shipyard.spawn())

    nav.report_game_state()
    # Send your moves back to the game environment, ending this turn.
    game.end_turn(nav.command_queue)