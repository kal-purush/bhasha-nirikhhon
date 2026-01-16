import hlt
from hlt import constants
from hlt.positionals import Position
from hlt.game_map import GameMap, Player, MapCell
from hlt.entity import Ship
from modes import Modes


class Navigation:
    def __init__(self, game):
        self.game = game
        self.game_map = game.game_map
        self.player = game.me
        self.ships = game.me.get_ships()
        self.bases = [game.me.shipyard] + game.me.get_dropoffs()
        self.ship_states = {}  # {ship.id: ShipState()}
        self.top_clusters = None
        self.command_queue = []
        self.PROD_STOP_TURN = 220

        # TODO: make cluster map a prop of navi class

    def farthest_ship_distance(self) -> int:
        bases = self.bases
        ships = self.ships
        if ships and bases:
            max_distances = []
            for base in bases:
                distances = [
                    self.game_map.calculate_distance(ship.position,
                                                     base.position)
                    for ship in ships
                ]
                max_distance = max(distances)
                max_distances.append(max_distance)

            return max(max_distances)
        else:
            return 0

    def _initialize_ship_states(self):
        """initialize all newly created ships with a default ship state"""
        for ship in self.ships:
            if ship.id not in self.ship_states:
                state = ShipState(Modes.collecting)
                self.ship_states[ship.id] = state

    def start_positions(self):
        staring_positions = self.player.shipyard.position.get_surrounding_cardinals(
        )
        return staring_positions

    def can_afford_move(self, ship: Ship):
        location_halite = self.game_map[ship.position].halite_amount
        move_cost = location_halite * 0.1
        return ship.halite_amount >= move_cost

    def low_hal_location(self, ship: Ship):
        return self.game_map[ship.
                             position].halite_amount < constants.MAX_HALITE * 0.1

    def should_move(self, ship: Ship):
        return self.can_afford_move(ship) and self.low_hal_location(ship)

    def update(self, game: hlt.Game):
        """update all game data and reset turn specific info"""
        self.game_map = game.game_map
        self.player = game.me
        self.ships = game.me.get_ships()
        self.command_queue = []
        self._initialize_ship_states()
        self._reset_current_moves()

    def select_move_hueristic(self, ship: Ship, destination: Position = None):
        surrounding_positions = ship.position.get_surrounding_cardinals()
        if destination:
            # should trigger recalc of map here is this evals to none
            direction_of_cluster = self.game_map._get_target_direction(
                ship.position, destination)
            best_positions = [
                ship.position.directional_offset(direction)
                for direction in direction_of_cluster if direction is not None
            ]
            if best_positions is not None:
                surrounding_positions = best_positions
        current_position = ship.position
        halite_locations = {(position): self.game_map[position].halite_amount
                            for position in surrounding_positions}
        halite_locations[current_position] = self.game_map[
            current_position].halite_amount * 1.8
        new_position = max(halite_locations, key=halite_locations.get)
        return new_position  #

    def select_destination_richness(self, ship: Ship, top_clusters):
        # get closest cluster to ship
        closest_cluster = None
        distance_to_cluster = self.game_map.width
        for cluster in top_clusters:
            distance = self.game_map.calculate_distance(
                ship.position, cluster.position)
            if distance < distance_to_cluster:
                distance_to_cluster = distance
                closest_cluster = cluster
        return closest_cluster.rich_position

    def unstuck(self, ship: Ship):
        pass

    def leave_dropoff(self, dropoff: Position):
        pass

    def dropoff_surrounded(self, dropoff: Position):
        if self.game_map[dropoff].is_occupied:
            adjacent_cells = dropoff.get_surrounding_cardinals()
            adj_cells_occupied = [
                self.game_map[cell].is_occupied for cell in adjacent_cells
            ]
            if adj_cells_occupied.count(True) == 4:
                return True
        return False

    def richest_clusters(self, top_n: int = 5):
        """returns a list of top clusters"""
        all_halite = {
            cell.position: Cluster(self.game_map, cell).halite_amount
            for cell in self.game_map
        }
        sorted_cells = sorted(all_halite, key=all_halite.get, reverse=True)
        sorted_cells = sorted_cells[:top_n]
        top_clusters = [
            Cluster(self.game_map, self.game_map[cell])
            for cell in sorted_cells
        ]
        self.top_clusters = top_clusters

    def _reset_current_moves(self):
        [ship.reset_moves() for ship in self.ship_states.values()]

    def command(self, command):
        self.command_queue.append(command)

    def can_produce(self):
        return (self.game.turn_number <= self.PROD_STOP_TURN
                and self.player.halite_amount >= constants.SHIP_COST
                and not self.game_map[self.player.shipyard].is_occupied)


class Cluster:
    def __init__(self, game_map: GameMap, center_cell: MapCell):
        self.game_map = game_map
        self.cluster_center = center_cell
        self.cluster = [center_cell]
        for x in range(-1, 2):
            for y in range(-1, 2):
                try:
                    cell = self.game_map[
                        self.cluster_center.position.directional_offset((x,
                                                                         y))]
                    self.cluster.append(cell)
                except Exception:
                    logging.warn(f"can't find cell at {(x,y)}")

    @property
    def position(self):
        return self.cluster_center.position

    @property
    def rich_position(self):
        clusters = {
            cluster.position: cluster.halite_amount
            for cluster in self.cluster
        }
        high_val_pos = max(clusters, key=clusters.get)
        return high_val_pos

    @property
    def halite_amount(self):
        """
        :return: total halite amount in 9 cell cluster
        """
        return (sum([cell.halite_amount for cell in self.cluster]))

    def __repr__(self):
        return f"Cluster {self.cluster_center}, halite: {self.halite_amount}"


class ShipState:
    def __init__(self, mode, destination=None, prev_move=None):
        self.mode = mode
        self.prev_move = prev_move
        self.current_move = None
        self.destination = destination

    def reset_moves(self):
        self.current_move = None

    def __repr__(self):
        return f"ShipState(mode: {self.mode}, prev_move:{self.prev_move}"