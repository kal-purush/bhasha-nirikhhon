from SPlayer import *
from Location import *

class Board():
    ep_transformation = [5, 4, 7, 6, 1, 0, 3, 2]
    effective_size = 6
    board_size = effective_size + 2 # for phantom tiles

    def __init__(self):
        self.num_tiles = 0
        self.tiles_on_board = []
        self.matrix_tile = [[None for i in range (self.board_size)] for i in range (self.board_size)]   # 8*8 (6 + 2 starting points)
        self.matrix_player = [[[None for i in range(8)] for i in range (self.board_size)] for i in range (self.board_size)] # 6*6 * [0..7]


    # public methods

    def get_num(self):
        return self.num_tiles

    # return bool, whether the move is legal
    def legal_play(self, new_tile, splayer):
        res = True
        if self.mock_walk_player(new_tile, splayer) == True:
            for tile in splayer.get_hand().enumerate_tiles(new_tile):
                if self.mock_walk_player(tile, splayer) == False:
                    res = False
                    break
        return res

    # return true if it kills player, false otherwise
    def mock_walk_player(self, new_tile, splayer):
        tile_loc = splayer.get_empty_spot()
        self.place_tile(new_tile, tile_loc)

        _, killed = self.walk_player(splayer)

        self.remove_tile(tile_loc)
        return killed

    def walk_player(self, splayer):
        cur_loc = splayer.get_location()
        over = False
        while not over:
            cur_loc, over, killed = self.walk(cur_loc)

        return cur_loc, killed

    def has_tile(self, tile):
        return tile in self.tiles_on_board

    def place_tile(self, new_tile, tile_loc):
        row, col = tile_loc.into_tuple()
        if not self.matrix_tile[row][col]: # check if the slot is empty
            self.matrix_tile[row][col] = new_tile
            self.tiles_on_board.append(new_tile)
            self.num_tiles += 1
        else:
            raise Exception("Slot is not empty when trying to place")

    def remove_tile(self, tile_loc):
        Tile_location.check_tile_loc(tile_loc)
        row, col = tile_loc.into_tuple()
        if not self.matrix_tile[row][col]: # check if the slot is empty
            raise Exception("Slot is empty when trying to remove")
        else:
            tile = self.matrix_tile[row][col]
            self.matrix_tile[row][col] = None
            self.tiles_on_board.remove(tile)
            self.num_tiles -= 1

    def add_player(self, splayer):
        row, col, ep = splayer.get_location().into_tuple()
        self.matrix_player[row][col][ep] = splayer

    def remove_player(self, splayer):
        row, col, ep = splayer.get_location().into_tuple()
        self.matrix_player[row][col][ep] = None

    def move_player(self, splayer, new_loc):
        self.remove_player(splayer)
        splayer.set_location(new_loc)
        self.add_player(splayer)

    def get_tile_by_loc(self,tile_loc):
        Tile_location.check_tile_loc(tile_loc)
        row, col = tile_loc.into_tuple()
        return self.matrix_tile[row][col]

    def get_splayer_by_loc(self,splayer_loc):
        row, col, ep = splayer_loc.into_tuple()
        return self.matrix_player[row][col][ep]


    # private methods

    # move the player by one tile
    # returns updated location, over?, and killed?
    def walk(self, cur_loc):
        row, col, ep = cur_loc.into_tuple()

        # reaches to the edge, over and killed
        if cur_loc.out_of_bound():
            return cur_loc, True, True

        tile_r, tile_c = self.get_next_tile_loc((row, col), ep)

        # check next tile
        if not self.matrix_tile[tile_r][tile_c]:
            # empty, over and not killed
            return cur_loc, True, False
        else:
            # not empty, not over and not killed
            entry_ep = self.compute_entry(ep)
            # update player's location
            next_tile = self.get_tile_by_loc(Tile_location((tile_r,tile_c)))

            exit_ep = next_tile.walk_path(entry_ep)
            cur_loc = Player_location((tile_r, tile_c, exit_ep))
            return cur_loc, False, False

    # return (row, col)
    # assumes the current location is not on the edge
    def get_next_tile_loc(self, tile_loc, ep):
        cur_facing = ep // 2
        if not (0 <= cur_facing <= 3):
            raise Exception("cur_facing is invalid, cur_facing = " + str(cur_facing))

        row, col = tile_loc
        if cur_facing == 0: row -= 1
        if cur_facing == 1: col += 1
        if cur_facing == 2: row += 1
        if cur_facing == 3: col -= 1

        return row, col

    def compute_entry(self, ep):
        if not (0 <= ep <= 7):
            raise Exception("ep is invalid, ep = " + str(ep))
        return self.ep_transformation[ep]