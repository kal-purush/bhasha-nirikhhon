import copy
import math
import threading

import config
import game
import utils


def possible_moves(game_state: game.Game):
    return [move for move in range(config.Dimensions.COLS) if not game_state.is_col_full(move)]


def min_max(game_state: game.Game, maximizing_player, depth):
    is_maximizing = maximizing_player == game_state.current_player

    if depth <= 0:
        value = board_evaluation(game_state)
        value = value if value < 4 else math.inf
        return value if is_maximizing else -value

    if game_state.winner:
        return math.inf if is_maximizing else -math.inf

    if game_state.is_board_full():
        return 0

    scores = []
    for c in possible_moves(game_state):
        # copy_state = copy.deepcopy(game_state)
        game_state.play(c)
        scores.append(min_max(game_state, maximizing_player, depth - 1))
        game_state.undo_play()

    return max(scores) if is_maximizing else min(scores)


def exec_best_move(game_state: game.Game):
    best_score = -math.inf
    best_move = None
    maximizing_player = game_state.current_player
    for c in possible_moves(game_state):
        copy_state = copy.deepcopy(game_state)
        copy_state.play(c)
        score = min_max(copy_state, maximizing_player, 5)
        if score > best_score:
            best_score = score
            best_move = c

    if best_move is None:
        game_state.play(possible_moves(game_state)[0])

    game_state.play(best_move)


def think(game_state: game.Game):
    threading.Thread


def board_evaluation(state: game.Game):
    for i in range(4)[::-1]:
        sub_list = [state.current_player for _ in range(i)]
        if utils.is_sublist(state.board, sub_list):
            return i
        if utils.is_sublist(utils.get_columns(state.board), sub_list):
            return i
        if utils.is_sublist(utils.diagonals(state.board), sub_list):
            return i
    return 0