from game import Gameplay, ActionOnClose


def start_game():
    game = Gameplay()
    game.run()
    action_on_close = game.get_action_on_close()
    if action_on_close == ActionOnClose.RESTART:
        start_game()


start_game()