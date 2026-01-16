class FrenchMessenger():
    def __init__(self, io):
        self.io = io

    def display_type_game(self):
        self.io.display_text("Type de jeu :\n\n"
                             "1 - Humain v. Humain\n"
                             "2 - Humain v. Ordinateur\n"
                             "3 - Ordinateur v. Ordinateur\n"
                            )

    def spot_not_free(self):
        self.io.display_text("la case doit etre vide")

    def display_range_board(self, min_range, max_range):
        self.io.display_text("position entre {min_range} et {max_range}".format(
            min_range=str(min_range), max_range=str(max_range -1)))

    def get_type_game_selected(self):
        return self.io.user_input("Selectionner le type de jeux : ")

    def display_which_start(self):
        self.io.display_text("Quel joueur commence :\n\n"
                             "1 - Joueur 1\n"
                             "2 - Joueur 2\n"
                            )

    def get_first_player(self):
        return self.io.user_input("Quel joueur doit commencer ? ")

    def display_player_turn(self, player_mark):
        self.io.display_text("Joueur {mark} tour".format(
            mark=str(player_mark)))

    def get_player_move(self):
        return self.io.user_input("Quel position : ")

    def display_board(self, board):
        self.io.display_board(board)

    def display_winner(self, winne_mark):
        self.io.display_text("Joueur {mark} gagne la partie".format(
            mark=str(winne_mark)))

    def display_tie(self):
        self.io.display_text("Match nul, personne ne gagne !")