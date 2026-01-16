import string


class InputValidator:

    def is_move_valid(self, input_move, board):
        if not self.__contains_invalid_character(input_move) and input_move.isdigit():
            player_input = int(input_move) - 1
            return board.valid_move(player_input)
        else:
            return False

    def __contains_invalid_character(self, input_move):
        return self.__is_alpha(input_move) or self.__is_white_space(input_move) or self.__is_special_character(input_move) or self.__contains_space(input_move) or self.__is_empty_space(input_move)

    def __is_alpha(self, input_move):
        return input_move.isalpha()

    def __is_white_space(self, input_move):
        return input_move.isspace()

    def __is_special_character(self, input_move):
        return input_move in string.punctuation

    def __contains_space(self, input_move):
        return " " in input_move

    def __is_empty_space(self, input_move):
        return input_move == ""