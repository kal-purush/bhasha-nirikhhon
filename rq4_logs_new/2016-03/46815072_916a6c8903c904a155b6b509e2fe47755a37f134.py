from french_messenger import FrenchMessenger
from english_messenger import EnglishMessenger


class GameInterface:
    def __init__(self, io):
        self.io = io
        self.game_interface = EnglishMessenger(self.io)
        self.create_language()

    def get_game_interface(self):
        return self.game_interface

    def create_language(self):
        self.game_interface.display_languages()
        self.set_language(self.get_language_selected())

    def get_language_selected(self):
        language = self.game_interface.get_language_selected()

        if language > 0 and language < 3:
            return language
        else:
            return self.get_language_selected()

    def set_language(self, language):
        if language == 1:
            self.game_interface = EnglishMessenger(self.io)
        elif language == 2:
            self.game_interface = FrenchMessenger(self.io)