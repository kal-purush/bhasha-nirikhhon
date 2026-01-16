from src.item import Item


class MixinKeyboard:
    def __init__(self):
        self.__language = "EN"
        MixinKeyboard.__language = self.__language

    def change_lang(self) -> None:
        self.__language = ("EN", "RU")[self.__language == "EN"]

    @property
    def language(self):
        return self.__language

    @language.setter
    def language(self, lang):
        raise AttributeError("property 'language' of 'Keyboard' object has no setter")


class Keyboard(Item, MixinKeyboard):
    pass