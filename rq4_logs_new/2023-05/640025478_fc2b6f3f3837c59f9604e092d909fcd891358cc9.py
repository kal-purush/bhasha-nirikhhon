from src.item import Item


class Language:
    """ Класс-миксин, реализует дополнительный функционал по хранению и изменению раскладки клавиатуры """
    def __init__(self, language: str = "EN") -> None:
        """ Инициализация атрибута класса """
        self.__language = language

    @property
    def language(self) -> str:
        """ Возвращает язык раскладки клавиатуры """
        return self.__language

    def change_lang(self) -> 'Language':
        """ Меняет язык раскладки клавиатуры """

        if self.__language == "EN":
            self.__language = "RU"
        else:
            self.__language = "EN"
        return self


class KeyBoard(Item, Language):
    """ Класс для представления клавиатуры в магазине. """
    def __init__(self, name: str, price: float, quantity: int) -> None:
        """ Инициализация атрибутов класса """
        super().__init__(name, price, quantity)
        Language.__init__(self)