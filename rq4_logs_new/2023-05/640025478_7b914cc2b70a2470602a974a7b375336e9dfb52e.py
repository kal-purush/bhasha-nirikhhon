from src.item import Item


class Phone(Item):
    """
    Класс для представления телефона в магазине.
    """
    def __init__(self, name: str, price: float, quantity: int, number_of_sim: int) -> None:
        """
        Создание экземпляра класса Phone.

        :param name: Название телефона.
        :param price: Цена за единицу телефона.
        :param quantity: Количество телефонов в магазине.
        :param number_of_sim: Количество поддерживаемых сим-карт в телефоне.
        """
        super().__init__(name, price, quantity)
        self.__number_of_sim = number_of_sim

    def __repr__(self) -> str:
        """
        Возвращает информацию об объекте: название класса(атрибуты экземпляра)
        """
        return f"{self.__class__.__name__}('{self.name}', {self.price}, {self.quantity}, {self.number_of_sim})"

    @property
    def number_of_sim(self) -> int:
        """
        Возвращает количество поддерживаемых сим-карт в телефоне
        """
        return self.__number_of_sim

    @number_of_sim.setter
    def number_of_sim(self, new_number_of_sim: int) -> None:
        """
        Присваивает атрибуту number_of_sim значение new_number_of_sim,
        при условии, что это целое число больше нуля
        """
        if isinstance(new_number_of_sim, int) and new_number_of_sim > 0:
            self.__number_of_sim = new_number_of_sim
        else:
            raise ValueError('Количество физических SIM-карт должно быть целым числом больше нуля')