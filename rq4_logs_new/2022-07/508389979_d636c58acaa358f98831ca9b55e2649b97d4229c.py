from fighter import Fighter
from fighterDecorator import FighterDecorator


class Jugador:

    def __init__(self, name: str, personaje: Fighter):
        self.__name: str = name
        self.__personaje: Fighter = personaje

    def get_personaje(self) -> Fighter:
        return self.__personaje

    def set_objetos(self, objetos: FighterDecorator):
        self.__personaje = objetos

    def get_name(self):
        return self.__name

    def show_statistics(self):
        print(f'Estadisticas {self.__name}')
        print("".center(20, "_"))
        print(f'|{"Vida": <10}|{self.get_personaje().get_hp():7}|')
        print(f'|{"Velocidad": <10}|{self.get_personaje().get_speed():7}|')
        print(f'|{"Ataque": <10}|{self.get_personaje().get_attack():7}|')
        print(f'|{"Defensa": <10}|{self.get_personaje().get_defence():7}|')
        print("".center(20, "_"))