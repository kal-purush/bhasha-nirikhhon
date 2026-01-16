from abc import ABC, abstractclassmethod

class Person(ABC):

    def __init__(self, p_type):
        self.p_type = p_type    # tipo (hacker ou serf)
        super().__init__()      # chamada do construtor da superclasse

    def row(self, boat):
        # TODO: logica de remada
        pass

    @abstractclassmethod        # metodo abstrato
    def board(self, boat):
        pass



class Serf(Person):

    def board(self, boat):
        # TODO: logica de um novo serf entrando
        pass


class Hacker(Person):

    def board(self, boat):
        # TODO: logica de um novo hacker entrando
        pass