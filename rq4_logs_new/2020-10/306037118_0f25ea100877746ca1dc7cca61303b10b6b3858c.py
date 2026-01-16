class School:
    def __init__(self, name, groups: list, personals: list):
        self.__name = name
        self.__groups = groups
        self.__personas = personals
        self.__teachers = []
    
    def __str__(self):
        return f'{self.__name}'