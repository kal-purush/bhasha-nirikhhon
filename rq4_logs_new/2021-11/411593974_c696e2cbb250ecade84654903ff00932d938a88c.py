from abc import ABC, abstractmethod


#1 

""" My Exeptions """
""" Я это так понял:)"""

class AttributeError(Exception):
    def __init__(self):
        super().__init__()
        raise NotExistException

class MyException(Exception):
    pass

class NotExistException(MyException):
    print(f'Для этого класса такого метода не существует')



""" Ferm """

class Animals(ABC):
    @abstractmethod
    def __init__(self, name, age = 0):
        self.name = name
        self.__age = age

    @abstractmethod
    def get_age(self):
        return self.__age

    @abstractmethod
    def get_older(self):
        self.__age += 1
        return self.__age

    @abstractmethod
    def walk(self):
        print(f'Top-top')

    @abstractmethod
    def voice(self):
        pass


class Birds(ABC):
    @abstractmethod
    def __init__(self, name, age = 0):
        self.name = name
        self.__age = age

    @abstractmethod
    def get_age(self):
        return self.__age

    @abstractmethod
    def get_older(self):
        self.__age += 1
        return self.__age

    @abstractmethod
    def fly(self):
        print(f'Fly-Fly')

    @abstractmethod
    def voice(self):
        pass




class Pig(Animals):    
    def __init__(self, name, age = 0):
        self.name = name
        self.__age = age
        print(f'Animal {self.__class__.__name__} with name {name} was born')

    def get_age(self):
        print(f'{self.__age} years')

    def get_older(self):
        self.__age += 1
        return self.__age

    def walk(self):
        print(f'Top-top')

    def voice(self):
        print('Hru-Hru')


class Cow(Animals):
    def __init__(self, name, age = 0):
        self.name = name
        self.__age = age
        print(f'Animal {self.__class__.__name__} with name {name} was born')

    def get_age(self):
        print(f'{self.__age} years')

    def get_older(self):
        self.__age += 1
        return self.__age

    def walk(self):
        print(f'Top-top')

    def voice(self):
        print('Mooo')


class Goose(Birds):
    def __init__(self, name, age = 0):
        self.name = name
        self.__age = age
        print(f'Bird {self.__class__.__name__} with name {name} was born')

    def get_age(self):
        return self.__age

    def get_older(self):
        self.__age += 1
        return self.__age

    def fly(self):
        print(f'Fly-Fly')

    def voice(self):
        print('Ga-Ga-Ga')


class Chicken(Birds):
    def __init__(self, name, age = 0):
        self.name = name
        self.__age = age
        print(f'Bird {self.__class__.__name__} with name {name} was born')

    def get_age(self):
        return self.__age

    def get_older(self):
        self.__age += 1
        return self.__age

    def fly(self):
        print(f'Fly-Fly')

    def voice(self):
        print('Kudah-Kudah')


class Ferm:
    def __init__(self, list_of_animals = []):
        self.animals = list_of_animals
        for i in self.animals:
            print (f'{i.__class__.__name__} {i.name} on the ferm')
    
    def get_animals_by_index(self, index):
        print(f'{self.animals[index].__class__.__name__} {self.animals[index].name}')
        
    def get_animals_by_for(self): 
        for i in self.animals:
            print (f'{i.__class__.__name__} {i.name}')

    def get_older(self):
        for animals in self.animals:
            animals.get_older()
        return 


pig_1 = Pig('"Nif-Nif"')
pig_1.ged_age()
pig_1.get_older()
pig_1.get_age()
pig_1.walk()
pig_1.voice()

cow_1 = Cow('"Black"')
cow_1.get_age()
cow_1.get_older()
cow_1.get_age()

ferm_1 = Ferm([pig_1, cow_1])
ferm_1.get_animals_by_index(1)
ferm_1.get_animals_by_for()
ferm_1.get_older()

pig_1.get_age()




#2

""" Book """
        
""" My Exceptions """
class MyExceptions(Exception):
    def __init__(self, message):
        message = 'Неверный тип вводимых данных'
        super().__init__(message)


class Book:
    def __init__(self, number_of_pages, year_of_publishing, author, price):
        print(f'Book was added')
        
        if isinstance(number_of_pages, int) == False:
            raise MyExceptions('Number of pages should be integer type')
        else:
            self.number_of_pages = number_of_pages
        
        if isinstance(year_of_publishing, int) == False:
            raise MyExceptions('Year of publishing should be integer type')
        else:
            self.year_of_publishing = year_of_publishing

        if isinstance(author, str) == False:
            raise MyExceptions('Author should be string type')
        else:
            self.author = author
        
        if isinstance(price, int) == False:
            raise MyExceptions('Price should be integer type')
        else:
            self.price = price

book_1 = Book('fdf', 138, 'Tom Soyer', '100')