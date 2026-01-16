class Person:

    def __init__(self, first, last, age, occupation):
        self._first = first
        self._last = last
        self._age = age
        self._occupation = occupation

    @property
    def first(self):
        return self._first
    @first.setter
    def first(self, value):
        self._first = value
        return self._first

    @property
    def last(self):
        return self._last
    @last.setter
    def last(self, value):
        self._last = value
        return self._last

    @property
    def age(self):
        return self._age
    
    @age.setter
    def age(self, value):
        if value >= 0:
            self._age = value
        else:
            raise ValueError("Age cannot be negative.")

    @property
    def occupation(self):
        return self._occupation
    
    @occupation.setter
    def occupation(self, value):
        self._occupation = value
        return self._occupation

person1 = Person('Jane', 'Goodall', -100, 'scientist')
person1.first = 'JANE'
person1.age = -100
# print(person1.age)