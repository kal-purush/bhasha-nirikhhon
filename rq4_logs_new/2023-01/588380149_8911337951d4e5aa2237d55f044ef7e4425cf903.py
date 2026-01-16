class animal:
    def __init__(self, age: int, name: str):
        self.name = name
        self.age = age
        self.sound = ""
    
    def make_noise(self):
        print(self.sound)

class dog(animal):
    def __init__(self, age: int, name: str):
        super().__init__(age, name)
        self.sound = 'woof'

class cat(animal):
    def __init__(self, age: int, name: str):
        super().__init__(age, name)
        self.sound = 'hello'


if __name__ == '__main__':
    andy = dog(4, 'andy')
    sara = cat(2, 'sara')
    andy.make_noise()
    sara.make_noise()