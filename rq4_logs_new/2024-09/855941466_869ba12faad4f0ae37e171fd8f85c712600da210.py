class Dog:

    birthday = '22/12/2004'
    #constructor for the dog object
    def __init__(self, name, breed, color):
        self.name = name
        self.breed = breed
        self.color = color

    def bark(self):
        print('Woooof...Wuhuhuhuu')
    def play(self):
        print(self.name, 'is playing')

    def dog_info(self):
        print('My dog is called ', self.name)
        print('It is a ', self.breed)
        print('It is ', self.color, 'in color')
        print('Birthday:', self.birthday)
    
dog1 = Dog('Rocky', 'German Shepherd', 'Red')

dog1.dog_info()
dog1.bark()