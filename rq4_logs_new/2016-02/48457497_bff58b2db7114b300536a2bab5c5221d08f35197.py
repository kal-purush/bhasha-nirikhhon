## animal is-a object (yes, sort of confusing) look at the extra credit
class Animal(object):
	pass

## Dog is-a Animal
class Dog(Animal):

	def __init__(self, name):
		## the dog has-a name
		self.name = name
		
## the cat is-a Animal
class Cat(Animal):

	def __init__(self, name):
		## the cat has-a name
		self.name = name
		
class Person(object):

	def __init__(self, name):
		## the person has a name
		self.name = name
		
		## Person has-a pet of some kind
		self.pet = satan
		
##  the employee is-a person
class Employee(Person):

	def __init__(self, name, salary):
		## hmm what is this strange magic?
		super(Employee, self).__init__(name)
		## the employee has a salary
		self.salary = salary
		
## the Fish is-a class
class Fish(object):
	def __init__(self, name):
		self.name = name
		
	def coloring(self, color):
		self.color = color
	
## the Salmon is-a Fish
class Salmon(Fish):
	def __init__(self, name):
		self.name = name
		
	def farm(self, farmed):
		self.farmed = farmed
	
## the Halibut is-a Fish
class Halibut(Fish):
	pass
	
## rover is-a dog
rover = Dog("Rover")
print rover.name

## satan is a cat
satan = Cat("Satan")
print satan.name

## mary is a person
mary = Person("Mary")
print mary.name

## mary has-a pet named satan
mary.pet = satan
print mary.pet
print satan.name

## frank is an employee who has-a salary of 120000
frank = Employee("Frank", 120000)
print frank.name
print frank.salary

## frank has-a pet named rover
frank.pet = rover
print frank.pet
print rover.name

## flipper is-a fish
flipper = Fish("flipper")
print flipper.name

## crouse is-a salmon
crouse = Salmon("Crouse")
print crouse.name

crouse.name = "SAM"
print crouse.name

## harry is-a halibut
harry = Halibut("Harry")
print harry.name