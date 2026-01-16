#!/usr/bin/python3 -tt

class Emp:
	#Class Attribute
	domain = "hackme.loacal"
	#Instance variable
	def __init__(self,age,name,dept):
		self.name = name
		self.age = age
		self.dept = dept
	#Instance method
	def intro(self):
		return"my name is {} and i am {} years old".format(self.name,self.age)
	def background_verification(self,ver_date):
		return"The annual background verification of {} was completed in the month of {}".format(self.name,ver_date)
	
def main():
	tinku = Emp(23,"pinky",'IT')
	print(tinku.intro())
	print(tinku.background_verification('dec 2018')	)

if __name__ == "__main__":
	main()