# find something you need to calculate ad write a new .py file that does it
# so uh, lets calculate the amount of coffee grounds required to brew 8 cups of coffee

ounces_in_a_cup = 5
alex_golden_ratio = 2 / 3 # divide desired ounces of coffee by this number to get the required amount of coffee grounds

fullPot = ounces_in_a_cup * 8

print("To brew 1 cup of coffee, you'll need approximately grams of coffee", ounces_in_a_cup * alex_golden_ratio)

def coffee_maker(n):
	# n is the ounces of coffee we want to brew, but what if we wanted to provide the number of cups we wanted instead?
	return n * alex_golden_ratio

def coffee_maker2(n):
	# now n represents the number of cups we want, so we'll need to first convert n to ounces
	n = n * ounces_in_a_cup # multiply the number of cups by the number of ounces in a single cup
	# print(n)
	return n / alex_golden_ratio

print(coffee_maker(45))
print(coffee_maker2(8))
