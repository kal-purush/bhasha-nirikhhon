# Solution to Problem 4
# Eoin Dowling 27-Feb-2019

# Starting program input from code created in solution 1 and adapting as necessary.
while True:
  try:
    start = int(input("Please enter a positive Integer "))
		assert (start > 0)
    break
  except:
    print("You entered a number less than 1 or a non numeric character, please try again.")
