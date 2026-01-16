import datetime

now = datetime.datetime.now()

age = raw_input("What is your current age?")

r_age = raw_input("At what age would you like to retire?")

int_1 = int(age)

int_2 = int(r_age)

n_years = int_2 - int_1

r_year = now.year + n_years

int_3 = int(n_years)

int_4 = int(r_year)

int_3 = str(int_3)

int_4 = str(int_4)

year_1 = str(now.year)

print("You have about " + int_3 + " years left before you can retire. ")

print("It's " + year_1 +", so you could retire in " + int_4 + ". ")
 


