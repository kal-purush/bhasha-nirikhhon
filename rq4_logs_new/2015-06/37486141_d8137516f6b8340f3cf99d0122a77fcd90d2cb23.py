import sys

def main():
   acceptible_activities = {1, 2, 3, 4, 5}
   gender = raw_input('Enter your gender(m or f): ')
   while(gender is not 'm' and gender is not 'f'):
      gender = raw_input('Please enter m or f: ')
   height = float(raw_input('Height in inches: '))
   weight = float(raw_input('Weight in pounds: '))
   age = float(raw_input('Age in years: '))

   if(gender is 'm'):
      bmr = 66
      bmr += (6.23 * weight)
      bmr += (12.7 * height)
      bmr -= (6.8 * age)
   else:
      bmr = 655
      bmr += (4.35 * weight)
      bmr += (4.7 * height)
      bmr -= (4.7 * age)

   print "Your BMR is " + str(bmr)

   activity = input("How active are you? 1 for sedentary, 2 for lightly active, 3 for moderately active, 4 for very active, and 5 for extremely active: ")

   while(activity not in acceptible_activities):
      activity = input("Please enter a number between 1 and 5: ")

   if activity is 1:
      total = bmr * 1.2

   elif activity is 2:
      total = bmr * 1.375

   elif activity is 3:
      total = bmr * 1.55

   elif activity is 4:
      total = bmr * 1.7

   elif activity is 5:
      total = bmr * 1.9

   print "You burn " + str(total) + " calories per day."

if __name__ == '__main__':
   main()