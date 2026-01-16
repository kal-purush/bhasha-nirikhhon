var = input("1 for About Me, 2 for superhero"
)

if not var == 1 or 2:
  while not var == 1 or 2:
    var = input("1 for About Me, 2 for superhero")

if var == 1:

  print("About Me")

  string1 = input("What is your name? ")
  string2 = input("What is your favorite food? ")
  string3 = input("What is your favorite day of the week? ")
  string4 = input("What is your favorite city? ")
  string5 = input("What is your favorite holiday? ")
  string6 = input("What is your favorite game? ")
  string7 = input("Do you exist (Exist or Don't Exist)? ")

  print("My name is", string1, ", My favorite food is ", string2, ",my favorite day of the week is ", string3, ", my favorite city is ",string4, ", my favorite holiday is ", string5, "and my favorite game is ", string6, ". I", string7, ".")

if var == 2:

  print("My Superhero")

  string1 = input("What is your superhero's name?")
  string2 = input("What city does your superhero protect?")
  string3 = input("")
  string4 = input("What is your superhero's power?")
  string5 = input("What is your superhero's weakness?")
  string6 = input("")
  string7 = input("")

  print("My superhero's name is ", string1, ". The city that my superhero protects is ", string2, "d", string3, ". My superhero's power is ", string4, ". My superhero's weakness is ", string5, "d", string6, "d", string7, ".")