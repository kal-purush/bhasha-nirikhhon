ef q2() :
    print("\n Answer is Correct. You can now attempt next Question.\n\n")
    print("Q.2 : Which component is used to compile, debug and execute the java programs?.\n")
    print("1] JRE")
    print("2] JIT")
    print("3] JDK")
    print("4] JVM")
    Ans2= input("\n Enter your Answer : ")

    if Ans2!='3':
      print("\n\n Your Answer is Incorrect. Please try again Next Year.")

    else:
      print("\n\n Congratulations! your cleared the EXAM.")

def exam() :
  print("----------------------------------------\n\n")
  print("All the Best for the MCQ Exam \n\n")
  print("----------------------------------------\n\n")
  print("Q.1 : Who Inventated JAVA Programming.\n")
  print("1] Guido van Rossum")
  print("2] James Gosling")
  print("3] Dennis Ritchie")
  print("4] Bjarne Stroustrup")

  Ans1= input("\n Enter your Answer : ")

  if Ans1=='2':
    q2()

  else:
    print("\n\n Your Answer is Incorrect. Please try again Next Year.")


from getpass import getpass


username = input("Enter User name : ")
pwd = getpass("Enter Password : ")
max=4
min=0

while max>min:
  chk=getpass("Reconfirm your Password : ")

  if chk==pwd:
    print("\n You can Now Start the Exam\n")
    exam()
    break

  else:
    min+=1
    print("Password is Incorrect, Try Again (",{max-min}," attempts)")

  if chk!=pwd:
    print("Access Denied")