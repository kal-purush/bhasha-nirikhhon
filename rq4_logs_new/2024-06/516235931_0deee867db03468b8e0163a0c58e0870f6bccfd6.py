import random

def math_game():
    num1 = random.randint(1, 10)
    num2 = random.randint(1, 10)
    answer = num1 + num2
    
    print("What is", num1, "+", num2, "?")
    user_answer = int(input())
    
    if user_answer == answer:
        print("Congratulations, you got the correct answer!")
    else:
        print("Sorry, that's incorrect. The correct answer is", answer)

def science_game():
    questions = {
        "What is the largest planet in our solar system?": "Jupiter",
        "What is the nearest star to Earth?": "Sun",
        "What is the process by which plants convert sunlight into energy?": "Photosynthesis",
        "What is the smallest particle that makes up all matter?": "Atom",
        "Ano ang true name nang teacher natin sa OOP?": "Sandro Marcos",
    }
    
    score = 0
    
    for question, answer in questions.items():
        print(question)
        user_answer = input().capitalize()
        
        if user_answer == answer:
            print("Correct!")
            score += 1
        else:
            print("Incorrect. The correct answer is", answer)
    
    print("You scored", score, "out of", len(questions), "questions.")

def print_center(message):
    print(message.center(50, '*'))
print
print_center("Welcome to the Grade Calculator with Games!")
print_center("Please choose an option:")
print("1. Calculate your grades")
print("2. Play a math game")
print("3. Play a science game")

option = int(input())

if option == 1:
    name = input("Please enter your name: ")
    math_grade = int(input("Please enter your math grade: "))
    science_grade = int(input("Please enter your science grade: "))
    english_grade = int(input("Please enter your English grade: "))
    
    total_score = math_grade + science_grade + english_grade
    average_score = total_score / 3
    
    print_center("SCORE REPORT")
    print("Name:", name)
    print("Math Grade:", math_grade)
    print("Science Grade:", science_grade)
    print("English Grade:", english_grade)
    print("Total Score:", total_score)
    print("Average Score:", average_score)
    
elif option == 2:
    math_game()
    
elif option == 3:
    science_game()
    
else:
    print("Invalid option. Please choose again.")