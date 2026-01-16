#import os
import datetime  # Used to store the timestamp in the leaderboard

import random  # Used for selecting random positive/negative feedback messages


# Define a class to encapsulate all quiz logic using OOP principles
class QuizGame:
    def __init__(self, name):
        self.name = name                      # Store the player's name
        self.score = 0                        # Track score for current round
        self.questions = []                   # List of questions
        self.answers = []                     # List of correct answers
        self.round_scores = []                # Keep scores across multiple rounds
        self.correct_feedback = [  "🎉 Hooray! You got it right!",    "✅ That's the spirit! Well done!",    "👏 Correct! You're on fire!",    "🎯 Bang on! Keep it up!",                "💡 Smart move! That's correct!"]   # List of positive responses    
        self.incorrect_feedback = [      "😓 Oops! Not quite right.",    "🙃 Incorrect... but don't give up!",    "🧐 Hmm, not this time.",    "😕 Not quite, but you're doing great!",    "🚫 That's not it. Try again!"] # List of encouraging responses


    def print_instructions(self):
        # Print quiz instructions
        print("\n🎯 INSTRUCTIONS 🎯")
        print("1. Answer each question to the best of your ability.")
        print("2. You get 3 attempts per question.")
        print("3. Correct answers earn full points, retrying gives partial points.")
        print("4. Type 'quit' anytime to exit the quiz.\n")

    def load_quiz(self, quiz_type):
        # Load questions and answers based on quiz type
        if quiz_type == "C":
            q_file_name = "current.txt"
            a_file_name = "current_answers.txt"
        elif quiz_type == "G":
            q_file_name = "general.txt"
            a_file_name = "general_answers.txt"
        else:
            print("Invalid quiz type. Please restart and enter 'C' or 'G'.")
            return False

        try:
            with open(q_file_name, "r") as q_file, open(a_file_name, "r") as a_file:
                self.questions = [line.strip() for line in q_file if line.strip()]
                self.answers = [line.strip().upper() for line in a_file if line.strip()]
            return True
        except FileNotFoundError:
            print("Quiz files not found.")
            return False

    def calculate_score(self, user_answer, correct_answer):
        # Score calculation logic with up to 3 attempts
        attempts = 0
        max_attempts = 3

        while attempts < max_attempts:
            if user_answer == correct_answer:
                # Randomly choose a positive feedback message
                print(random.choice(self.correct_feedback))
                break
            else:
                # Randomly choose an encouraging message for incorrect answer
                print(random.choice(self.incorrect_feedback))
                attempts += 1

                if attempts < max_attempts:
                    try_again = input("Do you want to try again? (Y/N): ").upper()
                    if try_again == "Y":
                        user_answer = input("Try again! Enter your answer (A/B/C/D or 'quit' to exit): ").upper()
                        if user_answer == "QUIT":
                            print("You chose to quit the quiz.")
                            return 0
                    else:
                        break
                else:
                    print("🙁 Out of attempts. Better luck next time!")
                    print(f"The correct answer was: {correct_answer}")

        # Return score depending on number of attempts
        if user_answer == correct_answer:
            return 4 * (1 / (2 ** attempts))  # Full or partial points
        else:
            return 0

    def run_quiz(self):
        # Run through questions one by one
        self.score = 0
        i = 0

        while i < len(self.questions):
            question = self.questions[i]
            options = self.questions[i+1:i+5]
            correct_answer = self.answers[i // 5]

            print("\n🎯 Question:")
            print(question)
            print("Options:")
            for opt in options:
                print(opt)

            user_answer = input("Your answer (A/B/C/D or type 'quit' to exit): ").upper()

            if user_answer == "QUIT":
                print("You chose to quit the quiz.")
                break

            if user_answer in ["A", "B", "C", "D"]:
                points = self.calculate_score(user_answer, correct_answer)
                self.score += points
            else:
                print("😓 Please enter a valid option (A, B, C, D or 'quit').")

            i += 5  # Move to next question block

        print(f"\n{self.name}, your score for this round is: {self.score}")
        self.round_scores.append(self.score)
        self.update_leaderboard()

        # 🎉 Display special message for best round
        if self.score == max(self.round_scores):
            print("🏆 Wow! This was your highest scoring round yet!")

    def update_leaderboard(self):
        # Append user's score and timestamp to the leaderboard file
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Get current date and time
        with open("score_leaderboard.txt", "a") as file:
            file.write(f"{self.name}: {self.score} at {timestamp}\n")  # Save name, score, and time

    def show_final_summary(self):
        # Display summary of user's scores across rounds
        print("\n🏁 QUIZ SUMMARY 🏁")
        for idx, score in enumerate(self.round_scores, 1):
            print(f"Round {idx}: {score} points")

        print(f"\nTotal Score: {sum(self.round_scores)}")

        if self.round_scores:
            avg = sum(self.round_scores) / len(self.round_scores)
            print(f"Average Score per Round: {round(avg, 2)}")

    def show_leaderboard(self):
        # Read and display leaderboard sorted by highest score
        print("\n📊 LEADERBOARD 📊")
        try:
            with open("score_leaderboard.txt", "r") as file:
                lines = file.readlines()

                scores = []
                for line in lines:
                    # Example line: Alice: 16.0 at 2025-04-13 18:30:55
                    parts = line.strip().split(" at ")
                    if len(parts) == 2:
                        name_score = parts[0]
                        time_str = parts[1]
                        try:
                            name, score_str = name_score.split(":")
                            score = float(score_str.strip())
                            scores.append((name.strip(), score, time_str))
                        except ValueError:
                            continue

                # Sort by score descending
                scores.sort(key=lambda x: x[1], reverse=True)

                if scores:
                    for rank, (name, score, time_str) in enumerate(scores, 1):
                        print(f"{rank}. {name} - {score} points (on {time_str})")
                else:
                    print("Leaderboard is empty.")
        except FileNotFoundError:
            print("Leaderboard file not found.")

# === MAIN PROGRAM LOGIC ===

# Ask user for name and show instructions
name = input("Welcome to Quizmaster! To start, please tell us your name:\n")
game = QuizGame(name)
game.print_instructions()

# Quiz loop: multiple rounds
while True:
    quiz_type = input(f"Hi {name}! What kind of quiz would you like to play?\nType C for Current Affairs or G for General: ").upper()

    if game.load_quiz(quiz_type):
        game.run_quiz()
    else:
        break  # Exit if loading fails

    # Ask whether to play again or show leaderboard
    play_again = input("\nDo you want to play another round? (Y = Yes, N = No, L = View Leaderboard): ").upper()
    if play_again == "Y":
        continue
    elif play_again == "L":
        game.show_leaderboard()
        game.show_final_summary()
        print("🎉 Thank you for playing! See you next time!")
        break
    else:
        game.show_final_summary()
        print("🎉 Thank you for playing! See you next time!")
        break