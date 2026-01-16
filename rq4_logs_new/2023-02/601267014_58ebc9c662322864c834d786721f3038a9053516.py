from datetime import datetime

from data_functions import find_answer, find_clue, find_crossword

# menu() outputs a menu to the standard output and reads from the standard input and returns it
def menu(error=False):

	if error:
		print("Sorry, the option you selected was invalid.")
		print("Please try again...")

	print("\n======================================================")
	print("| A) Search Clues              B) Search Answers     |")
	print("| C) Search for a Crossword    X) Exit Program       |")
	print("======================================================")

	option = input("Enter an option: ")

	return option

# search_clues() opts into Search Clue mode and allows the user to search 
#    the dictionary by clue values, it returns either "q" or "x" when the
#    user chooses to stop searching
def search_clues():
	print("\n               Mode: Search Clue")
	print("===============================================")
	print("B) Search Answer      C) Search for a Crossword")
	print("H) Help               Q) Quit Mode")
	print("X) Exit Program")

	clue = input("Enter a clue: ")

	# continuously prompt the user until they want to exit
	while(clue.lower() != "q" and clue.lower() != "x"):
		# users can freely switch between search modes 
		if (clue.lower() == "b"):
			clue = search_answers()
		elif (clue.lower() == "c"):
			clue = search_crosswords()
		elif (clue.lower() == "h"): # help will re-output the menu
			print("\n               Mode: Search Clue")
			print("===============================================")
			print("B) Search Answer      C) Search for a Crossword")
			print("H) Help               Q) Quit Mode")
			print("X) Exit Program")

			clue = input("Enter a clue: ") # reprompt
		else:
			find_answer(clue)
			clue = input("Enter a clue: ") # reprompt

	return clue

# search_answers() opts into Search Answers mode and allows the user to search
#    the dictionary by its keys, it returns either "q" or "x" when the user
#    chooses to stop searching
def search_answers():
	print("\n              Mode: Search Answer")
	print("===============================================")
	print("A) Search Clue        C) Search for a Crossword")
	print("H) Help               Q) Quit Mode")
	print("X) Exit Program")

	answer = input("Enter an answer: ")
	
	# continuously prompt the user until they want to exit
	while(answer.lower() != "q" and answer.lower() != "x"):

		# users can freely switch between search modes
		if (answer.lower() == "a"):
			answer = search_clues()
		elif (answer.lower() == "c"):
			answer = search_crosswords()
		elif (answer.lower() == "h"): # re-ouputs the help menu
			print("\n              Mode: Search Answer")
			print("===============================================")
			print("A) Search Clue        B) Search for a Crossword")
			print("H) Help               Q) Quit Mode")
			print("X) Exit Program")

			answer = input("Enter an answer: ") # reprompt
		else:
			find_clue(answer)
			answer = input("Enter an answer: ") # reprompt

	return answer

# search_crosswords() opts into Search Crossword mode and allows the user to
#    search the dictionary by its date values, it returns either "q" or "x" 
#    when the user chooses to stop searching
def search_crosswords():
	print("\n            Mode: Search Crossword")
	print("==============================================")
	print("A) Search Clue                B) Search Answer")
	print("H) Help                       Q) Quit Mode")
	print("X) Exit Program")

	input_date = input("Enter a date (mm/dd/yy): ")

	# continuously prompt the user until they want to exit
	while (input_date.lower() != "q" and input_date.lower() != "x"):
		if (input_date.lower() == "a"):
			input_date = search_clues()
		elif (input_date.lower() == "b"):
			input_date = search_answers()
		elif (input_date.lower() == "h"): # re-outputs the menu
			print("\n            Mode: Search Crossword")
			print("==============================================")
			print("A) Search Clue                B) Search Answer")
			print("H) Help                       Q) Quit Mode")
			print("X) Exit Program")

			input_date = input("Enter a date (mm/dd/yy): ") # reprompt
		else:
			# tests whether the date was entered in the proper format
			#    if it was not then a ValueError occurs and we inform the user
			try:
				date = datetime.strptime(input_date, "%m/%d/%y") # checks formatting
				find_crossword(input_date)
				input_date = input("Enter a date (mm/dd/yy): ") # reprompt
			except ValueError:
				print(f"Sorry, no matches found for {input_date}")
				input_date = input("Enter a date (mm/dd/yy): ") # reprompt

	return input_date