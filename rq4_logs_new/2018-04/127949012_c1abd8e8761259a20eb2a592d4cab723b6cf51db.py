# -*- coding: utf-8 -*-

import random

__names = "svgmrt"
__additional = "aeiou"
__random_factor = 3
__roll_number = 10

welcome_msg = "Welcome to Acronym Creator! Please give action number to continue."
first_input_prompt = {
    "selection": "Set/Change the names",
    "question": "Which names will be used in acronym, please write as one string: "
}
second_input_prompt = {
    "selection": "Set/Change the additional characters",
    "question": "Which additional characters will be used in acronym, please write as one string: "
}
third_input_prompt = {
    "selection": "Set/Change the random factor",
    "question": "Which random factor you will use in acronym: "
}
fourth_input_prompt = {
    "selection": "Set/Change the roll number",
    "question": "How many acronym you will create in one roll: "
}
roll_msg = "Roll the acronyms!"
exit_msg = "Exiting the application :("
change_msg = "Value has been changed!"
wrong_input_msg = "You wrote wrong input/action number..."

vowels = ['a', 'e', 'ı', 'i', 'o', 'ö', 'u', 'ü']


def input_filter(input):
    input = input.replace(" ", "")
    input = input.replace("\n", "")
    return input


def roll():
    __additional_chars = [ch for ch in __additional]
    for _ in range(__random_factor):
        __additional_chars.append('')
    for _ in range(__roll_number):
        print random.choice(__additional_chars) + ''.join([x + random.choice(__additional_chars) for x in __names])
    print ""


if __name__ == '__main__':
    is_exit = False
    print welcome_msg
    print "1) " + first_input_prompt['selection']
    print "2) " + second_input_prompt['selection']
    print "3) " + third_input_prompt['selection']
    print "4) " + fourth_input_prompt['selection']
    print "5) " + roll_msg
    print "6) Exit"
    while not is_exit:
        action = raw_input()
        if action == "1":
            __names = input_filter(raw_input(first_input_prompt['question']))
            print change_msg
        elif action == "2":
            __additional = input_filter(raw_input(second_input_prompt['question']))
            print change_msg
        elif action == "3":
            try:
                __random_factor = int(input_filter(raw_input(third_input_prompt['question'])))
                print change_msg
            except ValueError:
                print wrong_input_msg
        elif action == "4":
            try:
                __roll_number = int(input_filter(raw_input(third_input_prompt['question'])))
                print change_msg
            except ValueError:
                print wrong_input_msg
        elif action == "5":
            roll()
        elif action == "6":
            print exit_msg
            is_exit = True
        else:
            print wrong_input_msg