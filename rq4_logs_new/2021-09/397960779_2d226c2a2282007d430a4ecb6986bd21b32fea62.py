# TOOL FOR KEEPING A DIARY LOG BASED ON QUESTIONS SPECIFIED BY THE USER

from posixpath import split
import sys
import os
import datetime


def get_datetime_as_string():
    """ Function for getting todays date/time as string
        so it can be added to the users diary log """

    time = datetime.datetime.now()
    time_as_string = time.strftime("%c")
    return time_as_string


def set_questions_file_path():
    """ Returns local path to users questions file """

    current_file_path = os.path.realpath(__file__)

    questions_file_path = current_file_path.replace(
        'diary_tool.py', 'questions')

    return questions_file_path


def set_log_file_path():
    """ Returns local path to users diary log file """

    current_file_path = os.path.realpath(__file__)

    log_file_path = current_file_path.replace('diary_tool.py', 'diary_log')

    return log_file_path


def ask_and_await_answer(question_list):
    """
    Asks user defined questions from locally stored question file,
    waits for answer, then takes date/time, question and answer
    and stores in a log file with appropriate line spacing for
    readability
    """

    datetime = get_datetime_as_string()

    # opens file as file object using append parameter
    destination_file = open(log_file_path, 'a')

    for i in range(0, len(question_list)):

        answer = input('\n\n' + question_list[i] + '\n\n')

        # outputs answer to questions in correct format
        # to log file
        destination_file.write('\n\n' + datetime + '\n\n'
                               + question_list[i] + '\n\n' + answer + '\n\n')

    destination_file.close()

    # add extra line space in terminal for readability
    print('\n')


def get_questions_from_user():
    """ Asks user for questions they would like to ask themselves
        and adds them to a list 'users_question_list'. The list is
        later used in function 'ask_and_await_answer'. Exits upon user
        typing QUIT """

    flag = True
    question_list = []

    while flag:

        users_question = (input("\n\nEnter your question here, "
                                "or type QUIT \n\n"))

        if users_question == 'QUIT':
            print('\n')
            flag = False
        else:
            question_list.append(users_question)

    return question_list


def format_stored_questions_for_user():
    """ Takes questions file object for reading; reads contents
        and simultaneously removes new lines and empty elements
        for when function 'ask_and_await_answer needs to ask
        user questions in CLI """

    # adds contents of questions file lines to list of questions
    text_lines = questions_file_read.read().split('\n')

    # filters unwanted empty elements from list
    question_list = list(filter(None, text_lines))

    return question_list


if __name__ == '__main__':

    """ Functions that set up file paths """

    questions_path = set_questions_file_path()
    log_file_path = set_log_file_path()

    # opens file objects in either read or amend as appropriate for application
    # (some instances we want to read, others append)

    questions_file_read = open(questions_path, "r")
    questions_file_append = open(questions_path, "a")
    log_file_append = open(log_file_path, "a")

    # initializes users question list from locally stored file
    users_question_list = format_stored_questions_for_user()

    """ THIS AREA takes CLI args to specify which functionality
        user wants, as follows:

        'input' to update questions
        'ask' to ask the questions user has stored previously
        'clearq' to clear all the users questions
        'reset' to delete all log entries and start over """

    # when no CLI arg is given

    if len(sys.argv) < 2:
        print("\n\nNo argument given. "
              " Please use 'input' to update questions, "
              "'ask' to ask your questions, "
              "'clearq' to clear your questions, "
              " or 'reset' to delete all log entries "
              " and start from scratch\n\n")

    # when there are too many CLI args

    elif len(sys.argv) > 2:
        print('Too many arguments given, only 1 is required')

    else:

        # asks user to input their questions

        if sys.argv[1] == 'input':

            questions = get_questions_from_user()

            # then iterates through list of questions
            # and writes to questions file using
            # question file object for appending

            for i in questions:

                questions_file_append.write((i + '\n\n'))

        elif sys.argv[1] == 'ask':

            # asks the user the questions
            # that they previously input
            # and stores results in log file
            # or if no questions are present,
            # they are given the option to
            # store questions with y or n

            if users_question_list == []:

                flag = True
                while flag:
                    request = input(
                        "\n\n No questions currently stored. "
                        "Run CLI arg 'input' to update questions, "
                        "or would you like to update them now? y or n? \n\n")

                    if request == 'y':
                        flag = False
                        questions = get_questions_from_user()

                        # iterates through list of questions
                        # and writes to questions file

                        for i in questions:
                            questions_file_append.write((i + '\n\n'))

                    elif request == 'n':
                        break
                    else:
                        print("\n\nPlease enter y or n to continue")

            if users_question_list != []:
                ask_and_await_answer(users_question_list)

        # clears the users question file

        elif sys.argv[1] == 'clearq':

            # resets the list that stores the questions for writing
            users_question_list = []

            # deletes contents of questions file
            open(questions_path, "w").close()

            print('\n\nQuestions cleared\n\n')

        # deletes everything from the log file
        elif sys.argv[1] == 'reset':
            open(log_file_path, "w").close()
            print('\n\nDiary log reset\n\n')

        elif sys.argv[1] == 'help':
            print("\n\nUse 'input' to update questions, "
                  "'ask' to ask your questions, "
                  "'clearq' to clear your questions, "
                  "or 'reset' to delete all log entries "
                  "and start from scratch\n\n")

        else:
            print('\nInvalid input\n')