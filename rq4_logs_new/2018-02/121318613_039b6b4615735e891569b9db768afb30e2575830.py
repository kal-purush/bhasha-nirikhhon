import datetime
import logging
import sys

logging.basicConfig(level=logging.DEBUG)
#logging.setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)


def quit_game(correct, incorrect, dj):
    tally_file = datetime.datetime.now().strftime('%m%d%Y') + '.txt'
    
    percentage_correct = correct / (correct + incorrect)
    
    tally = str(correct) + ',' + str(incorrect) + ',' + str(dj) + ',' + str(percentage_correct)

    with open(tally_file, 'w') as file:
        file.write(tally + '\n')


if __name__ == '__main__':
    try:
        print('Press any key when ready..')
        start_input = input('\n')
        print('Press \'y\' for correct answer and \'n\' for incorrect answer.')
        print('Press \'d\' for double jeopardy answer, then \'y\' or \'n\' as usual.')
        print('Press \'q\' when done with game.')

        correct_count = 0
        incorrect_count = 0
        dj_count = 0
        while (True):
            response = input()

            if response == 'y':
                correct_count += 1
            elif response == 'n':
                incorrect_count += 1
            elif response == 'd':
                print('Double jeopardy!')
                dj_response = input('DOUBLE JEOPARDY RESPONSE')
                if dj_response == 'y':
                    correct_count += 1
                    dj_count += 1
            elif response == 'q':
                quit_game(correct_count, incorrect_count, dj_count)
                break

            print('Correct: ', correct_count)
            print('Incorrect: ', incorrect_count)
            print('Double Jeopardy Correct: ', dj_count)

        print('Good game!')

    except Exception:
        logger.exception('Exception raised.')
        logger.exception(e)

    except KeyboardInterrupt:
        logger.info('Exit signal received.')
        sys.exit()