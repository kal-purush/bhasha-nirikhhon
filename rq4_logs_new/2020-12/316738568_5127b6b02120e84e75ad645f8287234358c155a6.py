from util import iohandler

# --- solution ---


def init(input_file):
    groups = list()
    answers = str()
    for line in input_file:
        if line != '\n':
            answers += line.replace('\n', '')
        else:
            groups.append(answers.strip())
            answers = ''

    groups.append(answers.strip())
    return groups


def positive_answer_count(input_file):
    pos_answer_counter = 0
    for group in init(input_file):
        pos_answer_counter += len(set(group))
    return pos_answer_counter


# --- solution ---

if __name__ == '__main__':
    iohandler.end(str(positive_answer_count(iohandler.begin(__file__))))