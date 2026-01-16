from util import iohandler

# --- solution ---


def init(input_file):
    groups = list()
    answers = str()
    for line in input_file:
        if line != '\n':
            answers += line.replace('\n', '-')  # with this approach nl at eof is important
        else:
            groups.append(answers.strip())
            answers = ''

    groups.append(answers.strip())
    return groups


def positive_group_answers(input_file):
    pos_answer_counter = 0
    for group in init(input_file):
        member_num = group.count('-')
        group = group.replace('-', '')
        for char in group:
            if group.count(char) == member_num:
                group = group.replace(char, '')
                pos_answer_counter += 1

    return pos_answer_counter


# --- solution ---

if __name__ == '__main__':
    iohandler.end(str(positive_group_answers(iohandler.begin(__file__))))