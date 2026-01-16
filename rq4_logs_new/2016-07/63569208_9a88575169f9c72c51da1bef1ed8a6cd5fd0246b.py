class Student(object):
    """Student data - first name, last name, address."""
    def __init__(self, first_name, last_name, address):
        """Instantiates Student with first name, last name, and address."""
        self.first_name = first_name
        self.last_name = last_name
        self.address = address


class Question(object):
    """Question and answer data as well as an evaluation method."""
    def __init__(self, question, correct_answer):
        """Instantiates Question with a question and a correct answer."""
        self.question = question
        self.correct_answer = correct_answer

    def ask_and_evaluate(self):
        """Method for Question to compare user-input answer with the correct answer"""
        print self.question
        answer = raw_input(">>>   ")

        if answer == self.correct_answer:
            return True
        else:
            return False


class Exam(object):
    """Exam that holds list of questions and methods."""
    def __init__(self, name):
        """Instantiates Exam with an exam name."""
        self.name = name
        self.questions = []

    def add_question(self, question, correct_answer):
        """Exam method to add questions and answers to the list of questions."""
        new_question = Question(question, correct_answer)
        self.questions.append(new_question)

    def administer(self):
        """Exam method to administer the exam and return a score."""
        score = 0

        for exam_question in self.questions:
            if exam_question.ask_and_evaluate() is True:
                score += 1

        return score


def take_test(exam, student):
    """Function to administer an Exam, add score instance attribute for Student."""
    score = exam.administer()
    student.score = score

    if student.score is True:
        print "Congratulations! You have passed!"
    elif student.score is False:
        print "Sorry. You have failed."
    else:
        print "Test Score: {} out of {} correct".format(student.score, len(exam.questions))


class Quiz(Exam):
    """Class that holds Quiz data, which inherits from Exam."""
    def administer(self):
        """To override Exam's administer method to return True/False instead of score."""
        if super(Quiz, self).administer() >= (len(self.questions)/2):
            return True
        else:
            return False


def example():
    """Function to test Exam class."""
    sample_exam = Exam("Sample")
    sample_student = Student("Jane", "Doe", "101 Here")

    sample_exam.add_question("What is x?", "x")
    sample_exam.add_question("What is y?", "y")
    sample_exam.add_question("What is z?", "z")

    take_test(sample_exam, sample_student)


def example_quiz():
    """Function to test Quiz class."""
    sample_quiz = Quiz("Example")
    example_student = Student("John", "Doe", "102 There")

    sample_quiz.add_question("What is A?", "A")
    sample_quiz.add_question("What is B?", "B")
    sample_quiz.add_question("What is C?", "C")

    take_test(sample_quiz, example_student)