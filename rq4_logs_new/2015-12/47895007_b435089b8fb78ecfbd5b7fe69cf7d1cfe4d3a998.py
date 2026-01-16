#!/usr/local/bin/python
import random

def simulateOneStudentWeighted():
    ''' This will simulate a single student's academic career at Westlake under the current system.
    Assumptions:  
        Each student takes 28 classes, so 56 semesters
        These students have 4 classes that are essentially free (sport, art, yearbook, etc.) where they are a guaranteed 100
        To contend for the valedictorianship, a student must take between 15 and 19 AP classes and between 4 and 7 PAP classes, which will be uniformly randomly modeled
        To contend for the valedictorianship, a student will have 96 <= grade <= 100 for an AP, a 97-100 for PAP and a 98-100 for regular
        This covers the 4 unweighted a year thing and typical progressions through other requirements
        This simulation weights AP classes by 1.2 and pre-AP classes by 1.1
    '''
    total = 0
    numAPs = 30 + random.randint(0, 8)
    numPAPs = numAPs + 8 + random.randint(0, 6)
    for semester in range(48):
        if semester < numAPs:
            grade = random.randint(96,100)
            grade *= 1.2
        elif semester < numPAPs:
            grade = random.randint(97, 100)
            grade *= 1.1
        else:
            grade = random.randint(98, 100)
        total += grade
    GPA = total / 56.0
    return GPA

def simulateOneStudentUnweighted():
    ''' This will simulate a single student's academic career at Westlake under the proposed unweighted system.
    Assumptions:
        Each student takes 28 classes, so 56 semesters
        These students have 4 classes that are free (see above.)
        A contending student might take less AP classes under this system, so 12-19 AP and 4-7 PAP.
        To contend for the valedictorianship, a student will have 96 <= grade <= 100 for an AP, a 97-100 for PAP and a 98-100 for regular.
    '''
    total = 0
    numAPs = 24 + random.randint(0, 14)
    numPAPs = numAPs + 8 + random.randint(0, 6)
    for semester in range(48):
        if semester < numAPs:
            grade = random.randint(96,100)
        elif semester < numPAPs:
            grade = random.randint(97, 100)
        else:
            grade = random.randint(98, 100)
        total += grade
    GPA = total / 56.0
    return GPA







def simYear(weighted):
    ''' This will simulate a single year under the current system or new one. Returns true if there's a tie, false if not.
    One important assumption in this step is that there are realistically only about 10 students/year who
    are going to be competing for the valedictorianship. In our other system, this will be 15 to account for the increased number of students that could contend'''
    yearGrades = {0}
    for student in range(10 if weighted else 10):
        grade = simulateOneStudentWeighted() if weighted else simulateOneStudentUnweighted()
        if grade in yearGrades:
            return True
        else:
            yearGrades.add(grade)
    return False

def simTrialsUnweighted():
    '''this will print summary statistics of the number of ties in the given number of trials'''
    ties = 0.0
    numTrials = 100000
    for trial in range(numTrials):
        ties += 1 if simYear(False) else 0
    print("Unweighted:\n{} trials.\n{} ties\n{} tie rate.\n".format(numTrials, ties, ties/numTrials))


def simTrialsWeighted():
    '''this will print summary statistics of the number of ties in the given number of trials'''
    ties = 0.0
    numTrials = 100000
    for trial in range(numTrials):
        ties += 1 if simYear(True) else 0
    print("Weighted:\n{} trials.\n{} ties\n{} tie rate.\n".format(numTrials, ties, ties/numTrials))

def main():
    simTrialsWeighted()
    simTrialsUnweighted()


if __name__ == '__main__':
    main()