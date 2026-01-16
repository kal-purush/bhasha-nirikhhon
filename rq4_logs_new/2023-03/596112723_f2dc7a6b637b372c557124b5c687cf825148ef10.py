import os


studentDict: dict = {}
courseDict: dict = {}
markDict: dict = {}
class Student():
    def __init__(self):
        self.__number_of_student: int = 0
    def setStudentNumber(self, count_student_number: int):
        self.__number_of_student: int = count_student_number
    def getStudentNumber(self):
        return self.__number_of_student
    
    #take students info
    def studentsInfo():
        count_student_number: int = int(input("Enter number of student to take info: "))
        if count_student_number < 0:
            print("\nPls enter positive number!!!")
        if count_student_number > 0:
            print(f"\nEnter info for {count_student_number} students")
            for student in range(count_student_number):
                s_id: str = input("\nEnter student's id: ")
                if s_id not in studentDict:
                    s_name: str = input("Enter student's name: ")
                    s_date: str = input("Enter student's date birth: ")
                    s_month: str = input("Enter student's month birth: ")
                    s_year: str = input("Enter student's year birth: ")
                    student_Dob_list: list = [s_date, s_month, s_year]
                    student_Dob = "/".join(student_Dob_list)
                    studentDict[s_id] = {"Student name": s_name, "Student date of birth": student_Dob}
                    print(f"{s_id}: {s_name}> Has join the battle")
                else:
                    print("This id already exist pls, re-enter id")
                    continue

    #print out student list
    def printStudentDict():
        print("\nThis is the list of the current student: \n", studentDict)
        
                
          
class Course():
    def __init__(self):
        self.__number_of_course: int = 0
    def setCourseNumber(self, count_course_number: int):
        self.__number_of_course = count_course_number
    def getCourseNumber(self):
        return self.__number_of_course
    
    #take course info
    def courseInfo():
        count_course_number: int = int(input("Enter number of course: "))
        if count_course_number < 0:
            print("\nPls enter positive number!!!")
        if count_course_number > 0:
            print(f"\nEnter info for {count_course_number} courses")
            for student in range(count_course_number):
                c_id: str = input("\nEnter course's id: ")
                if c_id not in courseDict:
                    c_name: str = input("Enter course's name: ")
                    courseDict[c_id] = {"Course name": c_name}
                    print(f"Course {c_id}: {c_name} added")
                else:
                    print("Course id already exist, pls re-enter")
                    continue

    #print out course list
    def printCourseDict():
        print("\nThis is the list of the current course: \n", courseDict)
    
class Mark():
    
    #take mark of students
    def markInfo():
        ask_course: str = input("\nEnter course's ID to fill student mark: ")
        if ask_course in courseDict:
            print("\nEnter mark for student in this course:")
            for s_id in studentDict:
                mark: float = float(input(f"\nEnter mark for student's {studentDict[s_id]['Student name']}: "))
                markDict[s_id] = {"Course": ask_course, "Mark": mark}
                print(markDict)
        if ask_course not in courseDict:
            print("\nCourse does not exist")
     
    #print mark list        
    def printMark():
        check_course: str = input("\nEnter course's ID to check mark(type Exit to exit): ")
        while check_course not in courseDict:
            print("\nEnter correct course's ID")
            if check_course == "Exit":
                break
        else:
            print("\nHere is the student's mark list: ")
            for s_id in studentDict:
                # if s_id in markDict and check_course in courseDict:
                print(markDict)

class Functionalities():
    
    #excute a function
    def launchFunction(option: int):
        if option == 1:
            Student.studentsInfo()
        elif option == 2:
            Course.courseInfo()
        elif option == 3:
            Student.printStudentDict()
        elif option == 4:
            Course.printCourseDict()
        elif option == 5:
            Mark.markInfo()
        elif option == 6:
            Mark.printMark()
        else:
            print("\nYour option is not in the list")

if __name__ == "__main__":
    while True:
        print(f'''Choose an action:
                1.Take student info
                2.Take course info
                3.List student
                4.List class
                5.Take student mark
                6.Mark list
                ''')
        option: int = int(input("Pls choose 1 option above: "))
        Functionalities.launchFunction(option)
    
        




     
