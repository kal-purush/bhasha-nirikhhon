class Student:
    def _init_(self, name, reg, m1, m2, m3):
        self.name = name
        self.reg = reg
        self.marks1 = m1
        self.marks2 = m2
        self.marks3 = m3
        self.total_marks = m1 + m2 + m3
        self.average_marks = self.total_marks / 3
def display_student_details(reg):
    for st in students:
        if st.reg == reg:
            print("Name:", st.name)
            print("Reg No:", st.reg)
            print("Marks 1:", st.marks1)
            print("Marks 2:", st.marks2)
            print("Marks 3:", st.marks3)
            print("Total Marks:", st.total_marks)
            print("Average Marks:", st.average_marks)
            return
def calculate_average_and_rank():
    total_students = len(students)
    if total_students == 0:
        print("No students to calculate average and rank.")
        return
    total_marks_sum = sum(st.total_marks for st in students)
    average_marks = total_marks_sum / total_students
    print(f"Class Average Marks: {average_marks:.2f}")
    sorted_students = sorted(students, key=lambda st: st.total_marks, reverse=True)
    print("\nRank List:")
    for rank, st in enumerate(sorted_students, start=1):
        print(f"Rank {rank}: {st.name} (Reg No: {st.reg}) - Total Marks: {st.total_marks}")
students = []
n = int(input("Enter the number of students: "))
for i in range(n):
    name = input("Enter the name: ")
    reg = int(input("Enter the reg no: "))
    marks1 = int(input("Enter the marks1: "))
    marks2 = int(input("Enter the marks2: "))
    marks3 = int(input("Enter the marks3: "))
    st = Student(name, reg, marks1, marks2, marks3)
    students.append(st)
k = int(input("Enter the reg number of the student whose details are needed: "))
display_student_details(k)
calculate_average_and_rank()