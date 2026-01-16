
# This is the script to apply the Forest tree algorithm to the learning data
# Create a data array to hold the learning data
x_learning = []
# Read file one by one
lengthOfCourse = 0
lengthOfFaculty = 0
lengthOfStudent = 0
with open("result_Course_top") as f:
    for line in f:
        lengthOfCourse = lengthOfCourse + 1
        data_entry = []
        temp = line.split(',')
        for str in temp:
            data_entry.append(int(str))
        x_learning.append(data_entry)
f.closed
with open("result_Faculty_top") as f:
    for line in f:
        lengthOfFaculty = lengthOfFaculty + 1
        data_entry = []
        temp = line.split(',')
        for str in temp:
            data_entry.append(int(str))
        x_learning.append(data_entry)
f.closed
with open("result_Student_top") as f:
    for line in f:
        lengthOfStudent = lengthOfStudent + 1
        data_entry = []
        temp = line.split(',')
        for str in temp:
            data_entry.append(int(str))
        x_learning.append(data_entry)
f.closed
# Build y vector
# Class 0: course. Class 1: Faculty. Class 2: Student
y_learning = []
for i in range(1, lengthOfCourse + 1):
    y_learning.append(0)
for i in range(1, lengthOfFaculty + 1):
    y_learning.append(1)
for i in range(1, lengthOfStudent + 1):
    y_learning.append(2)

# Forests of randomized trees
from sklearn.ensemble import RandomForestClassifier

clf = RandomForestClassifier(n_estimators=10)
clf = clf.fit(x_learning, y_learning)

# test
testDataSize = 30
x_test = []

with open("course_test_top") as f:
    for line in f:
        data_entry = []
        temp = line.split(',')
        for str in temp:
            data_entry.append(int(str))
        x_test.append(data_entry)
f.closed

with open("faculty_test_top") as f:
    for line in f:
        data_entry = []
        temp = line.split(',')
        for str in temp:
            data_entry.append(int(str))
        x_test.append(data_entry)
f.closed

with open("student_test_top") as f:
    for line in f:
        data_entry = []
        temp = line.split(',')
        for str in temp:
            data_entry.append(int(str))
        x_test.append(data_entry)
f.closed
result = clf.predict(x_test)
print(result)