grade = {'F':0.00,'D0':1.00,'D+':1.50,'C0':2.00,'C+':2.50,'B0':3.00,'B+':3.50,'A0':4.00,'A+':4.50}
temp = input()
N, X = int(temp.split()[0]), float(temp.split()[1])
my_grades = [input().split() for _ in range(N-1)]
my_grades_sum = sum([float(i[0]) * grade[i[1]] for i in my_grades])
num = float(input())
divide = num
for i in my_grades:
    divide += int(i[0])

if float(str((my_grades_sum + (num * 4.5)) / divide)[:4]) <= X:
    print('impossible')
else:
    for i, value in enumerate(grade.values()):
        if float(str((my_grades_sum + (num * value)) / divide)[:4]) > X:
            print(list(grade.keys())[i])
            break