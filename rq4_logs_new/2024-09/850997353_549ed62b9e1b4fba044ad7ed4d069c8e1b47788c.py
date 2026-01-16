from flask import Flask, render_template, request

app = Flask(__name__)

class Student:
    def __init__(self, roll, name, marks):
        self.roll = roll
        self.name = name
        self.marks = marks
        self.total = sum(marks)
        self.per = self.total / 5
        self.grade = self.calculate_grade()
        self.result = self.calculate_result()

    def calculate_grade(self):
        if self.per >= 85:
            return "S"
        elif self.per >= 75:
            return "A"
        elif self.per >= 65:
            return "B"
        elif self.per >= 55:
            return "C"
        elif self.per >= 50:
            return "D"
        else:
            return "F"

    def calculate_result(self):
        pass_count = sum(mark >= 50 for mark in self.marks)
        if pass_count == 5:
            return "PASS"
        elif pass_count >= 3:
            return "COMP."
        else:
            return "FAIL"

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        roll = int(request.form['roll'])
        name = request.form['name']
        marks = [int(request.form[f'subject{i}']) for i in range(1, 6)]
        student = Student(roll, name, marks)
        return render_template('result.html', student=student)
    return render_template('index.html')

if __name__ == "__main__":
    app.run(debug=True)