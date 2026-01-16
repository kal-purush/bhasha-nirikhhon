from tinydb import TinyDB, Query
from faker import Faker
import random
from flask import render_template
from flask import Flask, request
from flask_bootstrap import Bootstrap

app = Flask(__name__)
bootstrap = Bootstrap()
# Bootstrap(app)
app.config["BOOTSTRAP_SERVE_LOCAL"] = True
bootstrap.init_app(app=app)


class database:
    def __init__(self):
        self.db = TinyDB('db.json')
        self.Student = Query()

    def insert(self, name, age, grade, chinese, math, english, phonenumber, email):
        self.db.insert({'name': name, 'age': age, 'grade': grade, 'chinese': chinese, 'math': math, 'english': english,
                        'phonenumber': phonenumber, 'email': email})

    def all(self):
        print(self.db.all())
        return self.db.all()

    def search(self, type, content):
        content = content.replace(" ", "")
        if content.find("%")!=-1:
            content = content.split("%")[0]
        if type == "name":
            SearchType = self.Student.name
        elif type == "age":
            SearchType = self.Student.age
            content = int(content)
        elif type == "grade":
            SearchType = self.Student.grade
            content = int(content)
        elif type == "chinese":
            SearchType = self.Student.chinese
            content = int(content)
        elif type == "math":
            SearchType = self.Student.math
            content = int(content)
        elif type == "english":
            SearchType = self.Student.english
            content = int(content)
        elif type == "phonenumber":
            SearchType = self.Student.phonenumber
        elif type == "email":
            SearchType = self.Student.email
        res = self.db.search(SearchType == content)
        print(res)
        return res

    def delete(self, name):
        self.db.remove(self.Student.name == name)

    def clear(self):
        self.db.remove(self.Student.math >= 0)


DB = database()


class student:
    def __init__(self, name, age, grade, chinese, math, english, phonenumber, email):
        self.name = name
        self.age = age
        self.grade = grade
        self.chinese = chinese
        self.math = math
        self.english = english
        self.phonenumber = phonenumber
        self.email = email

    def show(self):
        return self.name, self.age, self.grade, self.chinese, self.math, self.english, self.phonenumber, self.email


def insert(DB, name, age, grade, chinese, math, english, phonenumber, email):
    S = student(name, age, grade, chinese, math, english, phonenumber, email)
    name, age, grade, chinese, math, english, phonenumber, email = S.show()
    DB.insert(name, age, grade, chinese, math, english, phonenumber, email)


def MakeFakeData(DB, num):
    fake = Faker(locale='zh_CN')
    for _ in range(num):
        insert(DB, fake.name(), random.randint(19, 25), random.randint(1, 4), int(100 * random.random()),
               int(100 * random.random()), int(100 * random.random()), fake.phone_number(), fake.ascii_free_email())


def RFRM(DB):
    DB.remove()


@app.route('/', methods=['GET'])
def hello():
    return render_template('search.html')


@app.route('/result', methods=['GET'])
def search():
    try:
        type = request.args.get("type")
        content = request.args.get("content")
        return render_template('result.html', result=DB.search(type, content))
    except:
        return render_template('Wrong.html')


@app.route('/all')
def all():
    return render_template('result.html', result=DB.all())


@app.route('/delete', methods=['GET'])
def delete():
    try:
        content = request.args.get("content")
        DB.delete(content)
        return render_template('delete.html')
    except:
        return render_template('delete.html')


@app.route('/insert', methods=['GET'])
def inserth5():
    try:
        name = request.args.get("name")
        age = int(request.args.get("age"))
        grade = int(request.args.get("grade"))
        chinese = int(request.args.get("chinese"))
        math = int(request.args.get("math"))
        english = int(request.args.get("english"))
        phonenumber = request.args.get("phonenumber")
        email = request.args.get("email")
        insert(DB, name, age, grade, chinese, math, english, phonenumber, email)
        return render_template('insert.html')
    except:
        return render_template('insert.html')


if __name__ == '__main__':
    # MakeFakeData(DB, 20)
    app.run(port=3001)