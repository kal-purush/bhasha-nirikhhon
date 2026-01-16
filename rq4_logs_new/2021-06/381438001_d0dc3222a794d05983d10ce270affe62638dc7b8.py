from flask import Flask
from flask import render_template, redirect, url_for, request
from ml import *
from form import YearsForm
from flask_wtf.csrf import CSRFProtect
from config import Config

app = Flask(__name__)
csrf = CSRFProtect(app)
app.config.from_object(Config)

@app.route('/',methods=["GET","POST"])
def hello():
    form=YearsForm()
    if request.method == "GET":
        return render_template("index.html",form=form, salary = "")
    else:
        if form.validate_on_submit():
            ans = predict_salary(form.years.data)
            data = "Rs. " + str(ans)
            return render_template("index.html",form=form, salary =  data)