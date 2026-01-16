from flask import Flask
from flask import render_template
import requests

app = Flask(__name__)


@app.route("/")
def home():
    return render_template('index.html')


@app.route("/<name>")
def hello(name):
    user = str(name).title()

    age_response = requests.get(f"https://api.agify.io?name={name}")
    age = age_response.json()["age"]

    gender_response = requests.get(f"https://api.genderize.io?name={name}")
    gender = gender_response.json()["gender"]

    return render_template('index.html',
                           name=user,
                           gender=gender,
                           age=age)


@app.route("/blog")
def get_blog():
    blog_url = "https://api.npoint.io/c790b4d5cab58020d391"
    response = requests.get(blog_url)
    all_posts = response.json()
    return render_template("blog.html",
                           posts=all_posts)


if __name__ == "__main__":
    app.run(debug=True)