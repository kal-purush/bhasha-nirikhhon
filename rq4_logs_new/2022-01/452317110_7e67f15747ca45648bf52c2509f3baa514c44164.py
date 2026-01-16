# import re
from flask import Flask
from flask import render_template #handles html


app=Flask(__name__)

##test before introducing render_template
# @app.route('/')
# # @app.route("/about/") #means the content is in about page
# def home():
#     return "Website home page goes here!"


# @app.route("/about")
# def about():
#     return "this is the about page"


##using render template and creating template folder
##to keep html files
@app.route("/")
def home():
    return render_template('home.html')

@app.route("/about")
def about():
    return render_template("about.html")


    
if __name__ == "__main__":
    app.run(debug=True)