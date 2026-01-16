from flask import Flask, render_template, redirect

app = Flask(__name__)
#
@app.route("/")
@app.route("/index")
def home():
    return render_template("index.html", name='index')

@app.route("/gallery")
def gallery():
    return render_template("gallery.html", name='gallery')

@app.route("/contact")
def contact():
    return render_template("contact.html", name='contact')

@app.route("/about")
def about():
    return render_template("about.html", name='about')


# @app.route('/<filename>')          							#get the URL variable
# def home(filename):
#     return render_template("%s.html" % filename, name = filename)	#The argument should be in templates folder
#
@app.errorhandler(404)
def pageNotFound(e):
    return render_template('404.html'), 404

@app.errorhandler(500)
def pageNotFound(e):
    return render_template('500.html'), 500


if __name__ == "__main__":
    app.run(debug=True)