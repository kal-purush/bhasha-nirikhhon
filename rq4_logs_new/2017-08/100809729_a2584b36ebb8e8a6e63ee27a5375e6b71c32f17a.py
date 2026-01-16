from flask import Flask, render_template, redirect, request, session
import csv
import functions

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('list.html')


@app.route('/story', methods=['GET', 'POST'])
def form():
    return render_template('form.html')


@app.route('/save-story', methods=['Get', 'POST'])
def save():
    form = request.form
    functions.save_story(form)
    return redirect('/')


if __name__ == "__main__":
    app.run(
        debug=True,
        port=5000
    )