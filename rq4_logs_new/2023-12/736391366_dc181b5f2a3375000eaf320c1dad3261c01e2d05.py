from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__, template_folder="templates")

todos = [{"title": "Sample Todo", "description": "Sample description", "time": "Sample time", "image": None, "done": False}]

@app.route("/")
def index():
    return render_template("index.html", todos=todos)

@app.route("/add", methods=["POST"])
def add():
    title = request.form['title']
    description = request.form['description']
    time = request.form['time']
    image = request.files['image'] if 'image' in request.files else None

    image_url = process_image(image) if image else None

    todos.append({"title": title, "description": description, "time": time, "image": image_url, "done": False})
    return redirect(url_for("index"))

@app.route("/edit/<int:index>", methods=["GET", "POST"])
def edit(index):
    todo = todos[index]
    if request.method == "POST":
        todo['title'] = request.form["title"]
        todo['description'] = request.form["description"]
        todo['time'] = request.form["time"]
        image = request.files['image'] if 'image' in request.files else None

        todo['image'] = process_image(image) if image else None

        return redirect(url_for("index"))
    else:
        return render_template("edit.html", todo=todo, index=index)

@app.route("/check/<int:index>")
def check(index):
    todos[index]['done'] = not todos[index]['done']
    return redirect(url_for("index"))

@app.route("/delete/<int:index>", methods=["POST"])
def delete(index):
    del todos[index]
    return redirect(url_for("index"))

def process_image(image):
    return "/static/images/placeholder.jpg"

if __name__ == '__main__':
    app.run(debug=True)