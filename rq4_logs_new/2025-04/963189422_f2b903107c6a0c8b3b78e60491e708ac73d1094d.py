from flask import Flask, render_template

app = Flask(__name__)

@app.route("/")
def hello_world():
    return "Hello, world! Hai Cantik</p!"
    
@app.route('/about/')
def about():
    return "<p> Nama Saya Azizah</p>"

@app.route('/nama/<string:nama>')
def getnama(nama):
    return "nama saya Azizah1"

@app.route("/lembar/flaskland/")
def flaskland():
    return render_template("flaskland.html")

@app.route("/lembar/flaskland2/")
def flaskland2():
    return render_template("flaskland2.html")


@app.route('/user/<name>')  # Variabel name diambil dari URL
def user(name):
    return f"Hello, {name}!"

@app.route('/user1/<int:user_id>')  # Hanya menerima angka
def user_id(user_id):
    return f"User ID: {user_id}"

@app.route('/profile/<name>')
def profile(name):
    return render_template('profile.html', username=name)

app_name = "My Flask App"  # Variabel global

@app.route('/')
def home():
    return f"Welcome to {app_name}!"


if __name__ == "__main__":
    app.run(debug=False)