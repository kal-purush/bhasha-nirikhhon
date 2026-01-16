import os
from flask import Flask, flash, redirect, render_template, request, session
from flask_session import Session
from cs50 import SQL
from werkzeug.security import check_password_hash, generate_password_hash


app = Flask(__name__, template_folder='templates')

# Configure session to use filesystem (instead of signed cookies)
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_TYPE"] = "filesystem"
Session(app)

# Configure CS50 Library to use SQLite database
db = SQL("sqlite:///database.db")

# Create users table if it doesn't exist
db.execute(
    """CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE,
        hash TEXT NOT NULL
    )"""
)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        confirmation = request.form.get("confirmation")

        if not username or not password or not confirmation:
            flash("All fields are required", "error")
            return redirect("/signup")

        if password != confirmation:
            flash("Passwords do not match", "error")
            return redirect("/signup")

        existing_user = db.execute(
            "SELECT * FROM users WHERE username = ?", username
        )

        if existing_user:
            flash("Username already exists", "error")
            return redirect("/signup")

        hashed_password = generate_password_hash(password)
        db.execute(
            "INSERT INTO users (username, hash) VALUES (?, ?)", username, hashed_password
        )
        flash("Registration successful. Please log in.", "success")
        return redirect("login")
    else:
        return render_template("signup.html")



@app.route("/login", methods=["GET", "POST"])
def login():
    # Forget any user_id
    session.clear()

    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")

        if not username:
            flash("Invalid username and/or password", "error")
        elif not password:
            flash("Invalid username and/or password", "error")
        else:
            user = db.execute("SELECT * FROM users WHERE username = ?", username)

            if len(user) != 1 or not check_password_hash(user[0]["hash"], password):
                flash("Invalid username and/or password", "error")
            else:
                session["user_id"] = user[0]["id"]
                flash("Login successful", "success")
                return redirect("/home")

    return render_template("login.html")


@app.route("/home")
def home():
    if "user_id" not in session:
        return redirect("/login")
    return render_template("home.html")

@app.route("/logout")
def logout():
    session.clear()
    flash("Logged out successfully", "success")
    return redirect("/login")

@app.route("/exercise")
def exercise_library():
    return render_template("exercise.html")

@app.route("/calories")
def calories_calculator():
    return render_template("calories.html")

@app.route("/bmi")
def bmi_calculator():
    return render_template("bmi.html")

@app.route("/community")
def community():
    return render_template("community.html")

@app.route("/contact")
def contact():
    return render_template("contact.html")

if __name__ == "__main__":
    app.run()