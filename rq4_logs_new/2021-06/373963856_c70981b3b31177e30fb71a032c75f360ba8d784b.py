from setup_db import *
import sqlite3
from flask import Flask, render_template, request, redirect, url_for, g, flash, session
from werkzeug.security import generate_password_hash, check_password_hash


app = Flask(__name__)
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'

### DATABASE ###
DATABASE = './database.db'


def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()



### LOGIN ###

def valid_login(username, password):
    """Checks if username-password combination is valid."""
    # user password data typically would be stored in a database
    conn = get_db()

    hash = get_hash_for_login(conn, username)
    # the generate a password hash use the line below:
    # generate_password_hash("rawPassword")
    if hash != None:
        return check_password_hash(hash, password)
    return False

@app.route("/login", methods=["GET", "POST"])
def login():
    LoggedIn = False
    conn = get_db()
    if session.get("username", None) != None:
        LoggedIn = True
    if request.method == "POST":  # if the form was submitted (otherwise we just display form)
        if valid_login(request.form["username"], request.form["password"]):
            # conn = get_db()
            user = get_user_by_name(conn,request.form["username"])
            print(user)
            session["username"] = user["username"]
            session["userid"] = user["userid"]
            session["role"] = user["role"]
            return redirect(url_for("index"))
            LoggedIn = True
            print(LoggedIn)
        else:
            flash("Invalid username or password!")
            print("Invalid username or password!")
    return render_template("login.html", title="Login", username=session.get("username", None), role=session.get("role", None), LoggedIn = LoggedIn)

@app.route("/logout")
def logout():
    session.pop("username")
    session.pop("userid")
    session.pop("role")
    return redirect(url_for("index"))


### REGISTER ###
@app.route("/register", methods=["GET", "POST"])
def register():
    LoggedIn = False
    #conn = get_db()
    if session.get("username", None) != None:
        LoggedIn = True
    #validate username
    username = request.form.get("newusername","").strip()
    if username == "":
        flash("Enter username")
        return render_template("register.html", title = "Register",  username=session.get("username", None), role=session.get("role", None), LoggedIn = LoggedIn)
    if len(username) < 4:
        flash("Username must have at least 4 characters")
        return render_template("register.html", title = "Register",  username=session.get("username", None), role=session.get("role", None), LoggedIn = LoggedIn)

    pw = request.form.get("newpassword","")
    if pw == "":
        flash("Enter password")
        return render_template("register.html", title = "Register",  username=session.get("username", None), role=session.get("role", None), LoggedIn = LoggedIn)
    
    hash = generate_password_hash(pw)
    
    conn = get_db()
    id = add_user(conn, username, hash)
    if id == -1:
        flash("Username already taken")
        return render_template("register.html", title = "Register",  username=session.get("username", None), role=session.get("role", None), LoggedIn = LoggedIn)

    print(username)
    user = get_user_by_name(conn, username)
    session["userid"] = user["userid"]
    session["username"] = user["username"]
    session["role"] = user["role"]
    return redirect(url_for("index"))


###  PRODUCT ###
@app.route("/product/<int:prod_id>", methods=["GET", "POST"])
def product(prod_id):
    LoggedIn = False
    conn = get_db()
    if session.get("username", None) != None:
        LoggedIn = True
    product = get_product_by_prod_id(conn, prod_id)
    name = product["name"]
    
    size = request.args.get("size")
    quantity = request.args.get("count")
    totalprice = 0
    productscart = get_orders_by_userid(conn, session["userid"])
    #price = get_price_by_prod_id(conn, prod_id)
    #user = get_user_by_name()

    if request.method == "POST":
        size = request.form["size"]
        quantity = request.form["count"]
        totalprice = int(quantity) * float(product["price"])
        add_order(conn, session["userid"], prod_id, quantity, size, False, totalprice)
        
        return redirect(url_for("index"))

    return render_template("product.html", title = name, username=session.get("username", None), role=session.get("role", None), LoggedIn = LoggedIn, prod_id = prod_id, product = product)

### CART ###
@app.route("/cart", methods=["GET", "POST"])
def cart():
    LoggedIn = False
    conn = get_db()
    total = 0
    if session.get("username", None) != None:
        LoggedIn = True
    products = get_orders_by_userid(conn, session["userid"])
    for product in products:
        total += float(product["total"])
    
    if request.method == "POST":
        buy(conn, session["userid"])
        return redirect(url_for("index"))
    
    return render_template("cart.html", title="Cart", username=session.get("username", None), role=session.get("role", None), LoggedIn = LoggedIn, products = products, total = total)


@app.route("/remove/<int:order_id>", methods=["GET", "POST"])
def remove(order_id):
    LoggedIn = False
    conn = get_db()
    if session.get("username", None) != None:
        LoggedIn = True
    remove = remove_product_from_order(conn, order_id)
    return redirect(url_for("cart"))
    

### USER ###
@app.route("/user")
def user():
    LoggedIn = False
    conn = get_db()
    if session.get("username", None) != None:
        LoggedIn = True
    user = get_user_by_name(conn, session["username"])
    name = user["username"]
    if session.get("role", None) == "admin":
        purchases = select_orders(conn)
    else:
        purchases = get_orders_purchased_by_userid(conn, session["userid"])

    return render_template("user.html", title = name, username=session.get("username", None), role=session.get("role", None), LoggedIn = LoggedIn, user = user, purchases = purchases)


### ADD ##
@app.route("/add", methods=["GET", "POST"])
def add():
    LoggedIn = False
    name = ""
    price = 0
    image = ""
    descr = ""
    
    conn = get_db()
    if session.get("username", None) != None:
        LoggedIn = True

    name = request.args.get("namep")
    price = request.args.get("pricep")
    image = str(request.args.get("imagep"))
    descr = request.args.get("descrp")

    if request.method == "POST":
        name = request.form["namep"]
        price = request.form["pricep"]
        image = str(request.form["imagep"])
        descr = request.form("descrp")
        add_product(conn, name, price, image, descr)

    
    return render_template("add.html", title = "Add product", username=session.get("username", None), role=session.get("role", None), LoggedIn = LoggedIn)

### Index ###
@app.route("/")
def index():
    LoggedIn = False
    conn = get_db()
    if session.get("username", None) != None:
        LoggedIn = True
    return render_template("index.html", username=session.get("username", None), role=session.get("role", None), LoggedIn = LoggedIn, products = select_products(conn))

if __name__ == "__main__":
    app.run(debug = True)