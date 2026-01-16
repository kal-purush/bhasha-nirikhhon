from flask import Flask,render_template,request,redirect,url_for
import model

app = Flask(__name__)

@app.route("/",methods=["GET","POST"])
def login():
    if request.method=="GET":
        return render_template("login.html")
    else:
        username=request.form["username"]
        passwd=request.form["passwd"]
        fullName=request.form["fullName"]
        username_exists=model.check_if_bartender_exists(username)
        if username_exists:
            return render_template("register.html",errormsg=f"an account with the username '{username}' already exists. Enter new username.")
        else:
            model.register_new_bartender(fullName,username,passwd)
            return render_template("login.html",success="You have succesfully registered!")
            
@app.route("/index",methods=["GET","POST"])
def index():
    try:
        username= request.form["username"]
        passwd = request.form["passwd"]
    except:
        pass
    if request.method=="GET":
        bartender_name=model.get_active_bartender_name()
        try:
            message = request.args.get('invoiceMsg')
        except:
            message=""
        return render_template("index.html",name=bartender_name,invoiceMsg=message)
    else:
        valid = model.check_if_username_and_password_is_valid(username,passwd)
        if valid:
            sessionId=model.generate_pwd(12)
            model.set_bartender_status_to_active(sessionId,username)
            return render_template("index.html",name=username)
        else:
            return render_template("login.html",errormsg="INVALID USERNAME AND/OR PASSWORD")
        
@app.route("/register")
def register():
    return render_template("register.html")

@app.route("/table<int:table_id>",methods=["GET","POST"])
def table(table_id):
    if request.method=="GET":
        drinklist=model.get_complete_drink_list()
        bartenderId = model.get_active_bartender_id()
        active_table=model.get_table_bill_from_table_id(table_id)
        try:
            table1_bill=model.get_detailed_table_bill_from_active_table_id(active_table[0])
            total_bill_price=model.get_total_bill_price_from_bill_id(active_table[0])
            total_price=float(total_bill_price)
            
        except:
            bartenderID=model.get_active_bartender_id()
            #Prvo se pojavi da je empty racun, onda doda 0.00
            table1_bill=model.get_table_bill_from_table_id(table_id)
            if not table1_bill:
                model.create_new_table_bill(table_id,bartenderId[0])
            else:
                table1_bill=model.get_table_bill_from_table_id(table_id)
            total_price=0
        return render_template(f"table{table_id}.html",drinklist=drinklist,table1_bill=table1_bill,total_price=total_price,table_id=table_id)
@app.route("/logout")
def logout():
    model.set_bartender_to_logged_out()
    return redirect("/")

@app.route("/remove/<int:table_id>/<int:id>")
def remove(table_id,id):
    model.remove_drink_from_billitems(id)
    return redirect(f"/table{table_id}")

@app.route("/add/<int:table_id>/<int:id>")
def add(table_id,id):
    bill_id=model.get_bill_id_from_active_table_id(table_id)
    model.add_drink_to_billitems(bill_id,id)
    table_id=model.get_table_id_from_bill_id(bill_id)
    return redirect(f"/table{table_id}")

@app.route("/invoice/<int:table_id>/<float:total_price>")
def invoice(table_id,total_price):
    model.close_bill_and_get_invoice(total_price,table_id)
    #IZMENI U render template
    return redirect(url_for(".index",invoiceMsg=f"Table {table_id} bill successfully printed."))

@app.route("/shiftIncome")
def shiftIncome():
    bartenderId = model.get_active_bartender_id()[0]
    total_shift_bills = model.get_all_shift_bills(bartenderId)
    if not total_shift_bills:
        total_shift_bills="/"
    sum_total=model.get_total_shift_income(bartenderId)
    return render_template("shiftIncome.html", total_shift_bills=total_shift_bills,sum_total=sum_total)

if __name__ == "__main__":
    app.run(debug=True)