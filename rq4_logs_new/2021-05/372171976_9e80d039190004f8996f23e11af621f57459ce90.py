from mysql.connector import connect, Error
from tkinter import *
import time
from datetime import datetime, date
from tkcalendar import Calendar
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import pandas as pd
import pdfkit
import os

with connect(
    host="localhost",
    user="YOUR_USERNAME",
    password="YOUR_PASSWORD",
    auth_plugin='mysql_native_password'
) as connection:
    query = """
    CREATE DATABASE IF NOT EXISTS money_manager_db;
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        
connection = connect(
    host="localhost",
    user="YOUR_USERNAME",
    password="YOUR_PASSWORD",
    database = "money_manager_db",
    auth_plugin='mysql_native_password'
    )

create_users_query = """
create table IF NOT EXISTS users(
    username varchar(40) PRIMARY KEY,
    password varchar(40) NOT NULL,
    fname varchar(40),
    lname varchar(40),
    email varchar(60) UNIQUE,
    account_created date,
    balance float,
    earnings float,
    expenses float
);
"""
create_transactions_query = """
create table IF NOT EXISTS transactions(
    username varchar(40),
    transaction_stamp datetime PRIMARY KEY,
    category varchar(50) NOT NULL,
    note varchar(86),
    debit_credit INT NOT NULL,
    amount float NOT NULL,
    balance float NOT NULL,
    FOREIGN KEY (username) REFERENCES users(username)
);
"""
with connection.cursor() as cursor:
    try:
        cursor.execute(create_users_query)
        connection.commit()
    except:
        print("error, cant create table")
with connection.cursor() as cursor:
    try:
        cursor.execute(create_transactions_query)
        connection.commit()
    except:
        print("error, cant create table")

def backHome(obj, frame):
    frame.destroy()
    obj.setFrameHome()

def featuresMenu(window, username):
    fmenu = Frame(window, width=750, height=550, bg="#FBD5BD")
    fmenu.propagate(0)
    fmenu.pack(side=TOP)
    
    query = "Select * from users where username='%s'" % (username)
    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        fname = result[2]
        lname = result[3]
        email = result[4]
        balance = "Balance: "+ str(result[6])
        earnings = "Overall Earnings: "+ str(result[7])
        expenses = "Overall Expenses: "+ str(result[8])
        
    namevar = fname+" " +lname
    lmenu_name = Label(fmenu, text=namevar, font=("Arial",20), bg="#FBD5BD")
    lmenu_name.place(x=10, y = 20)
    lmenu_username = Label(fmenu, text=username, font=("Arial",15), bg="#FBD5BD")
    lmenu_username.place(x=10, y = 70)
    lmenu_email = Label(fmenu, text=email, font=("Arial",15), bg="#FBD5BD")
    lmenu_email.place(x=10, y = 120)
    lmenu_balance = Label(fmenu, text=balance, font=("Arial",15), bg="#FBD5BD")
    lmenu_balance.place(x=10, y = 170)
    lmenu_earnings = Label(fmenu, text=earnings, font=("Arial",15), bg="#FBD5BD")
    lmenu_earnings.place(x=10, y = 220)
    lmenu_expenses = Label(fmenu, text=expenses, font=("Arial",15), bg="#FBD5BD")
    lmenu_expenses.place(x=10, y = 270) 
    btmenu_logout = Button(fmenu, text = "Logout", fg = "black",bd = 3, bg = "#c8aa97",
                            cursor = "hand2", command = lambda: backHome(reg_login_obj, fmenu ))
    btmenu_logout.place(x=10, y=320)
    btmenu_logout.config(font = ("Arial", 13))

    btmenu_add = Button(fmenu, text = "Add new Earning/Expense", fg = "black",bd = 5, bg = "#c8aa97", 
                        cursor = "hand2", command = lambda: add_obj.menuGUI(fmenu, username))
    btmenu_add.place(x=300, y=20, width=400, height=70)
    btmenu_add.config(font = ("Arial", 15))

    btmenu_analysis = Button(fmenu, text = "View analysis of your money usage", fg = "black",bd = 5, bg = "#c8aa97", 
                        cursor = "hand2", command = lambda: analysis_obj.analysisGUI(fmenu, username))
    btmenu_analysis.place(x=300, y=150, width=400, height=70)
    btmenu_analysis.config(font = ("Arial", 15))

    btmenu_daily = Button(fmenu, text = "View money usage History", fg = "black",bd = 5, bg = "#c8aa97",
                        cursor = "hand2", command = lambda: history_obj.calendarGUI(fmenu, username))
    btmenu_daily.place(x=300, y=270, width=400, height=70)
    btmenu_daily.config(font = ("Arial", 15))

    btmenu_monthly = Button(fmenu, text = "Generate money usage Statement", fg = "black",bd = 5,  bg = "#c8aa97",
                        cursor = "hand2", command = lambda: statement_obj.statementGUI(fmenu, username))
    btmenu_monthly.place(x=300, y=400, width=400, height=70)
    btmenu_monthly.config(font = ("Arial", 15))

    
class RegisterLogin:
    def __init__(self, window):
        window.title('Money Manager')
        
    def setFrameHome(self):
        self.fmenu = Frame(window, width=750, height=550)
        self.fmenu.propagate(0)
        self.fmenu.pack(side=TOP)
        self.fmenu_bgimg = PhotoImage(file = "background-img.png", master = self.fmenu)
        self.bg = Label(self.fmenu,image=self.fmenu_bgimg,width=750,height=550)
        self.bg.place(relx=0, rely=0)
        self.lmenu_apptitle = Label(self.fmenu, text="MONEY MANAGER",font=("Broadway",30), bg="#FBD5BD")
        self.lmenu_apptitle.place(x=200,y=100)
        self.lmenu_apptext = Label(self.fmenu, 
                                   text="Manage your earnings and expenses smartly",
                                   font=("Calibri",20), bg="#FBD5BD")
        self.lmenu_apptext.place(x=150,y=200)

#         self.canvas = Canvas( self.fmenu, width = 400, height = 400)
#         self.img = PhotoImage(file = "background-img.png", master = self.canvas)
#         self.canvas.pack(fill = "both", expand = True)
#         self.canvas.create_image( 0, 0, image = self.img, anchor = "nw")
#         self.canvas.create_text( 200, 250, text = "Welcome")
        
        self.btmenu_reg = Button(self.fmenu, text = "Register", fg = "black",bd = 5,  
                                 cursor = "hand2", command = lambda : self.setFrameRegister(), bg="#FBD5BD")
        self.btmenu_reg.place(x=280, y=270, width=90, height=40)
        self.btmenu_reg.config(font = ("Calibri", 15))
        self.btmenu_login = Button(self.fmenu, text = "Login", fg = "black",bd = 5,  
                                   cursor = "hand2", command = lambda : self.setFrameLogin(), bg="#FBD5BD")
        self.btmenu_login.place(x=380, y=270, width=90, height=40)
        self.btmenu_login.config(font = ("Calibri", 15))
        
    def setFrameRegister(self):
        self.fmenu.destroy()
        self.fregister = Frame(window, width=800, height=800, bg="#FBD5BD")
        #bg="#cc9b6d"
        self.fregister.propagate(0)
        self.fregister.pack(side=TOP)

        self.btreg_back = Button(self.fregister, text = "GO BACK TO HOME", fg = "black",bd = 2, bg = "#c8aa97", 
                                     cursor = "hand2", command = lambda : backHome(self, self.fregister) )
        self.btreg_back.place(x=330, y=30)
        self.lblreg_title = Label(self.fregister, text="Create your account",font=("Arial Bold",10), bg="#FBD5BD")
        self.lblreg_title.place(x=320, y=60)
        conditions = """
        The username cannot contain any special character!
        Only letters, underscore and digits are allowed\n
        The password must be between 8-16 characters,
        and must contain at least 1 lowercase character,
        uppercase character, digit and a special character
        (Underscore is not a special character!)
        """
        self.lblreg_conditions = Label(self.fregister, font=("Arial",10),
                                       text=conditions, bg="#FBD5BD")
        self.lblreg_conditions.place(x=200,y=80)
        
        self.lblreg_fname = Label(self.fregister, text="First Name: ", font=("Arial Bold",10),bg="#FBD5BD")
        self.lblreg_fname.place(x=240, y=240)
        self.entryreg_fname = Entry(self.fregister, font=('arial', 10), width=28, bg="#eee")
        self.entryreg_fname.place(x=350, y=240)
        self.lblreg_lname = Label(self.fregister, text="Last Name: ", font=("Arial Bold",10), bg="#FBD5BD")
        self.lblreg_lname.place(x=240, y=270)
        self.entryreg_lname = Entry(self.fregister, font=('arial', 10), width=28, bg="#eee")
        self.entryreg_lname.place(x=350, y=270)
        self.lblreg_email = Label(self.fregister, text="Email: ", font=("Arial Bold",10), bg="#FBD5BD")
        self.lblreg_email.place(x=240, y=300)
        self.entryreg_email = Entry(self.fregister, font=('arial', 10), width=28, bg="#eee")
        self.entryreg_email.place(x=350, y=300)
        self.lblreg_user = Label(self.fregister, text="Username: ",font=("Arial Bold",10), bg="#FBD5BD")
        self.lblreg_user.place(x=240, y=330)
        self.entryreg_user = Entry(self.fregister, font=('arial', 10), width=28, bg="#eee")
        self.entryreg_user.place(x=350, y=330) 
        self.lblreg_pass = Label(self.fregister, text="Password: ",font=("Arial Bold",10), bg="#FBD5BD")
        self.lblreg_pass.place(x=240, y=360)
        self.entryreg_pass = Entry(self.fregister, font=('arial', 10), width=28, bg="#eee")
        self.entryreg_pass.place(x=350, y=360) 
        
        self.btreg_register = Button(self.fregister, text = "REGISTER", fg = "black",bd = 2, bg = "#c8aa97", 
                                     cursor = "hand2", command = lambda : self.checkRegisterInput())
        self.btreg_register.place(x=340, y=400, width=60, height=30)
        self.lblreg_message = Label(self.fregister, text="",font=("Arial",10), bg="#FBD5BD")
        self.lblreg_message.place(x=220, y=440, width = 300, height=50)
        
    def checkRegisterInput(self):
        username = self.entryreg_user.get()
        password = self.entryreg_pass.get()
        fname = self.entryreg_fname.get()
        lname = self.entryreg_lname.get()
        email = self.entryreg_email.get()
        for ch in username:
            if ch.isalnum()!=True and ch!='_':
                self.lblreg_message.configure(text = "Invalid username")
                return 0
        if len(password) not in range(8,17):
            self.lblreg_message.configure(text = "Invalid Password!")
            return 0
        elif any(ch.islower() for ch in password)!=True:
            self.lblreg_message.configure(text = "Invalid Password!")
            return 0
        elif any(ch.isupper() for ch in password)!=True:
            self.lblreg_message.configure(text = "Invalid Password!")
            return 0
        elif any(ch.isdigit() for ch in password)!=True:
            self.lblreg_message.configure(text = "Invalid Password!")
            return 0
        for ch in password:
            if ch.isalnum()!=True and ch!='_':
                account_created = date.today()
                balance = 0.00
                earnings = 0.00
                expenses = 0.00
                
                query = "SELECT email from users where email = '%s'" % (email)
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchall()
                    if(len(result)!=0):
                        self.lblreg_message.configure(text = "An account already exists with the entered email id!")
                        return 0
            
                query = """
                INSERT INTO users(username, password, fname, lname, email, account_created, balance, earnings, expenses)
                VALUES ('%s', MD5('%s'), '%s', '%s', '%s', '%s', '%f', '%f', '%f')
                """ % (username, password, fname, lname, email, account_created, balance, earnings, expenses)  
                try: 
                    with connection.cursor() as cursor:
                        cursor.execute(query)
                        connection.commit()
                except: 
                    self.lblreg_message.configure(text = "Username is already taken!")
                    connection.rollback()
                    return 0        
                else:
                    self.lblreg_message.configure(text = "Signup Successful")
                    self.fregister.update()
                    time.sleep(3)
                    backHome(self, self.fregister)
                    return 1
        self.lblreg_message.configure(text = "Invalid Password! \nNo special character")
        return 0

    def setFrameLogin(self):
        self.fmenu.destroy()
        self.flogin = Frame(window, width=800, height=800, bg="#FBD5BD")
        self.flogin.propagate(0)
        self.flogin.pack(side=TOP)
        self.bt_login_back = Button(self.flogin, text = "GO BACK TO HOME", fg = "black",bd = 2, bg = "#c8aa97", 
                                     cursor = "hand2", command = lambda : backHome(self, self.flogin) )
        self.bt_login_back.place(x=330, y=100)
        self.lbl_login_title = Label(self.flogin, text="Login to your account",font=("Arial Bold",10), bg="#FBD5BD")
        self.lbl_login_title.place(x=320, y=150)
        self.lbl_login_user = Label(self.flogin, text="Enter Username: ",font=("Arial Bold",10), bg="#FBD5BD")
        self.lbl_login_user.place(x=250, y=200)
        self.entry_login_user = Entry(self.flogin, font=('arial', 10), width=20, bg="#eee")
        self.entry_login_user.place(x=380, y=200) 
        self.lbl_login_pass = Label(self.flogin, text="Enter Password: ",font=("Arial Bold",10), bg="#FBD5BD")
        self.lbl_login_pass.place(x=250, y=250)
        self.entry_login_pass = Entry(self.flogin, font=('arial', 10), width=20, bg="#eee")
        self.entry_login_pass.place(x=380, y=250) 
        self.bt_login_login = Button(self.flogin, text = "Login", fg = "black",bd = 2, bg = "#c8aa97",  
                                     cursor = "hand2", command = lambda: self.checkLoginInput())
        self.bt_login_login.place(x=350, y=300, width=60, height=30)
        self.lbl_login_message = Label(self.flogin, text="",font=("Arial",10), bg="#FBD5BD")
        self.lbl_login_message.place(x=280, y=350, width = 200, height=50)
        
    def checkLoginInput(self):
        username = self.entry_login_user.get()
        password = self.entry_login_pass.get()
        query = ("Select * from users where username = %s and password = MD5(%s)")
        values = (username, password)
        with connection.cursor() as cursor:
            cursor.execute(query, values)
            result = cursor.fetchall()
            if len(result) == 0:
                self.lbl_login_message.configure(text = "User doesn't exist")
                return
            elif len(result) == 1:
                self.flogin.destroy()
                featuresMenu(window, username)
                return
                
class AddEarningExpense:
    def __init__(self, window):
        pass

    def menuGUI(self, frame, username):
        frame.destroy()
        self.fmenu = Frame(window, width=800, height=800, bg="#FBD5BD")
        self.fmenu.propagate(0)
        self.fmenu.pack(side=TOP)

        self.btmenu_earnings = Button(self.fmenu, text = "Add Earnings", fg = "black",bd = 5, bg = "#c8aa97", 
                        cursor = "hand2", command = lambda: self.addGUI(self.fmenu, username, 1))
        self.btmenu_earnings.place(relx=0.5, rely=0.3, width=400, height=70, anchor=CENTER)
        self.btmenu_earnings.config(font = ("Arial", 15))
        self.btmenu_expenses = Button(self.fmenu, text = "Add Expenses", fg = "black",bd = 5, bg = "#c8aa97", 
                        cursor = "hand2", command = lambda: self.addGUI(self.fmenu, username, -1))
        self.btmenu_expenses.place(relx=0.5, rely=0.5, width=400, height=70, anchor=CENTER)
        self.btmenu_expenses.config(font = ("Arial", 15))

    def addGUI(self, frame, username, debit_credit):
        frame.destroy()
        self.fadd = Frame(window, width=800, height=800, bg="#FBD5BD")
        self.fadd.propagate(0)
        self.fadd.pack(side=TOP)
        earning_categories = [
            "Salary",
            "Grants",
            "Rewards",
            "Return on Investments",
            "Refunds",
            "Interests",
            "Others"
        ]
        expense_categories = [
            "Groceries and Vegetables",
            "Food",
            "House bills",
            "Transportation",
            "Education",
            "Items for Home",
            "Health and Fitness",
            "Clothing",
            "Accessories/Beauty",
            "Appliances/Vehicles",
            "Medical spendings",
            "Hobbies",
            "Internet/Mobile Recharges",
            "Online Subscriptions",
            "Entertainment",
            "Travel",
            "Social/Gifts",
            "Insurance",
            "Taxes",
            "Others"
        ]

        self.lbladd_amount =  Label(self.fadd, text="Amount", font=("Arial",10), bg="#FBD5BD")
        self.lbladd_amount.place(relx=0.3, rely=0.2, height = 30)
        self.entryadd_amount = Entry(self.fadd, font=('arial', 10), width=28,  bg="white")
        self.entryadd_amount.place(relx=0.45, rely=0.2, height=30)

        self.lbladd_category =  Label(self.fadd, text="Category", font=("Arial",10), bg="#FBD5BD")
        self.lbladd_category.place(relx=0.3, rely=0.3, height = 30)
        self.category_clicked = StringVar()
        self.category_clicked.set("Select Category")
        if debit_credit==1:
            self.dadd_category = OptionMenu(self.fadd , self.category_clicked , *earning_categories)
        else:
            self.dadd_category = OptionMenu(self.fadd , self.category_clicked , *expense_categories)
        self.dadd_category.place(relx=0.45, rely=0.3, height = 30, width = 200)
    
        self.lbladd_note =  Label(self.fadd, text="Add a note (optional) - Max 150 characters", font=("Arial",10), bg="#FBD5BD")
        self.lbladd_note.place(relx=0.3, rely=0.4, height = 30)
        self.entryadd_note = Entry(self.fadd, font=('arial', 10), width=45,  bg="white")
        self.entryadd_note.place(relx=0.3, rely=0.5, height=60)

        self.btadd_add = Button(self.fadd, text = "Add", fg = "black",bd = 3,  bg = "#c8aa97",
                        cursor = "hand2", command = lambda: self.addTransaction(username, debit_credit))
        self.btadd_add.place(relx=0.3, rely=0.7, height=50, width=150)
        self.btadd_add.config(font = ("Arial", 15))

        self.btadd_cancel = Button(self.fadd, text = "Cancel", fg = "black",bd = 3, bg = "#c8aa97",
                        cursor = "hand2", command = lambda: self.cancel(username))
        self.btadd_cancel.place(relx=0.525, rely=0.7, height=50, width=150)
        self.btadd_cancel.config(font = ("Arial", 15))

        self.lbladd_message = Label(self.fadd, text="", font=("Arial",15), bg="#FBD5BD")
        self.lbladd_message.place(relx=0.5, rely=0.85, height = 30, anchor=CENTER)

    def addTransaction(self, username, debit_credit):
        category = self.category_clicked.get()
        note = self.entryadd_note.get()
        try:
            amount = float(self.entryadd_amount.get())
        except:
            if self.entryadd_amount.get()=="":
                self.lbladd_message.configure(text = "The amount cannot be left empty!")
            else:
                self.lbladd_message.configure(text = "The amount can only accept numbers and decimals!")
            return
        else:
            if category=="Select Category":
                self.lbladd_message.configure(text = "Please select a category!")
                return
            if len(note)>85:
                self.lbladd_message.configure(text = "The note cannot exceed 85 characters!")
                return
            transaction_stamp = datetime.now()

            if debit_credit == 1:
                extract_dbinfo_query= "select balance, earnings from users where username = '%s'"%(username)
                with connection.cursor() as cursor:
                    cursor.execute(extract_dbinfo_query)
                    result = cursor.fetchone()
                    balance = result[0]
                    earnings = result[1]
                    balance += amount
                    earnings += amount

                    update_users_query  = "UPDATE users SET balance = '%f', earnings = '%f' WHERE username = '%s'" % (balance, earnings, username)
                    add_earning_query = """
                    INSERT INTO transactions(username, category, note, transaction_stamp, debit_credit, amount, balance)
                    VALUES ('%s', '%s', '%s', '%s', '%d', '%f', '%f')
                    """ % (username, category, note, transaction_stamp, debit_credit, amount, balance)
                    cursor.execute(update_users_query)
                    cursor.execute(add_earning_query)
                    connection.commit()
                    self.lbladd_message.configure(text = "Earning Added")
                    self.fadd.update()
                    time.sleep(2)
                    self.lbladd_message.configure(text = "Returning to Profile.")
                    self.fadd.update()
                    time.sleep(1)
                    self.lbladd_message.configure(text = "Returning to Profile..")
                    self.fadd.update()
                    time.sleep(1)
                    self.lbladd_message.configure(text = "Returning to Profile...")
                    self.fadd.update()
                    time.sleep(1)
                    self.fadd.destroy()
                    featuresMenu(window, username)

            else:
                extract_dbinfo_query= "select balance, expenses from users where username = '%s'"%(username)
                with connection.cursor() as cursor:
                    cursor.execute(extract_dbinfo_query)
                    result = cursor.fetchone()
                    balance = result[0]
                    expenses = result[1]
                    balance -= amount
                    expenses += amount

                    update_users_query  = "UPDATE users SET balance = '%f', expenses = '%f' WHERE username = '%s'" % (balance, expenses, username)
                    add_expenses_query = """
                    INSERT INTO transactions(username, category, note, transaction_stamp, debit_credit, amount, balance)
                    VALUES ('%s', '%s', '%s', '%s', '%d', '%f', '%f')
                    """ % (username, category, note, transaction_stamp, debit_credit, amount, balance)
                    cursor.execute(update_users_query)
                    cursor.execute(add_expenses_query)
                    connection.commit()
                    self.lbladd_message.configure(text = "Expense Added")
                    self.fadd.update()
                    time.sleep(2)
                    self.lbladd_message.configure(text = "Returning to Profile.")
                    self.fadd.update()
                    time.sleep(1)
                    self.lbladd_message.configure(text = "Returning to Profile..")
                    self.fadd.update()
                    time.sleep(1)
                    self.lbladd_message.configure(text = "Returning to Profile...")
                    self.fadd.update()
                    time.sleep(1)
                    self.fadd.destroy()
                    featuresMenu(window, username)

    def cancel(self, username):
        self.fadd.destroy()
        featuresMenu(window, username)

class MoneyUsageHistory:

    def calendarGUI(self, frame, username):
        frame.destroy()
        self.fcalendar = Frame(window, width=750, height=550, bg="#FBD5BD")
        self.fcalendar.propagate(0)
        self.fcalendar.pack(side=TOP)
        current_date = date.today()
        current_year = int(current_date.strftime("%Y"))
        current_month = int(current_date.strftime("%m"))
        current_day = int(current_date.strftime("%d"))

        self.cal_obj = Calendar(self.fcalendar, selectmode = 'day',
                    year = current_year, month = current_month,
                    day = current_day)
        self.cal_obj.place(relx=0.4, rely=0.28, anchor=CENTER, width=300, height=295)

        self.btcal_gotoprofile = Button(self.fcalendar, text = "Go Back to Profile", fg = "black",bd = 5, bg = "#c8aa97",
                        cursor = "hand2", command = lambda: self.backToProfile(username))
        self.btcal_gotoprofile.place(relx=0.63,rely=0.1, width=150)
        self.btcal_gotoprofile.config(font = ("Arial", 11))

        self.btcal_getdetails = Button(self.fcalendar, text = "View Money usage \non selected date", fg = "black",bd = 5, bg = "#c8aa97", 
                        cursor = "hand2", command = lambda: self.getDetails(username))
        self.btcal_getdetails.place(relx=0.63,rely=0.2, width=150, height=70)
        self.btcal_getdetails.config(font = ("Arial", 11))
        self.lblcal_message = Label(self.fcalendar, text = "", bg="#FBD5BD")
        self.lblcal_message.place(relx=0.63,rely=0.35)

        query = "select * from transactions where username='%s'"%(username)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            for r in result:
                transaction_stamp =  r[1]
                event = transaction_stamp
                self.cal_obj.calevent_create(event, 'transaction', 'Transaction')
        self.cal_obj.tag_config('Transaction', background='yellow', foreground='black')

        # self.scroll_bar = Scrollbar(self.fcalendar)
        # self.scroll_bar.place(x=630,y=320, height=200)
        # mylist = Listbox(self.fcalendar, yscrollcommand = self.scroll_bar.set )
        # for line in range(1, 6):
        #     mylist.insert(END, f" %-5s | Categorycategorycategorycategory| 5000000"%(str(1)))
        #     mylist.insert(END,"__________________________________________________________________________________________________")
        #     mylist.insert(END, "abcd")
        # mylist.place(x=90,y=320, width=540, height=200)
        # self.scroll_bar.config( command = mylist.yview )

    def getDetails(self, username):

        self.ftable= Frame(self.fcalendar, bg="#FBD5BD")
        self.ftable.place(x=50, y=320, height=200)
        from tkinter import ttk
        columns = ( 'Category', 'Amount', 'Note')
        tree = ttk.Treeview(self.ftable, height=8, columns=columns, show='headings')
        tree.grid(row=0, column=0, sticky='news')
        for col in columns:
            tree.heading(col, text=col)
        tree.column(columns[0], width=150, anchor=CENTER)
        tree.column(columns[1], width=120, anchor=CENTER)
        tree.column(columns[2], width=370, anchor=CENTER)
        self.scroll_bar = Scrollbar(self.ftable, orient=VERTICAL, command=tree.yview)
        self.scroll_bar.place(relx=0.97, rely=0, height=186)
        tree.config(yscrollcommand=self.scroll_bar.set)

        # with connection.cursor() as cursor:
        #     cursor.execute('SELECT category, amount, note FROM transactions')
        #     for rec in cursor:
        #         self.tree.insert('', 'end', value=rec)

        selected_date = self.cal_obj.selection_get()
        selected_date_string = selected_date.strftime("%d-%m-%Y")
        self.lblcal_message.config(text = "Selected Date is: " + selected_date_string)
        query = "select category,amount,note,debit_credit from transactions where transaction_stamp LIKE '%s' and username='%s'"%(str(selected_date)+"%", username)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            for rec in result:
                rec = list(rec)
                if rec[3]==1:
                    rec[1] = "+" + str(rec[1])
                else:
                    rec[1] = "-" + str(rec[1])
                if rec[2]=="":
                    rec[2]="---"
                tree.insert('', 'end', value=rec[0:3])

    def backToProfile(self, username):
        self.fcalendar.destroy()
        featuresMenu(window, username)


class Analysis:
    
    def analysisGUI(self, frame, username):
        frame.destroy()
        self.fanalysis = Frame(window, width=750, height=550, bg="white" )
        self.fanalysis.propagate(0)
        self.fanalysis.pack(side=TOP)

        query = "Select balance, earnings, expenses from users where username='%s'" % (username)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            balance = "Balance: "+ str(result[0])
            earnings = "Overall Earnings: "+ str(result[1])
            expenses = "Overall Expenses: "+ str(result[2])
            
        self.lanalysis_balance = Label(self.fanalysis, text=balance, font=("Arial Bold",10), bg="white")
        self.lanalysis_balance.place(relx=0.02,rely=0.05)
        self.lanalysis_earnings = Label(self.fanalysis, text=earnings, font=("Arial Bold",10), bg="white")
        self.lanalysis_earnings.place(relx=0.02,rely=0.11)
        self.lanalysis_expenses = Label(self.fanalysis, text=expenses, font=("Arial Bold",10),  bg="white")
        self.lanalysis_expenses.place(relx=0.02,rely=0.17) 
        self.btanalysis_back = Button(self.fanalysis, text = "GO BACK", fg = "black",bd = 3,  
                                cursor = "hand2", command = lambda: self.backToProfile(username), bg="white")
        self.btanalysis_back.place(relx=0.02,rely=0.26)
        self.btanalysis_back.config(font = ("Arial Bold", 10))

        earning_data = {
            "Salary":0,
            "Grants":0,
            "Rewards":0,
            "Return on Investments":0,
            "Refunds":0,
            "Interests":0,
            "Others":0
        }
        expense_data = {
            "Groceries and Vegetables":0,
            "Food":0,
            "House bills":0,
            "Transportation":0,
            "Education":0,
            "Items for Home":0,
            "Health and Fitness":0,
            "Clothing":0,
            "Accessories/Beauty":0,
            "Appliances/Vehicles":0,
            "Medical spendings":0,
            "Hobbies":0,
            "Internet/Mobile Recharges":0,
            "Online Subscriptions":0,
            "Entertainment":0,
            "Travel":0,
            "Social/Gifts":0,
            "Insurance":0,
            "Taxes":0,
            "Others":0
        }

        current_day = date.today()
        current_year_month = current_day.strftime("%Y-%m")
        query = "SELECT debit_credit, category, amount from transactions where transaction_stamp LIKE '%s' and username='%s'"%(current_year_month+"-"+"%", username)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            for rec in result:
                if rec[0]==1:
                    for c in earning_data:
                        if rec[1]==c:
                            earning_data[c] += rec[2]
                            break
                elif rec[0]==-1:
                    for c in expense_data:
                        if rec[1]==c:
                            expense_data[c] += rec[2]
                            break
        earning_categories  = list(earning_data.keys())
        earning_amounts = list(earning_data.values())
        expense_categories  = list(expense_data.keys())
        expense_amounts = list(expense_data.values())

        # fig = plt.figure(figsize =(10, 7))
        # earnings_chart=plt.pie(earning_amounts)
        # fig.legend(earning_categories, loc = "center right", title="Current month's earnings") 
        # plt.show()
        # canvas = FigureCanvasTkAgg(fig, master =self.fanalysis)  
        # canvas.draw()
        # canvas.get_tk_widget().pack()

        fig1 = plt.Figure(figsize=(3,2), dpi=70)
        ax1 = fig1.add_subplot(121)
        ax1.pie(earning_amounts)
        ax1.legend(earning_categories, loc = "upper left", title="Categories",
                    bbox_to_anchor=(0.95, 0, 0.5, 1))
        ax1.set_title(current_day.strftime("%m-%Y")+" Earnings")
        canvas1 = FigureCanvasTkAgg(fig1, self.fanalysis )
        canvas1.get_tk_widget().place(x=0,y=100 )
        canvas1._tkcanvas.place(x=0,y=220,width=380, height=327 )

        fig2 = plt.Figure(figsize=(3,1), dpi=65)
        ax2 = fig2.add_subplot(121)
        ax2.pie(expense_amounts)
        ax2.legend(expense_categories, loc = "upper left", title="Categories",
                    bbox_to_anchor=(1.05, 0, 0.5, 1.5))
        ax2.set_title(current_day.strftime("%m-%Y")+" Expenses")
        canvas2 = FigureCanvasTkAgg(fig2, self.fanalysis)
        canvas2.get_tk_widget().place(x=100,y=100)
        canvas2._tkcanvas.place(x=330,y=220,width=425,height=327)

        balance_list = []
        transaction_stamp_list = []
        current_year = current_day.strftime("%Y")
        query = "SELECT balance, transaction_stamp from transactions where transaction_stamp LIKE '%s' and username='%s'"%(current_year+"-"+"%", username)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            for rec in result:
                balance_list.append(rec[0])
                transaction_stamp_list.append(rec[1])

        fig3 = plt.Figure(figsize=(3,3), dpi=80)
        ax3 = fig3.add_subplot(111)
        ax3.plot(transaction_stamp_list,balance_list,color='blue')
        ax3.axes.xaxis.set_ticklabels([])
        ax3.set_title("Balance Stats for current year")
        canvas3 = FigureCanvasTkAgg(fig3, self.fanalysis)
        canvas3.get_tk_widget().place(x=100,y=100)
        canvas3._tkcanvas.place(x=180,y=10,width=600, height=200)

    def backToProfile(self, username):
        self.fanalysis.destroy()
        featuresMenu(window, username)


class MoneyUsageStatement:
    def statementGUI(self, frame, username):
        frame.destroy()
        self.fstatement = Frame(window, width=750, height=550, bg="#FBD5BD")
        self.fstatement.propagate(0)
        self.fstatement.pack(side=TOP)

        self.btstatement_goback = Button(self.fstatement, text = "Go back to Profile", fg = "black",bd = 3, bg = "#c8aa97", 
                        cursor = "hand2", command = lambda: self.backToProfile(username))
        self.btstatement_goback.place(relx=0.5, rely=0.1, height=40, width=130, anchor=CENTER)
        self.btstatement_goback.config(font = ("Arial", 11))
        self.lblstatement_title =  Label(self.fstatement, text="Select the Month and Year to generate Statement", font=("Arial",15), bg="#FBD5BD")
        self.lblstatement_title.place(relx=0.5,rely=0.18,anchor=CENTER)
        self.lblstatement_month =  Label(self.fstatement, text="Month", font=("Arial",11), bg="#FBD5BD")
        self.lblstatement_month.place(relx=0.35,rely=0.3)
        self.lblstatement_year =  Label(self.fstatement, text="Year", font=("Arial",11), bg="#FBD5BD")
        self.lblstatement_year.place(relx=0.35,rely=0.4)
        
        months_dict = {
            "Jan":"01","Feb":"02","March":"03","April":"04","May":"05","June":"06",
            "July":"07","Aug":"08","Sept":"09","Oct":"10","Nov":"11","Dec":"12"
        }
        months = list(dict.keys(months_dict))
        current_date = date.today()
        current_year = current_date.strftime("%Y")
        years = [str(int(current_year)-1),current_year]

        self.month_clicked = StringVar()
        self.month_clicked.set("Select Month")
        self.dstatement_month = OptionMenu(self.fstatement , self.month_clicked , *months )
        self.dstatement_month.place(relx=0.45, rely=0.3, width = 120)
        self.year_clicked = StringVar()
        self.year_clicked.set("Select Year")
        self.dstatement_year = OptionMenu(self.fstatement , self.year_clicked , *years )
        self.dstatement_year.place(relx=0.45, rely=0.4, width = 120)
        self.btstatement_generate = Button(self.fstatement, text = "Generate PDF", fg = "black",bd = 3, bg = "#c8aa97", 
                        cursor = "hand2", command = lambda: self.generatePDF(username))
        self.btstatement_generate.place(relx=0.5, rely=0.55, height=50, width=150, anchor=CENTER)
        self.btstatement_generate.config(font = ("Arial", 11))
        self.lblstatement_message = Label(self.fstatement, text="", font=("Arial",11), bg="#FBD5BD")
        self.lblstatement_message.place(relx=0.5,rely=0.65,anchor=CENTER)

    def generatePDF(self, username):
        month = self.month_clicked.get()
        year = self.year_clicked.get()
        transactions = []
        months_dict = {
            "Jan":"01","Feb":"02","March":"03","April":"04","May":"05","June":"06",
            "July":"07","Aug":"08","Sept":"09","Oct":"10","Nov":"11","Dec":"12"
        }
        if month=="Select Month":
            self.lblstatement_message.configure(text = "Please select a month")
            return
        if year=="Select Year":
            self.lblstatement_message.configure(text = "Please select a year")
            return
        query = """
        SELECT transaction_stamp, category, note, amount, balance, debit_credit from transactions
        where username='%s' and transaction_stamp LIKE '%s'
        """%(username, year+"-"+months_dict[month]+"-%")
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            if len(result)==0:
                self.lblstatement_message.configure(text = "No earnings or expenses have been made in this month!")
                return
            for rec in result:
                rec = list(rec)
                rec[0] = rec[0].strftime("%d-%m-%Y")
                if rec[5]==1:
                    rec[3] = "+" + str(rec[3])
                else:
                    rec[3] = "-" + str(rec[3])
                if rec[2]=="":
                    rec[2]="---"
                transactions.append(rec[:-1])
        df = pd.DataFrame(transactions, columns = ['Date','Category','Note','Amount','Balance'])

        query_user = "Select fname, lname, email, balance from users where username='%s'"%(username)
        with connection.cursor(buffered=True) as cursor:
            cursor.execute(query_user)
            result=cursor.fetchone()
            name = result[0] + " " + result[1]
            email = result[2]
            balance = result[3]
        
        config = pdfkit.configuration(wkhtmltopdf="C:\\Program Files\\wkhtmltopdf\\bin\\wkhtmltopdf.exe")
        title = 'Money Usage Statement - ' + month + " - " + year
        userdetails = 'Name: ' + name + '<br>' + 'Email id: ' + email + '<br>' + 'Current Balance: ' + str(balance) 
        html_file_name = "statement.html"
        pdf_file_name = username + "_statement_" + month + "_" + year + ".pdf"
        self.to_html_styling(df, html_file_name, title, userdetails)
        download_path = os.path.join(os.path.expanduser('~'),'Downloads', pdf_file_name)
        pdfkit.from_file(html_file_name, download_path, configuration=config)

        self.lblstatement_message.configure(text = "")
        self.btstatement_pdf = Button(self.fstatement, text = "Open PDF", fg = "black",bd = 3, bg = "#c8aa97", 
                        cursor = "hand2", command = lambda: self.open(1, pdf_file_name))
        self.btstatement_pdf.place(relx=0.3, rely=0.7, height=50, width=150)
        self.btstatement_pdf.config(font = ("Arial", 11))
        self.btstatement_html = Button(self.fstatement, text = "Open in Browser", fg = "black",bd = 3, bg = "#c8aa97", 
                        cursor = "hand2", command = lambda: self.open(2, html_file_name))
        self.btstatement_html.place(relx=0.52, rely=0.7, height=50, width=150)
        self.btstatement_html.config(font = ("Arial", 11))

        # f = open('Statement.html','w')
        # a = df.to_html()
        # f.write(a)
        # f.close()
        # pdfkit.from_file('Statement.html', 'example.pdf')

        # table=df.to_html(classes='mystyle')
        # html_string = f'''<html>
        # <head><title>HTML Pandas Dataframe with CSS</title></head>
        # <link rel="stylesheet" type="text/css" href="styles.css"/>
        # <body>
        #     {table}
        # </body>
        # </html>
        # '''
        # pdfkit.from_string(html_string, download_path, configuration=config)

    def to_html_styling(self, df, filename, title, userdetails):
        HTML_TEMPLATE1 = '''
        <html>
        <head>
        <style>
        h2,h3 {
            text-align: center;
            font-family: Helvetica, Arial, sans-serif;
        }
        table { 
            margin-left: auto;
            margin-right: auto;
        }
        table, th, td {
            border: 1px solid black;
            border-collapse: collapse;
        }
        th, td {
            padding: 5px;
            text-align: center;
            font-family: Helvetica, Arial, sans-serif;
            font-size: 90%;
        }
        td:nth-child(1){
            width:20%;
        }
        td:nth-child(2){
            width:20%;
        }
        td:nth-child(3){
            width:20%;
        }
        td:nth-child(4){
            width:20%;
        }
        td:nth-child(5){
            width:20%;
        }
        table tbody tr:hover {
            background-color: #dddddd;
        }
        .wide {
            width: 90%; 
        }
        
        </style>
        </head>
        <body>
        '''
        HTML_TEMPLATE2 = '''
        </body>
        </html>
        '''

        ht = ''
        ht += '<h2> %s </h2>\n' % title
        ht += '<h3> %s </h3>\n' % userdetails
        ht += df.to_html(classes='wide', escape=False)
        with open(filename, 'w') as f:
            f.write(HTML_TEMPLATE1 + ht + HTML_TEMPLATE2)

    def open(self, pdf_html, file_name):
        if pdf_html==1:
            os.startfile(os.path.join(os.path.expanduser('~'),'Downloads', file_name))
        else:
            os.startfile(file_name)

    def backToProfile(self, username):
        self.fstatement.destroy()
        featuresMenu(window, username)

window=Tk()
window.geometry("750x550")
window.resizable(0, 0)
window.wm_iconbitmap('icon-img.ico')
reg_login_obj = RegisterLogin(window)
reg_login_obj.setFrameHome()

add_obj = AddEarningExpense(window)
history_obj = MoneyUsageHistory()
analysis_obj = Analysis()
statement_obj = MoneyUsageStatement()

window.mainloop()
connection.close()