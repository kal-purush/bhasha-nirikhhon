
from re import L
from stat import ST_UID
from tkinter  import *
from turtle import width
import mysql.connector
mydb =mysql.connector.connect(host="localhost",user="root",passwd="Shah@1234",database ="scholarship")
mycursor = mydb.cursor(buffered=True)
cursor1=mydb.cursor(buffered=True)
cursor2=mydb.cursor(buffered=True)
cursor3=mydb.cursor(buffered=True)
cursor4=mydb.cursor(buffered=True)
global stu_Id

def apply():
    mycursor.execute("select available_no from Snames where s_Id=%s",(id))
    x= mycursor.fetchone()
    if x==0 :
        Label(ascreen,text ="Seats not available").pack()
    else:
        try:
            mycursor.execute("SET autocommit = 0;")
            mycursor.execute("START TRANSACTION")
            mycursor.execute("Update Snames available_no = %s where s_Id=%s;Insert into takes values(%s,%s)",(x-1,id,stu_id,id))
            mycursor.execute("COMMIT")
            Label(ascreen,text="Successfully Applied")
            
        except mysql.connector.Error as error:
            print("Failed to update record to database rollback: {}".format(error))
            mycursor.execute("ROLLBACK")
        
def s_display():
    global ascreen
    ascreen=Toplevel(root)
    ascreen.title("List of scholarship")
    ascreen.geometry("1600x1200")
    Label(ascreen,text="List of scholarships available to apply").pack()
    
    for i in l:
        temp_text = i
        Label(ascreen, text=temp_text).pack()
    Label(ascreen,text="").pack()
    Label(ascreen,text="Enter the S_ID of the scholarship in which you want to apply").pack()
    global id
    id = IntVar()
    id_entry = Entry(ascreen,textvariable= id).pack()
    Button(ascreen,text="Apply",width = "10" ,height = "1",command=apply).pack()
def state_query():
    try:
        cursor1.execute("SET autocommit = 0;")
        cursor1.execute("START TRANSACTION")
        cursor1.execute("select * from Snames where s_Id in (select s_Id from Eligibility as e natural join Students as s natural join Stu_school natural join Stu_college natural join School_eligibility natural join College_eligibility where e.gender = s.gender and e.caste=s.caste and e.`parental_income(in lac)`>=s.`parental_income(in lac)` and e.state = s.state and ((percentage is not null and percentage >= reqd_percentage)or(gpa is not null and gpa >= reqd_gpa)) and stu_Id=%s)",(stu_id))
        global l
        l=cursor1.fetchall()
        cursor1.execute("COMMIT")
        s_display()
    except mysql.connector.Error as error:
        print("Failed to update record to database rollback: {}".format(error))
        mycursor.execute("ROLLBACK")
def all_query():
    try:
        cursor2.execute("SET autocommit = 0;")
        cursor2.execute("START TRANSACTION")
        cursor2.execute("select * from Snames where s_Id in (select s_Id from Eligibility as e natural join Students as s natural join Stu_school natural join Stu_college natural join School_eligibility natural join College_eligibility where e.gender = s.gender and e.caste=s.caste and e.`parental_income(in lac)`>=s.`parental_income(in lac)` and ((percentage is not null and percentage >= reqd_percentage)or(gpa is not null and gpa >= reqd_gpa)) and stu_Id=%s)",(stu_id))
        
        global l
        l=cursor2.fetchall()
        cursor2.execute("COMMIT")
        s_display()
    except mysql.connector.Error as error:
        print("Failed to update record to database rollback: {}".format(error))
        mycursor.execute("ROLLBACK")
def s_type():
    screen =Toplevel(root)
    screen.title("Type of scholarship")
    screen.geometry("300x250")
    Label(screen,text="Choose which type of scholarships you want to see").pack()
    Button(screen,text="Your State",width=10,height=1,command=state_query).pack()
    Button(screen,text="All",width=10,height=1,command=all_query).pack()
def school_entry():
    try:
        mycursor.execute("SET autocommit = 0;")
        mycursor.execute("START TRANSACTION")
        mycursor.execute("Insert into Stu_school values(%s,%s)",(stu_id,marks))
        mycursor.execute("COMMIT")
        print("Registered Successfully")
        gscreen.destroy()
    except mysql.connector.Error as error:
        print("Failed to update record to database rollback: {}".format(error))
        mycursor.execute("ROLLBACK")
def school():
    bscreen=Toplevel(root)
    bscreen.title("marks")
    bscreen.geometry("300x250")
    Label(bscreen,text="Enter your percentage").pack()
    global marks
    marks=0.00
    a=Entry(bscreen,textvariable=marks)
    a.pack()
    Button(bscreen,text="next",width = 10,height =1,command=school_entry).pack()
def college_entry():
    try:
        mycursor.execute("SET autocommit = 0;")
        mycursor.execute("START TRANSACTION")
        mycursor.execute("Insert into Stu_college values(%s,%s)",(stu_id,marks))
        mycursor.execute("COMMIT")
        print("Registered Successfully")
        gscreen.destroy()
    except mysql.connector.Error as error:
        print("Failed to update record to database rollback: {}".format(error))
        mycursor.execute("ROLLBACK")
def college():
    cscreen.destroy()
    bscreen=Toplevel(root)
    bscreen.title("marks")
    bscreen.geometry("300x250")
    Label(bscreen,text="Enter your GPA").pack()
    global marks
    marks=0.0
    a=Entry(bscreen,textvariable=marks)
    a.pack()
    Button(bscreen,text="next",width = 10,height =1,command=college_entry).pack()

def choice():
    gscreen.destroy()
    global cscreen
    cscreen = Toplevel(root)
    cscreen.title("Level Of Education")
    cscreen.geometry("300x250")
    Label(cscreen,text="Choose your current level of education").pack()
    Button(cscreen,text="School",width=10,height=1,command=school).pack()
    Button(cscreen,text="College",width=10,height=1,command=college).pack()

def inserting():
    username_info=username.get()
    password_info = password.get()
    name_info = name.get()
    city_info = city.get()
    state_info = state.get()
    pincode_info = pincode.get()
    phone_no_info = phone_no.get()
    stu_Id=9
    
    credential=(username_info,password_info)
    detail=(stu_Id,name_info,city_info,pincode_info,phone_no_info,state_info)
   
    try:
        mycursor.execute("SET autocommit = 0;")
        mycursor.execute("START TRANSACTION")
        mycursor.execute("insert into user_credentials values(%s,%s)",credential)
        mycursor.execute("insert into Students values(%s,%s,%s,%s,%s,%s)",detail)
        print("1")
        mycursor.execute("insert into Stu_user values(%s,%s)",(stu_Id,username_info))
        print("2")
        mycursor.execute("COMMIT")
        global stu_id
        stu_id=stu_Id
        stu_Id=stu_Id+1
        choice()
    except mysql.connector.Error as error:
        print("Failed to update record to database rollback: {}".format(error))
        mycursor.execute("ROLLBACK")
    

def register():
    global gscreen
    gscreen = Toplevel(root)
    gscreen.title("Registration Page")
    gscreen.geometry("700x550")
    global username
    global password,name,city,state,pincode,phone_no
    global username_entry
    global password_entry,name_entry,city_entry,state_entry,pincode_entry,phone_no_entry
    username = StringVar()
    password = StringVar()
    name = StringVar()
    city = StringVar()
    state = StringVar()
    pincode = StringVar()
    phone_no=IntVar()
    Label(gscreen,text= "Please enter the details below").pack()
    Label(gscreen,text= "").pack()
    Label(gscreen,text= "Username * ").pack()
    username_entry =Entry(gscreen,textvariable=username)
    username_entry.pack()
    Label(gscreen,text= "name * ").pack()
    name_entry =Entry(gscreen,textvariable=name)
    name_entry.pack()
    Label(gscreen,text= "gender(MALE/FEMALE) * ").pack()
    city_entry =Entry(gscreen,textvariable=city)
    city_entry.pack()
    Label(gscreen,text= "state * ").pack()
    state_entry =Entry(gscreen,textvariable=state)
    state_entry.pack()
    Label(gscreen,text= "caste(SC/ST/GEN/OBC) * ").pack()
    pincode_entry =Entry(gscreen,textvariable=pincode)
    pincode_entry.pack()
    Label(gscreen,text= "parental_income * ").pack()
    phone_no_entry =Entry(gscreen,textvariable=phone_no)
    phone_no_entry.pack()
    Label(gscreen,text= "Password * ").pack()
    password_entry= Entry(gscreen,textvariable=password)
    password_entry.pack()
    Button(gscreen,text="Register",width = 10,height=1,command=inserting).pack()

def login_check():
    username_info=username.get()
    password_info = password.get()
    global stu_id
    stu_id=IntVar()
    try:
        mycursor.execute("SET autocommit = 0;")
        mycursor.execute("START TRANSACTION")
        mycursor.execute("select * from user_credentials where username=%s and password=%s",(username_info,password_info))
        result = mycursor.fetchall()
        username_entry.delete(0,END)
        password_entry.delete(0,END)
        if len(result)==0:
            Label(lscreen,text="Wrong id or password").pack()
        else:
            print("Login successful")
            mycursor.execute("COMMIT")
            mycursor.execute("select stu_Id from Stu_user where username =%s",(username_info,))
            stu_id=mycursor.fetchone()
            lscreen.destroy()
            s_type()
    except mysql.connector.Error as error:
        print("Failed to update record to database rollback: {}".format(error))
        mycursor.execute("ROLLBACK")


    


def login():
    global lscreen
    lscreen = Toplevel(root)
    lscreen.title("Login Page")
    lscreen.geometry("300x250")
    global username
    global password
    global username_entry
    global password_entry
    username = StringVar()
    password = StringVar()
    Label(lscreen,text= "Please enter the details below").pack()
    Label(lscreen,text= "").pack()
    Label(lscreen,text= "Username * ").pack()
    username_entry =Entry(lscreen,textvariable=username)
    username_entry.pack()
    Label(lscreen,text= "Password * ").pack()
    password_entry= Entry(lscreen,textvariable=password)
    password_entry.pack()
    Button(lscreen,text="Login",width = 10,height=1,command=login_check).pack()



def start_screen():
    global root
    root = Tk()
    root.geometry("300x250")
    root.title("DBS Project")
    Label(text="Welcome to Scholarship Help Portal",bg="grey",width="300",height="2",font=("Calibri",13)).pack()
    Label(text="").pack()
    Button(text="Login",height="2",width="30",command=login).pack()
    Label(text="").pack()
    Button(text="Register",height="2",width="30",command=register).pack()
    root.mainloop()
start_screen()

