import tkinter as tk
from tkinter import messagebox, ttk
import mysql.connector
from tkinter import *

def get_value():
    id_value = id_entry_value.get()
    name_value = name_entry_value.get()
    course_value = course_entry_value.get()
    fee_value = fee_entry_value.get()

    


def show_table():
    # table view of data
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="newPassword",
        database='sona'
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM student_information")
    tree = ttk.Treeview(root)
    tree["columns"] = ("id", "name", "course", "fee")
    tree['show'] = 'headings'
    tree.column("id", width=100, minwidth=100, anchor=tk.CENTER)
    tree.column("name", width=200, minwidth=200, anchor=tk.CENTER)
    tree.column("course", width=200, minwidth=200, anchor=tk.CENTER)
    tree.column("fee", width=200, minwidth=200, anchor=tk.CENTER)

    tree.heading("id", text="ID", anchor=tk.CENTER)
    tree.heading("name", text="Name", anchor=tk.CENTER)
    tree.heading("course", text="Course", anchor=tk.CENTER)
    tree.heading("fee", text="Fee", anchor=tk.CENTER)

    i = 0
    for row in cursor:
        tree.insert('', i, text="", values=(row[0], row[1], row[2], row[3]))
        i = i + 1
    tree.pack()
    tree.place(x=0, y=300)


def add_info():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="newPassword",
        database='sona'
    )
    my_cursor = conn.cursor()

    name_value = name_entry_value.get()
    course_value = course_entry_value.get()
    fee_value = fee_entry_value.get()

    query = f"INSERT INTO student_information(name,course,fee) VALUES('{name_value}', '{course_value}', {fee_value})"
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()
    show_table()

def update_info():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="newPassword",
        database='sona'
    )
    my_cursor = conn.cursor()

    id_value = id_entry_value.get()
    name_value = name_entry_value.get()
    course_value = course_entry_value.get()
    fee_value = fee_entry_value.get()

    query = f"UPDATE student_information SET name='{name_value}' , course='{course_value}' , fee={fee_value} WHERE id={id_value} "
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()
    show_table()

def delete_info():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="newPassword",
        database='sona'
    )
    my_cursor = conn.cursor()

    id_value = id_entry_value.get()

    query = f"DELETE FROM student_information WHERE id={id_value} "
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()
    show_table()

root = Tk()
root.title('Registration Form')
root.geometry("700x500")

tk.Label(root, text="Student Registration", fg="black", font=(None, 20)).place(x=400, y=5)
tk.Label(root, text="Student ID").place(x=10, y=10)
tk.Label(root, text="Student Name").place(x=10, y=40)
tk.Label(root, text="Course").place(x=10, y=70)
tk.Label(root, text="Fee").place(x=10, y=100)






id_entry_value = StringVar(root, value="")
ttk.Entry(root, width=20, textvariable=id_entry_value).place(x=150, y=10)


name_entry_value = StringVar(root, value="")
ttk.Entry(root, width=20, textvariable=name_entry_value).place(x=150, y=40)


course_entry_value = StringVar(root, value="")
ttk.Entry(root, width=20, textvariable=course_entry_value).place(x=150, y=70)


fee_entry_value = StringVar(root, value="")
ttk.Entry(root, width=20, textvariable=fee_entry_value).place(x=150, y=100)


ttk.Button(root, text="Add", command=add_info).place(x=10, y=200)
ttk.Button(root, text="Update", command=update_info).place(x=100, y=200)
ttk.Button(root, text="Delete", command=delete_info).place(x=190, y=200)

show_table()
root.mainloop()