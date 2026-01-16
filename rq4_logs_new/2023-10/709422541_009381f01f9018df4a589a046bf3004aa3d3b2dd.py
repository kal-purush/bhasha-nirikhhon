import tkinter  as tk
from tkinter import ttk
import sqlite3

# Класс главного окна
class Main(tk.Frame): 
    def __init__(self, root): 
        super().__init__(root) 
        self.init_main() 
        self.db = db 
        self.view_records() 


#  главное окном
    def init_main(self):
        toolbar = tk.Frame(bg = '#d7d7d7', bd = 2)
        toolbar.pack(side=tk.TOP, fill = tk.X)


        #Добавить
        self.add_img = tk.PhotoImage(file='./img/add.png').subsample(7, 7)
        btn_add = tk.Button(toolbar, bg='#d7d7d7', bd = 1, image = self.add_img, command = self.open_child)

        btn_add.pack(side = tk.LEFT)

        # Изменить
        self.upd_img = tk.PhotoImage(file='./img/update.png').subsample(7, 7)
        btn_upd = tk.Button(toolbar, bg='#d7d7d7', bd = 1, image = self.upd_img, command = self.open_update_dialog)

        btn_upd.pack(side = tk.LEFT)

        # Удалить
        self.del_img = tk.PhotoImage(file='./img/delete.png').subsample(7, 7)
        btn_del = tk.Button(toolbar, bg='#d7d7d7', bd = 1, image = self.del_img, command = self.delete_records)

        btn_del.pack(side = tk.LEFT)

         # ПОИСК
        self.search_img = tk.PhotoImage(file='./img/search.png').subsample(7, 7)
        btn_search = tk.Button(toolbar, bg='#d7d7d7', bd = 1, image = self.search_img, command = self.open_search)

        btn_search.pack(side = tk.LEFT)

# ТАБЛИЦА

        #Добавляем столбцы
        self.tree = ttk.Treeview(self, columns = ('ID', 'name', 'phone', 'email', 'salary'),
                                 height = 45, show = 'headings')

        #Добавить параметры 
        self.tree.column('ID', width = 45, anchor = tk.CENTER)
        self.tree.column('name', width = 220, anchor = tk.CENTER)
        self.tree.column('phone', width = 140, anchor = tk.CENTER)
        self.tree.column('email', width = 140, anchor = tk.CENTER)
        self.tree.column('salary', width = 100, anchor = tk.CENTER)

        #Подписи 
        self.tree.heading('ID', text = 'ID')
        self.tree.heading('name', text = 'ФИО')
        self.tree.heading('phone', text = 'Телефон')
        self.tree.heading('email', text = 'E-mail')
        self.tree.heading('salary', text = 'Зарплата')

        #Упаковка
        self.tree.pack(side = tk.LEFT)



# МЕТОДЫ

    # Отображеине данных в TreeView
    def records(self, name, phone, email, salary):
        self.db.insert_data(name, phone, email, salary)
        self.view_records()

    def view_records(self):
        self.db.cur.execute(""" SELECT * FROM workers """)

        [self.tree.delete(i) for i in self.tree.get_children()]
        [self.tree.insert('', 'end', values = row)
         for row in self.db.cur.fetchall()]
        
           
     # метод обновления данных
    def update_record(self, name, phone, email, salary):
        id = self.tree.set(self.tree.selection()[0], '#1')
        self.db.cur.execute(""" UPDATE workers SET name = ?, phone = ?, email = ?, salary = ? WHERE ID = ?""",
                            (name, phone, email, salary, id))
        self.db.conn.commit()
        self.view_records()

    # Метод для удления данных
    def delete_records(self):
        for row in self.tree.selection():
            self.db.cur.execute(""" DELETE FROM workers WHERE ID = ? """,
                                (self.tree.set(row, '#1'),))
        self.db.conn.commit()
        self.view_records()

    # метод поиска
    def search_records(self, name):
        name = ('%' + name + '%')
        self.db.cur.execute(""" SELECT * FROM workers WHERE name LIKE ?""", (name, ))

        [self.tree.delete(i) for i in self.tree.get_children()]
        [self.tree.insert('', 'end', values=row)
         for row in self.db.cur.fetchall()]

#КЛАССЫ
    # Метод вызывающий окно добавление
    def open_child(self):
        Child()


    # Метод вызывающий окно ообновления
    def open_update_dialog(self):
        Update()

    # Метод вызывающий окно поиска
    def open_search(self):
        Search()

# Дочернее окно
class Child(tk.Toplevel):
    def __init__(self):
        super().__init__(root)
        self.init_child()
        self.view = app


    def init_child(self):
        self.title('Добавить работника')
        self.geometry('400x220')
        self.resizable(False, False)
        self.grab_set()
        self.focus_set()


        label_name = tk.Label(self, text='ФИО: ')
        label_name.place(x=50, y=30)
        label_phone = tk.Label(self, text='Телефон: ')
        label_phone.place(x=50, y=60)
        label_email = tk.Label(self, text='E-mail: ')
        label_email.place(x=50, y=90)
        label_salary = tk.Label(self, text='Зарплата: ')
        label_salary.place(x=50, y=120)
        
        self.entry_name = ttk.Entry(self)
        self.entry_name.place(x=200, y=30)
        self.entry_phone = ttk.Entry(self)
        self.entry_phone.place(x=200, y=60)
        self.entry_email = ttk.Entry(self)
        self.entry_email.place(x=200, y=90)
        self.entry_salary = ttk.Entry(self)
        self.entry_salary.place(x=200, y=120)



        # Кнопка закрытия
        self.btn_cancel = ttk.Button(self, text = 'Закрыть', command = self.destroy)
        self.btn_cancel.place(x = 300, y = 170)

        # Кнопка добавления
        self.btn_add = ttk.Button(self, text='Добавить')
        self.btn_add.place(x = 220, y = 170)
        self.btn_add.bind('<Button-1>', lambda event:
                          self.view.records(self.entry_name.get(),
                                            self.entry_phone.get(),
                                            self.entry_email.get(),
                                            self.entry_salary.get()))



# Класс редактирования контактов
class Update(Child):
    def __init__(self):
        super().__init__()
        self.init_update()
        self.db = db
        self.default_data()


    def init_update(self):
        self.title('Редактировать позицию')
        self.btn_add.destroy()

        self.btn_upd = ttk.Button(self, text = "Редактировать")
        self.btn_upd.bind('<Button-1>', lambda event:
                          self.view.update_record(self.entry_name.get(),
                                                self.entry_phone.get(),
                                                self.entry_email.get(),
                                                self.entry_salary.get() ))
        self.btn_upd.bind('<Button-1>', lambda event: self.destroy(), add = '+')
        self.btn_upd.place(x = 200, y = 170)

    def default_data(self):
        id = self.view.tree.set(self.view.tree.selection()[0], '#1')
        self.db.cur.execute(""" SELECT * FROM workers WHERE ID = ? """, (id, ))

        row = self.db.cur.fetchone()
        self.entry_name.insert(0, row[1])
        self.entry_phone.insert(0, row[2])
        self.entry_email.insert(0, row[3])
        self.entry_salary.insert(0, row[4])



# Класс поиска
class Search(tk.Toplevel):
    def __init__(self):
        super().__init__(root)
        self.init_search()
        self.view = app

    def init_search(self):
        self.title('Поис по работникам')
        self.geometry('300x100')
        self.resizable(False, False)
        self.grab_set()
        self.focus_set()

        label_name = tk.Label(self, text = 'ФИО')
        label_name.place(x = 20, y = 20)

        self.entry_name = tk.Entry(self)
        self.entry_name.place(x = 70, y = 20)

        # Кнопка закрытия
        self.btn_cansel = ttk.Button(self, text = 'Закрыть', command = self.destroy)
        self.btn_cansel.place(x = 200, y = 70)

        # Кнопка поиска
        self.btn_search = ttk.Button(self, text = 'Найти')
        self.btn_search.place(x = 70, y = 70)
        self.btn_search.bind('<Button-1>', lambda event:
                            self.view.search_records(self.entry_name.get()))
        self.btn_search.bind('<Button-1>', lambda event: self.destroy(), add = '+')


# База данных
class DB:
    def __init__(self):
        self.conn = sqlite3.connect('Worker.db')
        self.cur = self.conn.cursor()
        self.cur.execute(""" CREATE TABLE IF NOT EXISTS workers(
                            ID INTEGER PRIMARY KEY NOT NULL,
                            name TEXT,
                            phone TEXT,
                            email TEXT,
                            salary INTEGER )""")
        self.conn.commit()
    
    def insert_data(self, name, phone, email, salary):
        self.cur.execute(""" INSERT INTO workers (name, phone, email, salary)
                         VALUES (?, ?, ?, ?)""", (name, phone, email, salary))
        self.conn.commit()




if __name__ == '__main__':
    root = tk.Tk()
    db = DB()
    app = Main(root)
    app.pack()
    root.title('Список сотрудников компании')
    root.geometry('645x450')
    root.resizable(False, False)
    root.configure(bg = 'White')
    root.mainloop()
    