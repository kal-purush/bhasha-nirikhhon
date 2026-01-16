from tkinter import *
from tkinter import messagebox
from random import randint,choice,shuffle
import pyperclip
import json

FONT_NAME = 'Courier'
RED = '#e7305b'


#------------------------------------funtions----------------------------------------------------------------------#

def save():
    web = website_enrty.get()
    ema = email_entry.get()
    pword = password_entry.get()
    new_dict = {web:{'email':ema,'password':pword}}

    if len(web) == 0 or len(pword)==0:
        messagebox.showerror(title="Oops",message='please don\'t leave any fields empty')
    else:
        is_ok = messagebox.askokcancel(message=f'do you want to save?\nemail: {ema}\npassword: {pword}', title=web)
        if is_ok:
            try:
                with open('data.json', 'r') as data_file:
                    data = json.load(data_file)
            except FileNotFoundError:
                with open('data.json', 'w') as data_file:
                    json.dump(new_dict, data_file, indent=4)
            else:
                data.update(new_dict)
                with open('data.json', 'w') as data_file:
                    json.dump(data, data_file, indent=4)
            finally:

                website_enrty.delete(0, END)
                password_entry.delete(0, END)


def pass_gen():
    alphabets = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
                 'u', 'v', 'w', 'x', 'y', 'z']
    symbols = ['!', '#', '$', '%', '&', '(', ')', '*', '+']
    number = ['1', '2', '3', '4', '5', '6', '7', '8', '9']

    # user_alphabet = random.randint(8,10)
    # user_symblos = random.randint(2,4)
    # user_numbers = random.randint(2,4)

    password_list = [choice(alphabets) for _ in range(randint(8,10))]
    password_list += [choice(symbols) for _ in range(randint(2,4))]
    password_list += [choice(number) for _ in range(randint(2,4))]

    shuffle(password_list)
    #new_pass = ''
    # for i in password_list:
    #     new_pass += i
    # print(new_pass)
    final_password = ''.join(password_list)
    # print(final_password)
    password_entry.delete(0,END)
    password_entry.insert(0,final_password)
    pyperclip.copy(final_password)

def search_site():
    web = website_enrty.get()
    try:
        with open('data.json','r') as data_file:
            data = json.load(data_file)
    except FileNotFoundError:
        messagebox.showerror(title="Error", message='there\'s no entry yet')
    else:
        if web in data:
            em = data[web]['email']
            pas = data[web]['password']
            messagebox.showinfo(title=web, message=f'email: {em}\npass: {pas}')
            website_enrty.delete(0, END)
        elif len(web) == 0:
            messagebox.showerror(title="Oops", message='Please enter website name')
            website_enrty.delete(0, END)
        else:
            messagebox.showerror(title="Oops", message='there\'s not data on for this site')
            website_enrty.delete(0, END)

#-------------------------------------UI---------------------------------------------------------------#

#window
window = Tk()
window.title('password manager')
# window.minsize(width=350,height=400)
window.config(pady=20,padx=20)

#canvas
canvas = Canvas(width=300,height=200,highlightthickness=0)
img = PhotoImage(file='lock.png')
canvas.create_image(150,95,image= img)
canvas.grid(row=0, column=1)

#label
my_label = Label()
my_label.config(text='Password',fg=RED,font=(FONT_NAME, 30,'bold'))
my_label.grid(row=1,column=1,sticky='EW')

website = Label()
website.config(text='Website:')
website.grid(row=2,column=0)

email = Label()
email.config(text='Email/Username:')
email.grid(row=3,column=0)

password = Label()
password.config(text='Password:')
password.grid(row=4,column=0)

#entry
website_enrty = Entry()
website_enrty.config(width=21)
website_enrty.grid(row=2,column=1,sticky='EW')
website_enrty.focus()

email_entry = Entry()
email_entry.config(width=35)
email_entry.grid(row=3,column=1,columnspan=2,sticky='EW')
email_entry.insert(0,'name@gmail.com')

password_entry = Entry()
password_entry.config(width=21)
password_entry.grid(row=4,column=1,sticky='EW')

#button
generate = Button()
generate.config(text='Generate Password',width=14,command=pass_gen)
generate.grid(row=4,column=2)

add = Button()
add.config(text='Add',width=36,command=save)
add.grid(row=5,column=1,columnspan=2,sticky='EW')

search = Button()
search.config(text='search',width=14,command=search_site)
search.grid(row=2,column=2,sticky='EW')


window.mainloop()