from tkinter import *
from tkinter.filedialog import asksaveasfile
from cgitb import reset
colorbg='white'
colorbgboard="#7700ba"
screen=Tk()
screen.geometry('500x500')
screen.title('register form')
screen.configure(bg=colorbg,borderwidth=5,highlightthickness=7,highlightcolor=colorbgboard)


def sabtnam():
    file=asksaveasfile()
    file.write(firstname_entry.get()+'\n')
    file.write(lastname_entry.get()+'\n')
    file.write(tahsilat.get()+'\n')
    file.write(reshte11.get()+',')
    file.write(reshte22.get()+',')
    file.write(reshte33.get()+',')
    file.write(reshte44.get()+',')
    file.write(address_entry.get(1.0,END)+'\n')
    file.write(phone_entry.get()+'\n')
    file.close()
def clear():
    firstname_entry.delete(0,END)
    lastname_entry.delete(0,END)
    phone_entry.delete(0,END)
    tahsilat.set(reset)
    jender.set(reset)
    reshte11.set(0)
    reshte22.set(0)
    reshte33.set(0)
    reshte44.set(0)
    


formheading=Label(text='فرم ارزیابی',font=('b nazanin',25,"bold"),bg=colorbg,fg='#c17ce6',width='300',height='3')
formheading.pack()

firstname_text=Label(text=':نام',font=('b nazanin',14,"bold"),bg=colorbg,fg='#c17ce6')
firstname_text.place(x=1200,y=100)

firstname=StringVar()
firstname_entry=Entry(textvariable=firstname,width='60',border='3',highlightthickness=3,highlightcolor=colorbgboard,highlightbackground=colorbgboard,background=colorbg)
firstname_entry.place(x=800,y=100)

lastname=StringVar()
lastname_text=Label(text=': نام خانوادگی',font=('b nazanin',14,"bold"),bg=colorbg,fg='#c17ce6')
lastname_text.place(x=1200,y=150)

lastname_entry=Entry(textvariable=lastname,width='60',border='3',highlightthickness=3,highlightcolor=colorbgboard,highlightbackground=colorbgboard,background=colorbg)
lastname_entry.place(x=800,y=150)

tahsilat_text=Label(text=':تحصیلات',font=('b nazanin',14,"bold"),bg=colorbg,fg='#c17ce6')
tahsilat_text.place(x=1200,y=200)
tahsilat=StringVar()
tahsilat1=Radiobutton(screen,text=':دانش آموز',variable=tahsilat,value='danesh amoz',font=('b nazanin',14,"bold"),bg=colorbg,fg='#c17ce6',activebackground='#1f2421',activeforeground=colorbgboard).place(x=1200,y=250)

tahsilat2=Radiobutton(screen,text=': دیپلم',variable=tahsilat,value='diplom',font=('b nazanin',14,"bold"),bg=colorbg,fg='#c17ce6',activebackground='#1f2421',activeforeground=colorbgboard).place(x=1000,y=250)

tahsilat3=Radiobutton(screen,text=': کاردانی',variable=tahsilat,value='kardani',font=('b nazanin',14,"bold"),bg=colorbg,fg='#c17ce6',activebackground='#1f2421',activeforeground=colorbgboard).place(x=800,y=250)

tahsilat4=Radiobutton(screen,text=' : کارشناسی ',variable=tahsilat,value='karshenasi',font=('b nazanin',14,"bold"),bg=colorbg,fg='#c17ce6',activebackground='#1f2421',activeforeground=colorbgboard).place(x=600,y=250)

tahsilat5=Radiobutton(screen,text=': کارشناسی ارشد ',variable=tahsilat,value='karshenasi arshad',font=('b nazanin',14,"bold"),bg=colorbg,fg='#c17ce6',activebackground='#1f2421',activeforeground=colorbgboard).place(x=400,y=250)

tahsilat6=Radiobutton(screen,text=': دکترا ',variable=tahsilat,value='doctora',font=('b nazanin',14,"bold"),bg=colorbg,fg='#c17ce6',activebackground='#1f2421',activeforeground=colorbgboard).place(x=200,y=250)

jender_text=Label(text=': جنسیت ',font=('b nazanin',14,"bold"),bg=colorbg,fg='#c17ce6')
jender_text.place(x=1200,y=350)

jender=StringVar()
jender1=Radiobutton(screen,text=' :خانم',variable=jender,value='khanom',font=('b nazanin',14,"bold"),bg=colorbg,fg='#c17ce6',activebackground='#1f2421',activeforeground=colorbgboard).place(x=1000,y=350)

jender2=Radiobutton(screen,text=' :آقا',variable=jender,value='aqa',font=('b nazanin',14,"bold"),bg=colorbg,fg='#c17ce6',activebackground='#1f2421',activeforeground=colorbgboard).place(x=800,y=350)

tahsilatfavorit_text=Label(text=':رشته تحصیلی مورد علاقه ',font=('b nazanin',14,"bold"),bg=colorbg,fg='#c17ce6')
tahsilatfavorit_text.place(x=1150,y=400)
reshte11=StringVar()
reshte22=StringVar()
reshte33=StringVar()
reshte44=StringVar()
reshte1=Checkbutton(screen,text='شبکه',font=('b nazanin',14,'bold'),bg=colorbg,fg='#c17ce6',variable=reshte11,activebackground=colorbgboard,activeforeground=colorbgboard,onvalue='network').place(x=1200,y=450)

reshte2=Checkbutton(screen,text='وب',font=('b nazanin',14,'bold'),bg=colorbg,fg='#c17ce6',variable=reshte22,activebackground=colorbgboard,activeforeground=colorbgboard,onvalue='web').place(x=1000,y=450)

reshte3=Checkbutton(screen,text='برنامه نویسی',font=('b nazanin',14,'bold'),bg=colorbg,fg='#c17ce6',variable=reshte33,activebackground=colorbgboard,activeforeground=colorbgboard,onvalue='barname nevisi').place(x=800,y=450)

reshte4=Checkbutton(screen,text='مهارت های پایه',font=('b nazanin',14,'bold'),bg=colorbg,fg='#9b58be',variable=reshte44,activebackground=colorbgboard,activeforeground=colorbgboard,onvalue='maharathaye paye').place(x=600,y=450)

address_text=Label(screen,text=':آدرس',font=('b nazanin',14,'bold'),bg=colorbg,fg='#c17ce6')
address_text.place(x=1250,y=500)
address_entry=Text(screen, height=5,width=50,border=3,highlightthickness=3,highlightcolor=colorbgboard,highlightbackground=colorbgboard,background=colorbg)
address_entry.place(x=800,y=500)

phone_text=Label(text=':تلفن',font=('b nazanin',14,"bold"),bg=colorbg,fg='#c17ce6')
phone_text.place(x=1200,y=600)

phone=StringVar()
phone_entry=Entry(textvariable=phone,width='60',border='3',highlightthickness=3,highlightcolor=colorbgboard,highlightbackground=colorbgboard,background=colorbg)
phone_entry.place(x=800,y=600)

sabtnam_button=Button(screen,text='تبتنام',height=1,width=4,border=3,font=('b nazanin',14,"bold"),bg=colorbg,fg='#c17ce6',activebackground=colorbgboard,activeforeground=colorbgboard,command=lambda:sabtnam())
sabtnam_button.place(x=600,y=500)

clear_button=Button(screen,text='پاک ',height=1,width=4,border=3,font=('b nazanin',14,"bold"),bg=colorbg,fg='#c17ce6',activebackground=colorbgboard,activeforeground=colorbgboard,command=lambda:clear())
clear_button.place(x=400,y=500)


mainloop()