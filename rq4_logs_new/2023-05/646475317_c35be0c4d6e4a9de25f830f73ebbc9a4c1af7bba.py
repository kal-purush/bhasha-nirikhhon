from tkinter import *
import sys
from tkinter import ttk
import os
import time
import csv
import sqlite3

from tkinter.scrolledtext import ScrolledText
from tkinter.messagebox import*
conn=sqlite3.connect('contacts.db')
#print("database opened")
try:
    conn.execute('''CREATE TABLE PHONEBOOK
                 (FIRST_NAME TEXT PRIMARY KEY NOT NULL,
                 LAST_NAME              TEXT NOT NULL,
                 EMAIL                 TEXT NOT NULL,
                 PHONE                 TEXT  NOT NULL);''')
except:
    #print('existing table')
    print("")
    
def main():
    root=Tk()
    root.title("Sirisha's Phonebook")
    app=login(root)
    
class login:
    
    def __init__(self,master):
        self.master=master
        self.master.minsize(200,200)
        self.master.geometry('200x200')
        self.login_entry=[1]*2
        self.login_labels=[1]*2
        self.texts=['USERNAME','PASSWORD']
        y=40
        for i in range(2):
            self.login_entry[i]=Entry(self.master)
            self.login_entry[i].place(x=70,y=y)
            self.login_labels[i]=Label(self.master,text=self.texts[i])
            self.login_labels[i].place(x=2,y=y)
            y=y+70
        button=Button(self.master,text="ENTER",command=self.enter)
        button.place(x=100,y=170)
        button=Button(self.master,text="SIGN UP")
        button.place(x=40,y=170)
        self.login_entry[1].bind("<return>")
        self.login_entry[1].configure(show='*')
        
    def enter(self): 
        s=self.login_entry[0].get()
        t=self.login_entry[1].get()
        new=Toplevel(self.master)
        app=Back_up(new)
        self.master.geometry('200x200+200+700')
        cursor=conn.execute("SELECT  USERNAME,PASSWORD   from PASSWORD")
        for row in cursor:
            if (s==row[0]|  t==row[1]):
                pass
            else:
                print('wrong credentials')
                
class Back_up:
    
     def  __init__(self,master):
        self.master=master
        self.master.minsize(200,200)
        self.master.geometry('400x400+0+0')
        self.master.maxsize(400,400)
        self.master.configure(bg='black')
        canvas = Canvas(self.master,width=370, height=38,bg='cyan')
        canvas.place(x=15,y=350)
        self.toplabel=Label(self.master,text='Phonebook Management System',
                            font=('verdana',14,'bold'),fg='white',bg='teal')
        self.toplabel.pack(side=TOP,fill=BOTH,expand=NO)
        self.labels=[1]*4
        self.labels1=['FIRST NAME','SURNAME','E-MAIL','PHONE NUMBER']
        self.entries=[1]*4
        y=50
        y1=70
        for i in range(4):
            self.labels[i]=Label(self.master,text=self.labels1[i], font=('cambria',9,'bold'),fg='white',bg='teal',width=40)
            self.labels[i].place(x=50,y=y)
            self.entries[i]=Entry(self.master, font=('cambria',9,'bold'),bg='powder blue',fg='black')
            self.entries[i].place(x=120,y=y1)
            y+=40
            y1+=40
        self.btns=[1]*2
        self.btns1=['SAVE','RESET']
        x=125
        for i in range(2):
            self.btns[i]=Button(self.master,text=self.btns1[i],bg='pink')
            self.btns[i].place(x=x,y=230)
            x=x+90   
        self.btns[0].configure(command=self.save)
        self.btns[1].configure(command=self.reset1)
        self.searchbtn=Button(self.master,text='SEE CONTACT LIST', font=('cambria',9,'bold'),bg='teal',fg='white',command=self.see)
        self.searchbtn.place(x=50,y=270)
        self.fold_btn=Button(self.master,text='SEARCH CONTACT', font=('cambria',9,'bold'),bg='teal',fg='white',command=self.search_window)
        self.fold_btn.place(x=200,y=270)
        self.searchbtn.configure(command=self.see)
        
     def save(self):
        f=self.entries[0].get()
        l=self.entries[1].get()
        e=self.entries[2].get()
        p=self.entries[3].get()
        if f and p != '':
                 conn.execute("INSERT INTO PHONEBOOK VALUES(%r,%r,%r,%r)"%(f,l,e,p));
                 conn.commit()
        else:
            showwarning(message='Fill mandatory fields' )
        backup=open("contacts.txt",'a')
        contacts=[]
        contacts2=[]
        phone={}
        backup=open("contacts.txt",'a')
        backup2=open("contacts2.txt",'a')
        prompt2=self.entries[3] .get()
        phone2={self.entries[0].get():prompt2}
        for i in range(4):
            prompt=self.entries[i].get()
            phone[self.labels1[i]]=prompt
        contacts.append(phone)
        print(contacts,file=backup)
        print(phone2,file=backup2)
        with open("cont.txt",'a') as contfile:
                        contfileWriter=csv.writer(contfile)
                        contfileWriter.writerow([self.entries[0].get(),self.entries[1].get(),
                                                 self.entries[2].get(),self.entries[3].get()])
                        print("Record has been written to file")
                        contfile.close()
                        
     def reset1(self):
         for i in range(4):
             self.entries[i].delete(0,END)
             
     def search_window(self):
         self.membertype = ttk.Combobox(self.master,state='read-only')
         self.membertype.place(x=110,y=300)
         self.membertype['value']=['SEARCH FIRST NAME','SEARCH LAST NAME','SEARCH PHONE']
         self.membertype.current(0)
         self.entry1=Entry(self.master,font=('cambria',13,'bold'),bg='cyan',fg='dark blue')
         self.entry1.place(x=30,y=325)
         self.searchbtn=Button(self.master,text='SEARCH',command=self.search)
         self.searchbtn.place(x=250,y=322)
         
     def search(self) :
         cursor=conn.execute("SELECT FIRST_NAME,LAST_NAME,EMAIL,PHONE from PHONEBOOK")
         s=self.entry1.get()
         p=self.membertype.get()
         if p=='SEARCH FIRST NAME':
             for row in cursor:
                  if row[0]==s:
                             showinfo(title="Search Results" ,message='{} {}\n{}\n'.format(row[0],row[1],row[3]))
         if p=='SEARCH LAST NAME':
             for row in cursor:
                  if row[1]==s:
                             showinfo(title="Search Results" ,message='{} {}\n{}\n'.format(row[0],row[1],row[3]))
         if p=='SEARCH PHONE':
             for row in cursor:
                  if row[3]==s:
                             showinfo(title="Search Results" ,message='{} {}\n{}\n'.format(row[0],row[1],row[3]))
                             
     def see(self):
             cursor=conn.execute("SELECT FIRST_NAME,LAST_NAME,EMAIL,PHONE from PHONEBOOK")
             new=Toplevel()
             new.geometry('240x300+500+0')
             new.maxsize(240,300)
             new.configure(bg='pink')
             toplabel=Label(new,text='Contacts',font=('cambria',12,'bold'),bg='pink')
             toplabel.place(x=50,y=10)
             self.listbox=ScrolledText(new,font=('cambria',9,'bold'),height=17,width=25,bd=5,bg='black',fg='white')
             self.listbox.place(x=20,y=30)
             backup=open("contacts2.txt").read()
             self.listbox.insert(END,backup)
             count=0
             for row in cursor:
                 self.listbox.insert(END,'FIRST NAME: {}'.format(row[0]))
                 self.listbox.insert(END,'\n')
                 self.listbox.insert(END,'LAST NAME: {}'.format(row[1]))
                 self.listbox.insert(END,'\n')
                 self.listbox.insert(END,'EMAIL: {}'.format(row[2]))
                 self.listbox.insert(END,'\n')
                 self.listbox.insert(END,'PHONE: {}'.format(row[3]))
                 self.listbox.insert(END,'\n\n')
                 count=count+1
             self.bottomlabel=Label(new,text='{} contacts'.format(count),font=('cambria',9,'bold'))
             self.bottomlabel.place(x=90,y=278)     
                       
if __name__=='__main__':
    main()