# -*- coding: utf-8 -*-
"""
Created on Fri Feb 28 00:20:16 2020

@author: Manish

"""
import mysql.connector
from tkinter import *
import random


class Game:
    
    def __init__(self):
        
        try:
            self.con = mysql.connector.connect(host='localhost',user='root',passwd='',database='project')
            self.mycursor = self.con.cursor()
            
        except Exception as e:
            print(e)
        
        self.colorset=['purple','orange','red','yellow','blue','green','white','brown','pink','cyan']
        self.timeleft = 30
        self.score=0
        
        
        self.root = Tk()
        self.root.title('CHROMA COMBAT')
        self.root.geometry('500x600')
        self.root.configure(bg='#323232')
        
        
        self.frame= Frame(self.root,bg='#323232')
        
        
        
        Label(self.root,text='<<<<<< Chroma Combat >>>>>>',width=600,bg='#1c1c1b',fg='#a6ff4d',font=('sylfaen',25,'bold')).pack(pady=20)
        self.frame.pack()
        
        
        self.mainMenu()
        
        self.root.mainloop()
        
        
        
        
    def mainMenu(self):
        
        self.clearWindow()
        
        Label(self.frame,text='Returning User',bg='#323232',fg='#a6ff4d',font=('courier new',15,'bold')).pack(pady=20)
        
        login = Button(self.frame,text='LOGIN',bg='#212121',fg='white',width=10,height=1,font=('cambria',15),command=lambda:self.loginMenu())
        login.pack(pady=(8,20))
        
        Label(self.frame,text='New User',bg='#323232',fg='#a6ff4d',font=('courier new',15,'bold')).pack(pady=20)
    
        register = Button(self.frame,text='REGISTER',bg='#212121',fg='white',width=10,height=1,font=('cambria',15),command=lambda:self.registerMenu())
        register.pack(pady=(8,20))    
    
    
    def registerMenu(self):
        
        
        self.clearWindow()
        
        label = Label(self.frame,text='NAME:',fg='#a6ff4d',bg='#323232',font = ('courier new',15))
        label.grid(row=0,column=1,pady=(10,10),padx=40)
        
        
        self.newname=Entry(self.frame)
        self.newname.grid(row=0,column=2,ipady=5)
        self.newname.focus_set()
        
        label = Label(self.frame,text='EMAIL ID:',fg='#a6ff4d',bg='#323232',font = ('courier new',15))
        label.grid(row=1,column=1,pady=(10,10))
        
        self.newemail=Entry(self.frame)
        self.newemail.grid(row=1,column=2,ipady=5)
        
        label = Label(self.frame,text='PASSWORD:',fg='#a6ff4d',bg='#323232',font = ('courier new',15))
        label.grid(row=2,column=1,pady=(10,10))
        
        self.newpassword=Entry(self.frame)
        self.newpassword.grid(row=2,column=2,ipady=5)
        
        register = Button(self.frame,text='REGISTER',bg='#212121',fg='white',width=10,height=1,font=('cambria',15),command=lambda:self.register())
        register.grid(row=4,column=1,columnspan=2,pady=(10,10))
        
        
        back = Button(self.frame,text='Home',bg='#212121',fg='white',width=7,height=1,font=('cambria',15),command=lambda:self.mainMenu())
        back.grid(row=5,column=1,columnspan=2,pady=(20,10))
                      
        self.root.bind('<Return>',self.register)
        
            
    def register(self,event=None):
    
    
    
        try:
            self.mycursor.execute("INSERT into users (name,email,password) values ('{}','{}','{}')".format(self.newname.get(),self.newemail.get(),self.newpassword.get()))
            self.con.commit()
            
            
            self.clearWindow()
            
            label = Label(self.frame,text="Thank You for registering with us.",bg='#323232',fg='#a6ff4d',font=('courier new',16,))
            label.pack(pady=(30,10))
            
            button = Button(self.frame,text="Login to Continue",bg='#212121',fg='white',width=15,height=1,font=('cambria',15),command=lambda:self.loginMenu())
            button.pack(pady=(10,10))
            
            self.root.bind('<Return>',self.loginMenu)
            
            
                    
                          
            
            
        
        except mysql.connector.IntegrityError:
            
            result = Label(self.frame,bg='#323232',fg='white',font= ('courier new',15,'italic'))
            
            
            
            result.configure(text="Email already registered!!!")
                
            result.grid(row=3,column=1,columnspan=2,pady=(10,10))
            
                
    
    def loginMenu(self,event=None):
        
        self.clearWindow()
        
        emaillabel = Label(self.frame,text='EMAIL ID:',fg='#a6ff4d',bg='#323232',font = ('courier new',15))
        emaillabel.grid(row=0,column=0,pady=(10,10))
        
        self.emailField=Entry(self.frame)
        self.emailField.grid(row=0,column=1,ipady=5)
        
        passwordLabel = Label(self.frame,text='PASSWORD:',fg='#a6ff4d',bg='#323232',font = ('courier new',15))
        passwordLabel.grid(row=1,column=0,pady=(10,10))
        
        self.passwordField=Entry(self.frame,show='*')
        self.passwordField.grid(row=1,column=1,ipady=5)
        
        login_button = Button(self.frame,text='LOGIN',bg='#212121',fg='white',width=10,height=1,font=('cambria',15),command=lambda:self.login())
        login_button.grid(row=4,columnspan=2,pady=(10,10))
        
        self.emailField.focus_set()
        
        back = Button(self.frame,text='HOME',bg='#212121',fg='white',width=7,height=1,font=('cambria',15),command=lambda:self.mainMenu())
        back.grid(row=7,columnspan=2,pady=(20,10))

        
        self.root.bind('<Return>',self.login)
        
        
    def login(self,event=None):
        
         try:
        
            
        
            self.mycursor.execute("Select * from users where email='{}' and password='{}'".format(self.emailField.get(),self.passwordField.get()))
            output = self.mycursor.fetchall()
            if(output):
                 
                self.email = self.emailField.get()
                self.password = self.passwordField.get()
                
                self.fetchdetails()
                self.startScreen()
                
                                
                
                
                
            else:
                result=Label(self.frame,text='Incorrect Creditionals,Try again',bg='#323232',fg='white',font=('Verdana',10,'italic'))
                result.grid(row=2,columnspan=2,pady=(10,10))
                
                Label(self.frame,text="New User!!! Register here",bg='#323232',fg='white',font=('Verdana',10,'italic')).grid(row=5,columnspan=2,pady=(10,10))
                Button(self.frame,text='REGISTER',bg='#212121',fg='white',width=10,height=1,font=('cambria',15),command=lambda:self.registerMenu()).grid(row=6,columnspan=2,pady=(10,10))
         
         except:
            
            pass
        
        
    
    def clearWindow(self):
        
        for widget in self.frame.winfo_children():
            widget.destroy()
            
            
    def fetchdetails(self):
        
        self.mycursor.execute("Select name,highscore from users where email='{}'".format(self.email))
        output = self.mycursor.fetchall()
        
        
        self.name = output[0][0]
        self.highscore = output[0][1]
        
        
            
            
    
    def startScreen(self,event=None):
        
        self.clearWindow()
        
        
        
        result=Label(self.frame,text='Welcome {}'.format(self.name.capitalize()),bg='#323232',fg='#fc9630',font=('courier new',20,'italic','bold'))
        result.pack(pady=(30,10))
        
        startButton = Button(self.frame,text='Start Game',bg='#212121',fg='white',width=10,height=1,font=('cambria',15),command=lambda:self.instructionScreen())
        startButton.pack(pady=(20,20))
        
        leaderButton = Button(self.frame,text='Leaderboard',bg='#212121',fg='white',width=12,height=1,font=('cambria',15),command=lambda:self.leaderBoardScreen())
        leaderButton.pack(pady=(20,20))
        
        back = Button(self.frame,text='Logout',bg='#212121',fg='white',width=12,height=1,font=('cambria',15),command=lambda:self.mainMenu())
        back.pack(pady=(40,10))
        
        
        self.root.bind('<Key-Return>',self.instructionScreen)
        
    
    def instructionScreen(self,event=None):
        
        self.score = 0
        
        self.clearWindow()
        
        inst = "Spell the Chroma of word,\n not the word"
        Label(self.frame,text=inst,bg='#323232',fg='#a6ff4d',font = ('courier new',15),height=4).pack(pady=(20,20))
        
        begin=Button(self.frame,text="Let's Go",bg='#212121',fg='white',width=10,height=1,font=('cambria',15),command=lambda:self.gameScreen())
        begin.pack(pady=(20,20))
        
        self.root.bind('<Return>',self.gameScreen)
        
    
    
    def gameScreen(self,event=None):
        
        
        self.clearWindow()
        
        
        
        self.timer = Label(self.frame,text="Time left:"+str(self.timeleft),bg='#323232',fg='#a6ff4d',font=('courier new',12))
        self.timer.grid(row=0,column = 0,pady=(10,10))
        
        self.scoreLabel = Label(self.frame,text="Score:"+str(self.score),bg='#323232',fg='#a6ff4d',font=('courier new',12))
        self.scoreLabel.grid(row=0,column=2)
        
        self.word = Label(self.frame,text="Press Enter \nto Begin",bg='#323232',fg='#a6ff4d',font=('segoe script',18,'bold'))
        self.word.grid(row=1,column=1,pady=(10,10),ipady=8)
        
        self.data = Entry(self.frame,font=('courier new', 15),justify=CENTER)
        
        
        self.root.bind('<Key-Return>',self.startGame)

        
        self.data.grid(row =3,column=1,pady=(10,10))
        
        self.data.focus_set()              
              
        
    def startGame(self,event):
        
        if self.timeleft == 30:
            
            self.countDown()
        
        if self.timeleft ==0:
            
            self.completeScreen()
        
        else:
            
            self.nextColor()
        
    
    def countDown(self):
        
        if self.timeleft>0:
            
            self.timeleft -=1
            
            self.timer.config(text='Time left: '+str(self.timeleft))
            
            self.timer.after(1000,self.countDown)
        
    
    def nextColor(self):
        
        if self.timeleft>0:
            
            self.data.focus_set()
            
            if self.data.get().lower() == self.colorset[1].lower():
                self.score+=1
            
            self.data.delete(0,END)
            
            random.shuffle(self.colorset)
            
            self.word.config(text=str(self.colorset[0]).capitalize(),fg=str(self.colorset[1]))
            
            self.scoreLabel.config(text='Score: '+str(self.score))
        
        
    def completeScreen(self):
        
        self.timeleft=30
        
        
        self.clearWindow()
        
        label = Label(self.frame,text='Your Current Score: '+str(self.score),bg='#323232',fg='#a6ff4d',font=('courier new',15))
        label.pack(pady=(30,10))
        
        if self.score > self.highscore:
            
            self.mycursor.execute("update users set highscore = {} where email = '{}'".format(self.score,self.email))
            self.con.commit()
            self.fetchdetails()
            label = Label(self.frame,text='Your New Highscore:'+str(self.highscore),bg='#323232',fg='#a6ff4d',font=('courier new',15))
            label.pack(pady=(20,20))
            
        else:
            
        
        
            label = Label(self.frame,text='Highscore:'+str(self.highscore),bg='#323232',fg='#a6ff4d',font=('courier new',15))
            label.pack(pady=(20,20))
            
        Button(self.frame,text='Back to Game',bg='#212121',fg='white',width=17,height=1,font=('cambria',15),command=lambda:self.startScreen()).pack(pady=(20,20))

        self.root.bind('<Return>',self.startScreen)        

        
    def leaderBoardScreen(self):
        
        self.clearWindow()
        
        self.mycursor.execute("select name,highscore from users order by highscore desc limit 5")
        
        result = self.mycursor.fetchall()
        
        for i,j in zip(result,range(len(result))):
            
            Label(self.frame,text=i[0].capitalize(),bg='#323232',fg='#a6ff4d',font=('courier new',15)).grid(row=j,column=0)
            Label(self.frame,text=i[1],bg='#323232',fg='#a6ff4d',font=('courier new',15)).grid(row=j,column=1)
        
        Label(self.frame,text=self.name.capitalize(),bg='#323232',fg='#a6ff4d',font=('courier new',15)).grid(row=j+1,column=0,pady=(20,10))
        Label(self.frame,text=self.highscore,bg='#323232',fg='#a6ff4d',font=('courier new',15)).grid(row=j+1,column=1,pady=(10,10))
        
            
        Button(self.frame,text='Back to Game',bg='#212121',fg='white',width=18,height=1,font=('cambria',15),command=lambda:self.startScreen()).grid(row=j+2,columnspan=2) 
        
        self.root.bind('<Return>',self.startScreen)
        
    
    
        
    
    
obj=Game()
        
        