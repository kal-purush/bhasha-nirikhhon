from datetime import datetime
import os
import numpy as np

#login or Signup
def authentication():  
    x=int(input("Press a key out of given below options:\n1.Login\n2.Signup\n3.Exit\n"))

    if x==1:     #Login
        name=input("Enter your name :")
        if name=='':      #check if name is empty
            print("Username empty")    
            return False,""
        
        p=input("Enter your password :")
        if p=='':     #check if password is empty
            print("Password empty")
            return False,""
        
        if os.path.exists('login_details.npy'):
            f=np.load('login_details.npy')    #Load login details file to check if username and password exists or not
            for x in f:
                l=x.split('$')
                if l[0]==name:
                    if l[1]==p:
                        return True,name
                    else:
                        print("Wrong password")
                        return False,""
               
        print("Sign Up required")
        return False,""

    elif x==2:    #Signup
        name=input("Enter your name :")
        if name=='':    #check if name is empty
            print("Username empty")
            return False,""
        
        p=input("Enter your password :")    
        if p=='':    #check if passwaord is empty
            print("Password empty")
            return False,""
        
        text=name+'$'+p
        if os.path.exists('login_details.npy'):
            f=np.load('login_details.npy')    #Load login_details file

            if f.shape[0]==10:      #check if no.of users exceed limit or not
                print("No.of users limit cannot exceed 10")
                return False,""

            for x in f:     #check if username already exists 
                x=x.split('$')[0]
                if(x==name):
                    print("Username already exists")
                    return False,""
                
            f=np.append(f,[text])   #append new username and password
        else:
            f=np.array([text])
            
        np.save('login_details.npy',f)     #save login_details   in npy format
        return True,name

    elif x==3:    #exit the app
        exit()
        
    else:    #wrong key pressed
        print("Wrong key pressed")
        return False,""

#journel list and entry
def journel(name):
    x=int(input("Press a key out of given below options:\n1.List all his previous entries\n2.Enter a new entry\n3.Exit\n")) 
    
    if x==3:
        exit()   #exit the app
        
    dataset_path='./data/'   #path of folder where data is stored in files
    
    if os.path.exists(dataset_path+name+'.npy'):
        f=np.load(dataset_path+name+'.npy')     #Load the file and store all entries in a numpy array
    else:
        f=np.array([])
                    
    if x==1:    #print all previous entries
        if f.shape[0]==0:
            print("no entry present")
        else:    
            for i in f:
                print(i)
                        
    elif x==2:    #enter a new entry
        j=input("enter the text entry :")
        today=datetime.now().strftime('%d %b %Y %I.%M%p')
        text=today+' - '+j
                    
        if(f.shape[0]<50):   #store new entries  if no.of entries less than 50
            f=np.append(f,[text])
                            
        else:    #store last 50 entries
            f=f[1:]
            f=np.append(f,[text])
            
        np.save(dataset_path+name+'.npy',f)      #store all entries in npy format   
            
                    
                
    
flag=False    
while(flag==False):
    flag,s=authentication()   
while(True):
    journel(s)