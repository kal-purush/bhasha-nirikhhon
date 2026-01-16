import peewee
from model import Slambook
from query import insertSlambook
from query import deletequery
from query import updateSlambook
from query import searchSlambook
from query import viewSlambook
import sys
import os
q='y'
while (q=='y') or (q=='Y'):
    os.system('cls')
    print('Main Menu\n')
    print('1-Add Information')
    print('2-Delete Information')
    print('3-Update Information')
    print('4-View Information')
    print('5-Search Information')
    print('6-Exit')
    choice=int(input('Enter choice: '))
    if choice==1:
        os.system('cls')
        a=input('Slambook ID')
        b=input('Fullname: ')
        c=input('Nickname: ')
        d=input('Birthday: ')
        e=input('Age: ')
        f=input('Gender: ')
        g=input('Contact: ')
        h=input('EmailAddress:')
        insertSlambook(a,b,c,d,e,f,g,h)

    elif choice==2:
        viewquery()
        id=int(input('Enter ID:'))
        deletequery(id)
        viewSlambook()

    elif choice==3:
        os.system('cls')
        updateSlambook()

    elif choice==4:
        os.system('cls')
        viewSlambook()

    elif choice==5:
        os.system('cls')
        searchSlambook()

    elif choice==6:
        os.system('cls')
        print('\nBye Ralph.')
        sys.exit()
    else:
        os.system('cls')
        print('\nInvalid Choice!')
    q=input('Go back to Main Menu?[y=YES / n=EXIT]: ')