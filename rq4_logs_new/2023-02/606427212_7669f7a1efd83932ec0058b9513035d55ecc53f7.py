import mysql.connector as msc
mycon=msc.connect(host='localhost',user='root',passwd='root',database='library')
mycur=mycon.cursor()
mycur.execute('use library')
if mycon.is_connected():
    print('Connection made')
else:
    print('Connection Break')
print('- - - - - - - - - -')
def newbook():
    bookname=input('Enter book name- ')
    bookuid=int(input('Enter book ID- '))
    issueuid='Null'
    query="insert into data values({0},'{1}','{2}')".format(bookuid,bookname,issueuid)
    mycur.execute(query)
    mycon.commit()
    print('New Book Added')
    print('- - - - - - - - - -')
def deletebook():
    bookuid=int(input('Enter Book ID- '))
    query="delete from data where bookuid={}".format(bookuid)
    mycur.execute(query)
    mycon.commit()
def status():
    bookuid=int(input('Enter book ID- '))
    query="select issueuid from data where bookuid={0}".format(bookuid)
    mycur.execute(query)
    data=mycur.fetchone()
    if data!=None:
        print(data)
    else:
        print('Record not found')
def unissue():
    bookuid=int(input('Enter book ID- '))
    issueid='Null'
    query="update data set issueuid={0} where bookuid={1}".format(issueid,bookuid)
    mycur.execute(query)
    mycon.commit()
def issue():
    bookuid=int(input('Enter book ID- '))
    query1="select bookuid from data where bookuid={0}".format(bookuid)
    mycur.execute(query1)
    data=mycur.fetchone()
    if data!=None:
        print("BOOK UID- ",data)
        issueuid=input('Enter Student ID- ')
        query="update data set issueuid={0} where bookuid={1}".format(issueuid,bookuid)
        mycur.execute(query)
        mycon.commit()
    else:
        print('Record not found')
def allcheck():
    query="select * from data"
    mycur.execute(query)
    data=mycur.fetchall()
    if data!=None:
        print(data)
    else:
        print("No data!")
#MENU
while True:
    print('''---------------------
New Book record-1
Delete book record-2
Check Issue Status-3
Issue a book-4
See all data-5
Un-issue the book-6
------------------------------''')
    opt=int(input('Enter option- '))
    if opt==1:
        newbook()
    elif opt==2:
        deletebook()
    elif opt==3:
        status()
    elif opt==4:
        issue()
    elif opt==5:
        allcheck()
    elif opt==6:
        unissue()