import random
import mysql.connector
connections=mysql.connector.connect(host="localhost" ,port="3306",user="root",password="123456789", database="project")
cursor=connections.cursor()

def com(num):
    if num<=100 and num>=95:
        return 'both are nice,your lucky person,more care,trust,strong willed,live long,traveling more'
    elif num<=95 and num>=85:
        return 'both are nice,beautiful persons,more care,your lucky,trust me'
    elif num<=85 and num>=75:
                return 'live long,trust,both understand,traveling more'
    elif num<=75 and num>=65:
                return 'sweet relationship,both understand,live long'
    elif num<=65 and num>=55:
                return 'security,believe,care,live long,traveling more'
    elif num<=55 and num>=45:
        return "believeyourself,don't desappoint,more care"
    else:
        return "don't desappoint,believe yourself time changes everything,trust me"
def new_data(Fname,Sname):
    global per
    per=random.choice(range(40,100+1))
    comment=com(per)
    cursor.execute("insert into relationship (fname,sname,percentage,comments) values( %s,%s,%s,%s)",(Fname,Sname,per,comment))
    connections.commit()
print('Percentage between two persons ')
Fname=input("Enter Your Name=")
Sname=input("Enter your favourite Person=")
if (Fname=="" or " ")or(Sname==" " or "")or(Fname+Sname.isalnum() or Fname+Sname.isnumeric()):
    print("Enter Useful Names So Try Again")
else:
    cursor.execute("select fname,sname,percentage,comments from relationship where fname=%s and sname=%s",(Fname,Sname))
    listvalues=cursor.fetchall()
    if len(listvalues)==0:
        cursor.execute("select fname,sname,percentage,comments from relationship where fname=%s and sname=%s",(Sname,Fname))
        listvalues=cursor.fetchall()
    if len(listvalues)==0:
        new_data(Fname,Sname)
        print()
        print("-->{} & {} -->->{}".format(Fname,Sname,per))
        print("-->{}".format(com(per)))
        print()
    else:
        cursor.execute("select fname,sname,percentage,comments from relationship where fname=%s and sname=%s",(Sname,Fname))
        listvalues1=cursor.fetchall()
        print()
        print("-->{} & {} -->->{}".format(listvalues[0][0],listvalues[0][1],listvalues[0][2]))
        print("-->{}".format(listvalues[0][3]))
        print()
    cursor.close()
    connections.close()