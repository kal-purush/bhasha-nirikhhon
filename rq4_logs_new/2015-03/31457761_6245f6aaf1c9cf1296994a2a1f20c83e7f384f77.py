#!/usr/bin/python3
import sys
import os
import sqlite3
running = True

def createTab():
    if(not os.path.isfile("ABC.db")):
        os.popen("Sqlite3 ABC.db")
    con.sqlite3.connect('ABC.db')
    c = con.cursor()
    
    c.execute("create table if not exists postiii(p_id integer,\
               P_Nam varchar(100), p_t varchar(1000),cit integer(10))")
    con.commit()
    con.close()
def insertdb(pt, pc, cat=0):
   
    con = sqlite3.connect('ABC.db')
    c = con.cursor()
    c1 = con.cursor() 
    c1.execute("select p_id from postii where P_Nam=?",(pt,))
    data = c1.fetchall()
    if len(data) == 0:
        c.execute('''insert into postii(P_Nam,P_t,cit) values(?,?,?)''',(pt, pc,cat))
    else:
        print("Already exists")
    con.commit()
    con.close()   
    print("Post name:%s" % pt)
    print("Post Comment:%s" %pc)

def listdb():
    con = sqlite3.connect('ABC.db')
    c = con.cursor()
    c.execute("select * from postii")
    data = c.fetchall()
    for eachr in data:
       print("Post Name:%s" %eachr[1])
       print("Content:%s " %eachr[2])
       
def searchdb(key):
    con = sqlite3.connect('ABC.db')
    c = con.cursor()
    c.execute("select * from postii")
    data = c.fetchall()
    #print(data)
    for row in data:
       if key == row[1] or key == row[2]:
           print(row)
    con.close()
             
def createCat(cname):
    print(cname)
    con =sqlite3.connect('ABC.db')
    c = con.cursor()
    c.execute("select cit from categories where cname=?",(cname,))
    listname = c.fetchall();
    if len(listname) == 0:
         c.execute("insert into categories(cname) values(?)",(cname,))
    else:
        print("Already exists")
    c1 = con.cursor()   
    con.commit()
    con.close()

"""to print category list"""
def listCat():
    con = sqlite3.connect('ABC.db')
    c = con.cursor()
    c.execute("select * from categories")
    data = c.fetchall()
    for row in data:
       print("Category ID:%s" %row[0])
       print("Category Name: %s" %row[1])
    con.close()

def assignCat(cid, pid):
    con = sqlite3.connect('ABC.db')
    c1 = con.cursor()
    c1.execute("select p_id from postii")
    postids = c1.fetchall()
    for data in postids:
        if data[0] != pid:
           break
        else: 
           print("Correct ID")
    con = sqlite3.connect('ABC.db')
    c = con.cursor()
    qry = "update postii set cit= " + str(cid) + " where p_id= " + str(pid)
    c.execute(qry) 
    con.commit()
    con.close()
    print("Assigned")
"""blog.py post add "title" "content" --category "cat-name"""
def bothCmd(lst):
     insertdb(lst[2],lst[3],lst[5])
     createCat(lst[5])     
 
def cmdDet(arg):
    try:
        if (arg[0] == '--help'):
           print('Commands for using blog')       
           print('post')
           print('post list ')	
           print('post add ["title"]["content"] - add a new blog')
           print('post list')
           print('post search')
           print('post add [title][content] --category [catname]')
           print('\ncategory')
           print('category add [category name]')
           print('category list')
           print('category [assign post ID] [Cat ID]')
           
        
        if(arg[4]):
            if (arg[4] == '--category'):
                 bothCmd(arg)
        
        elif (arg[0] == 'post'): 	#post
            if (arg[1] == 'add'):  
                 # add
                insertdb(arg[2] , arg[3])
            elif (arg[1] == 'search'):   # Search 
                searchdb(arg[2])               
            elif (arg[1] == 'list'):     #list
                listdb()    
        elif (arg[0] == 'category'):  # category 
            if (arg[1] == 'add'):      # adding category name             
                createCat(arg[2]) 
            elif(arg[1] == 'list'):
                listCat()
            elif(arg[1] == 'assign'):
                assignCat(arg[2],arg[3])                
        elif (arg[0] != 'category' or arg[0] != 'post'):
            print('Command doesn\'t exist:')
            print('for help type blog.py --help')
    except:
        print('for help type blog.py --help')


def main(argv):
    if __name__ == "__main__":
        arg = argv[1:]
        cmdDet(arg)

main(sys.argv)