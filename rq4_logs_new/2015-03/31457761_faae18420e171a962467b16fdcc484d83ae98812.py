import sqlite3
import unittest 
#insert post function testing
def insertPost(post_Nam, post_Cont, categ=0):
    try:
        con = sqlite3.connect('ABC.db')
        check = 0
        pwlist = post_Nam.split(" ")   
        for each in range(len(pwlist)):
            if (pwlist[each].isalnum() == False):
                check = 0
            else:
                check = 1
        post_Nam = post_Nam.lstrip()
        if (check):
            c = con.cursor()
            c.execute("select pid from post_tab where post_nam=?",(post_Nam,))
            listname = c.fetchall();
            if len(listname) == 0:
                c.execute("insert into post_tab(post_nam, post_cont,cati)\
                           values(?,?,?)",(post_Nam, post_Cont, categ))
                return("Post Updated")
            else:
               print("Post Name is already exists")
               return("Invalid Post Name")
    except Exception as err:
        print(str(err))
    finally:
        con.commit()
        con.close()

class test_insert_pos(unittest.TestCase):
    def setUp(self):
        self.pNam = "Post no 123"
        self.pName = ""
        self.pCont = "Content Good"
        self.cat = 2  
    def test_cat_present1(self):
        self.assertEqual(insertPost(self.pNam, self.pCont, self.cat), "Invalid Post Name")
    def test_cat_not_present(self):
        self.assertRaises(Exception,insertPost(self.pName, self.pCont), "Invalid Post Name")
      

if __name__ == "__main__":
    unittest.main()