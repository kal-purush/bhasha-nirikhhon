import sqlite3
import unittest
def listCat():
    try:
        con = sqlite3.connect('ABC.db')
        c = con.cursor()
        c.execute("select * from category_tab")
        data = c.fetchall()
        print("Category List")
        for row in data:
            print("ID:%s   Name:%s" %(row[0], row[1]))
        #print("Category Name: %s" %row[1])
    except Exception as err:
        return("List is empty", err)    
    finally:
        con.close()
class Test_listcat(unittest.TestCase):
    def test_list(self):
        self.assertEqual(listCat(),None)
    def test_list_empty(self):
        self.assertRaises("List is empty",listCat())

if __name__== "__main__":
    unittest.main()