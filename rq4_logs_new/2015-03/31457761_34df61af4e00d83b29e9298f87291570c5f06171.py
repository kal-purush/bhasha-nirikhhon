import unittest
import sqlite3
def createCat(cname):
    try:
        check = 1
        con = sqlite3.connect('ABC.db')
        p = cname.split(" ")   
        for each in range(len(p)):
            if (p[each].isalnum() == False):
                check = 0
        if (check):
            c = con.cursor()
            c.execute("insert into category_tab(categ) values(?)",(cname,))                
            return(1)
        else:
            print("category Name Invalid")
            return(0)      
    except Exception as error:
        return(str(error))
        print("Category Name already exists or default value used")        
        
    finally:
        con.commit()
        con.close()

class MyTest(unittest.TestCase):
    def setUp(self):
       self.catName= "AnyNamy1"
    def test_some_catName(self):
       self.assertRaises(Exception,createCat(self.catName))
    def test_some_catName_present(self):
       self.assertEqual(createCat(self.catName), 'UNIQUE constraint failed: category_tab.categ')
    def test_default_value(self):
       self.assertTrue(createCat("default"),'UNIQUE constraint failed: category_tab.categ')
    def test_def_value(self):
       self.assertTrue(createCat("def"),'UNIQUE constraint failed: category_tab.categ')
   
if __name__ == '__main__':
    unittest.main()