import sqlite3
import unittest
#Search Database for keyword specified
def searchdb(key):
    try:   
        con = sqlite3.connect('ABC.db')
        c = con.cursor()
        query = "select * from post_tab where post_nam like\
                '%" + key + "%' OR post_Cont like '%" + key + "%'"
        #print(query)
        if (key == ""):
            print("Search Keyword is empty")
            return("Search Keyword is empty")
        c.execute(query)
        for row in c.fetchall():
            print("Post Name: %s" %row[1])
            print("Post Content: %s" %row[2])
    except TypeError:
        print("Integer Value is provided") 
        return("Integer Value is provided")
    except Exception as err:
        print("Command is not valid",str(err))   
    finally:
        con.close()

class test_search(unittest.TestCase):
    def setUp(self):
        self.key1 = "1"
        self.key2 = 2
        self.key3 = "def"
        self.key4 = ""
    def tearDown(self):
        pass
    def test_number_search(self):
        self.assertEqual(searchdb(self.key1), None)
    def test_Non_argument(self):
        self.assertEqual(searchdb(self.key4), "Search Keyword is empty")
    def test_integer_search(self):
        self.assertRaises(TypeError, searchdb(self.key2))
    def test_keyword_search(self):
        self.assertEqual(searchdb(self.key3), None)

if __name__ == '__main__':
   unittest.main()