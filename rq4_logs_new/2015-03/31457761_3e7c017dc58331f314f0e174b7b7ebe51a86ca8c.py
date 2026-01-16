import sqlite3
import unittest
import blog as assignCat
def assignCat(pid, cid):
    try:
        con = sqlite3.connect('ABC.db')
        c = con.cursor()
         
        query = "update post_tab set cati= " + str(cid) + " where pid= " + str(pid)
        print(query)
        c.execute(query)
    except Exception as error:
        raise Exception("Category id or post id invalid")
    finally:         
        con.commit()
        con.close()
class test_assign(unittest.TestCase):
   def setUp(self):
       self.false_int = "a"
       self.false_p = "abc"
   def tearDown(self):
       pass 
   def test_postid_0(self):
       self.assertEqual(assignCat(0, 2), None)
   def test_category_not_present(self):
       self.assertEqual(assignCat(1, 7), None)
   def test_exception_raisee(self):
       self.assertRaises(Exception ,assignCat(0, 0))
if __name__ == '__main__':
   unittest.main()