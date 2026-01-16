import unittest
import sqlite3
import blog as bgcmd

class test_9th_cmd(unittest.TestCase):
    def setUp(self):
        self.arg =("post","add"," \"Exdample Post\"","\"Comment\"", "--category", "\"Sample\"")
        self.arg1 =("post","add"," \"Example Post\"","\"Comment\"", "--category", "jhg")
        self.arg2 =("post","add","","\"Co\"", "--category", "asdf")
    def test_post_feild_empty(self):
        self.assertEqual(bgcmd.bothCmd(self.arg), None)
    def test_post_exists(self):
        self.assertEqual(bgcmd.bothCmd(self.arg2),None)    
if __name__ == "__main__":
    unittest.main()