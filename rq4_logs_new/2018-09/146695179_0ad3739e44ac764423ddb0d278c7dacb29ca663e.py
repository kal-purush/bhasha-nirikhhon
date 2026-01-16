import GitTopFive
import unittest
import json
import sys


class TestCase(unittest.TestCase):

    # set up test client
    def setUp(self):
        GitTopFive.app.testing=True
        self.app = GitTopFive.app.test_client()

    def query(self, org, token=None):
        if token:
            return self.app.get('/org/%s?token=%s' %(org,token))
        return self.app.get('/org/%s' % org)

    def test_errors(self):
        # test bad organization name input
        rv = self.query("foo","9932edc29898190ff4fa6affec7955f336fba53e")
        self.assertEqual({"message": "foo is not a valid organization."}, json.loads(rv.get_data().decode(sys.getdefaultencoding())))
        # test bad token input
        rv = self.query("github", "foo")
        self.assertEqual({"message": "Your OAuth token is incorrect. Make sure to include the 'token' field in your request."}, json.loads(rv.get_data().decode(sys.getdefaultencoding())))

    def testContent(self):
        olist= ["github", "railsgirls", "octobox", "adobe"]
        persist = None
        for organization in olist:
            rv = json.loads(self.query(organization, "9932edc29898190ff4fa6affec7955f336fba53e").get_data().decode(sys.getdefaultencoding()))
            assert rv["organization"] == organization
            assert rv["pulls"] == sorted(rv["pulls"], reverse=True)
            assert len(rv["repositories"]) <= 5
            if organization == "adobe":
                persist = rv
        rv = json.loads(self.query("adobe", "9932edc29898190ff4fa6affec7955f336fba53e").get_data().decode(sys.getdefaultencoding()))
        assert rv == persist

if __name__ == '__main__':
    print("Tests started.")
    unittest.main()