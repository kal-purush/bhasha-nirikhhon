import unittest
import CI_server


class Repo:
    def __init__(self):
        self.working_dir = 'tests'


class CIBuilderTest(unittest.TestCase):

    def test_lint_result(self):
        """
        Test case to see if lint error is captured
        Expected response is Default
        """
        ci_helper = CI_server.CIServerHelper
        result1, _ = ci_helper.ci_build(self, repo=Repo(), filepath="test_workflow1.yml")
        result2, _ = ci_helper.ci_build(self, repo=Repo(), filepath="test_workflow2.yml")
        self.assertTrue(result1)
        self.assertFalse(result2)

    def test_autotest_correct(self):
        """
        Test case to see if test result is captured and is correct
        Expected response is Default
        """
        ci_helper = CI_server.CIServerHelper
        result3, _ = ci_helper.ci_test(self, repo=Repo(), filepath="test_workflow3.yml")
        self.assertTrue(result3)

    def test_autotest_wrong(self):
        """
        Test case to see if test result is captured and is wrong
        Expected response is Default
        """
        ci_helper = CI_server.CIServerHelper
        result4, _ = ci_helper.ci_test(self, repo=Repo(), filepath="test_workflow4.yml")
        result6, _ = ci_helper.ci_test(self, repo=Repo(), filepath="test_workflow6.yml")
        self.assertFalse(result4)
        self.assertFalse(result6)

    def test_autobuild_result(self):
        """
        Test case to see if build result is captured
        Expected response is Default
        """
        ci_helper = CI_server.CIServerHelper
        result5, _ = ci_helper.ci_build(self, repo=Repo(), filepath="test_workflow5.yml")
        self.assertTrue(result5)


if __name__ == '__main__':
    unittest.main()