import pytest
from pytestpackage.class_to_test import SomeClassToTest


@pytest.mark.usefixtures("setUp", "oneTimeSetUp")
class TestClassDemo:

    @pytest.fixture(autouse=True)
    def classSetup(self, oneTimeSetUp):
        self.abc = SomeClassToTest(self.value)

    def test_method1(self):
        result = self.abc.Plus2Number(5, 9)
        assert result == 34
        print("Done test method 1")

    def test_method2(self):
        result = self.abc.Multi2Number(2, 5)
        assert result == 200
        print("Done test method 2")


"""
#cmd to run the test with pytest html report
<file direction> py.test -s -v <filename>.py --browser <param> --html=<report_name>.html
"""
