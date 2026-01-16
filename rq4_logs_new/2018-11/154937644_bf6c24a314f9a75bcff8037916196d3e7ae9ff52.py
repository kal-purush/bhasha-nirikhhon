import unittest

from parameterized import param, parameterized

from argparse import Namespace
from pandas.core.frame import DataFrame

from app.command.add import Add
from app.error.column_already_exists_error import ColumnAlreadyExistsError
from app.error.column_not_found_error import ColumnNotFoundError


class TestAdd(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @parameterized.expand([
        # 最後に追加
        param(
            "at the end",
            input={
                "file": "src/tests/data/test_commnad.csv",
                "column": "new_col",
                "default": None,
                "first": None,
                "after": None,
            },
            expected=DataFrame({
                'user_id': ['1', '2', '3'],
                'username': ['username1', 'username2', 'username3'],
                'new_col': [None, None, None]})
        ),
        # default デフォルト値を指定
        param(
            "set default",
            input={
                "file": "src/tests/data/test_commnad.csv",
                "column": "new_col",
                "default": "42",
                "first": None,
                "after": None,
            },
            expected=DataFrame({
                'user_id': ['1', '2', '3'],
                'username': ['username1', 'username2', 'username3'],
                'new_col': ['42', '42', '42']})
        ),
        # first 先頭に追加
        param(
            "at the first",
            input={
                "file": "src/tests/data/test_commnad.csv",
                "column": "new_col",
                "default": None,
                "first": True,
                "after": None,
            },
            expected=DataFrame({
                'new_col': [None, None, None],
                'user_id': ['1', '2', '3'],
                'username': ['username1', 'username2', 'username3']})
        ),
        # after 指定した項目の後に追加
        param(
            "after user_id",
            input={
                "file": "src/tests/data/test_commnad.csv",
                "column": "new_col",
                "default": None,
                "first": None,
                "after": "user_id",
            },
            expected=DataFrame({
                'user_id': ['1', '2', '3'],
                'new_col': [None, None, None],
                'username': ['username1', 'username2', 'username3']})
        ),
    ])
    def test_procee_normal(self, _, input, expected):
        sub_command = Add(Namespace(**input))
        df = sub_command.read()
        actual = sub_command.process(df)
        self.assertTrue(actual.equals(expected), "actual={}".format(actual))

    @parameterized.expand([
        # 既に存在する項目名を追加しようとした
        param(
            "column_already_exist",
            input={
                "file": "src/tests/data/test_commnad.csv",
                "column": "username",  # NG
                "default": None,
                "first": None,
                "after": None,
            },
            expected=ColumnAlreadyExistsError
        ),
        # 存在しない項目の後に追加しようとした
        param(
            "column_not_found",
            input={
                "file": "src/tests/data/test_commnad.csv",
                "column": "new",
                "default": None,
                "first": None,
                "after": "hoge",  # NG
            },
            expected=ColumnNotFoundError
        ),
    ])
    def test_procee_exception(self, _, input, expected):
        sub_command = Add(Namespace(**input))
        df = sub_command.read()
        with self.assertRaises(expected):
            sub_command.process(df)


if __name__ == '__main__':
    unittest.main()