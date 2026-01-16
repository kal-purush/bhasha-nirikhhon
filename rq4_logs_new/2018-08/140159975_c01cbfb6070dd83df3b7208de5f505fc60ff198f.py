import pytest
import unittest
import pandas as pd
from bqsqoop.utils.pandas_util import PandasUtil


class TestPandasUtil(unittest.TestCase):
    def test_no_fixes_case(self):
        data = [
            dict(colA="val1", colB=1),
            dict(colA="val2", colB=2)
        ]
        df = pd.DataFrame.from_dict(data)
        df_after_fix = PandasUtil.fix_dataframe(df)
        self.assertEqual(df_after_fix.to_json(
        ), '{"colA":{"0":"val1","1":"val2"},"colB":{"0":1,"1":2}}')

    def test_fix_datetime_fields(self):
        data = [
            dict(colA='Fri, 03 Aug 2018 03:18:40 GMT', colB=1,
                 colC='Sat, 04 Aug 2018 03:18:40 GMT'),
            dict(colA='Fri, 03 Aug 2018 05:18:40 GMT', colB=2,
                 colC='Invalid datetime format')
        ]
        df = pd.DataFrame.from_dict(data)
        df_after_fix = PandasUtil.fix_dataframe(
            df, datetime_fields=["colA", "colC"],
            datetime_format="%a, %d %b %Y %H:%M:%S %Z"
        )
        self.assertEqual(df_after_fix.to_json(
        ), '{"colA":{"0":1533266320000,"1":1533273520000},"colB":{"0":1,"1":2},"colC":{"0":1533352720000,"1":null}}')   # noqa

    def test_type_castings(self):
        data = [
            dict(colA=2, colB=1, colC=3),
            dict(colA=6, colB=5, colC=4),
        ]
        df = pd.DataFrame.from_dict(data)
        df_after_fix = PandasUtil.fix_dataframe(
            df, type_castings={"colA": "string", "colC": "string"}
        )
        self.assertEqual(df_after_fix.to_json(
        ), '{"colA":{"0":"2","1":"6"},"colB":{"0":1,"1":5},"colC":{"0":"3","1":"4"}}')   # noqa

    def test_type_castings_unkown_type(self):
        data = [
            dict(colA=2, colB=1, colC=3),
            dict(colA=6, colB=5, colC=4),
        ]
        df = pd.DataFrame.from_dict(data)
        with pytest.raises(
                Exception,
                match=r'Type cast not implemented for type wrong_type'):
            df_after_fix = PandasUtil.fix_dataframe(
                df, type_castings={"colA": "wrong_type", "colC": "string"}
            )