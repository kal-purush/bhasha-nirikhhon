import pandas as pd


class PandasUtil():
    @classmethod
    def fix_dataframe(self, df, type_castings={},
                      datetime_fields=[], datetime_format=None,
                      column_schema={}):
        """To do fixes like data types, type_castings and empty columns
        """
        _df = df.copy()
        self._cast_types(_df, type_castings)
        if datetime_format:
            self._fix_datetime(
                _df, datetime_fields, datetime_format)
        if column_schema:
            self._fix_missing_columns(_df, column_schema)
        return _df

    @classmethod
    def _cast_types(self, df, type_castings):
        for _name, _type in type_castings.items():
            if _type == "string":
                df[[_name]] = df[[_name]].astype(str)
            else:
                raise Exception("Type cast not implemented for type %s"
                                % _type)

    @classmethod
    def _fix_datetime(self, df, datetime_fields, datetime_format):
        for _name in datetime_fields:
            df[_name] = pd.to_datetime(
                df[_name], format=datetime_format, errors="coerce")

    @classmethod
    def _fix_missing_columns(self, df, column_schema):
        pass