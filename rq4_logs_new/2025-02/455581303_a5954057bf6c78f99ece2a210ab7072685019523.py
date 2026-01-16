"""Module to characterize values. Only considering following types:
float, int, pint.Quantity, pandas.Series, pandas.DataFrame."""

# Terminology:
#
# We distinguish the following objects (mutually exclusive):
#     Values (single)                                              After tools_unit.normalize
#     ---------------
#     float                                                        float
#     int                                                          float
#     dimensionless Quantity                                       float
#     dimensional Quantity                                         dimensional Quantity
#
#     1D-values
#     ---------
#     float-Series                                                 float-Series
#     int-Series                                                   float-Series
#     dimensionless pint-Series                                    float-Series
#     dimensional pint-Series                                      dimensional pint-Series
#     Series of pint-quantities of uniform dimensionality          dimensional pint-Series
#     Series of pint-quantities of non-uniform dimensionality      Series of pint-quantities of non-uniform dimensionality
#
#     2D-values
#     ---------
#     DataFrame


import pint
import pandas as pd
import pint_pandas

from . import unit as tools_unit


# Timeseries


def is_timeframe(v: pd.Series | pd.DataFrame) -> bool:
    """True if Series or Dataframe with datetimeindex with frequency set. False if not DatetimeIndex. Error otherwise."""
    if not isinstance(v.index, pd.DatetimeIndex):
        return False
    elif v.index.freq is not None:
        return True
    raise ValueError("Found DatetimeIndex without frequency.")


# Dimensionality.


def has_uniform_dimensionality(v: pd.Series) -> bool:
    """True if pint-Series, or if Series of quantities with same dimension."""
    if isinstance(v.dtype, pint_pandas.PintType):
        return True  # pintseries
    elif pd.api.types.is_numeric_dtype(v.dtype):  # check for PintType first!
        return True  # floatseries
    else:
        try:
            _ = v.pint.convert_object_dtype()
        except (pint.DimensionalityError, AttributeError):
            return False
        return True


def dimensionality(v: tools_unit.ALLOWED_TYPES) -> pint.util.UnitsContainer:
    v = tools_unit.normalize(v)
    if isinstance(v, float):
        return tools_unit.ureg.get_dimensionality("[]")
    elif isinstance(v, pint.Quantity):
        return v.dimensionality
    elif isinstance(v, pd.Series):
        pass