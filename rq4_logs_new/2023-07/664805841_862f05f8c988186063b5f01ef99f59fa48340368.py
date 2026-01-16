import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

class GroupbyImputer(BaseEstimator, TransformerMixin):
  def __init__(self, column_to_fill, groupby_column, num_groups=None, aggfunc='median'):
    self.column_to_fill = column_to_fill
    self.groupby_column = groupby_column
    self.aggfunc = aggfunc
    if num_groups:
      self.num_groups = num_groups
    else: self.num_groups = 4

  
  def fit(self, X, y=None):
    # for numeric
    if (pd.api.types.is_numeric_dtype(X[self.groupby_column])) and (X[self.groupby_column].nunique() > self.num_groups):      
      X['groups'] = pd.cut(X[self.groupby_column], self.num_groups)
      self.groups = X['groups'].unique()   
      self.medians = []
      for group in self.groups:
        self.medians.append(X[X['groups'] == group][self.column_to_fill].agg(self.aggfunc))   
    # for categorial
    else:
      self.groups = X[self.groupby_column].unique()
      self.medians = []
      for group in self.groups:
        self.medians.append(X[X[self.groupby_column] == group][self.column_to_fill].agg(self.aggfunc))
    return self

  def transform(self, X, y=None):
    new_dfs = []
    for i in range(len(self.groups)):
      # for numeric
      if (pd.api.types.is_numeric_dtype(X[self.groupby_column])) and (X[self.groupby_column].nunique() > self.num_groups):      
        X['groups'] = pd.cut(X[self.groupby_column], self.num_groups)
        group_df = X[X['groups'] == self.groups[i]].copy()
      # for categorial  
      else:
        group_df = X[X[self.groupby_column] == self.groups[i]].copy()

      group_df[self.column_to_fill] = group_df[self.column_to_fill].fillna(self.medians[i])
      new_dfs.append(group_df)
    new_df = pd.concat(new_dfs).reindex(X.index)  
    return new_df

  def agg(aggfunc):
    if aggfunc == 'mean':
      return pd.DataFrame.mean
    elif aggfunc == 'min':
      return pd.DataFrame.min
    elif aggfunc == 'max':
      return pd.DataFrame.max
    else: return pd.DataFrame.median