from pyspark.sql.functions import expr, coalesce, when, col, isnan, count, max
import re

# STRART split_data
def split_data(ds, col, ratio = 0.8):
  """
  Stratified Split based on col with ratio (default = 0.8) and save your dataset
  Return path to train and test set and the data
  """
  
  from pyspark.sql.functions import lit

  seed = 76

  # Get fractions of test and train set
  fractions = ds.select(col).distinct().withColumn("fraction", lit(ratio)).rdd.collectAsMap()

  m_train_data = ds.stat.sampleBy(col, fractions, seed)
  m_test_data = ds.subtract(m_train_data)
  
  #return train and test DFs and their paths
  return m_train_data, m_test_data
#END split_data



# START save_csv
def save_csv(df, directory, folder_name):
  """
  Return: path to saved df 
   
  Save df to path :  directory + folder_nam
     
  User has to specify ds_name and try_name 
  directory : '/FileStore/CasaProject/'
       
  """
  
  m_dir = directory + folder_name
   
  # Save files
  df.write.csv(m_dir, header= True)
  
  return m_dir
# END save_csv


# START cond_statement
def cond_statement(col_name, given_dict):
  """
  Return the column name new_col_name that contains values based on column old_col
  """
  cond_str = "CASE"
  for key, val in given_dict.items():
    cond_str = cond_str + " WHEN " + col_name + " = '" + str(key) + "' then '" + str(val) + "'"
  
  
  cond_str += ' END AS abc'
  
  return cond_str
  # END cond_statement



# START cast_col_to_types
def cast_col_to_types(df, cols, to_type = 'double'):
  for col in cols:
    new_col_name = col + to_type
    df = df.withColumn(new_col_name, df[col].cast(to_type))
    df = df.drop(col)
    df = df.withColumnRenamed(new_col_name, col)
  
  return df
# END cast_col_to_types




# START cleaning_stages
def cleaning_stages (df, **kwargs):


#Cleaning_stages to store stages to clean up the dataset 
#(filter nulls, impute missing data, wrangling data, etc)

#TODO: Check for types of df - has to be spark DF

  #to_dos = ['drop_cols', 'cast_cols_dtype', 'fill_na', 'impute_cols']
  if 'drop_cols' in kwargs.keys():
    df = df.drop(*kwargs['drop_cols'])


  if 'cast_cols_dtype' in kwargs.keys():
    df = cast_col_to_types(df, kwargs['cast_cols_dtype'][0], to_type = kwargs['cast_cols_dtype'][1])


  if 'fill_na' in kwargs.keys():
    df = df.fillna(kwargs['fill_na'][1], subset = kwargs['fill_na'][0])


  if 'impute_cols' in kwargs.keys():
    for val in kwargs['impute_cols']:
     
      to_be_imputed_col = val[0]# val[0] is the name of column to be imputed
      expr_sentence = val[1] #val[1] is expression string to fill nulls with
      col_new_name = "new_" + to_be_imputed_col
      df = df.withColumn(col_new_name, expr(expr_sentence)) #val[1] is expression string to fill nulls with
      filled = "filled_" + to_be_imputed_col
      df = df.withColumn(filled, coalesce(df[to_be_imputed_col], df[col_new_name]))
      # Drop the two intermidiate columns then rename imputed col with its original name
      df = df.drop(col_new_name, to_be_imputed_col)
      df = df.withColumnRenamed(filled, to_be_imputed_col)
  

  if 'rank_cols' in kwargs.keys():
    for val in kwargs['rank_cols']:
     
      to_be_ranked_col = val[0]# val[0] is the name of column to be imputed
      expr_sentence = val[1] #val[1] is expression string to fill nulls with
      col_new_name = "ranked_" + to_be_ranked_col
      df = df.withColumn(col_new_name, expr(expr_sentence)) #val[1] is expression string to fill nulls with


  if 'convert_cols' in kwargs.keys():
    for val in kwargs['convert_cols']:
      to_be_converted_col = val[0] # name of col to store converted cols
      expr_sentence = val[1] # expression string to convert cols
      col_new_name = "converted_" + to_be_converted_col
      df = df.withColumn(col_new_name, expr(expr_sentence))

     
       
  return df

# END cleaning_stages
