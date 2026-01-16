import tensorflow as tf
import time
from util import tf_help

writer = tf_help.crete_file_writer(__file__)
sess = tf.Session()

features = {
    'sales': [[5], [10], [8], [9]],
    'department': ['sports', 'sports', 'gardening', 'whatever']
}

# assigns integer ID for every category
department_column = tf.feature_column.categorical_column_with_vocabulary_list(
    key = 'department',
    vocabulary_list = ['sports', 'gardening'],
    num_oov_buckets=1 # everything what is out of vocabulary will go here
)

print(department_column, type(department_column))

# maps IDs into one_hot_encoding
department_column = tf.feature_column.indicator_column(department_column)
print(department_column, type(department_column))

columns = [
    tf.feature_column.numeric_column('sales'),
    department_column
]

inputs = tf.feature_column.input_layer(features, columns)

var_init = tf.global_variables_initializer()
table_init = tf.tables_initializer()

sess.run((var_init, table_init))

print(sess.run(inputs))

# [[ 1.  0.  5.]
#  [ 1.  0. 10.]
#  [ 0.  1.  8.]
#  [ 0.  1.  9.]]


writer.add_graph(tf.get_default_graph())
time.sleep(2)  # wait for writer writes events to disk
print("done")