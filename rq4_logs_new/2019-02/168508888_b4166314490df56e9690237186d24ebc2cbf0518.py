import tensorflow as tf
import sys

import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'


FILE_PREDICT=sys.argv[2]
SAVE_PATH=sys.argv[1]

feature_names = [
    'Elo',
    'Pts_div_pld',
    'GfA',
    'GaA']

feature_columns = [ tf.feature_column.numeric_column(i) for i in feature_names ]

classifier = tf.estimator.DNNClassifier(feature_columns=feature_columns,
	hidden_units = [10,10],
	n_classes = 3,
	model_dir = SAVE_PATH)

# In memory predictions read
def my_new_input_fn(file_path):
    def decode_csv(line):
        parsed_line = tf.decode_csv(line, [[0.], [0.], [0.], [0.]])
        
        features = parsed_line  # Everything but last elements are the features
        d = dict(zip(feature_names, features))
        return d

    dataset = (tf.data.TextLineDataset(file_path)  # Read text file
               .skip(1)  # Skip header row
               .map(decode_csv))  # Transform each elem by applying decode_csv fn
    
    dataset = dataset.batch(32)  # Batch size to use
    iterator = dataset.make_one_shot_iterator()
    batch_features = iterator.get_next()
    return batch_features, None

results = classifier.predict(input_fn = lambda: my_new_input_fn(FILE_PREDICT))

for r in results:
	idx = r["class_ids"][0]
	print(",",float("{0:.2f}".format(r["probabilities"][0])),",",float("{0:.2f}".format(r["probabilities"][1])),",",float("{0:.2f}".format(r["probabilities"][2])))