from flask import Flask, request, jsonify
import tensorflow as tf
import re
import numpy as np

app = Flask(__name__)


@app.route('/')
def index():
    """
    this is a root dir of my server
    :return: str
    """
    return "This is just a RNN TextClassifier web app"


@app.route('/users/<user>')
def hello_user(user):
    """
    this serves as a demo purpose
    :param user:
    :return: str
    """
    return "Hello %s!" % user


def vectorize_user_input(user_input_text):
    """
    vectorizing user input
    :param user_input_text:
    :return:
    """
    vocab_processor = tf.contrib.learn.preprocessing.VocabularyProcessor.restore(storage_dir + vocab_text_processor)
    text_string = re.sub(r'([^\s\w]|_|[0-9])+', '', user_input_text)
    text_string = " ".join(text_string.split())
    text_string = text_string.lower()
    text_string = text_string.split()
    return np.array(list(vocab_processor.transform(text_string)))


@app.route('/api/get_text_prediction', methods=['POST'])
def get_text_prediction():
    """
    predicts requested text whether it is ham or spam
    :return: json
    """
    json = request.get_json()
    print(json)
    if len(json['text']) == 0:
        return jsonify({'error': 'invalid input'})

    print('---user_data before processing :: ', json['text'])
    vectorized_user_input_text = vectorize_user_input(json['text'])
    print('---user_data after vectorized user_input_text.shape :: ', vectorized_user_input_text.shape)

    # load the model
    graph = tf.Graph()
    with graph.as_default():
        sess = tf.Session()
        with sess.as_default():
            # Load the saved meta graph and restore variables
            saver = tf.train.import_meta_graph("{}.meta".format(storage_dir + MODEL_NAME + '.ckpt'))
            saver.restore(sess, storage_dir + MODEL_NAME + '.ckpt')

            # Get the placeholders from the graph by name
            input_ = graph.get_operation_by_name(input_text_node).outputs[0]
            keep_prob = graph.get_operation_by_name(keep_prob_node).outputs[0]
            probability_output = graph.get_operation_by_name(probability_outputs).outputs[0]

            # Make the prediction
            eval_feed_dict = {input_: vectorized_user_input_text, keep_prob: 1.0}
            probability_prediction = sess.run(tf.reduce_mean(probability_output, 0), eval_feed_dict)

            # Print output (Or save to file or DB connection?)
            print('Probability of Ham: {:.4}'.format(probability_prediction[1]))

    return jsonify({'Ham probability': '{:.4}'.format(probability_prediction[1])})


if __name__ == '__main__':
    storage_dir = '/Users/mohammed-2284/Documents/ZTF_projects/RNNTextClassifier/out/'
    vocab_text_processor = 'vocab'
    MODEL_NAME = 'rnn_text_classifier'

    input_text_node = 'input_text'
    keep_prob_node = 'keep_prob'
    probability_outputs = 'probability_outputs'

    app.run(host='0.0.0.0', port=5000)