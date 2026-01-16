from flask import Flask
from flask import render_template
from flask import jsonify
from flask import request
import tensorflow as tf
tf.enable_eager_execution()
from tensorflow import keras
from keras.models import load_model
import functools

app = Flask(__name__)
model = keras.models.load_model('sample.h5')

@app.route('/')
def index():
  return render_template('index.html')

@app.route('/query', methods = ['POST'])
def query():
  return jsonify(hello_world(model, start_string=request.json['query']))

def hello_world(model, start_string):
  char2idx = {' ': 0, 'H': 1, 'W': 2, 'd': 3, 'e': 4, 'l': 5, 'o': 6, 'r': 7}
  idx2char = [' ', 'H', 'W', 'd', 'e', 'l', 'o', 'r']
  # Number of characters to generate
  num_generate = 10

  # Converting our start string to numbers (vectorizing)
  input_eval = [char2idx[s] for s in start_string]
  input_eval = tf.expand_dims(input_eval, 0)

  text_generated = []
  temperature = 1.0

  # Here batch size == 1
  model.reset_states()
  for i in range(num_generate):
      predictions = model(input_eval)
      # remove the batch dimension
      predictions = tf.squeeze(predictions, 0)
      predictions = predictions / temperature

      # predicted_id = tf.multinomial(tf.exp(predictions), num_samples=1)[-1,0].eval()
      predicted_id = tf.multinomial(predictions, num_samples=1)[-1,0].numpy()
      input_eval = tf.expand_dims([predicted_id], 0)
      text_generated.append(idx2char[predicted_id])

  return (start_string + ''.join(text_generated))