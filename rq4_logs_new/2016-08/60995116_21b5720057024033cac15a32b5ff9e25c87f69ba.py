from keras.models import load_model
from preprocess import char_indices, indices_char, maxlen, chars, text
from sampling_utils.sample import sample
import numpy as np
import random
import os

print len(chars)
name = "LSTM.h5"
model = load_model(os.path.join("models", name))
# choose how diverse our sampled output will be
diversity = 0.5

# first we have to generate a seed sequence of length 40 (default)

start_index = random.randint(0, len(text) - maxlen - 1)
generated = ''
sentence = text[start_index: start_index + maxlen]
generated += sentence

# length of generated text
length = 400
# one-hot encode generated string in [1, 40, 60] matrix (put the text in a format the compute can understand)

for _ in xrange(length):
    x = np.zeros((1, maxlen, len(chars)))
    for t, char in enumerate(sentence):
        x[0, t, char_indices[char]] = 1.

    #put our randomly pulled seed into our model
    #our model takes a sequence of 40 characters and predicts the next
    # input dimms (40, 60) output dims (60)
    preds = model.predict(x, verbose=0)[0]
    # sample from our 1/60 output
    next_index = sample(preds, diversity)
    # convert one-hot vector back to string
    next_char = indices_char[next_index]
    generated += next_char
    sentence = sentence[1:] + next_char

print generated