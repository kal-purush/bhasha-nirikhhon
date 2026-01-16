from keras.models import Sequential
from keras.layers import Dense, Embedding, LSTM, Dropout, Merge, RepeatVector, Activation

"""
With the following model, i think the training and test pairs should look like:
x = [I, S0, ..., SN-1]
t = [S0, ...,, SN]
Where S0 and SN are special START and STOP symbols (for example: 0 and 999).
Words should first be mapped to integers, so for example:
x = [[0.21, ..., 0.03], 0, 34, 543, 76]
t = [0, 34, 543, 76, 999]
"""

def get_LSTM_model(img_input_dim, vocabulary_size):
	# TODO: Dont know whether this is big enough
	word_img_embedding_size = 128
	hidden_neurons_LSTM = 256
	# Use dropout to reduce overfitting.
	dropout = 0.2

	image_model = Sequential()
	# One layer 'embedding' from image features to shared img-word space
	image_model.add(Dense(word_img_embedding_size, input_dim=img_input_dim))
	image_model.add(Dropout(dropout))
	# Reshape so that we can concatenate word embeddings
	image_model.add(RepeatVector(1))

	lang_model = Sequential()
	# One layer embedding from word to shared img-word space
	lang_model.add(Embedding(vocabulary_size, word_img_embedding_size))
	lang_model.add(Dropout(dropout))

	model = Sequential()
	# Merge image and language model so that we can provide input of the form
	# [I, S0, S1, ... , SN-1]
	model.add(Merge([image_model, lang_model], mode='concat', concat_axis=1))

	# Create LSTM layer(s)
	# TODO: Might get performance improvement from using unroll=True, but need to specify time dimension
	# We could search for the max caption length in training set (+5%) (+3 for I, START and STOP).
	model.add(LSTM(hidden_neurons_LSTM))
	model.add(Dropout(dropout))
	model.add(Dense(vocabulary_size))
	model.add(Activation('softmax'))

	print(model.summary())

	# TODO: How to choose optimizer
	# TODO: Should we use metric=['accuracy']? and why?
	model.compile(loss='categorical_crossentropy', optimizer='rmsprop')

	return model