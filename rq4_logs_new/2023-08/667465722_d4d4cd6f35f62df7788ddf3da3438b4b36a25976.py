import pandas as pd
import re
import pickle
import matplotlib.pyplot as plt
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
from collections import defaultdict
from sklearn.model_selection import train_test_split
import numpy as np
from tensorflow.keras import layers
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Embedding, LSTM, SpatialDropout1D, SimpleRNN, Activation
from tensorflow.keras import optimizers
from tensorflow.keras.callbacks import EarlyStopping, TensorBoard
from tensorflow.keras.layers import Flatten
from tensorflow.keras import backend as K
from sklearn import metrics
from sklearn.metrics import classification_report
from sklearn.metrics import accuracy_score
from sklearn. model_selection import KFold

# Membaca file TSV
df = pd.read_csv('train_preprocess.tsv.txt',
                 sep='\t',
                 names = ["text", "label"]
)
# print(df.shape) mengecek berapa jumlah kolom dan barisnya
# print(df.label.value_counts()) mengecek jumlah jenis pada label


# 1. Membersihkan data dengan regesi
def cleansing(sent):
    string = sent.lower()
    string = re.sub(r'[^\w\s]', ' ', string)  # Menggantikan karakter non-alphanumeric dengan spasi
    return string
df['text_clean'] = df.text.apply(cleansing) # akan menambah 1 kolom untuk bagian data yang sudah dibersihkan
# print(df.head())

# 2. Mensortir data berdasarkan sentimen
neg = df.loc[df['label'] == 'negative'].text_clean.tolist()
neu = df.loc[df['label'] == 'neutral'].text_clean.tolist()
pos = df.loc[df['label'] == 'positive'].text_clean.tolist()

neg_label = df.loc[df['label'] == 'negative'].label.tolist()
neu_label = df.loc[df['label'] == 'neutral'].label.tolist()
pos_label = df.loc[df['label'] == 'positive'].label.tolist()

total_data = pos + neu + neg
labels = pos_label + neu_label + neg_label

print("Pos: %s, Neu: %s, Neg: %s" % (len(pos), len(neu), len(neg)))
print("Total data: %s" % len(total_data))

#3. Featrure Extraction menggunakan modul"Tokenizer" dan"Pad_sequences"
max_features = 100000
tokenizer = Tokenizer(num_words=max_features, split=' ', lower=True)
tokenizer.fit_on_texts(total_data)
with open('tokenizer.pickle', 'wb') as handle:
    pickle.dump(tokenizer, handle, protocol=pickle.HIGHEST_PROTOCOL)
    print("tokenizer.pickle has created!")
# Memanggil modul tokenizer
X = tokenizer.texts_to_sequences(total_data)

vocab_size = len(tokenizer.word_index)
maxlen = max(len(x) for x in X )
# Memanggil modul pad sequences
X = pad_sequences(X)
with open('x_pad_sequences.pickle', 'wb') as handle:
    pickle.dump(X, handle, protocol=pickle.HIGHEST_PROTOCOL)
    print("x_pad_sequences.pickle has created!")

# 4. Memasukkan label datanya ke variabel y
Y = pd.get_dummies(labels)
Y = Y.values

with open('y_labels.pickle', 'wb') as handle:
    pickle.dump(Y, handle, protocol=pickle.HIGHEST_PROTOCOL)
    print("y_labels.pickle has created!")

# 5. Melakukan Training dan uji dataset
# Variabel x = menyimpan data teks pickle
file = open("x_pad_sequences.pickle", "rb")
X = pickle.load(file)
file.close()

# Varibel y = menyimpan label data pickle
file = open("y_labels.pickle", "rb")
Y = pickle.load(file)
file.close()

X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state=1)

embed_dim = 100
units = 64
# MEMBUAT SCRIPT MODEL LSTM
model = Sequential()
model.add(Embedding(max_features, embed_dim, input_length=X.shape[1]))
model.add(LSTM(units, dropout=0.2))
model.add(Dense(3, activation='softmax')) # activasi "SOFTMAX"
sgd = optimizers.Adam(learning_rate=0.001)
model.compile(loss = 'binary_crossentropy', optimizer='adam', metrics = ['accuracy'])
print(model.summary())

adam = optimizers.Adam(learning_rate = 0.001)
model.compile(loss = 'categorical_crossentropy', optimizer = adam, metrics = ['accuracy'])

# untuk mengantisipasi biar kita mendaapatkan nilai yang terbaik, EARLYSTOPPING
es = EarlyStopping(monitor='val_loss', mode='min', verbose=1)
history = model.fit(X_train, y_train, epochs=10, validation_data=(X_test, y_test), verbose=1, callbacks=[es])

# EVALUASI DENGAN 5 BAGIAN YAITU MATRIX, ACCURACY, F1, RECALL DAN PRECISION
predictions = model.predict(X_test)
y_pred = predictions
matrix_test = metrics.classification_report(y_test.argmax(axis=1), y_pred.argmax(axis=1))
print("Testing selesai")
print(matrix_test)


kf = KFold(n_splits=5, random_state=42, shuffle=True)
accuracies = []
y=Y
embed_dim = 100
units = 64
for iteration, data in enumerate(kf.split(X), start=1):
    data_train = X[data[0]]
    target_train = y[data[0]]
    
    data_test = X[data[1]]
    target_test = y[data[1]]
    
    model = Sequential()
    model.add(Embedding(max_features, embed_dim, input_length=X.shape[1]))
    model.add(LSTM(units, dropout=0.2))
    model.add(Dense(3,activation='softmax'))
    sgd = optimizers.Adam(learning_rate = 0.001)
    model.compile(loss = 'binary_crossentropy', optimizer = adam, metrics = ['accuracy'])
    
    adam = optimizers.Adam(learning_rate=0.001)
    model.compile(loss = 'categorical_crossentropy', optimizer = adam, metrics = ['accuracy'])
    
    es = EarlyStopping(monitor='val_loss', mode='min', verbose=1)
    history = model.fit(X_train, y_train, epochs=10, batch_size=10, validation_data=(X_test, y_test), verbose=1, callbacks=[es])
    
    predictions = model.predict(X_test)
    y_pred = predictions
# Iterasi data latih yg kita punya
accuracy = accuracy_score(y_test.argmax(axis=1), y_pred.argmax(axis=1))
print("Training ke-", iteration)
print(classification_report(y_test.argmax(axis=1), y_pred.argmax(axis=1)))
print("=======================================================")
accuracies.append(accuracy)

average_accuracy = np.mean(accuracies)

print()
print()
print()
print("Rata-rata Accuracy: ", average_accuracy)


plt.style.use('ggplot')

def plot_history(history):
    acc = history.history['accuracy']
    val_acc = history.history['val_accuracy']
    loss = history.history['loss']
    val_loss = history.history['val_loss']
    x = range(1, len(acc) + 1)
    
    plt.figure(figsize=(12, 5))
    plt.subplot(1, 2, 1)
    plt.plot(x, acc, 'b', label='Training acc')
    plt.plot(x, val_acc, 'r', label='Validation acc')
    plt.title('Training and validation accuracy')
    plt.legend()
    plt.subplot(1, 2, 2)
    plt.plot(x, loss, 'b', label='Training loss')
    plt.plot(x, val_loss, 'r', label='Validation loss')
    plt.title('Training and validation loss')
    plt.legend()
    
plt.show()
plot_history(history)
model.save('model.h5')
print('model has created!')

import re
from keras.models import load_model
input_text = '''
Rasa syukur, cukup.
'''
def cleansing (sent):
    string = sent.lower()
    string = re.sub(r'[^a-zA-Z0-9]', ' ', string)
    return string

sentiment = ['negative', 'neutral', 'positive']

text = [cleansing(input_text)]
predicted = tokenizer.texts_to_sequences(text)
guess = pad_sequences(predicted, maxlen=X.shape[1])

model = load_model('model.h5')
prediction = model.predict(guess)
polarity = np.argmax(prediction[0])

print("Text: ", text[0])
print("Sentiment: ", sentiment[polarity])