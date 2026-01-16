import numpy as np
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.datasets import cifar10
from tensorflow.keras.utils import to_categorical
from matplotlib import pyplot as plt


# ucitaj CIFAR-10 podatkovni skup
(X_train, y_train), (X_test, y_test) = cifar10.load_data()

# prikazi 9 slika iz skupa za ucenje
plt.figure()
for i in range(9):
    plt.subplot(330 + 1 + i)
    plt.xticks([])
    plt.yticks([])
    plt.imshow(X_train[i])

plt.show()


# pripremi podatke (skaliraj ih na raspon [0,1]])
X_train_n = X_train.astype('float32') / 255.0
X_test_n = X_test.astype('float32') / 255.0

# 1-od-K kodiranje
y_train = to_categorical(y_train)
y_test = to_categorical(y_test)

# CNN mreza
model = keras.Sequential()
model.add(layers.Input(shape=(32, 32, 3)))
model.add(layers.Conv2D(filters=32, kernel_size=(3, 3), activation='relu',
                        padding='same'))
model.add(layers.MaxPooling2D(pool_size=(2, 2)))
model.add(layers.Conv2D(filters=64, kernel_size=(3, 3), activation='relu', 
                        padding='same'))
model.add(layers.MaxPooling2D(pool_size=(2, 2)))
model.add(layers.Conv2D(filters=128, kernel_size=(3, 3), activation='relu', 
                        padding='same'))
model.add(layers.MaxPooling2D(pool_size=(2, 2)))
model.add(layers.Flatten())
model.add(layers.Dense(500, activation='relu'))
model.add(layers.Dropout(0.3))
model.add(layers.Dense(10, activation='softmax'))

model.summary()

# definiraj listu s funkcijama povratnog poziva
my_callbacks = [
    keras.callbacks.EarlyStopping(monitor="val_loss", patience=5, verbose=1),
    keras.callbacks.TensorBoard(log_dir='logs/cnn_dropout', 
                                update_freq=100)
]

optimizer = keras.optimizers.Adam(learning_rate=1.0)
model.compile(optimizer=optimizer, loss='categorical_crossentropy',
              metrics=['accuracy'])

model.fit(X_train_n, y_train, epochs=40, batch_size=64, callbacks=my_callbacks,
          validation_split=0.1)


score = model.evaluate(X_test_n, y_test, verbose=0)
print(f'Tocnost na testnom skupu podataka: {100.0*score[1]:.2f}')

# 1. zadatak
# 1.1. CNN mreza sastoji se od konvolucijskih slojeva, slojeva sažimanja, 
# flatten sloja i potpuno povezanih slojeva
# 1.3. Sto se dogodilo tijekom ucenja mreze? Na pocetku se povecavaju i 
# tocnost na skupu podataka za ucenje i na validacijskom skupu, a funkcije
# gubitka se smanjuju
# na 7. epohi tocnost na validacijskom skupu smanjuje se u odnosu na 
# prethodnu epohu , a funkcija gubitka na validacijskom skupu se povecava
# 1.3. Tocnost na skupu podataka za testiranje: 73.32

# 2.zadatak
# Nakon dodavanja droput sloja između dva potpuno povezana sloja, tocnost na
# skupu podataka za testiranje se povecala na 74.75 sto znaci da su se
# performanse poboljsale dodavanjem dropout sloja. Dropout sloj smanjuje
# ovisnost modela o pojedinacnim znacajkama cime se smanjuje mogucnost
# overfittinga

# 3. zadatak
# Dodavanjem ranog zaustavljanja s patience=5 zaustavlja proces ucenja na 11.
# epohi zato sto se funkcja gubitka na validacijskom skupu pocinje povecavati
# na 7. epohi,
# odnosno 5 uzastopnih epoha nije se smanjila prosjecna vrijednost funkcije
# gubitka na validacijskom skupu

# 4. zadatak
# 4.1. Ukoliko se koristi jako mala velicina serije proces ucenja traje dulje,
# kada je batch size 64, potrebno je 17s da se jedna epoha izvrsi, a kada je 
# batch size 1 potrebno je 397 sekundi
# 4.2. Ukoliko se koristi jako mali learning rate, tocnost na testnom skupu
# podataka se smanjuje, za learning_rate=0.01, dolazi do ranog zaustavljanja
# na 13. epohi i tocnost na testnom skupu podataka je 43.06
# 4.3. Ukoliko se odredjeni slojevi izbace iz mreze, tocnost na testnom skupu
# podataka se smanjuje
# 4.4. Ukoliko se skup podataka za ucenje smanji za 50%, tocnost na testnom
# skupu podataka se smanjuje