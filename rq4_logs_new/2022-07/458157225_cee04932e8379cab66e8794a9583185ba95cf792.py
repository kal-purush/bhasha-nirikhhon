import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from sklearn.model_selection import train_test_split
matplotlib.rcParams['axes.unicode_minus'] = False
from gensim.models.word2vec import Word2Vec
from konlpy.tag import Okt
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.datasets import imdb
from tensorflow.keras.utils import plot_model

#-----------------------------------------------------------------------
# 데이터 불러오기
(train_input, train_target), (test_input, test_target) = keras.datasets.fashion_mnist.load_data()

# 3차원 데이터 변환 : 3차원이어야 함.
train_scaled = train_input.reshape(-1, 28, 28, 1) / 255.0

# train,test데이터 분리
train_scaled, val_scaled, train_target, val_target = train_test_split(
    train_scaled, train_target, test_size=0.2, random_state=42)

#-------------------------------------------------------------------------
# 합성곱 신경망 선언
model = keras.Sequential()

# 32개필터, 3은 (3,3) depth 1- 입력크기와 같음,  
model.add(keras.layers.Conv2D(32, kernel_size=3, activation='relu', 
                              padding='same', input_shape=(28,28,1)))

# 최대풀링 - 사이즈 절반으로 줄임 - (28,28,32) -> (14,14,32)
model.add(keras.layers.MaxPooling2D(2))

# 1번 더 사용 크기는 64필터 - (14,14,32) -> 64
model.add(keras.layers.Conv2D(64, kernel_size=(3,3), activation='relu', 
                              padding='same'))
model.add(keras.layers.MaxPooling2D(2))

model.add(keras.layers.Flatten())
model.add(keras.layers.Dense(100, activation='relu'))
model.add(keras.layers.Dropout(0.4))
model.add(keras.layers.Dense(10, activation='softmax'))

# 모델 요약
model.summary()

# 모델 input,output 정보
keras.utils.plot_model(model)

# 이미지 저장
keras.utils.plot_model(model, show_shapes=True, to_file='cnn-architecture.png', dpi=300)

#-----------------------------------------------------------------
# 모델 훈련
model.compile(optimizer='adam', loss='sparse_categorical_crossentropy', 
              metrics='accuracy')

checkpoint_cb = keras.callbacks.ModelCheckpoint('best-cnn-model.h5', 
                                                save_best_only=True)
early_stopping_cb = keras.callbacks.EarlyStopping(patience=2,
                                                  restore_best_weights=True)

history = model.fit(train_scaled, train_target, epochs=20,
                    validation_data=(val_scaled, val_target),
                    callbacks=[checkpoint_cb, early_stopping_cb])


# 그래프 그리기
plt.plot(history.history['loss'])
plt.plot(history.history['val_loss'])
plt.xlabel('epoch')
plt.ylabel('loss')
plt.legend(['train', 'val'])
plt.show()

#-----------------------------------------------------------
# 정확도
score = model.evaluate(val_scaled, val_target)
print("loss accuracy : ",score)

# test데이터 0번 출력
plt.imshow(val_scaled[0].reshape(28, 28), cmap='gray_r')
plt.show()

#-------------------------------------------------------------
# 1번 예측 - 10개의 확률 출력
# 9번이 100% 예측
preds = model.predict(val_scaled[0:1])
print(preds)

# 1-10까지 막대그래프 출력
plt.bar(range(1, 11), preds[0])
plt.xlabel('class')
plt.ylabel('prob.')
plt.show()

classes = ['티셔츠', '바지', '스웨터', '드레스', '코트',
           '샌달', '셔츠', '스니커즈', '가방', '앵클 부츠']

# 예측 글자 출력 - 최대크기 위치 번호 출력 8 위치
# argmax 배열중 가장 큰 값 위치 출력
print(classes[np.argmax(preds)])

# test데이터 1개 입력데이터 만듬
test_scaled = test_input.reshape(-1, 28, 28, 1) / 255.0

# 정확도
print(model.evaluate(test_scaled, test_target))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from sklearn.model_selection import train_test_split
matplotlib.rcParams['axes.unicode_minus'] = False
from gensim.models.word2vec import Word2Vec
from konlpy.tag import Okt
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.datasets import imdb
from tensorflow.keras.utils import plot_model

#-----------------------------------------------------------------------
# 모델 불러오기
model = keras.models.load_model('best-cnn-model.h5')
# 모델의 층 객체를 list로 보여줌.
print(model.layers)

conv = model.layers[0]
print(conv.weights[0].shape, conv.weights[1].shape)