import numpy as np
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from knn import KNN

angry_train = np.load('../data/selected_landmarks_train/angry_landmarks.npy')
disgust_train = np.load('../data/selected_landmarks_train/disgust_landmarks.npy')
fear_train = np.load('../data/selected_landmarks_train/fear_landmarks.npy') 
happy_train = np.load('../data/selected_landmarks_train/happy_landmarks.npy')
neutral_train = np.load('../data/selected_landmarks_train/neutral_landmarks.npy')
sad_train = np.load('../data/selected_landmarks_train/sad_landmarks.npy')
suprise_train = np.load('../data/selected_landmarks_train/surprise_landmarks.npy')

angry_test = np.load('../data/selected_landmarks_test/angry_landmarks.npy')
disgust_test = np.load('../data/selected_landmarks_test/disgust_landmarks.npy')
fear_test = np.load('../data/selected_landmarks_test/fear_landmarks.npy') 
happy_test = np.load('../data/selected_landmarks_test/happy_landmarks.npy')
neutral_test = np.load('../data/selected_landmarks_test/neutral_landmarks.npy')
sad_test = np.load('../data/selected_landmarks_test/sad_landmarks.npy')
suprise_test = np.load('../data/selected_landmarks_test/surprise_landmarks.npy')

train_data = {
    'angry': angry_train,
    'disgust': disgust_train,
    'fear': fear_train,
    'happy': happy_train,
    'neutral': neutral_train,
    'sad': sad_train,
    'suprise': suprise_train
}

test_data = {
    'angry': angry_test,
    'disgust': disgust_test,
    'fear': fear_test,
    'happy': happy_test,
    'neutral': neutral_test,
    'sad': sad_test,
    'suprise': suprise_test
}

train_X_list = []
train_y_list = []

for emotion, data in train_data.items():
    # Reshape each sample from (90, 2) to a flat vector of length 180
    train_X_list.append(data.reshape(data.shape[0], -1))
    # Create an array of labels for the emotion
    train_y_list.append(np.full((data.shape[0],), emotion))


X_train = np.concatenate(train_X_list, axis=0)
y_train = np.concatenate(train_y_list, axis=0)

test_X_list = []
test_y_list = []

for emotion, data in test_data.items():
    test_X_list.append(data.reshape(data.shape[0], -1))
    test_y_list.append(np.full((data.shape[0],), emotion))

X_test = np.concatenate(test_X_list, axis=0)
y_test = np.concatenate(test_y_list, axis=0)

scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

knn = KNN(1)
knn.fit(X_train_scaled, y_train)

accuracy = knn.score(X_test_scaled, y_test)
print("\nTest Accuracy:", accuracy)