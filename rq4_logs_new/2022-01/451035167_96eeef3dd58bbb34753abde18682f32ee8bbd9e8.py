
import glob
from sklearn.model_selection import train_test_split
import tensorflow as tf
import numpy as np
from tensorflow.keras.optimizers import Adam
from sklearn import svm
from keras_pyramid_pooling_module import PyramidPoolingModule
from sklearn import metrics

n = glob.glob('data/disease/disease/*.*')
nd = glob.glob('data/disease/non-disease/*.*')

data = []
labels = []
for i in n:   
    image=tf.keras.preprocessing.image.load_img(i, color_mode='rgb', 
    target_size= (280,280))
    image=np.array(image)
    data.append(image)
    labels.append(1)
for i in nd:   
    image=tf.keras.preprocessing.image.load_img(i, color_mode='rgb', 
    target_size= (280,280))
    image=np.array(image)
    data.append(image)
    labels.append(2)

data = np.array(data)
labels = np.array(labels)


X_train, X_test, ytrain, ytest = train_test_split(data, labels, test_size=0.2,
                                                random_state=42)

model = tf.keras.models.Sequential([
    tf.keras.layers.Conv2D(16,(3,3),activation = "relu" , input_shape = (280,280,3)) ,
    tf.keras.layers.MaxPooling2D(2,2),
    tf.keras.layers.Conv2D(32,(3,3),activation = "relu") ,  
    tf.keras.layers.MaxPooling2D(2,2),
    tf.keras.layers.Conv2D(64,(3,3),activation = "relu") ,  
    tf.keras.layers.MaxPooling2D(2,2),
    tf.keras.layers.Conv2D(128,(3,3),activation = "relu"),  
    tf.keras.layers.MaxPooling2D(2,2),
    tf.keras.layers.Conv2D(128,(3,3),activation = "relu"),  
    PyramidPoolingModule(128, (3, 3),padding='same'),
    tf.keras.layers.Flatten(), 
    tf.keras.layers.Dense(550,activation="relu"),      #Adding the Hidden layer
    tf.keras.layers.Dropout(0.1,seed = 2019),
    tf.keras.layers.Dense(400,activation ="relu"),
    tf.keras.layers.Dropout(0.3,seed = 2019),
    tf.keras.layers.Dense(300,activation="relu"),
    tf.keras.layers.Dropout(0.4,seed = 2019),
    tf.keras.layers.Dense(200,activation ="relu"),
    tf.keras.layers.Dropout(0.2,seed = 2019),
    tf.keras.layers.Dense(5,activation = "softmax")   #Adding the Output Layer
])


model.summary()
opt = Adam(lr=0.000001
           )
#model learing the features from the dataset
model.compile(optimizer = opt , loss = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True) , metrics = ['accuracy'])
history = model.fit(X_train,ytrain,epochs =20 , validation_data = (X_test, ytest))
# get the accuracy,validation,accuracy,loss,validation loss after the each epochs
acc = history.history['accuracy']
val_acc = history.history['val_accuracy']
loss = history.history['loss']
val_loss = history.history['val_loss']
# setting the epoch range 
epochs_range = range(100)
i=glob.glob('test data/4.jpg')
x=tf.keras.preprocessing.image.load_img(i[0], color_mode='rgb', target_size= (280,280))
image=np.array(x)
image=image.reshape(1,280,280,3)
predict_x=model.predict(image)


classes_x=np.argmax(predict_x,axis=1)

print(classes_x)
if classes_x==1:
    print("3-class")
    
elif classes_x==2:
    print("non-disease")
    x=tf.keras.preprocessing.image.load_img(i[0], color_mode='rgb', target_size= (280,280))
    image=np.array(x)
    image=image.reshape(1,280,280,3)
    predict_x=model.predict(image)
    result = np.argmax(predict_x)

    n1 = glob.glob('data/class/Class 1/*.*')
    nd1 = glob.glob('data/class/Class 2/*.*')
    
    data1 = []
    labels1 = []
    
    for i in n1:   
        image=tf.keras.preprocessing.image.load_img(i, color_mode='grayscale', 
        target_size= (280,280))
        image=np.array(image)
        image = image.reshape(-1)
        data1.append(image)
        labels1.append(1)
    for i in nd1:   
        image=tf.keras.preprocessing.image.load_img(i, color_mode='grayscale', 
        target_size= (280,280))
        image=np.array(image)
        image = image.reshape(-1)
        data1.append(image)
        labels1.append(2)
    
    data1 = np.array(data1)
    labels1 = np.array(labels1)
    
    
    X_train, X_test, y_train, y_test = train_test_split(data1, labels1, test_size=0.3,
                                                    random_state=1)
    
    
    
    #Create a svm Classifier
    clf = svm.SVC(kernel='rbf', random_state=1, gamma=1.0, C=10.0) # Linear Kernel
    
    #Train the model using the training sets
    clf.fit(X_train, y_train)
    
    #Predict the response for test dataset
    y_pred = clf.predict(X_test)
    
    #Import scikit-learn metrics module for accuracy calculation
    
    
    # Model Accuracy: how often is the classifier correct?
    print("Accuracy:",metrics.accuracy_score(y_test, y_pred))
    
    print ("Classification report for %s" % clf)
    print (metrics.classification_report(y_test, y_pred))
    print ("Confusion matrix")
    print (metrics.confusion_matrix(y_test, y_pred))
    
    
    i=i=glob.glob('test data/10.jpg')
    image=tf.keras.preprocessing.image.load_img(i[0], color_mode='grayscale', 
    target_size= (280,280))
    image=np.array(image)
    image = image.reshape(1,-1)
    d = clf.predict(image)
    print(d)
    
    if d==1:
        print("class-1")
    else:
        print("class-2")