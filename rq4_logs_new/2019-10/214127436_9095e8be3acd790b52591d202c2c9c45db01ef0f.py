import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2' 
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import tensorflow as tf
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE


learning_rate = 0.05
training_epochs = 500
batch_size = 1100


df = pd.read_csv("Data.csv") 
test_set2 = df[4400:5508].copy()

y_test2 = test_set2['Activities_Types'].values
#y_test2 = y_test2.reshape(1108,1)

#print(df[:10]) # 5508*69
d = pd.get_dummies(df['Activities_Types'])

df.drop(['Activities_Types'], axis=1,inplace=True) # inplace=True 消除原始資料
#res = pd.concat([df, d], axis=1)  # axis 0: 對行操作; 1: 對列操作

train_set = df[:4400].copy()
test_set = df[4400:5508].copy()

y_train = d[:4400].copy().values
y_train = y_train.reshape(4400,6)
x_train = train_set.values

y_test = d[4400:5508].copy().values
y_test = y_test.reshape(1108,6)
x_test = test_set.values


total_batch = int(len(x_train)/batch_size)
#print(total_batch)

#"""
def add_layer(inputs,in_size,out_size,layer_name,activation_function=None):   # af = None => linear function
    # add one more layer and return output on this layer
    with tf.name_scope('layer'):
        with tf.name_scope('Weights'):
            Weights = tf.Variable(tf.random_normal([in_size,out_size]),name='W')
        with tf.name_scope('Biases'):    
            biases = tf.Variable(tf.zeros([1,out_size]) + 0.1,name='b')
        with tf.name_scope('Wx_plus_b'):
            Wx_plus_b = tf.add(tf.matmul(inputs,Weights),biases)
            #Wx_plus_b = tf.matmul(inputs,Weights) + biases
        if activation_function is None:
            outputs = Wx_plus_b
        else:
            outputs = activation_function(Wx_plus_b)
        return outputs

# define placeholder for inputs to network
with tf.name_scope('inputs'):    
    xs = tf.placeholder(tf.float32, [None, 68]) 
    ys = tf.placeholder(tf.float32, [None, 6])

# add output layer
l1 = add_layer(xs, 68, 40, 'l1', activation_function=tf.nn.tanh)
#l2 = add_layer(l1, 40, 20, 'l2', activation_function=tf.nn.tanh)
#prediction = add_layer(l2, 20, 6, 'l3', activation_function=tf.nn.softmax)
prediction = add_layer(l1, 40, 6, 'l2', activation_function=tf.nn.softmax)

def compute_accuracy(v_xs, v_ys):
    global prediction
    y_pre = sess.run(prediction, feed_dict={xs: v_xs})
    correct_prediction = tf.equal(tf.argmax(y_pre,1), tf.argmax(v_ys,1)) ###
    accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))
    result = sess.run(accuracy, feed_dict={xs: v_xs, ys: v_ys})
    return result

# the error between prediction and real data
cross_entropy = tf.reduce_mean(-tf.reduce_sum(ys * tf.log(prediction),reduction_indices=[1])) + 1e-8 # loss
#cross_entropy = tf.reduce_mean(-tf.reduce_sum(ys * tf.log(prediction),reduction_indices=[1])) * total_batch + 1e-8 # loss
train_step = tf.train.AdamOptimizer(learning_rate).minimize(cross_entropy) 
#train_step = tf.train.GradientDescentOptimizer(learning_rate).minimize(cross_entropy) ### 1. (iii)
#train_step = tf.train.AdagradOptimizer(learning_rate).minimize(cross_entropy) ### 1. (iii)

#"""

train_loss = np.zeros([training_epochs,1])
train_accuracy = np.zeros([training_epochs,1])
validate_loss = np.zeros([training_epochs,1])
validate_accuracy = np.zeros([training_epochs,1])

tp = np.zeros(6)
fp = np.zeros(6)
fn = np.zeros(6)

Precision = np.zeros(6)
Recall = np.zeros(6)
F1_score = np.zeros(6)


#"""

# create tensorflow structure start ###
#sess = tf.Session()   


gpu_options = tf.GPUOptions(per_process_gpu_memory_fraction=0.333)  # if RAM is not enough 

#with tf.Session(config=tf.ConfigProto(gpu_options=gpu_options)) as sess:
with tf.Session() as sess: 
    #sess = tf.Session(config=tf.ConfigProto(gpu_options=gpu_options))  

    sess.run(tf.global_variables_initializer()) # Activate the initial ### important
    sess.run(tf.local_variables_initializer()) 

    for epoch in range(training_epochs):
        #print(epoch)
        #sess.run(train_step, feed_dict={xs: x_train, ys:y_train})
        
        ### mini-batch
        #"""
        for batch in range(total_batch):
            start = batch * batch_size
            end = start + batch_size
            sess.run(train_step, feed_dict={xs: x_train[start:end], ys:y_train[start:end]})
        #"""
        
        #"""
        train_loss[epoch]= sess.run(cross_entropy,feed_dict={xs: x_train,ys:y_train})
        train_accuracy[epoch] = compute_accuracy(x_train, y_train)
        validate_loss[epoch] = sess.run(cross_entropy,feed_dict={xs: x_test,ys:y_test})
        validate_accuracy[epoch] = compute_accuracy(x_test, y_test)
        #"""

        if epoch % 50 == 0: 
            print(epoch) 
            
            print("Train set")
            print('Loss: ', sess.run(cross_entropy,feed_dict={xs: x_train,ys:y_train}))  
            print('Accuracy: ', compute_accuracy(x_train, y_train))
            #print('Loss: ', train_loss[epoch])
            #print('Accuracy: ', train_accuracy[epoch]) 
            
            print("Validation set")
            print('Loss: ', sess.run(cross_entropy,feed_dict={xs: x_test,ys:y_test})) 
            print('Accuracy: ', compute_accuracy(x_test, y_test))
            #print('Loss: ', validate_loss[epoch])
            #print('Accuracy: ', validate_accuracy[epoch])
            
        if epoch == (training_epochs-1): 
            print(epoch) 
            
            print("Train set")
            print('Loss: ', sess.run(cross_entropy,feed_dict={xs: x_train,ys:y_train}))  
            print('Accuracy ', compute_accuracy(x_train, y_train))
            #print('Loss: ', train_loss[epoch])
            #print('Accuracy ', train_accuracy[epoch]) 
            
            print("Validation set")
            print('Loss: ', sess.run(cross_entropy,feed_dict={xs: x_test,ys:y_test})) 
            print('Accuracy ', compute_accuracy(x_test, y_test))
            #print('Loss: ', validate_loss[epoch])
            #print('Accuracy ', validate_accuracy[epoch])
    
    ### Calculate TP, FP, FN  
    y_pred = sess.run(prediction, feed_dict={xs:x_test}) 
    results = y_pred.argmax(1) + 1 
    for i in range(len(y_pred)):
        if(y_test2[i] == 1):
            if(results[i] == 1):
                tp[0] += 1
            else:
                fn[0] += 1            ###( FN: if class:1, precdict != class, actually == class;
                fp[results[i]-1] += 1 ###  FP: if class:1, predicit == class, actually != class)
        elif(y_test2[i] == 2):
            if(results[i] == 2):
                tp[1] += 1
            else:
                fn[1] += 1
                fp[results[i]-1] += 1    
        elif(y_test2[i] == 3):
            if(results[i] == 3):
                tp[2] += 1
            else:
                fn[2] += 1
                fp[results[i]-1] += 1
        elif(y_test2[i] == 4):
            if(results[i] == 4):
                tp[3] += 1
            else:
                fn[3] += 1
                fp[results[i]-1] += 1    
        elif(y_test2[i] == 5):
            if(results[i] == 5):
                tp[4] += 1
            else:
                fn[4] += 1
                fp[results[i]-1] += 1   
        elif(y_test2[i] == 6):
            if(results[i] == 6):
                tp[5] += 1
            else:
                fn[5] += 1
                fp[results[i]-1] += 1          
    
    for i in range(6):
        Precision[i] = tp[i]/(tp[i] + fp[i])
        Recall[i] = tp[i]/(tp[i] + fn[i])
        F1_score[i] = 2 * (Precision[i] * Recall[i]) / (Precision[i] + Recall[i])
 
    ### 2. Hidden Test Set
    fd = pd.read_csv("Test_no_Ac.csv") 
    
    x_hidden = fd.values
    
    y_hidden = sess.run(prediction, feed_dict={xs:x_hidden}) 
    Hidden_results = y_hidden.argmax(1) + 1
    
    length = len(x_hidden)
    #Index = []
    #for i in range(len(x_hidden)):
    #    Index.append(str(1 + i))
    
    file1 = open('107062512_answer.txt','w')
    for i in range(length):
        file1.write(str(1 + i))
        file1.write("\t")
        file1.write(str(Hidden_results[i]))
        file1.write("\n")
    
    
    
### Precision, Recall, F1-score 1. (ii)
tp_temp = 0
tpfp_temp = 0
tpfn_temp = 0

for i in range(6):
     tp_temp += tp[i]
     tpfp_temp += tp[i] + fp[i]
     tpfn_temp += tp[i] + fn[i]
    
Micro_Precision = tp_temp / tpfp_temp
Micro_Recall = tp_temp / tpfn_temp
Micro_F1_score = 2 * (Micro_Precision * Micro_Recall) / (Micro_Precision + Micro_Recall)

Macro_Precision = 0.0
Macro_Recall = 0.0
for i in range(6): 
    Macro_Precision += Precision[i]
    Macro_Recall += Recall[i]

Macro_Precision = Macro_Precision / 6 
Macro_Recall = Macro_Recall / 6   
Macro_F1_score = 2 * (Macro_Precision * Macro_Recall) / (Macro_Precision + Macro_Recall)

#"""
for i in range(6):
    print('Class :', i + 1)
    print('Precision = ', Precision[i])
    print('Recall = ', Recall[i])
    print('F1_score = ', F1_score[i])

print('Micro_Precision = ', Micro_Precision)
print('Micro_Recall = ', Micro_Recall)
print('Micro_F1_score = ', Micro_F1_score)
print('Macro_Precision = ', Macro_Precision)
print('Macro_Recall = ', Macro_Recall)
print('Macro_F1_score = ', Macro_F1_score)
#"""

### Plot graph  1. (i)(a,b,c,d)      
#"""      
### Loss
plt.figure()
y_range = range(0,training_epochs)       

plt.plot(y_range, train_loss, color='blue', label="train")   
plt.plot(y_range, validate_loss, color='orange', label="validate")

plt.xlabel('epoch')
plt.ylabel('loss')
plt.legend(loc='best')       
plt.show()

### Accuracy
plt.figure()

plt.plot(y_range, train_accuracy, color='blue', label="train")   
plt.plot(y_range, validate_accuracy, color='orange', label="validate")

plt.xlabel('epoch')
plt.ylabel('accuracy')
plt.legend(loc='best')       
plt.show()


### PCA 1. (iv)

pca = PCA(n_components=2)
X_r = pca.fit(x_test).transform(x_test)

# Percentage of variance explained for each components
print('explained variance ratio (first two components): %s'
      % str(pca.explained_variance_ratio_))
plt.figure()

colors = ['navy', 'turquoise', 'darkorange','red','blue','yellow']
target_names = ['1','2','3','4','5','6']
lw = 2
### y_test2 is (1108, ) (1-Dimension)
for color, i, target_name in zip(colors, [1,2,3,4,5,6], target_names):
    plt.scatter(X_r[y_test2 == i, 0], X_r[y_test2 == i, 1], color=color, alpha=.8, lw=lw,
                label=target_name)
    
plt.legend(loc='best', shadow=False, scatterpoints=1)
plt.title('PCA')

plt.show()


### T-SNE 1. (v)

tsne = TSNE(n_components=2)
X_r = tsne.fit(x_test).fit_transform(x_test)

plt.figure()

colors = ['navy', 'turquoise', 'darkorange','red','blue','yellow']
target_names = ['1','2','3','4','5','6']
lw = 2
### y_test2 is (1108, ) (1-Dimension)
for color, i, target_name in zip(colors, [1,2,3,4,5,6], target_names):
    plt.scatter(X_r[y_test2 == i, 0], X_r[y_test2 == i, 1], color=color, alpha=.8, lw=lw,
                label=target_name)
    
plt.legend(loc='best', shadow=False, scatterpoints=1)
plt.title('T-SNE')

plt.show()
#"""

