#!/usr/bin/env python
# coding: utf-8

# ### Import necessary libraries

# In[1]:


from pyspark.sql import SparkSession
from pyspark import SparkContext
import numpy as np
import sys
from pyspark.sql.functions import col, expr, when
from  pyspark.sql.functions import countDistinct
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql import functions as F


# ### Create the Spark Context and name the application.

# In[2]:


sc = SparkContext()


# In[3]:


spark = SparkSession.builder.appName('Flight-Delay').getOrCreate()


# ### Read the randomly sampled data file. On AWS this is read as a commandline argument.
# - For the sake of this notebook, we have taken only 100 samples. However we have run the algorithm on 100,000 records on AWS.

# In[4]:


df = spark.read.csv('raw_sample.csv', header = True, inferSchema = True)
# df = spark.read.csv(sys.argv[1], header = True, inferSchema = True)


# ### Feature Selection
# - We selected the features which have the most impact on the target variable
# - Load the data in a pyspark dataframe
# - Clean the data by dropping all the null valued rows
# - Encode the traget variable with 0 as the flight being on time and 1 as the flight being delayed (+ value)

# In[5]:


required_features = ['MONTH', 'DAY_OF_WEEK', 'AIRLINE','ORIGIN_AIRPORT','DESTINATION_AIRPORT','SCHEDULED_DEPARTURE',
                     'DEPARTURE_TIME', 'DEPARTURE_DELAY', 'TAXI_OUT','WHEELS_OFF', 'SCHEDULED_TIME','ELAPSED_TIME','AIR_TIME','WHEELS_ON','TAXI_IN',  'DISTANCE',
           'SCHEDULED_ARRIVAL','ARRIVAL_TIME', 'ARRIVAL_DELAY']


# In[6]:


df_req = df.select(required_features)


# In[7]:


df_req_clean = df_req.dropna()


# In[8]:


data = df_req_clean.withColumn("ARRIVAL_DELAY", when(col("ARRIVAL_DELAY") > 0,'YES').when(col("ARRIVAL_DELAY") <=0 ,'NO'))


# In[9]:


data.agg(countDistinct(data.ARRIVAL_DELAY).alias('c')).collect()


# ### Data Pipelining
# - A Pipeline is specified as a sequence of stages, and each stage is either a Transformer or an Estimator. These stages are run in order, and the input DataFrame is transformed as it passes through each stage. For Transformer stages, the transform() method is called on the DataFrame. For Estimator stages, the fit() method is called to produce a Transformer (which becomes part of the PipelineModel, or fitted Pipeline), and that Transformer’s transform() method is called on the DataFrame.

# In[11]:


categoricalColumns = ['AIRLINE', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT']
stages = []


# In[12]:


for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
    encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
    stages += [stringIndexer, encoder]


# In[14]:


label_stringIdx = StringIndexer(inputCol = 'ARRIVAL_DELAY', outputCol = 'label')
stages += [label_stringIdx]


# In[15]:


numericCols = ['MONTH','DAY_OF_WEEK', 'SCHEDULED_DEPARTURE', 'DEPARTURE_TIME', 'DEPARTURE_DELAY', 'TAXI_OUT', 'WHEELS_OFF','SCHEDULED_TIME','ELAPSED_TIME','AIR_TIME','WHEELS_ON','TAXI_IN','DISTANCE','SCHEDULED_ARRIVAL','ARRIVAL_TIME']
assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]


# In[16]:


pipeline = Pipeline(stages = stages)
pipelineModel = pipeline.fit(data)
data = pipelineModel.transform(data)
# selectedCols = ['label', 'features']+required_features
selectedCols = ['label', 'features']
data = data.select(selectedCols)


# ### Create Test and Train sets
# - Add an index to each of the input features vector and label pairs.
# - Split the dataset randomly into train and test
# - Convert the train and test features into a numpy array
# - Join the rdds to create an rdd with input vectors and their corressponding labels referenced to a key.

# In[17]:


data=data.withColumn("idx",F.monotonically_increasing_id())


# In[18]:


train,test = data.randomSplit([0.7,0.3])


# In[19]:


train_X_rdd=train.select(['features', 'idx']).rdd.map(lambda x : np.array(x)).map(lambda a: (a[1],np.array(a[0])))
test_X_rdd=test.select(['features', 'idx']).rdd.map(lambda x : np.array(x)).map(lambda a: (a[1],np.array(a[0])))


# In[20]:


train_Y_rdd = train.select(['label', 'idx']).rdd.map(lambda x : np.array(x)).map(lambda a: (int(a[1]),int(a[0])))
test_Y_rdd = test.select(['label', 'idx']).rdd.map(lambda x : np.array(x)).map(lambda a: (int(a[1]),int(a[0])))


# In[21]:


train_xy = train_X_rdd.join(train_Y_rdd)
test_xy = test_X_rdd.join(test_Y_rdd)


# ### Logistic Regression model
# - We have implemented the Stochastic Gradient Descent version of Logistic Regressio.
# - def fun_weights(): This function takes a tuple of input vector and its output label and returns component of the weight to be deducted from the total weights. A summation of all these components is then deducted from the final weight vector.
# - def fun_bias(): This function takes a tuple of input vector and it output label and returns a component of the bias that is to be deducted from the total bias. A summation of all these components is then deducted from the final bias value.

# In[22]:


def fun_weights(x_y):
    linear_model = np.dot(x_y[0],weights)+bias
    y_pred = 1/(1+np.exp(-linear_model)) #Sigmoid function
    dw_comp = np.dot(x_y[0],(y_pred-x_y[1]))
    return dw_comp


# In[23]:


def fun_bias(x_y):
    linear_model = np.dot(x_y[0],weights)+bias
    y_pred = 1/(1+np.exp(-linear_model)) #Sigmoid function
    db_comp = y_pred-x_y[1]
    return db_comp


# - Persist data RDDs for faster data access.

# In[24]:


train_X_rdd.persist()
train_Y_rdd.persist()


# In[25]:


train_features = len(train_X_rdd.collect()[0][1])
train_samples = train_X_rdd.count()


# - Initialize the weights to a zero vector of size train_features. The bias is also initialized to be 0. The learning rate is kept as 0.1. The model is trained for 100 iterations where the weights and bias is updated at every iteration.

# In[26]:


weights = np.zeros(train_features)
bias = 0
lr = 0.1
for i in range(100):
    dw = train_xy.map(lambda x: ("key",fun_weights(x[1]))).reduceByKey(lambda x,y: x+y).map(lambda x:x[1]*1/train_samples)
    db = train_xy.map(lambda x: ("key",fun_bias(x[1]))).reduceByKey(lambda x,y: x+y).map(lambda x:x[1]*1/train_samples)
    weights -= lr*dw.collect()[0]
    bias -= lr*db.collect()[0]


# In[27]:


test_X_rdd.persist()
test_Y_rdd.persist()


# - Predictions for the test set.

# In[28]:


def predictions(x_y):
    linear_model = np.dot(x_y[0],weights)+bias
    y_pred = 1/(1+np.exp(-linear_model)) #Sigmoid function
    if (y_pred >0.5):
        return 1
    else:
        return 0


# In[29]:


pred=test_xy.map(lambda x: (x[0], predictions(x[1])))


# - Compute predictions for the test set and create a comparison RDD with train and test targets to evaluate classification metrics

# In[30]:


comparison_rdd = test_Y_rdd.join(pred)


# In[ ]:


comparison_rdd.saveAsTextFile(sys.argv[2])


# In[31]:


y_true_pred = comparison_rdd.map(lambda c : (c[1])).collect()


# In[32]:


test = []
pred = []
for i in range(len(y_true_pred)):
    test.append(y_true_pred[i][1])
    pred.append(y_true_pred[i][0])


# ### Classification Metrics

# In[33]:


def conf_matrix(pred, test):
    conf_matrix={'TP':0,'TN':0,'FP':0,'FN':0}
    
    for i in range(len(test)):
        if test[i]==1 and pred[i]==0:
            conf_matrix['FN']+=1
        else:
            if test[i]==1 and pred[i]==1:
                conf_matrix['TP']+=1
            else:
                if test[i]==0 and pred[i]==1:
                    conf_matrix['FP']+=1
                else:
                    if test[i]==0 and pred[i]==0:
                        conf_matrix['TN']+=1
    return conf_matrix

def get_accuracy(pred, test, matrix):
    accuracy=float(matrix['TP']+matrix['TN'])/(matrix['TP']+matrix['TN']+matrix['FN']+matrix['FP'])
    return accuracy

def get_precision(pred, test, matrix):
    try:
        precision=float(matrix['TP'])/(matrix['TP']+matrix['FP'])
    except:
        precision=0
            
    return precision

def get_recall(pred,test, matrix):
    
    try:
        recall=float(matrix['TP'])/(matrix['TP']+matrix['FN'])
    except:
        recall=0
    return recall

def get_f1_measure(pred,test, matrix):
    precision=get_precision(pred,test, matrix)
    recall=get_recall(pred,test, matrix)
    try:
        f1_measure= 2*precision*recall/(precision+recall)
    except:
        f1_measure=0
    return f1_measure


# In[39]:


matrix=conf_matrix(pred,test)
print("Custom model Stats: Logistic Regression")
print("-"*30)
print("Confusion Matrix: ",matrix)
print("-"*30)
print("Accuracy: ",get_accuracy(pred, test, matrix))
print("Precision: ",get_precision(pred, test, matrix))
print("Recall: ",get_recall(pred, test, matrix))
print("F-measure: ",get_f1_measure(pred, test, matrix))


# ### Implementation of ML LIB Classification algorithms
# - Logistic Regression 

# In[34]:


train_ML, test_ML = data.randomSplit([0.7, 0.3])


# In[35]:


from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=100)
lrModel = lr.fit(train_ML)


# In[36]:


predictions = lrModel.transform(test_ML)


# - Random Forest

# In[37]:


from pyspark.ml.classification import RandomForestClassifier
rf = RandomForestClassifier(labelCol='label', featuresCol='features', 
                            numTrees=20, maxDepth=20)
rfModel = rf.fit(train_ML)


# In[38]:


predictionsrf = rfModel.transform(test_ML)


# - Evaluation Metrics

# In[40]:


from pyspark.mllib.evaluation import MulticlassMetrics

results = predictions.select(['prediction', 'label'])
predictionAndLabels=results.rdd
metrics = MulticlassMetrics(predictionAndLabels)

cm=metrics.confusionMatrix().toArray()
accuracy=(cm[0][0]+cm[1][1])/cm.sum()
precision=(cm[0][0])/(cm[0][0]+cm[1][0])
recall=(cm[0][0])/(cm[0][0]+cm[0][1])
f1Score=(2*precision*recall)/(precision+recall)
print("ML LIB model stats: Logistic Regression")
print("-"*30)
# print("Confusion matrix =%s" % metrics.confusionMatrix())
print("Accuracy = %s" % accuracy)
print("Precision = %s" % precision)
print("Recall = %s" % recall)
print("F1 Score = %s" % f1Score)


# In[41]:


results_rf = predictionsrf.select(['prediction', 'label'])
predictionAndLabels_rf=results_rf.rdd
metrics = MulticlassMetrics(predictionAndLabels_rf)

cm=metrics.confusionMatrix().toArray()
accuracy=(cm[0][0]+cm[1][1])/cm.sum()
precision=(cm[0][0])/(cm[0][0]+cm[1][0])
recall=(cm[0][0])/(cm[0][0]+cm[0][1])
f1Score=(2*precision*recall)/(precision+recall)
print("ML LIB model stats: Random Forest")
print("-"*30)
# print("Confusion matrix =%s" % metrics.confusionMatrix())
print("Accuracy = %s" % accuracy)
print("Precision = %s" % precision)
print("Recall = %s" % recall)
print("F1 Score = %s" % f1Score)


# - Unpersist all the RDDs and stop the Spark Context

# In[42]:


train_X_rdd.unpersist()
train_Y_rdd.unpersist()

test_X_rdd.unpersist()
test_Y_rdd.unpersist()

sc.stop()
