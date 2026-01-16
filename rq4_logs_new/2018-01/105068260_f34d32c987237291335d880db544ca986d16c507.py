# -*- coding: utf-8 -*-
"""
Using kmeans to create a bag of visual words and SVM to predict
object recognition in images.

By: Austin Schwinn, Jérémie Blanchard, and Oussama Bouldjedri.

MLDM Master's Year 2
Fall Semester 2017
"""
import os
###############################################################################
#Set Params
Classes = os.listdir('D:/GD/MLDM/Computer_Vision_Project/cnn5/data/training')
test_dir = 'D:/GD/MLDM/Computer_Vision_Project/Data/test_VOC2007/JPEGImages/'
test_ann_dir = "D:/GD/MLDM/Computer_Vision_Project/Data/test_VOC2007/Annotations2/"
results_file = 'D:/GD/MLDM/Computer_Vision_Project/cnn5/results/cvp_bow_sigmoid_10_10_results.csv'
specific_img = None #[] Create a list of specific images in the directory
###############################################################################


#%%
#Set working directory
os.chdir('D:/GD/MLDM/Computer_Vision_Project/github2')
#os.path.dirname(os.path.abspath(__file__))
os.getcwd()

import features_kmeans as km
import pandas as pd
import timeit
import numpy as np
import pickle
import cv2
import xml.etree.ElementTree as ET


from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn import svm
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import normalize
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score



#%%
#Load assetts
obj_total = pickle.load(open('D:/GD/MLDM/Computer_Vision_Project/obj_total.sav','rb'))   
obj_desc_list = obj_total[0] 
obj_id = obj_total[1]  
obj_name   = obj_total[2] 

pca = pickle.load(open('D:/GD/MLDM/Computer_Vision_Project/obj_pca.sav','rb'))
pca_obj_desc = pd.DataFrame(obj_desc_list)
pca_obj_desc = pd.DataFrame(pca.transform(pca_obj_desc))
obj_kmeans_total = pickle.load(open('D:/GD/MLDM/Computer_Vision_Project/obj_kmeans.sav','rb'))
svms = pickle.load(open('D:/GD/MLDM/Computer_Vision_Project/svms_obj3.sav','rb'))    
#%%
#Get frequency of each descripter
def desc_freq(clusters):
    freq = np.array(np.repeat(0,1000),dtype='float64').reshape((1,1000))
    code, counts = np.unique(clusters, return_counts=True)
    
    for j in range(len(code)):
        freq[0,code[j]] = counts[j]
    
    return freq
#%%
def make_prediction(test_dir,test_ann_dir,target_size,specific_img=None):
#%%
    #Get all test images
    test_imgs = os.listdir(test_dir)
    test_anns = os.listdir(test_ann_dir)
    pred_class = pd.DataFrame(index=Classes)
    true_class = pd.DataFrame(index=Classes)
    
    
    if specific_img:
        test_imgs = [x for x in test_imgs if x in specific_img]
    
    
    #Iterate and get prediction and correct class tables
    desc = []
    img_num = []
    img_labels = []
    for img_name in test_imgs:
        img_name = img_name[:-4]
        img_num = img_num + [img_name]
        #Ensure we have correct label values
        if (img_name+'.xml') in test_anns:
            
            print('descr: ' + img_name)
            #Load image
            #img = Image.open(test_dir+img_name+'.jpg')
            img = cv2.imread((test_dir+img_name+'.jpg'))
        
            #Convert to grayscale
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    
    
            #load feature detection algorithms
            #brisk=cv2.BRISK_create()
            sift = cv2.xfeatures2d.SIFT_create()
      
            #Run feature detection algorithm
            #k_brisk = brisk.detect(gray,None)
            k_sift = sift.detect(gray,None)
           
            #Computer keypoints and descriptors
            #k_brisk,d_brisk = brisk.compute(gray,k_brisk)
            k_sift,d_sift = sift.compute(gray,k_sift)
            desc = desc + [d_sift]
    
            #Use pca to reduce dimensionality
            pca_desc = pd.DataFrame(pca.transform(d_sift))
      
            #Predict using KMEANS to get bag of words for svm training objects
            clusters = obj_kmeans_total.predict(pca_desc.iloc[:,0:50])          
            
            #Format to frequency of cluster for svm training feed
            #obj_cluster_pivot = km.cluster_format(obj_clusters_total, obj_id,obj_name)
            cluster_freq_pivot = desc_freq(clusters)  

            #Normalize
            #cluster_freq_pivot = normalize(cluster_freq_pivot,axis=1)
    
            #Predict using SVM
            preds = pd.DataFrame()
            for label, svm in svms.items():
                    pred = svm.predict(cluster_freq_pivot)
                    preds.loc[label,img_name] = pred
            
            #Continually combine
            pred_class = pred_class.join(preds)
            
            print(img_name)
           
            #Get correct labels
            tree = ET.parse(test_ann_dir + img_name + '.xml')
            root = tree.getroot() 
            
            class_names = []                
            for child in root:
                if child.tag == "object":
                    obj_name = child.find("name").text
                    if obj_name  not in class_names:
                        class_names += [obj_name]
                        img_labels += [class_names]
            
            #Create one hot encoding
            one_hot = pd.DataFrame(np.repeat(0,20),index=Classes,columns=[img_name])
            for class_name in class_names:
                one_hot.loc[class_name,img_name] = 1
            true_class = true_class.join(one_hot)
    results = pd.DataFrame(columns=['tp','fp','tn','fn','acc','prec','rec'])         
    #Compare predictions vs true labels
    tp = 0
    fp = 0
    tn = 0
    fn = 0
    for y in pred_class.index:
        temp_tp = 0
        temp_fp = 0
        temp_tn = 0
        temp_fn = 0
        for x in pred_class.columns:
            true_val = true_class.loc[y,x] 
            pred_val = pred_class.loc[y,x]
            if ((true_val==1) & (pred_val==1)):
                tp += 1
                temp_tp += 1
            elif ((true_val==0) & (pred_val==1)):
                fp += 1
                temp_fp += 1
            elif ((true_val==1) & (pred_val==0)):
                fn += 1
                temp_fn += 1
            elif ((true_val==0) & (pred_val==0)):
                tn += 1
                temp_tn += 1
        results.loc[y,'tp'] = temp_tp
        results.loc[y,'fp'] = temp_fp
        results.loc[y,'tn'] = temp_tn
        results.loc[y,'fn'] = temp_fn
        results.loc[y,'acc'] = ((temp_tp+temp_tn)/(temp_tp+temp_fp+temp_tn+temp_fn))
        if (temp_tp+temp_fp) > 0:
            results.loc[y,'prec'] = (temp_tp/(temp_tp+temp_fp))
        if (temp_tp+temp_fn) > 0:
            results.loc[y,'rec'] = (temp_tp/(temp_tp+temp_fn))  
    
    #Print prediction vs actual
    for x in pred_class.columns:
        print('#######################################')
        print('Image: ' + str(x))
        print('***************************************')
        print('Predicted Labels:')
        for y in pred_class.index:
            print(str(y) + ': ' + str(pred_class.loc[y,x]))                
        print('***************************************')
        print('True Labels:')
        for y in pred_class.index:
            print(str(y) + ': ' + str(true_class.loc[y,x]))                
        print('***************************************')
    print('True Positives: ' + str(tp))
    print('False Positives: ' + str(fp))
    print('True Negatives: ' + str(tn))
    print('False Negatives: ' + str(fn))
    #Accuracy, precision, recall
    print('Accuracy: ' + str((tp+tn)/(tp+fp+tn+fn)))
    print('Precision: ' + str(tp/(tp+fp)))
    print('Recall: ' + str(tp/(tp+fn)))
    print('#######################################')
                
    results.loc['Total','tp'] = tp
    results.loc['Total','fp'] = fp
    results.loc['Total','tn'] = tn
    results.loc['Total','fn'] = fn
    results.loc['Total','acc'] = ((tp+tn)/(tp+fp+tn+fn))
    results.loc['Total','prec'] = (tp/(tp+fp))
    results.loc['Total','rec'] = (tp/(tp+fn))
    results_file = 'D:/GD/MLDM/Computer_Vision_Project/cnn5/results/cvp_bow_sigmoid_10_10_results2.csv'
    results.to_csv(results_file)  
    
    return pred_class, true_class
    
   
        

#Run Prediction
target_size = (299, 299) #fixed size for InceptionV3 architecture
print('Starting prediction model')
pred_class, true_class = make_prediction(test_dir,test_ann_dir,target_size,specific_img)
#check = make_prediction(test_dir,test_ann_dir,target_size,specific_img)