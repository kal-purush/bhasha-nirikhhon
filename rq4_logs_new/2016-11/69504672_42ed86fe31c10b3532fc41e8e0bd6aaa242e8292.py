# -*- coding: utf-8 -*-
"""
Created on Mon Oct 17 22:30:23 2016

@author: hangmengyu
"""

import random   
<span style="font-family:microsoft yahei;">
def train_test_split(ylabel, random_state=1):  
    import random   
    index=random.sample(range(len(ylabel)),50*random_state)  
    list_train=[]  
    list_test=[]  
    i=0  
    for s in range(len(ylabel)):  
        if i in index:  
            list_test.append(i)  
        else:  
            list_train.append(i)  
        i+=1  
    return list_train,list_test  
 ###############对特征进行分割#############################  
feature_cols = ['TV', 'Radio','Newspaper']  
X1 = data[feature_cols]  
y1 = data.Sales  
list_train,list_test=train_test_split(y1)#random_state的默认值是1  
  
X1_train=X1.ix[list_train] #这里使用来DataFrame的ix（）函数，可以将指定list中的索引的记录全部放在一起  
X1_test=X1.ix[list_test]  
y1_train=y1.ix[list_train]  
y1_test=y1.ix[list_test]  
#######################开始进行模型的训练########################################  
linreg.fit(X1_train, y1_train)</span>  
<span style="font-family:microsoft yahei;">######################预测#############</span> 
<span style="font-family:microsoft yahei;">y1_pred = linreg.predict(X1_test)  
print model  
print linreg.intercept_  
print linreg.coef_  
#################评价测度##############</span>  
<span style="font-family:microsoft yahei;">sum_mean1=0  
for i in range(len(y1_pred)):  
    sum_mean1+=(y1_pred[i]-y1_test.values[i])**2  
sum_erro1=np.sqrt(sum_mean1/50)  
# calculate RMSE by hand  
print "RMSE by hand:",sum_erro1</span> 