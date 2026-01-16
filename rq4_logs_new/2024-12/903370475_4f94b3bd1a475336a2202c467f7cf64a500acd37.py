import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.metrics import r2_score, accuracy_score, classification_report, precision_recall_fscore_support
from scipy.stats import chi2_contingency
from statsmodels.stats.outliers_influence import variance_inflation_factor
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
import warnings
import os


df1 = pd.read_excel('/Users/propelld/Desktop/DS/Projects/Credit-Risk-Modelling/case_study1.xlsx')
df2 = pd.read_excel('/Users/propelld/Desktop/DS/Projects/Credit-Risk-Modelling/case_study2.xlsx')



for col in df1.columns:
    try:
        if df1[df1[col] == -99999].shape[0] != 0:
            print(f"{col} {df1[df1[col] == -99999].shape[0]}")
    except KeyError:
        print(f"Column '{col}' not found in df1.")
        
df1 = df1[(df1 != -99999).all(axis=1)]

for col in df2.columns:
    if df2[df2[col] == -99999].shape[0] != 0:
        print(f"{col} {df2[df2[col] == -99999].shape[0]}") 

cols_to_drop = []
for col in df2.columns:
    if df2[df2[col] == -99999].shape[0] >= 10000:
        cols_to_drop.append(col)

df2 = df2.drop(cols_to_drop,axis=1) #Dropping columns

for col in df2.columns: #Dropping rows
    df2 = df2.loc[df2[col]!=-99999]
    


df = pd.merge(df1,df2, how='inner',on='PROSPECTID')


#Categorical and Numerical
categorical_cols = []
for col in df.columns:
    if df[col].dtype == 'object':
        categorical_cols.append(col)
        

# To find out which categorical variables are important 

#Chi2 for categorical variable relationship with categorical variable

for col in [value for value in categorical_cols if value!='Approved_Flag']:
        chi2, p_val, _, _ = chi2_contingency(pd.crosstab(df[col],df['Approved_Flag']))
        print(col, '---',p_val)

# All categorical variables have p_val <0.05 -> all are important




#NOW MOVING ON TO NUMERICAL FEATURES

numerical_columns = []
for i in df.columns:
    if df[i].dtype != 'object' and i not in ['PROSPECTID','Approved_Flag']:
        numerical_columns.append(i)
        

#Before checking their dependency on dependent variable.
#First check if there is any multicollinearity in any of these variables? How to check

#VIF (Variation Inflation Factor)
vif_data = df[numerical_columns]
total_columns  = vif_data.shape[1]
columns_to_be_kept = []
column_index=0

for i in range(0,total_columns):
    vif_value = variance_inflation_factor(vif_data, column_index)
    print(vif_data.columns[column_index], '-->', vif_value)
    
    
    if vif_value <= 6:
        columns_to_be_kept.append(numerical_columns[i])
        column_index = column_index+1
    
    else:
        vif_data = vif_data.drop([ numerical_columns[i] ], axis=1)


#ANOVA TEST
#To check each numerical variable association with target variable

from scipy.stats import f_oneway

columns_to_be_kept_after_anova = []

for i in columns_to_be_kept:
    a = list(df[i])
    b = list(df['Approved_Flag'])
    
    group_P1 = [value for value, group in zip(a,b) if group=='P1']
    group_P2 = [value for value, group in zip(a,b) if group=='P2']
    group_P3 = [value for value, group in zip(a,b) if group=='P3']
    group_P4 = [value for value, group in zip(a,b) if group=='P4']
        
    f_statistic, p_value = f_oneway(group_P1,group_P2,group_P3,group_P4)
    
    if p_value <= 0.05:
        columns_to_be_kept_after_anova.append(i)
        

#Feature selection is done for categorical and numerical columns
print(len(columns_to_be_kept_after_anova))
print(categorical_cols)

#Listing all final features
features = columns_to_be_kept_after_anova + ['MARITALSTATUS', 'EDUCATION', 'GENDER', 'last_prod_enq2', 'first_prod_enq2']
df = df[features + ['Approved_Flag']]

#Only Education as ordinal
df['EDUCATION'] = df['EDUCATION'].apply(lambda x: 
    1 if x in ['SSC', 'OTHERS'] else 
    2 if x == '12TH' else 
    3 if x in ['GRADUATE', 'UNDER GRADUATE','PROFESSIONAL'] else 
    4 if x == 'POST-GRADUATE' else None)

    
df_encoded = pd.get_dummies(df, columns=[col for col in categorical_cols if col not in ['EDUCATION','Approved_Flag']])



#Machine Learning model fitting

#Why not scaling?
#First build first model without scaling/standardization and see how model is performing, whether scaling is even needed


#1. Random Forrest
y = df_encoded.Approved_Flag
x = df_encoded.drop(columns='Approved_Flag')    

x_train, x_test, y_train, y_test = train_test_split(x,y,test_size=0.2,random_state = 4)

rf_classifier = RandomForestClassifier(n_estimators=1,random_state=42)

rf_classifier.fit(x_train,y_train)

y_pred = rf_classifier.predict(x_test)

accuracy = accuracy_score(y_test, y_pred)
print(f"Accuracy : {accuracy}")
print()
precision, recall, f1_score, _ = precision_recall_fscore_support(y_test, y_pred)

for i, v in enumerate(['p1','p2','p3','p4']):
    print(f"Class {v}")
    print(f"Precision - {precision[i]}")
    print(f"Recall - {recall[i]}")
    print(f"F1_Score - {f1_score[i]}")
    print()
    


debug = list(zip(x_train.columns, rf_classifier.feature_importances_))
debug_sorted = sorted(debug, key=lambda x: x[1], reverse=True)  # Sort by importance

# Display the sorted feature importances
for feature, importance in debug_sorted:
    print(f"{feature}: {importance:.4f}")


#2. XGBOOST
import xgboost as xgb
from sklearn.preprocessing import LabelEncoder

xgb_classifier = xgb.XGBClassifier(objective='multi:softmax',num_class=4)


y = df_encoded['Approved_Flag'] 
x = df_encoded.drop(['Approved_Flag'],axis=1)


label_encoder = LabelEncoder()
y_encoded = label_encoder.fit_transform(y)

x_train, x_test, y_train, y_test = train_test_split(x,y_encoded,test_size=0.2,random_state = 42) 


xgb_classifier.fit(x_train,y_train)
y_pred = xgb_classifier.predict(x_test)

accuracy = accuracy_score(y_test,y_pred)
print()
print(f'Accuracy: {accuracy:.2f}')
print()

precision, recall, f1_score, _ = precision_recall_fscore_support(y_test,y_pred)

for i , v  in enumerate(['p1','p2','p3','p4']):
    print(f"Class {v}")
    print(f"Precision : {precision[i]}")
    print(f"Recall : {recall[i]}")
    print(f"F1 Score : {f1_score[i]}")
    print()



#3. Decision Tree
from sklearn.tree import DecisionTreeClassifier

dt_model = DecisionTreeClassifier()
dt_model.fit(x_train,y_train)

y_pred = dt_model.predict(x_test)

accuracy = accuracy_score(y_test,y_pred)
print()
print(f'Accuracy: {accuracy:.2f}')
print()

precision, recall, f1_score, _ = precision_recall_fscore_support(y_test,y_pred)

for i , v  in enumerate(['p1','p2','p3','p4']):
    print(f"Class {v}")
    print(f"Precision : {precision[i]}")
    print(f"Recall : {recall[i]}")
    print(f"F1 Score : {f1_score[i]}")
    print()



#Try Finetuning on XGBoost
#1. Feature Engineering
#2. Hyperparameter tuning


# Hyperparameter tuning
params_grid = {
    'colsample_bytree' : [0.7, 0.9, 1],
    'learning_rate' : [0.01,0.05],
    'max_depth' : [5,10,15,20],
    'alpha' : [0.1,1,10],
    'n_estimators' : [50,100,200]
    }

from sklearn.model_selection import GridSearchCV

model = xgb.XGBClassifier(objective='multi:softmax', num_class=4)
grid = GridSearchCV(estimator = model, 
                    param_grid = params_grid,cv=5,scoring = 'accuracy',verbose = 3)

grid.fit(x_train,y_train)


best_model = grid.best_estimator_
y_pred = best_model.predict(x_test)

accuracy = accuracy_score(best_model.predict(x_train),y_train)
print()
print(f'Train Accuracy: {accuracy:.2f}')
print()

accuracy = accuracy_score(y_test,y_pred)
print()
print(f'Test Accuracy: {accuracy:.2f}')
print()

precision, recall, f1_score, _ = precision_recall_fscore_support(y_test,y_pred)

for i , v  in enumerate(['p1','p2','p3','p4']):
    print(f"Class {v}")
    print(f"Precision : {precision[i]}")
    print(f"Recall : {recall[i]}")
    print(f"F1 Score : {f1_score[i]}")
    print()
    
    

df_unseen = pd.read_excel('/Users/propelld/Desktop/DS/Projects/Credit-Risk-Modelling/Unseen_Dataset.xlsx')


features = columns_to_be_kept_after_anova + ['MARITALSTATUS', 'EDUCATION', 'GENDER', 'last_prod_enq2', 'first_prod_enq2']
df_unseen = df_unseen[features]

#Only Education as ordinal
df_unseen['EDUCATION'] = df_unseen['EDUCATION'].apply(lambda x: 
    1 if x in ['SSC', 'OTHERS'] else 
    2 if x == '12TH' else 
    3 if x in ['GRADUATE', 'UNDER GRADUATE','PROFESSIONAL'] else 
    4 if x == 'POST-GRADUATE' else None)

    
df_unseen_encoded = pd.get_dummies(df_unseen, columns=[col for col in categorical_cols if col not in ['EDUCATION','Approved_Flag']])

df_unseen_predicted = best_model.predict(df_unseen_encoded)


      