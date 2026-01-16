# -*- coding: utf-8 -*-
import collections
import string
import math
import pandas as pd
import seaborn as sns
import statsmodels.api as sm
from nltk.corpus import stopwords 
from nltk.stem.wordnet import WordNetLemmatizer
from textblob import TextBlob
import matplotlib.pyplot as plt
from wordcloud import WordCloud

def clean(doc):
    exclude = set(string.punctuation) 
    stop = set(stopwords.words('english'))
    lemma = WordNetLemmatizer()
    stop_free = " ".join([i for i in doc.lower().split() if i not in stop])
    punc_free = ''.join(ch for ch in stop_free if ch not in exclude)
    normalized = " ".join(lemma.lemmatize(word) for word in punc_free.split())
    return normalized

def get_keyword_freq(dataset):
    df_keyFreq = pd.DataFrame(columns=['Keyword','Frequency','name'])
    for name in dataset['name'].unique():
        df_small = dataset[dataset['name'] == name][['pos','neg']]
        df_values = df_small.values.tolist()
        lis = []
        
        for value in df_values:
            for val in value:
                if val != 0:
                    spl = clean(val).split()            
                    for s in spl:
                        lis.append(s)
                else:
                    continue        
        dic = collections.Counter()
        for value in lis:
            if value in dic.keys():
                dic[value] += 1
            else:
                dic[value] = 1   
        top_3 = list(dic.most_common()[:3])    
        df_freq = pd.DataFrame(top_3,columns=['Keyword','Frequency'])
        df_freq['name'] = name
        df_keyFreq = df_keyFreq.append(df_freq)
    return df_keyFreq

def senti_analysis(dataset):
    dic_senti = {}
    for name in dataset['name'].unique():
        print (name)
        df_pos = dataset[dataset['name'] == name][['pos']]
        pos_values = df_pos.values.tolist()
        sen1 = ''.join(str(value) for value in pos_values)
        blob_pos = TextBlob(sen1)
        df_neg = dataset[dataset['name'] == name][['neg']]
        neg_values = df_neg.values.tolist()
        sen2 = ''.join(str(value) for value in neg_values)
        blob_neg = TextBlob(sen2)      
        df_small = dataset[dataset['name'] == name][['pos','neg']]
        pos_neg_values = df_small.values.tolist()    
        sen3 = ''.join(str(value) for value in pos_neg_values)
        blob = TextBlob(sen3)
        lis_senti = [blob_pos.sentiment.polarity, blob_neg.sentiment.polarity, blob.sentiment.polarity]
        dic_senti[name]=lis_senti
    df_senti = pd.DataFrame.from_dict(dic_senti,orient='index')
    df_senti.columns = ['pos','neg','total']
    return df_senti

            
def data_cleaning(dataframe):
    raw_data = dataframe
    print(raw_data.columns)   # Shows 48 columns' names (name + overall score + 46 variables)
    print(raw_data.describe())
    print(raw_data.info())  # Shows missing values

    sns.distplot(raw_data['Overall score']) 
    df = raw_data.dropna() 
    df = df.reset_index(drop=True)
    for i in range(0,len(df)):
        df.loc[df['Zip code'] < 10000, 'Zip code'] = df['Zip code'].median()
        df.loc[df['Location'] > 10, 'Location'] = df['Location'].median()
        for ch in [' miles from center',' feet from center', ',','(', ')']:
            if ch in df.loc[df.index[i],'Dist']:
                df.loc[df.index[i],'Dist']= df.loc[df.index[i],'Dist'].replace(ch, '')
    df['Dist'] = (df['Dist'].str.strip('(' and ')')).astype(float)
                
                
    df = df.rename(index=str, columns={"Overall score": "Score", "Number of reviewers": 'numRev'})
    
    df['numRev'] = (df['numRev'].str.replace(',', '')).astype(float)  # convert number of reviewers to float
    
    col_drop = []
    for col in df.columns[11:df.shape[1]-1]:
        if (df[col].sum()/df.shape[0]) < 0.05 or (df[col].sum()/df.shape[0])> 0.95:
            col_drop.append(col)
            
    df = df.drop(col_drop, axis = 1)
    df = df.reset_index(drop=True)
    sns.distplot(df['Score']) 
    return df

def ols_regression(dataFrame, xList, y):
    X = dataFrame[xList].astype(float) # Independent variables
    Y = dataFrame[y].astype(float) # Dependent variable
    
    X = sm.add_constant(X) # adding a constant      
    model = sm.OLS(Y, X).fit()      
    print_model = model.summary()
    print(print_model)

def scatter_plot(dataFrame, xList, y):
    fig, axes = plt.subplots(2,5,sharey=False,figsize=(10,5)) # Set the positions of graphs
    fig.tight_layout()
    for i in range(5):
        dataFrame.plot.scatter(x=xList[i],y=y,ylim=(4,10),ax=axes[0,i])
    for i in range(4):
        dataFrame.plot.scatter(x=xList[i+5],y=y,ylim=(4,10),ax=axes[1,i])
        
def box_plot(dataFrame,variable,y):
    fig, ax = plt.subplots(figsize=(12,6))
    sns.boxplot(x=variable,y=y,data=dataFrame)  
    ax.set_ylim(4,10)
    plt.xticks(rotation=90)
    plt.show()

def heat_map(dataFrame):
    corrmat = dataFrame.corr()
    f, ax = plt.subplots(figsize=(12, 9))
    sns.heatmap(corrmat, vmax=.8, square=True, ax=ax)  
    plt.show()

def highest_heat_map(dataFrame, number):
    k = number
    corrmat = dataFrame.corr()
    top = corrmat.nlargest(k, 'Score').index # Find the index of top 10 highest correlated variables
    top_mat = corrmat.loc[top, top] # Location their postions in heat map by index
    fig,ax = plt.subplots(figsize=(8,6))
    sns.set(font_scale=1.25)
    sns.heatmap(top_mat, annot=True, annot_kws={'size':12}, square=True)
    plt.show()

def difference_mean(df, number):
    df_X = df.drop(['Score'],axis=1)    
    quantity = df_X.columns.drop(['Zip code','name'])  
    low = df[df['Score'] < number][quantity].mean() # Calculates the mean of variables with overall score < 8.2
    high = df[df['Score'] >= number][quantity].mean() # Calculates the mean of variables with overall score > 8.2
    diff = pd.DataFrame()
    diff['feature'] = quantity
    diff['difference'] = ((high-low)/low).values # Calculate the change between two means
    plt.figure(figsize=(10,4))
    sns.barplot(data=diff, x='feature', y='difference')
    plt.xticks(rotation=90)
    plt.show()

def wordcloud_maker(string):
    wordcloud = WordCloud(collocations=False,width=480, height=480, margin=0).generate(string)
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.margins(x=0, y=0)
    plt.show() 
    
def sentiment_analysis_city(dataFrame):
    g = sns.lmplot( x="senti_x", y="Score", data=dataFrame, fit_reg=False, hue='Label', scatter_kws={"s": 50}, size=10, legend_out = True)
    ax = plt.gca()
    g._legend.set_title("Hotel Cluster")
    new_labels = ['Low', 'Medium', 'High']
    for t, l in zip(g._legend.texts, new_labels): t.set_text(l)
    ax.set_title("User Satisfaction vs Rating of Hotel")
    ax.set(xlabel='User Satisfaction Score of Hotel', ylabel='Rating of Hotel')

def merge_with_senti(df, senti):
    senti.columns = ['name','pos','neg','senti']
    senti['senti_normal'] = 10*(senti["senti"]-senti["senti"].min())/(senti["senti"].max()-senti["senti"].min())
    senti = senti.drop(['pos','neg'],axis=1)
    df_all = pd.merge(df,senti,on=['name'])
    return df_all

def display_table(df):
    print('')
    df = df.rename(columns={"Cleanliness":"Clean","Comfort":"Comf","Value for money": "Money", "Facilities":"Facility","Free WiFi": "Wifi","Location":"Loc"}) 
    df.columns.values[11:-5] = [x.lower() for x in df.columns.values[11:-5]]
    df = df.reset_index(drop=True)
    if (df.shape[0] == 3):
        print ("I found these hotels for you:\n")
    else:
        print ("Below are the details for comparison.")
    print (df[['name','Clean','Comf','Facility','Staff', 'Money','Wifi','Loc']]) # 'Dist'
    print ("* Comf = Comfort, Money = Value for money, Loc = Location")
    print ('')

def print_fac(df):
    print ('')
    df = df.drop(['Free WiFi.1'],axis=1)
    num = df.shape[0]
    for i in range(num):
        h1 = []
        for col in df.columns[11:-5]:
            if df.iloc[i][col] == 1:
                h1.append(col)
        h1 = [x.lower() for x in h1]
        if len(h1) == 0:
            print (df.iloc[i]['name'] + " didn't mention any special facilities on Booking.com.\n")
            print ('')
            continue
        print (df.iloc[i]['name'] + " has important facilities including " + ", ".join(h1[0:len(h1)-1]) + " and " + h1[len(h1)-1] + ".")
        print ('')

def get_distance(hotelName, df):
    dic = {}
    label = df[df['name'] == hotelName].Label.values
    df2 = df[df['Label'].values == label]
    i = 1
    for name in df2['name']:
        dist = 0
        for col in df2.columns[2:-2]:
            dist += (df2.loc[df2['name'] == name][col].values - df2.loc[df2['name'] == hotelName][col].values)**2
        dic[name] = math.sqrt(dist)
        print ("I'm calculating {} hotel's similarity in total.".format(i))
        i += 1
    return dic
