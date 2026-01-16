import joblib

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import squarify
from datetime import datetime
from importlib import reload
from sklearn.manifold import TSNE
from sklearn.cluster import KMeans
from scipy.cluster import hierarchy
from sklearn.cluster import AgglomerativeClustering
import plotly.express as px
import altair as alt
import pickle
import plotly.figure_factory as ff
import base64
from pathlib import Path
from numpy import dot
from numpy.linalg import norm
from wordcloud import WordCloud
import time

###import gensim
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel, cosine_similarity
from underthesea import word_tokenize, pos_tag, sent_tokenize
from gensim import corpora, models, similarities
import jieba
import re
import warnings

###import ALS
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import isnan, when, count, col, udf
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from time import time
from operator import itemgetter
from pyspark.ml.recommendation import ALSModel



from st_aggrid import AgGrid,GridUpdateMode,DataReturnMode, JsCode,ColumnsAutoSizeMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
warnings.filterwarnings("ignore")

import streamlit as st

@st.cache_data  
def load_data(url):
    df = pd.read_csv(url,delimiter=',',index_col=0)
    return df

def show_image_from_url(image_url):
    return(f"")

def show_result_ALS(results,products_raw):
    list_result=map(int, results["Product ID"].to_list())
    df_=products_raw.loc[products_raw["item_id"].isin(list_result)]
    st.write(df_)
    col_1,col_2=st.columns(2)
    check_col=True
    for index, row in df_.iterrows():
        if check_col==True:
            col_1.write(row["name"])
            col_1.image(row["image"])
            col_1.write("Rating:"+str(results[results["Product ID"]==str(row["item_id"])]["Rating"].values[0]))
            check_col=False
        elif check_col==False:
            col_2.write(row["name"])
            col_2.image(row["image"])
            col_2.write("Rating:"+str(results[results["Product ID"]==str(row["item_id"])]["Rating"].values[0]))
            check_col=True

def show_result_gensim(results,products_raw):
    list_result=results["item_id"].to_list()
    df_=products_raw.loc[products_raw["item_id"].isin(list_result)]
    col_1,col_2=st.columns(2)
    check_col=True
    for index, row in df_.iterrows():
        if check_col==True:
            col_1.write(row["name"])
            col_1.image(row["image"])
            col_1.write("Similarity score:"+str(results[results["item_id"]==row["item_id"]]["score"].values[0]))
            check_col=False
        elif check_col==False:
            col_2.write(row["name"])
            col_2.image(row["image"])
            col_2.write("Similarity score: "+str(results[results["item_id"]==row["item_id"]]["score"].values[0]))
            check_col=True


def recommend_item(item_id, num):
    recs = result[item_id][:num]
    col_1,col_2=st.columns(2)
    check_col=True
    for rec in recs:
        image=products_raw.loc[products_raw['item_id']==rec[1]]['image'].to_list()[0]
        name=products_raw.loc[products_raw['item_id']==rec[1]]['name'].to_list()[0].split('-')[0]
        if check_col==True:
            col_1.write(name)
            col_1.image(image)
            col_1.write("Similarity score: "+str(rec[0]))
            check_col=False
        elif check_col==False:
            col_2.write(name)
            col_2.image(image)
            col_2.write("Similarity score: "+str(rec[0]))
            check_col=True
        


def filter_customerID(df_merged,customer_id):
    index=df_merged[df_merged["customer_id"]==customer_id].index.values.astype(int)[0]
    sp=df_merged[df_merged["customer_id"]==customer_id].replace("\(", "", regex=True).replace("\)", "", regex=True)
    list_product_ID=[]
    list_product_Rating=[]
    list_customerID=[]
    for i in range(1,6):
        index_="sp"+str(i)
        list_product_ID.append(sp[index_][index].split(",")[0])
        list_product_Rating.append(sp[index_][index].split(",")[1])
        list_customerID.append(customer_id)
    df=pd.DataFrame({"CustomerID":list_customerID,"Product ID":list_product_ID,"Rating":list_product_Rating})
    return df
    # id_sp1, score_sp1=sp["sp1"][index].split(",")
    # id_sp2, score_sp2=sp["sp2"][index].split(",")
    # id_sp3, score_sp3=sp["sp3"][index].split(",")
    # id_sp4, score_sp4=sp["sp4"][index].split(",")
    # id_sp5, score_sp5=sp["sp5"][index].split(",")

# def convert_date(df):
#     string_to_date = lambda x : datetime.strptime(str(x), "%Y%m%d").date()
#     data['date'] = data['date'].apply(string_to_date)
#     data['date'] = data['date'].astype('datetime64[ns]')


def cluster_function_k5(prediction):
    if prediction ==0:
        return "Almost lost"
    elif prediction ==1:
        return "Lost"
    elif prediction ==2:
        return "Star"
    elif prediction ==3:
        return "Regular"
    return "New"

def cluster_function_k6(prediction):
    if prediction ==5:
        return "Star"
    elif prediction ==2:
        return "Big Spender"
    elif prediction ==3:
        return "Cooling Down"
    elif prediction ==1:
        return "Loyal"
    elif prediction ==0:
        return "Regular"
    return "Lost Cheap"



def img_to_bytes(img_path):
    img_bytes = Path(img_path).read_bytes()
    encoded = base64.b64encode(img_bytes).decode()
    return encoded
def img_to_html(img_path):
    img_html = "<img src='data:image/png;base64,{}' class='img-fluid' width='600'>".format(
    img_to_bytes(img_path)
    )
    return img_html

STOP_WORD_FILE='Data/vietnamese-stopwords.txt'

#LOAD wrong words
file = open('Data/wrong-word.txt', 'r', encoding="utf8")
wrong_lst = file.read().split('\n')
file.close()

with open(STOP_WORD_FILE, 'r', encoding='utf-8') as file:
    stop_words = file.read()

stop_words = stop_words.split('\n')


condition = st.sidebar.selectbox(
    "Select the visualization",
    ("Introduction", "EDA", "Preprocessing Data","Content-based Filtering","Collaborative Filtering","New Recommendation")
)

def recomender(view_product,dictionary,tfidf,index):
        view_product=view_product.lower().split()
        kw_vector=dictionary.doc2bow(view_product)
        print("View product's vector:")
        print(kw_vector)
        
        #similarity calculation
        sim=index[tfidf[kw_vector]]
        
        list_id=[]
        list_score=[]
        for i in range(len(sim)):
            list_id.append(i)
            list_score.append(sim[i])
            
        df_result=pd.DataFrame({"id":list_id, "score":list_score})
        
        #five highest scores
        five_highest_score=df_result.sort_values(by="score",ascending=False).head(10)
        print("Five highest scores: ")
        print(five_highest_score)
        print("Ids to list:")
        idToList=list(five_highest_score["id"])
        print(idToList)
        
        products_find=products[products.index.isin(idToList)]
        results=products_find[["index","item_id","product_content"]]
        results=pd.concat([results,five_highest_score],axis=1).sort_values(by="score",ascending=False)

        return results


# ------------- Introduction ------------------------

if condition == 'Introduction':

    #st.image(os.path.join(os.path.abspath(''), 'data', 'dataset-cover.jpg'))
    st.subheader('About')
    
    ## FALTA O CHECK ON GITHUB
    st.write("""
    For e-commerce and social networking platforms, recommender systems are crucial 
    because they can direct customers to a more involved or related product, 
    encouraging them to make additional purchases.

    There are two popular recommender systems:
    Content-Based Filtering (CBF) and Collaborative Filtering (CF)

    In this project, we will create a recommendation for Tiki dataset using both recommender systems. 
    The expected outcome is to generate a recommendation for the five most similar products for each userr
    """)

   
    st.image("Data/image.jpeg")

    st.subheader("Overview of Content-Based Filtering")
    st.write("""
    Content-based filtering uses item features to recommend other items similar to what the user likes, based on their previous actions or explicit feedback
    Because of its quick processing and capacity to optimize usage, we selected the GenSim (Generate Similar) algorithm, 
    a well-known open source natural language processing library used for unsupervised subject modeling.\n
    Besides, you can consider another Similarity Metrics such as
    Cosine Similarity, Pearson Similarity,KNN item-based collaborative filtering..
             """)
    
    st.subheader("Overview of Collaborative Filtering")
    st.write("""
    Collaborative filtering is a technique that can filter out items that a user might like on the basis of reactions by similar users.

    It works by searching a large group of people for users who have similar interests to a specific user. 
    It considers the things they like and combines them to generate a ranked list of recommendations.

    Apache Spark ML implements ALS for collaborative filtering, a very popular algorithm for making recommendations.
    

    Most important hyper-params in Alternating Least Square (ALS):
    * maxIter: the maximum number of iterations to run (defaults to 10)
    * rank: the number of latent factors in the model (defaults to 10)
    * regParam: the regularization parameter in ALS (defaults to 1.0)

    """)
# ------------- EDA ------------------------

elif condition == 'EDA':
    products= pd.read_csv("Data/ProductRaw.csv",delimiter=',')
    reviews=pd.read_csv("Data/ReviewRaw.csv",delimiter=',')
    products=products.reset_index()


    original_title_data = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>EDA with Products Data</b></p>'
    st.markdown(original_title_data,unsafe_allow_html=True)

    # products["image_"] = products["image"].apply( lambda x: show_image_from_url(x["image"]))
    # products=products.to_html()
    st.dataframe(products)

    des_product=products.describe()
    original_title_describe = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>Descriptive Statistics</b></p>'
    st.markdown(original_title_describe,unsafe_allow_html=True)
    st.write(des_product)
    

    original_title_duplicate = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>Check Null and Duplicates</b></p>'
    st.markdown(original_title_duplicate,unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    st.markdown("""
        <style>
        div[data-testid="metric-container"] {
        background-color: rgba(28, 131, 225, 0.1);
        border: 1px solid rgba(28, 131, 225, 0.1);
        padding: 5% 5% 5% 10%;
        border-radius: 5px;
        color: rgb(30, 103, 119);
        overflow-wrap: break-word;
        }

        /* breakline for metric text         */
        div[data-testid="metric-container"] > label[data-testid="stMetricLabel"] > div {
        overflow-wrap: break-word;
        white-space: break-spaces;
        color: blue;
        }

        [data-testid="stMetricValue"] {
            font-size: 30px;
        }
        </style>
        """
    , unsafe_allow_html=True)

    data_check_null=products.isnull().sum().to_frame('counts')
   
    
    col1.metric("Total number of null columns",str(data_check_null.loc[data_check_null["counts"]>0].columns.size))
    col2.metric("Total duplicate lines",str(products.duplicated().sum()))


    original_title_duplicate = '<p style="font-family:Garamond, serif; color:Black; font-size: 20px;"><b> &#9830; Show null values</b></p>'
    st.markdown(original_title_duplicate,unsafe_allow_html=True)
    st.dataframe(data_check_null)

    original_title_visual_products = '<p style="font-family:Garamond, serif; color:black; font-size: 20px;"><b> &#9830; There are no duplicates in data, but there are three null numbers in the description.We dont remove these null values because the content we create comprises of the product name, description. We still have the name value for content </b></p>'
    st.markdown(original_title_visual_products,unsafe_allow_html=True)


    original_title_visual_products = '<p style="font-family:Garamond, serif; color:blue; font-size: 30px;"><b>Visualization Products Data</b></p>'
    st.markdown(original_title_visual_products,unsafe_allow_html=True)


    most_rating=products.groupby('item_id').size().reset_index(name='counts').sort_values("counts",ascending=False)

    rating_frequency = []
    entries = products.shape[0]
    for i in range(most_rating.counts.nunique()):
        rating_frequency.append(float(most_rating[most_rating['counts'] == i+1]["counts"].count())/entries*100)
    numbers = [i for i in range(1, most_rating.counts.nunique()+1)]

    df_rating_frequency = pd.DataFrame({'number': numbers, 'rating_frequency': rating_frequency})
    #st.dataframe(most_rating)

    original_title_visual_products = '<p style="font-family:Garamond, serif; color:black; font-size: 20px;"><b> &#9830; 98% of products have at least one rating.</b></p>'
    st.markdown(original_title_visual_products,unsafe_allow_html=True)

    fig__rating_frequency=plt.figure(figsize=(11, 7))
    ax=sns.barplot(x="number", y="rating_frequency", data=df_rating_frequency, color="lightskyblue")
    for i in ax.containers:
        ax.bar_label(i,fmt='{:,.2f}%')
    plt.title("Percentage of the Product Reviews Frequency",fontsize=20,color="blue",y=1.05)
    st.pyplot(fig__rating_frequency)

    ###top_10_price
    original_title_visual_products = '<p style="font-family:Garamond, serif; color:black; font-size: 20px;"><b> &#9830; Overal, the rating range is 4 to 5 stars. 50% of customers (2392/4401) gave it four ratings or higher.</b></p>'
    st.markdown(original_title_visual_products,unsafe_allow_html=True)
    fig__rating_top_10_price=plt.figure(figsize=(11, 7))
    plt.hist(products["rating"],alpha=0.5, histtype='stepfilled', color='steelblue', edgecolor='none',bins=5)
    plt.ylabel('Frequency')
    plt.xlabel('Rating')
    plt.title("Distribution About The Frequency of Rating",fontsize=20,color="blue",y=1.05)
    st.pyplot(fig__rating_top_10_price)

    ###most_brand
    original_title_visual_products = '<p style="font-family:Garamond, serif; color:black; font-size: 20px;"><b> &#9830; Among the 521 brands, OEM is the most famous.</b></p>'
    st.markdown(original_title_visual_products,unsafe_allow_html=True)
    most_brand=products.groupby('brand').size().reset_index(name='counts').sort_values("counts",ascending=False)
    fig__rating_most_brand=plt.figure(figsize=(11, 7))
    sns.barplot(x="brand", y="counts", data=most_brand.sort_values("counts",ascending=False).head(10), palette="Blues_d")
    plt.title("Top 10 most Interested Brands",fontsize=20,color="blue",y=1.05)
    st.pyplot(fig__rating_most_brand)

    ###most_price
    original_title_visual_products = '<p style="font-family:Garamond, serif; color:black; font-size: 20px;"><b> &#9830; Products under 400,000 are given much more ratings than those over 400,000</b></p>'
    st.markdown(original_title_visual_products,unsafe_allow_html=True)
    most_price=products.groupby('price').size().reset_index(name='counts').sort_values("counts",ascending=False)
    fig__rating_most_price=plt.figure(figsize=(11, 7))
    sns.barplot(x="price", y="counts", data=most_price.loc[(most_price["price"]<400000)].sort_values("counts",ascending=False).head(10), palette="Blues_d")
    plt.title("Top 10 most Interested Price",fontsize=20,color="blue",y=1.05)
    st.pyplot(fig__rating_most_price)

    ###
    original_title_visual_products = '<p style="font-family:Garamond, serif; color:black; font-size: 20px;"><b> &#9830; Here are the most popular keywords</b></p>'
    st.markdown(original_title_visual_products,unsafe_allow_html=True)


    st.markdown("<p style='text-align: center; color: grey;'>"+img_to_html('Data/word_cloud_interest_products.png')+"</p>", unsafe_allow_html=True)

    original_title_data = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>EDA with RevieDataws Data</b></p>'
    st.markdown(original_title_data,unsafe_allow_html=True)
    st.dataframe(reviews)

    original_title_visual_products = '<p style="font-family:Garamond, serif; color:black; font-size: 20px;"><b> &#9830; Top 10 customers with numerous rating</b></p>'
    st.markdown(original_title_visual_products,unsafe_allow_html=True)
    most_customer=reviews.groupby(["customer_id"]).size().reset_index(name='counts').sort_values("counts",ascending=False).head(10)
    fig__rating_most_customer=plt.figure(figsize=(11, 7))
    sns.barplot(x="customer_id", y="counts", data=most_customer.sort_values("counts",ascending=False).head(10), palette="Blues_d")
    plt.title("Top 10 Customers With The Most Reviews",fontsize=20,color="blue",y=1.05)
    st.pyplot(fig__rating_most_customer)


    original_title_visual_products = '<p style="font-family:Garamond, serif; color:black; font-size: 20px;"><b> &#9830; As we talk above, 50% of the evaluations are rated four stars or higher. Many positive keywords can also be found in customer evaluation keywords</b></p>'
    st.markdown(original_title_visual_products,unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: grey;'>"+img_to_html('Data/word_cloud_most_reviews.png')+"</p>", unsafe_allow_html=True)
    
    # products_name_list = [i for i in products.name]
    # text = " ".join(name for name in products_name_list)
    # wordcloud = WordCloud(background_color="black").generate(text)
    # fig__rating_word_cloud_1=plt.figure(figsize=(11, 7))
    # plt.imshow(wordcloud, interpolation='bilinear')
    # plt.axis("off")
    # st.pyplot(fig__rating_word_cloud_1)

elif condition == "Preprocessing Data":
    original_title_null = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>Raw Dataset</b></p>'
    st.markdown(original_title_null,unsafe_allow_html=True)

    products= pd.read_csv("Data/ProductRaw.csv",delimiter=',')
    reviews=pd.read_csv("Data/ReviewRaw.csv",delimiter=',')
    products=products.reset_index()
    st.dataframe(products)

    original_title_null = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>Dataset with selected columns to prepare to run the model</b></p>'
    st.markdown(original_title_null,unsafe_allow_html=True)

    products["product_content"]=products["name"]+" "+products["description"]
    stringcols = products.select_dtypes(include='object').columns
    products[stringcols] = products[stringcols].astype("str")
    products=products[["index","item_id","rating","product_content"]]
    st.dataframe(products)

    original_title_null = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>Data Processing</b></p>'
    st.markdown(original_title_null,unsafe_allow_html=True)

    original_title_visual_products = '<p style="font-family:Garamond, serif; color:black; font-size: 20px;"><b> &#9830; Perform data processing steps:</b></p>'
    st.markdown(original_title_visual_products,unsafe_allow_html=True)

    text_="""
        - Standardize Vietnamese unicode
        - Remove punctuation & Numbers
        - Removed stopwords, some meaningless words and wrong words
        - Remove excess blank space
        - Post tag and Word Tokenizer"""
            
    st.markdown(text_,unsafe_allow_html=True)

    original_title_visual_products = '<p style="font-family:Garamond, serif; color:black; font-size: 20px;"><b> &#9830; Using text Processing Library to Clean Text</b></p>'
    st.markdown(original_title_visual_products,unsafe_allow_html=True)

    code_lib_clean_text_1 ='''products["product_content"]=products["name"]+" "+products["description"]
stringcols = products.select_dtypes(include='object').columns
products[stringcols] = products[stringcols].astype("str")
products=products[["index","item_id","rating","product_content"]]
products["product_content_wt"]=products["product_content"].apply(lambda x: word_tokenize(x, format="text"))
product_postag=products["product_content"].apply(lambda x: pos_tag(x))
    '''
    st.code(code_lib_clean_text_1, language ='python')

    code_lib_clean_text_2 ='''def loaddicchar():
    uniChars = "àáảãạâầấẩẫậăằắẳẵặèéẻẽẹêềếểễệđìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵÀÁẢÃẠÂẦẤẨẪẬĂẰẮẲẴẶÈÉẺẼẸÊỀẾỂỄỆĐÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴÂĂĐÔƠƯ"
    unsignChars = "aaaaaaaaaaaaaaaaaeeeeeeeeeeediiiiiooooooooooooooooouuuuuuuuuuuyyyyyAAAAAAAAAAAAAAAAAEEEEEEEEEEEDIIIOOOOOOOOOOOOOOOOOOOUUUUUUUUUUUYYYYYAADOOU"

    dic = {}
    char1252 = 'à|á|ả|ã|ạ|ầ|ấ|ẩ|ẫ|ậ|ằ|ắ|ẳ|ẵ|ặ|è|é|ẻ|ẽ|ẹ|ề|ế|ể|ễ|ệ|ì|í|ỉ|ĩ|ị|ò|ó|ỏ|õ|ọ|ồ|ố|ổ|ỗ|ộ|ờ|ớ|ở|ỡ|ợ|ù|ú|ủ|ũ|ụ|ừ|ứ|ử|ữ|ự|ỳ|ý|ỷ|ỹ|ỵ|À|Á|Ả|Ã|Ạ|Ầ|Ấ|Ẩ|Ẫ|Ậ|Ằ|Ắ|Ẳ|Ẵ|Ặ|È|É|Ẻ|Ẽ|Ẹ|Ề|Ế|Ể|Ễ|Ệ|Ì|Í|Ỉ|Ĩ|Ị|Ò|Ó|Ỏ|Õ|Ọ|Ồ|Ố|Ổ|Ỗ|Ộ|Ờ|Ớ|Ở|Ỡ|Ợ|Ù|Ú|Ủ|Ũ|Ụ|Ừ|Ứ|Ử|Ữ|Ự|Ỳ|Ý|Ỷ|Ỹ|Ỵ'.split(
        '|')
    charutf8 = "à|á|ả|ã|ạ|ầ|ấ|ẩ|ẫ|ậ|ằ|ắ|ẳ|ẵ|ặ|è|é|ẻ|ẽ|ẹ|ề|ế|ể|ễ|ệ|ì|í|ỉ|ĩ|ị|ò|ó|ỏ|õ|ọ|ồ|ố|ổ|ỗ|ộ|ờ|ớ|ở|ỡ|ợ|ù|ú|ủ|ũ|ụ|ừ|ứ|ử|ữ|ự|ỳ|ý|ỷ|ỹ|ỵ|À|Á|Ả|Ã|Ạ|Ầ|Ấ|Ẩ|Ẫ|Ậ|Ằ|Ắ|Ẳ|Ẵ|Ặ|È|É|Ẻ|Ẽ|Ẹ|Ề|Ế|Ể|Ễ|Ệ|Ì|Í|Ỉ|Ĩ|Ị|Ò|Ó|Ỏ|Õ|Ọ|Ồ|Ố|Ổ|Ỗ|Ộ|Ờ|Ớ|Ở|Ỡ|Ợ|Ù|Ú|Ủ|Ũ|Ụ|Ừ|Ứ|Ử|Ữ|Ự|Ỳ|Ý|Ỷ|Ỹ|Ỵ".split(
        '|')
    for i in range(len(char1252)):
        dic[char1252[i]] = charutf8[i]
    return dic
 
# Đưa toàn bộ dữ liệu qua hàm này để chuẩn hóa lại
def convert_unicode(txt):
    dicchar = loaddicchar()
    return regex.sub(
        r'à|á|ả|ã|ạ|ầ|ấ|ẩ|ẫ|ậ|ằ|ắ|ẳ|ẵ|ặ|è|é|ẻ|ẽ|ẹ|ề|ế|ể|ễ|ệ|ì|í|ỉ|ĩ|ị|ò|ó|ỏ|õ|ọ|ồ|ố|ổ|ỗ|ộ|ờ|ớ|ở|ỡ|ợ|ù|ú|ủ|ũ|ụ|ừ|ứ|ử|ữ|ự|ỳ|ý|ỷ|ỹ|ỵ|À|Á|Ả|Ã|Ạ|Ầ|Ấ|Ẩ|Ẫ|Ậ|Ằ|Ắ|Ẳ|Ẵ|Ặ|È|É|Ẻ|Ẽ|Ẹ|Ề|Ế|Ể|Ễ|Ệ|Ì|Í|Ỉ|Ĩ|Ị|Ò|Ó|Ỏ|Õ|Ọ|Ồ|Ố|Ổ|Ỗ|Ộ|Ờ|Ớ|Ở|Ỡ|Ợ|Ù|Ú|Ủ|Ũ|Ụ|Ừ|Ứ|Ử|Ữ|Ự|Ỳ|Ý|Ỷ|Ỹ|Ỵ',
        lambda x: dicchar[x.group()], txt)

def process_text(text, wrong_lst):
    document = text.lower()
    document = document.replace("’",'')
    document = regex.sub(r'\.+', ".", document)
    new_sentence =''
    for sentence in sent_tokenize(document):
        # if not(sentence.isascii()):
        # ###### CONVERT EMOJICON
        # sentence = ''.join(emoji_dict[word]+' ' if word in emoji_dict else word for word in list(sentence))
        # ###### CONVERT TEENCODE
        # sentence = ' '.join(teen_dict[word] if word in teen_dict else word for word in sentence.split())
        ###### DEL Punctuation & Numbers
        pattern = r'(?i)\b[a-záàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệóòỏõọôốồổỗộơớờởỡợíìỉĩịúùủũụưứừửữựýỳỷỹỵđ]+\b'
        sentence = ' '.join(regex.findall(pattern,sentence))
        ###### DEL wrong words   
        sentence = ' '.join('' if word in wrong_lst else word for word in sentence.split())
        new_sentence = new_sentence+ sentence + '. '                    
    document = new_sentence  
    #print(document)
    ###### DEL excess blank space
    document = regex.sub(r'\s+', ' ', document).strip()
    return document
    '''
    st.code(code_lib_clean_text_2, language ='python')


    code_lib_clean_text_1 ='''pre_data_lst=[]
for row in range(len(products)):
    document = products.iloc[row]["product_content_wt"]
    document=  process_text(document,wrong_lst)
    document = convert_unicode(document)
    pre_data_lst.append(document)

products['product_content_wt'] = pre_data_lst
    '''
    st.code(code_lib_clean_text_1, language ='python')


    original_title_visual_products = '<p style="font-family:Garamond, serif; color:black; font-size: 20px;"><b> &#9830; Data after processing</b></p>'
    st.markdown(original_title_visual_products,unsafe_allow_html=True)



    pre_products= pd.read_csv("Data/pre_products.csv",delimiter=',',index_col=0)
    st.dataframe(pre_products)

elif condition=="Content-based Filtering":

    list_Model_=["Gensim","Cosine Similarity"]
    select_model_content_base = st.sidebar.selectbox(
        'Select the Model with ',
        [i for i in list_Model_]  
    )

    if select_model_content_base == "Gensim":
        original_title_data = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>Raw Dataset</b></p>'
        st.markdown(original_title_data,unsafe_allow_html=True)
        products_raw= pd.read_csv("Data/ProductRaw.csv",delimiter=',',index_col=0)
        products_raw=products_raw.reset_index()
        st.dataframe(products_raw)
        original_title_data = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>After preprocessing we have the product content like this</b></p>'
        st.markdown(original_title_data,unsafe_allow_html=True)
        products= pd.read_csv("Data/pre_products.csv",delimiter=',',index_col=0)
        st.dataframe(products)

        # Tokenize(split) the sentences into words
        # products_gem = [[text for text in x.split()] for x in products.product_content_wt]

        # # remove some special elements in texts
        # products_gem_re = [[re.sub('[0-9]+','', e) for e in text] for text in products_gem] # số
        # products_gem_re = [[t.lower() for t in text if not t in ['', ' ', ',', '.', '...', '-',':', ';', '?', '%', '(', ')', '+', '/', 'g', 'ml']] for text in  products_gem_re] # ký tự đặc biệt
        # products_gem_re = [[t for t in text if not t in stop_words] for text in products_gem_re] # stopword
        # suy nghĩ làm thêm

        original_title_data = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>Apply Gensim Algorithm</b></p>'
        st.markdown(original_title_data,unsafe_allow_html=True)

        with open('Data/products_gem_re.pkl', 'rb') as f:
            products_gem_re = pickle.load(f)

        code ='''dictionary=corpora.Dictionary(products_gem_re)
    feature_cnt = len(dictionary.token2id)
    tfidf = models.TfidfModel(corpus) 
    index = similarities.SparseMatrixSimilarity(tfidf[corpus], num_features=feature_cnt)
        '''
        st.code(code, language ='python')

        original_title_data = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>After that we write a recommend function to make future recommendation easier</b></p>'
        st.markdown(original_title_data,unsafe_allow_html=True)

        code_rec ='''def recomender(view_product,dictionary,tfidf,index):
        view_product=view_product.lower().split()
        kw_vector=dictionary.doc2bow(view_product)
        print("View product's vector:")
        print(kw_vector)
        
        #similarity calculation
        sim=index[tfidf[kw_vector]]
        
        list_id=[]
        list_score=[]
        for i in range(len(sim)):
            list_id.append(i)
            list_score.append(sim[i])
            
        df_result=pd.DataFrame({"id":list_id, "score":list_score})
        
        #five highest scores
        five_highest_score=df_result.sort_values(by="score",ascending=False).head()
        print("Five highest scores: ")
        print(five_highest_score)
        print("Ids to list:")
        idToList=list(five_highest_score["id"])
        print(idToList)
        
        products_find=products[products.index.isin(idToList)]
        results=products_find[["index","item_id","product_content"]]
        results=pd.concat([results,five_highest_score],axis=1).sort_values(by="score",ascending=False)
        return results
        '''
        st.code(code_rec, language ='python')

        dictionary=corpora.Dictionary(products_gem_re)

        feature_cnt = len(dictionary.token2id)
        # st.write('Number of feature in dictionary', feature_cnt)

        corpus = [dictionary.doc2bow(text) for text in products_gem_re]
        tfidf = models.TfidfModel(corpus)
        index = similarities.SparseMatrixSimilarity(tfidf[corpus], num_features=feature_cnt)

        original_title_data = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>For Example We choose Product ID = 299461</b></p>'
        st.markdown(original_title_data,unsafe_allow_html=True)

        product_ID= 299461
        product= products[products.item_id==product_ID].head(1)
        product[["index","item_id","product_content"]]
        name_description_pre=product["product_content"].to_string(index=False)


        results=recomender(name_description_pre,dictionary,tfidf,index)

        original_title_data = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>With Gensim, people who like Product ID = 299461 also like these</b></p>'
        st.markdown(original_title_data,unsafe_allow_html=True)
        results= results[results.item_id != product_ID]
        st.dataframe(results)

    elif select_model_content_base == "Cosine Similarity":
        original_title_data = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>Raw Dataset</b></p>'
        st.markdown(original_title_data,unsafe_allow_html=True)
        products_raw= pd.read_csv("Data/ProductRaw.csv",delimiter=',',index_col=0)
        products_raw=products_raw.reset_index()
        st.dataframe(products_raw)
        original_title_data = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>After preprocessing we have the product content like this</b></p>'
        st.markdown(original_title_data,unsafe_allow_html=True)
        products= pd.read_csv("Data/pre_products.csv",delimiter=',',index_col=0)
        st.dataframe(products)

        content_base_cosine= pd.read_csv("Data/content_base_cosine.csv",delimiter=',',index_col=0)

        # Tokenize(split) the sentences into words
        # products_gem = [[text for text in x.split()] for x in products.product_content_wt]

        # # remove some special elements in texts
        # products_gem_re = [[re.sub('[0-9]+','', e) for e in text] for text in products_gem] # số
        # products_gem_re = [[t.lower() for t in text if not t in ['', ' ', ',', '.', '...', '-',':', ';', '?', '%', '(', ')', '+', '/', 'g', 'ml']] for text in  products_gem_re] # ký tự đặc biệt
        # products_gem_re = [[t for t in text if not t in stop_words] for text in products_gem_re] # stopword
        # suy nghĩ làm thêm

        original_title_data = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>Apply Cosine Similarity</b></p>'
        st.markdown(original_title_data,unsafe_allow_html=True)


        code ='''X = [1,2]
    Y = [2,2]
    cos_sim = dot(X,Y) / (norm(X)*norm(Y))
    tf = TfidfVectorizer(analyzer='word', min_df=0, stop_words=stop_words)
    tfidf_matrix = tf.fit_transform(products.product_content_wt)
    cosine_similarities = cosine_similarity(tfidf_matrix, tfidf_matrix)
    df_show = pd.DataFrame(cosine_similarities)
        '''
        st.code(code, language ='python')

        original_title_data = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>After that we write a recommend function to make future recommendation easier</b></p>'
        st.markdown(original_title_data,unsafe_allow_html=True)

        code_rec =''' result = {}
    for idx, row in products.iterrows():    
        similar_indices = cosine_similarities[idx].argsort()[-11:-1]
        similar_items = [(cosine_similarities[idx][i], products['item_id'][i]) for i in similar_indices]
        result[row['item_id']] = similar_items[0:]
    info = []
    for p_id, v in result.items():
        for item in v:
            info.append({
            'product_id': p_id,
            'recommend_pd':item[1],
            'score': item[0],
            'name_product':products.loc[products["item_id"]==item[1]]["product_content"].to_list()[0].split('-')[0]})
    content_base_df = pd.DataFrame(info)
    def show_result(content_base_df,id):
        return content_base_df.loc[content_base_df["product_id"]==id]
        '''
        st.code(code_rec, language ='python')

        X = [1,2]
        Y = [2,2]
        cos_sim = dot(X,Y) / (norm(X)*norm(Y))
        tf = TfidfVectorizer(analyzer='word', min_df=0, stop_words=stop_words)
        tfidf_matrix = tf.fit_transform(products.product_content_wt)
        cosine_similarities = cosine_similarity(tfidf_matrix, tfidf_matrix)
        df_show = pd.DataFrame(cosine_similarities)


        result = {}
        for idx, row in products.iterrows():    
            similar_indices = cosine_similarities[idx].argsort()[-11:-1]
            similar_items = [(cosine_similarities[idx][i], products_raw['item_id'][i]) for i in similar_indices]
            result[row['item_id']] = similar_items[0:]

        original_title_data = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>For Example We choose Product ID = 48102821</b></p>'
        st.markdown(original_title_data,unsafe_allow_html=True)
        st.write(products_raw.loc[products_raw["item_id"]==48102821])
        original_title_data = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>With Cosine, people who like Product ID = 48102821 also like these</b></p>'
        st.markdown(original_title_data,unsafe_allow_html=True)
        product_ID_1= 48102821
        result_1=content_base_cosine.loc[content_base_cosine["product_id"]==product_ID_1]
        st.dataframe(result_1)


        # product_ID_2= 299461
        # result_1=content_base_cosine.loc[content_base_cosine["product_id"]==product_ID_2]
        # st.dataframe(result_1)

elif condition == "Collaborative Filtering":
    original_title_data = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>Dataset</b></p>'
    st.markdown(original_title_data,unsafe_allow_html=True)
    products= pd.read_csv("Data/ReviewRaw.csv",delimiter=',')
    st.dataframe(products)

    original_title_data = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>We only use \"customer_id, product_id, rating\" apply to ALS Model</b></p>'
    st.markdown(original_title_data,unsafe_allow_html=True)

    code ='''
        als = ALS(maxIter=10, 
                regParam=0.3,           
                rank = 20,
                userCol="customer_id_idx", 
                itemCol="product_id_idx", 
                ratingCol="rating", 
                coldStartStrategy="drop",
                nonnegative=True)
model = als.fit(training)
        '''
    st.code(code, language='python')

    original_title_visual_products = '<p style="font-family:Garamond, serif; color:black; font-size: 20px;"><b> &#9830; Root-mean-square error = 1.1435969076972106 </b></p>'
    st.markdown(original_title_visual_products,unsafe_allow_html=True)

    original_title_data = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>Beside we can use CrossValidator to find Best Paramaters</b></p>'
    st.markdown(original_title_data,unsafe_allow_html=True)

    code_cros ='''
        # Add hyperparameters and their respective values to param_grid
param_grid = ParamGridBuilder() \n\
            .addGrid(als.rank, [10, 20, 30]) \n\
            .addGrid(als.regParam, [.05, .1, .15, 0.2, 0.25, 0.3]) \n\
            .build()
crossvalidation = CrossValidator(estimator = als, estimatorParamMaps = param_grid, evaluator = evaluator, numFolds=5)
model = crossvalidation.fit(training)
best_model = model.bestModel
print("*Best Model*")
print("Rank:", best_model._java_obj.parent().getRank(),"-  MaxIter:", best_model._java_obj.parent().getMaxIter(), "-  RegParam:", best_model._java_obj.parent().getRegParam())
print("RMSE value after cross validation is: ", evaluator.evaluate(best_model.transform(test)))
        '''
    st.code(code_cros, language='python')


    original_title_visual_products = '<p style="font-family:Garamond, serif; color:black; font-size: 20px;"><b> &#9830; Best Model:<br> &emsp; &emsp; Rank: 10 -  MaxIter: 10 -  RegParam: 0.3<br> &emsp; &emsp; Root-mean-square error = 1.1273757596106933</b></p>'
    st.markdown(original_title_visual_products,unsafe_allow_html=True)

    original_title_data = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>Recommender Function to Extract Data</b></p>'
    st.markdown(original_title_data,unsafe_allow_html=True)

    code_recommender ='''
def filter_customerID(df_merged,customer_id):
    index=df_merged[df_merged["customer_id"]==customer_id].index.values.astype(int)[0]
    sp=df_merged[df_merged["customer_id"]==customer_id].replace("\(", "", regex=True).replace("\)", "", regex=True)
    list_product_ID=[]
    list_product_Rating=[]
    list_customerID=[]
    for i in range(1,6):
        index_="sp"+str(i)
        list_product_ID.append(sp[index_][index].split(",")[0])
        list_product_Rating.append(sp[index_][index].split(",")[1])
        list_customerID.append(customer_id)
    df=pd.DataFrame({"CustomerID":list_customerID,"Product ID":list_product_ID,"Rating":list_product_Rating})
    return df
        '''
    st.code(code_recommender, language='python')

    original_title_visual_products = '<p style="font-family:Garamond, serif; color:black; font-size: 20px;"><b> &#9830; Examine the recommendation outcome</b></p>'
    st.markdown(original_title_visual_products,unsafe_allow_html=True)

    product_als_recommendation= load_data("https://khaihoan.gmazi.com/Project_2_ALS.csv")
    original_title_visual_products = '<p style="font-family:Garamond, serif; color:black; font-size: 20px;"><b> &emsp; With Customer ID= 21013443</b></p>'
    st.markdown(original_title_visual_products,unsafe_allow_html=True)
    st.dataframe(filter_customerID(product_als_recommendation,21013443))

    original_title_visual_products = '<p style="font-family:Garamond, serif; color:black; font-size: 20px;"><b> &emsp; With Customer ID= 10</b></p>'
    st.markdown(original_title_visual_products,unsafe_allow_html=True)
    st.dataframe(filter_customerID(product_als_recommendation,10))








elif condition == 'New Recommendation':

    products= pd.read_csv("Data/pre_products.csv",delimiter=',',index_col=0)
    products_raw= pd.read_csv("Data/ProductRaw.csv",delimiter=',')
    reviews=pd.read_csv("Data/ReviewRaw.csv",delimiter=',')

    #st.dataframe(reviews)

    with open('Data/products_gem_re.pkl', 'rb') as f:
        products_gem_re = pickle.load(f)

   




    flag = False
    lines = None

    

    list_Model=["Content-based Filtering","Collaborative Filtering"]
    select_model = st.sidebar.selectbox(
        'Select the Model with ',
        [i for i in list_Model]  
    )

    html_str_select_model = f"""
                <style>
                p.a {{
                font: bold 40px Garamond, serif;
                color:blue
                }}
                </style>
                <p class="a"><b>{select_model}</b></p>
                """
    st.markdown(html_str_select_model,unsafe_allow_html=True)


    # if select_model=="Gensim":
    #     #st.dataframe(products.head(10))

    #     gd = GridOptionsBuilder.from_dataframe(products_raw)
    #     gd.configure_pagination(enabled=True)
    #     gd.configure_default_column(editable=True, groupable=True,enableValue=True,enableRowGroup=True)
    #     gd.configure_side_bar()

    #     sel_mode = 'multiple'
    #     gd.configure_selection(selection_mode=sel_mode, use_checkbox=True)
    #     gridoptions = gd.build()
    #     grid_table = AgGrid(products_raw, gridOptions=gridoptions,
    #                         enable_enterprise_modules=True,
    #                         update_mode=GridUpdateMode.SELECTION_CHANGED | GridUpdateMode.VALUE_CHANGED| GridUpdateMode.MODEL_CHANGED,
    #                         data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
    #                         header_checkbox_selection_filtered_only=True,
    #                         height=500,
    #                         allow_unsafe_jscode=True,
    #                         columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
    #                         fit_columns_on_grid_load=False
    #                         )

    #     sel_row = grid_table["selected_rows"]
            
    #     #st.subheader("Created Data")
    #     original_title_null = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>Created Data</b></p>'
    #     st.markdown(original_title_null,unsafe_allow_html=True)
    #     list_id=[]
    #     if sel_row!=[]:
    #         df_selected = pd.DataFrame(sel_row)
    #         df_selected=df_selected.drop("_selectedRowNodeInfo",axis=1)
    #         st.dataframe(df_selected)
    #         #st.write(df_selected.columns)
    #         for index, row in df_selected.iterrows():
    #             list_id.append(row["item_id"])

    #         #st.write(list_id)

    #         for index, row in df_selected.iterrows():
                
    #             dictionary=corpora.Dictionary(products_gem_re)
    #             feature_cnt = len(dictionary.token2id)
    #             corpus = [dictionary.doc2bow(text) for text in products_gem_re]
    #             tfidf = models.TfidfModel(corpus)
    #             index = similarities.SparseMatrixSimilarity(tfidf[corpus], num_features=feature_cnt)

    #             product_ID_input= row["item_id"]
    #             product= products[products.item_id==product_ID_input].head(1)
    #             name_description_pre=product["product_content"].to_string(index=False)


    #             results=recomender(name_description_pre,dictionary,tfidf,index)

    #             #st.subheader('With Gensim, people who like Product ID = '+str(row["item_id"])+" : "+"\""+str(products_raw.loc[products_raw["item_id"]==row["item_id"]]["name"].values[0])+"\""+' also like these')

    #             st.subheader("Top 10 products similar to "+"\""+str(products_raw[products_raw["item_id"]==product_ID_input]["name"].values[0])+":"+"\"")
    #             results= results[results.item_id != product_ID_input]

    #             show_result_gensim(results,products_raw)
            

    if select_model=="Collaborative Filtering":

        product_als_recommendation= load_data("https://khaihoan.gmazi.com/Project_2_ALS.csv")
        st.dataframe(product_als_recommendation)


        gd_reviews= GridOptionsBuilder.from_dataframe(reviews.head(1000))
        gd_reviews.configure_pagination(enabled=True)
        gd_reviews.configure_default_column(editable=True, groupable=True,enableValue=True,enableRowGroup=True)
        gd_reviews.configure_side_bar()

        sel_mode = 'multiple'
        gd_reviews.configure_selection(selection_mode=sel_mode, use_checkbox=True)
        gridoptions_reviews = gd_reviews.build()
        grid_table_reviews = AgGrid(reviews.head(1000), gridOptions=gridoptions_reviews,
                            enable_enterprise_modules=True,
                            update_mode=GridUpdateMode.SELECTION_CHANGED | GridUpdateMode.VALUE_CHANGED| GridUpdateMode.MODEL_CHANGED,
                            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                            header_checkbox_selection_filtered_only=True,
                            height=500,
                            allow_unsafe_jscode=True,
                            columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
                            fit_columns_on_grid_load=False
                            )

        sel_row_reviews = grid_table_reviews["selected_rows"]
            
        original_title_null = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>Created Data</b></p>'
        st.markdown(original_title_null,unsafe_allow_html=True)
        list_id=[]
        if sel_row_reviews!=[]:
            df_selected = pd.DataFrame(sel_row_reviews)
            df_selected=df_selected.drop("_selectedRowNodeInfo",axis=1)
            st.dataframe(df_selected)

            for index, row in df_selected.iterrows():
                str_title="Result with Customer ID = "+str(row["customer_id"])
                html_str = f"""
                <style>
                p.a {{
                font: bold 20px Garamond, serif;
                }}
                </style>
                <p class="a"><b> &#9830; {str_title}</b></p>
                """
                st.markdown(html_str, unsafe_allow_html=True)
                #st.write(str_title)
                #original_title_visual_products = '<p style="font-family:Garamond, serif; color:black; font-size: 20px;" class="a"><a><b> &#9830; {str_title}</b></a></p>'
                #st.markdown(original_title_visual_products,unsafe_allow_html=True)
                customer_ID_input= row["customer_id"]
                #st.dataframe(filter_customerID(product_als_recommendation,customer_ID_input))
                results=filter_customerID(product_als_recommendation,customer_ID_input)
                st.write(results)

                show_result_ALS(results,products_raw)

    elif select_model=="Content-based Filtering":
        #st.dataframe(products.head(10))

        gd = GridOptionsBuilder.from_dataframe(products_raw)
        gd.configure_pagination(enabled=True)
        gd.configure_default_column(editable=True, groupable=True,enableValue=True,enableRowGroup=True)
        gd.configure_side_bar()

        sel_mode = 'multiple'
        gd.configure_selection(selection_mode=sel_mode, use_checkbox=True)
        gridoptions = gd.build()
        grid_table = AgGrid(products_raw, gridOptions=gridoptions,
                            enable_enterprise_modules=True,
                            update_mode=GridUpdateMode.SELECTION_CHANGED | GridUpdateMode.VALUE_CHANGED| GridUpdateMode.MODEL_CHANGED,
                            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                            header_checkbox_selection_filtered_only=True,
                            height=500,
                            allow_unsafe_jscode=True,
                            columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
                            fit_columns_on_grid_load=False
                            )

        sel_row = grid_table["selected_rows"]
            
        #st.subheader("Created Data")
        original_title_null = '<p style="font-family:Garamond, serif; color:Blue; font-size: 30px;"><b>Created Data</b></p>'
        st.markdown(original_title_null,unsafe_allow_html=True)
        list_id=[]

        X = [1,2]
        Y = [2,2]
        cos_sim = dot(X,Y) / (norm(X)*norm(Y))
        tf = TfidfVectorizer(analyzer='word', min_df=0, stop_words=stop_words)
        tfidf_matrix = tf.fit_transform(products.product_content_wt)
        cosine_similarities = cosine_similarity(tfidf_matrix, tfidf_matrix)
        df_show = pd.DataFrame(cosine_similarities)

        result = {}
        for idx, row in products.iterrows():    
            similar_indices = cosine_similarities[idx].argsort()[-11:-1]
            similar_items = [(cosine_similarities[idx][i], products['item_id'][i]) for i in similar_indices]
            result[row['item_id']] = similar_items[0:]
        if sel_row!=[]:
            df_selected = pd.DataFrame(sel_row)
            df_selected=df_selected.drop("_selectedRowNodeInfo",axis=1)
            st.dataframe(df_selected)

            for index, row in df_selected.iterrows():
                name_="Top 10 Products similar with "+"\""+str(products_raw[products_raw["item_id"]==row["item_id"]]["name"].to_list()[0])+"\""
                html_str_name = f"""
                <style>
                p.a {{
                font: bold 30px Garamond, serif;
                color:blue
                }}
                </style>
                <p class="a"><b>{name_}</b></p>
                """
                st.markdown(html_str_name,unsafe_allow_html=True)
                recommend_item(row["item_id"], 10)



        

        
        