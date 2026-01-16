#!/usr/bin/python
#-*-coding: utf-8 -*-
##from __future__ import absolute_import
###
from flask import Flask, jsonify, render_template, request
import numpy as np
import geopy.distance as ps
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage,TemplateSendMessage,ImageSendMessage, StickerSendMessage, AudioSendMessage, FlexSendMessage
)
from linebot.models.template import *
from linebot import (
    LineBotApi, WebhookHandler
)

import pandas as pd
import plotly.graph_objs as go
from pythainlp.corpus import thai_stopwords
from sklearn.naive_bayes import MultinomialNB
import requests
import warnings
warnings.filterwarnings('ignore')
from pythainlp import sent_tokenize, word_tokenize
from nltk.sentiment.util import *
nltk.download('punkt')
nltk.download('stopwords')
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer  # TF-IDF
from pythainlp.util import normalize
test_menu=pd.read_csv("./thai_menu.csv")
dat = pd.read_excel('addb.xlsx')
conv = pd.read_excel('conversation.xlsx')
def regex_nodigits_new(s):
    '''use regex to clean string:
    get rid of punctuations, capitalized letters and numbers'''
    s = re.sub(r'[\d]','',str(s))#replace digits with empty str
    return s #only returning ingredients of first recipe?
menu_test=test_menu
menu_test2=test_menu
menu_test2_ingr=menu_test2['สูตรอาหาร'].apply(regex_nodigits_new)
# print(menu_test2_ingr)

def replace_text(text):
    lenx=len(text)
    i=0
    while i < lenx:
      text[i] = text[i].replace("\n", "")
      i += 1
    return text

menu_test2_ingr=replace_text(menu_test2_ingr)

menu_test2_ingr=menu_test2_ingr.apply(normalize)
stopwords = list(thai_stopwords())

def remove_stopword(text):
    lenx=len(text)
    i=0
    while i < lenx:

      list_word = word_tokenize(text[i], keep_whitespace=False)
      text[i] = [i for i in list_word if i not in stopwords]
      i += 1
    return text

menu_test3_ingr=remove_stopword(menu_test2_ingr)

from pythainlp.tokenize import sent_tokenize
def remove_stopword2(text):
    lenx=len(text)
    i=0
    while i < lenx:
      list_word = sent_tokenize(text[i], engine="whitespace+newline", keep_whitespace=False)
      text[i] = [i for i in list_word if i not in stopwords]
      i += 1
    return text

menu_test4_ingr=menu_test2['ชื่ออาหาร'].apply(regex_nodigits_new)
menu_test5_ingr=menu_test4_ingr.apply(normalize)
menu_test66_ingr=remove_stopword2(menu_test5_ingr)
menu_test_name=menu_test66_ingr

def remove_list(text):
    lenx=len(text)
    i=0
    while i < lenx:
        del text[i][1:]
        i += 1
    return text

menu_test_name2=remove_list(menu_test_name)

def remove_stopword3(text):
    lenx=len(text)
    i=0
    while i < lenx:
      list_word = word_tokenize(text[i], keep_whitespace=False)
      text[i] = [i for i in list_word if i not in stopwords]
      i += 1
    return text

menu_test44_ingr=menu_test2['ชื่ออาหาร'].apply(regex_nodigits_new)
menu_test55_ingr=menu_test44_ingr.apply(normalize)
menu_test77_ingr=remove_stopword3(menu_test55_ingr)

menu_test7_ingr=menu_test2['ประเภทอาหาร'].apply(regex_nodigits_new)

menu_test8_ingr=menu_test7_ingr.apply(normalize)
menu_test9_ingr=remove_stopword(menu_test8_ingr)
menu_test9_ingr22=menu_test2['ประเภทอาหาร'].apply(regex_nodigits_new)
menu_test9_ingr22=menu_test9_ingr22.apply(normalize)
menu_test9_ingr22=remove_stopword(menu_test9_ingr22)

def remove_list2(text):
    lenx=len(text)
    i=0
    while i < lenx:
        del text[i][2:]
        del text[i][0]
        i += 1
    return text

menu_test9_ingr22=remove_list2(menu_test9_ingr22)
menu_test_name22=menu_test_name2.apply(', '.join)
menu_test777_ingr=menu_test77_ingr.apply(', '.join)
menu_test33_ingr=menu_test3_ingr.apply(', '.join)
menu_test99_ingr=menu_test9_ingr.apply(', '.join)
menu_test99_ingr22=menu_test9_ingr22.apply(', '.join)
menu_test99_ingr2222= menu_test99_ingr22+','
menu_df = pd.DataFrame(list(zip(menu_test77_ingr,menu_test3_ingr,menu_test9_ingr,menu_test9_ingr22,menu_test_name2)),
               columns =['รายละเอียดอาหาร', 'สูตรอาหาร','ประเภทอาหาร','สัญชาติอาหาร','ชื่ออาหาร'])
menu_df2 = pd.DataFrame(list(zip(menu_test777_ingr,menu_test33_ingr,menu_test99_ingr,menu_test99_ingr22,menu_test_name22)),
               columns =['รายละเอียดอาหาร', 'สูตรอาหาร','ประเภทอาหาร','สัญชาติอาหาร','ชื่ออาหาร'])
menu_df3 = pd.DataFrame(list(zip(menu_test99_ingr2222+menu_test777_ingr+menu_test33_ingr,menu_test_name22)),
               columns =['รายละเอียดอาหาร','ชื่ออาหาร'])
menu_df3 = menu_df3.drop_duplicates(subset=['ชื่ออาหาร'],keep='last')
menu_df3=menu_df3.reset_index(drop=True)
menu_df4 = pd.DataFrame(list(zip(menu_test_name22,menu_test777_ingr+menu_test33_ingr+menu_test99_ingr,menu_test99_ingr22)),
               columns =['ชื่ออาหาร','รายละเอียดอาหาร','สัญชาติอาหาร'])
menu_df4
menu_df4 = menu_df4.drop_duplicates(subset=['ชื่ออาหาร'],keep='last')
menu_df4=menu_df4.reset_index(drop=True)
train=menu_df4
total = train.isnull().sum().sort_values(ascending = False)
percent = (train.isnull().sum()/train.isnull().count()*100).sort_values(ascending = False)
missing_train_data  = pd.concat([total, percent], axis=1, keys=['Total missing', 'Percent missing'])
# print("             # of Rows, Columns:",train.shape)
# print(missing_train_data.head())

color_theme = dict(color = ['rgba(221,160,221,1)','rgba(169,169,169,1)','rgba(255,160,122,1)','rgba(176,224,230,1)','rgba(169,169,169,1)','rgba(255,160,122,1)','rgba(176,224,230,1)',
                   'rgba(188,143,143,1)','rgba(221,160,221,1)','rgba(169,169,169,1)','rgba(255,160,122,1)','rgba(176,224,230,1)','rgba(189,183,107,1)','rgba(188,143,143,1)','rgba(221,160,221,1)','rgba(169,169,169,1)','rgba(255,160,122,1)','rgba(176,224,230,1)','rgba(169,169,169,1)','rgba(255,160,122,1)'])
temp = train['สัญชาติอาหาร'].value_counts()
trace = go.Bar(y=temp.index[::-1],x=(temp)[::-1],orientation = 'h',marker=color_theme)
layout = go.Layout(title = "เมนูแต่ละสัญชาติอาหาร",xaxis=dict(title='จำนวนเมนูอาหาร',tickfont=dict(size=14,)),
                   yaxis=dict(title='สัญชาติอาหาร',titlefont=dict(size=16),tickfont=dict(size=14)),margin=dict(l=200,))
data = [trace]
fig = go.Figure(data=data, layout=layout)
# iplot(fig,filename='basic-bar')
menu_df3.describe()
train = menu_df3
len(train)
train.shape
data_train= train['รายละเอียดอาหาร']
target_train= train['ชื่ออาหาร']
# print(data_train.shape)
# print(target_train.shape)
# create a count vectorizer object
count_vect = CountVectorizer(analyzer='word', token_pattern=r'\w{1,}')
count_vect.fit(train['รายละเอียดอาหาร'])

# transform the training and validation data using count vectorizer object
xtrain_count =  count_vect.transform(data_train)
# word level tf-idf
tfidf_vect = TfidfVectorizer(analyzer='word', token_pattern=r'\w{1,}', max_features=5000)
tfidf_vect.fit(train['รายละเอียดอาหาร'])
xtrain_tfidf =  tfidf_vect.transform(data_train)
# ngram level tf-idf
tfidf_vect_ngram = TfidfVectorizer(analyzer='word', token_pattern=r'\w{1,}', ngram_range=(1,3), max_features=5000)
tfidf_vect_ngram.fit(train['รายละเอียดอาหาร'])
xtrain_tfidf_ngram =  tfidf_vect_ngram.transform(data_train)

nb_classifier = MultinomialNB()
nb_classifier.fit(xtrain_tfidf_ngram, target_train)
# creating an empty list


# import main
app = Flask(__name__)

lineaccesstoken = '7kGi30bHhAA2h2RdzoswZfDv7BSliIgudIJcClSNbQtgk/2kHbf4PdI2p7fL2FFUyoZhDdeKq8q8OIKqaxNf+ZA0ywGA3xJ6K96Bo6eEWKSgRGHS3rTWPLP2y+AELX+sQ53a+ONDC7e2k6Z620EssQdB04t89/1O/w1cDnyilFU='
line_bot_api = LineBotApi(lineaccesstoken)
flex = ''
casedata = pd.read_excel('casedata.xlsx')

####################### new ########################
@app.route('/')
def index():
    return "Hello World!"

@app.route('/aek')
def aek():
    lst = []
    inpmessage = "ชื่อ"
    res = conv[conv['intent'] == inpmessage]
    if len(res) == 0:
        flex = flexmessage(inpmessage)
    else:
        flex = res['callback'].values[0]

    if flex == 'nodata':
        lst.append(flex)  # adding the element
        xtest_count = count_vect.transform(lst)
        xtest_tfidf = tfidf_vect.transform(lst)
        xtest_tfidf_ngram = tfidf_vect_ngram.transform(lst)
        nb_preds = nb_classifier.predict(xtest_tfidf_ngram)
        if len(nb_preds[0]) > 0 and nb_preds[0]!='Tompei':
            replyObj = str(nb_preds)
        else:
            replyObj = "ลองอธิบายเจาะจงกว่านี้ได้มั๊ยอะ"
    else: replyObj = flex
    return replyObj
@app.route('/webhook', methods=['POST'])
def callback():
    json_line = request.get_json(force=False,cache=False)
    json_line = json.dumps(json_line)
    decoded = json.loads(json_line)
    no_event = len(decoded['events'])
    for i in range(no_event):
        event = decoded['events'][i]
        event_handle(event)
    return '',200


def event_handle(event):
    print(event)
    try:
        userId = event['source']['userId']
    except:
        print('error cannot get userId')
        return ''

    try:
        rtoken = event['replyToken']
    except:
        print('error cannot get rtoken')
        return ''
    if 'message' in event.keys():
        try:
            msgType = event["message"]["type"]
            msgId = event["message"]["id"]
        except:
            print('error cannot get msgID, and msgType')
            sk_id = np.random.randint(1,17)
            replyObj = StickerSendMessage(package_id=str(1),sticker_id=str(sk_id))
            line_bot_api.reply_message(rtoken, replyObj)
            return ''
    if 'postback' in event.keys():
        msgType = 'postback'

    if msgType == "text":
        msg = str(event["message"]["text"])
        replyObj = handle_text(msg)
        line_bot_api.reply_message(rtoken, replyObj)

    if msgType == "postback":
        msg = str(event["postback"]["data"])
        replyObj = handle_postback(msg)
        line_bot_api.reply_message(rtoken, replyObj)

    if msgType == "location":
        lat = event["message"]["latitude"]
        lng = event["message"]["longitude"]
        #txtresult = handle_location(lat,lng,casedata,3)
        result = getcaseflex(lat,lng)
        replyObj = FlexSendMessage(alt_text='Flex Message alt text', contents=result)
        line_bot_api.reply_message(rtoken, replyObj)
    else:
        sk_id = np.random.randint(1,17)
        replyObj = StickerSendMessage(package_id=str(1),sticker_id=str(sk_id))
        line_bot_api.reply_message(rtoken, replyObj)
    return ''




def getdata(query):
    res = dat[dat['QueryWord']==query]
    if len(res)==0:
        return 'nodata'
    else:
        productName = res['ProductName'].values[0]
        imgUrl = res['ImgUrl'].values[0]
        desc = res['Description'].values[0]
        cont = res['Contact'].values[0]
        return productName,imgUrl,desc,cont

def flexmessage(query):
    res = getdata(query)
    if res == 'nodata':
        return 'nodata'
    else:
        productName,imgUrl,desc,cont = res
    flex = '''
    {
        "type": "bubble",
        "hero": {
          "type": "image",
          "url": "%s",
          "margin": "none",
          "size": "full",
          "aspectRatio": "1:1",
          "aspectMode": "cover",
          "action": {
            "type": "uri",
            "label": "Action",
            "uri": "https://linecorp.com"
          }
        },
        "body": {
          "type": "box",
          "layout": "vertical",
          "spacing": "md",
          "action": {
            "type": "uri",
            "label": "Action",
            "uri": "https://linecorp.com"
          },
          "contents": [
            {
              "type": "text",
              "text": "%s",
              "size": "xl",
              "weight": "bold"
            },
            {
              "type": "text",
              "text": "%s",
              "wrap": true
            }
          ]
        },
        "footer": {
          "type": "box",
          "layout": "vertical",
          "contents": [
            {
              "type": "button",
              "action": {
                "type": "postback",
                "label": "ติดต่อคนขาย",
                "data": "%s"
              },
              "color": "#F67878",
              "style": "primary"
            }
          ]
        }
      }'''%(imgUrl,productName,desc,cont)
    return flex

from linebot.models import (TextSendMessage,FlexSendMessage)
import json
def handle_text(inpmessage):
    lst = []
    res = conv[conv['intent'] == inpmessage]
    if len(res) == 0:
        flex = flexmessage(inpmessage)
    else:
        flex = res['callback'].values[0]

    if flex == 'nodata':
        lst.append(inpmessage)  # adding the element
        xtest_count = count_vect.transform(lst)
        xtest_tfidf = tfidf_vect.transform(lst)
        xtest_tfidf_ngram = tfidf_vect_ngram.transform(lst)
        nb_preds = nb_classifier.predict(xtest_tfidf_ngram)
        if len(nb_preds[0]) > 0 and nb_preds[0] != 'Tompei':
            replyObj = TextSendMessage(text=nb_preds[0])
        else:
            replyObj = TextSendMessage(text="ลองอธิบายเจาะจงกว่านี้ได้มั๊ยอะ")
    else:
        replyObj = flex
    return replyObj

def handle_postback(inpmessage):
    replyObj = TextSendMessage(text=inpmessage)
    return replyObj


def handle_location(lat,lng,cdat,topK):
    result = getdistace(lat, lng,cdat)
    result = result.sort_values(by='km')
    result = result.iloc[0:topK]
    txtResult = ''
    for i in range(len(result)):
        kmdistance = '%.1f'%(result.iloc[i]['km'])
        newssource = str(result.iloc[i]['News_Soruce'])
        txtResult = txtResult + 'ห่าง %s กิโลเมตร\n%s\n\n'%(kmdistance,newssource)
    return txtResult[0:-2]


def getcaseflex(lat,lng):
    url = 'http://botnoiflexapi.herokuapp.com/getnearcase?lat=%s&long=%s'%(lat,lng)
    res = requests.get(url).json()
    return res

def getdistace(latitude, longitude,cdat):
  coords_1 = (float(latitude), float(longitude))
  ## create list of all reference locations from a pandas DataFrame
  latlngList = cdat[['Latitude','Longitude']].values
  ## loop and calculate distance in KM using geopy.distance library and append to distance list
  kmsumList = []
  for latlng in latlngList:
    coords_2 = (float(latlng[0]),float(latlng[1]))
    kmsumList.append(ps.vincenty(coords_1, coords_2).km)
  cdat['km'] = kmsumList
  return cdat


if __name__ == '__main__':
    app.run(debug=True)