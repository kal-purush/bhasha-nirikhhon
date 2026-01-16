#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from flask import Flask
from flask import Flask, request, abort
from linebot import  LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent,TextMessage,TextSendMessage, ImageSendMessage,StickerSendMessage, LocationSendMessage, QuickReply, QuickReplyButton, MessageAction
import requests
import pickle
import xgboost

import pandas as pd # 引用套件並縮寫為 pd 
import numpy as np

app = Flask(__name__)
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

line_bot_api = LineBotApi('9LnP4RSdlAed7w9N/JX217BzrygaAHVbhHGJxpuhJ5zxaMdRlhlHl2Exdu0CvHAghYYwzvWJAY2rHy00zbz4UjmMJbBFvt9VOgUA9o2QmTjzkmwKxXoKH+2ZHJj/fYop2UTfsvRCaRIqmEMl+ik3JQdB04t89/1O/w1cDnyilFU=')
handler = WebhookHandler('c8d744e733ec659dc613cb5dcf6d029c')

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

@handler.add(MessageEvent, message=TextMessage) 
def handle_message(event):
    mtext = event.message.text
    format_s = '請輸入客戶資料\n1.年齡：\n2.開戶時長(月份)：\n3.近一年的餘額：'
    if "請輸入" in mtext:
        try:
            # 第一步驟，輸入數值型資料
            message = step1(mtext)
            line_bot_api.reply_message(event.reply_token,message)
        except:
            line_bot_api.reply_message(event.reply_token,TextSendMessage(text="請複製後在問題後方填寫"))
            
    elif "重新填寫" in mtext:
        try:
            get_data(flag=1)
            line_bot_api.reply_message(event.reply_token,TextSendMessage(text=format_s))
        except:
            line_bot_api.reply_message(event.reply_token,TextSendMessage(text="重新填寫失敗"))
            
    elif "下一步" in mtext:
        try:
            message = step2(mtext)
            line_bot_api.reply_message(event.reply_token,message)
        except:
            line_bot_api.reply_message(event.reply_token,TextSendMessage(text="第2步發生錯誤"))
            
    elif "4." in mtext:
        try:
            if mtext.split('.')[1][1] == '是':
                value = 'Yes'
            elif mtext.split('.')[1][1] == '否':
                value = 'No'
            save_data(flag=2, value=value)
            message = step3()
            line_bot_api.reply_message(event.reply_token,message)
        except:
            line_bot_api.reply_message(event.reply_token,TextSendMessage(text="第2步發生錯誤"))
            
    elif "5." in mtext:
        try:
            if mtext.split('.')[1][1] == '是':
                value = 'Yes'
            elif mtext.split('.')[1][1] == '否':
                value = 'No'
            save_data(flag=3, value=value)
            message = step4()
            line_bot_api.reply_message(event.reply_token,message)
        except:
            line_bot_api.reply_message(event.reply_token,TextSendMessage(text="第3步發生錯誤"))
            
    elif "6." in mtext:
        try:
            if mtext.split('.')[1][1:] == 'Entrepreneur':
                value = 'Entrepreneur'
            elif mtext.split('.')[1][1:] == 'Self Employed':
                value = 'Self Employed'
            elif mtext.split('.')[1][1:] == 'Salaried':
                value = 'Salaried'
            elif mtext.split('.')[1][1:] == 'Other':
                value = 'Other'
            save_data(flag=4, value=value)
            c = get_data(2)
            s = f'您的資料為\n1.年齡：{c[0]}\n2.開戶時長(月份)：{c[1]}\n3.近一年的餘額：{c[2]}\n4.是否已使用信貸服務?{c[3]}\n5.是否近三個月使用金融服務?{c[4]}\n6.職稱類別：{c[5]}'
            message =  step5(s)
            line_bot_api.reply_message(event.reply_token,message)
        except:
            line_bot_api.reply_message(event.reply_token,TextSendMessage(text="第4步發生錯誤"))
    elif "確認" in mtext:
        #開始預測
        row = get_data(2)
        A = [row[0]]
        V = [row[1]]
        AV = [row[2]]
        CR = [row[3]]
        IS = [row[4]]
        OC = [row[5]]
        df = pd.DataFrame({'Age': A,
                           'Vintage': V,
                           'Avg_Account_Balance': AV,
                           'Credit_Product': CR,
                           'Is_Active': IS,
                           'Occupation': OC                  
                           })
        df = pd.DataFrame(df)
        Active_mapping = {"No": 0,"Yes":1}
        df['Is_Active'] = df['Is_Active'].map(Active_mapping)
        Credit_mapping = {"No": 0,"Yes":1}
        df['Credit_Product'] = df['Credit_Product'].map(Credit_mapping)
        Occupation_mapping = {"Entrepreneur": 0,"Other":1, "Salaried":2, "Self_Employed":3}
        df['Occupation'] = df['Occupation'].map(Occupation_mapping)
        df['Age'] = (df['Age']-42.807315)/14.850996
        df['Vintage'] = (df['Vintage']-44.285522)/31.235351
        df['Avg_Account_Balance'] = (df['Avg_Account_Balance']-13.720060)/0.620936
        loaded_model = pickle.load(open("pima_pickle.dat", "rb"))

        # make predictions for test data
        y_pred = loaded_model.predict(df)
        predictions = [round(value) for value in y_pred]        
        if predictions[0]:
            final = '會購買!'
        else:
            final = '不會購買!'   
        #row之順序為 (int)age, vintage, avg_account_balance\\\\(str)credit_product is_active, occupation
        try:
            message = TextSendMessage(
                text = final
            )
            line_bot_api.reply_message(event.reply_token, message)
        except:
            line_bot_api.reply_message(event.reply_token,TextSendMessage(text='發生錯誤!'))
#         line_bot_api.reply_message(event.reply_token,TextSendMessage(text = row))
    elif "取消" in mtext:
        get_data(1)
        line_bot_api.reply_message(event.reply_token,TextSendMessage(text = format_s))
    else:    
        line_bot_api.reply_message(event.reply_token,TextSendMessage(text=format_s))

def preprocessing(mtext):
    try:        
        s_list = mtext.split('\n')
        age = int(s_list[1].split('：')[1])
        vintage = int(s_list[2].split('：')[1])
        avg_account_balance = int(s_list[3].split('：')[1])
#         credit_product = s_list[4].split('?')[1]
#         is_active = s_list[5].split('?')[1]
#         occupation = s_list[6].split('：')[1]
#         s = f'您的資料為\n1.年齡：{age}\n2.開戶時長(月份)：{vintage}\n3.近一年的餘額：{avg_account_balance}\n4.是否已使用信貸服務?{credit_product}\n5.是否近三個月使用金融服務?{is_active}\n6.職稱類別：{occupation}'
        s = f'您的資料為\n1.年齡：{age}\n2.開戶時長(月份)：{vintage}\n3.近一年的餘額：{avg_account_balance}'
    except:
        line_bot_api.reply_message(event.reply_token,TextSendMessage(text='輸入錯誤!'))
#     control_step(step)
#     return s
    return age, vintage, avg_account_balance, s

def save_data(flag=None, value=None,  age=None, vintage=None, avg_account_balance=None):
    df = pd.read_csv('output.csv', encoding= 'utf-8')
    index = []
    for col in df.columns: 
        index.append(col)
    data = np.array(df.values)
    data = data.tolist()
    if flag == 1:
        data.append([age, vintage, avg_account_balance, -1, -1, -1])
    else:
        data[-1][flag+1] = value
    dataframe = pd.DataFrame(data, columns = index)
    dataframe.to_csv('output.csv', index=False)
    
def get_data(flag):
    df = pd.read_csv('output.csv', encoding= 'utf-8')
    data = np.array(df.values)
    index = []
    for i in df.columns: 
        index.append(i)
    data = data.tolist()
    if flag == 1:
        data.pop()
        dataframe = pd.DataFrame(data, columns = index)
        dataframe.to_csv('output.csv', index=False)
        return
    return data[-1]
        
    
    
def step1(mtext):
    age, vintage, avg_account_balance, s = preprocessing(mtext)
    save_data(flag=1, age=age, vintage=vintage, avg_account_balance=avg_account_balance)
    message = TextSendMessage(
        text = s,
        quick_reply=QuickReply(
            items=[
                QuickReplyButton(
                    action=MessageAction(label="下一步", text="下一步")
                ),
                QuickReplyButton(
                    action=MessageAction(label="重新填寫", text="重新填寫1.2.3")
                )
            ]
        )
    )
    return message

def step2(mtext):
    message = TextSendMessage(
        text = '4.是否已使用信貸服務?',
        quick_reply=QuickReply(
            items=[
                QuickReplyButton(
                    action=MessageAction(label="是", text="4. 是")
                ),
                QuickReplyButton(
                    action=MessageAction(label="否", text="4. 否")
                )
#                 ,QuickReplyButton(
#                     action=MessageAction(label="重新填寫", text="重新填寫4")
#                 )
            ]
        )
    )
    return message

def step3():
    message = TextSendMessage(
        text = '5.是否近三個月使用金融服務?',
        quick_reply=QuickReply(
            items=[
                QuickReplyButton(
                    action=MessageAction(label="是", text="5. 是")
                ),
                QuickReplyButton(
                    action=MessageAction(label="否", text="5. 否")
                )
#                 ,QuickReplyButton(
#                     action=MessageAction(label="重新填寫", text="重新填寫5")
#                 )
            ]
        )
    )
    return message
def step4():
    message = TextSendMessage(
        text = '6.職稱類別?',
        quick_reply=QuickReply(
            items=[
                QuickReplyButton(
                    action=MessageAction(label="Entrepreneur", text="6. Entrepreneur")
                ),
                QuickReplyButton(
                    action=MessageAction(label="Self Employed", text="6. Self Employed")
                ),
                QuickReplyButton(
                    action=MessageAction(label="Salaried", text="6. Salaried")
                ),
                QuickReplyButton(
                    action=MessageAction(label="Other", text="6. Other")
                )
#                 ,QuickReplyButton(
#                     action=MessageAction(label="重新填寫", text="重新填寫6")
#                 )
            ]
        )
    )
    return message

def step5(s):
    message = TextSendMessage(
        text = s,
        quick_reply=QuickReply(
            items=[
                QuickReplyButton(
                    action=MessageAction(label="確認", text="確認")
                ),
                QuickReplyButton(
                    action=MessageAction(label="取消", text="取消")
                )
            ]
        )
    )
    return message
if __name__ == '__main__':
    app.run()
