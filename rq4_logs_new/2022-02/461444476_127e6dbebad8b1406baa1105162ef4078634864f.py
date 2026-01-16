#!/usr/bin/env python
# coding: utf-8

# # Pyinstallerからexeファイル生成するコマンド
# 
# 1. cd で.pyファイルがある所まで移動
# 2. pyinstaller paratranz_CityofGangsters_JpCsv_to_GameTxt.py --onefile

# In[105]:


# print("ParaTranzからダウンロードしたCSVファイルを、CityofGangstersで使用できるように変換します")
# print("exeファイルと同じ階層に「jptut.csv」、「jptut.csv」が存在することを確認してから何かキーを押してください")
print("Press any key to start.")
a = input()


# In[99]:


import os

currentDir = os.getcwd()
# print(currentDir)
jptutCsvPath = currentDir + '/jptut.csv'
jpCsvPath = currentDir + '/jp.csv'
# print(jptutCsvPath)
# print(jpCsvPath)

if(os.path.exists(jptutCsvPath)==False):
    print("jptut.csv could not be found")
    a = input()
    exit()
    
if(os.path.exists(jpCsvPath)==False):
    print("jp.csv could not be found")
    a = input()
    exit()


# In[100]:


import pandas as pd
import os

# currentDir = os.getcwd()
# print(currentDir)

converCsvFilePath = currentDir + '/jptut.csv'
# print(converCsvFilePath)

# データフレームを作成
csv_input = pd.read_csv(filepath_or_buffer=converCsvFilePath, encoding='utf8', sep=',')

# インプットの項目数（行数 * カラム数）を返却します。
# print(csv_input.columns)

# 指定したカラムだけ抽出したDataFrameオブジェクトを返却します。
# print(csv_input[['﻿__id', 'en', 'en.1']]) 

for index, data in csv_input.iterrows():
    # 未翻訳の文字列が空白になってしまうため、一旦英語で保管
    if pd.isnull(data['en.1']):
        csv_input.loc[index, 'en.1'] = data['en']
        
# CSV ファイルとして出力
csv_input.to_csv(converCsvFilePath.replace('.csv', '.txt'), columns=['﻿__id', 'en.1'], sep='\t', index=False, header=False)


# In[101]:


import pandas as pd
import os

currentDir = os.getcwd()
# print(currentDir)

converCsvFilePath = currentDir + '/jp.csv'
# print(converCsvFilePath)

# データフレームを作成
csv_input = pd.read_csv(filepath_or_buffer=converCsvFilePath, encoding='utf8', sep=',')

# インプットの項目数（行数 * カラム数）を返却します。
# print(csv_input.columns)

# 指定したカラムだけ抽出したDataFrameオブジェクトを返却します。
# print(csv_input[['﻿__id', 'jp', 'jp.1']]) 

for index, data in csv_input.iterrows():
    # 未翻訳の文字列が空白になってしまうため、一旦英語で保管
    if pd.isnull(data['jp.1']):
        csv_input.loc[index, 'jp.1'] = data['jp']
        
# TXTファイルとして出力
csv_input.to_csv(converCsvFilePath.replace('.csv', '.txt'), columns=['﻿__id', 'jp.1'], sep='\t', index=False, header=False)


# In[102]:


converTxtFilePath = currentDir + '/jptut.txt'

with open(converTxtFilePath) as reader:
    s = reader.read()

    # CityOfGangsters特有?のHeaderを追記 
    s = 'KEY	VALUE	COMMENTS\n__id	jp\n\n'+ s

    # カンマが大文字になってしまうため小文字に変換
    s = s.replace('”', '"')

    with open(converTxtFilePath, 'w') as writer:
        writer.write(s)


# In[103]:


converTxtFilePath = currentDir + '/jp.txt'

with open(converTxtFilePath) as reader:
    s = reader.read()

    # CityOfGangsters特有?のHeaderを追記 
    s = 'Key	German	English\n__id	jp	en\n__name	日本語	English\n\n__id	jp\n__name	日本語\n' +s

    # カンマが大文字になってしまうため小文字に変換
    s = s.replace('”', '"')

    with open(converTxtFilePath, 'w') as writer:
        writer.write(s)


# In[104]:


# print("変換が完了しました！")
# print("exeファイルと同じ階層に「jptut.txt」、「jptut.txt」が生成されているはずです")
# print("ファイルを「City of Gangsters\CoG_Data\StreamingAssets\Loc」に配置して完了です！")
print("Conversion completed!")
print("jptut.txt and jptut.txt are generated in the same path as the exe file.")
a = input()


# In[ ]:



