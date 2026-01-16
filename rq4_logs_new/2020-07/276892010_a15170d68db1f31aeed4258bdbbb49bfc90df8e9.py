import os
import zipfile
import requests

#Делаем Get запрос для скачивания файла (он в zip)
response = requests.get("https://op.mos.ru/EHDWSREST/catalog/export/get?id=785973")

# 200 Reat ответ "Ok"
if response.status_code != 200:
    print("Server Error")
else:
    print("Server Ok state")

#ответ в кодировке windows-1251
response.encoding = 'windows-1251'

#Временная дирректория чтоб не захламлять
os.mkdir(".\\temp")

#создаем временный zip и записываем туда пришедшее инфо
f = open('.\\temp\\responce.zip', 'wb')
f.write(response.content)
f.close()

#открываем скаченный zip и читаем
z = zipfile.ZipFile('.\\temp\\responce.zip', 'r')
z.extractall(".\\temp")