#!/usr/bin/env python
# encoding: utf-8
# -*- coding: cp949 -*-

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import paramiko

scope = [
'https://spreadsheets.google.com/feeds',
'https://www.googleapis.com/auth/drive',
]

json_file_name = 'auth'

credentials = ServiceAccountCredentials.from_json_keyfile_name(json_file_name, scope)
gc = gspread.authorize(credentials)

spreadsheet_url = 'sheet'
doc = gc.open_by_url(spreadsheet_url)

title1 = '<?xml version="1.0" encoding="utf-8"?>'+ "\n";
title2 = '<CATEGORY TYPE="MSG" LANG="ko">'+ "\n";
end = '</CATEGORY>';

volt_file_name = ['lang.volt.ko.wprx', 'lang.volt.en.wprx', 'lang.volt.jp.wprx', 'lang.volt.ru.wprx', 'lang.volt.uk.wprx']
audit_file_name = ['lang.audit.ko.wprx', 'lang.audit.en.wprx', 'lang.audit.jp.wprx', 'lang.audit.ru.wprx', 'lang.audit.uk.wprx']
ctrl_file_name = ['lang.ctrl.ko.wprx', 'lang.ctrl.en.wprx', 'lang.ctrl.jp.wprx', 'lang.ctrl.ru.wprx', 'lang.ctrl.uk.wprx']
msg_file_name = ['lang.msg.ko.wprx', 'lang.msg.en.wprx', 'lang.msg.jp.wprx', 'lang.msg.ru.wprx', 'lang.msg.uk.wprx']

###################################################
# X_VOLT wprx
###################################################
worksheet = doc.worksheet('1. X_VOLT_wprx')

# 한국어
row_data = worksheet.col_values(7)
row_data = [x.encode('utf-8') for x in row_data]

f = open('lang.volt.ko.wprx', 'w')
f.write(title1)
f.write(title2)
for index, value in enumerate(row_data):
    if index>3:
        f.write(str(value) + "\n")
f.write(end)
f.close()

# 일본어
row_data = worksheet.col_values(6)
row_data = [x.encode('utf-8') for x in row_data]

f = open('lang.volt.jp.wprx', 'w')
f.write(title1)
f.write(title2)
for index, value in enumerate(row_data):
    if index>3:
        f.write(str(value) + "\n")
f.write(end)
f.close()

# 영어
row_data = worksheet.col_values(5)
row_data =  [x.encode('utf-8') for x in row_data]

f = open('lang.volt.en.wprx', 'w')
f.write(title1)
f.write(title2)
for index, value in enumerate(row_data):
    if index>3:
        f.write(str(value) + "\n")
f.write(end)
f.close()

# 우크라이나어
row_data = worksheet.col_values(4)
row_data =  [x.encode('utf-8') for x in row_data]

f = open('lang.volt.uk.wprx', 'w')
f.write(title1)
f.write(title2)
for index, value in enumerate(row_data):
    if index>3:
        f.write(str(value) + "\n")
f.write(end)
f.close()

# 러시아어
row_data = worksheet.col_values(3)
row_data =  [x.encode('utf-8') for x in row_data]

f = open('lang.volt.uk.wprx', 'w')
f.write(title1)
f.write(title2)
for index, value in enumerate(row_data):
    if index>3:
        f.write(str(value) + "\n")
f.write(end)
f.close()


###################################################
# WMC 
###################################################


worksheet = doc.worksheet('2. WMC_wprx')

#############################
# 한국어
#############################

row_data = worksheet.col_values(7)
row_data = [x.encode('utf-8') for x in row_data]

f = open('lang.msg.ko.wprx', 'w')
f.write(title1)
f.write(title2)

this_idx = 0

for index, value in enumerate(row_data):
    if index>3 and str(value) == "":
        this_idx = index
        break

    if index>3:
        f.write(str(value) + "\n")


f.write(end)
f.close()

f = open('lang.ctrl.ko.wprx', 'w')
f.write(title1)
f.write(title2)

for index, value in enumerate(row_data):
    if index>this_idx and str(value) == "":
        this_idx = index
        break

    if index>this_idx:
        f.write(str(value) + "\n")

f.write(end)
f.close()

f = open('lang.audit.ko.wprx', 'w')
f.write(title1)
f.write(title2)

for index, value in enumerate(row_data):
    if index>this_idx:
        f.write(str(value) + "\n")

f.write(end)
f.close()


#############################
# 우크라이나어
#############################

row_data = worksheet.col_values(6)
row_data = [x.encode('utf-8') for x in row_data]

f = open('lang.msg.uk.wprx', 'w')
f.write(title1)
f.write(title2)

this_idx = 0

for index, value in enumerate(row_data):
    if index>3 and str(value) == "":
        this_idx = index
        break

    if index>3:
        f.write(str(value) + "\n")


f.write(end)
f.close()

f = open('lang.ctrl.uk.wprx', 'w')
f.write(title1)
f.write(title2)

for index, value in enumerate(row_data):
    if index>this_idx and str(value) == "":
        this_idx = index
        break

    if index>this_idx:
        f.write(str(value) + "\n")

f.write(end)
f.close()

f = open('lang.audit.uk.wprx', 'w')
f.write(title1)
f.write(title2)

for index, value in enumerate(row_data):
    if index>this_idx:
        f.write(str(value) + "\n")

f.write(end)


#############################
# 러시아어
#############################

row_data = worksheet.col_values(5)
row_data = [x.encode('utf-8') for x in row_data]

f = open('lang.msg.ru.wprx', 'w')
f.write(title1)
f.write(title2)

this_idx = 0

for index, value in enumerate(row_data):
    if index>3 and str(value) == "":
        this_idx = index
        break

    if index>3:
        f.write(str(value) + "\n")


f.write(end)
f.close()

f = open('lang.ctrl.ru.wprx', 'w')
f.write(title1)
f.write(title2)

for index, value in enumerate(row_data):
    if index>this_idx and str(value) == "":
        this_idx = index
        break

    if index>this_idx:
        f.write(str(value) + "\n")

f.write(end)
f.close()

f = open('lang.audit.ru.wprx', 'w')
f.write(title1)
f.write(title2)

for index, value in enumerate(row_data):
    if index>this_idx:
        f.write(str(value) + "\n")

f.write(end)


#############################
# 일본어
#############################

row_data = worksheet.col_values(4)
row_data = [x.encode('utf-8') for x in row_data]

f = open('lang.msg.jp.wprx', 'w')
f.write(title1)
f.write(title2)

this_idx = 0

for index, value in enumerate(row_data):
    if index>3 and str(value) == "":
        this_idx = index
        break

    if index>3:
        f.write(str(value) + "\n")


f.write(end)
f.close()

f = open('lang.ctrl.jp.wprx', 'w')
f.write(title1)
f.write(title2)

for index, value in enumerate(row_data):
    if index>this_idx and str(value) == "":
        this_idx = index
        break

    if index>this_idx:
        f.write(str(value) + "\n")

f.write(end)
f.close()

f = open('lang.audit.jp.wprx', 'w')
f.write(title1)
f.write(title2)

for index, value in enumerate(row_data):
    if index>this_idx:
        f.write(str(value) + "\n")

f.write(end)



#############################
# 영어
#############################

row_data = worksheet.col_values(3)
row_data = [x.encode('utf-8') for x in row_data]

f = open('lang.msg.en.wprx', 'w')
f.write(title1)
f.write(title2)

this_idx = 0

for index, value in enumerate(row_data):
    if index>3 and str(value) == "":
        this_idx = index
        break

    if index>3:
        f.write(str(value) + "\n")


f.write(end)
f.close()

f = open('lang.ctrl.en.wprx', 'w')
f.write(title1)
f.write(title2)

for index, value in enumerate(row_data):
    if index>this_idx and str(value) == "":
        this_idx = index
        break

    if index>this_idx:
        f.write(str(value) + "\n")

f.write(end)
f.close()

f = open('lang.audit.en.wprx', 'w')
f.write(title1)
f.write(title2)

for index, value in enumerate(row_data):
    if index>this_idx:
        f.write(str(value) + "\n")

f.write(end)

##############################################
# Change PO & MO
##############################################

try:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
    ## host 
    # ssh.connect( )

    print("ssh connect success")

    sftp = ssh.open_sftp()

    for data in volt_file_name:
        sftp.put(data, '경로')

    print("sftp success")

    stdin, stdout, stderr = ssh.exec_command("명령어") # 명령어 실행
    lines = stdout.readlines() # 실행한 명령어에 대한 결과 텍스트
    resultData = ''.join(lines)
 
    print(resultData) # 결과 확인

    stdin, stdout, stderr = ssh.exec_command("/tmp/wmc_en_us.xml")
    lines = stdout.readlines() # 실행한 명령어에 대한 결과 텍스트
    resultData = ''.join(lines)

    print(resultData) # 결과 확인

    stdin, stdout, stderr = ssh.exec_command("/tmp/wmc_ja_jp.xml")
    lines = stdout.readlines() # 실행한 명령어에 대한 결과 텍스트
    resultData = ''.join(lines)

    print(resultData) # 결과 확인

    ssh.close()

except Exception as err: 
    print(err)