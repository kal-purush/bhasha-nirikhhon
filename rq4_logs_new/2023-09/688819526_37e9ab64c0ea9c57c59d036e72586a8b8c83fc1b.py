from bardapi import BardCookies
import re
import os
import pandas as pd
import json

cookie_dict = {
    "__Secure-1PSID": "bQhpqI0t6y2dsxpuQhufJtY3_o0G0dEMFfRwbPvqI_LFlMD63D2V63K9jiLvPmxxSVbP1g.",
    "__Secure-1PSIDTS": "sidts-CjEB3e41hdyRV_f4aa_Xko5j97VwUdEgmpO35Wq4HCmjhhgZLra4rXinZt2fyVgGjEXxEAA",
    "__Secure-1PSIDCC": "APoG2W9xlXqSiSEJUwdL6E-CwawpYpno-kmktUkhZINaJ41vpSePqqAu-dPG71gR1DnXCrwKISg",
    # Any cookie values you want to pass session object.
}

bard = BardCookies(cookie_dict=cookie_dict)

subject_list = []
categories = ['일상','사회','과학','스포츠','문화/예술','환경']
# categories = ['일상']
for cat in categories:
    question = f"""{cat}에 연관된 논쟁적인 글쓰기를 위한 주제를 제시합니다:
    - 최근 일주일 이슈를 반영하여 10개 출력해주세요
    - 주제만 json으로 출력해주세요. key는 '주제1','주제2',...입니다.
    - 20자 미만으로 출력합니다
    - 10대 학생을 대상으로 합니다
    - 정치와 관련된 주제는 제외합니다
    - 중복된 주제를 제외합니다"""

    response = bard.get_answer(question)
    print(response)
    p = re.compile('\{[^\*]+\}')
    subjects = p.findall(response['content'])
    print(subjects)
    subjects = json.loads(subjects[0])
    print(subjects.values())

    for subject in subjects.values():
        subject_list.append([cat,subject.strip()])
#     p = re.compile('\*\*([^\*]+)\*\*')
#     subjects = p.findall(response['content'])
#
#     for subject in subjects:
#         subject_list.append([cat,subject])
#
data = pd.DataFrame(subject_list,columns=['category','subject'])
data.to_csv('trending_topic.csv',index=False)
print(data)