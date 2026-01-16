#%%
#문장의 처음은 대문자로 시작하며,
#이외에는 어디에도 대문자가 나타나지 않는다.
# 이 조건에 따라 대문자가 SPLIT하는 조건이라는것을 넌지시 알려주었다.
#이를 위해 re 모듈을 사용하였다.
import re

sentence = list(map(str, re.split(r'(?=[A-Z])',input())))

for sent in sentence:
    try:
        #설명은 자세하게 못하겠지만 sent[:7] == 'What is'로 한것과 (sent[:1] + sent[1:7].lower()) == 'What is')는 차이가 있는듯 하다.
        if (sent[:1] + sent[1:7].lower()) == 'What is' and '?' in sent:
            sent = sent.replace('What','Forty-two')
            sent = sent.replace('? ', '.')
            sent = sent.replace('?','.')
            print(sent)    #빈 공백이 들어왔을 예외사항 처리
    except:
        continue