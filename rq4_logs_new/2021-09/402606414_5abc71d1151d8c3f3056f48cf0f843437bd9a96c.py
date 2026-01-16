from bs4 import BeautifulSoup
from urllib.request import urlopen
import pandas as pd

url = "https://finance.naver.com/sise/sise_index.nhn?code=KOSDAQ"

page =urlopen(url)
soup = BeautifulSoup(page, 'lxml')
#print(soup. title)

all_info = []

## 코스닥 지수
kosdaq_index = soup.find("em", id= "now_value").text
print("코스닥 지수는 :",kosdaq_index)


##거래대금(백만)
kosdaq_amount = soup.find("td", id="amount").text
print("거래대금(백만)은 : ",kosdaq_amount)

##장중최고
kosdaq_max = soup.find("td", id="high_value").text
print("장중최고는 :",kosdaq_max)


##장중최저
kosdaq_min = soup.find("td", id="low_value").text
print("장중최저는 :",kosdaq_min)

## 거래량(천주)
deal_info = soup.find("td", id= "quant").text
print("거래량(천주)는 :",deal_info)

##52주 최고
week52_max1 = soup.find("table",class_="table_kos_index")
#print(week52_max1)
week52_max2 = week52_max1.find_all("tr")[2]
# print(week52_max2)
week52_max3 = week52_max2.find("td",class_="td").text
print("52주 최고는 :",week52_max3)

##52주 최저
week52_min = week52_max2.find("td", class_="td2").text
print("52주 최저는 :",week52_min)

all_info.append(kosdaq_index)
all_info.append(deal_info)
all_info.append(kosdaq_amount)
all_info.append(kosdaq_max)
all_info.append(kosdaq_min)
all_info.append(week52_max3)
all_info.append(week52_min)

info_list = ["코스닥 지수","거래량(천주)","거래대금(백만)","장중최고","장충최저","52주최고","52주최저"]
dic_dict = {"구분":info_list, "코스닥 정보": all_info}
dat = pd.DataFrame(dic_dict)
dat.to_csv("코스닥정보.csv", index=False)
dat.to_excel("코스닥정보.xlsx", index=False)