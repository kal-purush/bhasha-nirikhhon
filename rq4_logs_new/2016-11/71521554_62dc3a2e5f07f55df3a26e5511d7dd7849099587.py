# POST /student/ibt/history/score.aspx/GetQuestions HTTP/1.1
# Host: benedu.co.kr
# Connection: close
# Content-Length: 80
# Origin: https://benedu.co.kr
# X-Requested-With: XMLHttpRequest
# User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2911.0 Safari/537.36
# Content-Type: application/json; charset=UTF-8
# Accept: */*
# Referer: https://benedu.co.kr/student/ibt/history/score.aspx?practiceid=137660&sbj_code=02
# Accept-Encoding: gzip, deflate, br
# Accept-Language: ko-KR,ko;q=0.8,en-US;q=0.6,en;q=0.4
# Cookie: ASP.NET_SessionId=hzzfnf2yb5rw51inzat00eb3; iBT.NET_SessionId=C075p07/UgiimGdgt+hm6PsLu1Cl1fOuej/1soPQVvc=; history_sbjcd=; history_curIdx=0; history_ctgcode=; history_sorttype=; history_sortorder=
#
# {"qst_ids":[5831,6134,5841,80658,74774,78417],"ptc_id":137660,"print_type":true}
from urllib.request import urlopen

url="https://benedu.co.kr/student/ibt/history/score.aspx/GetQuestions"
conn = urlopen(url)
body = conn.read()
request.add_header('cookie', "ASP.NET_SessionId=hzzfnf2yb5rw51inzat00eb3; iBT.NET_SessionId=C075p07/UgiimGdgt+hm6PsLu1Cl1fOuej/1soPQVvc=; history_sbjcd=; history_curIdx=0; history_ctgcode=; history_sorttype=; history_sortorder=")

# print(body)