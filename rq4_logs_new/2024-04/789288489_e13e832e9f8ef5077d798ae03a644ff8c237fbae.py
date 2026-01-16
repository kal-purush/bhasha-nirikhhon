import requests

URL = "https://coopjbnu.kr/function/ajax.get.rest.data.php" 

data = { "code": "mobile1" }

with open("./cafeteria_menu.html", "w", encoding="UTF-8") as f: # .은 해당폴더, ..은 상위폴더 / W = write / encoding="UTF-8" - 한글깨짐방지
    res = requests.post(URL, data=data) # requests의 post 기능 사용
    res.encoding = "UTF-8" 
    f.write(res.text)

# local에서 -m pip install requests를 통해 requests 설치
# post 방식 - 추가적인 데이터를 계속 지급, 추가정보 제공(깃허브 실행시 로그인 정보 등의 추가 정보 지급)(링크 복붙해도 그 화면이 아니라 로그인 창 나옴)(중요정보)   
# get 방식 - url 자체에 정보 부여 (다 적음)(유튜브는 끝부분을 통해 영상고유번호를 지급해 복사해서 붙여넣어도 재생가능)(길이제한)(개방정보)