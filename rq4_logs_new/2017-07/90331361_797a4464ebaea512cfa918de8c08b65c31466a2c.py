import requests
from bs4 import BeautifulSoup

def getFulltext(movie_list):
	res = requests.get(movie_list['url'])
	print("電影名稱 : " + str(movie_list['name']))
	print("網址 : " + str(movie_list['url']))
	soup = BeautifulSoup(res.text, 'html5lib')
	content = soup.find('div', {'class':'text full'})
	print(content.text)
	print("--------------------------------------------------")

if __name__ == '__main__':
	res = requests.get('https://tw.movies.yahoo.com/movie_thisweek.html')
	#print(res.status_code)
	soup = BeautifulSoup(res.text, 'html5lib')
	movie_list = []
	t = soup.find('div',{'class':'group'}).find_all('div', {'class':'text'})
	for st in t:
		name = st.h4.a.text
		url = st.h4.a['href'].split('*')[1]
		movie_list.append({
			'name':name,
			'url':url
		})
	print("共 " + str(len(movie_list)) + " 筆資料")
	for a in movie_list:
		getFulltext(a)
	
	