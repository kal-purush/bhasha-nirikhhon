import re

from utils import *

source = open('melon.html', 'rt').read()

# <a href=....>(내용)</a>
PATTERN_A_CONTENT = re.compile(r'<a.*?>(.*?)</a>')
# <tr class="lst50"></tr>
PATTERN_TR = re.compile(r'<tr.*?class="lst50".*?>.*?</tr>', re.DOTALL)
# <td>...</td> 요소를 찾기 위한 정규표현식
PATTERN_TD = re.compile(r'<td.*?>.*?</td>', re.DOTALL)
# img태그의 'src'내용을 가져오는 정규표현식
PATTERN_IMG = re.compile(r'<img.*?src="(.*?)".*?>', re.DOTALL)
# rank
PATTERN_RANK = re.compile(r'<span.*?class=".*?rank.*?".*?>(.*?)</span>', re.DOTALL)

# 결과 dict리스트를 담을 result 리스트 변수
result = []

# lst50이라는 class를 가진 tr요소들을 모두 찾아 순회
for tr in re.finditer(PATTERN_TR, source):
    # 각 tr요소가 가진 td list를 td_list에 할당. 인덱스 연산을 할 것이므로 findall사용
    td_list = re.findall(PATTERN_TD, tr.group())

    # 값들이 들어있는 태그들
    td_rank = td_list[1]
    td_img_cover = td_list[3]
    td_title_artist = td_list[5]
    div_title = find_tag('div', td_title_artist, class_='rank01')
    div_artist = find_tag('div', td_title_artist, class_='rank02')
    td_album = td_list[6]

    # 각 태그들에서 값을 찾음
    rank = re.search(PATTERN_RANK, td_rank).group(1)
    url_img_cover = re.search(PATTERN_IMG, td_img_cover).group(1)
    title_a_tag = find_tag('a', div_title)
    title = get_tag_content(title_a_tag)
    artist = re.search(PATTERN_A_CONTENT, div_artist).group(1)
    album = re.search(PATTERN_A_CONTENT, td_album).group(1)

    # 결과 리스트에 dict하나씩 생성해서 append
    result.append({
        'rank': rank,
        'title': title,
        'url_img_cover': url_img_cover,
        'artist': artist,
        'album': album,
    })

for item in result:
    print(item)