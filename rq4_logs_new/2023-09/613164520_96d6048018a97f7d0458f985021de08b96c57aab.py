from bs4 import BeautifulSoup
import re
from tqdm import tqdm
from post_crawling import get_html, get_post_content
import pandas as pd



def post_list_crawling():
    URL = "https://sw.skku.edu/sw/notice.do?mode=list&&articleLimit=1000&article.offset=0"
    soup = BeautifulSoup(
        get_html(URL)
    )
    board_list = soup.find("ul",{"class":"board-list-wrap"})
    board_list_items = board_list.find_all("li")
    board_list_items = [item.find("a").get("href") for item in board_list_items if item.find("a")]
    board_list_items = [re.search(r'articleNo=(\d+)', url).group(1) for url in board_list_items]
    post_content = {
        "Title" : [],
        "Category" : [],
        "Date": [],
        "Content" : [],
        "Url" : []
    }
    for item in tqdm(board_list_items, desc= "Crawling"):
        ret = get_post_content(item)
        if ret:
            post_content["Title"] += [ret["post_title"]]
            post_content["Category"] += [ret["post_category"]]
            post_content["Url"] += [ret["post_url"]]
            post_content["Date"] += [ret["post_date"]]
            post_content["Content"] += [ret["post_content"]]


    print(len(post_content["Title"]))

    
    df = pd.DataFrame(post_content)

    df.to_csv('file_content_list.csv', index=False)
 
    


post_list_crawling()