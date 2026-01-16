# -*- coding: utf-8 -*-
import pandas as pd
import requests
from bs4 import BeautifulSoup

class ReviewScrapy:
    def __init__(self,urlList):
        self.urlList = urlList
        
    def get_reviews(self):
        df_reviews = pd.DataFrame(columns=['pos','neg','stay','score','name'])
        i = 1
        for url in self.urlList:
            poss = []
            negs = []
            stay_dates = []
            scores = []
            page = requests.get(url)
            soup = BeautifulSoup(page.content, 'html.parser')
            review = soup.find(id="basiclayout") # finds the contents contain all the information
            if review.find(class_="hp__hotel-title") is not None:
                hotelName = (review.find(class_="hp__hotel-title").h2.get_text()).strip('\n') # get hotel name and strip the '\n'
            else:
                continue
            try: 
                base_rev_url = "https://www.booking.com" + soup.find(class_='show_all_reviews_btn').get('href')
            except:
                print (url)
                continue
            print (base_rev_url)
            page = requests.get(base_rev_url)
            soup = BeautifulSoup(page.content, 'html.parser')
            reviews = soup.findAll(class_='review_item_review')
            for review in reviews:
                rev_pos = review.find(class_="review_pos ")
                if rev_pos is not None:
                    pos_text = rev_pos.get_text().strip('\n눇')
                else:
                    pos_text = 0
                poss.append(pos_text)
                rev_neg = review.find(class_="review_neg ")
                if rev_neg is not None:
                    neg_text = rev_neg.get_text().strip('\n눉')
                else:
                    neg_text = 0
                negs.append(neg_text)
                stay_date = review.find(class_='review_staydate ')
                if stay_date is not None:
                    stay_text = stay_date.get_text().strip('\nStayed in')
                else:
                    stay_text = 'Jan 1990'
                stay_dates.append(stay_text)
                score = review.find(class_="review-score-badge")
                if score is not None:
                    text = score.get_text().strip('\n')
                else:
                    text = 0
                scores.append(text)
            if soup.find(class_="page_link review_next_page") is not None:
                next_find = soup.find(class_="page_link review_next_page").find('a')
            else:
                continue
            while next_find is not None:
                next_url = "https://www.booking.com" + next_find.get('href')
                print (next_url)
                page = requests.get(next_url)
                soup = BeautifulSoup(page.content, 'html.parser')
                reviews = soup.findAll(class_='review_item_review')
                for review in reviews:
                    rev_pos = review.find(class_="review_pos ")
                    if rev_pos is not None:
                        pos_text = rev_pos.get_text().strip('\n눇')
                    else:
                        pos_text = 0
                    poss.append(pos_text)
                    rev_neg = review.find(class_="review_neg ")
                    if rev_neg is not None:
                        neg_text = rev_neg.get_text().strip('\n눉')
                    else:
                        neg_text = 0
                    negs.append(neg_text)
                    stay_date = review.find(class_='review_staydate ')
                    if stay_date is not None:
                        stay_text = stay_date.get_text().strip('\nStayed in')
                    else:
                        stay_text = 'Jan 1990'
                    stay_dates.append(stay_text)
                    score = review.find(class_="review-score-badge")
                    if score is not None:
                        text = score.get_text().strip('\n')
                    else:
                        text = 0
                    scores.append(text)
                next_find = soup.find(class_="page_link review_next_page").find('a')
            print ("I'm getting {} hotels' info in total.".format(i))
            i += 1
            df_pos = pd.DataFrame(poss,columns=['pos'])
            df_neg = pd.DataFrame(negs,columns=['neg'])
            df_stay = pd.DataFrame(stay_dates, columns=['stay'])
            df_score = pd.DataFrame(scores, columns=['score'])
            df_all = pd.concat([df_pos, df_neg,df_stay,df_score],axis=1)
            df_all['name'] = hotelName
            df_reviews = df_reviews.append(df_all)
            self.df_pos = df_pos
            self.df_neg = df_neg
            self.df_reviews = df_reviews

    