__author__ = "Akhil Gupta"

'''
	This script is used to get the information about the hotel
	such as Id, Name, url, Stars, Ratings. 
'''


import urllib2
import re
from bs4 import BeautifulSoup as bs

class RestaurantRating():

	def __init__(self):					
	
		self.fobj = open('restaurant_info.txt', 'a')
	
	def getRatings(self):
		page = 1
		while(page<=89):
			dic = {}
			url=urllib2.urlopen("http://www.burrp.com/bangalore/restaurants/"+str(page)).read()
			soup=bs(url)
			div=soup.find("div",{"class":"restnt_list_wrap"})
			ul=div.find("ul",{"id":"listing_content"})
			li=ul.findAll("li")
			
			for ele in range(len(li)):
				try : 	
					a_tag = li[ele].find("a")
					hotel_link = a_tag.get("href")
					hotel_name =a_tag.getText() 
					temp = hotel_link.split('/')
					hotel_id = temp[-1]
					star = li[ele].find("div",{"class":"list_star"}).getText()
					rating = li[ele].find("div",{"class":"list_rat"}).getText()
					rating  = re.findall(r'\d+',rating)[0]
					dic.update({"hotel_link" : str(hotel_link)})
					dic.update({"hotel_id":str(hotel_id)})
					dic.update({"hotel_star":eval(star)})
					dic.update({"hotel_rating":eval(rating)})
					dic.update({"hotel_name":str(hotel_name)})
					# div2=li[j].find("div",{"class":"lg_col MT5"})

					
					self.fobj.write(str(dic) + '\n')
				except Exception as e: 
					print "Exception ::", e

			print "Finished page ==> ",page
			page+= 1

if __name__ =="__main__":
	obj = RestaurantRating()
	obj.getRatings()