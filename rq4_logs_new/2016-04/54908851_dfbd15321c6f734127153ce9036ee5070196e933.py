# -*- coding: utf-8 -*-
"""
Created on Mon Mar 28 22:31:09 2016

@author: Karen
"""
from yelp.client import Client
from yelp.oauth1_authenticator import Oauth1Authenticator

#global loc

def yelp(loc):
    auth = Oauth1Authenticator(
        consumer_key="NnmZFkzKNYQ0zeJ0lHQcBQ",
        consumer_secret="w7eAQ8cOW5gMLgVGc8-Oe3Qz_2Y",
        token="z1jrNwOaEXGX-xlI0X31YdLzLoTg_gDo",
        token_secret="jRSxlgyKVPkBHw2gDkooYz8YuPA"
    )
    
    client = Client(auth)
    
    params = {
        'sort': '2',
        #'sort': '1',
        'term': 'cold coffee',
        'lang': 'en',
        #'radius_filter': '10'
    }
    
    response = client.search(loc, **params)
    
    for i in range(response.total):
        print response.businesses[i].name
        print response.businesses[i].rating
        print response.businesses[i].location.city
        #print response.businesses[i].location.coordinate.latitude, response.businesses[i].location.coordinate.longitude
    	
    #client.search_by_coordinates(33.7629,	-84.4226, **params)

#def questions():
    
    
def main():
    
    # do autolocation
    
    # get location from user 
    print '"{}"'.format('Please enter your current location')
    loc = raw_input()
    
    yelp(loc)
    
    # ask the first main question
    print '"{}"'.format('How are you feeling: 1.hungry or 2.bored?')
    feeling = raw_input()
    
    if(feeling == '1'):
        
    else(feeling == '2'):
        
    

    
main()