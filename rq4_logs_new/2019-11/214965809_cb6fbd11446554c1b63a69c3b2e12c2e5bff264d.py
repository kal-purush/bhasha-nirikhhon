import requests
import json 

def Twitter():
    #Get Request from Personal Twitter Crawler
    TwitterResponse = requests.get('https://breadplaza.com/api/public/index.php/api/ntusg')
    #Format Data into JSOn
    twitter = TwitterResponse.json()
    #Empty Dictionary to holds filtered data response from twitter
    twitterDictionary = {}
    #Create Empty List for Iteration in stall.html
    twitterPostList = []
    for i in range(len(twitter)):
        #Create Custom Dictonary for twitter response
        twitterDictionary["Name"] = twitter[i]["user"]["name"]
        twitterDictionary["Image"] = twitter[i]["user"]["profile_banner_url"]
        twitterDictionary["Url"] = twitter[i]["entities"]["urls"][0]["url"]
        twitterDictionary["Posted"] = twitter[i]["created_at"][:-14]
        twitterDictionary["Text"] = twitter[i]["text"]

        #Append Dictonary to list
        twitterPostList.append(twitterDictionary.copy())
    #Pass Twitter News post to app.py (Flask App)
    return twitterPostList
    
    
    