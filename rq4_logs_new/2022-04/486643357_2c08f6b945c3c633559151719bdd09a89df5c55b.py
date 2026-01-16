import requests
from bs4 import BeautifulSoup as BS
import pymongo as mongo


goat = 0
time= ""
list = []
keepGoing = True

client = mongo.MongoClient("mongodb://localhost:27017")
mydb = client["Scraper"]
mycol = mydb["Data"]

while keepGoing == True:
    
    request = requests.get("https://www.blockchain.com/btc/unconfirmed-transactions")
    only_text = request.text
    soup = BS(only_text, "html.parser")
    data = soup.find_all("div", {"class" : "sc-1g6z4xm-0 hXyplo"})

    for t in data:
        text = t.text
        text = text.replace("Hash","")
        text = text.replace("Time"," ")
        text = text.replace("Amount","")
        text = text.replace("(BTC)","")
        text = text.replace("BTC","")
        text = text.replace(" (USD)","")
        text = text.split(" ")
        list.append(text)

    list.reverse()

    if len(time) == 0:
            time = text[1]

    for index in list:

        if index[1] == time:

            if float(index[2]) > goat:

                goat = float(index[2])

                highest = index[0]
                timeHighest = index[1]
                BTC = index[2]
                USD = index[3]

        if index[1] > time: 

            mydict = {"Hash": highest, "Time": timeHighest, "BTC": str(BTC) + " BTC", "USD": USD}

            y = mycol.insert_one(mydict)

            time = index[1]
            goat = 0 
            list = [] 

            y.inserted_id   