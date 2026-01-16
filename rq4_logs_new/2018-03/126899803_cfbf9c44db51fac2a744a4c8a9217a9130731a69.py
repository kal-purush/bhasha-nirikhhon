'''
Created on Mar 26, 2018

@author: siva
'''

from flask import Flask
app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello World!"

@app.route("/denomination/<int:dollar_value>")
def denomination(dollar_value):

    number_of_coins = 0;
    
    denominations = {}
    
    leftover = 0;
    
    denominations["100"] = dollar_value/100;
    leftover = dollar_value%100;
    
    denominations["20"] = leftover/20;
    leftover = leftover%20;
    
    denominations["5"] = leftover/5;
    leftover = leftover%5;
    
    denominations["1"] = leftover;
    
    return "Total Number of Coins : " + str(denominations["100"] + denominations["20"] + denominations["5"] + denominations["1"])


if __name__ == '__main__':
    app.run()