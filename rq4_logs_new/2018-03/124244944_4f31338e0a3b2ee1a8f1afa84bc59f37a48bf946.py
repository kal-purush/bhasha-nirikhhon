from functools import reduce
from textblob import TextBlob

class User(object):
    """ classifies the attributes of the user and gets the overall sentiment of the users requests """
    def __init__(self, session, timestamp):

        self.log_list = [Request(session['chat'][log]) for log in session['chat']]
        print(self.log_list)
        self.sessionid = timestamp
        self.session = session

    def get_sentiment_overall(self):
        request = [req.get_sentiment() for req in self.log_list]
        sen_array = []
        for sen in request:
            sen_array.append(sen[1])

        if len(sen_array) > 1:
            mean_sen = reduce((lambda x,y: x + y), sen_array) / len(sen_array)
            return mean_sen
        else:
            return sen_array


class Request(object):

    def __init__(self, log):
        self.id = id
        self.query = log['request']['query']
        self.client = log['request']['query']['client']
        self.location = log['request']['query']['location']
        self.notmatched = log['request']['query']['notmatched']
        self.productid = log['request']['query']['productid']
        self.product_key_words = log['request']['query']['product_key_words']
        self.question_key_words = log['request']['query']['question_key_words']
        self.question_words = log['request']['query']['question_words']
        self.verb = log['request']['query']['verb']
        self.sentiment = log['request']['sentiment']
        self.sentiment_type = log['request']['type']
        self.text = log['request']['text']
        self.response = log['response']['text']

    def get_sentiment(self):
        string = self.text
        polarity = TextBlob(string)
        pol = polarity.sentiment

        if pol[0] > 0:
            self.sentiment = pol[0]
            self.sentiment_type = 'pos'
        elif pol[0] == 0:
            self.sentiment = pol[0]
            self.sentiment_type = 'neu'
        else:
            self.sentiment = pol[0]
            self.sentiment_type = 'neg'

        return (self.sentiment_type, self.sentiment)