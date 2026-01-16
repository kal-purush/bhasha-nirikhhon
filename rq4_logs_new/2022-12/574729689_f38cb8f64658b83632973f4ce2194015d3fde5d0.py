import time
import os
import logging
import sys
import pickle
import pulsar
import openai
import pandas as pd
import matplotlib.pyplot as plt


openai.api_key = "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"
pickled_model = pickle.load(open('/Users/justincun/DSC180A/project-info/starter-stream-py/src/astra_streaming/logistic_regression_model.pkl', 'rb'))        

def encode(input_text):
    response = openai.Embedding.create(
        input = input_text,
        model="text-similarity-davinci-001"
    )
    return response['data'][0]['embedding']

def classify_text(input_text):
    max_id = pickled_model.predict_proba([encode(input_text)]).argmax()
    return pickled_model.classes_[max_id]


class Consumer(object):
    """
    Create a pulsar consumer that consumes tweets to a topic
    """
    def __init__(self):
        self.token = os.getenv("ASTRA_STREAMING_TOKEN")
        self.service_url = os.getenv("ASTRA_STREAMING_URL")
        self.subscription =  "persistent://twitter-test-1/default/preprocess-topic"
        self.client = pulsar.Client(self.service_url,
                                    authentication=pulsar.AuthenticationToken(self.token))
        self.consumer = self.client.subscribe(self.subscription, 'my-subscription')
        self.labels = []
        self.tweets = []

    def read_messages(self):
        """
        Create and send random messages
        """
        while True:
            try:
                # Log Tweet and Sentiment on terminal
                msg = self.consumer.receive(2000)
                tweet = str(msg.data(), 'UTF-8')
                sentiment = classify_text(tweet)
                self.labels.append(sentiment)
                self.tweets.append(tweet)
                logging.info("Tweet Classified! '{}', sentiment='{}' \n".format(tweet, sentiment))
                
                # Output DataFrame and Barchart
                if len(self.labels) == 3000:
                    df = pd.DataFrame({'Tweet': self.tweets, 'Sentiment': self.labels})
                    values = df['Sentiment'].value_counts()
                    values.plot(kind='bar',  figsize=(8, 6), width=0.6, color=['#2BAC6B', '#EE9926', '#959997'])
                    plt.title('Sentiment Analysis')
                    plt.ylabel('Counts')
                    plt.xticks(rotation=0)
                    plt.xlabel('Sentiment')
                    plt.savefig('/Users/justincun/DSC180A/project-info/starter-stream-py/src/astra_streaming/results_chart.png')
                    df.to_csv('/Users/justincun/DSC180A/project-info/starter-stream-py/src/astra_streaming/results_df.csv')  
                    print(df)
                    print(values)


                # Acknowledging the message to remove from message backlog
                self.consumer.acknowledge(msg)
            except:
                logging.info("Still waiting for a message...");
            time.sleep(1)
        
    

        
def read_messages():
    """
    Create an instance of the consumer and
    Fire it up to read messages until the program is terminated
    """
    consumer = Consumer()
    consumer.read_messages()
    consumer.client.close()


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO)
    read_messages()