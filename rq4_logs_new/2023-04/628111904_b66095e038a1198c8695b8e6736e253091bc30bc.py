##5 * * * * /home/aspera/my_script.sh

from twitch_listener import utils
import argparse
import json
import sqlite3
import pandas as pd
import numpy as np
import openai


class sentimentAnalyzer:
    def __init__(self):
        self.chat_df = pd.DataFrame()
        self.OPENAI_API_KEY = ""
        
    def readDatabase(self):
        # ************************************************************
        # Read the token       
        # ************************************************************
        with open('config.json', 'r') as file_to_read:
            json_data = json.load(file_to_read)
            self.OPENAI_API_KEY = json_data["OPENAI_API_KEY"]
        
        # Create a SQL connection to our SQLite database
        conn = sqlite3.connect('../data/db.sqlite3',isolation_level=None)

        # Load the data into a DataFrame
        self.chat_df = pd.read_sql('select date,stream_datetime, stream_length, username, message_text, channel_name,stream_topic,stream_title, chatter_count, viewer_count, follower_count, subscriber_count, stream_date, stream_id from chats_table_demo', conn)

        # Be sure to close the connection
        conn.close()    
        
    def emote_lookup(self):
        # ************************************************************
        #  Read Emote fact file , convert it to json and save it as emotes.json
        # ************************************************************
        csvFilePath = '../data/facttable/EmoteFactTable.csv'
        jsonFilePath = '../data/emotes.json' 
        
        utils.emote_to_json(csvFilePath,jsonFilePath)
        emote_json = utils.reload_json(jsonFilePath)
    
        self.chat_df['new_message_text'] = self.chat_df['message_text'].map(lambda x: utils.replace_emoticons(x, emote_json))
        self.loadIntoDatabase()
    
    def sentiment_score(self):
        # ************************************************************
        # Score sentiment of chat messages.
        # ************************************************************
        batch_size = 80
        openai.api_key =  self.OPENAI_API_KEY
        print("Print first line of new message text "+ self.chat_df['new_message_text'][0])
        total_rows = self.chat_df.shape[0]
        print("chat_len: "+ str(total_rows))

        self.chat_df['general_sentiment'] =''
        self.chat_df['specific_sentiment'] =''
        
        # Define a function to generate an embedding for a text string using the OpenAI API
        def get_embedding(text, model="text-embedding-ada-002"):
            # Replace any newline characters in the text with spaces
            text = text.replace("\n", " ")
            # Call the OpenAI Embedding API to get an embedding for the text
            embedding = openai.Embedding.create(input=[text], model=model)['data'][0]['embedding']
            # Convert the embedding from a string to a numpy array
            embedding = np.fromstring(embedding, sep=" ")
            return embedding
        
        # Convert the string representations of the embeddings to numpy arrays
        self.chat_df['ada_embedding'] = self.chat_df.ada_embedding.apply(eval).apply(np.array)

        # loop through all the rows
        for i in range(0, total_rows, batch_size):
            # get the 1000 chats from the data frame
            chat_df_batch = self.chat_df.iloc[i:i+batch_size]
            chat_batch_list = chat_df_batch['new_message_text'].tolist()
            
            j = 1
            chat_batch_str=''
            for chat in chat_batch_list:
                chat_batch_str = chat_batch_str + str(j) +'.'+'"'+ chat +'"'+'\n'
                j=j+1
        
            response_genlist = utils.general_sentiments(chat_batch_str)
            response_spelist = utils.specific_sentiments(chat_batch_str) 

         
            #chat_df.loc[i:i+batch_size, 'general_sentiment'] = response_genlist
            self.chat_df['general_sentiment'].iloc[i:i+batch_size] = response_genlist
            print("**********")
            print("i",i)
            print("i+batch_size", i+batch_size)
            print(response_spelist)
            print("length ",len(response_spelist))
            self.chat_df['specific_sentiment'].iloc[i:i+batch_size] = response_spelist
          
            print("Completed: "+ str(i) + " out of "+ str(total_rows))
            
    def loadIntoDatabase(self):
        # ************************************************************
        # Load the sentiment scores into the database
        # ************************************************************
        # Create a SQL connection to our SQLite database
        conn = sqlite3.connect('../data/chat_table.sqlite3',isolation_level=None)
        self.chat_df.to_sql('chats', conn, index = False)
        #Commit the change
        conn.commit()
        # Be sure to close the connection
        conn.close()
        
    def main(self):
        self.readDatabase()
        self.emote_lookup()
        self.sentiment_score()
        self.loadSentimentIntoDatabase()
        
if __name__ == "__main__":
    sentimentAnalyzer = sentimentAnalyzer()
    sentimentAnalyzer.main()
    