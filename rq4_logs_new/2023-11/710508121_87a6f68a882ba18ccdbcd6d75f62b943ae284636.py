from better_profanity import profanity
import pymongo
import os
from dotenv import load_dotenv
load_dotenv()



def record_event_to_db():
    db_client = pymongo.MongoClient(os.getenv('mongo_string'))
    database = db_client.get_database("CodingChallengeBot")
    collection = database.get_collection("Users")
    print(collection)
    # Insert a document into the collection
    document ={
        "profanity_warnings":[
        { 
            "message_content":"test baddy word",
            "profanity_word":"baddy",
            "date":"11-18-2023"}
        ]
    }
    collection.insert_one(document)


class Hand_profanity:
    decision= False
    def __init__(self,discord_user_id,message_content):
        self.discord_user_id =discord_user_id
        self.severity_level= ""
        self.profanity_word =""
        self.message_content =message_content
        self.date = "" 
        

    
    def is_it_bad_word(self):
        self.descision = profanity.contains_profanity(self.message_content)
        return self.descision
        
    
    def warn_user(self, warning_message):
        # Check user DB check number of offenses based on some rules take action
        # create  private thread with warning message user. 
        pass
    
    def check_action_rule(self):
        # Check user DB check number of offenses based on some rules take action
        
        if 1:
            self.warn_user("Kicking you out because of ....")
        elif 2:
            self.warn_user("Kicking you out because of ....")
        elif 3:
            self.warn_user("Kicking you out because of ....")
           
        pass
    
    def add_to_probation(self):
        
        # warn user
        self.warn_user("Watchout ... buddy")

        pass
    
    def kick_user(self):
        # warn user
        self.warn_user("Kicking you out because of ....")
        pass
    
    