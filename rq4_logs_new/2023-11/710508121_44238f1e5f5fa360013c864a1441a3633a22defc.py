from better_profanity import profanity
import pymongo
from CSBotCommon import PalmApi
from CSBotCommon import Bot
import time
import datetime
import os
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

class ProfanityDB:
    
    def __init__(self,document_data):
        self.document_data=document_data     
        # set up DB connection
        self.db_client = pymongo.MongoClient(os.getenv('mongo_string'))
        self.database = self.db_client.get_database("CodingChallengeBot")
        self.collection = self.database.get_collection("Users")
        
    def get_collection_by_user(self, user_id):
        user_document = self.collection.find_one({"_id": user_id})
        if user_document:
            return user_document
        else:
            return False
    
    def record_event_to_db(self ):
        # Check if user exist in DB if so update record else add record
        result = self.collection.find_one({"_id": self.document_data['_id']})
        if result:
            self.update_record()       
        else:
            insert_result = self.collection.insert_one(self.document_data)
            if insert_result.inserted_id:
                print("adding new record")
                return True
            else:
                return False
        
    def update_record(self):
        user_filter = {"_id":self.document_data['_id']}
        # Specify the new "profanity_warnings" entry
        new_offense =self.document_data['profanity_warnings'][0]    
        updated_document = {"$push": {"profanity_warnings": new_offense}}
        # Update the document with a new "profanity_warnings" entry
        result = self.collection.update_one(
            user_filter,
            updated_document
        )   
        if result.modified_count > 0:
            print("updating record")
            return True
        else:
            return False
        
class Hand_profanity:
    decision= False
    def __init__(self,discord_user_id,message_content):
        self.discord_user_id =discord_user_id
        self.severity_level= ""
        self.profanity_words =[]
        self.message_content =message_content
        
        current_time = datetime.datetime.utcnow()
        current_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        self.date = current_time
        
    def determine_severity_level(self):
        df = pd.read_csv('profanityen.csv')

        max_severity = float('-inf')  # Start with negative infinity to ensure any value encountered will be greater

        for search_word in self.profanity_words:
            filtered_df = df[df['text'].str.contains(search_word, case=False, na=False)]

            if not filtered_df.empty:
                max_severity = max(max_severity, filtered_df['severity_rating'].max())

        if max_severity > float('-inf'):
            return max_severity
        else:
            return 0

    def is_it_bad_word(self):
        # The provided list will replace the default wordlist.
        df = pd.read_csv('profanityen.csv')
        custom_badwords = df['text'].tolist()

        profanity.load_censor_words(custom_badwords)
        self.descision = profanity.contains_profanity(self.message_content)
        if self.descision == True: 
            for word in self.message_content.split():
                if profanity.contains_profanity(word):
                    self.profanity_words.append(word)

            descision_onj ={
                "severity_level": self.determine_severity_level(),
                "profanity_words":self.profanity_words
            }
            document_data ={
                "_id":self.discord_user_id,
                "profanity_warnings":[      
                    {
                    "message_content" :self.message_content,
                    "profanity_words":self.profanity_words,
                    "severity_level":descision_onj["severity_level"],
                    "date":self.date,
                    
                    },    
                ],
            }
            # check rules
            self.check_action_rule(descision_onj)
            # Add record to DB
            ProfanityDB(document_data).record_event_to_db()

            return self.descision
        else:
            return False
                   
    def warn_user(self, warning_message_obj):
        try:
            bot = Bot(os.getenv('bot_key'))
            bot.create_private_thread(self.discord_user_id,os.getenv('challenge_channel_id'),warning_message_obj)
            bot.run()

        except:
            print("Failed to warn user")

    def check_action_rule(self,descision_onj):

        warning_message = f"""
Hello @{self.discord_user_id},

You are receiving this message because one of your recent messages on the server contained profanity. We want to maintain a positive and respectful environment for all members.
Please be mindful of our community guidelines and refrain from using inappropriate language. Continued violations may result in further action.
If you have any questions or concerns, feel free to reach out to the moderation team.

Thank you for your cooperation.

Best regards,
### Moderation Team
                        """
        severity = descision_onj['severity_level']
        coach_user =range(0,1)
        ban_user= range(1,2)
        kick_user =range(2,3)


        if severity in coach_user:
            warning_message_obj={
                "warning_type":"COACH",
                "warning_message":warning_message,
            }
            return self.warn_user(warning_message_obj)

        elif severity in ban_user:
            warning_message_obj={
                "warning_type":"BAN",
                "warning_message":warning_message,
            }
            return self.warn_user(warning_message_obj)

        elif severity in kick_user:
            warning_message_obj={
                "warning_type":"KICK",
                "warning_message":warning_message,
            }
            self.warn_user(warning_message_obj)
        else:
            return False
    
    def add_to_probation(self):
        
        # warn user
        self.warn_user("Watchout ... buddy")

        pass
    
    def kick_user(self):
        # warn user
        self.warn_user("Kicking you out because of ....")
        pass

# db = ProfanityDB(document1)
# user_collection = db.get_collection_by_user("gigi_gio")
# profanity_warnings =user_collection['profanity_warnings']

# handle =Hand_profanity("900420056947785739", "boobs 69 dickhead")
#
# print(handle.is_it_bad_word())