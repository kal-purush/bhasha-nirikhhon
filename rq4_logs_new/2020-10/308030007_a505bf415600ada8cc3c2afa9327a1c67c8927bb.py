from pymongo import MongoClient

class MongoConnect():

    def save(self, json):
        try:
            client = MongoClient('localhost', 27017)
            database = client.cadastro_pessoa 
            new_register = database.register_person 
            id = new_register.insert_one(json).inserted_id
        except Exception as e:
            print("Problems saving record")
            print(json)
            print(e)