class States:

    COLLECTION_NAME = 'states'

    def __init__(self, core):
        self.db = core.db
        self.collection = self.db[self.COLLECTION_NAME]

        self.states = {}
        self.restore()

    def restore(self):
        states = list(self.collection.find())

        for state in states:
            self.states[state['chat_id']] = state['data']

    def push(self, chat_id, data):

        self.collection.update_one({'chat_id': chat_id}, {'$set': {'data': data}}, upsert=True)

        if chat_id not in self.states:
            self.states[chat_id] = [data]
            return

        self.states[chat_id].append(data)

    def pop(self, chat_id):
        if chat_id not in self.states:
            return None

        data = self.states[chat_id].pop()

        self.collection.update_one({'chat_id': chat_id}, {'$set': {'data': data}})

        return data

    def reset(self, chat_id):

        self.collection.remove_one({'chat_id': chat_id})

        if chat_id in self.states:
            del self.states[chat_id]

    def get(self, chat_id):
        if chat_id in self.states:
            last_index = len(self.states[chat_id]) - 1
            return self.states[chat_id][last_index]

        return None