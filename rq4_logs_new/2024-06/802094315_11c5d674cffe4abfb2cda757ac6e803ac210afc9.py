class MessageStruct:
    def __init__(self, text_message, uid, time_send, deep_link):
        self.deep_link = deep_link
        self.time_send = time_send
        self.uid = uid
        self.text_message = text_message

    def to_json(self):
        return {
            "uid": self.uid,
            "deep_link": self.deep_link,
            "time_send": self.time_send,
            "text_message": self.text_message
        }