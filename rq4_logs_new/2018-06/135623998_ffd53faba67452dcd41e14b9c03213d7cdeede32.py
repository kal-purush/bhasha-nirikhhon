class comment:
    def __init__(self,body,author,timestamp):
        self.body=body
        self.author=author
        self.timestamp=timestamp

    def getBody(self):
        return self.body;
    def getAuthor(self):
        return self.author;
    def getTimestamp(self):
        return self.timestamp;
    def setBody(self,body):
        self.body=body;
    def setAuthor(self,author):
        self.author=author;
    def setTimestamp(self,timestamp):
        self.timestamp=timestamp;
        
        
