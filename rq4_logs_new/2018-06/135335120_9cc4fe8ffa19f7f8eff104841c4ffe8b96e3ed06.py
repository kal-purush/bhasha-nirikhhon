# app/models.py

class _request:
    def __init__(self, request_id, request_type, request_title, request_body ):
        self.request_id = request_id
        self.request_type = request_type
        self.request_title = request_title
        self.request_body = request_body
    
    def get_id(self):
        return self.request_id
    
    def get_type(self):
        return self.request_type

    def get_title(self):
        return self.request_title
    
    def get_body(self):
        return self.request_body