'''This module represents the question entity'''
from datetime import datetime
from .db import get_database
from psycopg2.extras import RealDictCursor

from app.api.v2.models.meetup_model import MeetupModel

class QuestionModel:
    '''Entity representation for a question'''
    def __init__(self):
        '''initialize db connection'''
        self.conn = get_database()

    def add_question(self, questions):
        '''Add a new question to the data store'''
        return self.conn.cursor().execute("""
            INSERT INTO questions (title, body, meetup_id, created_by) VALUES (%(title)s, %(body)s, %(meetup_id)s, %(created_by)s);""",questions)

    def get_all_questions_by_meetup_id(self,meetup_id):
        '''Fetch all meetup questions'''
        query_string = 'SELECT * FROM questions WHERE meetup_id = %s;'
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query_string, (meetup_id,))
        return cursor.fetchall()

    def update_upvotes(self):
        '''Increment votes'''
        query_string = 'update questions set upvotes = upvotes +1;'
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query_string,)
        self.conn.commit()
        return self.conn.commit('''update questions set upvotes = upvotes +1;''')