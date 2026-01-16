from datetime import date, datetime
import re

class Validation():
   def __init__(self, type, minLength, maxLength):
      self.type = type
      self.minLength = minLength
      self.maxLength = maxLength

class Validation_Tool():
   def __init__(self):
      self.validation = {}
      self.validation['first_name'] = Validation(type = 'string', minLength = 2, maxLength = 50)
      self.validation['middle_name'] = Validation(type = 'string', minLength = 2, maxLength = 50)
      self.validation['last_name'] = Validation(type = 'string', minLength = 2, maxLength = 50)
      self.validation['dob'] = Validation(type = 'date', minLength = 10, maxLength=10)
      self.validation['mobile'] = Validation(type = 'integer', minLength=10, maxLength =10)
      self.validation['email'] = Validation(type = 'string', minLength=5, maxLength=50)
      self.validation['age'] = Validation(type= 'integer', minLength=1, maxLength=3)
      self.validation['gender'] = Validation(type='string', minLength=4, maxLength=6)
      self.validation['city'] = Validation(type='string', minLength = 2, maxLength = 50)
      self.validation['state'] = Validation(type='string', minLength = 2, maxLength = 50)
      self.validation['pin'] = Validation(type='integer', minLength = 6, maxLength = 6)
      self.validation['aadhar'] = Validation(type='integer', minLength = 12, maxLength = 12)
      self.validation['blood_grp'] = Validation(type='string', minLength = 2, maxLength = 3)
      self.validation['dose_num'] = Validation(type='integer', minLength = 1, maxLength = 1)
      self.validation['prev_id'] = Validation(type='integer', minLength = 12, maxLength = 12)
      self.validation['prev_date'] = Validation(type='date', minLength = 10, maxLength = 10)
      self.validation['vaccine_name'] = Validation(type='string', minLength=7, maxLength=10)
      self.validation['preferred_date'] = Validation(type='date', minLength=10, maxLength=10)

      self.testcase = {}
      self.testcase['first_name'] = 'pass'
      self.testcase['middle_name'] = 'pass'
      self.testcase['last_name'] = 'pass'
      self.testcase['dob'] = 'pass'
      self.testcase['mobile'] = 'pass'
      self.testcase['email'] = 'pass'
      self.testcase['age'] = 'pass'
      self.testcase['gender'] = 'pass'
      self.testcase['city'] = 'pass'
      self.testcase['state'] = 'pass'
      self.testcase['pin'] = 'pass'
      self.testcase['aadhar'] = 'pass'
      self.testcase['blood_grp'] = 'pass'
      self.testcase['dose_num'] = 'pass'
      self.testcase['prev_id'] = 'pass'
      self.testcase['prev_date'] = 'pass'
      self.testcase['vaccine_name'] = 'pass'
      self.testcase['preferred_date'] = 'pass'
   



