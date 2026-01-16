import re

class EmailExtractor:
    def __init__(self, email=None):
        #zadeklarowanie zmiennych
            self.email = email
            self.name = ""
            self.surname = ""
            self.isStudent = ""
            self.isMale = ""
            self.regex = r"(?P<name>[a-z]+)\.(?P<surname>[a-z]+)[0-9]*@(?P<is_student>(student.)?)(wat\.edu\.pl)"

    def is_student(self)->bool: #funkcja porównująca czy jest studentem
        test = re.match(self.regex, self.email) #funkcja match porównuje
        if test.group(3):
             return True
        else:
            return False
    def is_male(self)->bool: #funkcja porównująca czy jest mezczyzna
        test = re.match(self.regex, self.email)
        name = test.group(1)
        if name[-1]=='a':
            return False
        else:
            return True
    def get_name(self)->str: #funkcja porównująca imiona
        test = re.match(self.regex, self.email)
        if test:
            x = test.group(1)
            x = x.capitalize()
            return x
    def get_surname(self)->str: #funkcja porównująca nazwiska
        test = re.match(self.regex, self.email)
        if test:
            x = test.group(2)
            x = x.capitalize()
            return x
