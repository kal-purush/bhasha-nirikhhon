import http.client
import json

class BMIAPI:
    
    def __init__(self, height, weight, age, gender):
        self.height = height
        self.weight = weight
        self.age = age
        self.gender = gender

    def getage(self):
        return self.age

    def getheight(self):
        return self.height

    def getweight(self):
        return self.weight
    
    def getgender(self):
        return self.gender

    def setage(self, age):
        self.age = age

    def setheight(self, height):
        self.height = height

    def setweight(self, weight):
        self.weight = weight

    def setgender(self, gender):
        self.gender = gender

    def DailyCalorieIntake(self):
        conn = http.client.HTTPSConnection("fitness-calculator.p.rapidapi.com")

        headers = {
            'x-rapidapi-host': "fitness-calculator.p.rapidapi.com",
            'x-rapidapi-key': "a915dc6668msha39736cfbc92288p1c5e44jsn0ac0ad17adbe"
            }

        conn.request("GET", "/dailycalory?heigth="+str(self.height)+"&age="+str(self.age)+"&gender="+self.gender+"&weigth="+str(self.weight), headers=headers)

        res = conn.getresponse()
        data = res.read()
        data_str = data.decode("utf-8")
        data_dic = json.loads(data_str)
        print(type(data_dic))
        print(data_dic)

    def BodyMassIndex(self):
        conn = http.client.HTTPSConnection("fitness-calculator.p.rapidapi.com")

        headers = {
            'x-rapidapi-host': "fitness-calculator.p.rapidapi.com",
            'x-rapidapi-key': "a915dc6668msha39736cfbc92288p1c5e44jsn0ac0ad17adbe"
            }

        conn.request("GET", "/bmi?age="+str(self.age)+"&height="+str(self.height)+"&weight="+str(self.weight), headers=headers)

        res = conn.getresponse()
        data = res.read()
        data_str = data.decode("utf-8")
        data_dic = json.loads(data_str)
        print(type(data_dic))
        print(data_dic)

    def MacroNutrients(self):
        conn = http.client.HTTPSConnection("fitness-calculator.p.rapidapi.com")

        headers = {
            'x-rapidapi-host': "fitness-calculator.p.rapidapi.com",
            'x-rapidapi-key': "a915dc6668msha39736cfbc92288p1c5e44jsn0ac0ad17adbe"
            }

        conn.request("GET", "/macros", headers=headers)

        res = conn.getresponse()
        data = res.read()
        data_str = data.decode("utf-8")
        data_dic = json.loads(data_str)
        print(type(data_dic))
        print(data_dic)

    def BodyFatPercentage(self): #maybe
        conn = http.client.HTTPSConnection("fitness-calculator.p.rapidapi.com")

        headers = {
            'x-rapidapi-host': "fitness-calculator.p.rapidapi.com",
            'x-rapidapi-key': "a915dc6668msha39736cfbc92288p1c5e44jsn0ac0ad17adbe"
            }

        conn.request("GET", "/bodyfat?waist=96&gender=male&neck=50&heigth=178&hip=92&age=25&weigth=70", headers=headers)

        res = conn.getresponse()
        data = res.read()
        data_str = data.decode("utf-8")
        data_dic = json.loads(data_str)
        print(type(data_dic))
        print(data_dic)
        
    def IdealWeight(self):
        conn = http.client.HTTPSConnection("fitness-calculator.p.rapidapi.com")

        headers = {
            'x-rapidapi-host': "fitness-calculator.p.rapidapi.com",
            'x-rapidapi-key': "a915dc6668msha39736cfbc92288p1c5e44jsn0ac0ad17adbe"
            }

        conn.request("GET", "/idealweight?weight="+str(self.weight)+"&gender="+self.gender+"&height="+str(self.height), headers=headers)

        res = conn.getresponse()
        data = res.read()
        data_str = data.decode("utf-8")
        data_dic = json.loads(data_str)
        print(type(data_dic))
        print(data_dic)