'''this is the most complete version of what I want'''
import mysql.connector
import urllib.parse
import urllib.request
import json
import datetime

api_key = "9225175d64d542b98b0172830181712"

mydb = mysql.connector.connect(
	host= "localhost",
	user="root",
	password="root",
	database="ski_report")


mycursor = mydb.cursor(dictionary=True)

mycursor.execute("""SELECT * FROM resorts ;""")
myresult = mycursor.fetchall()


'''this will take gps points and auto generate an api request''' 
def snowfall_from(location):
	base_url = 'https://api.worldweatheronline.com/premium/v1/ski.ashx?'
	args = {'q': location, 'num_of_days': 1, 'key': api_key, 'format': 'json'}
	url = base_url + urllib.parse.urlencode(args)
	contents = urllib.request.urlopen(url).read()
	json_data = json.loads(contents)['data']
	weather = json_data['weather'][0]
	snowfall = weather['totalSnowfall_cm']	
	return snowfall

data = []

for var in myresult:
	resort_id = var['id']
	date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
	snowfall = snowfall_from(var['location'])
	data.append({"resort_id" : resort_id, "date" : date, "snowfall" : snowfall})
print(data)

for row in data:
	query = (
		f"INSERT INTO Reports (resort_id, date, snowfall, price)\n"
		f"VALUES ({row['resort_id']}, '{row['date']}', {row['snowfall']}, 0.0)"
	)
	mycursor.execute(query)
	print(query)

mydb.commit()

mydb.close()