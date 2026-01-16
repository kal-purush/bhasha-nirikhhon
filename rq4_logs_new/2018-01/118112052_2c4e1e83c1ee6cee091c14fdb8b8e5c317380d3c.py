from flask import Flask
import requests
import csv

app = Flask(__name__)

# @app.route("/sendMessage")
# def sendMessage():
# 	pass()


def parseCsv():
	with open("data.csv", "rb") as csvFile:
		csvReader = csv.reader(csvFile, delimiter=',')
		for row in csvReader:
			print (row)
	return "Success"

@app.route("/run")
def run():
	r = requests.get('https://oyorooms.freshdesk.com/reports/scheduled_exports/6374371511853668/download_file.json', auth=('29ecd4SXzdbNYsmDJp0Z', 'X'))
	csvURL = ((r.json())["export"])["url"]
	csvData = requests.get(csvURL)
	with open("data.csv", "w") as csvFile:
		csvFile.write(csvData.content)
	parseCsv()