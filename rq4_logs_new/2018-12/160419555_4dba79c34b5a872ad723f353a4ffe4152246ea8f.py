import json
import requests 
from subprocess import call

with open("test.json") as data_file:
    data = [json.loads(line) for line in data_file]

    #number = next(data_file).strip()
# call('ls')

response = requests.get("https://jsonplaceholder.typicode.com/todos")
todos = json.loads(response.text)

print json.dumps(todos, sort_keys=True,
				indent=4, separators=(',', ': '))



