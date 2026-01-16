import json
import requests

file=open("json_file.json","r+")

#json file ko line by line python nai rad kar paata
#lines=file.readline()


#json.loads ka use wahan hota hai jahan pe seedha json object ho, agar python object hoga toh usko json 
#convert karne k liye json.dumps method use karo


#File ko read karna hai toh load method ka use karo
#json_payload=json.loads(file)
print(type(file.read))
json_payload=json.dumps(file.read())

print(type(json_payload))
'''
for key in json_payload['details']:
    print(key)
'''


url="https://jsonplaceholder.typicode.com/comments"

response=requests.get(url)
#print(response.json())

json_data=json.dumps(response.json(), indent=4)
print(type(json_data))
#print(json_data)

json_data_dict=json.loads(json_data)
#print(json_data_dict)

print(json_data_dict[1]['id'])

print(len(json_data_dict))


for key in range(0,len(json_data_dict)):
    print(str(json_data_dict[key]['id'])+"::"+json_data_dict[key]['name'])


import csv

header_fields=['id','name']
file_name="information.csv"

csv_file=open(file_name,"w",newline="")
#Ek writer object banate hain jo ki json response se mili dictionary
#ko csv mein ghusedega,
########################################################
writer=csv.DictWriter(csv_file,restval='MISSING',fieldnames=header_fields,extrasaction='ignore')
#########################################################

#Below code will write the header field
writer.writeheader()

#writing dictionary values in csv
writer.writerows(json_data_dict)

'''
for lines in lines:
    print(line)
'''