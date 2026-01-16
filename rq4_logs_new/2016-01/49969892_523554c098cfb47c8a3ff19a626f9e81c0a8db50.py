import json
#data is the F5 JSON response to the query to https://localhost/mgmt/tm/security/dos/device-config/stats

def parser(data):
  JSON_Dict = {}
  for entry in data["entries"]:
	newdict = {}
	print json.dumps(entry, indent=4, sort_keys=True)
	print "=" * 70
	for stat in data["entries"][entry]['nestedStats']["entries"]:
		newdict[stat] = data["entries"][entry]['nestedStats']["entries"][stat]
	print newdict.pop('vectorName')['description']
	for key in newdict:
		print "%s \t %s " %(key.strip('common.'), newdict[key]['value'])
	print "=" * 70