import json
import csv
from collections import defaultdict

# places = {'name': 'What do you want to do?', 'children':[]}

d = {}

with open("yelp.csv", 'r') as data_file:
	data = csv.DictReader(data_file, delimiter=",")
	for row in data:
		type_item = d.get(row['type'], dict())
		cat_item = type_item.get(row['category'], list())
		cat_item.append({'name':row['name'], 'rating':row['rating']})
		type_item[row['category']] = cat_item
		d[row['type']] = type_item

def merge_two_dicts(x, y):
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z

c = defaultdict(dict)

for key1 in d:
	if key1 == 'food':
		c[key1] = d[key1]
	else:
		c['entertainment'] = merge_two_dicts(c['entertainment'], d[key1])

g = {}

for key1 in c:
	n_1 = key1.capitalize()
	g[n_1] = {}

	for key2 in c[key1].keys():
		if key2 == 'newamerican':
			n_2 = 'New American'
		elif key2 == 'indpak':
			n_2 = 'Indian/Pakistan'
		elif key2 == 'ethnicmarkets':
			n_2 = "Ethnic Markets"
		else:
			n_2 = key2.capitalize()

		g[n_1][n_2] = c[key1][key2]

json_dict = {'name': 'What do you want to do?', 'children':[]}
type_dict = defaultdict(list)

for i in g.keys():
	for j in g[i].keys():
		type_dict[i].append({'name':j, 'children':g[i][j]})

for i in type_dict.keys():
	json_dict['children'].append({'name':i, 'children':type_dict[i]})

with open('result.json', 'w') as fp:
    json.dump(json_dict, fp)