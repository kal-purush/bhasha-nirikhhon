from collections import defaultdict

# slow way in handling dicts with missing keys
dict1 = {}

countries = ['us', 'cn', 'in']
values = ["value", "value2"]

for country in countries:
    for value in values:
        # one search for country
        if country not in dict1:
            # possible third search for country, if it doesn't exist to add []
            dict1[country] = []
        # second search for country to append value
        dict1[country].append(value)
print(dict1)



# better way to handle dicts with missing keys
dict2 = {}

for country in countries:
    for value in values:
        # only one search for country
        dict2.setdefault(country, []).append(value)
print(dict2)


# another way to handle dicts with missing keys - use defaultdict from collections
dict3 = defaultdict(list)
for country in countries:
    for value in values:
        dict3[country].append(value)
print(dict2)