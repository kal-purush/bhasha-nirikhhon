import csv


with open("favourites.csv","r") as file:
    reader = csv.DictReader(file)
    count = {}
    for row in reader:
        lang = row["Language"]
        if lang in count:
            count[lang] += 1
        else:
            count[lang] = 1
for language in sorted(count, key = lambda x: count[x], reverse= True):
    print(language,count[language])