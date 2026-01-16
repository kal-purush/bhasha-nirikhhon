import csv

def csv_reader(file_obj):
	reader = csv.DictReader(file_obj, delimiter=',')
	print(reader.fieldnames)

	file = open('[en] yolanda nov 9.csv', 'wt')
	writer = csv.DictWriter(file,delimiter=',',lineterminator='\n',fieldnames=next(reader))

	line2 = reader.fieldnames

	writer.writerow({
					'user': line2[2],
					'language': line2[4],
					'user location': line2[8],
					'lat': line2[5],
					'lng': line2[6],
					'user utc offset': line2[7],
					'text': line2[3],
					'user time zone': line2[9],
					'id': line2[0],
					'created_at': line2[1]
					})

	isFirst = True
	for line in reader:
		if line['language'] == 'en':
				writer.writerow({
					'user': line['user'],
					'language': line['language'],
					'user location': line['user location'],
					'lat': line['lat'],
					'lng': line['lng'],
					'user utc offset': line['user utc offset'],
					'text': line['text'],
					'user time zone': line['user time zone'],
					'id': line['id'],
					'created_at': line['created_at']
					})

csv_path = "yolanda nov 9.csv"
with open(csv_path, "rt") as f_obj:
	csv_reader(f_obj)