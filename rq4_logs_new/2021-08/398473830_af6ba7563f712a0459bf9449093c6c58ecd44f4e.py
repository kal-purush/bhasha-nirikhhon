import requests
import csv
#https://diemthi.vnanet.vn/Home/SearchBySobaodanh?code=01000002&nam=2021
URL = 'https://diemthi.vnanet.vn/Home/SearchBySobaodanh'

start = 1000002
end = 1000003
header = ['Code', 'Toan', 'NguVan', 'NgoaiNgu', 'VatLi', 'HoaHoc', 'SinhHoc', 'KHTN', 'DiaLi', 'LichSu', 'GDCD', 'KHXH']

with open('clean_data.csv', 'w', encoding='utf-8', newline='') as f:
	writer = csv.DictWriter(f, fieldnames=header)
	writer.writeheader()
	for i in range(start, end):
		sbd = '0' + str(i)
		payload = {'code': sbd, 'nam': '2021'}
		response = requests.get(URL, params=payload)
		js = response.json()

		#js['result'][0] is a dictionary since js['result'] is a list having 01 element which is a dictionary
		if len(js['result']):
			result_dict = js['result'][0]
			#remove unnessesary items
			keys_to_remove = ['CityCode', 'CityArea', 'ResultGroup','Result']
			for key in keys_to_remove:
				result_dict.pop(key)

			#write data into a csv file
			
			data = [result_dict]
			for x, y in data[0].items():
				if y == '':
					data[0][x] = '-1'
				writer.writerows(data)
		else:
			new_dict = {}
			for key in header:
				if key == 'Code':
					new_dict.update({key: sbd})
				else:
					new_dict.update({key: '-1'})
			data = [new_dict]

			writer.writerows(data)