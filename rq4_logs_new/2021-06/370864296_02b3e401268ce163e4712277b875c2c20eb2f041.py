import csv

def main():
	products_file = "E:/GitHub/2021-cs111-programming-with-functions/w09-text-files/prove-csv-grocery-program/products.csv"
	products = read_products(products_file)

	print('Products')
	for number, product in products.items():
		print(number, product)

	requests_file = "E:/GitHub/2021-cs111-programming-with-functions/w09-text-files/prove-csv-grocery-program/request.csv"
	process_request(requests_file, products_file)


def read_products(file):
	print()
	products = {}
	with open(file, "rt") as infile:
		reader = csv.reader(infile)
		next(reader)
		for line in reader:
			product_number = line[0]
			product_name = line[1]
			product_price = line[2]
			products[product_number] = [product_name, product_price]
	
	return products
	

def process_request(requests_file, products_file):
	product_dic = read_products(products_file)
	with open(requests_file, "rt") as infile:
		reader = csv.reader(infile)
		next(reader)
		print('Requested items:')
		for line in reader:
			product_number = line[0]
			product_quantity = line[1]
			if product_number in product_dic.keys():
				product = product_dic[product_number]
				name = product[0]
				price = product[1]
				print(name + ': ' + product_quantity + ' @ ' + price)


if __name__ == "__main__":
	main()