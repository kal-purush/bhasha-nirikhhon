from pymongo import MongoClient

connectionURL = "mongodb+srv://christopherkim511:realEstate@cluster0-owylp.mongodb.net/test?retryWrites=true&w=majority"
client = MongoClient(connectionURL)
quotes_db = client["RealEstate"]
quotes_col = quotes_db["PropertyTaxAssessments11096"]

query1 = {"address.oneLine":"59 WANSER AVE # 69, INWOOD, NY 11096"}

doc = quotes_col.find(query1)
for x in doc:
	print(x)