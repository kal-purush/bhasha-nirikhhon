from pymongo import MongoClient
from flask import Flask, request, jsonify

app = Flask(__name__)
# MongoDB connection URI
mongo_url = "mongodb+srv://21uai019:O2OLlukOZ8POqvUg@megaproject-snapattend.tuf5i1s.mongodb.net/"

# Connect to MongoDB
client = MongoClient(mongo_url)

# Access the database

# Access the 'faculty' collection

# Fetch all documents

# Print each document






@app.route('/get_department', methods=['GET'])
def get_department():
    db = client["SA-General"]

    faculty_collection = db['faculty']
    email = request.args.get('email')
    faculty_docs = faculty_collection.find_one({"email":"skshirgave@gmail.com"})
    # print("FY_"+dept)
    dept= faculty_docs['department']
    return jsonify({"department": f"FY_{dept}"})




if __name__ == '__main__':
    app.run(debug=True)


