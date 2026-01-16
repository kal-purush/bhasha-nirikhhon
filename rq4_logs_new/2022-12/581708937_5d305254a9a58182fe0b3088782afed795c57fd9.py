import firebase_admin
from firebase_admin import credentials, firestore

cred = credentials.Certificate("KEY.json")
firebase_admin.initialize_app(cred)

db = firestore.client()

docs = [
{
  "Kcal": "無提供",
  "food": "薯條(中)",
  "price": "50"
},
{
  "Kcal": "183",
  "food": "酥嫩鷄翅(2塊)",
  "price": "49"
},
{
  "Kcal": "93",
  "food": "玉米湯",
  "price": "40"
},
{
  "Kcal": "無提供",
  "food": "OREO冰炫風",
  "price": "55"
}

]
#doc_ref = db.collection("期末專案-麥當勞").document("1111")
#doc_ref.set(doc)

collection_ref = db.collection("期末專案-麥當勞")
for doc in docs:
  collection_ref.add(doc)
