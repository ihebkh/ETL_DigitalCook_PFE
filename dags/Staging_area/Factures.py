import json
from pymongo import MongoClient
from bson import ObjectId

mongo_uri = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
client = MongoClient(mongo_uri)
db = client["PowerBi"]
collection = db["factures"]

file_path = r"C:\Users\khmir\Desktop\ETL_DigitalCook_PFE\data\factures.json"
with open(file_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

inserted_count = 0
for doc in data:
    print("Traitement du document...")
    if "_id" in doc:
        try:
            if isinstance(doc["_id"], dict) and "$oid" in doc["_id"]:
                doc["_id"] = ObjectId(doc["_id"]["$oid"])
            elif isinstance(doc["_id"], str):
                doc["_id"] = ObjectId(doc["_id"])
            if not collection.find_one({"_id": doc["_id"]}):
                collection.insert_one(doc)
                inserted_count += 1
                print(f"Document inséré : {doc['_id']}")
            else:
                print(f"Document déjà existant : {doc['_id']}")
        except Exception as e:
            print(f"Erreur avec le document {doc.get('_id', 'inconnu')}: {e}")
    else:
        print("Document sans champ _id :", doc)

print(f"{inserted_count} nouvelles factures insérées.")
