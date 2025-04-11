from pymongo import MongoClient

# Function to get the influencer_pk by matching the chargeDaffaire ObjectId
def get_influencer_pk_from_chargeDaffaire(chargeDaffaire_oid, dim_influencer_collection):
    if not chargeDaffaire_oid:
        return None
    influencer = dim_influencer_collection.find_one({"_id": chargeDaffaire_oid})
    if not influencer:
        return None
    return influencer.get("influencer_pk")

def display_id_and_chargeDaffaire_pk():
    # MongoDB connection setup
    mongo_client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = mongo_client["PowerBi"]

    # Access the required collections
    dossiers_collection = db["dossiers"]
    users_collection = db["frontusers"]  # Assuming frontusers collection holds the _id for users
    dim_influencer_collection = db["users"]

    # Iterate over each document in the dossiers collection
    dossiers = dossiers_collection.find()

    for doc in dossiers:
        # Extract the _id from the dossier
        document_id = doc.get("_id")

        # Match the _id with the users collection to get the user's chargeDaffaire
        user = users_collection.find_one({"_id": document_id})
        if not user or "chargeDaffaire" not in user:
            continue

        # Extract chargeDaffaire ObjectId from the user document
        chargeDaffaire_oid = user.get("chargeDaffaire")

        # Retrieve the influencer_pk by matching chargeDaffaire_oid with dim_influencer collection
        influencer_pk = get_influencer_pk_from_chargeDaffaire(chargeDaffaire_oid, dim_influencer_collection)

        # Print the _id from dossiers and the corresponding chargeDaffaire_pk (influencer_pk)
        print(f"_id (dossier): {document_id}, chargeDaffaire_pk: {influencer_pk}")

if __name__ == "__main__":
    display_id_and_chargeDaffaire_pk()
