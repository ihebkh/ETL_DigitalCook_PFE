from pymongo import MongoClient
from pymongo.collection import Collection

def get_mongo_collections():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = client["PowerBi"]
    return client, db["offredemplois"], db["frontusers"], db["entreprises"]

def main():
    try:
        client, offres, users, entreprises = get_mongo_collections()

        assert isinstance(offres, Collection), "offredemplois n'est pas une collection MongoDB"
        assert isinstance(users, Collection), "frontusers n'est pas une collection MongoDB"
        assert isinstance(entreprises, Collection), "entreprises n'est pas une collection MongoDB"

        print("✅ Connexion MongoDB réussie")
        print("🗂️ Collections récupérées :")
        print(f" - offres       : {offres.name}")
        print(f" - utilisateurs : {users.name}")
        print(f" - entreprises  : {entreprises.name}")

    except AssertionError as ae:
        print("❌ Erreur d'assertion :", ae)

    except Exception as e:
        print("❌ Erreur lors de la connexion à MongoDB :", e)

if __name__ == "__main__":
    main()
