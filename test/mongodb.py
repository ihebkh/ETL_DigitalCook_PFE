import unittest
from pymongo import MongoClient

def get_all_collections():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = client["PowerBi"]
    
    collections = [
        "parrainageinfluencers", "dossiers", "entitecomercals", "filieres", 
        "secteurdactivities", "centrefinancements", "formations", 
        "commercialinfos", "agenceinfos", "factures", "products", 
        "offredetudes", "frontusers", "parrainages", "users", 
        "entreprises", "universities", "offredemplois", "privileges", 
        "influencerinfos", "profiles", "businessfinderinfos"
    ]
    
    existing_collections = []
    for collection in collections:
        if collection in db.list_collection_names():
            existing_collections.append(collection)
    
    client.close()
    return existing_collections

class TestMongoDBCollections(unittest.TestCase):

    def test_collections_exist(self):
        collections = get_all_collections()
        expected_collections = [
            "parrainageinfluencers", "dossiers", "entitecomercals", "filieres", 
            "secteurdactivities", "centrefinancements", "formations", 
            "commercialinfos", "agenceinfos", "factures", "products", 
            "offredetudes", "frontusers", "parrainages", "users", 
            "entreprises", "universities", "offredemplois", "privileges", 
            "influencerinfos", "profiles", "businessfinderinfos"
        ]
        
        for collection in expected_collections:
            assert collection in collections, f"La collection {collection} est manquante dans la base de données"

    def test_connection(self):
        try:
            client, *_ = get_all_collections()
            self.assertTrue(client, "La connexion MongoDB a échoué")
        except Exception as e:
            self.fail(f"Erreur de connexion MongoDB : {e}")

if __name__ == "__main__":
    unittest.main(argv=[''], exit=False)
