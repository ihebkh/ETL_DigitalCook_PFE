import os
import json
import psycopg2
from pymongo import MongoClient

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"
    
    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return client, mongo_db, collection

def get_postgres_connection():
    return psycopg2.connect(dbname="DW_DigitalCook", user='postgres', password='admin', host='localhost', port='5432')

def reset_client_pk():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT setval('dim_client_client_pk_seq', 1, false)")
    conn.commit()
    cur.close()
    conn.close()

def list_mongodb_content():
    client, _, _ = get_mongodb_connection()
    databases = client.list_database_names()
    print("Bases de données MongoDB:")
    for db in databases:
        print(f"- {db}")
        mongo_db = client[db]
        collections = mongo_db.list_collection_names()
        print("  Collections:")
        for collection in collections:
            print(f"    - {collection}")
    client.close()

def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = list(collection.find({}, {"_id": 0}))  
    client.close()
    return mongo_data

def transform_data(mongo_data):
    seen_matricules = set()
    transformed_data = []
    for record in mongo_data:
        matricule = record.get("matricule")
        if matricule in seen_matricules:
            continue  
        seen_matricules.add(matricule)
        transformed_data.append({
            "matricule": matricule,
            "nom": record.get("nom"),
            "prenom": record.get("prenom"),
            "birthdate": record.get("profile", {}).get("birthDate"),
            "nationality": record.get("profile", {}).get("nationality"),
            "adresseDomicile": record.get("profile", {}).get("adresseDomicile"),
            "pays": record.get("profile", {}).get("pays"),
            "situation": record.get("profile", {}).get("situation"),
            "etatcivile": record.get("profile", {}).get("etatCivil"),
            "photo": record.get("google_Photo", record.get("profile", {}).get("google_Photo", None)),
            "metier": record.get("profile", {}).get("metier", None),
            "intituleposte": record.get("profile", {}).get("intituleposte", None),
            "niveau_etude_actuelle": record.get("profile", {}).get("niveau_etude_actuelle", None),
            "disponibilite": record.get("profile", {}).get("disponibilite", None)
        })
    return transformed_data

def load_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()
    reset_client_pk()
    
    insert_query = """
    INSERT INTO dim_client (matricule, nom, prenom, birthdate, nationality, adresseDomicile, pays, situation, etatcivile, photo, metier, intituleposte, niveau_etude_actuelle, disponibilite)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (matricule) DO UPDATE SET 
        nom = EXCLUDED.nom,
        prenom = EXCLUDED.prenom,
        birthdate = EXCLUDED.birthdate,
        nationality = EXCLUDED.nationality,
        adresseDomicile = EXCLUDED.adresseDomicile,
        pays = EXCLUDED.pays,
        situation = EXCLUDED.situation,
        etatcivile = EXCLUDED.etatcivile,
        photo = EXCLUDED.photo,
        metier = EXCLUDED.metier,
        intituleposte = EXCLUDED.intituleposte,
        niveau_etude_actuelle = EXCLUDED.niveau_etude_actuelle,
        disponibilite = EXCLUDED.disponibilite
    """
    
    for record in data:
        values = (
            record["matricule"],
            record["nom"].strip() if record["nom"] else None,
            record["prenom"].strip() if record["prenom"] else None,
            record["birthdate"] if record["birthdate"] else None,
            record["nationality"].strip() if record["nationality"] else None,
            record["adresseDomicile"].strip() if record["adresseDomicile"] else None,
            record["pays"].strip() if record["pays"] else None,
            record["situation"].strip() if record["situation"] else None,
            record["etatcivile"].strip() if record["etatcivile"] else None,
            record["photo"].strip() if record["photo"] else None,
            record["metier"].strip() if record["metier"] else None,
            record["intituleposte"].strip() if record["intituleposte"] else None,
            record["niveau_etude_actuelle"].strip() if record["niveau_etude_actuelle"] else None,
            record["disponibilite"].strip() if record["disponibilite"] else None
        )
        cur.execute(insert_query, values)
    
    conn.commit()
    cur.close()
    conn.close()

def main():
    print("--- Listing MongoDB Content ---")
    list_mongodb_content()
    
    raw_data = extract_from_mongodb()
    transformed_data = transform_data(raw_data)
    if transformed_data:
        load_into_postgres(transformed_data)
        print("Données insérées avec succès dans PostgreSQL.")
    else:
        print("Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
