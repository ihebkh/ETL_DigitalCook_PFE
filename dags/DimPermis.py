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

def get_next_permis_code():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM dim_permis_conduire")
    count = cur.fetchone()[0] + 1
    cur.close()
    conn.close()
    return f"code{str(count).zfill(3)}"  # Updated to return the format "code001", "code002", ...

def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = list(collection.find({}, {"_id": 0}))  
    client.close()
    return mongo_data

def transform_data(mongo_data):
    seen_categories = set()
    transformed_data = []
    
    for record in mongo_data:
        permis_list = []
        
        # Extract from profile
        if "profile" in record and "permisConduire" in record["profile"]:
            permis_list.extend(record["profile"]["permisConduire"])

        # Extract from simpleProfile
        if "simpleProfile" in record and "permisConduire" in record["simpleProfile"]:
            permis_list.extend(record["simpleProfile"]["permisConduire"])

        for permis in permis_list:
            category = permis.strip()
            if category and category not in seen_categories:
                seen_categories.add(category)
                transformed_data.append({
                    "permis_code": get_next_permis_code(),
                    "categorie": category
                })
    
    return transformed_data

def load_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    insert_query = """
    INSERT INTO dim_permis_conduire (permis_code, categorie)
    VALUES (%s, %s)
    ON CONFLICT (categorie) DO UPDATE SET 
        permis_code = EXCLUDED.permis_code
    """
    
    for record in data:
        values = (
            record["permis_code"],
            record["categorie"]
        )
        cur.execute(insert_query, values)
    
    conn.commit()
    cur.close()
    conn.close()

def main():
    print("--- Extraction et chargement des permis de conduire ---")
    raw_data = extract_from_mongodb()
    transformed_data = transform_data(raw_data)
    if transformed_data:
        load_into_postgres(transformed_data)
        print("Données insérées avec succès dans PostgreSQL.")
    else:
        print("Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
