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

def get_existing_competences():
    """ Récupère les compétences existantes dans la base de données. """
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT experience_name FROM dim_competence")
    competences = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return competences

def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = list(collection.find({}, {"_id": 0, "profile.experiences.competances": 1}))  
    client.close()
    return mongo_data

def transform_data(mongo_data, existing_competences):
    transformed_data = []
    competence_code_counter = len(existing_competences)

    for record in mongo_data:
        experiences = record.get("profile", {}).get("experiences", [])
        for experience in experiences:
            competences = experience.get("competances", [])
            for competence in competences:
                competence = competence.strip() if competence else None
                if competence and competence not in existing_competences:
                    competence_code_counter += 1
                    new_competence_code = f"COMP{str(competence_code_counter).zfill(2)}"
                    transformed_data.append({
                        "competence_code": new_competence_code,
                        "experience_name": competence
                    })
                    existing_competences.add(competence)

    print("Données transformées :", transformed_data)
    return transformed_data

def load_into_postgres(data):
    if not data:
        print("Aucune compétence à insérer.")
        return
    
    conn = get_postgres_connection()
    cur = conn.cursor()
    insert_query = """
    INSERT INTO dim_competence (competence_code, experience_name)
    SELECT %s, %s
    WHERE NOT EXISTS (SELECT 1 FROM dim_competence WHERE experience_name = %s)
    """
    update_query = """
    UPDATE dim_competence
    SET experience_name = %s
    WHERE experience_name = %s
    """
    
    for record in data:
        cur.execute(insert_query, (record["competence_code"], record["experience_name"], record["experience_name"]))
        if cur.rowcount == 0:
            cur.execute(update_query, (record["experience_name"], record["experience_name"]))
    
    conn.commit()
    cur.close()
    conn.close()

def main():
    print("--- Extraction et chargement des compétences ---")
    raw_data = extract_from_mongodb()
    
    existing_competences = get_existing_competences()
    
    transformed_data = transform_data(raw_data, existing_competences)
    
    if transformed_data:
        load_into_postgres(transformed_data)
        print("Données insérées avec succès dans PostgreSQL.")
    else:
        print("Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
