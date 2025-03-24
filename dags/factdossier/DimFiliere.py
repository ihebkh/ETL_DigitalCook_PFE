import psycopg2
from pymongo import MongoClient
import re

def get_postgresql_connection():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    
    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    return client, mongo_db

def extract_universities_from_mongodb():
    client, mongo_db = get_mongodb_connection()
    collection = mongo_db["universities"]

    universities = collection.find({}, {"_id": 0, "filiere": 1})

    filieres = []
    for university in universities:
        for filiere in university.get("filiere", []):
            filieres.append(filiere)

    client.close()
    return filieres

def extract_filieres_from_mongodb():
    client, mongo_db = get_mongodb_connection()
    collection = mongo_db["filieres"]

    filieres_data = collection.find({}, {"_id": 0, "nomfiliere": 1, "domaine": 1, "diplome": 1})

    filieres = []
    for record in filieres_data:
        filieres.append(record)

    client.close()
    return filieres

def generate_filiere_code(index):
    return f"filiere{str(index).zfill(4)}"

def clean_price(prix):
    numeric_value = re.sub(r"[^\d.,]", "", prix)
    try:
        numeric_value = numeric_value.replace(",", ".")
        return float(numeric_value) if numeric_value else None
    except ValueError:
        return None

def insert_or_update_filiere(filiere, index):
    conn = get_postgresql_connection()
    cursor = conn.cursor()

    filierecode = generate_filiere_code(index)

    nomfiliere = filiere.get("nomfiliere", "")
    domaine = filiere.get("domaine", "")
    diplome = filiere.get("diplome", "")

    prix = clean_price(filiere.get("prix", ""))
    prerequis = filiere.get("prerequis", "")
    adresse = filiere.get("adresse", "")
    codepostal = filiere.get("codepostal", "")

    query = """
    INSERT INTO dim_filiere (filierecode, nomfiliere, domaine, diplome, prix, prerequis, adresse, codepostal)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (filierecode) DO UPDATE
    SET nomfiliere = EXCLUDED.nomfiliere,
        domaine = EXCLUDED.domaine,
        diplome = EXCLUDED.diplome,
        prix = EXCLUDED.prix,
        prerequis = EXCLUDED.prerequis,
        adresse = EXCLUDED.adresse,
        codepostal = EXCLUDED.codepostal;
    """

    cursor.execute(query, (filierecode, nomfiliere, domaine, diplome, prix, prerequis, adresse, codepostal))
    conn.commit()
    cursor.close()
    conn.close()

def main():
    filieres_universities = extract_universities_from_mongodb()
    filieres_data = extract_filieres_from_mongodb()
    all_filieres = filieres_universities + filieres_data

    print("Filières extraites de MongoDB avec leurs codes générés :")
    for index, filiere in enumerate(all_filieres, start=1):
        print(f"Insertion/Modification de la filière : {filiere['nomfiliere']} - {generate_filiere_code(index)}")
        insert_or_update_filiere(filiere, index)

if __name__ == "__main__":
    main()
