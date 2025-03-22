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
    MONGO_COLLECTION = "universities" 

    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return client, mongo_db, collection

def extract_filiere_from_mongodb():
    client, mongo_db, collection = get_mongodb_connection()

    universities = collection.find({}, {"_id": 0, "filiere": 1})

    filieres = []

    for university in universities:
        for filiere in university.get("filiere", []):
            filieres.append(filiere)

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
    filieres = extract_filiere_from_mongodb()
    print("Filières extraites de MongoDB avec leurs codes générés:")
    
    for index, filiere in enumerate(filieres, start=1):
        print(f"Insertion/Modification de la filière : {filiere['nomfiliere']} - {generate_filiere_code(index)}")
        insert_or_update_filiere(filiere, index)

if __name__ == "__main__":
    main()
