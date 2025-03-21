import psycopg2
from pymongo import MongoClient

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

def extract_partners_from_mongodb():
    client, mongo_db, collection = get_mongodb_connection()
    universities = collection.find({}, {"_id": 0, "partenairesAcademique": 1})

    partenaires = []
    for university in universities:
        for partenaire in university.get("partenairesAcademique", []):
            partenaires.append(partenaire)

    client.close()
    return partenaires

def generate_partenaire_academique_code(index):
    return f"partenaireAcad{str(index).zfill(4)}"

def insert_or_update_partenaire(partenaire, index):
    conn = get_postgresql_connection()
    cursor = conn.cursor()

    codepartenaireacademique = generate_partenaire_academique_code(index)

    query = """
    INSERT INTO dim_partenaire_academique (codepartenaireacademique, nom_partenaire)
    VALUES (%s, %s)
    ON CONFLICT (codepartenaireacademique) DO UPDATE
    SET nom_partenaire = EXCLUDED.nom_partenaire;
    """
    cursor.execute(query, (codepartenaireacademique, partenaire))
    conn.commit()
    cursor.close()
    conn.close()

def main():
    partenaires = extract_partners_from_mongodb()
    print("Partenaires académiques extraits de MongoDB avec leurs codes générés:")
    
    for index, partenaire in enumerate(partenaires, start=1):
        print(f"Insertion/Modification du partenaire académique : {partenaire} - {generate_partenaire_academique_code(index)}")
        insert_or_update_partenaire(partenaire, index)

if __name__ == "__main__":
    main()
