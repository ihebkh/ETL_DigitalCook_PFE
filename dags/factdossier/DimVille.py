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

def extract_villes_from_mongodb():
    client, mongo_db, collection = get_mongodb_connection()
    universities = collection.find({}, {"_id": 0, "ville": 1})
    villes = set()
    for university in universities:
        villes.update(university.get("ville", []))

    client.close()
    return list(villes)

def generate_city_code(index):
    return f"ville{str(index).zfill(4)}"

def insert_or_update_ville(codeville, nom_ville):
    conn = get_postgresql_connection()
    cursor = conn.cursor()

    query = """
    INSERT INTO dim_ville (codeville, nom_ville)
    VALUES (%s, %s)
    ON CONFLICT (codeville) DO UPDATE
    SET nom_ville = EXCLUDED.nom_ville;
    """
    cursor.execute(query, (codeville, nom_ville))
    conn.commit()
    cursor.close()
    conn.close()

def main():
    villes = extract_villes_from_mongodb()
    print("Villes extraites de MongoDB avec leurs codes générés:")
    
    for index, ville in enumerate(villes, start=1):
        city_code = generate_city_code(index)
        print(f"Insertion/Modification de la ville : {ville} - {city_code}")
        insert_or_update_ville(city_code, ville)  

if __name__ == "__main__":
    main()
