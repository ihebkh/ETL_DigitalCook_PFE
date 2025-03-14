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

def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = list(collection.find({}, {"_id": 0, "profile": 1, "simpleProfile": 1})) 
    client.close()
    return mongo_data

def transform_data(mongo_data):
    transformed_data = []
    visa_counter = 1

    for record in mongo_data:
        profiles = [record.get("profile", {})]
        simple_profile = record.get("simpleProfile", {})
        if simple_profile:
            profiles.append(simple_profile)

        for profile_item in profiles:
            visas = profile_item.get("visa", [])

            for visa in visas:
                if not visa:
                    continue 
                
                transformed_data.append({
                    "visa_code": f"VISA{str(visa_counter).zfill(2)}",
                    "visa_type": visa.get("type", "").strip() or None,
                    "date_entree": visa.get("dateEntree"),
                    "date_sortie": visa.get("dateSortie"),
                    "destination": visa.get("destination", "").strip() or None,
                    "duree": visa.get("dureeValidite", {}).get("duree", 0),
                    "duree_type": visa.get("dureeValidite", {}).get("type", "").strip() or None,
                    "nb_entree": visa.get("nbEntree", "").strip() or None
                })
                visa_counter += 1

    return transformed_data

def load_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO dim_visa (visacode, visa_type, date_entree, date_sortie, destination, duree, duree_type, nb_entree)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (visacode) DO UPDATE SET 
        visa_type = EXCLUDED.visa_type,
        date_entree = EXCLUDED.date_entree,
        date_sortie = EXCLUDED.date_sortie,
        destination = EXCLUDED.destination,
        duree = EXCLUDED.duree,
        duree_type = EXCLUDED.duree_type,
        nb_entree = EXCLUDED.nb_entree
    RETURNING visacode, visa_type, date_entree, date_sortie, destination, duree, duree_type, nb_entree, xmin;
    """

    for record in data:
        values = (
            record["visa_code"],
            record["visa_type"],
            record["date_entree"],
            record["date_sortie"],
            record["destination"],
            record["duree"],
            record["duree_type"],
            record["nb_entree"]
        )
        cur.execute(insert_query, values)
        result = cur.fetchone()
        if result:
            if result[-1] == 0:
                print(f"Inserted new record: {result}")
            else:
                print(f"Updated existing record: {result}")

    conn.commit()
    cur.close()
    conn.close()

def main():
    print("--- Extraction et chargement des visas ---")
    raw_data = extract_from_mongodb()
    if not raw_data:
        print(" Aucune donnée trouvée dans MongoDB.")
        return

    transformed_data = transform_data(raw_data)
    if transformed_data:
        print(" Données transformées :")
        for record in transformed_data:
            print(f"visa_code: {record['visa_code']}, visa_type: {record['visa_type']}, date_entree: {record['date_entree']}, date_sortie: {record['date_sortie']}, destination: {record['destination']}, duree: {record['duree']}, duree_type: {record['duree_type']}, nb_entree: {record['nb_entree']}")
        
        load_into_postgres(transformed_data)
        print(" Données insérées/mises à jour avec succès dans PostgreSQL.")
    else:
        print(" Aucune donnée de visa à insérer.")

if __name__ == "__main__":
    main()
