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
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

def generate_location_code(existing_codes):
    if not existing_codes:
        return "LOC001" 
    else:
        last_number = max(int(code.replace("LOC", "")) for code in existing_codes)
        new_number = last_number + 1
        return f"LOC{str(new_number).zfill(3)}"

def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = collection.find({}, {"_id": 0, "profile.preferedJobLocations": 1})

    job_locations = []
    existing_entries = set() 

    for user in mongo_data:
        if isinstance(user, dict) and "profile" in user and isinstance(user["profile"], dict):
            locations = user["profile"].get("preferedJobLocations", [])

            if isinstance(locations, list):
                for loc in locations:
                    if isinstance(loc, dict):
                        pays = loc.get("pays", "").strip()
                        ville = loc.get("ville", "").strip()
                        region = loc.get("region", "").strip()

                       
                        location_tuple = (pays, ville, region)
                        if location_tuple not in existing_entries:
                            existing_entries.add(location_tuple)
                            job_locations.append({
                                "preferedJobLocationsCode": None,
                                "pays": pays,
                                "ville": ville,
                                "region": region
                            })

    client.close()
    
    print(" Localisations extraites :", job_locations)
    return job_locations

def load_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT preferedJobLocationsCode, pays, ville, region FROM Dim_preferedJobLocations")
    existing_entries = {(row[1], row[2], row[3]): row[0] for row in cur.fetchall()}
    insert_or_update_query = """
    INSERT INTO Dim_preferedJobLocations (preferedJobLocationsCode, pays, ville, region)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (preferedJobLocationsCode) 
    DO UPDATE SET 
        pays = EXCLUDED.pays,
        ville = EXCLUDED.ville,
        region = EXCLUDED.region;
    """

    for record in data:
        location_tuple = (record["pays"], record["ville"], record["region"])
        if location_tuple in existing_entries:
            record["preferedJobLocationsCode"] = existing_entries[location_tuple]
        else:
            if record["preferedJobLocationsCode"] is None:
                record["preferedJobLocationsCode"] = generate_location_code(existing_entries.values())
            existing_entries[location_tuple] = record["preferedJobLocationsCode"]

        values = (
            record["preferedJobLocationsCode"],
            record["pays"],
            record["ville"],
            record["region"],
        )
        print(f"Insertion / Mise à jour : {values}")
        cur.execute(insert_or_update_query, values)

    conn.commit()
    cur.close()
    conn.close()


def main():
    print("--- Extraction et chargement des préférences de job ---")
    
    raw_data = extract_from_mongodb()
    
    if raw_data:
        load_into_postgres(raw_data)
        print(" Données insérées/mises à jour avec succès dans PostgreSQL.")
    else:
        print(" Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
