import psycopg2
from pymongo import MongoClient
import re

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "parrainageinfluencers"

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

def generate_full_name_code(existing_codes):
    valid_codes = [code for code in existing_codes if re.match(r"^FULL\d{3}$", code)]
    
    if not valid_codes:
        return "FULL001"
    else:
        last_number = max(int(code.replace("FULL", "")) for code in valid_codes)
        new_number = last_number + 1
        return f"FULL{str(new_number).zfill(3)}"

def extract_full_names_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = collection.find({}, {"_id": 0, "fullName": 1})

    full_names = set()

    for user in mongo_data:
        if isinstance(user, dict):
            full_name = user.get("fullName", "")
            if full_name.strip():
                full_names.add(full_name.strip())

    client.close()

    print("Full names extraits:", full_names)
    return [{"fullName": name} for name in full_names]  # Ne plus générer de code, laisser PostgreSQL gérer l'auto-incrémentation

def load_full_names_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT fullName FROM dim_parinnage_influencer")
    existing_full_names = {row[0] for row in cur.fetchall()}

    insert_query = """
    INSERT INTO dim_parinnage_influencer (fullName)
    VALUES (%s)
    ON CONFLICT (fullName) DO NOTHING;
    """

    update_query = """
    UPDATE dim_parinnage_influencer
    SET fullName = %s
    WHERE fullName = %s;
    """

    for record in data:
        if record["fullName"] in existing_full_names:
            print(f"Mise à jour du fullName : {record['fullName']}")
            cur.execute(update_query, (record["fullName"], record["fullName"]))
        else:
            print(f"Insertion du fullName : {record['fullName']}")
            cur.execute(insert_query, (record["fullName"],))

    conn.commit()
    cur.close()
    conn.close()

def main():
    print("--- Extraction et chargement des fullNames ---")
    
    raw_data = extract_full_names_from_mongodb()
    
    if raw_data:
        load_full_names_into_postgres(raw_data)
        print("Données insérées/mises à jour avec succès dans PostgreSQL.")
    else:
        print("Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
