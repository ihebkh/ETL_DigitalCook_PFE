import psycopg2
from pymongo import MongoClient
import re

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

def generate_interests_code(existing_codes):
    valid_codes = [code for code in existing_codes if re.match(r"^INT\d{3}$", code)]
    
    if not valid_codes:
        return "INT001"
    else:
        last_number = max(int(code.replace("INT", "")) for code in valid_codes)
        new_number = last_number + 1
        return f"INT{str(new_number).zfill(3)}"

def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = collection.find({}, {"_id": 0, "matricule": 1, "profile.interests": 1, "simpleProfile.interests": 1})

    interests = set()

    for user in mongo_data:
        if isinstance(user, dict):
            if "profile" in user and isinstance(user["profile"], dict):
                user_interests = user["profile"].get("interests", [])
                if isinstance(user_interests, list):
                    for interest in user_interests:
                        if isinstance(interest, str) and interest.strip():
                            interests.add(interest.strip())
            if "simpleProfile" in user and isinstance(user["simpleProfile"], dict):
                user_interests = user["simpleProfile"].get("interests", [])
                if isinstance(user_interests, list):
                    for interest in user_interests:
                        if isinstance(interest, str) and interest.strip():
                            interests.add(interest.strip())

    client.close()
    
    print(" Intérêts extraits :", interests)
    return [{"interestsCode": None, "interests": i} for i in interests]

def load_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT interests FROM Dim_interests")
    existing_interests = {row[0] for row in cur.fetchall()}
    cur.execute("SELECT interestsCode FROM Dim_interests")
    existing_codes = {row[0] for row in cur.fetchall()}

    insert_query = """
    INSERT INTO Dim_interests (interestsCode, interests)
    VALUES (%s, %s)
    ON CONFLICT (interestsCode) DO NOTHING;
    """

    update_query = """
    UPDATE Dim_interests
    SET interests = %s
    WHERE interestsCode = %s;
    """

    for record in data:
        if record["interestsCode"] is None:
            record["interestsCode"] = generate_interests_code(existing_codes)
            existing_codes.add(record["interestsCode"])
        if record["interests"] in existing_interests:
            print(f" Mise à jour de l'intérêt : {record['interests']}")
            cur.execute(update_query, (record["interests"], record["interestsCode"]))
        else:
            print(f" Insertion de l'intérêt : {record['interests']}")
            cur.execute(insert_query, (record["interestsCode"], record["interests"]))

    conn.commit()
    cur.close()
    conn.close()

def main():
    print("--- Extraction et chargement des intérêts ---")
    
    raw_data = extract_from_mongodb()
    
    if raw_data:
        load_into_postgres(raw_data)
        print(" Données insérées/mises à jour avec succès dans PostgreSQL.")
    else:
        print(" Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
