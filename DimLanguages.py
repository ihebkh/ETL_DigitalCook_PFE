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

def generate_langue_code(existing_codes):
    valid_codes = [code for code in existing_codes if isinstance(code, str) and code.startswith("LANG")]
    
    if not valid_codes:
        return "LANG001"
    else:
        last_number = max(int(code.replace("LANG", "")) for code in valid_codes)
        new_number = last_number + 1
        return f"LANG{str(new_number).zfill(3)}"

def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = collection.find({}, {"_id": 0, "profile.languages": 1})

    languages = []
    existing_labels = set()  
    for user in mongo_data:
        if isinstance(user, dict) and "profile" in user and isinstance(user["profile"], dict):
            language_list = user["profile"].get("languages", [])

            if isinstance(language_list, list):  
                for lang in language_list:
                    if isinstance(lang, dict):  
                        label = lang.get("label", "").strip()
                        level = lang.get("level", "").strip()

                        if label and level and label not in existing_labels:
                            existing_labels.add(label)
                            languages.append({
                                "langue_code": None,  
                                "label": label,
                                "level": level
                            })

    client.close()
    
    print(" Langues extraites :", languages)  
    return languages

def load_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()

    cur.execute("SELECT langue_code FROM dim_languages")
    existing_codes = {row[0] for row in cur.fetchall()}

    insert_query = """
    INSERT INTO dim_languages (langue_code, label, level)
    VALUES (%s, %s, %s)
    RETURNING langue_code;
    """

    update_query = """
    UPDATE dim_languages
    SET level = %s
    WHERE langue_code = %s;
    """

    for record in data:
        if record["langue_code"] is None:  
            record["langue_code"] = generate_langue_code(existing_codes)
            existing_codes.add(record["langue_code"])  

        values = (
            record["langue_code"],
            record["label"],
            record["level"],
        )
        cur.execute("""
        SELECT 1 FROM dim_languages
        WHERE label = %s AND level = %s
        """, (record["label"], record["level"]))
        if cur.fetchone():
            print(f" Mise à jour de l'intérêt : {values}")
            cur.execute(update_query, (record["level"], record["langue_code"]))
        else:
            print(f" Insertion de l'intérêt : {values}")
            cur.execute(insert_query, values)

    conn.commit()
    cur.close()
    conn.close()

def main():
    print("--- Extraction et chargement des langues ---")
    
    raw_data = extract_from_mongodb()
    
    if raw_data:
        load_into_postgres(raw_data)
        print(" Données insérées/mises à jour avec succès dans PostgreSQL.")
    else:
        print(" Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
