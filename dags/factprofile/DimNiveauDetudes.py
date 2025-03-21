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

def safe_int(value):
    try:
        return int(value) if value and isinstance(value, (str, int)) and str(value).isdigit() else None
    except ValueError:
        return None

def generate_diplome_code(existing_codes):
    if existing_codes:
        existing_numbers = [int(code.replace("DIP", "")) for code in existing_codes if isinstance(code, str) and code.startswith("DIP")]
        if existing_numbers:
            last_number = max(existing_numbers) + 1
        else:
            last_number = 1
    else:
        last_number = 1
    new_code = f"DIP{str(last_number).zfill(3)}"
    existing_codes.add(new_code)
    return new_code


def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = collection.find({}, {"_id": 0, "profile.niveauDetudes": 1, "simpleProfile.niveauDetudes": 1})

    niveaux_etudes = []
    
    for user in mongo_data:
        if isinstance(user, dict) and "profile" in user and isinstance(user["profile"], dict):
            niveau_list = user["profile"].get("niveauDetudes", [])
            
            if isinstance(niveau_list, list):
                for niveau in niveau_list:
                    if isinstance(niveau, dict) and niveau.get("label"):
                        niveaux_etudes.append({
                            "diplome_code": None,  
                            "label": niveau.get("label"),
                            "universite": niveau.get("universite"),
                            "start_year": safe_int(niveau.get("du", {}).get("year", "")),
                            "start_month": safe_int(niveau.get("du", {}).get("month", "")),
                            "end_year": safe_int(niveau.get("au", {}).get("year", "")),
                            "end_month": safe_int(niveau.get("au", {}).get("month", "")),
                            "nom_diplome": niveau.get("nomDiplome"),
                            "pays": niveau.get("pays"),
                        })

        if isinstance(user, dict) and "simpleProfile" in user and isinstance(user["simpleProfile"], dict):
            niveau_list = user["simpleProfile"].get("niveauDetudes", [])
            
            if isinstance(niveau_list, list):
                for niveau in niveau_list:
                    if isinstance(niveau, dict) and niveau.get("label"):
                        niveaux_etudes.append({
                            "diplome_code": None,  
                            "label": niveau.get("label"),
                            "universite": niveau.get("universite"),
                            "start_year": safe_int(niveau.get("du", {}).get("year", "")),
                            "start_month": safe_int(niveau.get("du", {}).get("month", "")),
                            "end_year": safe_int(niveau.get("au", {}).get("year", "")),
                            "end_month": safe_int(niveau.get("au", {}).get("month", "")),
                            "nom_diplome": niveau.get("nomDiplome"),
                            "pays": niveau.get("pays"),
                        })

    client.close()
    
    print(" Niveaux d'études extraits :", niveaux_etudes) 
    return niveaux_etudes

def load_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT SUBSTRING(diplome_code, 4)::INTEGER FROM dim_niveau_d_etudes WHERE diplome_code LIKE 'DIP%'")
    existing_codes = {row[0] for row in cur.fetchall()} 

    insert_query = """
    INSERT INTO dim_niveau_d_etudes (diplome_code, label, universite, start_year, start_month, end_year, end_month, nom_diplome, pays)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING diplome_code;
    """

    update_query = """
    UPDATE dim_niveau_d_etudes
    SET label = %s, universite = %s, start_year = %s, start_month = %s, end_year = %s, end_month = %s, nom_diplome = %s, pays = %s
    WHERE diplome_code = %s;
    """

    for record in data:
        if not any([record.get("label"), record.get("universite"), record.get("nom_diplome")]):
            print(f"Skipping record with missing essential data: {record}")
            continue

        if record["diplome_code"] is None:  
            record["diplome_code"] = generate_diplome_code(existing_codes)
            existing_codes.add(record["diplome_code"])  

        values = (
            record["diplome_code"],
            record["label"],
            record["universite"],
            record["start_year"],
            record["start_month"],
            record["end_year"],
            record["end_month"],
            record["nom_diplome"],
            record["pays"],
        )
        cur.execute("SELECT 1 FROM dim_niveau_d_etudes WHERE diplome_code = %s", (record["diplome_code"],))

        if cur.fetchone():
            print(f" Mise à jour du diplôme {record['diplome_code']}: {values}")
            cur.execute(update_query, (*values[1:], record["diplome_code"]))  
        else:
            print(f" Insertion du diplôme {record['diplome_code']}: {values}")
            cur.execute(insert_query, values)

    conn.commit()
    cur.close()
    conn.close()

def main():
    print("--- Extraction et chargement des niveaux d'études ---")
    
    raw_data = extract_from_mongodb()
    
    if raw_data:
        load_into_postgres(raw_data)
        print(" Données insérées/mises à jour avec succès dans PostgreSQL.")
    else:
        print(" Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
