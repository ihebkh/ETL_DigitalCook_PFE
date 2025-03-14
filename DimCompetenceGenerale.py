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

def generate_competence_code(existing_codes):
    if not existing_codes:
        return "COMP001" 
    else:
        last_number = max(int(code.replace("COMP", "")) for code in existing_codes)
        new_number = last_number + 1
        return f"COMP{str(new_number).zfill(3)}"

def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = collection.find({}, {"_id": 0, "profile.competenceGenerales": 1})

    competences = set()

    for user in mongo_data:
        if isinstance(user, dict) and "profile" in user and isinstance(user["profile"], dict):
            user_competences = user["profile"].get("competenceGenerales", [])

            if isinstance(user_competences, list):
                for competence in user_competences:
                    if isinstance(competence, str) and competence.strip(): 
                        competences.add(competence.strip())

    client.close()
    
    print(" Compétences extraites :", competences) 
    return [{"competenceCode": None, "competence_name": c} for c in competences]

def load_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT competenceCode FROM Dim_competence_generale")
    existing_codes = {row[0] for row in cur.fetchall()}

    insert_query = """
    INSERT INTO Dim_competence_generale (competenceCode, competence_name)
    VALUES (%s, %s)
    ON CONFLICT (competence_name) DO NOTHING;
    """

    update_query = """
    UPDATE Dim_competence_generale
    SET competence_name = %s
    WHERE competenceCode = %s;
    """

    for record in data:
        if record["competenceCode"] is None: 
            record["competenceCode"] = generate_competence_code(existing_codes)
            existing_codes.add(record["competenceCode"]) 
        cur.execute("SELECT competence_name FROM Dim_competence_generale WHERE competenceCode = %s", (record["competenceCode"],))
        existing_competence = cur.fetchone()

        if existing_competence:
            print(f" Mise à jour de l'intérêt : {record['competence_name']}")
            cur.execute(update_query, (record["competence_name"], record["competenceCode"]))
        else:
            print(f" Insertion de l'intérêt : {record['competence_name']}")
            cur.execute(insert_query, (record["competenceCode"], record["competence_name"]))

    conn.commit()
    cur.close()
    conn.close()

def main():
    print("--- Extraction et chargement des compétences ---")
    
    raw_data = extract_from_mongodb()
    
    if raw_data:
        load_into_postgres(raw_data)
        print(" Données insérées/mises à jour avec succès dans PostgreSQL.")
    else:
        print(" Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
