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

def generate_certification_code(existing_codes):
    if not existing_codes:
        return "CERT001"  
    else:
        last_number = max(int(code.replace("CERT", "")) for code in existing_codes)
        new_number = last_number + 1
        return f"CERT{str(new_number).zfill(3)}"

def validate_year_month(year, month):
    if not (year.isdigit() and len(year) == 4):
        year = None 
    if not (month.isdigit() and 1 <= int(month) <= 12):
        month = None

    return year, month

def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = collection.find({}, {"_id": 0, "profile.certifications": 1})

    certifications = set() 

    for user in mongo_data:
        if isinstance(user, dict) and "profile" in user and isinstance(user["profile"], dict):
            user_certifications = user["profile"].get("certifications", [])

            if isinstance(user_certifications, list):
                for certif in user_certifications:
                    if isinstance(certif, dict):
                        nom = certif.get("nomCertification", "").strip()
                        year = certif.get("year", "").strip()
                        month = certif.get("month", "").strip()
                        year, month = validate_year_month(year, month)

                        if nom:
                            certifications.add((nom, year, month))

    client.close()
    
    print(" Certifications extraites :", certifications)
    return [{"certificationCode": None, "nom": c[0], "year": c[1], "month": c[2]} for c in certifications]

def load_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT certificationCode, nom, year, month FROM Dim_certification")
    existing_certifications = {(row[1], row[2], row[3]): row[0] for row in cur.fetchall()}

    update_query = """
    UPDATE Dim_certification
    SET certificationCode = %s, nom = %s, year = %s, month = %s
    WHERE nom = %s AND year = %s AND month = %s;
    """

    insert_query = """
    INSERT INTO Dim_certification (certificationCode, nom, year, month)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (certificationCode) DO NOTHING;
    """

    for record in data:
        if record["certificationCode"] is None:
            record["certificationCode"] = generate_certification_code(existing_certifications.values())
            existing_certifications[(record["nom"], record["year"], record["month"])] = record["certificationCode"]
        existing_code = existing_certifications.get((record["nom"], record["year"], record["month"]))

        if existing_code:
            values = (
                record["certificationCode"],
                record["nom"],
                record["year"],
                record["month"],
                record["nom"],
                record["year"],
                record["month"],
            )
            print(f" Mise à jour : {values}")
            cur.execute(update_query, values)
        else:
            values = (
                record["certificationCode"],
                record["nom"],
                record["year"],
                record["month"],
            )
            print(f" Insertion : {values}")
            cur.execute(insert_query, values)

    conn.commit()
    cur.close()
    conn.close()

def main():
    print("--- Extraction et chargement des certifications ---")
    
    raw_data = extract_from_mongodb()
    
    if raw_data:
        load_into_postgres(raw_data)
        print(" Données insérées/mises à jour avec succès dans PostgreSQL.")
    else:
        print(" Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
