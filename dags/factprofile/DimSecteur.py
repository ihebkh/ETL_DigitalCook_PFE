import psycopg2
from pymongo import MongoClient

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "secteurdactivities"

    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return client, mongo_db, collection

def get_postgres_connection():
    conn = psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )
    return conn

def generate_secteur_code(existing_codes):
    if not existing_codes:
        return "sect0001"
    else:
        last_number = max(int(code.replace("sect", "")) for code in existing_codes)
        new_number = last_number + 1
        return f"sect{str(new_number).zfill(4)}"

def insert_or_update_data(outer_label):
    conn = get_postgres_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT secteur_code FROM dim_secteur")
    existing_codes = [row[0] for row in cursor.fetchall()]

    new_secteur_code = generate_secteur_code(existing_codes)

    transformed_label = outer_label.strip().lower()

    cursor.execute("""
        INSERT INTO dim_secteur (secteur_code, label)
        VALUES (%s, %s)
        ON CONFLICT (label) 
        DO UPDATE SET secteur_code = EXCLUDED.secteur_code
    """, (new_secteur_code, transformed_label))
    
    conn.commit()
    cursor.close()
    conn.close()

def extract_labels_and_insert():
    client, mongo_db, collection = get_mongodb_connection()

    conn = get_postgres_connection()
    cursor = conn.cursor()

    documents = collection.find()

    for doc in documents:
        outer_label = doc.get('label')
        insert_or_update_data(outer_label)

    cursor.close()
    conn.close()

if __name__ == "__main__":
    extract_labels_and_insert()
