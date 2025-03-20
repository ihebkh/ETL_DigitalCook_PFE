import psycopg2
from pymongo import MongoClient

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "secteurdactivities"

    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    print("Connected to MongoDB.")
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

def insert_or_update_data(rome_code, outer_label, pk):
    conn = get_postgres_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO dim_secteur (secteur_code, label, metier_fk)
        VALUES (%s, %s, %s)
        ON CONFLICT (secteur_code) 
        DO UPDATE SET label = EXCLUDED.label, metier_fk = EXCLUDED.metier_fk
    """, (rome_code, outer_label, pk))
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

        if 'jobs' in doc:
            for job in doc['jobs']:
                rome_code = job.get('romeCode')
                if rome_code:
                    print(f"ROMECODE: {rome_code}, Label: {outer_label}")
                    cursor.execute("SELECT metier_pk FROM public.dim_metier WHERE romecode = %s", (rome_code,))
                    result = cursor.fetchone()

                    if result:
                        pk = result[0]
                        print(f"  Associated PK: {pk}")
                        insert_or_update_data(rome_code, outer_label, pk)
                    else:
                        print(f"  No PK found for ROME code {rome_code}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    extract_labels_and_insert()
