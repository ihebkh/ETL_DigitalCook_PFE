from pymongo import MongoClient
import psycopg2

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "secteurdactivities"

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

def extract_jobs_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = collection.find({}, {"_id": 0, "jobs": 1})

    jobs = []

    for document in mongo_data:
        if "jobs" in document:
            for job in document["jobs"]:
                job_info = {
                    "label": job.get("label"),
                    "romeCode": job.get("romeCode"),
                    "mainName": job.get("mainName"),
                    "subDomain": job.get("subDomain")
                }
                jobs.append(job_info)

    client.close()
    return jobs

def load_jobs_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()

    for job in data:
        cur.execute("""
            INSERT INTO Dim_Metier (romeCode, label_jobs, mainname_jobs, subdomain_jobs)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (romeCode) DO UPDATE SET
                label_jobs = EXCLUDED.label_jobs,
                mainname_jobs = EXCLUDED.mainname_jobs,
                subdomain_jobs = EXCLUDED.subdomain_jobs;
        """, (job["romeCode"], job["label"], job["mainName"], job["subDomain"]))

    conn.commit()
    cur.close()
    conn.close()

def main():
    print("--- Extraction et chargement des jobs ---")
    
    jobs_data = extract_jobs_from_mongodb()
    
    if jobs_data:
        load_jobs_into_postgres(jobs_data)
        print("Données des jobs insérées ou mises à jour avec succès dans PostgreSQL.")
    else:
        print("Aucune donnée de jobs à insérer.")

if __name__ == "__main__":
    main()