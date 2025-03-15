import os
import json
import psycopg2
from pymongo import MongoClient

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"

    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    print("Connected to MongoDB.")
    return client, mongo_db, collection

def get_postgres_connection():
    conn = psycopg2.connect(dbname="DW_DigitalCook", user='postgres', password='admin', host='localhost', port='5432')
    print("Connected to PostgreSQL.")
    return conn

def get_next_certification_code(current_count):
    return f"certif{str(current_count).zfill(4)}"  # Incremental code with leading zeros (e.g., "certif0001")

def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = list(collection.find({}, {"_id": 0}))  # Fetching all data from MongoDB
    print(f"Extracted {len(mongo_data)} records from MongoDB.")
    
    # Display some details of the extracted data (showing the first few records for example)
    print("First few records from MongoDB:")
    for record in mongo_data[:3]:  # Show first 3 records
        print(record)
    
    client.close()
    return mongo_data

def transform_data(mongo_data):
    seen_certifications = set()
    transformed_data = []
    current_certification_code = 1  # Initialize the starting code as certif0001
    
    for idx, record in enumerate(mongo_data):
        certifications_list = []
        
        # Extract from profile
        if "profile" in record and "certifications" in record["profile"]:
            certifications_list.extend(record["profile"]["certifications"])

        # Extract from simpleProfile
        if "simpleProfile" in record and "certifications" in record["simpleProfile"]:
            certifications_list.extend(record["simpleProfile"]["certifications"])

        for cert in certifications_list:
            # Check if the certification is a string or a dictionary
            if isinstance(cert, str):
                nomCertification = cert.strip()
                year = "Unknown"  # Default value if year and month are not provided
                month = "Unknown"
            elif isinstance(cert, dict):
                nomCertification = cert.get("nomCertification", "").strip()
                year = cert.get("year", "").strip()
                month = cert.get("month", "").strip()
            else:
                # Skip invalid data types
                continue

            if nomCertification and (nomCertification, year, month) not in seen_certifications:
                seen_certifications.add((nomCertification, year, month))
                
                # Generate the certification code
                certification_code = get_next_certification_code(current_certification_code)
                current_certification_code += 1  # Increment the certification code for next record
                
                transformed_data.append({
                    "certification_code": certification_code,
                    "nomCertification": nomCertification,
                    "year": year,
                    "month": month
                })
        
        # Display transformation details
        print(f"Processed record {idx + 1} from MongoDB: {record}")
    
    print(f"Transformed {len(transformed_data)} records.")
    return transformed_data

def load_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    insert_query = """
    INSERT INTO dim_certification (certificationcode, nom, year, month)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (certificationcode) DO UPDATE SET 
        certificationcode = EXCLUDED.certificationcode
    """
    
    for idx, record in enumerate(data):
        values = (
            record["certification_code"],
            record["nomCertification"],
            record["year"],
            record["month"]
        )
        cur.execute(insert_query, values)

        # Display each record being inserted into PostgreSQL
        print(f"Inserting record {idx + 1} into PostgreSQL: {record}")
    
    conn.commit()
    print(f"Inserted {len(data)} records into PostgreSQL.")
    cur.close()
    conn.close()

def main():
    print("--- Extraction et chargement des certifications ---")
    raw_data = extract_from_mongodb()
    transformed_data = transform_data(raw_data)
    if transformed_data:
        load_into_postgres(transformed_data)
        print("Données insérées avec succès dans PostgreSQL.")
    else:
        print("Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
